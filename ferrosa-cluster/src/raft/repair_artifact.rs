//! Incoming repair bytes only; sealing confers no physical activation authority.

use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

/// One attempt's file, retained across the transition from writer to reader.
struct OwnedRepairFile {
    file: File,
    path: PathBuf,
}

impl Drop for OwnedRepairFile {
    fn drop(&mut self) {
        // Cleanup is limited to the path successfully created by this attempt.
        let _ = fs::remove_file(&self.path);
    }
}

/// Streams borrowed chunks into a fresh file with fixed-size verification state.
/// The caller must provide a path in a private, stable staging directory.
pub(crate) struct RepairArtifactStage {
    owned: OwnedRepairFile,
    expected_len: u64,
    received: u64,
    expected_digest: [u8; 32],
    hasher: Sha256,
    failed: bool,
}

impl RepairArtifactStage {
    /// Rejects declarations over the configured limit before creating any file.
    pub(crate) fn begin(
        path: &Path,
        expected_len: u64,
        max_len: u64,
        expected_digest: [u8; 32],
    ) -> io::Result<Self> {
        if expected_len > max_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "repair artifact declaration exceeds configured limit",
            ));
        }
        let path = path.to_path_buf();
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        // Do not arm cleanup until create_new succeeds: an existing path belongs
        // to someone else, including when it is a symlink.
        let file = options.open(&path)?;
        Ok(Self {
            owned: OwnedRepairFile { file, path },
            expected_len,
            received: 0,
            expected_digest,
            hasher: Sha256::new(),
            failed: false,
        })
    }

    /// Validates all bounds before writing; any error permanently poisons staging.
    pub(crate) fn write_chunk(&mut self, offset: u64, bytes: &[u8]) -> io::Result<()> {
        if self.failed {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "repair artifact staging already failed",
            ));
        }
        self.failed = true;
        if offset != self.received {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "repair artifact chunk offset is not sequential",
            ));
        }
        let length = u64::try_from(bytes.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "repair artifact chunk too large",
            )
        })?;
        let end = offset.checked_add(length).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "repair artifact offset overflow",
            )
        })?;
        // begin established expected_len <= max_len, so this enforces both caps.
        if end > self.expected_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "repair artifact chunk exceeds declared length",
            ));
        }
        self.owned.file.write_all(bytes)?;
        self.hasher.update(bytes);
        self.received = end;
        self.failed = false;
        Ok(())
    }

    /// Explicit EOF: verify exact bytes, sync, and replace the writer with a reader.
    pub(crate) fn finish(mut self) -> io::Result<SealedRepairArtifact> {
        if self.failed || self.received != self.expected_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "repair artifact staging failed or length is incomplete",
            ));
        }
        let digest: [u8; 32] = self.hasher.finalize().into();
        if digest != self.expected_digest {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "repair artifact SHA256 mismatch",
            ));
        }
        self.owned.file.sync_all()?;
        self.owned.file = File::open(&self.owned.path)?;
        Ok(SealedRepairArtifact { owned: self.owned })
    }
}

/// Owns verified incoming bytes through a private read-only handle at offset zero.
/// Dropping it removes the unadopted file. This is not a verified storage checkpoint.
pub(crate) struct SealedRepairArtifact {
    owned: OwnedRepairFile,
}

impl Read for SealedRepairArtifact {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.owned.file.read(bytes)
    }
}

impl Seek for SealedRepairArtifact {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.owned.file.seek(position)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::{Read, Seek, SeekFrom};

    use super::{RepairArtifactStage, SealedRepairArtifact};
    use crate::raft::quarantine::RecoveryAdmission;

    #[test]
    fn only_exact_ordered_finished_bytes_seal_an_owned_artifact() {
        // RED: these production types intentionally do not exist yet.
        // Proposed byte-only API (no metadata/admission/activation authority):
        // begin(&Path, expected_len: u64, max_len: u64, sha256: [u8; 32])
        //     -> Result<RepairArtifactStage, _>
        // write_chunk(&mut self, offset: u64, bytes: &[u8]) -> Result<(), _>
        // finish(self) -> Result<SealedRepairArtifact, _> // explicit EOF
        // Dropping either owner removes only its unadopted staging file.
        // SealedRepairArtifact owns a private read-only handle, implements
        // Read + Seek, and exposes no File, writable clone, or mutable handle.
        // The writer/hasher must stream borrowed chunks directly to disk:
        // no whole-transfer Vec or per-chunk clone. Construction may allocate
        // fixed metadata and one staging path. This tiny functional test does
        // not measure allocations; GREEN must also review that implementation.
        const PAYLOAD: &[u8] = b"abc";
        // Computed outside this test with Python hashlib.sha256(b"abc").
        const SHA256: [u8; 32] = [
            0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae,
            0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61,
            0xf2, 0x00, 0x15, 0xad,
        ];

        // Reject common writable-handle escapes without adding a dependency.
        // A GREEN API review must also reject explicit File-returning methods.
        macro_rules! assert_not_impl {
            ($ty:ty, $bound:path) => {{
                trait AmbiguousIfImpl<A> {
                    fn probe() {}
                }
                impl<T: ?Sized> AmbiguousIfImpl<()> for T {}
                struct Implemented;
                impl<T: ?Sized + $bound> AmbiguousIfImpl<Implemented> for T {}
                let _ = <$ty as AmbiguousIfImpl<_>>::probe;
            }};
        }
        assert_not_impl!(SealedRepairArtifact, std::io::Write);
        assert_not_impl!(SealedRepairArtifact, std::ops::Deref);
        assert_not_impl!(SealedRepairArtifact, AsRef<fs::File>);
        assert_not_impl!(SealedRepairArtifact, AsMut<fs::File>);

        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        enum Outcome {
            Sealed,
            BeginRejected,
            ChunkRejected,
            FinishRejected,
            Cancelled,
        }
        struct Case {
            name: &'static str,
            expected_len: u64,
            max_len: u64,
            digest: [u8; 32],
            chunks: &'static [(u64, &'static [u8])],
            existing: bool,
            outcome: Outcome,
        }
        let ordinary = |name, chunks, outcome| Case {
            name,
            expected_len: 3,
            max_len: 3,
            digest: SHA256,
            chunks,
            existing: false,
            outcome,
        };
        let cases = [
            ordinary(
                "one chunk at length limit",
                &[(0, PAYLOAD)],
                Outcome::Sealed,
            ),
            ordinary("ordered split", &[(0, b"a"), (1, b"bc")], Outcome::Sealed),
            ordinary(
                "one-byte chunks",
                &[(0, b"a"), (1, b"b"), (2, b"c")],
                Outcome::Sealed,
            ),
            Case {
                max_len: 2,
                ..ordinary("over limit before staging", &[], Outcome::BeginRejected)
            },
            Case {
                expected_len: u64::MAX,
                ..ordinary(
                    "huge declaration before staging",
                    &[],
                    Outcome::BeginRejected,
                )
            },
            ordinary(
                "wrong initial offset",
                &[(1, PAYLOAD)],
                Outcome::ChunkRejected,
            ),
            ordinary(
                "duplicate offset",
                &[(0, b"a"), (0, b"bc")],
                Outcome::ChunkRejected,
            ),
            ordinary("gap", &[(0, b"a"), (2, b"bc")], Outcome::ChunkRejected),
            ordinary(
                "out-of-order offset",
                &[(0, b"ab"), (1, b"c")],
                Outcome::ChunkRejected,
            ),
            ordinary(
                "overflowing offset",
                &[(u64::MAX, b"a")],
                Outcome::ChunkRejected,
            ),
            ordinary(
                "excess first chunk",
                &[(0, b"abcd")],
                Outcome::ChunkRejected,
            ),
            ordinary(
                "excess after exact bytes",
                &[(0, PAYLOAD), (3, b"d")],
                Outcome::ChunkRejected,
            ),
            ordinary("truncated", &[(0, b"ab")], Outcome::FinishRejected),
            ordinary("EOF before bytes", &[], Outcome::FinishRejected),
            ordinary(
                "same-length corrupt bytes",
                &[(0, b"abd")],
                Outcome::FinishRejected,
            ),
            Case {
                digest: [0; 32],
                ..ordinary(
                    "wrong expected digest",
                    &[(0, PAYLOAD)],
                    Outcome::FinishRejected,
                )
            },
            ordinary("cancel before bytes", &[], Outcome::Cancelled),
            ordinary("cancel partial transfer", &[(0, b"a")], Outcome::Cancelled),
            ordinary(
                "exact bytes without EOF",
                &[(0, PAYLOAD)],
                Outcome::Cancelled,
            ),
            Case {
                existing: true,
                ..ordinary(
                    "never overwrite existing artifact",
                    &[],
                    Outcome::BeginRejected,
                )
            },
        ];

        for case in cases {
            // Given isolated staging, another attempt's file, and quarantine.
            let directory = tempfile::tempdir().unwrap();
            let staging = directory.path().join("this-attempt.part");
            let other_attempt = directory.path().join("other-attempt.part");
            fs::write(&other_attempt, b"keep other attempt").unwrap();
            if case.existing {
                fs::write(&staging, b"keep existing artifact").unwrap();
            }
            let admission = RecoveryAdmission::new();
            let epoch = admission.quarantine("dc-a", "interrupted physical effects");
            let reason = admission.reason();

            // When chunks are borrowed in order, only consuming finish (EOF)
            // can create the sealed owner. A write error poisons that attempt.
            let mut sealed: Option<SealedRepairArtifact> = None;
            let observed = match RepairArtifactStage::begin(
                &staging,
                case.expected_len,
                case.max_len,
                case.digest,
            ) {
                Err(_) => Outcome::BeginRejected,
                Ok(mut stage) => {
                    assert!(staging.is_file(), "{} did not stage to disk", case.name);
                    let mut rejected = false;
                    for &(offset, bytes) in case.chunks {
                        if stage.write_chunk(offset, bytes).is_err() {
                            rejected = true;
                            break;
                        }
                    }
                    if rejected {
                        assert!(
                            stage.finish().is_err(),
                            "{} sealed after a rejected chunk",
                            case.name
                        );
                        Outcome::ChunkRejected
                    } else if case.outcome == Outcome::Cancelled {
                        drop(stage);
                        Outcome::Cancelled
                    } else {
                        match stage.finish() {
                            Ok(artifact) => {
                                sealed = Some(artifact);
                                Outcome::Sealed
                            }
                            Err(_) => Outcome::FinishRejected,
                        }
                    }
                }
            };
            assert_eq!(observed, case.outcome, "{}", case.name);
            assert_eq!(
                sealed.is_some(),
                case.outcome == Outcome::Sealed,
                "{}",
                case.name
            );

            // Then an owned sealed reader survives consumption of the stage,
            // starts at zero, sees the exact bytes and EOF, and supports seek.
            if let Some(mut artifact) = sealed {
                let mut bytes = [0; 3];
                artifact.read_exact(&mut bytes).unwrap();
                assert_eq!(&bytes, PAYLOAD, "{}", case.name);
                let mut extra = [0; 1];
                assert_eq!(artifact.read(&mut extra).unwrap(), 0, "{}", case.name);
                artifact.seek(SeekFrom::Start(1)).unwrap();
                let mut suffix = [0; 2];
                artifact.read_exact(&mut suffix).unwrap();
                assert_eq!(&suffix, b"bc", "{}", case.name);
                assert!(staging.is_file(), "{} lost its owned staging", case.name);
                drop(artifact); // still unadopted: release this attempt only
            }
            if case.existing {
                assert_eq!(
                    fs::read(&staging).unwrap(),
                    b"keep existing artifact",
                    "{}",
                    case.name
                );
            } else {
                assert!(!staging.exists(), "{} leaked unadopted staging", case.name);
            }
            assert_eq!(
                fs::read(&other_attempt).unwrap(),
                b"keep other attempt",
                "{}",
                case.name
            );
            assert_eq!(
                fs::read_dir(directory.path()).unwrap().count(),
                1 + usize::from(case.existing),
                "{} left unexpected staging files",
                case.name
            );
            assert!(
                admission.admits().is_err(),
                "{} cleared quarantine",
                case.name
            );
            assert_eq!(admission.epoch(), epoch, "{}", case.name);
            assert_eq!(admission.reason(), reason, "{}", case.name);
        }
    }
}
