//! Module: On-disk record framing for the Accord protocol log.
//! Correctness: Correct when every record written by [`frame_record`] is
//!   recovered byte-identically by [`read_framed_log`], a crash mid-write
//!   truncates only the final record, and a pre-framing file is REPORTED
//!   rather than misparsed.
//! Last revised: 2026-07-26
//! Last changed: Created — length-prefix framing so the protocol log can be
//!   replayed at startup (t_7b5788a3).
//!
//! # Why this exists
//!
//! `FileSyncWriter` used to append serialized entries as raw concatenated
//! bytes, with no length prefix and no delimiter. But
//! `AccordProtocolEntry::deserialize` needs an EXACT record boundary — it reads
//! the CRC from the final four bytes of the slice it is given — and
//! `CrashRecoveryReplay::replay` takes records already split apart.
//!
//! So the durable log was written but could not be read back into records, and
//! nothing replayed it: Accord protocol state was lost on every restart.
//!
//! # Format
//!
//! ```text
//! [8-byte magic "FACCLOG1"]  then repeated:  [u32 LE length][length bytes]
//! ```
//!
//! The magic matters. Without it, the first four bytes of a pre-framing file
//! would be read as a length — yielding an arbitrary number that could silently
//! produce plausible-looking garbage records. With it, a legacy file is
//! identified and surfaced instead of guessed at.
//!
//! Per-record CRC still lives inside the entry (`AccordProtocolEntry`), so
//! framing only has to find boundaries; corruption WITHIN a record is caught by
//! the existing CRC check during replay.

use std::io;
use std::path::Path;

/// Magic header identifying a length-framed protocol log.
pub const FRAMED_LOG_MAGIC: &[u8; 8] = b"FACCLOG1";

/// Largest record accepted while reading. A length beyond this means the file
/// is corrupt or not actually framed; treat it as a truncated tail rather than
/// attempting a multi-gigabyte allocation from a bad length.
pub const MAX_RECORD_LEN: usize = 64 * 1024 * 1024;

/// Wrap one serialized entry in its length prefix.
pub fn frame_record(payload: &[u8]) -> Vec<u8> {
    let mut framed = Vec::with_capacity(payload.len() + 4);
    framed.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    framed.extend_from_slice(payload);
    framed
}

/// Outcome of reading a framed protocol log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FramedLogRead {
    /// Complete records, in write order, ready for `CrashRecoveryReplay::replay`.
    pub records: Vec<Vec<u8>>,
    /// A partial record at end of file — the signature of a crash mid-write.
    /// Expected and benign: that entry never completed its fsync, so no reply
    /// was ever produced for it.
    pub truncated_tail: bool,
}

/// Why a protocol log could not be read.
#[derive(Debug)]
pub enum FramedLogError {
    /// The file exists and is non-empty but carries no framing magic — written
    /// by a pre-framing build. Callers MUST surface this, never silently treat
    /// it as empty: "no records" and "records I cannot read" are different, and
    /// conflating them would quietly discard recoverable transaction state.
    LegacyUnframed { bytes: u64 },
    /// The file could not be read at all.
    Io(io::Error),
}

impl std::fmt::Display for FramedLogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LegacyUnframed { bytes } => write!(
                f,
                "protocol log has no framing magic ({bytes} bytes) — written by a \
                 pre-framing build; its records cannot be delimited"
            ),
            Self::Io(e) => write!(f, "protocol log read failed: {e}"),
        }
    }
}

impl std::error::Error for FramedLogError {}

/// Read a length-framed protocol log into its records.
///
/// A missing file yields zero records — a node that has never written protocol
/// state is a normal cold start, not an error.
///
/// A truncated final record sets `truncated_tail` and stops the walk. That is
/// the expected crash-mid-write signature: `write_and_sync` had not returned, so
/// no reply depended on that entry.
pub fn read_framed_log(path: &Path) -> Result<FramedLogRead, FramedLogError> {
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Ok(FramedLogRead {
                records: Vec::new(),
                truncated_tail: false,
            })
        }
        Err(e) => return Err(FramedLogError::Io(e)),
    };

    if bytes.is_empty() {
        return Ok(FramedLogRead {
            records: Vec::new(),
            truncated_tail: false,
        });
    }

    if bytes.len() < FRAMED_LOG_MAGIC.len() || &bytes[..FRAMED_LOG_MAGIC.len()] != FRAMED_LOG_MAGIC
    {
        return Err(FramedLogError::LegacyUnframed {
            bytes: bytes.len() as u64,
        });
    }

    let mut records = Vec::new();
    let mut pos = FRAMED_LOG_MAGIC.len();
    let mut truncated_tail = false;

    while pos < bytes.len() {
        // Length prefix must be complete.
        if pos + 4 > bytes.len() {
            truncated_tail = true;
            break;
        }
        let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().expect("4 bytes")) as usize;
        pos += 4;

        // A length past the end, or absurdly large, means the tail is torn.
        // Stop rather than allocating from a bad length.
        if len > MAX_RECORD_LEN || pos + len > bytes.len() {
            truncated_tail = true;
            break;
        }

        records.push(bytes[pos..pos + len].to_vec());
        pos += len;
    }

    Ok(FramedLogRead {
        records,
        truncated_tail,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_log(dir: &Path, chunks: &[&[u8]]) -> std::path::PathBuf {
        let path = dir.join("protocol.log");
        let mut buf = FRAMED_LOG_MAGIC.to_vec();
        for c in chunks {
            buf.extend_from_slice(&frame_record(c));
        }
        std::fs::write(&path, buf).expect("write log");
        path
    }

    #[test]
    fn records_round_trip_byte_identically() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = write_log(dir.path(), &[b"first", b"", b"a much longer third record"]);

        let read = read_framed_log(&path).expect("reads");
        assert_eq!(
            read.records,
            vec![
                b"first".to_vec(),
                Vec::new(),
                b"a much longer third record".to_vec()
            ],
            "records must survive framing byte-identically, including an empty one"
        );
        assert!(!read.truncated_tail);
    }

    /// A missing log is a cold start, not a failure.
    #[test]
    fn missing_file_is_zero_records_not_an_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let read = read_framed_log(&dir.path().join("absent.log")).expect("cold start is ok");
        assert!(read.records.is_empty());
        assert!(!read.truncated_tail);
    }

    /// Crash mid-write: the final record's payload never landed. Everything
    /// before it must still be recovered, and the tear must be reported.
    #[test]
    fn truncated_payload_keeps_earlier_records_and_flags_the_tail() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("protocol.log");
        let mut buf = FRAMED_LOG_MAGIC.to_vec();
        buf.extend_from_slice(&frame_record(b"complete"));
        buf.extend_from_slice(&(99u32).to_le_bytes()); // claims 99 bytes...
        buf.extend_from_slice(b"only-a-few"); // ...but far fewer follow
        std::fs::write(&path, buf).expect("write");

        let read = read_framed_log(&path).expect("reads");
        assert_eq!(read.records, vec![b"complete".to_vec()]);
        assert!(
            read.truncated_tail,
            "a torn final record must be reported, not silently dropped"
        );
    }

    /// Crash between the length prefix and the payload.
    #[test]
    fn truncated_length_prefix_flags_the_tail() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("protocol.log");
        let mut buf = FRAMED_LOG_MAGIC.to_vec();
        buf.extend_from_slice(&frame_record(b"complete"));
        buf.extend_from_slice(&[0x01, 0x02]); // half a length prefix
        std::fs::write(&path, buf).expect("write");

        let read = read_framed_log(&path).expect("reads");
        assert_eq!(read.records, vec![b"complete".to_vec()]);
        assert!(read.truncated_tail);
    }

    /// THE reason the magic exists: a pre-framing file must be identified, not
    /// misread. Its first four bytes would otherwise be taken as a length.
    #[test]
    fn legacy_unframed_file_is_surfaced_not_misparsed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("protocol.log");
        // Raw concatenated entries, as a pre-framing build wrote them.
        std::fs::write(&path, b"\x01raw-entry-bytes-with-crc\x02another").expect("write");

        match read_framed_log(&path) {
            Err(FramedLogError::LegacyUnframed { bytes }) => {
                assert!(bytes > 0);
            }
            other => panic!("legacy file must be surfaced, got {other:?}"),
        }
    }

    /// An absurd length must not drive an allocation.
    #[test]
    fn absurd_record_length_is_treated_as_a_torn_tail() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("protocol.log");
        let mut buf = FRAMED_LOG_MAGIC.to_vec();
        buf.extend_from_slice(&frame_record(b"good"));
        buf.extend_from_slice(&(u32::MAX).to_le_bytes());
        std::fs::write(&path, buf).expect("write");

        let read = read_framed_log(&path).expect("reads");
        assert_eq!(read.records, vec![b"good".to_vec()]);
        assert!(read.truncated_tail);
    }

    /// A file containing only the magic is a log that was created but never
    /// appended to — zero records, cleanly.
    #[test]
    fn magic_only_file_is_zero_records() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("protocol.log");
        std::fs::write(&path, FRAMED_LOG_MAGIC).expect("write");

        let read = read_framed_log(&path).expect("reads");
        assert!(read.records.is_empty());
        assert!(!read.truncated_tail);
    }

    // -----------------------------------------------------------------------
    // End-to-end: the three components must actually fit together
    // -----------------------------------------------------------------------

    /// THE integration proof. Before framing, these three could not compose:
    /// `FileSyncWriter` appended unframed bytes, `AccordProtocolEntry` needed an
    /// exact boundary, and `CrashRecoveryReplay` wanted pre-split records — so
    /// the log was durable but unreadable and nothing replayed it.
    ///
    /// This drives the REAL production writer, reads its file back, and replays
    /// it, asserting the transaction state survives a simulated restart.
    #[test]
    fn writer_output_replays_into_recovered_transaction_state() {
        use crate::accord::crash_recovery::{CrashRecoveryReplay, ReplayedPhase};
        use crate::accord::entries::{AccordProtocolEntry, Timestamp, TxnId};
        use crate::accord::sync_writer::{FileSyncWriter, SyncWriter};

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("protocol.log");
        let writer = FileSyncWriter::new(path.clone());

        let ts = |m: u64| Timestamp {
            epoch_micros: m,
            logical: 0,
        };
        let txn = TxnId {
            node: 7,
            timestamp: ts(100),
        };

        // Two protocol events for one transaction, exactly as production writes
        // them: serialize, then write_and_sync.
        let pre = AccordProtocolEntry::PreAccepted {
            txn_id: txn,
            t0: ts(100),
            t: ts(100),
            deps: vec![],
        };
        let com = AccordProtocolEntry::Committed {
            txn_id: txn,
            t: ts(140),
            deps: vec![],
        };
        assert!(writer.write_and_sync(&pre.serialize()).is_ok());
        assert!(writer.write_and_sync(&com.serialize()).is_ok());

        // Simulated restart: read the file back and replay it.
        let read = read_framed_log(&path).expect("framed log reads back");
        assert!(
            !read.truncated_tail,
            "a cleanly-closed log must not look torn"
        );
        assert_eq!(read.records.len(), 2, "both records recovered");

        let mut replay = CrashRecoveryReplay::new();
        replay.replay(&read.records);

        assert_eq!(
            replay.skipped_count(),
            0,
            "framing must not corrupt records: {:?}",
            read.records
        );
        let state = replay
            .txn_states()
            .get(&txn)
            .expect("the transaction survived the restart");
        assert_eq!(
            state.phase,
            ReplayedPhase::Committed,
            "the COMMIT decision must survive — this is the state whose loss \
             would force a blind abort of its intents"
        );
    }
}
