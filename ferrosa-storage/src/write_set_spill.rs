//! Local temp/staging for a large transaction's write-set payloads.
//!
//! A PostgreSQL cluster commit hands its whole write-set to ONE multi-key Accord
//! transaction. The encoded mutation payloads travel only in the Apply phase —
//! PreAccept/Accept/Commit carry keys, not payloads — yet the coordinator holds
//! every payload resident from the moment the write-set is built until Apply
//! finishes. That is what makes a transactional bulk load (`pgbench -i`:
//! `TRUNCATE` + `COPY` + `COMMIT`, ~1.1M rows in ONE transaction) OOM a 4 GB node:
//! the coordinator materializes the whole write-set and then copies it again per
//! replica.
//!
//! [`WriteSetSpill`] stages those payloads in a local temp file and hands the bytes
//! back by index on demand, so the coordinator's resident write-set is the KEYS —
//! which Accord's conflict ordering and the per-shard participant set genuinely need
//! — plus a small offset table, never the payloads. The payloads move to disk; the
//! keys do not.
//!
//! # Reusing the spill machinery
//!
//! Cleanup rides on [`TempSortTableReservation`] (the same guard the ORDER BY and
//! `DISTINCT` spills use): dropping the spill removes the staging directory, so a
//! failed or abandoned commit cleans up exactly like a successful one. The staging
//! policy borrows [`crate::spill_budget`]'s budget detection, but gates on an
//! explicit absolute floor — [`WRITE_SET_SPILL_FLOOR_BYTES`] — rather than the ORDER
//! BY threshold, because that threshold is a fraction of the *process* budget and so
//! is the wrong scale for bounding one transaction's write-set.
//!
//! Below the floor the write-set stays resident: a disk round-trip costs more than
//! the memory it saves, and small commitments (LWTs, single-row DML) must not touch
//! the filesystem at all.

use std::fs::File;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Mutex;

use ferrosa_common::{Error, Result};

use crate::engine::TempSortTableReservation;

/// Payload bytes at or above which the write-set payloads are staged on disk.
///
/// Not [`crate::spill_budget::process_spill_threshold_bytes`]: that is a fraction of
/// the process memory budget (default 50%), which on a 4 GB node is ~2 GB — far above
/// the write-set a single transaction materializes and far above what a 4 GB node can
/// spare alongside the storage engine. This is an absolute cap on one transaction's
/// resident payload set.
pub const WRITE_SET_SPILL_FLOOR_BYTES: u64 = 8 * 1024 * 1024;

/// A transaction's write-set payloads staged in a local temp file.
///
/// Addressable by index — the position of the entry in the transaction's write-set —
/// so the Apply phase can read each payload back exactly where Accord needs it
/// (per-peer fan-out, the coordinator's own apply) without the whole set resident.
pub struct WriteSetSpill {
    /// Owns the staging directory; dropping the spill removes it.
    _reservation: TempSortTableReservation,
    path: PathBuf,
    /// Byte offset of each staged payload, by write-set index.
    offsets: Vec<u64>,
    /// Payload length in bytes, by write-set index.
    lens: Vec<u32>,
    /// Reopened lazily for reads (the writer is closed once staging completes).
    file: Mutex<File>,
    /// Total payload bytes staged (observability + tests).
    bytes: u64,
}

impl std::fmt::Debug for WriteSetSpill {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteSetSpill")
            .field("entries", &self.offsets.len())
            .field("bytes", &self.bytes)
            .finish_non_exhaustive()
    }
}

impl WriteSetSpill {
    /// Whether a write-set of `total_bytes` payload should be staged on disk.
    pub fn should_stage(total_bytes: u64) -> bool {
        total_bytes >= WRITE_SET_SPILL_FLOOR_BYTES
    }

    /// Stage `blobs` under `reservation`'s directory, draining each entry as it is
    /// written so the in-memory copy is freed as we go.
    ///
    /// On success every `blobs[i]` is left EMPTY: the bytes now live on disk and are
    /// read back through [`Self::mutation`]. A write error leaves the reservation
    /// intact (the caller's `blobs` are then partially drained, which is why a staging
    /// failure is only ever surfaced as a failed commit, never retried in place).
    pub fn stage(reservation: TempSortTableReservation, blobs: &mut [Vec<u8>]) -> Result<Self> {
        let path = reservation.path().join("write-set.bin");
        let mut offsets: Vec<u64> = Vec::with_capacity(blobs.len());
        let mut lens: Vec<u32> = Vec::with_capacity(blobs.len());
        let mut bytes = 0u64;
        {
            let file = File::create(&path).map_err(|e| {
                Error::InvalidFormat(format!("write-set spill: create {}: {e}", path.display()))
            })?;
            let mut writer = BufWriter::new(file);
            for blob in blobs.iter_mut() {
                let payload = std::mem::take(blob);
                let len = u32::try_from(payload.len()).map_err(|_| {
                    Error::InvalidFormat(format!(
                        "write-set spill: payload of {} bytes exceeds the u32 staging bound",
                        payload.len()
                    ))
                })?;
                offsets.push(bytes);
                lens.push(len);
                writer.write_all(&payload).map_err(|e| {
                    Error::InvalidFormat(format!("write-set spill: write {}: {e}", path.display()))
                })?;
                bytes += u64::from(len);
                // `payload` dropped here: the resident copy is freed as we go, so
                // staging peak extra residency is one payload, not the whole set.
            }
            writer.flush().map_err(|e| {
                Error::InvalidFormat(format!("write-set spill: flush {}: {e}", path.display()))
            })?;
        }
        let file = File::open(&path).map_err(|e| {
            Error::InvalidFormat(format!("write-set spill: reopen {}: {e}", path.display()))
        })?;
        Ok(Self {
            _reservation: reservation,
            path,
            offsets,
            lens,
            file: Mutex::new(file),
            bytes,
        })
    }

    /// Number of staged write-set entries.
    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    /// Whether no payload was staged.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Total payload bytes staged on disk.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Bytes of the resident index (offset + length tables) — the only part of a
    /// staged write-set that stays in memory.
    pub fn resident_index_bytes(&self) -> u64 {
        (self.offsets.capacity() * std::mem::size_of::<u64>()
            + self.lens.capacity() * std::mem::size_of::<u32>()) as u64
    }

    /// Read back the payload staged for write-set entry `index`.
    ///
    /// FAILS LOUD on an index that was never staged: a missing payload must never
    /// become an empty mutation that the applier silently drops.
    pub fn mutation(&self, index: usize) -> Result<Vec<u8>> {
        let (&offset, &len) = self
            .offsets
            .get(index)
            .zip(self.lens.get(index))
            .ok_or_else(|| {
                Error::InvalidData(format!(
                    "write-set spill: index {index} is out of range ({} entries staged)",
                    self.offsets.len()
                ))
            })?;
        let mut buf = vec![0u8; len as usize];
        let mut file = self
            .file
            .lock()
            .map_err(|_| Error::InvalidData("write-set spill: read lock poisoned".to_string()))?;
        file.seek(SeekFrom::Start(offset)).map_err(|e| {
            Error::InvalidData(format!(
                "write-set spill: seek {}: {e}",
                self.path.display()
            ))
        })?;
        file.read_exact(&mut buf).map_err(|e| {
            Error::InvalidData(format!(
                "write-set spill: read {} bytes at {offset} from {}: {e}",
                buf.len(),
                self.path.display()
            ))
        })?;
        Ok(buf)
    }
}

/// Create a staging directory under the process temp root and return the reservation
/// that owns it.
///
/// Kept here (not on the engine) because the coordinator-side commit has no engine
/// handle: the write-set spill is a property of the commit, not of a table.
pub fn reserve_write_set_stage() -> Result<TempSortTableReservation> {
    let root = std::env::temp_dir().join("ferrosa-write-set-spill");
    std::fs::create_dir_all(&root).map_err(|e| {
        Error::InvalidFormat(format!(
            "write-set spill: create staging root {}: {e}",
            root.display()
        ))
    })?;
    let dir = root.join(format!("stage-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&dir).map_err(|e| {
        Error::InvalidFormat(format!(
            "write-set spill: create staging dir {}: {e}",
            dir.display()
        ))
    })?;
    Ok(TempSortTableReservation::claim_dir(dir))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stage(blobs: &mut [Vec<u8>]) -> WriteSetSpill {
        let reservation = reserve_write_set_stage().expect("stage dir");
        WriteSetSpill::stage(reservation, blobs).expect("stage blobs")
    }

    #[test]
    fn staged_payloads_read_back_byte_for_byte() {
        let mut blobs = vec![
            b"first".to_vec(),
            Vec::new(),
            b"a longer third payload".to_vec(),
        ];
        let spill = stage(&mut blobs);
        assert_eq!(spill.len(), 3);
        assert_eq!(
            spill.bytes(),
            5 + 22,
            "the empty payload contributes no bytes"
        );
        assert_eq!(spill.mutation(0).unwrap(), b"first");
        // An EMPTY payload round-trips as empty, never as "missing".
        assert_eq!(spill.mutation(1).unwrap(), b"");
        assert_eq!(spill.mutation(2).unwrap(), b"a longer third payload");
        assert!(
            spill.resident_index_bytes() < 128,
            "only the offset/length index stays resident"
        );
    }

    #[test]
    fn staging_drains_the_resident_copies() {
        let mut blobs = vec![vec![7u8; 4096], vec![9u8; 4096]];
        let spill = stage(&mut blobs);
        assert!(
            blobs.iter().all(|b| b.is_empty()),
            "staging must free the resident payloads as it writes them"
        );
        assert_eq!(spill.mutation(0).unwrap(), vec![7u8; 4096]);
        assert_eq!(spill.mutation(1).unwrap(), vec![9u8; 4096]);
    }

    #[test]
    fn an_out_of_range_index_fails_loud() {
        let mut blobs = vec![b"only".to_vec()];
        let spill = stage(&mut blobs);
        let error = spill.mutation(1).expect_err("index 1 was never staged");
        let message = error.to_string();
        assert!(
            message.contains("out of range"),
            "an un-staged payload must fail loud, never read as empty: {message}"
        );
    }

    #[test]
    fn dropping_the_spill_removes_the_staging_directory() {
        let mut blobs = vec![b"payload".to_vec()];
        let spill = stage(&mut blobs);
        let dir = spill._reservation.path().to_path_buf();
        assert!(dir.exists());
        drop(spill);
        assert!(
            !dir.exists(),
            "dropping the spill must remove its staging directory"
        );
    }

    #[test]
    fn the_staging_floor_keeps_small_write_sets_resident() {
        assert!(!WriteSetSpill::should_stage(0));
        assert!(!WriteSetSpill::should_stage(
            WRITE_SET_SPILL_FLOOR_BYTES - 1
        ));
        assert!(WriteSetSpill::should_stage(WRITE_SET_SPILL_FLOOR_BYTES));
    }
}
