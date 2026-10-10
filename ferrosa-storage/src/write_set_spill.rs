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
//! # The staged region is MMAPPED
//!
//! The staging file is memory-mapped read-only once written. Reading an entry is a
//! slice of that mapping ([`WriteSetSpill::entry`]) — no `seek`, no `read_exact`, no
//! per-read lock, no syscall at all. The pre-mmap path paid one `lseek` + one `read`
//! (and a `Mutex` acquisition) PER ENTRY: at N = 1 100 000 the coordinator's Apply
//! fan-out resolved every write-set entry through that path, i.e. ~1.1M syscalls
//! serialized behind one mutex, which the `FERROSA_PG_COMMIT_PROFILE` fan-out line
//! priced as the bulk of `serialize_ms`. A slice of the mapping removes that cost
//! entirely, and lets the mapped region be handed to the wire instead of being read
//! into an intermediate buffer per peer.
//!
//! [`WriteSetSpill::entry`] is the genuinely zero-copy accessor: it borrows the
//! mapping. The region-REFERENCE Apply wire
//! (`ferrosa_net::protocol::encode_accord_apply_v2_region`) consumes exactly these
//! borrowed slices and writes them into ONE contiguous region on the frame — no
//! per-entry capnp struct and no per-entry owned copy on the coordinator. The
//! staging file plus the read-only mapping are owned behind the `Arc<WriteSetSpill>`
//! the coordinator driver holds and are released on the LAST drop of that `Arc`
//! ([`WriteSetSpill`] declares `map` before its temp-dir reservation, so the region is
//! unmapped before the directory is removed). [`WriteSetSpill::mutation`] — the owned
//! twin that `WriteSetEntry::mutation` (`Vec<u8>`) still needs on the legacy inline
//! path — remains, but the hot bulk-load path no longer goes through it.
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
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use ferrosa_common::{Error, Result};

use crate::engine::TempSortTableReservation;

/// Payload bytes at or above which the write-set payloads are staged on disk.
///
/// This is the DEFAULT resident buffer size for a write-set spill. It is a
/// streaming BUFFER SIZE, not a cap: a write-set larger than it is staged, never
/// refused. Override it at runtime with
/// [`crate::write_set_stage::WRITE_SET_SPILL_THRESHOLD_ENV`]
/// (`FERROSA_WRITE_SET_SPILL_THRESHOLD_BYTES`).
///
/// Not [`crate::spill_budget::process_spill_threshold_bytes`]: that is a fraction of
/// the process memory budget (default 50%), which on a 4 GB node is ~2 GB — far above
/// the write-set a single transaction materializes and far above what a 4 GB node can
/// spare alongside the storage engine. This is an absolute cap on one transaction's
/// resident payload set.
pub const WRITE_SET_SPILL_FLOOR_BYTES: u64 = 8 * 1024 * 1024;

/// A transaction's write-set payloads staged in a local temp file and mapped.
///
/// Addressable by index — the position of the entry in the transaction's write-set —
/// so the Apply phase can read each payload back exactly where Accord needs it
/// (per-peer fan-out, the coordinator's own apply) without the whole set resident.
pub struct WriteSetSpill {
    /// The staged payload region, memory-mapped read-only.
    ///
    /// Declared FIRST so it is unmapped before `_reservation` removes the staging
    /// directory: on a platform where removing a file that is still mapped fails, the
    /// map is already gone by the time the directory is unlinked. `None` only when the
    /// staging file is zero bytes, which `mmap` refuses; every entry is then empty by
    /// construction.
    map: Option<memmap2::Mmap>,
    /// Owns the staging directory; dropping the spill removes it.
    _reservation: TempSortTableReservation,
    path: PathBuf,
    /// Byte offset of each staged payload, by write-set index.
    offsets: Vec<u64>,
    /// Payload length in bytes, by write-set index.
    lens: Vec<u32>,
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
    ///
    /// The threshold is the runtime-tunable
    /// [`crate::write_set_stage::WRITE_SET_SPILL_THRESHOLD_ENV`], defaulting to
    /// [`WRITE_SET_SPILL_FLOOR_BYTES`]. This is a streaming BUFFER SIZE, never a
    /// cap: a larger write-set is staged, not refused.
    pub fn should_stage(total_bytes: u64) -> bool {
        let threshold = crate::write_set_stage::resolve_spill_threshold(
            std::env::var(crate::write_set_stage::WRITE_SET_SPILL_THRESHOLD_ENV)
                .ok()
                .as_deref(),
        );
        total_bytes >= threshold
    }

    /// Stage `blobs` under `reservation`'s directory, draining each entry as it is
    /// written so the in-memory copy is freed as we go, then memory-map the region.
    ///
    /// On success every `blobs[i]` is left EMPTY: the bytes now live on disk, mapped
    /// read-only, and are read back through [`Self::entry`]. A write error leaves the
    /// reservation intact (the caller's `blobs` are then partially drained, which is
    /// why a staging failure is only ever surfaced as a failed commit, never retried
    /// in place).
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
        let map = if bytes == 0 {
            // `mmap` refuses a zero-length mapping; with no payload bytes every
            // entry is empty, which `entry` returns without touching the map.
            None
        } else {
            // SAFETY: the staging file is private to this spill — it lives in a
            // fresh directory created by `reserve_write_set_stage`, the writer is
            // closed above, and no writable handle to it survives. Nothing can
            // mutate the bytes under the mapping.
            Some(unsafe { memmap2::Mmap::map(&file) }.map_err(|e| {
                Error::InvalidFormat(format!("write-set spill: mmap {}: {e}", path.display()))
            })?)
        };
        // The file handle is not needed once the region is mapped; the mapping owns
        // the pages.
        drop(file);
        Ok(Self {
            map,
            _reservation: reservation,
            path,
            offsets,
            lens,
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

    /// Borrow the payload staged for write-set entry `index` — a slice of the
    /// staging region's memory map, not a fresh allocation and not a syscall.
    ///
    /// FAILS LOUD on an index that was never staged, and on a staged range that
    /// would escape the mapping (a corrupt offset table must never read past the
    /// mapped region).
    pub fn entry(&self, index: usize) -> Result<&[u8]> {
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
        let start = usize::try_from(offset).map_err(|_| {
            Error::InvalidData(format!(
                "write-set spill: staged offset {offset} does not fit in usize"
            ))
        })?;
        let end = start.checked_add(len as usize).ok_or_else(|| {
            Error::InvalidData(format!(
                "write-set spill: staged range {start}..(start+{len}) overflows usize"
            ))
        })?;
        match &self.map {
            Some(map) => map.get(start..end).ok_or_else(|| {
                Error::InvalidData(format!(
                    "write-set spill: staged range {start}..{end} escapes the {} byte mapping of {}",
                    map.len(),
                    self.path.display()
                ))
            }),
            // A zero-byte staging file: no mapping, and every entry is empty.
            None if len == 0 => Ok(&[]),
            None => Err(Error::InvalidData(format!(
                "write-set spill: entry {index} of {} claims {len} bytes but nothing was staged",
                self.path.display()
            ))),
        }
    }

    /// Read back the payload staged for write-set entry `index` as an owned copy.
    ///
    /// FAILS LOUD on an index that was never staged: a missing payload must never
    /// become an empty mutation that the applier silently drops. This is the owned
    /// twin of [`Self::entry`]; callers that can borrow should use `entry`.
    pub fn mutation(&self, index: usize) -> Result<Vec<u8>> {
        Ok(self.entry(index)?.to_vec())
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

    /// The mapped accessor and the owned accessor must agree byte for byte with what
    /// was staged, across every shape the write-set ever has: a small payload, an
    /// EMPTY payload (a key with no mutation), a payload with embedded NUL bytes, and
    /// a multi-KiB payload that crosses a page boundary.
    #[test]
    fn mmap_entry_reads_identical_bytes_to_the_owned_mutation_path() {
        let originals: Vec<Vec<u8>> = vec![
            b"first".to_vec(),
            Vec::new(),
            b"a\x00longer\x00third payload".to_vec(),
            vec![0xABu8; 5000],
        ];
        let mut blobs = originals.clone();
        let spill = stage(&mut blobs);
        for (index, expected) in originals.iter().enumerate() {
            assert_eq!(
                spill.entry(index).unwrap(),
                expected.as_slice(),
                "the mapped entry {index} must be byte-identical to what was staged"
            );
            assert_eq!(
                spill.mutation(index).unwrap(),
                *expected,
                "the owned read must agree with the mapped one for entry {index}"
            );
        }
        // The un-staged index fails loud on the mapped accessor too.
        let error = spill
            .entry(originals.len())
            .expect_err("index past the end was never staged");
        assert!(
            error.to_string().contains("out of range"),
            "a mapped out-of-range read must fail loud: {error}"
        );
    }

    /// The genuinely zero-copy property: reading an entry returns a STABLE slice of
    /// the mapping — the same address every time, because the bytes were never read
    /// into a per-call buffer. The negative control is the owned accessor, whose
    /// address is free to differ between calls precisely because it allocates.
    #[test]
    fn mmap_entry_is_a_stable_slice_of_the_mapping() {
        let mut blobs = vec![vec![7u8; 4096], vec![9u8; 4096]];
        let spill = stage(&mut blobs);

        let first = spill.entry(0).unwrap();
        let second = spill.entry(0).unwrap();
        assert_eq!(first, second);
        assert_eq!(
            first.as_ptr(),
            second.as_ptr(),
            "an entry must be a slice of the mapping, not a fresh allocation per read"
        );

        // Negative control: the owned twin copies, so it must NOT alias the mapping.
        let owned = spill.mutation(0).unwrap();
        assert_eq!(owned, vec![7u8; 4096]);
        assert_ne!(
            owned.as_ptr(),
            first.as_ptr(),
            "the owned read must be a copy, never the mapping's own storage"
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

    /// The mapped region is owned behind an `Arc`; consumers borrow slices of it and the
    /// FILE plus the MAPPING are released exactly once, on the LAST drop of that `Arc` —
    /// never copied out and never leaked. A borrow of the mapping stays valid while any
    /// `Arc` clone is alive, and the staging directory (and hence the region) is removed
    /// only when the last clone goes.
    #[test]
    fn the_mapped_region_is_released_only_on_the_last_arc_drop() {
        let mut blobs = vec![vec![0x5Au8; 4096]];
        let spill = std::sync::Arc::new(stage(&mut blobs));
        let dir = spill._reservation.path().to_path_buf();
        let first = std::sync::Arc::clone(&spill);
        let last = std::sync::Arc::clone(&spill);
        assert_eq!(std::sync::Arc::strong_count(&spill), 3);

        drop(spill);
        drop(first);
        assert!(
            dir.exists(),
            "the staging dir must survive while a borrow of the mapping is alive"
        );
        assert_eq!(
            last.entry(0).unwrap(),
            vec![0x5Au8; 4096].as_slice(),
            "a live Arc clone still borrows the mapping"
        );

        drop(last);
        assert!(
            !dir.exists(),
            "the file and mapping are released on the LAST Arc drop"
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
