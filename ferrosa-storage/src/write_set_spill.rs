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
    /// Owns the staging directory; dropping the spill removes it. `None` only for a
    /// spill built entirely within the resident buffer ([`WriteSetSpillBuilder`]),
    /// where nothing ever reached the filesystem.
    _reservation: Option<TempSortTableReservation>,
    path: PathBuf,
    /// The resident prefix: entries appended while the buffer was under the threshold
    /// and never drained to disk. Built by [`WriteSetSpillBuilder`]; EMPTY for a spill
    /// assembled by [`WriteSetSpill::stage`], whose entries are all spilled.
    ///
    /// Declared before the spilled index so a spill with a resident prefix reads entry
    /// `i` from `resident[i]` for `i < resident.len()`, else from the spilled region at
    /// `i - resident.len()`.
    resident: Vec<Vec<u8>>,
    /// Sum of [`Self::resident`] lengths. Bounded by the staging threshold.
    resident_bytes: u64,
    /// Byte offset of each SPILLED payload, in append order (after the resident prefix).
    offsets: Vec<u64>,
    /// Payload length in bytes, by spilled index.
    lens: Vec<u32>,
    /// Total payload bytes staged ON DISK (observability + tests). The resident prefix
    /// is reported separately via [`Self::resident_bytes`]; `bytes()` is the disk figure
    /// the spill exists to bound.
    bytes: u64,
}

impl std::fmt::Debug for WriteSetSpill {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteSetSpill")
            .field("entries", &self.len())
            .field("resident", &self.resident.len())
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
            // `flush` only drains `BufWriter`'s buffer into the kernel (`File::flush`
            // is a no-op), so the payload is still DIRTY in the page cache. Write it
            // back now, before the mapping exists, so the mapped pages are CLEAN and
            // reclaimable — a dirty mapped file-backed page cannot be reclaimed until
            // writeback, which is what drives the OOM. See `crate::staged_pages`.
            crate::staged_pages::flush_staged_pages(writer.get_ref()).map_err(|e| {
                Error::InvalidFormat(format!("write-set spill: sync {}: {e}", path.display()))
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
            _reservation: Some(reservation),
            path,
            resident: Vec::new(),
            resident_bytes: 0,
            offsets,
            lens,
            bytes,
        })
    }

    /// Number of staged write-set entries (resident prefix + spilled).
    pub fn len(&self) -> usize {
        self.resident.len() + self.offsets.len()
    }

    /// Whether no payload was staged.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Total payload bytes staged on disk. See [`WriteSetSpillBuilder::resident_bytes`]
    /// for the in-memory prefix of a builder-assembled spill.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Whether any payload bytes reached the staging file.
    ///
    /// True when this spill owns a staging directory — i.e. its payloads did not all fit the
    /// resident buffer and were drained to disk. False only for a spill built entirely within
    /// the resident buffer ([`WriteSetSpillBuilder`]), where nothing ever touched the
    /// filesystem. This is the finished-spill twin of
    /// [`WriteSetSpillBuilder::spilled_to_disk`]: where the builder reports "the writer was
    /// opened", this reports "the staging directory exists", which is the same fact one step
    /// later. A degenerate all-empty spill still created the file and so reports `true`; there
    /// is simply no payload bulk to be resident.
    #[must_use]
    pub fn spilled_to_disk(&self) -> bool {
        self._reservation.is_some()
    }

    /// Payload bytes still held in the resident prefix. Zero for a spill assembled by
    /// [`Self::stage`] (all entries spilled); bounded by the staging threshold for one
    /// assembled by [`WriteSetSpillBuilder`].
    pub fn resident_bytes(&self) -> u64 {
        self.resident_bytes
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
        // The resident prefix: entries that never reached the filesystem. Read in place.
        if let Some(resident) = self.resident.get(index) {
            return Ok(resident);
        }
        // Past the prefix, entry `index` is SPILLED entry `index - resident.len()`.
        let spilled = index - self.resident.len();
        let (&offset, &len) = self
            .offsets
            .get(spilled)
            .zip(self.lens.get(spilled))
            .ok_or_else(|| {
                Error::InvalidData(format!(
                    "write-set spill: index {index} is out of range ({} entries staged)",
                    self.len()
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

/// Build a [`WriteSetSpill`] **one payload at a time**, spilling past a bounded resident
/// buffer — the STREAMING twin of [`WriteSetSpill::stage`].
///
/// [`WriteSetSpill::stage`] takes an already-assembled `&mut [Vec<u8>]`, so it can only
/// bound the LIFETIME of the payload bulk, never its RESIDENCE: the caller held the
/// whole write-set to hand it over. This builder removes that prerequisite. [`append`]
/// accepts one encoded payload as it is produced, keeps a bounded resident prefix, and
/// drains to disk past the threshold, so a caller driving a decoder can stream a
/// write-set of any size into a spill without ever holding it.
///
/// ## The buffer is a BUFFER, never a cap
///
/// The resident limit is the runtime-tunable
/// [`crate::write_set_stage::WRITE_SET_SPILL_THRESHOLD_ENV`]
/// (`FERROSA_WRITE_SET_SPILL_THRESHOLD_BYTES`, default [`WRITE_SET_SPILL_FLOOR_BYTES`]).
/// A write-set larger than it SPILLS; it is never refused or truncated. Below the
/// threshold nothing touches the filesystem, so a small commitment (an LWT, single-row
/// DML) pays nothing — the staging directory is created lazily on the first spill.
///
/// [`append`]: WriteSetSpillBuilder::append
pub struct WriteSetSpillBuilder {
    /// Resident buffer size before the builder spills. A BUFFER SIZE, not a cap.
    threshold_bytes: u64,
    /// Payloads held in memory while under the threshold. Drained in order on the first
    /// spill; never re-filled after that, so `resident.len()` freezes once spilling.
    resident: Vec<Vec<u8>>,
    /// Sum of `resident` lengths.
    resident_bytes: u64,
    /// Owns the staging directory; `None` until the first spill.
    reservation: Option<TempSortTableReservation>,
    /// The staging file path; set with the reservation on the first spill.
    path: PathBuf,
    /// The spill writer; `None` while fully resident.
    writer: Option<BufWriter<File>>,
    /// Byte offset of each SPILLED payload, in append order.
    offsets: Vec<u64>,
    /// Length in bytes of each SPILLED payload.
    lens: Vec<u32>,
    /// Total payload bytes written to disk.
    bytes: u64,
}

impl std::fmt::Debug for WriteSetSpillBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteSetSpillBuilder")
            .field("entries", &self.len())
            .field("resident", &self.resident.len())
            .field("resident_bytes", &self.resident_bytes)
            .field("spilled_to_disk", &self.spilled_to_disk())
            .finish_non_exhaustive()
    }
}

impl Default for WriteSetSpillBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl WriteSetSpillBuilder {
    /// A builder keeping `threshold_bytes` resident before spilling. A zero threshold is
    /// clamped to one byte (spill on the first payload) rather than refused.
    #[must_use]
    pub fn with_threshold(threshold_bytes: u64) -> Self {
        Self {
            threshold_bytes: threshold_bytes.max(1),
            resident: Vec::new(),
            resident_bytes: 0,
            reservation: None,
            path: PathBuf::new(),
            writer: None,
            offsets: Vec::new(),
            lens: Vec::new(),
            bytes: 0,
        }
    }

    /// A builder using the process-wide
    /// [`crate::write_set_stage::WRITE_SET_SPILL_THRESHOLD_ENV`] setting, read once here.
    #[must_use]
    pub fn new() -> Self {
        let threshold = crate::write_set_stage::resolve_spill_threshold(
            std::env::var(crate::write_set_stage::WRITE_SET_SPILL_THRESHOLD_ENV)
                .ok()
                .as_deref(),
        );
        Self::with_threshold(threshold)
    }

    /// Number of payloads appended so far (resident + spilled).
    #[must_use]
    pub fn len(&self) -> usize {
        self.resident.len() + self.offsets.len()
    }

    /// Whether nothing has been appended.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Payload bytes still held in memory. Never exceeds the threshold once spilling.
    #[must_use]
    pub fn resident_bytes(&self) -> u64 {
        self.resident_bytes
    }

    /// Whether anything has reached the disk yet.
    #[must_use]
    pub fn spilled_to_disk(&self) -> bool {
        self.writer.is_some()
    }

    /// Append one encoded payload, spilling the resident prefix if this append would push
    /// the resident buffer past the threshold.
    ///
    /// A write error is surfaced, never swallowed: a builder that silently dropped a
    /// payload would stage a write-set missing rows.
    ///
    /// # Errors
    ///
    /// A payload longer than `u32::MAX`, or a staging I/O failure.
    pub fn append(&mut self, payload: &[u8]) -> Result<()> {
        let len = u32::try_from(payload.len()).map_err(|_| {
            Error::InvalidFormat(format!(
                "write-set spill: payload of {} bytes exceeds the u32 staging bound",
                payload.len()
            ))
        })?;
        if self.writer.is_none() {
            // Stay resident while the whole prefix still fits the buffer. The comparison
            // is on `<=` so a payload landing EXACTLY on the threshold stays resident.
            if self.resident_bytes + u64::from(len) <= self.threshold_bytes {
                self.resident.push(payload.to_vec());
                self.resident_bytes += u64::from(len);
                return Ok(());
            }
            self.begin_spill()?;
        }
        self.write_spilled(payload, len)
    }

    /// Create the staging file and drain the resident prefix into it, in order.
    fn begin_spill(&mut self) -> Result<()> {
        let reservation = reserve_write_set_stage()?;
        let path = reservation.path().join("write-set.bin");
        let file = File::create(&path).map_err(|e| {
            Error::InvalidFormat(format!("write-set spill: create {}: {e}", path.display()))
        })?;
        let mut writer = BufWriter::new(file);
        // Drain the resident prefix in order. `std::mem::take` frees each payload's memory
        // as it is written, so the drain's extra residency is one payload, not the prefix.
        for blob in self.resident.drain(..) {
            let len = u32::try_from(blob.len()).unwrap_or(u32::MAX);
            self.offsets.push(self.bytes);
            self.lens.push(len);
            writer.write_all(&blob).map_err(|e| {
                Error::InvalidFormat(format!("write-set spill: write {}: {e}", path.display()))
            })?;
            self.bytes += u64::from(len);
        }
        self.resident_bytes = 0;
        self.path = path;
        self.writer = Some(writer);
        self.reservation = Some(reservation);
        Ok(())
    }

    fn write_spilled(&mut self, payload: &[u8], len: u32) -> Result<()> {
        let writer = self
            .writer
            .as_mut()
            .expect("write_spilled is only called once the builder is spilling");
        self.offsets.push(self.bytes);
        self.lens.push(len);
        writer
            .write_all(payload)
            .map_err(|e| Error::InvalidFormat(format!("write-set spill: write payload: {e}")))?;
        self.bytes += u64::from(len);
        Ok(())
    }

    /// Flush, memory-map the spilled region, and return the finished [`WriteSetSpill`].
    ///
    /// A builder that never spilled returns a spill with no mapping and no staging
    /// directory: every entry is read from the resident prefix.
    ///
    /// # Errors
    ///
    /// A flush, reopen, or mmap failure is surfaced.
    pub fn finish(mut self) -> Result<WriteSetSpill> {
        let (map, reservation) = match self.writer.take() {
            Some(mut writer) => {
                writer
                    .flush()
                    .map_err(|e| Error::InvalidFormat(format!("write-set spill: flush: {e}")))?;
                // Drop the writer so the file is closed before mapping.
                drop(writer);
                let reservation = self
                    .reservation
                    .take()
                    .expect("a spilling builder always owns its reservation");
                let file = File::open(&self.path).map_err(|e| {
                    Error::InvalidFormat(format!(
                        "write-set spill: reopen {}: {e}",
                        self.path.display()
                    ))
                })?;
                let map = if self.bytes == 0 {
                    None
                } else {
                    // SAFETY: the staging file is private to this builder — it lives in a
                    // fresh directory created by `reserve_write_set_stage`, the writer is
                    // closed above, and no writable handle survives.
                    Some(unsafe { memmap2::Mmap::map(&file) }.map_err(|e| {
                        Error::InvalidFormat(format!(
                            "write-set spill: mmap {}: {e}",
                            self.path.display()
                        ))
                    })?)
                };
                drop(file);
                (map, Some(reservation))
            }
            None => (None, None),
        };
        Ok(WriteSetSpill {
            map,
            _reservation: reservation,
            path: self.path,
            resident: self.resident,
            resident_bytes: self.resident_bytes,
            offsets: self.offsets,
            lens: self.lens,
            bytes: self.bytes,
        })
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
        let dir = spill
            ._reservation
            .as_ref()
            .expect("a staged spill owns its reservation")
            .path()
            .to_path_buf();
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
        let dir = spill
            ._reservation
            .as_ref()
            .expect("a staged spill owns its reservation")
            .path()
            .to_path_buf();
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

    /// INVARIANT (the owner's): after the staging file is written and mapped, its
    /// MAPPED pages are CLEAN — the kernel's own dirty, file-backed count for the
    /// mapping is ZERO — so they can be reclaimed under pressure. A dirty mapped
    /// file-backed page cannot be evicted until writeback, so on a memory-limited
    /// node the staged write-set is unreleasable and drives the OOM. Measured with
    /// `/proc/self/smaps`, the kernel's accounting, not our instrumentation.
    ///
    /// The negative control (an unsynced file mapped the same way) proves the
    /// assertion detects the defect and is not vacuously true.
    #[cfg(target_os = "linux")]
    #[test]
    fn the_mapped_staging_pages_are_clean_and_reclaimable() {
        use crate::staged_pages::probe::{
            mapping_private_dirty_bytes, staging_fs_has_writeback, touch_every_page,
        };

        // An EXACT stated N: 8 MiB in a single payload — over the staging floor,
        // and a whole number of 4 KiB pages.
        const N: usize = 8 * 1024 * 1024;

        let reservation = reserve_write_set_stage().expect("stage dir");
        let dir = reservation.path().to_path_buf();
        if !staging_fs_has_writeback(&dir) {
            eprintln!(
                "skipping: {} is on a filesystem with no writeback (tmpfs/ramfs)",
                dir.display()
            );
            return;
        }

        let mut blobs = vec![vec![0xABu8; N]];
        let spill = WriteSetSpill::stage(reservation, &mut blobs).expect("stage blobs");
        let staged_path = spill._reservation.path().join("write-set.bin");

        // Fault every mapped page so the kernel has resident pages to account.
        let touched = touch_every_page(spill.entry(0).expect("entry"));
        assert!(
            touched > 0,
            "the payload must read back through the mapping"
        );

        let dirty = mapping_private_dirty_bytes(&staged_path)
            .expect("the staging file must be mapped into this process");
        assert_eq!(
            dirty, 0,
            "the mapped staging pages must be CLEAN: {dirty} bytes are DIRTY, and a \
             dirty mapped file-backed page cannot be reclaimed until writeback"
        );

        // NEGATIVE CONTROL: the same measurement, on a file written but NOT synced
        // before it is mapped, must be NON-ZERO.
        let control_path = dir.join("control.bin");
        {
            let mut writer = BufWriter::new(File::create(&control_path).expect("control create"));
            writer.write_all(&vec![0xCDu8; N]).expect("control write");
            writer.flush().expect("control flush");
        }
        let control_file = File::open(&control_path).expect("control reopen");
        // SAFETY: `control_path` is private to this test, closed for writing, and
        // never mutated under the mapping.
        let control_map = unsafe { memmap2::Mmap::map(&control_file) }.expect("control map");
        let control_touched = touch_every_page(&control_map[..]);
        assert!(control_touched > 0);
        let control_dirty =
            mapping_private_dirty_bytes(&control_path).expect("control must be mapped");
        assert!(
            control_dirty > 0,
            "negative control: an unsynced mapping must show DIRTY pages, or this \
             assertion cannot detect the defect"
        );
    }

    // --- WriteSetSpillBuilder: the STREAMING twin of `stage` ---------------------------------
    //
    // These lock the property the builder exists for: a producer can stream a write-set of ANY
    // size in without ever holding it, because the builder keeps a bounded resident prefix and
    // spills the rest. `stage` cannot do this — it demands the whole `&mut [Vec<u8>]` first.

    /// Under the threshold the builder never touches disk, and every payload reads back
    /// byte-identically in append order.
    #[test]
    fn builder_stays_resident_below_threshold_and_reads_back_in_order() {
        let payloads: Vec<Vec<u8>> = (0..64usize).map(|i| vec![i as u8; i + 1]).collect();
        let mut builder = WriteSetSpillBuilder::with_threshold(u64::MAX);
        for payload in &payloads {
            builder.append(payload).unwrap();
        }
        assert!(
            !builder.spilled_to_disk(),
            "a small set must not touch disk"
        );
        let spill = builder.finish().unwrap();
        assert_eq!(spill.len(), payloads.len());
        assert_eq!(spill.bytes(), 0, "nothing reached the filesystem");
        assert_eq!(spill.resident_bytes(), builder_resident_total(&payloads));
        for (index, expected) in payloads.iter().enumerate() {
            assert_eq!(
                spill.entry(index).unwrap(),
                expected.as_slice(),
                "entry {index}"
            );
        }
    }

    fn builder_resident_total(payloads: &[Vec<u8>]) -> u64 {
        payloads.iter().map(|p| p.len() as u64).sum()
    }

    /// Past the threshold the builder SPILLS, the resident prefix stays within the buffer, and
    /// every payload still reads back byte-identically in append order.
    #[test]
    fn builder_spills_past_threshold_bounded_and_ordered() {
        let payloads: Vec<Vec<u8>> = (0..512usize).map(|i| vec![(i % 251) as u8; 64]).collect();
        let threshold = 256u64;
        let mut builder = WriteSetSpillBuilder::with_threshold(threshold);
        for payload in &payloads {
            builder.append(payload).unwrap();
        }
        assert!(
            builder.spilled_to_disk(),
            "past the buffer the builder spills"
        );
        assert!(
            builder.resident_bytes() <= threshold,
            "resident payload must stay within the buffer, got {} > {threshold}",
            builder.resident_bytes()
        );
        let spill = builder.finish().unwrap();
        assert_eq!(spill.len(), payloads.len(), "no entry is dropped");
        assert!(spill.bytes() > 0, "the bulk reached the filesystem");
        for (index, expected) in payloads.iter().enumerate() {
            assert_eq!(
                spill.entry(index).unwrap(),
                expected.as_slice(),
                "append order preserved at {index}"
            );
        }
    }

    /// A builder's output is byte-for-byte identical to `WriteSetSpill::stage`'s over the same
    /// payloads — the streaming path is a drop-in for the assembled one.
    #[test]
    fn builder_matches_stage_byte_for_byte() {
        let payloads: Vec<Vec<u8>> = (0..300usize)
            .map(|i| vec![(i % 97) as u8; 40 + i % 13])
            .collect();

        let mut builder = WriteSetSpillBuilder::with_threshold(128);
        for payload in &payloads {
            builder.append(payload).unwrap();
        }
        let streamed = builder.finish().unwrap();

        let reservation = reserve_write_set_stage().unwrap();
        let mut blobs = payloads.clone();
        let assembled = WriteSetSpill::stage(reservation, &mut blobs).unwrap();

        assert_eq!(streamed.len(), assembled.len());
        assert_eq!(streamed.bytes(), assembled.bytes());
        for index in 0..payloads.len() {
            assert_eq!(
                streamed.entry(index).unwrap(),
                assembled.entry(index).unwrap(),
                "entry {index} differs between the streaming and assembled spills"
            );
        }
    }

    /// NEGATIVE CONTROL for `builder_matches_stage_byte_for_byte` and
    /// `builder_spills_past_threshold_bounded_and_ordered`: the differential comparison and the
    /// per-index order assertion are LIVE, not vacuous. Flipping one payload byte and swapping two
    /// adjacent entries each make the read-back DIFFER from the streamed spill, so a spill that
    /// returned the wrong bytes — or the right bytes in the wrong order — would fail those tests
    /// rather than pass silently.
    #[test]
    fn a_mutated_or_reordered_set_differs_from_the_streamed_spill() {
        let payloads: Vec<Vec<u8>> = (0..300usize)
            .map(|i| vec![(i % 97) as u8; 40 + i % 13])
            .collect();
        let spill_over = |input: &[Vec<u8>]| {
            let mut builder = WriteSetSpillBuilder::with_threshold(128);
            for payload in input {
                builder.append(payload).unwrap();
            }
            builder.finish().unwrap()
        };

        let baseline = spill_over(&payloads);

        let mut mutated = payloads.clone();
        mutated[0][0] ^= 0xFF; // one wrong byte in entry 0
        let mut swapped = payloads.clone();
        swapped.swap(0, 1); // the right bytes, adjacent entries transposed

        for (label, variant) in [("mutated", mutated), ("swapped", swapped)] {
            let other = spill_over(&variant);
            assert!(
                (0..payloads.len()).any(|i| other.entry(i).unwrap() != baseline.entry(i).unwrap()),
                "the {label} control must DIFFER from the streamed spill — if it matched, the \
                 byte-identity and write-order assertions could never fail"
            );
        }
    }

    /// No cap: a write-set far larger than the buffer is ACCEPTED and spilled, never refused.
    /// The negative control (below) confirms the assertion is not vacuous.
    #[test]
    fn builder_accepts_a_write_set_far_larger_than_its_buffer() {
        let threshold = 1024u64;
        let payloads: Vec<Vec<u8>> = (0..4096usize).map(|i| vec![(i % 128) as u8; 256]).collect();
        let total: u64 = payloads.iter().map(|p| p.len() as u64).sum();
        assert!(
            total > threshold * 100,
            "the fixture must dwarf the buffer to prove there is no cap"
        );

        let mut builder = WriteSetSpillBuilder::with_threshold(threshold);
        for payload in &payloads {
            builder
                .append(payload)
                .expect("a large write-set must be accepted, never refused");
        }
        let spill = builder.finish().unwrap();
        assert_eq!(spill.len(), payloads.len(), "every payload is retained");
        assert!(
            spill.resident_bytes() <= threshold || spill.resident_bytes() == 0,
            "resident stays bounded regardless of total size"
        );
        assert_eq!(spill.bytes(), total, "the whole set reached disk");
    }

    /// NEGATIVE CONTROL: a `Vec<Vec<u8>>` accumulator — the shape the builder removes — grows
    /// without bound, so `builder_spills_past_threshold_bounded_and_ordered`'s bound is measuring
    /// the streaming, not a constant. This is the control that lets the guard fail.
    #[test]
    fn a_collecting_vec_accumulator_grows_without_bound() {
        let payloads: Vec<Vec<u8>> = (0..512usize).map(|_i| vec![0u8; 64]).collect();
        let threshold = 256u64;
        let mut collected: Vec<Vec<u8>> = Vec::new();
        for payload in &payloads {
            collected.push(payload.clone());
        }
        let collected_bytes: u64 = collected.iter().map(|p| p.len() as u64).sum();
        assert!(
            collected_bytes > threshold,
            "the control must exceed the buffer, else the guard is vacuous"
        );
        let mut builder = WriteSetSpillBuilder::with_threshold(threshold);
        for payload in &payloads {
            builder.append(payload).unwrap();
        }
        assert!(
            builder.resident_bytes() < collected_bytes,
            "the streaming builder holds strictly less than the collecting Vec: {} vs {collected_bytes}",
            builder.resident_bytes()
        );
    }
}
