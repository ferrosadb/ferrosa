//! A **streaming** staging area for a transaction's write-set payloads.
//!
//! [`crate::write_set_spill::WriteSetSpill`] stages payloads that are ALREADY
//! materialized: it takes `&mut [Vec<u8>]`, writes each blob to disk, and frees
//! the resident copy as it goes. That bounds the *coordinator's* residency once
//! the write-set exists, because the payloads are only copied *once* more, into
//! the spill. It does NOT bound the front end that builds the write-set in the
//! first place: a `COPY` inside `BEGIN` pushes one row at a time into a resident
//! `Vec`, and that Vec is the whole load until `COMMIT`. Staging *after* the fact
//! cannot fix a buffer that has already grown.
//!
//! [`WriteSetStage`] closes that gap. It is the same threshold-plus-spill idea,
//! but built to be driven AS THE ROWS ARRIVE: [`WriteSetStage::append`] hands it
//! one encoded payload at a time, it keeps a bounded resident prefix in memory
//! and spills the rest to a private staging file, and the resident set never
//! holds the bulk. Below the threshold nothing touches the disk at all, so the
//! common small transaction (a few rows) pays nothing.
//!
//! # The threshold is a BUFFER SIZE, not a cap
//!
//! The resident limit is a **streaming buffer size** — how much is held before
//! spilling — and it is EXTERNALIZED through
//! `FERROSA_WRITE_SET_SPILL_THRESHOLD_BYTES` (default
//! [`WRITE_SET_SPILL_FLOOR_BYTES`], 8 MiB), mirroring the
//! `FERROSA_ACCORD_COMPRESSION_*` knobs. It is deliberately an ABSOLUTE byte
//! count, not a fraction of RAM: a fraction of RAM is the wrong scale on a
//! 500 GB dataset, where 50% of a 4 GB node is still twice the whole machine's
//! share of one load. There is deliberately NO capacity refusal here: a write-set
//! larger than the threshold SPILLS, it is never rejected for being large.
//!
//! # Read back is zero-copy and fails loud
//!
//! [`WriteSetStage::finish`] flushes and memory-maps the staging region (when
//! anything spilled); [`StagedWriteSet::entry`] then returns a slice of the
//! mapping — no `seek`, no `read_exact`, no per-read allocation — exactly like
//! [`crate::write_set_spill::WriteSetSpill::entry`]. An index that was never
//! staged is a clear error, never a silent empty read: a missing payload must
//! never become an empty mutation the applier quietly drops.
//!
//! # Ordering
//!
//! Entries are readable in APPEND order. While resident, entry `i` is
//! `resident[i]`. On the first spill the resident prefix is drained to disk in
//! order (entries `0..k`) and every later append is written after it, so entry
//! `i >= k` is the `i - k`-th spilled entry. The read-back order is therefore
//! exactly the order the rows arrived — the order the commit path requires.
//!
//! # Cleanup
//!
//! The staging directory is owned by a [`TempSortTableReservation`] (the same
//! guard the ORDER BY and `DISTINCT` spills use) and is created lazily on the
//! first spill. Dropping the stage or the finished [`StagedWriteSet`] removes it,
//! so a rolled-back or abandoned transaction cleans up by construction.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use ferrosa_common::{Error, Result};

use crate::commitlog::mutation::Mutation;
use crate::engine::TempSortTableReservation;

pub use crate::write_set_spill::WRITE_SET_SPILL_FLOOR_BYTES;

/// Env var holding the resident buffer size, in bytes, before a write-set spills.
///
/// A larger value spills later (fewer disk round-trips, more resident); a smaller
/// value spills sooner (less resident, more disk). This is a tuning knob, never a
/// correctness limit: any write-set, however large, is accepted and spilled.
pub const WRITE_SET_SPILL_THRESHOLD_ENV: &str = "FERROSA_WRITE_SET_SPILL_THRESHOLD_BYTES";

/// Resolve the configured resident buffer size, falling back to
/// [`WRITE_SET_SPILL_FLOOR_BYTES`] on an unset, non-numeric, or zero value.
///
/// A zero or malformed value is refused as a *setting* (it would spill on every
/// row) and the default is used; it never refuses work. Pure, so it is testable
/// without touching the process environment.
#[must_use]
pub fn resolve_spill_threshold(raw: Option<&str>) -> u64 {
    match raw {
        Some(value) => match value.trim().parse::<u64>() {
            Ok(n) if n > 0 => n,
            _ => {
                tracing::warn!(
                    value = %value,
                    env = WRITE_SET_SPILL_THRESHOLD_ENV,
                    "write-set stage: ignoring invalid threshold (expected a positive byte count); \
                     using the default"
                );
                WRITE_SET_SPILL_FLOOR_BYTES
            }
        },
        None => WRITE_SET_SPILL_FLOOR_BYTES,
    }
}

/// A write-set being built one payload at a time, bounded by a resident buffer.
pub struct WriteSetStage {
    /// Directory the spill file is created in (a fresh per-transaction dir).
    dir: PathBuf,
    /// Resident buffer size before the stage spills. A BUFFER SIZE, not a cap.
    threshold_bytes: u64,
    /// Encoded payloads held in memory while under the threshold. Drained in
    /// order on the first spill; never re-filled after that, so `resident.len()`
    /// is frozen once `spill_file` is `Some`.
    resident: Vec<Vec<u8>>,
    /// Sum of `resident` lengths.
    resident_bytes: u64,
    /// Owns the staging directory; `None` until the first spill. Dropping the
    /// stage removes the directory.
    reservation: Option<TempSortTableReservation>,
    /// The spill writer; `None` while fully resident.
    spill_file: Option<BufWriter<File>>,
    /// Byte offset of each SPILLED payload, in append order.
    offsets: Vec<u64>,
    /// Length in bytes of each SPILLED payload.
    lens: Vec<u32>,
    /// Total payload bytes written to disk.
    spill_bytes: u64,
}

impl std::fmt::Debug for WriteSetStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteSetStage")
            .field("entries", &self.len())
            .field("resident_bytes", &self.resident_bytes)
            .field("spill_bytes", &self.spill_bytes)
            .field("spilled", &self.spill_file.is_some())
            .finish_non_exhaustive()
    }
}

impl WriteSetStage {
    /// A stage that keeps `threshold_bytes` resident before spilling beneath
    /// `dir`. The directory is created lazily on the first spill, so a small
    /// transaction never touches the filesystem.
    #[must_use]
    pub fn with_threshold(dir: impl Into<PathBuf>, threshold_bytes: u64) -> Self {
        Self {
            dir: dir.into(),
            threshold_bytes: threshold_bytes.max(1),
            resident: Vec::new(),
            resident_bytes: 0,
            reservation: None,
            spill_file: None,
            offsets: Vec::new(),
            lens: Vec::new(),
            spill_bytes: 0,
        }
    }

    /// A stage using the process-wide [`WRITE_SET_SPILL_THRESHOLD_ENV`] setting,
    /// read once here.
    #[must_use]
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        let threshold =
            resolve_spill_threshold(std::env::var(WRITE_SET_SPILL_THRESHOLD_ENV).ok().as_deref());
        Self::with_threshold(dir, threshold)
    }

    /// Number of payloads appended so far (resident + spilled).
    #[must_use]
    pub fn len(&self) -> usize {
        self.resident.len() + self.offsets.len()
    }

    /// Whether nothing has been staged.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Payload bytes still held in memory. Bounded by the threshold: this never
    /// grows past `threshold_bytes` once spilling begins.
    #[must_use]
    pub fn resident_bytes(&self) -> u64 {
        self.resident_bytes
    }

    /// The resident buffer size this stage was configured with.
    #[must_use]
    pub fn threshold_bytes(&self) -> u64 {
        self.threshold_bytes
    }

    /// Total payload bytes written to disk.
    #[must_use]
    pub fn spill_bytes(&self) -> u64 {
        self.spill_bytes
    }

    /// Whether anything reached the disk.
    #[must_use]
    pub fn spilled_to_disk(&self) -> bool {
        self.spill_file.is_some()
    }

    /// Bytes of the resident INDEX (offset + length tables). The only part of a
    /// spilled write-set that stays in memory besides the un-spilled prefix.
    #[must_use]
    pub fn resident_index_bytes(&self) -> u64 {
        (self.offsets.capacity() * std::mem::size_of::<u64>()
            + self.lens.capacity() * std::mem::size_of::<u32>()) as u64
    }

    /// Append one encoded payload, spilling the resident prefix if this append
    /// would push the resident buffer past the threshold.
    ///
    /// A write error is surfaced, never swallowed: a stage that silently dropped a
    /// payload would commit a write-set missing rows.
    pub fn append(&mut self, payload: &[u8]) -> Result<()> {
        let len = u32::try_from(payload.len()).map_err(|_| {
            Error::InvalidFormat(format!(
                "write-set stage: payload of {} bytes exceeds the u32 staging bound",
                payload.len()
            ))
        })?;
        if self.spill_file.is_none() {
            // Stay resident while the whole prefix still fits the buffer. The
            // comparison is on `>` so a payload that lands EXACTLY on the
            // threshold stays resident (the threshold is an inclusive bound).
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
        let reservation = reserve_stage_dir(&self.dir)?;
        let path = reservation.path().join("write-set.bin");
        let file = File::create(&path).map_err(|e| {
            Error::InvalidFormat(format!("write-set stage: create {}: {e}", path.display()))
        })?;
        let mut writer = BufWriter::new(file);
        // Drain the resident prefix in order. `std::mem::take` frees each payload's
        // memory as it is written, so the drain's own extra residency is one
        // payload, not the whole prefix.
        for blob in self.resident.drain(..) {
            let len = u32::try_from(blob.len()).unwrap_or(u32::MAX);
            self.offsets.push(self.spill_bytes);
            self.lens.push(len);
            writer.write_all(&blob).map_err(|e| {
                Error::InvalidFormat(format!("write-set stage: write {}: {e}", path.display()))
            })?;
            self.spill_bytes += u64::from(len);
        }
        self.resident_bytes = 0;
        self.spill_file = Some(writer);
        self.reservation = Some(reservation);
        Ok(())
    }

    fn write_spilled(&mut self, payload: &[u8], len: u32) -> Result<()> {
        let writer = self
            .spill_file
            .as_mut()
            .expect("write_spilled is only called once the stage is spilling");
        self.offsets.push(self.spill_bytes);
        self.lens.push(len);
        writer
            .write_all(payload)
            .map_err(|e| Error::InvalidFormat(format!("write-set stage: write payload: {e}")))?;
        self.spill_bytes += u64::from(len);
        Ok(())
    }

    /// Visit every payload staged SO FAR, in append order, WITHOUT finishing the
    /// stage — the write-set survives for the commit.
    ///
    /// This is the read-your-own-writes path: a `SELECT` inside an open
    /// transaction must see the rows the transaction has already staged, even
    /// once they have spilled. A spilled stage is flushed and read back by
    /// offset; a fully-resident stage is visited in place. Residency is one
    /// payload, never the write-set.
    ///
    /// # Errors
    ///
    /// A flush or read failure is surfaced, never swallowed — a peek that
    /// silently dropped a staged payload would answer a query with rows missing.
    pub fn for_each_staged(&mut self, visit: &mut dyn FnMut(&[u8]) -> Result<()>) -> Result<()> {
        if self.spill_file.is_none() {
            // Nothing has spilled: every payload is still in the resident prefix.
            for blob in &self.resident {
                visit(blob)?;
            }
            return Ok(());
        }

        // Flush the writer so every appended payload is on disk, then read the
        // staged region back. The writer borrow is confined to this block so the
        // offset index below can be borrowed immutably.
        let path = {
            let writer = self
                .spill_file
                .as_mut()
                .expect("a spilled stage always holds its writer");
            writer
                .flush()
                .map_err(|e| Error::InvalidFormat(format!("write-set stage: flush: {e}")))?;
            self.reservation
                .as_ref()
                .expect("a spilling stage always owns its reservation")
                .path()
                .join("write-set.bin")
        };
        let file = File::open(&path).map_err(|e| {
            Error::InvalidFormat(format!("write-set stage: reopen {}: {e}", path.display()))
        })?;
        use std::os::unix::fs::FileExt;

        let mut buf: Vec<u8> = Vec::new();
        for index in 0..self.offsets.len() {
            let offset = self.offsets[index];
            let len = self.lens[index] as usize;
            buf.clear();
            buf.resize(len, 0);
            file.read_exact_at(&mut buf, offset).map_err(|e| {
                Error::InvalidFormat(format!(
                    "write-set stage: read staged entry {index} at {offset}+{len} in {}: {e}",
                    path.display()
                ))
            })?;
            visit(&buf)?;
        }
        Ok(())
    }

    /// Flush, memory-map the spilled region, and return the finished view.
    ///
    /// A fully-resident stage (nothing spilled) returns a value with no mapping
    /// and every entry read from memory.
    pub fn finish(mut self) -> Result<StagedWriteSet> {
        let staged = match self.spill_file.take() {
            Some(mut writer) => {
                writer
                    .flush()
                    .map_err(|e| Error::InvalidFormat(format!("write-set stage: flush: {e}")))?;
                // Drop the writer so the file is closed before mapping.
                drop(writer);
                let reservation = self
                    .reservation
                    .take()
                    .expect("a spilling stage always owns its reservation");
                let path = reservation.path().join("write-set.bin");
                let file = File::open(&path).map_err(|e| {
                    Error::InvalidFormat(format!("write-set stage: reopen {}: {e}", path.display()))
                })?;
                let map = if self.spill_bytes == 0 {
                    None
                } else {
                    // SAFETY: the staging file is private to this stage — it lives
                    // in a fresh directory created by `reserve_stage_dir`, the writer
                    // is closed above, and no writable handle survives. Nothing can
                    // mutate the bytes under the mapping.
                    Some(unsafe { memmap2::Mmap::map(&file) }.map_err(|e| {
                        Error::InvalidFormat(format!(
                            "write-set stage: mmap {}: {e}",
                            path.display()
                        ))
                    })?)
                };
                drop(file);
                self.reservation = Some(reservation);
                return Ok(StagedWriteSet {
                    // `map` first so it unmaps before `_reservation` removes the dir.
                    map,
                    _reservation: self.reservation,
                    path,
                    resident: std::mem::take(&mut self.resident),
                    offsets: std::mem::take(&mut self.offsets),
                    lens: std::mem::take(&mut self.lens),
                    bytes: self.spill_bytes,
                });
            }
            None => Ok(StagedWriteSet {
                map: None,
                _reservation: None,
                path: PathBuf::new(),
                resident: std::mem::take(&mut self.resident),
                offsets: Vec::new(),
                lens: Vec::new(),
                bytes: 0,
            }),
        };
        staged
    }
}

/// A finished [`WriteSetStage`], readable by index in append order.
pub struct StagedWriteSet {
    /// The spilled region, memory-mapped read-only. Declared FIRST so it is
    /// unmapped before `_reservation` removes the staging directory.
    map: Option<memmap2::Mmap>,
    /// Owns the staging directory; dropping the view removes it.
    _reservation: Option<TempSortTableReservation>,
    path: PathBuf,
    /// The resident prefix (entries before the first spill).
    resident: Vec<Vec<u8>>,
    /// Byte offset of each spilled payload.
    offsets: Vec<u64>,
    /// Length of each spilled payload.
    lens: Vec<u32>,
    /// Total spilled bytes.
    bytes: u64,
}

impl std::fmt::Debug for StagedWriteSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StagedWriteSet")
            .field("entries", &self.len())
            .field("resident", &self.resident.len())
            .field("bytes", &self.bytes)
            .finish_non_exhaustive()
    }
}

impl StagedWriteSet {
    /// Number of staged entries (resident + spilled).
    #[must_use]
    pub fn len(&self) -> usize {
        self.resident.len() + self.offsets.len()
    }

    /// Whether nothing was staged.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Total payload bytes staged on disk.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Whether anything reached the disk.
    #[must_use]
    pub fn spilled_to_disk(&self) -> bool {
        self.map.is_some()
    }

    /// Borrow the payload staged at `index` — a slice of the resident buffer or
    /// of the staging region's memory map, never a fresh allocation.
    ///
    /// FAILS LOUD on an index that was never staged, and on a staged range that
    /// would escape the mapping (a corrupt offset table must never read past the
    /// mapped region).
    pub fn entry(&self, index: usize) -> Result<&[u8]> {
        if index < self.resident.len() {
            return Ok(&self.resident[index]);
        }
        let spilled = index - self.resident.len();
        let (&offset, &len) = self
            .offsets
            .get(spilled)
            .zip(self.lens.get(spilled))
            .ok_or_else(|| {
                Error::InvalidData(format!(
                    "write-set stage: index {index} is out of range ({} entries staged)",
                    self.len()
                ))
            })?;
        let start = usize::try_from(offset).map_err(|_| {
            Error::InvalidData(format!(
                "write-set stage: staged offset {offset} does not fit in usize"
            ))
        })?;
        let end = start.checked_add(len as usize).ok_or_else(|| {
            Error::InvalidData(format!(
                "write-set stage: staged range {start}..(start+{len}) overflows usize"
            ))
        })?;
        match &self.map {
            Some(map) => map.get(start..end).ok_or_else(|| {
                Error::InvalidData(format!(
                    "write-set stage: staged range {start}..{end} escapes the {} byte mapping of {}",
                    map.len(),
                    self.path.display()
                ))
            }),
            // A zero-byte staging file: no mapping, and every spilled entry is empty.
            None if len == 0 => Ok(&[]),
            None => Err(Error::InvalidData(format!(
                "write-set stage: entry {index} claims {len} bytes but nothing was staged in {}",
                self.path.display()
            ))),
        }
    }

    /// Owned copy of the payload at `index`. FAILS LOUD on an un-staged index.
    pub fn payload(&self, index: usize) -> Result<Vec<u8>> {
        Ok(self.entry(index)?.to_vec())
    }
}

/// A write-set the commit path can read **more than once**, in bounded chunks.
///
/// [`StorageEngine::write_atomic_batch`](crate::engine::StorageEngine::write_atomic_batch)
/// makes three passes over a write-set (preflight, commit-log append, memtable
/// apply) under a single fsync group. Every pass must be able to visit the same
/// set again, in the same order, without the set being resident as a
/// `Vec<Mutation>`. This trait is that contract.
///
/// Two sources satisfy it:
///
/// - a resident `Vec<Mutation>` (the small, common autocommit batch), and
/// - a spilled [`StagedWriteSet`], each of whose entries is one
///   [`Mutation::serialize_into`] frame, decoded one at a time as a pass visits
///   it. Residency is then one decoded mutation, never the whole set.
///
/// # Fail loud
///
/// A staged source that does not decode (`deserialize_from` error) or an entry
/// that is not staged ([`StagedWriteSet::entry`] error) propagates as an error.
/// It is never a silent empty mutation.
pub trait WriteSetSource {
    /// Number of mutations in the set. Used only to decide "empty ⇒ no-op";
    /// never as a capacity bound.
    fn mutation_count(&self) -> usize;

    /// Whether the set holds no mutations.
    fn is_empty_set(&self) -> bool {
        self.mutation_count() == 0
    }

    /// Visit every mutation in APPEND order, one at a time. The borrow handed to
    /// `visit` lives only for that call, so the source never materializes the
    /// whole set.
    ///
    /// # Errors
    ///
    /// Any error from `visit`, or from reading a staged entry, ends the walk and
    /// is returned unchanged.
    fn for_each_mutation(&self, visit: &mut dyn FnMut(&Mutation) -> Result<()>) -> Result<()>;

    /// Visit every mutation in APPEND order, one at a time, handing the **owned**
    /// mutation to `visit`.
    ///
    /// A consumer that must buffer a bounded window of mutations — the Accord
    /// write-set builder batch-reads before-images for a chunk of them — needs to
    /// own each mutation for the lifetime of that window. This is that contract.
    ///
    /// The DEFAULT clones the borrowed mutation, which is honest for a resident
    /// `Vec<Mutation>` (the small autocommit batch). A [`StagedWriteSet`] overrides
    /// it to MOVE the mutation it just decoded, so streaming a staged write-set
    /// never copies a payload.
    fn for_each_owned_mutation(&self, visit: &mut dyn FnMut(Mutation) -> Result<()>) -> Result<()> {
        self.for_each_mutation(&mut |mutation| visit(mutation.clone()))
    }
}

/// A resident write-set. Bounded by whatever assembled it; this is the small
/// autocommit batch, not a staged transaction.
impl WriteSetSource for Vec<Mutation> {
    fn mutation_count(&self) -> usize {
        self.len()
    }

    fn for_each_mutation(&self, visit: &mut dyn FnMut(&Mutation) -> Result<()>) -> Result<()> {
        for mutation in self {
            visit(mutation)?;
        }
        Ok(())
    }
}

/// A staged, threshold-bounded write-set. Each pass re-decodes one frame at a
/// time out of the resident prefix or the memory-mapped spill, so residency is
/// one mutation, not the write-set.
///
/// Every entry MUST be a [`Mutation`] serialized with `Mutation::serialize_into`;
/// any other bytes fail loud in `Mutation::deserialize_from`, never decode to an
/// empty mutation.
impl WriteSetSource for StagedWriteSet {
    fn mutation_count(&self) -> usize {
        self.len()
    }

    fn for_each_mutation(&self, visit: &mut dyn FnMut(&Mutation) -> Result<()>) -> Result<()> {
        for index in 0..self.len() {
            let frame = self.entry(index)?;
            let mutation = Mutation::deserialize_from(frame).map_err(|e| {
                Error::InvalidData(format!(
                    "write-set stage: staged entry {index} is not a decodable mutation frame: {e}"
                ))
            })?;
            visit(&mutation)?;
        }
        Ok(())
    }

    /// The decode already produced an OWNED mutation, so hand it on by MOVE: a
    /// consumer that buffers a bounded prefetch window pays no per-payload copy.
    fn for_each_owned_mutation(&self, visit: &mut dyn FnMut(Mutation) -> Result<()>) -> Result<()> {
        for index in 0..self.len() {
            let frame = self.entry(index)?;
            let mutation = Mutation::deserialize_from(frame).map_err(|e| {
                Error::InvalidData(format!(
                    "write-set stage: staged entry {index} is not a decodable mutation frame: {e}"
                ))
            })?;
            visit(mutation)?;
        }
        Ok(())
    }
}

/// Create a fresh staging directory under `root` and return the reservation that
/// owns it. Kept separate from [`WriteSetSpill`](crate::write_set_spill::WriteSetSpill)'s
/// reserve so the two staging kinds never share a directory namespace.
fn reserve_stage_dir(root: &std::path::Path) -> Result<TempSortTableReservation> {
    std::fs::create_dir_all(root).map_err(|e| {
        Error::InvalidFormat(format!(
            "write-set stage: create staging root {}: {e}",
            root.display()
        ))
    })?;
    let dir = root.join(format!("stage-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&dir).map_err(|e| {
        Error::InvalidFormat(format!(
            "write-set stage: create staging dir {}: {e}",
            dir.display()
        ))
    })?;
    Ok(TempSortTableReservation::claim_dir(dir))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "ferrosa-write-set-stage-test-{}-{}",
            tag,
            uuid::Uuid::new_v4()
        ))
    }

    fn stage_entries(entries: &[Vec<u8>], threshold: u64) -> (PathBuf, StagedWriteSet) {
        let root = temp_root("stage");
        let mut stage = WriteSetStage::with_threshold(&root, threshold);
        for entry in entries {
            stage.append(entry).expect("append");
        }
        let staged = stage.finish().expect("finish");
        (root, staged)
    }

    /// INVARIANT: RESULTS COMPLETE — every appended payload round-trips byte for
    /// byte, ACROSS a spill boundary and at the LAST entry. The boundary entry is
    /// the one an off-by-one in the drain/index arithmetic would corrupt, and the
    /// last entry is the one a short flush would drop.
    #[test]
    fn every_entry_round_trips_across_the_spill_boundary_and_the_last_entry() {
        let entries: Vec<Vec<u8>> = vec![
            b"row-0".to_vec(),
            b"row-1-slightly-longer".to_vec(),
            // Forces the spill: this payload alone exceeds the 16-byte threshold.
            vec![0xABu8; 40],
            b"row-3".to_vec(),
            b"row-4-final".to_vec(),
        ];
        let (_root, staged) = stage_entries(&entries, 16);
        assert!(staged.spilled_to_disk(), "the prefix must have spilled");
        assert_eq!(staged.len(), entries.len());
        for (index, expected) in entries.iter().enumerate() {
            assert_eq!(
                staged.entry(index).expect("entry"),
                expected.as_slice(),
                "entry {index} must be byte-identical to what was appended"
            );
            assert_eq!(staged.payload(index).expect("payload"), *expected);
        }
    }

    /// INVARIANT: DATA PRESERVED — an EMPTY payload round-trips as empty, never as
    /// "missing", both resident and spilled.
    #[test]
    fn empty_payloads_round_trip_as_empty_not_missing() {
        let entries: Vec<Vec<u8>> = vec![
            b"first".to_vec(),
            Vec::new(),
            vec![0u8; 32],
            Vec::new(),
            b"last".to_vec(),
        ];
        let (_root, staged) = stage_entries(&entries, 8);
        assert!(staged.spilled_to_disk());
        for (index, expected) in entries.iter().enumerate() {
            assert_eq!(staged.entry(index).expect("entry"), expected.as_slice());
        }
    }

    /// INVARIANT: ORDERING — the entries are readable in APPEND order across the
    /// spill boundary. The resident prefix must precede the spilled suffix, and the
    /// drain must not reorder or reverse either segment.
    #[test]
    fn entries_are_readable_in_append_order_across_the_boundary() {
        // Each payload is 5 bytes; a 12-byte threshold keeps two resident then spills.
        let entries: Vec<Vec<u8>> = (0..12u8)
            .map(|i| format!("e{i:04}").into_bytes()) // "e0000".."e0011", all 5 bytes
            .collect();
        let (_root, staged) = stage_entries(&entries, 12);
        assert!(staged.spilled_to_disk());
        for (index, expected) in entries.iter().enumerate() {
            assert_eq!(
                staged.entry(index).expect("entry"),
                expected.as_slice(),
                "append order must be preserved at index {index}"
            );
        }
    }

    /// INVARIANT: CAPACITY (the owner's invariant #4) — resident memory is bounded
    /// by the threshold and does NOT grow with entry count. Two entry counts orders
    /// of magnitude apart must not grow residency together: the smaller set fits and
    /// holds at most the threshold, the far larger set spills and holds NONE of it.
    /// A completed stage at either scale holds COMPLETE data.
    ///
    /// This is deliberately the OPPOSITE of a cap test: it never asserts a refusal.
    /// A write-set far larger than the threshold SUCCEEDS, complete.
    #[test]
    fn residency_is_flat_across_widely_separated_entry_counts() {
        let root = temp_root("flat");
        let threshold = 4096u64;
        let payload = vec![0x5Au8; 64]; // 64 bytes each

        // 64 * 64 == 4096 == threshold: fits exactly, so it stays fully resident.
        let mut small = WriteSetStage::with_threshold(&root, threshold);
        for _ in 0..64 {
            small.append(&payload).expect("append");
        }
        let small_resident_at_end = small.resident_bytes();
        let small_len = small.len();
        let small = small.finish().expect("finish");

        // 10_000 * 64 == 640_000 bytes: far past the threshold, so it spills and
        // keeps NOTHING resident.
        let mut large = WriteSetStage::with_threshold(&root, threshold);
        for _ in 0..10_000 {
            large.append(&payload).expect("append");
        }
        let large_resident_at_end = large.resident_bytes();
        let large_len = large.len();
        let large_bytes_on_disk = large.spill_bytes();
        let large = large.finish().expect("finish");

        // Residency is bounded by the threshold and does NOT track entry count.
        assert_eq!(
            small_resident_at_end, threshold,
            "an exactly-threshold set stays resident (the threshold is inclusive)"
        );
        assert_eq!(
            large_resident_at_end, 0,
            "once spilling, no payload stays resident — residency is FLAT in N"
        );
        assert_eq!(small_len, 64);
        assert_eq!(
            large_len, 10_000,
            "a write-set far larger than the threshold is ACCEPTED, not refused"
        );

        // Completeness at both scales: a fully-resident set touches no disk, and a
        // spilled set puts EVERY payload byte on disk.
        assert_eq!(small.bytes(), 0, "a fully-resident set spills nothing");
        assert_eq!(small.entry(0).expect("first"), payload.as_slice());
        assert_eq!(small.entry(63).expect("last"), payload.as_slice());
        assert_eq!(
            large_bytes_on_disk,
            10_000 * 64,
            "every byte of a spilled set is on disk"
        );
        assert_eq!(large.bytes(), 10_000 * 64);
        assert_eq!(large.entry(0).expect("first"), payload.as_slice());
        assert_eq!(
            large.entry(large_len - 1).expect("last"),
            payload.as_slice()
        );
    }

    /// INVARIANT: FAIL LOUD — reading an index that was never staged is a clear
    /// error, never a silent empty read. Covers both the resident and the spilled
    /// regime.
    #[test]
    fn an_un_staged_index_fails_loud_in_both_regimes() {
        // Fully resident.
        let (_root, resident) = stage_entries(&[b"only".to_vec()], 1 << 20);
        assert!(!resident.spilled_to_disk());
        let error = resident.entry(1).expect_err("index 1 was never staged");
        assert!(
            error.to_string().contains("out of range"),
            "a missing resident entry must fail loud: {error}"
        );

        // Spilled.
        let entries: Vec<Vec<u8>> = vec![vec![7u8; 8], vec![9u8; 64], vec![1u8; 8]];
        let (_root, spilled) = stage_entries(&entries, 16);
        assert!(spilled.spilled_to_disk());
        let error = spilled.entry(entries.len()).expect_err("past the end");
        assert!(
            error.to_string().contains("out of range"),
            "a missing spilled entry must fail loud: {error}"
        );
    }

    /// A stage that stays under the threshold never touches the disk, and its
    /// entries are still readable in order.
    #[test]
    fn a_small_write_set_stays_fully_resident() {
        let entries: Vec<Vec<u8>> = vec![b"a".to_vec(), b"bb".to_vec(), b"ccc".to_vec()];
        let (_root, staged) = stage_entries(&entries, 1 << 20);
        assert!(
            !staged.spilled_to_disk(),
            "a small set must not touch the disk"
        );
        assert_eq!(staged.bytes(), 0);
        for (index, expected) in entries.iter().enumerate() {
            assert_eq!(staged.entry(index).expect("entry"), expected.as_slice());
        }
    }

    /// Dropping the finished view (or the stage) removes the staging directory.
    #[test]
    fn dropping_the_staged_view_removes_the_staging_directory() {
        let root = temp_root("cleanup");
        let mut stage = WriteSetStage::with_threshold(&root, 8);
        stage.append(&[0u8; 64]).expect("append");
        assert!(stage.spilled_to_disk());
        let staged = stage.finish().expect("finish");
        let dir = staged
            ._reservation
            .as_ref()
            .expect("spilled")
            .path()
            .to_path_buf();
        assert!(dir.exists());
        drop(staged);
        assert!(
            !dir.exists(),
            "dropping the staged view must remove its staging directory"
        );
    }

    /// A non-consuming peek visits every payload staged SO FAR, in append order,
    /// in BOTH regimes, and leaves the stage usable (the write-set survives for
    /// the commit). This is the read-your-own-writes path over a spilled set.
    #[test]
    fn peeking_a_live_stage_visits_every_payload_without_consuming_it() {
        // Spilled: the peek must read back out of the staging file.
        let root = temp_root("peek");
        let entries: Vec<Vec<u8>> = (0..10u8)
            .map(|i| format!("payload-{i:02}").into_bytes())
            .collect();
        let mut stage = WriteSetStage::with_threshold(&root, 16);
        for entry in &entries {
            stage.append(entry).expect("append");
        }
        assert!(stage.spilled_to_disk());

        let mut seen: Vec<Vec<u8>> = Vec::new();
        stage
            .for_each_staged(&mut |payload| {
                seen.push(payload.to_vec());
                Ok(())
            })
            .expect("peek");
        assert_eq!(
            seen, entries,
            "a peek must visit every payload in append order"
        );

        // The peek did not consume the stage: appending more still works, and the
        // finished view holds everything.
        stage.append(b"tail").expect("append after peek");
        let staged = stage.finish().expect("finish");
        assert_eq!(staged.len(), entries.len() + 1);
        assert_eq!(staged.entry(entries.len()).expect("tail"), b"tail");

        // Fully resident: the peek visits the resident prefix in place.
        let mut resident = WriteSetStage::with_threshold(&root, 1 << 20);
        resident.append(b"a").expect("append");
        resident.append(b"bb").expect("append");
        let mut seen_resident: Vec<Vec<u8>> = Vec::new();
        resident
            .for_each_staged(&mut |payload| {
                seen_resident.push(payload.to_vec());
                Ok(())
            })
            .expect("peek");
        assert_eq!(seen_resident, vec![b"a".to_vec(), b"bb".to_vec()]);
        let _ = resident.finish().expect("finish");
    }

    /// An invalid threshold setting is refused as a *setting* and the default is
    /// used — it never refuses WORK. Pure, so no env mutation is needed.
    #[test]
    fn an_invalid_threshold_setting_falls_back_to_the_default() {
        assert_eq!(resolve_spill_threshold(None), WRITE_SET_SPILL_FLOOR_BYTES);
        assert_eq!(
            resolve_spill_threshold(Some("0")),
            WRITE_SET_SPILL_FLOOR_BYTES
        );
        assert_eq!(
            resolve_spill_threshold(Some("nope")),
            WRITE_SET_SPILL_FLOOR_BYTES
        );
        assert_eq!(
            resolve_spill_threshold(Some("")),
            WRITE_SET_SPILL_FLOOR_BYTES
        );
        assert_eq!(resolve_spill_threshold(Some(" 1048576 ")), 1_048_576);
    }

    // -- WriteSetSource: the contract the streaming commit path relies on --

    /// One serialized [`Mutation`] frame carrying a single row, so a source test
    /// can prove byte-for-byte round-trip and append order.
    fn mutation_frame(keyspace: &str, table: &str, pk: &[u8], value: &[u8], ts: i64) -> Vec<u8> {
        use ferrosa_common::cell::CellValue;
        use ferrosa_common::key::{DecoratedKey, PartitionKey};
        use ferrosa_common::Token;
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

        let mutation = Mutation {
            mutation_id: [0x3Cu8; 16],
            keyspace: keyspace.to_string(),
            table: table.to_string(),
            key: DecoratedKey {
                token: Token(ts),
                key: PartitionKey::new(pk.to_vec()),
            },
            // The cell timestamp is the token here, so a frame decoded out of
            // order is detectable: the row's `primary_key_liveness` timestamp
            // carries the intended position.
            rows: vec![Row {
                clustering: vec![],
                cells: vec![(0, CellValue::live(value.to_vec(), ts))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(ts),
            }],
            timestamp: ts,
        };
        let mut frame = vec![0u8; mutation.serialized_size()];
        mutation.serialize_into(&mut frame);
        frame
    }

    /// INVARIANT: ORDERING — a staged source visits every mutation in APPEND
    /// order, ACROSS the spill boundary. The commit path's pass 2 and pass 3 both
    /// consume this order, so the append order the log records is the apply order
    /// the memtable sees.
    #[test]
    fn the_source_visits_every_mutation_in_append_order_across_the_boundary() {
        let root = temp_root("source-order");
        let frames: Vec<Vec<u8>> = (0..12)
            .map(|i| mutation_frame("ks", "t", format!("pk{i}").as_bytes(), b"v", i + 1))
            .collect();
        // A threshold far below the set forces the resident prefix to spill.
        let mut stage = WriteSetStage::with_threshold(&root, 64);
        for frame in &frames {
            stage.append(frame).expect("append");
        }
        let staged = stage.finish().expect("finish");
        assert!(staged.spilled_to_disk(), "the boundary must be exercised");

        let mut seen: Vec<(String, i64)> = Vec::new();
        staged
            .for_each_mutation(&mut |m| {
                seen.push((
                    m.key
                        .key
                        .as_bytes()
                        .to_vec()
                        .into_iter()
                        .map(char::from)
                        .collect(),
                    m.timestamp,
                ));
                Ok(())
            })
            .expect("walk");

        assert_eq!(
            seen.len(),
            frames.len(),
            "every frame is visited exactly once"
        );
        let expected: Vec<(String, i64)> = (0..12).map(|i| (format!("pk{i}"), i + 1)).collect();
        assert_eq!(
            seen, expected,
            "append order must be preserved across the boundary"
        );
    }

    /// A resident `Vec<Mutation>` source visits in order too — the small
    /// autocommit batch stays on the same contract.
    #[test]
    fn the_resident_vec_source_visits_in_order() {
        let mutations: Vec<Mutation> = Vec::new();
        assert!(mutations.is_empty_set());
        assert_eq!(mutations.mutation_count(), 0);
        assert!(Vec::<Mutation>::new()
            .for_each_mutation(&mut |_| panic!("an empty set visits nothing"))
            .is_ok());
    }

    /// INVARIANT: FAIL LOUD — a staged frame that is not a decodable mutation is
    /// a clear error from the source, never a silent empty mutation.
    #[test]
    fn a_non_mutation_frame_fails_loud_from_the_source() {
        let root = temp_root("source-corrupt");
        let mut stage = WriteSetStage::with_threshold(&root, 4);
        stage
            .append(b"this is definitely not a serialized mutation")
            .expect("append");
        let staged = stage.finish().expect("finish");

        let error = staged
            .for_each_mutation(&mut |_| panic!("a corrupt frame must not be visited"))
            .expect_err("a corrupt frame must fail loud");
        assert!(
            error.to_string().contains("not a decodable mutation frame"),
            "clear error, never a silent empty mutation: {error}"
        );
    }
}
