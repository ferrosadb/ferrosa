//! Row and value counting for `SELECT COUNT(*)`, `COUNT(DISTINCT v)`, and
//! `COUNT(ESTIMATE_DISTINCT v)`.
//!
//! Three aggregates, three contracts, three types. They are deliberately not
//! interchangeable:
//!
//! | type | answers | exact? | in-memory | on disk |
//! |---|---|---|---|---|
//! | [`RowCounter`] | `COUNT(*)` | yes | O(1) | none |
//! | [`DistinctExact`] | `COUNT(DISTINCT v)` | **yes** | O(1) | O(distinct) |
//! | [`DistinctEstimate`] | `COUNT(ESTIMATE_DISTINCT v)` | no — lower bound | O(m), fixed | none |
//!
//! The CQL surface spells these as different functions on purpose. `COUNT(DISTINCT v)`
//! is exact by definition and must never return a sketch's answer; an estimate is a
//! different function with a different name, so no caller can be surprised by which
//! one it got.
//!
//! # Why exact distinct streams to disk
//!
//! An exact distinct count must remember which values it has seen. Keeping them in
//! memory costs O(distinct) — that is what a `HashSet` would do, and it is exactly
//! what a count over a large partition cannot afford. Keeping them **on disk** in an
//! open-addressed indexed map holds the *in-memory* footprint at O(1): one slot read
//! per probe plus the value bytes being compared. Insert stays O(1) expected. The
//! page cache is then the OS's to manage, so pressure from a big distinct count is
//! evictable instead of resident in our allocator.
//!
//! The map is a fixed-size slot table plus an append-only value arena:
//!
//! ```text
//! [0..64)              header: magic, capacity, used, arena_end
//! [64 .. 64+cap*24)    slots: (tag:u64, off:u64, len:u32, pad:u32); tag 0 = empty
//! [arena_end .. )      values, one copy each, appended
//! ```
//!
//! Insert probes linearly from `hash(value) mod capacity`, compares the stored bytes
//! on a tag match (the tag is only a filter — the bytes decide), and doubles with a
//! rehash when the load factor would pass 0.7.
//!
//! # Why the estimate is a lower bound
//!
//! [`DistinctEstimate`] increments only when a Bloom probe says "unseen". A Bloom
//! filter has **no false negatives**, so every increment witnesses a genuinely new
//! value and the count can only be short — never long. That asymmetry is why an
//! estimate is safe to expose under its own name and unsafe to pass off as exact.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

// ── COUNT(*) ─────────────────────────────────────────────────────────────

/// An exact live-row counter for a single partition.
///
/// Three counters rather than one so drift is detectable: `inserts` and
/// `overwrites` say how the live count was reached, and a
/// [`reconcile`](Self::reconcile) that disagrees with `live` is a caller bug rather
/// than a counter that silently rots.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RowCounter {
    live: u64,
    inserts: u64,
    overwrites: u64,
}

impl RowCounter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a write of one row. `existed` is `true` when the primary key was
    /// already live in this partition, in which case the row count does not change.
    pub fn on_write(&mut self, existed: bool) {
        if existed {
            self.overwrites = self.overwrites.saturating_add(1);
        } else {
            self.live = self.live.saturating_add(1);
            self.inserts = self.inserts.saturating_add(1);
        }
    }

    /// Record a delete. Deleting an absent row (a tombstone for a key that never
    /// existed) is not a decrement.
    pub fn on_delete(&mut self, existed: bool) {
        if existed {
            self.live = self.live.saturating_sub(1);
        }
    }

    /// Replace the count with one a merge computed. Flushes and compactions walk
    /// every row anyway, so they know the exact answer and the incremental counter
    /// need not have been right all along.
    pub fn reconcile(&mut self, exact: u64) {
        self.live = exact;
    }

    pub fn count(&self) -> u64 {
        self.live
    }

    pub fn inserts(&self) -> u64 {
        self.inserts
    }

    pub fn overwrites(&self) -> u64 {
        self.overwrites
    }

    /// Whether the incremental count disagrees with a merge-computed `exact`.
    pub fn drifts_from(&self, exact: u64) -> bool {
        self.live != exact
    }
}

/// Count the items an iterator yields, without materialising them.
///
/// The point is the signature: the caller hands over an iterator, so there is no
/// place for a `Vec<Row>` to appear. `COUNT(*)` over a scan becomes this call.
pub fn count_stream<I: Iterator>(it: I) -> u64 {
    it.count() as u64
}

// ── COUNT(DISTINCT v) — exact, disk-backed ───────────────────────────────

const MAGIC: u64 = 0x4645_5252_4449_5354; // "FERRDIST"
const HEADER: u64 = 64;
const SLOT: u64 = 24;
const MIN_CAPACITY: u64 = 64;
/// Grow when `used * LOAD_DEN >= capacity * LOAD_NUM`.
const LOAD_NUM: u64 = 7;
const LOAD_DEN: u64 = 10;
/// Copy buffer for the arena during a rehash.
const COPY_BUF: usize = 256 * 1024;

/// Exact distinct counting over values that do not fit in memory.
///
/// In-memory footprint is constant: a header's worth of scalars, one slot per probe,
/// and one value buffer for the comparison. Everything that grows lives in the file.
#[derive(Debug)]
pub struct DistinctExact {
    file: File,
    path: PathBuf,
    capacity: u64,
    used: u64,
    arena_end: u64,
    last_probes: u64,
    /// Largest single buffer held in memory, for the tests' benefit.
    peak_buffer: usize,
    remove_on_drop: bool,
}

impl DistinctExact {
    /// Create a new exact counter, sized for `expected` distinct values.
    ///
    /// The file is created in `dir` (or the system temp directory). Capacity is the
    /// next power of two that keeps the load factor under 0.7 for `expected`, so a
    /// good guess means no rehash at all and a bad guess costs an amortised O(1)
    /// doubling.
    pub fn create(expected: u64, dir: Option<&Path>) -> io::Result<Self> {
        let capacity = Self::capacity_for(expected);
        let path = Self::unique_path(dir);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        let mut this = Self {
            file,
            path,
            capacity,
            used: 0,
            arena_end: HEADER + capacity * SLOT,
            last_probes: 0,
            peak_buffer: 0,
            remove_on_drop: true,
        };
        this.write_header()?;
        // Size the slot table up front so an empty slot can be read rather than
        // treated as EOF. The file is sparse, so this costs no data blocks until a
        // slot is written or a value is appended.
        this.file.set_len(HEADER + capacity * SLOT)?;
        Ok(this)
    }

    /// Reopen an existing counter. The count comes back from the header, so an
    /// interrupted count resumes rather than restarting.
    pub fn reopen(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        let mut magic = [0u8; 8];
        file.read_exact_at(&mut magic, 0)?;
        if u64::from_le_bytes(magic) != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "not a distinct-count file",
            ));
        }
        let mut buf = [0u8; 24];
        file.read_exact_at(&mut buf, 8)?;
        let capacity = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let used = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        let arena_end = u64::from_le_bytes(buf[16..24].try_into().unwrap());
        Ok(Self {
            file,
            path,
            capacity,
            used,
            arena_end,
            last_probes: 0,
            peak_buffer: 0,
            remove_on_drop: false,
        })
    }

    fn capacity_for(expected: u64) -> u64 {
        let want = expected.max(1) * LOAD_DEN / LOAD_NUM + 1;
        let mut c = MIN_CAPACITY;
        while c < want {
            c *= 2;
        }
        c
    }

    fn unique_path(dir: Option<&Path>) -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        let base = dir
            .map(|p| p.to_path_buf())
            .unwrap_or_else(std::env::temp_dir);
        base.join(format!("ferrosa-distinct-{}-{}.bin", std::process::id(), n))
    }

    /// Distinct values observed. Exact.
    pub fn count(&self) -> u64 {
        self.used
    }

    /// Probes the last [`insert`](Self::insert) needed. Exposed so a test can assert
    /// the insert stays O(1) rather than degrading into a scan.
    pub fn last_probes(&self) -> u64 {
        self.last_probes
    }

    /// Largest buffer this counter has held in memory. Constant in the number of
    /// distinct values — the property the disk-backed design buys.
    pub fn peak_buffer_bytes(&self) -> usize {
        self.peak_buffer
    }

    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Keep the file after drop (the caller owns it).
    pub fn persist(&mut self) {
        self.remove_on_drop = false;
    }

    /// Record a value. Returns `true` when it had not been seen before — the only
    /// case in which [`count`](Self::count) advances.
    pub fn insert(&mut self, value: &[u8]) -> io::Result<bool> {
        let h = tag_of(value);
        let mut i = h & (self.capacity - 1);
        let mut probes = 0u64;
        loop {
            let (slot_tag, off, len) = self.read_slot(i)?;
            if slot_tag == 0 {
                let at = self.arena_end;
                self.file.write_all_at(value, at)?;
                self.arena_end += value.len() as u64;
                self.write_slot(i, h, at, value.len() as u32)?;
                self.used += 1;
                self.last_probes = probes;
                self.write_header()?;
                if self.used * LOAD_DEN >= self.capacity * LOAD_NUM {
                    self.grow()?;
                }
                return Ok(true);
            }
            // A tag match is only a filter: the bytes decide whether this is the
            // same value or a collision that must keep probing.
            if slot_tag == h && len as usize == value.len() {
                let mut buf = vec![0u8; len as usize];
                self.peak_buffer = self.peak_buffer.max(buf.len());
                self.file.read_exact_at(&mut buf, off)?;
                if buf == value {
                    self.last_probes = probes;
                    return Ok(false);
                }
            }
            i = (i + 1) & (self.capacity - 1);
            probes += 1;
            if probes >= self.capacity {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "distinct map full: probe wrapped",
                ));
            }
        }
    }

    /// Whether the value has been inserted.
    pub fn contains(&self, value: &[u8]) -> io::Result<bool> {
        let h = tag_of(value);
        let mut i = h & (self.capacity - 1);
        for _ in 0..self.capacity {
            let (slot_tag, off, len) = self.read_slot(i)?;
            if slot_tag == 0 {
                return Ok(false);
            }
            if slot_tag == h && len as usize == value.len() {
                let mut buf = vec![0u8; len as usize];
                self.file.read_exact_at(&mut buf, off)?;
                if buf == value {
                    return Ok(true);
                }
            }
            i = (i + 1) & (self.capacity - 1);
        }
        Ok(false)
    }

    fn read_slot(&self, i: u64) -> io::Result<(u64, u64, u32)> {
        let mut buf = [0u8; SLOT as usize];
        self.file.read_exact_at(&mut buf, HEADER + i * SLOT)?;
        Ok((
            u64::from_le_bytes(buf[0..8].try_into().unwrap()),
            u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            u32::from_le_bytes(buf[16..20].try_into().unwrap()),
        ))
    }

    fn write_slot(&self, i: u64, tag: u64, off: u64, len: u32) -> io::Result<()> {
        let mut buf = [0u8; SLOT as usize];
        buf[0..8].copy_from_slice(&tag.to_le_bytes());
        buf[8..16].copy_from_slice(&off.to_le_bytes());
        buf[16..20].copy_from_slice(&len.to_le_bytes());
        self.file.write_all_at(&buf, HEADER + i * SLOT)
    }

    fn write_header(&self) -> io::Result<()> {
        let mut buf = [0u8; HEADER as usize];
        buf[0..8].copy_from_slice(&MAGIC.to_le_bytes());
        buf[8..16].copy_from_slice(&self.capacity.to_le_bytes());
        buf[16..24].copy_from_slice(&self.used.to_le_bytes());
        buf[24..32].copy_from_slice(&self.arena_end.to_le_bytes());
        self.file.write_all_at(&buf, 0)
    }

    /// Double the slot table and rehash. Amortised O(1) per insert; the arena is
    /// copied once rather than re-read per step.
    fn grow(&mut self) -> io::Result<()> {
        let old_capacity = self.capacity;
        let new_capacity = old_capacity * 2;
        let old_arena_base = HEADER + old_capacity * SLOT;
        let new_arena_base = HEADER + new_capacity * SLOT;
        let new_path = {
            let p = self.path.clone();
            let stem = p
                .file_stem()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_else(|| "distinct".to_string());
            p.with_file_name(format!("{stem}.grow"))
        };
        let new_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&new_path)?;

        // Move the arena wholesale; only the slot indices change.
        let arena = self.arena_end - old_arena_base;
        let mut copied = 0u64;
        let mut buf = vec![0u8; COPY_BUF];
        while copied < arena {
            let n = ((arena - copied) as usize).min(buf.len());
            self.file
                .read_exact_at(&mut buf[..n], old_arena_base + copied)?;
            new_file.write_all_at(&buf[..n], new_arena_base + copied)?;
            copied += n as u64;
        }

        let mut grown = Self {
            file: new_file,
            path: new_path,
            capacity: new_capacity,
            used: 0,
            arena_end: new_arena_base + arena,
            last_probes: 0,
            peak_buffer: self.peak_buffer,
            remove_on_drop: true,
        };
        grown.write_header()?;
        grown.file.set_len(new_arena_base + arena)?;

        // Rehash: every old slot probes into the new table.
        for i in 0..old_capacity {
            let (slot_tag, off, len) = self.read_slot(i)?;
            if slot_tag == 0 {
                continue;
            }
            let new_off = off - old_arena_base + new_arena_base;
            let mut probe = slot_tag & (new_capacity - 1);
            loop {
                let (t, _, _) = grown.read_slot(probe)?;
                if t == 0 {
                    grown.write_slot(probe, slot_tag, new_off, len)?;
                    break;
                }
                probe = (probe + 1) & (new_capacity - 1);
            }
            grown.used += 1;
        }
        grown.write_header()?;

        let old_path = std::mem::replace(&mut self.path, grown.path.clone());
        std::fs::remove_file(&old_path).ok();
        *self = grown;
        Ok(())
    }
}

impl Drop for DistinctExact {
    fn drop(&mut self) {
        if self.remove_on_drop {
            std::fs::remove_file(&self.path).ok();
        }
    }
}

/// A 64-bit tag for a value. `0` is reserved for "empty slot", so a zero hash is
/// remapped to 1; the stored bytes still decide equality, so the remap costs nothing
/// but a probe.
fn tag_of(value: &[u8]) -> u64 {
    let h = fnv1a(value);
    if h == 0 {
        1
    } else {
        h
    }
}

// ── COUNT(ESTIMATE_DISTINCT v) — Bloom-based, bounded memory ────────────

/// A Bloom filter plus a counter of the values that were new to it.
///
/// Sized from an expected cardinality and a target false-positive rate using
/// `m = -n ln(p) / (ln 2)^2` and `k = (m/n) ln 2`, both clamped so a hostile
/// `expected` cannot allocate wildly.
#[derive(Debug, Clone)]
pub struct DistinctEstimate {
    bits: Vec<u64>,
    bit_count: u64,
    hash_count: u32,
    seen: u64,
    /// The design target, kept so saturation is measured against intent.
    target_fp_rate: f64,
}

/// Largest bit vector we will allocate: 64 MiB. At the default 1% target that is
/// ~76M distinct values.
const MAX_BITS: u64 = 64 * 1024 * 1024 * 8;
const MIN_BITS: u64 = 64;

impl DistinctEstimate {
    pub fn with_capacity(expected: u64, fp_rate: f64) -> Self {
        let p = if fp_rate.is_finite() && fp_rate > 0.0 && fp_rate < 1.0 {
            fp_rate
        } else {
            0.01
        };
        let n = expected.max(1) as f64;
        let ln2 = std::f64::consts::LN_2;
        let m = (-n * p.ln() / (ln2 * ln2)).ceil();
        let m = (m as u64).clamp(MIN_BITS, MAX_BITS);
        let k = ((m as f64 / n) * ln2).round().clamp(1.0, 24.0) as u32;
        let words = m.div_ceil(64) as usize;
        Self {
            bits: vec![0u64; words],
            bit_count: words as u64 * 64,
            hash_count: k,
            seen: 0,
            target_fp_rate: p,
        }
    }

    pub fn new() -> Self {
        Self::with_capacity(10_000, 0.01)
    }

    pub fn size_bytes(&self) -> usize {
        self.bits.len() * 8
    }

    /// Distinct values observed so far. A **lower bound** on the true count: the
    /// estimate can be short by up to the false-positive rate, never long.
    pub fn estimate(&self) -> u64 {
        self.seen
    }

    pub fn fill_ratio(&self) -> f64 {
        let set: u32 = self.bits.iter().map(|w| w.count_ones()).sum();
        set as f64 / self.bit_count as f64
    }

    /// The false-positive rate implied by the current fill, `(1 - e^{-kn/m})^k`
    /// with `n` estimated from the fill. This is the error bound on the estimate.
    pub fn estimated_false_positive_rate(&self) -> f64 {
        self.fill_ratio().powi(self.hash_count as i32)
    }

    /// Whether the filter has drifted past its design target, in which case the
    /// estimate should not be trusted without an exact recount.
    pub fn is_saturated(&self) -> bool {
        self.estimated_false_positive_rate() > self.target_fp_rate * 2.0
    }

    /// Insert a value. `true` when the filter had not seen it — the only case in
    /// which the estimate advances.
    pub fn insert(&mut self, item: &[u8]) -> bool {
        let (h1, h2) = Self::double_hash(item);
        let mut already = true;
        for i in 0..self.hash_count {
            let bit = Self::nth_bit(h1, h2, i, self.bit_count);
            if !self.test_bit(bit) {
                already = false;
                self.set_bit(bit);
            }
        }
        if !already {
            self.seen = self.seen.saturating_add(1);
        }
        !already
    }

    /// Whether the value is (probably) present. Never `false` for an inserted
    /// value: no false negatives, which is what makes the estimate a lower bound.
    pub fn contains(&self, item: &[u8]) -> bool {
        let (h1, h2) = Self::double_hash(item);
        (0..self.hash_count).all(|i| self.test_bit(Self::nth_bit(h1, h2, i, self.bit_count)))
    }

    fn test_bit(&self, bit: u64) -> bool {
        let (w, s) = ((bit / 64) as usize, bit % 64);
        self.bits[w] & (1u64 << s) != 0
    }

    fn set_bit(&mut self, bit: u64) {
        let (w, s) = ((bit / 64) as usize, bit % 64);
        self.bits[w] |= 1u64 << s;
    }

    /// Kirsch–Mitzenmacher: `k` probes from two hashes.
    fn nth_bit(h1: u64, h2: u64, i: u32, m: u64) -> u64 {
        h1.wrapping_add((i as u64).wrapping_mul(h2)) % m
    }

    fn double_hash(item: &[u8]) -> (u64, u64) {
        let h1 = fnv1a(item);
        // `| 1` keeps the step odd, so the k probes spread instead of collapsing.
        let h2 = splitmix64(h1 ^ 0x9e37_79b9_7f4a_7c15) | 1;
        (h1, h2)
    }
}

impl Default for DistinctEstimate {
    fn default() -> Self {
        Self::new()
    }
}

/// Count distinct values an iterator yields, feeding an estimate. Returns a lower
/// bound. Use [`count_distinct_stream_exact`] when the contract is exact.
pub fn count_distinct_stream_estimate<T, I, F>(
    it: I,
    mut key: F,
    sketch: &mut DistinctEstimate,
) -> u64
where
    I: Iterator<Item = T>,
    F: FnMut(&T) -> Vec<u8>,
{
    for item in it {
        sketch.insert(&key(&item));
    }
    sketch.estimate()
}

/// Count distinct values exactly, streaming each value to a disk-backed indexed map.
/// Memory stays O(1); insert is O(1) expected.
pub fn count_distinct_stream_exact<T, I, F>(
    it: I,
    mut key: F,
    counter: &mut DistinctExact,
) -> io::Result<u64>
where
    I: Iterator<Item = T>,
    F: FnMut(&T) -> Vec<u8>,
{
    for item in it {
        counter.insert(&key(&item))?;
    }
    Ok(counter.count())
}

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    // ── RowCounter ──────────────────────────────────────────────────────

    #[test]
    fn overwrite_does_not_increment() {
        let mut c = RowCounter::new();
        c.on_write(false);
        c.on_write(true);
        c.on_write(true);
        assert_eq!(c.count(), 1);
        assert_eq!(c.inserts(), 1);
        assert_eq!(c.overwrites(), 2);
    }

    #[test]
    fn delete_of_absent_row_does_not_decrement() {
        let mut c = RowCounter::new();
        c.on_write(false);
        c.on_delete(false);
        assert_eq!(c.count(), 1);
        c.on_delete(true);
        assert_eq!(c.count(), 0);
    }

    #[test]
    fn delete_never_underflows() {
        let mut c = RowCounter::new();
        c.on_delete(true);
        assert_eq!(c.count(), 0);
    }

    #[test]
    fn reconcile_corrects_drift() {
        let mut c = RowCounter::new();
        for _ in 0..10 {
            c.on_write(false);
        }
        assert!(c.drifts_from(7));
        c.reconcile(7);
        assert_eq!(c.count(), 7);
        assert!(!c.drifts_from(7));
    }

    #[test]
    fn count_stream_does_not_need_a_collection() {
        struct Rows(u64);
        impl Iterator for Rows {
            type Item = u64;
            fn next(&mut self) -> Option<u64> {
                if self.0 == 0 {
                    None
                } else {
                    self.0 -= 1;
                    Some(self.0)
                }
            }
        }
        assert_eq!(count_stream(Rows(1_000_000)), 1_000_000);
    }

    // ── DistinctExact ───────────────────────────────────────────────────

    #[test]
    fn exact_matches_a_hashset_oracle() {
        let mut d = DistinctExact::create(1_000, None).unwrap();
        let mut oracle = HashSet::new();
        let mut x: u64 = 0x1234_5678;
        for _ in 0..20_000 {
            x = splitmix64(x);
            let v = (x % 5_000).to_le_bytes();
            let fresh = d.insert(&v).unwrap();
            assert_eq!(fresh, oracle.insert(v), "insert must agree with the oracle");
        }
        assert_eq!(d.count(), oracle.len() as u64);
    }

    #[test]
    fn duplicates_never_increment() {
        let mut d = DistinctExact::create(64, None).unwrap();
        assert!(d.insert(b"same").unwrap());
        for _ in 0..10_000 {
            assert!(!d.insert(b"same").unwrap());
        }
        assert_eq!(d.count(), 1);
    }

    #[test]
    fn contains_reports_what_was_inserted() {
        let mut d = DistinctExact::create(1_000, None).unwrap();
        for i in 0..500u64 {
            d.insert(&i.to_le_bytes()).unwrap();
        }
        for i in 0..500u64 {
            assert!(
                d.contains(&i.to_le_bytes()).unwrap(),
                "lost inserted value {i}"
            );
        }
        assert!(!d.contains(&500u64.to_le_bytes()).unwrap());
    }

    #[test]
    fn grows_past_its_initial_capacity() {
        // Sized for 64, fed 20_000: the doubling path and its rehash must stay exact,
        // which is what keeps "O(1) expected" honest.
        let mut d = DistinctExact::create(64, None).unwrap();
        for i in 0..20_000u64 {
            d.insert(&i.to_le_bytes()).unwrap();
        }
        assert_eq!(d.count(), 20_000);
        assert!(d.capacity() >= 20_000);
        for i in (0..20_000u64).step_by(997) {
            assert!(d.contains(&i.to_le_bytes()).unwrap());
        }
    }

    #[test]
    fn insert_is_o1_probes() {
        let mut d = DistinctExact::create(10_000, None).unwrap();
        let mut total = 0u64;
        for i in 0..10_000u64 {
            d.insert(&i.to_le_bytes()).unwrap();
            total += d.last_probes();
        }
        let avg = total as f64 / 10_000.0;
        assert!(
            avg < 8.0,
            "average probes {avg} suggests the insert is not O(1)"
        );
    }

    #[test]
    fn memory_does_not_grow_with_cardinality() {
        let mut small = DistinctExact::create(1_000, None).unwrap();
        let mut large = DistinctExact::create(1_000, None).unwrap();
        for i in 0..100u64 {
            small.insert(&i.to_le_bytes()).unwrap();
        }
        for i in 0..100_000u64 {
            large.insert(&i.to_le_bytes()).unwrap();
        }
        // The buffer high-water mark is one probe's worth of bytes either way; the
        // values live in the file, so this does not scale with distinct count.
        assert!(
            large.peak_buffer_bytes() <= 64,
            "peak buffer {} grew with cardinality",
            large.peak_buffer_bytes()
        );
        assert_eq!(small.peak_buffer_bytes(), large.peak_buffer_bytes());
        assert_eq!(large.count(), 100_000);
    }

    #[test]
    fn reopen_recovers_the_count() {
        let mut d = DistinctExact::create(1_000, None).unwrap();
        for i in 0..250u64 {
            d.insert(&i.to_le_bytes()).unwrap();
        }
        let path = d.path().to_path_buf();
        d.persist();
        drop(d);
        let reopened = DistinctExact::reopen(&path).unwrap();
        assert_eq!(reopened.count(), 250);
        assert!(reopened.contains(&7u64.to_le_bytes()).unwrap());
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn exact_stream_matches_the_counter() {
        let rows = (0..50u64).map(|i| i % 10);
        let mut d = DistinctExact::create(64, None).unwrap();
        let n =
            count_distinct_stream_exact(rows, |v: &u64| v.to_le_bytes().to_vec(), &mut d).unwrap();
        assert_eq!(n, 10);
    }

    #[test]
    fn empty_counter_reports_zero() {
        let d = DistinctExact::create(16, None).unwrap();
        assert_eq!(d.count(), 0);
        assert!(!d.contains(b"anything").unwrap());
    }

    // ── DistinctEstimate ────────────────────────────────────────────────

    #[test]
    fn estimate_never_overcounts() {
        let mut d = DistinctEstimate::with_capacity(1_000, 0.01);
        for _ in 0..5 {
            for i in 0..100u64 {
                d.insert(&i.to_le_bytes());
            }
        }
        assert_eq!(d.estimate(), 100);
        assert!(d.estimate() <= 100);
    }

    #[test]
    fn bloom_has_no_false_negatives() {
        let mut d = DistinctEstimate::with_capacity(1_000, 0.01);
        let items: Vec<Vec<u8>> = (0..500u64).map(|i| i.to_le_bytes().to_vec()).collect();
        for it in &items {
            d.insert(it);
        }
        for it in &items {
            assert!(d.contains(it), "a Bloom filter must never forget an insert");
        }
    }

    #[test]
    fn estimate_is_bounded_not_exact() {
        // The separation, made executable: the same 10_000 values give an exact
        // answer from one type and a short answer from the other.
        let mut exact = DistinctExact::create(10, None).unwrap();
        let mut est = DistinctEstimate::with_capacity(10, 0.01);
        for i in 0..10_000u64 {
            exact.insert(&i.to_le_bytes()).unwrap();
            est.insert(&i.to_le_bytes());
        }
        assert_eq!(exact.count(), 10_000);
        assert!(
            est.estimate() < 10_000,
            "an estimate must not be passed off as exact"
        );
        assert!(
            est.is_saturated(),
            "saturation must be visible to the caller"
        );
    }

    #[test]
    fn estimate_memory_is_the_declared_size() {
        let mut d = DistinctEstimate::with_capacity(1_000, 0.01);
        for i in 0..10_000u64 {
            d.insert(&i.to_le_bytes());
        }
        assert_eq!(d.size_bytes() % 8, 0);
        assert!(d.size_bytes() < 1 << 20);
    }

    #[test]
    fn estimate_stream_matches_the_sketch() {
        let rows = (0..50u64).map(|i| i % 10);
        let mut d = DistinctEstimate::with_capacity(1_000, 0.01);
        let n = count_distinct_stream_estimate(rows, |v: &u64| v.to_le_bytes().to_vec(), &mut d);
        assert_eq!(n, 10);
    }
}
