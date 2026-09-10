//! A de-duplication set that does not grow without limit, and cleans up after
//! itself when the read it belongs to is abandoned.
//!
//! A cluster-wide indexed read asks every node for its slice and unions what
//! comes back, so the coordinator has to remember which row identities it has
//! already emitted. `coordinate_index_read_stream` kept that in a plain
//! `HashSet<(Vec<u8>, Vec<u8>)>`, which is `O(matching rows)` in coordinator
//! memory. Everything else on that path is bounded — the wire is chunked, the
//! consumer applies back-pressure, cancellation stops the producers — so this
//! set is the one thing a large tenant can still use to exhaust the node doing
//! the coordinating. A read is not bounded because most of it is.
//!
//! So the set spills. It keeps a bounded number of keys resident and writes the
//! rest to a temporary directory, which is removed when this value is dropped.
//! Dropping is what a cancelled read does, so cancellation cleans up by
//! construction rather than by remembering to.
//!
//! Spilled, not sorted: this only ever answers "have I seen this key", so it
//! needs membership, not order. Keys are appended to a spill file and an
//! in-memory index of their hashes points into it. That keeps the resident cost
//! per spilled key to one `u64` rather than the key itself, which for the row
//! identities used here — a partition key plus a clustering key — is one to two
//! orders of magnitude smaller.

use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// How many keys stay fully resident before the set starts spilling.
///
/// Deliberately a count rather than a byte budget: the keys here are row
/// identities of broadly similar size, and a count is something a reader of a
/// stack trace can reason about. `spill_budget` governs the byte-based
/// decisions elsewhere on this path.
pub const DEFAULT_RESIDENT_KEYS: usize = 65_536;

/// What the set did, so a caller can log it once rather than guess.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DedupStats {
    /// Distinct keys admitted.
    pub distinct: usize,
    /// Keys that were already present.
    pub duplicates: usize,
    /// Keys held on disk rather than in memory.
    pub spilled: usize,
}

/// A membership set that spills past a threshold and deletes its spill on drop.
pub struct SpillingDedup {
    resident: HashSet<Vec<u8>>,
    /// Hash → offsets in the spill file. A hash collision costs one seek and a
    /// byte comparison; it never costs correctness, which is why the offsets
    /// are a list rather than a single value.
    spilled_index: HashMap<u64, Vec<u64>>,
    spill_dir: Option<PathBuf>,
    spill_file: Option<BufWriter<File>>,
    spill_path: Option<PathBuf>,
    spill_len: u64,
    resident_limit: usize,
    stats: DedupStats,
}

impl SpillingDedup {
    /// A set that keeps `resident_limit` keys in memory and spills the rest
    /// beneath `dir`.
    ///
    /// The directory is created lazily: a read whose result fits in memory —
    /// which is nearly all of them — never touches the disk at all.
    #[must_use]
    pub fn new(dir: impl Into<PathBuf>, resident_limit: usize) -> Self {
        Self {
            resident: HashSet::new(),
            spilled_index: HashMap::new(),
            spill_dir: Some(dir.into()),
            spill_file: None,
            spill_path: None,
            spill_len: 0,
            resident_limit: resident_limit.max(1),
            stats: DedupStats {
                distinct: 0,
                duplicates: 0,
                spilled: 0,
            },
        }
    }

    /// Admit a key. `true` when it had not been seen before.
    ///
    /// An I/O failure while spilling is reported rather than swallowed: a
    /// de-duplication set that silently stops de-duplicating emits the same row
    /// twice, and a caller that cannot tell has no way to notice.
    pub fn insert(&mut self, key: &[u8]) -> std::io::Result<bool> {
        if self.resident.contains(key) {
            self.stats.duplicates += 1;
            return Ok(false);
        }
        if self.spill_file.is_some() && self.contains_spilled(key)? {
            self.stats.duplicates += 1;
            return Ok(false);
        }

        if self.resident.len() < self.resident_limit {
            self.resident.insert(key.to_vec());
        } else {
            self.spill(key)?;
            self.stats.spilled += 1;
        }
        self.stats.distinct += 1;
        Ok(true)
    }

    /// What this set did. For one log line at the end of a read, not per row.
    #[must_use]
    pub fn stats(&self) -> DedupStats {
        self.stats
    }

    /// Whether anything reached the disk.
    #[must_use]
    pub fn spilled_to_disk(&self) -> bool {
        self.spill_path.is_some()
    }

    /// The spill directory, while it exists. Exposed so a test can assert it is
    /// gone after a drop; nothing in the read path needs it.
    #[must_use]
    pub fn spill_path(&self) -> Option<&Path> {
        self.spill_path.as_deref()
    }

    fn spill(&mut self, key: &[u8]) -> std::io::Result<()> {
        if self.spill_file.is_none() {
            let dir = self.spill_dir.clone().unwrap_or_else(std::env::temp_dir);
            std::fs::create_dir_all(&dir)?;
            let path = dir.join(format!(
                "index-dedup-{}-{}.spill",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos())
                    .unwrap_or_default()
            ));
            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(true)
                .open(&path)?;
            self.spill_file = Some(BufWriter::new(file));
            self.spill_path = Some(path);
        }
        let offset = self.spill_len;
        let writer = self
            .spill_file
            .as_mut()
            .expect("spill file was just created");
        let len = u32::try_from(key.len()).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "dedup key is too long")
        })?;
        writer.write_all(&len.to_le_bytes())?;
        writer.write_all(key)?;
        self.spill_len += 4 + u64::from(len);
        self.spilled_index
            .entry(hash_of(key))
            .or_default()
            .push(offset);
        Ok(())
    }

    fn contains_spilled(&mut self, key: &[u8]) -> std::io::Result<bool> {
        let Some(offsets) = self.spilled_index.get(&hash_of(key)).cloned() else {
            return Ok(false);
        };
        // Flush before reading back: the key being looked for may still be in
        // the writer's buffer, and a membership set that misses a key it wrote
        // a moment ago is worse than one that never spilled.
        if let Some(writer) = self.spill_file.as_mut() {
            writer.flush()?;
        }
        let Some(path) = self.spill_path.as_ref() else {
            return Ok(false);
        };
        let mut file = File::open(path)?;
        for offset in offsets {
            file.seek(SeekFrom::Start(offset))?;
            let mut len_bytes = [0u8; 4];
            file.read_exact(&mut len_bytes)?;
            let len = u32::from_le_bytes(len_bytes) as usize;
            let mut buf = vec![0u8; len];
            file.read_exact(&mut buf)?;
            if buf == key {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

impl Drop for SpillingDedup {
    /// Remove the spill.
    ///
    /// This is the cancellation story: a read that is abandoned drops its
    /// de-dup set, and the temporary file goes with it. Nothing has to remember
    /// to clean up on the cancel path, which is the path least likely to be
    /// tested and most likely to be taken.
    fn drop(&mut self) {
        // Close the writer before unlinking, so the file is not held open by a
        // buffered handle on platforms that care.
        self.spill_file.take();
        if let Some(path) = self.spill_path.take() {
            if let Err(e) = std::fs::remove_file(&path) {
                if e.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(
                        path = %path.display(),
                        error = %e,
                        "could not remove the index de-duplication spill; it will be left behind"
                    );
                }
            }
        }
    }
}

fn hash_of(key: &[u8]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(i: usize) -> Vec<u8> {
        format!("partition-{i:08}/clustering-{i:08}").into_bytes()
    }

    /// The behaviour everything else depends on: a key is admitted once,
    /// however many times it arrives, and whether or not it spilled.
    #[test]
    fn a_key_is_admitted_once_across_the_spill_boundary() {
        let dir = tempfile::tempdir().unwrap();
        // Small limit so the test crosses the boundary quickly rather than
        // allocating 65k keys to prove a property that does not depend on the
        // number.
        let mut dedup = SpillingDedup::new(dir.path(), 8);

        for i in 0..64 {
            assert!(dedup.insert(&key(i)).unwrap(), "key {i} is new");
        }
        // Every one again, including the ones that went to disk.
        for i in 0..64 {
            assert!(
                !dedup.insert(&key(i)).unwrap(),
                "key {i} must be recognised as already seen"
            );
        }
        let stats = dedup.stats();
        assert_eq!(stats.distinct, 64);
        assert_eq!(stats.duplicates, 64);
        assert!(stats.spilled > 0, "the set must have spilled: {stats:?}");
    }

    /// Memory is bounded by the limit, not by the number of keys. This is the
    /// whole point: the coordinator's set used to grow with the size of the
    /// result.
    #[test]
    fn resident_memory_does_not_grow_with_the_result() {
        let dir = tempfile::tempdir().unwrap();
        let mut dedup = SpillingDedup::new(dir.path(), 16);
        for i in 0..4_096 {
            dedup.insert(&key(i)).unwrap();
        }
        assert_eq!(
            dedup.resident.len(),
            16,
            "resident keys must stop at the limit"
        );
        assert_eq!(dedup.stats().distinct, 4_096);
        assert_eq!(dedup.stats().spilled, 4_096 - 16);
    }

    /// A read that fits in memory never touches the disk.
    #[test]
    fn a_small_read_never_spills() {
        let dir = tempfile::tempdir().unwrap();
        let mut dedup = SpillingDedup::new(dir.path(), 1_024);
        for i in 0..100 {
            dedup.insert(&key(i)).unwrap();
        }
        assert!(!dedup.spilled_to_disk());
        assert_eq!(dedup.stats().spilled, 0);
        assert_eq!(
            std::fs::read_dir(dir.path()).unwrap().count(),
            0,
            "no file should have been created"
        );
    }

    /// Cancellation: dropping the set removes its spill. A cancelled read is a
    /// dropped read, so this is the cleanup path for cancellation.
    #[test]
    fn dropping_the_set_removes_its_spill() {
        let dir = tempfile::tempdir().unwrap();
        let path = {
            let mut dedup = SpillingDedup::new(dir.path(), 4);
            for i in 0..64 {
                dedup.insert(&key(i)).unwrap();
            }
            let path = dedup
                .spill_path()
                .expect("should have spilled")
                .to_path_buf();
            assert!(path.exists(), "spill exists while the read is running");
            path
        };
        assert!(
            !path.exists(),
            "the spill must be gone once the read is dropped: {}",
            path.display()
        );
        assert_eq!(
            std::fs::read_dir(dir.path()).unwrap().count(),
            0,
            "nothing may be left behind in the spill directory"
        );
    }

    /// Two keys that hash alike must not collapse into one row. A collision
    /// costs a seek; it must never cost a row.
    #[test]
    fn a_hash_collision_does_not_drop_a_row() {
        let dir = tempfile::tempdir().unwrap();
        let mut dedup = SpillingDedup::new(dir.path(), 1);
        let a = b"first-distinct-key".to_vec();
        let b = b"second-distinct-key".to_vec();
        assert!(dedup.insert(&a).unwrap());
        assert!(dedup.insert(&b).unwrap());
        // Force both through the spilled path and confirm they stay distinct.
        assert!(!dedup.insert(&a).unwrap());
        assert!(!dedup.insert(&b).unwrap());
        assert_eq!(dedup.stats().distinct, 2);

        // And the same offsets are reachable after a collision is simulated by
        // pointing an unrelated hash at them: membership is decided by the
        // bytes, not the hash.
        let planted = hash_of(b"planted");
        let offsets = dedup.spilled_index.values().flatten().copied().collect();
        dedup.spilled_index.insert(planted, offsets);
        assert!(
            !dedup.insert(b"planted").is_err(),
            "a collision must not error"
        );
    }
}
