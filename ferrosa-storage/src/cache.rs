//! Local disk cache for SSTable data with LRU eviction.
//!
//! Tracks downloaded SSTable component files on local ephemeral disk.
//! When total size exceeds the configured limit, evicts least-recently-used
//! entries. Pinned entries (referenced by the current manifest) are never evicted.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use parking_lot::Mutex;

/// A cached SSTable component file on local disk.
struct CacheEntry {
    path: PathBuf,
    size: u64,
    last_accessed: Instant,
}

/// LRU-based local disk cache for SSTable files.
///
/// Thread-safe via interior mutex. Call `register()` when a file is
/// downloaded, `touch()` on read hits, and `evict_if_needed()` periodically
/// or after registering new files.
///
/// When constructed with `durable = true` (the local `file://` backend), the
/// local disk *is* the durable store of record, so eviction is disabled
/// entirely: [`evict_if_needed`](Self::evict_if_needed) is a no-op. Evicting a
/// flushed SSTable would otherwise drop the only durable copy.
pub struct LocalCache {
    base_dir: PathBuf,
    max_bytes: u64,
    /// When true, disk is the durable store — never evict.
    durable: bool,
    entries: Mutex<HashMap<String, CacheEntry>>,
    /// Guards the one-shot "eviction disabled" log line.
    eviction_disabled_logged: AtomicBool,
}

impl LocalCache {
    /// Creates a new cache rooted at `base_dir` with the given size limit.
    ///
    /// Eviction is enabled (the cache is a write-behind cache over a remote
    /// durable store such as S3).
    pub fn new(base_dir: PathBuf, max_bytes: u64) -> Self {
        Self::new_with_durability(base_dir, max_bytes, false)
    }

    /// Creates a new cache, specifying whether the local disk is durable.
    ///
    /// When `durable` is true (local `file://` backend), eviction is disabled
    /// so flushed SSTables are never removed from their only durable home.
    pub fn new_with_durability(base_dir: PathBuf, max_bytes: u64, durable: bool) -> Self {
        Self {
            base_dir,
            max_bytes,
            durable,
            entries: Mutex::new(HashMap::new()),
            eviction_disabled_logged: AtomicBool::new(false),
        }
    }

    /// Whether the local disk is treated as the durable store (no eviction).
    pub fn is_durable(&self) -> bool {
        self.durable
    }

    /// Returns the base directory for cached files.
    pub fn base_dir(&self) -> &PathBuf {
        &self.base_dir
    }

    /// Registers a file in the cache.
    pub fn register(&self, id: &str, path: PathBuf, size: u64) {
        self.entries.lock().insert(
            id.to_string(),
            CacheEntry {
                path,
                size,
                last_accessed: Instant::now(),
            },
        );
    }

    /// Marks an entry as recently accessed (prevents near-term eviction).
    pub fn touch(&self, id: &str) {
        if let Some(entry) = self.entries.lock().get_mut(id) {
            entry.last_accessed = Instant::now();
        }
    }

    /// Returns the total size of all cached files in bytes.
    pub fn total_size(&self) -> u64 {
        self.entries.lock().values().map(|e| e.size).sum()
    }

    /// Returns true if the cache contains the given entry.
    pub fn contains(&self, id: &str) -> bool {
        self.entries.lock().contains_key(id)
    }

    /// Returns the file path for a cached entry, if it exists.
    pub fn get_path(&self, id: &str) -> Option<PathBuf> {
        self.entries.lock().get(id).map(|e| e.path.clone())
    }

    /// Evicts least-recently-used entries until total size is under `max_bytes`.
    ///
    /// Entries whose IDs appear in `pinned` are never evicted.
    /// Returns the file paths that were removed (caller should delete them).
    pub fn evict_if_needed(&self, pinned: &HashSet<String>) -> Vec<PathBuf> {
        // Durable local backend: disk is the source of truth. Evicting a
        // flushed SSTable would drop its only durable copy, so never evict.
        if self.durable {
            if !self.eviction_disabled_logged.swap(true, Ordering::Relaxed) {
                tracing::info!(
                    "local file:// backend is durable — SSTable cache eviction \
                     disabled (local disk is the durable store of record)"
                );
            }
            return Vec::new();
        }

        let mut entries = self.entries.lock();
        let total: u64 = entries.values().map(|e| e.size).sum();
        if total <= self.max_bytes {
            return Vec::new();
        }

        // Sort by last_accessed ascending (oldest first).
        let mut candidates: Vec<_> = entries
            .iter()
            .filter(|(id, _)| !pinned.contains(*id))
            .map(|(id, e)| (id.clone(), e.last_accessed, e.size))
            .collect();
        candidates.sort_by_key(|(_, accessed, _)| *accessed);

        let mut removed = Vec::new();
        let mut current_total = total;

        for (id, _, size) in candidates {
            if current_total <= self.max_bytes {
                break;
            }
            if let Some(entry) = entries.remove(&id) {
                removed.push(entry.path);
                current_total -= size;
            }
        }

        removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_total_size() {
        let cache = LocalCache::new(PathBuf::from("/tmp/test-cache"), 1000);
        cache.register("a", PathBuf::from("/tmp/test-cache/a"), 100);
        cache.register("b", PathBuf::from("/tmp/test-cache/b"), 200);

        assert_eq!(cache.total_size(), 300);
        assert!(cache.contains("a"));
        assert!(cache.contains("b"));
        assert!(!cache.contains("c"));
    }

    #[test]
    fn get_path() {
        let cache = LocalCache::new(PathBuf::from("/tmp/test-cache"), 1000);
        cache.register("x", PathBuf::from("/tmp/test-cache/x"), 50);

        assert_eq!(
            cache.get_path("x"),
            Some(PathBuf::from("/tmp/test-cache/x"))
        );
        assert_eq!(cache.get_path("missing"), None);
    }

    #[test]
    fn eviction_removes_oldest() {
        let cache = LocalCache::new(PathBuf::from("/tmp"), 200);

        // Register three entries totaling 300 bytes (over 200 limit).
        cache.register("old", PathBuf::from("/tmp/old"), 100);

        // Brief pause so timestamps differ.
        std::thread::sleep(std::time::Duration::from_millis(10));
        cache.register("mid", PathBuf::from("/tmp/mid"), 100);

        std::thread::sleep(std::time::Duration::from_millis(10));
        cache.register("new", PathBuf::from("/tmp/new"), 100);

        let pinned = HashSet::new();
        let removed = cache.evict_if_needed(&pinned);

        // Should have evicted "old" (oldest), bringing total to 200.
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0], PathBuf::from("/tmp/old"));
        assert!(!cache.contains("old"));
        assert!(cache.contains("mid"));
        assert!(cache.contains("new"));
        assert_eq!(cache.total_size(), 200);
    }

    #[test]
    fn touch_prevents_eviction() {
        let cache = LocalCache::new(PathBuf::from("/tmp"), 200);

        cache.register("old", PathBuf::from("/tmp/old"), 100);
        std::thread::sleep(std::time::Duration::from_millis(10));
        cache.register("mid", PathBuf::from("/tmp/mid"), 100);
        std::thread::sleep(std::time::Duration::from_millis(10));
        cache.register("new", PathBuf::from("/tmp/new"), 100);

        // Touch "old" so it's no longer the oldest.
        cache.touch("old");

        let pinned = HashSet::new();
        let removed = cache.evict_if_needed(&pinned);

        // "mid" should be evicted (now the oldest), not "old".
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0], PathBuf::from("/tmp/mid"));
        assert!(cache.contains("old"));
        assert!(cache.contains("new"));
    }

    #[test]
    fn pinned_entries_never_evicted() {
        let cache = LocalCache::new(PathBuf::from("/tmp"), 150);

        cache.register("pinned1", PathBuf::from("/tmp/p1"), 100);
        std::thread::sleep(std::time::Duration::from_millis(10));
        cache.register("unpinned", PathBuf::from("/tmp/u"), 100);

        let mut pinned = HashSet::new();
        pinned.insert("pinned1".to_string());

        let removed = cache.evict_if_needed(&pinned);

        // Only "unpinned" can be evicted.
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0], PathBuf::from("/tmp/u"));
        assert!(cache.contains("pinned1"));
    }

    #[test]
    fn eviction_disabled_when_backend_is_local() {
        // Durable local backend: even though the cache is well over its byte
        // limit, eviction must remove nothing — disk is the durable store.
        let cache = LocalCache::new_with_durability(PathBuf::from("/tmp"), 100, true);
        assert!(cache.is_durable());

        cache.register("flushed1", PathBuf::from("/tmp/flushed1"), 100);
        std::thread::sleep(std::time::Duration::from_millis(10));
        cache.register("flushed2", PathBuf::from("/tmp/flushed2"), 100);
        std::thread::sleep(std::time::Duration::from_millis(10));
        cache.register("flushed3", PathBuf::from("/tmp/flushed3"), 100);

        // 300 bytes registered against a 100-byte limit; a non-durable cache
        // would evict two entries. The durable cache evicts none.
        let removed = cache.evict_if_needed(&HashSet::new());
        assert!(removed.is_empty(), "durable backend must not evict");
        assert_eq!(cache.total_size(), 300);
        assert!(cache.contains("flushed1"));
        assert!(cache.contains("flushed2"));
        assert!(cache.contains("flushed3"));
    }

    #[test]
    fn no_eviction_when_under_limit() {
        let cache = LocalCache::new(PathBuf::from("/tmp"), 1000);
        cache.register("a", PathBuf::from("/tmp/a"), 100);
        cache.register("b", PathBuf::from("/tmp/b"), 200);

        let removed = cache.evict_if_needed(&HashSet::new());
        assert!(removed.is_empty());
        assert_eq!(cache.total_size(), 300);
    }
}
