//! Bounded LRU pool of open SSTable readers.
//!
//! The storage engine must hold memory **bounded regardless of SSTable count**.
//! Holding one resident reader per SSTable makes resident memory `O(count)`,
//! which OOM-kills a node bloated with thousands of small SSTables (see
//! `specs/todo/p0-unbounded-sstable-reader-memory-oom.md`). This pool caps the
//! number of simultaneously-resident readers: readers are opened on demand and
//! the least-recently-used **idle** reader is evicted past the cap.
//!
//! Design decisions (see `specs/proposed/p0-bounded-sstable-reader-design.md`):
//!
//! - **Engine-wide**: one pool shared by all tables, keyed by `(table, gen)`, so
//!   the bound is global rather than per-table.
//! - **Soft cap**: an in-use reader (held by an active read/merge) is never
//!   evicted. If every cached reader is in use the cap is *exceeded* and the
//!   breach is logged + metered — correctness over a hard bound. The staged
//!   read-merge keeps a single operation's working set below the cap, so
//!   breaches should be rare.
//! - **No IO under the lock**: `get_or_open` opens the SSTable *outside* the
//!   mutex (double-checked insert), so concurrent opens of distinct generations
//!   proceed in parallel instead of serializing behind one lock.
//!
//! The pool is generic over key `K` and value `V` so it is unit-testable without
//! real SSTable files; production uses `K = (TableId, gen)` and
//! `V = SSTableReader<FileReadAt>`.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// Default cap on simultaneously-resident readers. Override with
/// `FERROSA_SSTABLE_READER_CACHE_CAP`.
pub const DEFAULT_READER_CACHE_CAP: usize = 256;

/// Resolve the configured resident-reader cap (env override, sane default,
/// never zero).
pub fn configured_reader_cache_cap() -> usize {
    std::env::var("FERROSA_SSTABLE_READER_CACHE_CAP")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_READER_CACHE_CAP)
}

struct Entry<V> {
    value: Arc<V>,
    /// Logical clock stamp of the most recent access; lowest = least-recently-used.
    last_used: u64,
}

/// Bounded LRU pool of open readers. See module docs.
pub struct ReaderPool<K: Eq + Hash + Clone, V> {
    inner: Mutex<HashMap<K, Entry<V>>>,
    clock: AtomicU64,
    cap: usize,
    peak: AtomicUsize,
    soft_cap_breaches: AtomicU64,
}

impl<K: Eq + Hash + Clone, V> ReaderPool<K, V> {
    /// Create a pool capped at `cap` resident readers (clamped to ≥ 1).
    pub fn new(cap: usize) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            clock: AtomicU64::new(0),
            cap: cap.max(1),
            peak: AtomicUsize::new(0),
            soft_cap_breaches: AtomicU64::new(0),
        }
    }

    /// Configured cap for this pool.
    pub fn capacity(&self) -> usize {
        self.cap
    }

    /// Current number of resident readers.
    pub fn resident(&self) -> usize {
        self.inner.lock().expect("reader pool mutex poisoned").len()
    }

    /// High-water mark of resident readers since creation (for tests + metrics).
    pub fn peak_resident(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }

    /// Count of times the soft cap was exceeded because all cached readers were
    /// in use. Non-zero in steady state should alert.
    pub fn soft_cap_breaches(&self) -> u64 {
        self.soft_cap_breaches.load(Ordering::Relaxed)
    }

    fn tick(&self) -> u64 {
        self.clock.fetch_add(1, Ordering::Relaxed)
    }

    /// Return the cached reader for `key`, or open it via `open` and cache it.
    ///
    /// `open` is invoked **without** the pool lock held, so opens of distinct
    /// keys run concurrently. If two threads race to open the same key, both may
    /// open it but only one instance is cached; both callers receive a valid
    /// reader.
    pub fn get_or_open<F, E>(&self, key: K, open: F) -> Result<Arc<V>, E>
    where
        F: FnOnce() -> Result<V, E>,
    {
        // Fast path: already cached. Bump recency and return.
        {
            let mut guard = self.inner.lock().expect("reader pool mutex poisoned");
            if let Some(entry) = guard.get_mut(&key) {
                entry.last_used = self.tick();
                return Ok(Arc::clone(&entry.value));
            }
        }

        // Open outside the lock (may do file IO).
        let value = Arc::new(open()?);

        // Re-check under the lock: another thread may have opened it meanwhile.
        let mut guard = self.inner.lock().expect("reader pool mutex poisoned");
        if let Some(entry) = guard.get_mut(&key) {
            entry.last_used = self.tick();
            return Ok(Arc::clone(&entry.value));
        }
        let last_used = self.tick();
        guard.insert(
            key,
            Entry {
                value: Arc::clone(&value),
                last_used,
            },
        );
        self.enforce_cap(&mut guard);
        let len = guard.len();
        self.peak.fetch_max(len, Ordering::Relaxed);
        Ok(value)
    }

    /// Evict least-recently-used **idle** readers until resident ≤ cap. A reader
    /// is idle when the pool holds the only reference (`strong_count == 1`); an
    /// in-use reader is never evicted (soft cap — logged + metered).
    fn enforce_cap(&self, guard: &mut HashMap<K, Entry<V>>) {
        while guard.len() > self.cap {
            let victim = guard
                .iter()
                .filter(|(_, e)| Arc::strong_count(&e.value) == 1)
                .min_by_key(|(_, e)| e.last_used)
                .map(|(k, _)| k.clone());
            match victim {
                Some(key) => {
                    guard.remove(&key);
                }
                None => {
                    self.soft_cap_breaches.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        resident = guard.len(),
                        cap = self.cap,
                        "SSTable reader pool over soft cap: all cached readers in use"
                    );
                    break;
                }
            }
        }
    }

    /// Drop a reader from the pool — e.g. its generation was removed by a
    /// compaction/flush swap, so it must not be served or reopened. Any caller
    /// still holding an `Arc` keeps its reader alive until it finishes.
    pub fn remove(&self, key: &K) {
        self.inner
            .lock()
            .expect("reader pool mutex poisoned")
            .remove(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;

    /// Open count lets tests assert cache hits avoid reopening.
    fn counting_open(counter: &AtomicU32, value: u32) -> Result<u32, ()> {
        counter.fetch_add(1, Ordering::SeqCst);
        Ok(value)
    }

    #[test]
    fn opens_on_miss_then_serves_from_cache() {
        let pool: ReaderPool<u32, u32> = ReaderPool::new(8);
        let opens = AtomicU32::new(0);

        let a = pool.get_or_open(1, || counting_open(&opens, 100)).unwrap();
        assert_eq!(*a, 100);
        assert_eq!(opens.load(Ordering::SeqCst), 1);

        // Second get for the same key is a cache hit — open not called again,
        // and the same underlying value is returned.
        let b = pool.get_or_open(1, || counting_open(&opens, 999)).unwrap();
        assert_eq!(*b, 100);
        assert_eq!(opens.load(Ordering::SeqCst), 1, "cache hit must not reopen");
        assert!(Arc::ptr_eq(&a, &b));
    }

    #[test]
    fn evicts_lru_idle_past_cap() {
        let pool: ReaderPool<u32, u32> = ReaderPool::new(2);
        let opens = AtomicU32::new(0);

        // Insert 3 readers, dropping each returned Arc so the pool holds the
        // only reference (idle, evictable).
        for k in 1..=3u32 {
            let _ = pool
                .get_or_open(k, || counting_open(&opens, k * 10))
                .unwrap();
        }
        assert_eq!(pool.resident(), 2, "cap=2 must evict down to 2 resident");
        assert_eq!(pool.peak_resident(), 2);
        assert_eq!(pool.soft_cap_breaches(), 0);

        // Key 1 was the LRU → evicted → reopening it calls open again.
        let before = opens.load(Ordering::SeqCst);
        let _ = pool.get_or_open(1, || counting_open(&opens, 10)).unwrap();
        assert_eq!(
            opens.load(Ordering::SeqCst),
            before + 1,
            "evicted LRU key must reopen"
        );
    }

    #[test]
    fn never_evicts_in_use_readers_soft_cap() {
        let pool: ReaderPool<u32, u32> = ReaderPool::new(1);
        let opens = AtomicU32::new(0);

        // Hold both Arcs (in use) — neither can be evicted, so the pool exceeds
        // its cap of 1 rather than dropping a reader someone is using.
        let _a = pool.get_or_open(1, || counting_open(&opens, 10)).unwrap();
        let _b = pool.get_or_open(2, || counting_open(&opens, 20)).unwrap();
        assert_eq!(pool.resident(), 2, "in-use readers must not be evicted");
        assert!(
            pool.soft_cap_breaches() >= 1,
            "exceeding the cap on in-use readers must be metered"
        );
    }

    #[test]
    fn remove_drops_entry() {
        let pool: ReaderPool<u32, u32> = ReaderPool::new(8);
        let opens = AtomicU32::new(0);
        let _ = pool.get_or_open(1, || counting_open(&opens, 10)).unwrap();
        assert_eq!(pool.resident(), 1);
        pool.remove(&1);
        assert_eq!(pool.resident(), 0);
        // Re-open after removal calls open again.
        let _ = pool.get_or_open(1, || counting_open(&opens, 10)).unwrap();
        assert_eq!(opens.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn concurrent_distinct_keys_all_cache_without_deadlock() {
        let pool: Arc<ReaderPool<u32, u32>> = Arc::new(ReaderPool::new(64));
        let mut handles = Vec::new();
        for k in 0..32u32 {
            let p = Arc::clone(&pool);
            handles.push(std::thread::spawn(move || {
                let v = p.get_or_open(k, || Ok::<u32, ()>(k * 2)).unwrap();
                assert_eq!(*v, k * 2);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(pool.resident(), 32);
        assert!(pool.peak_resident() <= 64);
    }
}
