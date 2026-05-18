//! Positional I/O traits and file-system implementations.
//!
//! [`ReadAt`] and [`WriteAt`] decouple SSTable logic from the backing store.
//! This module provides file-system implementations ([`FileReadAt`] and
//! [`FileWriteAt`]) using `pread`/`pwrite` on Unix. The S3 implementation
//! lives in `ferrosa-storage`.
//!
//! # Design
//!
//! Positional I/O avoids shared file offset state, making it safe to read
//! from multiple threads without external synchronization (each call specifies
//! its offset). This matches SSTable access patterns where multiple index
//! lookups happen concurrently.

use ferrosa_common::Result;

/// Positional read — read bytes at an offset without seeking.
///
/// ```no_run
/// use ferrosa_sstable::io::ReadAt;
/// use ferrosa_common::Result;
///
/// fn read_header(reader: &impl ReadAt) -> Result<[u8; 4]> {
///     let mut buf = [0u8; 4];
///     reader.read_at(&mut buf, 0)?;
///     Ok(buf)
/// }
/// ```
pub trait ReadAt {
    /// Read bytes into `buf` starting at `offset`.
    /// Returns the number of bytes read (may be less than `buf.len()` at EOF).
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize>;

    /// Returns the total length of the underlying data.
    fn len(&self) -> Result<u64>;

    /// Returns true if the underlying data is empty.
    fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Read exactly `buf.len()` bytes at `offset`, or return an error.
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        let n = self.read_at(buf, offset)?;
        if n != buf.len() {
            return Err(ferrosa_common::Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("read_exact_at: wanted {} bytes, got {}", buf.len(), n),
            )));
        }
        Ok(())
    }
}

/// Positional write — write bytes at an offset.
pub trait WriteAt {
    /// Write `buf` starting at `offset`.
    /// Returns the number of bytes written.
    fn write_at(&mut self, buf: &[u8], offset: u64) -> Result<usize>;

    /// Flush any buffered data to the underlying store.
    fn flush(&mut self) -> Result<()>;

    /// Write all bytes in `buf` at `offset`, or return an error.
    fn write_all_at(&mut self, buf: &[u8], offset: u64) -> Result<()> {
        let n = self.write_at(buf, offset)?;
        if n != buf.len() {
            return Err(ferrosa_common::Error::Io(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                format!("write_all_at: wanted {} bytes, wrote {}", buf.len(), n),
            )));
        }
        Ok(())
    }
}

/// Bounded LRU cache of open file descriptors shared by all `FileReadAt`
/// instances that route through it. Same path => one cached `Arc<File>`,
/// reused by every read until the entry is evicted by capacity pressure
/// or by `FileReadAt::drop` (which removes its own path on the way out so
/// a compacted SSTable's inode is not kept alive by a stale fd).
///
/// `pread` is offsetful (no shared seek state), so sharing one `File` via
/// `Arc` across threads is safe — see `std::os::unix::fs::FileExt::read_at`.
///
/// Capacity is a tunable (`FERROSA_FD_CACHE_SIZE`, default 1024) chosen so a
/// node with hundreds of active SSTables fits comfortably under typical
/// `RLIMIT_NOFILE` values while still amortising opens across queries.
pub struct FdCache {
    inner: std::sync::Mutex<lru::LruCache<std::path::PathBuf, std::sync::Arc<std::fs::File>>>,
    opens: std::sync::atomic::AtomicU64,
    gets: std::sync::atomic::AtomicU64,
}

impl FdCache {
    /// Build a cache with the given capacity. Pre-allocates the LRU index.
    pub fn with_capacity(capacity: std::num::NonZeroUsize) -> Self {
        Self {
            inner: std::sync::Mutex::new(lru::LruCache::new(capacity)),
            opens: std::sync::atomic::AtomicU64::new(0),
            gets: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Number of entries currently held.
    pub fn len(&self) -> usize {
        self.inner.lock().expect("fd cache poisoned").len()
    }

    /// True when no entries are cached.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether `path` is currently cached.
    pub fn contains(&self, path: &std::path::Path) -> bool {
        // `peek` does not promote the entry — important if a caller is just
        // sampling the cache (tests, metrics) and doesn't want to perturb
        // LRU order.
        self.inner
            .lock()
            .expect("fd cache poisoned")
            .peek(path)
            .is_some()
    }

    /// Cumulative count of `File::open` calls made by this cache. A cheap
    /// proxy for cache-miss rate: misses go up by one per open, hits do
    /// not advance it.
    pub fn opens_observed(&self) -> u64 {
        self.opens.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Cumulative count of `get_or_open` calls — i.e. cache lookups that
    /// took the global mutex. A `FileReadAt`'s per-reader fast path bypasses
    /// the cache after its first successful read, so a healthy production
    /// pattern is `gets_observed << pread count`.
    pub fn gets_observed(&self) -> u64 {
        self.gets.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Drop `path`'s cached fd, if any. Idempotent.
    fn invalidate(&self, path: &std::path::Path) {
        let _ = self.inner.lock().expect("fd cache poisoned").pop(path);
    }

    #[doc(hidden)]
    pub fn invalidate_for_test(&self, path: &std::path::Path) {
        self.invalidate(path);
    }

    /// Obtain a shared handle to `path`, opening + inserting on miss.
    ///
    /// On a hit the entry is promoted to most-recently-used. On a miss the
    /// lock is dropped around the `open()` syscall so concurrent misses
    /// do not serialise on the cache mutex; a re-check on reinsert keeps
    /// the cache single-valued per path (a racing thread's `Arc<File>` is
    /// dropped if it lost the race, closing its fd).
    fn get_or_open(&self, path: &std::path::Path) -> Result<std::sync::Arc<std::fs::File>> {
        self.gets.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        {
            let mut guard = self.inner.lock().expect("fd cache poisoned");
            if let Some(f) = guard.get(path) {
                return Ok(std::sync::Arc::clone(f));
            }
        }
        let file = std::sync::Arc::new(std::fs::File::open(path)?);
        self.opens
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut guard = self.inner.lock().expect("fd cache poisoned");
        if let Some(f) = guard.get(path) {
            return Ok(std::sync::Arc::clone(f));
        }
        guard.put(path.to_path_buf(), std::sync::Arc::clone(&file));
        Ok(file)
    }
}

const DEFAULT_FD_CACHE_CAPACITY: usize = 1024;

fn global_fd_cache() -> &'static std::sync::Arc<FdCache> {
    static CACHE: std::sync::OnceLock<std::sync::Arc<FdCache>> = std::sync::OnceLock::new();
    CACHE.get_or_init(|| {
        let cap = std::env::var("FERROSA_FD_CACHE_SIZE")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .and_then(std::num::NonZeroUsize::new)
            .unwrap_or_else(|| {
                std::num::NonZeroUsize::new(DEFAULT_FD_CACHE_CAPACITY)
                    .expect("DEFAULT_FD_CACHE_CAPACITY is non-zero")
            });
        std::sync::Arc::new(FdCache::with_capacity(cap))
    })
}

/// File-system implementation of [`ReadAt`] using `pread` on Unix.
///
/// Backed by a per-process LRU [`FdCache`]: idle readers hold no fd, the
/// first read opens + caches the descriptor, subsequent reads to the same
/// path reuse the cached handle. Replaces the previous reopen-per-pread
/// pattern (see `bug-streaming-range-read-perf-50x-floor.md`).
pub struct FileReadAt {
    path: std::path::PathBuf,
    cache: std::sync::Arc<FdCache>,
    /// Per-reader cached handle. Populated on first successful `read_at`;
    /// subsequent reads use it directly and skip the global cache (no mutex,
    /// no `PathBuf` hash — both showed up as the dominant cost after the
    /// LRU cache landed). Holds an `Arc<File>` so the handle outlives any
    /// LRU eviction of the same path from the global cache.
    handle: std::sync::OnceLock<std::sync::Arc<std::fs::File>>,
}

impl FileReadAt {
    /// Open a file for positional reading via the process-wide LRU fd cache.
    /// Validates the path is readable; does not retain a descriptor.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self> {
        Self::open_with_cache(path, std::sync::Arc::clone(global_fd_cache()))
    }

    /// Open against an explicit cache — used by tests so capacity and
    /// eviction can be observed in isolation from the global cache.
    pub fn open_with_cache(
        path: impl AsRef<std::path::Path>,
        cache: std::sync::Arc<FdCache>,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        std::fs::File::open(&path)?;
        Ok(Self {
            path,
            cache,
            handle: std::sync::OnceLock::new(),
        })
    }

    /// Whether this reader has already cached its file handle locally
    /// (true after the first successful `read_at`).
    pub fn is_handle_cached(&self) -> bool {
        self.handle.get().is_some()
    }
}

impl Drop for FileReadAt {
    fn drop(&mut self) {
        // Evict on drop so a compacted/deleted SSTable's inode is released
        // promptly. FileReadAt is single-owner (never cloned), so the path
        // is unique to this reader at drop time.
        self.cache.invalidate(&self.path);
    }
}

impl ReadAt for FileReadAt {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        // Fast path: local handle is set, skip the global cache entirely.
        // Slow path (first read, or local handle missing): consult the
        // global LRU; populate the local handle so subsequent reads stay fast.
        let file: &std::fs::File = if let Some(f) = self.handle.get() {
            f
        } else {
            let from_cache = self.cache.get_or_open(&self.path)?;
            // `set` may race with another thread on the same FileReadAt
            // (rare: FileReadAt is single-owner in practice). Either Arc
            // points at a valid open file for the same path; the losing
            // Arc drops, releasing its surplus reference.
            let _ = self.handle.set(from_cache);
            self.handle.get().expect("OnceLock just initialized")
        };
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            Ok(file.read_at(buf, offset)?)
        }
        #[cfg(not(unix))]
        {
            // pread is unix-only; on non-unix fall back to a per-call clone
            // + seek so concurrent reads do not corrupt a shared offset.
            use std::io::{Read, Seek, SeekFrom};
            let mut file = file.try_clone()?;
            file.seek(SeekFrom::Start(offset))?;
            Ok(file.read(buf)?)
        }
    }

    fn len(&self) -> Result<u64> {
        Ok(std::fs::metadata(&self.path)?.len())
    }
}

/// File-system implementation of [`WriteAt`] using `pwrite` on Unix.
pub struct FileWriteAt {
    file: std::fs::File,
}

impl FileWriteAt {
    /// Create a new file for positional writing.
    pub fn create(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let file = std::fs::File::create(path)?;
        Ok(Self { file })
    }
}

impl WriteAt for FileWriteAt {
    fn write_at(&mut self, buf: &[u8], offset: u64) -> Result<usize> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            Ok(self.file.write_at(buf, offset)?)
        }
        #[cfg(not(unix))]
        {
            use std::io::{Seek, SeekFrom, Write};
            self.file.seek(SeekFrom::Start(offset))?;
            Ok(self.file.write(buf)?)
        }
    }

    fn flush(&mut self) -> Result<()> {
        use std::io::Write;
        Ok(self.file.flush()?)
    }
}

/// In-memory implementation of [`ReadAt`] for testing.
impl ReadAt for &[u8] {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let offset = offset as usize;
        let slice_len = <[u8]>::len(self);
        if offset >= slice_len {
            return Ok(0);
        }
        let available = &self[offset..];
        let n = buf.len().min(available.len());
        buf[..n].copy_from_slice(&available[..n]);
        Ok(n)
    }

    fn len(&self) -> Result<u64> {
        Ok(<[u8]>::len(self) as u64)
    }
}

/// In-memory implementation of [`ReadAt`] for testing.
impl ReadAt for Vec<u8> {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        self.as_slice().read_at(buf, offset)
    }

    fn len(&self) -> Result<u64> {
        Ok(Vec::len(self) as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slice_read_at_basic() {
        let data: &[u8] = b"hello world";
        let mut buf = [0u8; 5];
        let n = data.read_at(&mut buf, 0).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"hello");
    }

    #[test]
    fn slice_read_at_offset() {
        let data: &[u8] = b"hello world";
        let mut buf = [0u8; 5];
        let n = data.read_at(&mut buf, 6).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"world");
    }

    #[test]
    fn slice_read_at_past_eof() {
        let data: &[u8] = b"hi";
        let mut buf = [0u8; 5];
        let n = data.read_at(&mut buf, 100).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn slice_read_at_partial() {
        let data: &[u8] = b"hi";
        let mut buf = [0u8; 5];
        let n = data.read_at(&mut buf, 0).unwrap();
        assert_eq!(n, 2);
        assert_eq!(&buf[..2], b"hi");
    }

    #[test]
    fn slice_len() {
        let data: &[u8] = b"hello";
        assert_eq!(ReadAt::len(&data).unwrap(), 5);
    }

    #[test]
    fn slice_is_empty() {
        let empty: &[u8] = b"";
        assert!(ReadAt::is_empty(&empty).unwrap());
        let nonempty: &[u8] = b"x";
        assert!(!ReadAt::is_empty(&nonempty).unwrap());
    }

    #[test]
    fn read_exact_at_success() {
        let data: &[u8] = b"hello";
        let mut buf = [0u8; 5];
        data.read_exact_at(&mut buf, 0).unwrap();
        assert_eq!(&buf, b"hello");
    }

    #[test]
    fn read_exact_at_eof_error() {
        let data: &[u8] = b"hi";
        let mut buf = [0u8; 5];
        let err = data.read_exact_at(&mut buf, 0).unwrap_err();
        assert!(err.to_string().contains("wanted 5 bytes, got 2"));
    }

    #[test]
    fn file_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.dat");

        let data = b"hello world from ferrosa";
        {
            let mut writer = FileWriteAt::create(&path).unwrap();
            writer.write_all_at(data, 0).unwrap();
            writer.flush().unwrap();
        }

        let reader = FileReadAt::open(&path).unwrap();
        assert_eq!(reader.len().unwrap(), data.len() as u64);

        let mut buf = vec![0u8; data.len()];
        reader.read_exact_at(&mut buf, 0).unwrap();
        assert_eq!(buf, data);

        // Partial read at offset
        let mut buf2 = [0u8; 5];
        reader.read_exact_at(&mut buf2, 6).unwrap();
        assert_eq!(&buf2, b"world");
    }

    #[test]
    fn file_write_at_offset() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("offset.dat");

        let mut writer = FileWriteAt::create(&path).unwrap();
        // Write "hello" at offset 0
        writer.write_all_at(b"hello", 0).unwrap();
        // Write "world" at offset 10
        writer.write_all_at(b"world", 10).unwrap();
        writer.flush().unwrap();

        let reader = FileReadAt::open(&path).unwrap();
        assert_eq!(reader.len().unwrap(), 15);

        let mut buf = [0u8; 5];
        reader.read_exact_at(&mut buf, 0).unwrap();
        assert_eq!(&buf, b"hello");

        reader.read_exact_at(&mut buf, 10).unwrap();
        assert_eq!(&buf, b"world");
    }

    // ---- LRU fd-cache contracts ----
    //
    // The previous contract ("idle FileReadAt holds no fd AND each read closes
    // its transient descriptor immediately") was reopen-per-pread. Profiling
    // (`specs/in-process/bug-streaming-range-read-perf-50x-floor.md`) showed
    // this dominated streaming range-read cost — ~10 reopen syscalls per
    // partition body decode. The new contract:
    //
    //   1. Constructing a `FileReadAt` does not open an fd (idle = no fd).
    //   2. First `read_at` opens the fd and inserts it into a tunable LRU
    //      cache keyed by path. Subsequent reads to the same path reuse it.
    //   3. When capacity is exceeded the LRU entry is evicted; its fd closes
    //      when the last `Arc<File>` drops.
    //   4. Dropping a `FileReadAt` evicts its entry — so when compaction
    //      removes an SSTable on disk, the cache does not pin its inode.

    use super::{FdCache, FileReadAt, ReadAt, WriteAt};
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    #[test]
    fn open_does_not_populate_cache() {
        let cache = Arc::new(FdCache::with_capacity(NonZeroUsize::new(8).unwrap()));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("idle.db");
        std::fs::write(&path, b"abcd").unwrap();

        let _reader = FileReadAt::open_with_cache(&path, Arc::clone(&cache)).unwrap();

        assert_eq!(
            cache.len(),
            0,
            "constructing FileReadAt must not insert into the fd cache"
        );
    }

    #[test]
    fn first_read_populates_cache_and_subsequent_reads_reuse_it() {
        let cache = Arc::new(FdCache::with_capacity(NonZeroUsize::new(8).unwrap()));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("reuse.db");
        std::fs::write(&path, b"abcdefghij").unwrap();

        let reader = FileReadAt::open_with_cache(&path, Arc::clone(&cache)).unwrap();
        let opens_before = cache.opens_observed();

        for offset in 0..8 {
            let mut buf = [0u8; 2];
            reader.read_exact_at(&mut buf, offset).unwrap();
        }

        let opens_after = cache.opens_observed();
        assert_eq!(cache.len(), 1, "single path => single cache entry");
        assert!(cache.contains(&path));
        assert_eq!(
            opens_after - opens_before,
            1,
            "the underlying file must be opened exactly once across N reads"
        );
    }

    #[test]
    fn many_readers_same_path_share_one_fd() {
        let cache = Arc::new(FdCache::with_capacity(NonZeroUsize::new(8).unwrap()));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shared.db");
        std::fs::write(&path, b"ferrosa shared component").unwrap();
        let readers: Vec<_> = (0..32)
            .map(|_| FileReadAt::open_with_cache(&path, Arc::clone(&cache)).unwrap())
            .collect();
        let opens_before = cache.opens_observed();

        for r in &readers {
            let mut buf = [0u8; 7];
            r.read_exact_at(&mut buf, 0).unwrap();
            assert_eq!(&buf, b"ferrosa");
        }

        assert_eq!(cache.len(), 1, "all 32 readers must collapse to one entry");
        assert_eq!(
            cache.opens_observed() - opens_before,
            1,
            "underlying open() must be called exactly once across all readers"
        );
    }

    #[test]
    fn lru_evicts_least_recently_used_when_capacity_exceeded() {
        let cache = Arc::new(FdCache::with_capacity(NonZeroUsize::new(3).unwrap()));
        let dir = tempfile::tempdir().unwrap();

        let mut entries = Vec::new();
        for i in 0..4 {
            let p = dir.path().join(format!("file-{i}.db"));
            std::fs::write(&p, b"data").unwrap();
            let r = FileReadAt::open_with_cache(&p, Arc::clone(&cache)).unwrap();
            let mut buf = [0u8; 2];
            r.read_exact_at(&mut buf, 0).unwrap();
            entries.push((p, r));
        }

        assert_eq!(cache.len(), 3, "cache must not exceed capacity");
        assert!(
            !cache.contains(&entries[0].0),
            "oldest entry must be evicted: {:?}",
            entries[0].0
        );
        for (p, _) in &entries[1..] {
            assert!(cache.contains(p), "recent entry missing: {p:?}");
        }
    }

    // ---- Per-reader local handle fast-path contracts ----
    //
    // Profiling the LRU fd cache showed `Path::hash` at 5.59% CPU and
    // `LruCache::get` at 0.59% per read — every read takes the cache
    // mutex and hashes the full PathBuf. After the first read, a
    // `FileReadAt` should fast-path subsequent reads through a local
    // `Arc<File>` and skip the global cache entirely.

    #[test]
    fn second_read_does_not_touch_global_cache() {
        let cache = Arc::new(FdCache::with_capacity(NonZeroUsize::new(8).unwrap()));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fast.db");
        std::fs::write(&path, b"abcdefgh").unwrap();
        let reader = FileReadAt::open_with_cache(&path, Arc::clone(&cache)).unwrap();

        let mut buf = [0u8; 4];
        reader.read_exact_at(&mut buf, 0).unwrap();
        let gets_after_first = cache.gets_observed();
        for offset in 0..4 {
            reader.read_exact_at(&mut buf, offset).unwrap();
        }
        let gets_after_many = cache.gets_observed();

        assert_eq!(
            gets_after_many - gets_after_first,
            0,
            "subsequent reads must hit the per-reader local handle, never the global cache"
        );
    }

    #[test]
    fn handle_is_cached_locally_after_first_read() {
        let cache = Arc::new(FdCache::with_capacity(NonZeroUsize::new(8).unwrap()));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("local.db");
        std::fs::write(&path, b"abcd").unwrap();
        let reader = FileReadAt::open_with_cache(&path, Arc::clone(&cache)).unwrap();

        assert!(!reader.is_handle_cached(), "no local fd before first read");
        let mut buf = [0u8; 2];
        reader.read_exact_at(&mut buf, 0).unwrap();
        assert!(
            reader.is_handle_cached(),
            "first read must populate the local handle"
        );
    }

    #[test]
    fn local_handle_survives_global_cache_eviction() {
        let cache = Arc::new(FdCache::with_capacity(NonZeroUsize::new(2).unwrap()));
        let dir = tempfile::tempdir().unwrap();

        let pinned_path = dir.path().join("pinned.db");
        std::fs::write(&pinned_path, b"persist").unwrap();
        let pinned = FileReadAt::open_with_cache(&pinned_path, Arc::clone(&cache)).unwrap();
        let mut buf = [0u8; 7];
        pinned.read_exact_at(&mut buf, 0).unwrap();
        assert_eq!(&buf, b"persist");

        // Push other paths through the cache until `pinned_path` is evicted.
        // Hold the readers in a Vec so their Drop does not invalidate their
        // own entries — we want capacity pressure to do the eviction work.
        let mut victims = Vec::new();
        for i in 0..5 {
            let p = dir.path().join(format!("victim-{i}.db"));
            std::fs::write(&p, b"victim!").unwrap();
            let r = FileReadAt::open_with_cache(&p, Arc::clone(&cache)).unwrap();
            let mut b = [0u8; 7];
            r.read_exact_at(&mut b, 0).unwrap();
            victims.push(r);
        }
        assert!(
            !cache.contains(&pinned_path),
            "pinned path should have been evicted by LRU pressure"
        );

        // The pinned reader must still read fine via its locally cached handle.
        let gets_before = cache.gets_observed();
        let opens_before = cache.opens_observed();
        let mut buf = [0u8; 7];
        pinned.read_exact_at(&mut buf, 0).unwrap();
        assert_eq!(&buf, b"persist");
        assert_eq!(
            cache.gets_observed() - gets_before,
            0,
            "fast-path read must not touch global cache after eviction"
        );
        assert_eq!(
            cache.opens_observed() - opens_before,
            0,
            "fast-path read must not reopen the file"
        );
    }

    #[test]
    fn cross_reader_first_reads_still_share_via_global_cache() {
        let cache = Arc::new(FdCache::with_capacity(NonZeroUsize::new(8).unwrap()));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shared-cross.db");
        std::fs::write(&path, b"abcdef").unwrap();
        let reader1 = FileReadAt::open_with_cache(&path, Arc::clone(&cache)).unwrap();
        let reader2 = FileReadAt::open_with_cache(&path, Arc::clone(&cache)).unwrap();
        let opens_before = cache.opens_observed();

        let mut buf = [0u8; 3];
        reader1.read_exact_at(&mut buf, 0).unwrap();
        reader2.read_exact_at(&mut buf, 0).unwrap();

        assert_eq!(
            cache.opens_observed() - opens_before,
            1,
            "second reader's first read must hit the global cache and share the fd"
        );
    }

    #[test]
    fn dropping_filereadat_evicts_cache_entry() {
        let cache = Arc::new(FdCache::with_capacity(NonZeroUsize::new(8).unwrap()));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("drop.db");
        std::fs::write(&path, b"xyz").unwrap();

        let reader = FileReadAt::open_with_cache(&path, Arc::clone(&cache)).unwrap();
        let mut buf = [0u8; 2];
        reader.read_exact_at(&mut buf, 0).unwrap();
        assert!(cache.contains(&path));

        drop(reader);

        assert!(
            !cache.contains(&path),
            "Drop must evict the entry so a deleted SSTable's inode does not stay pinned"
        );
    }

    #[test]
    fn read_returns_error_when_underlying_file_is_unreadable() {
        let cache = Arc::new(FdCache::with_capacity(NonZeroUsize::new(4).unwrap()));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing.db");
        // Validation at open time still requires the file to exist; first read
        // populates the cache from the same path.
        std::fs::write(&path, b"present").unwrap();
        let reader = FileReadAt::open_with_cache(&path, Arc::clone(&cache)).unwrap();

        // Remove the file *and* drop the FileReadAt's cache entry, so the next
        // read has to reopen. The reopen must fail loudly.
        std::fs::remove_file(&path).unwrap();
        cache.invalidate_for_test(&path);

        let mut buf = [0u8; 4];
        let err = reader.read_exact_at(&mut buf, 0).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("No such file") || msg.contains("missing.db") || msg.contains("ENOENT"),
            "expected open-error surfaced from read_at, got: {msg}"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn idle_filereadat_holds_no_process_fd() {
        fn open_fd_count() -> usize {
            std::fs::read_dir("/proc/self/fd").unwrap().count()
        }

        let cache = Arc::new(FdCache::with_capacity(NonZeroUsize::new(64).unwrap()));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("idle-proc-fd.db");
        std::fs::write(&path, b"ferrosa idle bytes").unwrap();

        let baseline = open_fd_count();
        let readers: Vec<_> = (0..256)
            .map(|_| FileReadAt::open_with_cache(&path, Arc::clone(&cache)).unwrap())
            .collect();
        let after_open = open_fd_count();

        assert!(
            after_open <= baseline + 8,
            "constructing FileReadAt must not pin fds: baseline={baseline}, after_open={after_open}"
        );
        drop(readers);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn active_same_path_readers_pin_at_most_one_fd() {
        fn open_fd_count() -> usize {
            std::fs::read_dir("/proc/self/fd").unwrap().count()
        }

        let cache = Arc::new(FdCache::with_capacity(NonZeroUsize::new(64).unwrap()));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("active-proc-fd.db");
        std::fs::write(&path, b"ferrosa active bytes").unwrap();

        let baseline = open_fd_count();
        let readers: Vec<_> = (0..256)
            .map(|_| FileReadAt::open_with_cache(&path, Arc::clone(&cache)).unwrap())
            .collect();
        for r in &readers {
            let mut buf = [0u8; 7];
            r.read_exact_at(&mut buf, 0).unwrap();
            assert_eq!(&buf, b"ferrosa");
        }
        let after_reads = open_fd_count();

        assert!(
            after_reads <= baseline + 8,
            "same-path readers must share one cache entry, not 256 fds: baseline={baseline}, after_reads={after_reads}"
        );
    }
}
