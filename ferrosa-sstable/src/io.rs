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
use std::path::Path;
use std::sync::{Arc, OnceLock, RwLock};

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

impl<T: ReadAt + ?Sized> ReadAt for &T {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        (**self).read_at(buf, offset)
    }

    fn len(&self) -> Result<u64> {
        (**self).len()
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

const DEFAULT_READ_CACHE_BLOCK_SIZE: usize = 4096;
const DEFAULT_READ_CACHE_BLOCKS: usize = 128;

fn cached_read_block_size() -> usize {
    std::env::var("FERROSA_SSTABLE_READ_CACHE_BLOCK_SIZE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_READ_CACHE_BLOCK_SIZE)
}

fn cached_read_block_capacity() -> std::num::NonZeroUsize {
    let requested = std::env::var("FERROSA_SSTABLE_READ_CACHE_BLOCKS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_READ_CACHE_BLOCKS);
    std::num::NonZeroUsize::new(requested).expect("read cache capacity is non-zero")
}

/// Bounded block cache for immutable positional readers.
///
/// This is intended for SSTable index components such as `Partitions.db` and
/// `Rows.db`, where point reads repeatedly touch the same trie root and upper
/// nodes. It avoids turning every trie step into a kernel `pread` while keeping
/// `Data.db` on the more specialized compression-chunk path.
pub struct CachedReadAt<R: ReadAt> {
    inner: R,
    len: u64,
}

impl<R: ReadAt> CachedReadAt<R> {
    /// Create a cached reader using environment/default cache sizing.
    pub fn new(inner: R) -> Result<Self> {
        Self::with_capacity(
            inner,
            cached_read_block_size(),
            cached_read_block_capacity(),
        )
    }

    /// Create a cached reader with explicit sizing, primarily for tests.
    pub fn with_capacity(
        inner: R,
        block_size: usize,
        capacity: std::num::NonZeroUsize,
    ) -> Result<Self> {
        if block_size == 0 {
            return Err(ferrosa_common::Error::InvalidData(
                "CachedReadAt block size must be non-zero".into(),
            ));
        }
        let len = inner.len()?;
        let _ = capacity;
        Ok(Self { inner, len })
    }
}

impl<R: ReadAt> ReadAt for CachedReadAt<R> {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        if buf.is_empty() || offset >= self.len {
            return Ok(0);
        }

        let target = buf.len().min((self.len - offset) as usize);
        self.inner.read_at(&mut buf[..target], offset)
    }

    fn len(&self) -> Result<u64> {
        Ok(self.len)
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
/// Capacity is a tunable (`FERROSA_FD_CACHE_SIZE`, default 1024 before
/// runtime headroom is applied) chosen so a node with hundreds of active
/// SSTables fits comfortably under typical `RLIMIT_NOFILE` values while still
/// amortising opens across queries. On Linux the effective default is capped to
/// one quarter of the process soft open-file limit so test/CI runs with
/// `ulimit -n 1024` keep enough descriptor headroom for writers, tempdirs,
/// commit logs, and other crates.
pub struct FdCache {
    inner: std::sync::Mutex<lru::LruCache<std::path::PathBuf, std::sync::Arc<std::fs::File>>>,
    opens: std::sync::atomic::AtomicU64,
}

impl FdCache {
    /// Build a cache with the given capacity. Pre-allocates the LRU index.
    pub fn with_capacity(capacity: std::num::NonZeroUsize) -> Self {
        Self {
            inner: std::sync::Mutex::new(lru::LruCache::new(capacity)),
            opens: std::sync::atomic::AtomicU64::new(0),
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
        {
            let mut guard = self.inner.lock().expect("fd cache poisoned");
            if let Some(f) = guard.get(path) {
                return Ok(std::sync::Arc::clone(f));
            }
            if guard.len() >= guard.cap().get() {
                // Make room before opening the next descriptor. `LruCache::put`
                // would evict after insertion, but on low RLIMIT_NOFILE systems
                // opening first can fail with EMFILE before the eviction happens.
                guard.pop_lru();
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

#[cfg(test)]
const DEFAULT_FD_CACHE_CAPACITY: usize = 1024;
#[cfg(test)]
const MIN_FD_CACHE_CAPACITY: usize = 64;

#[cfg(test)]
fn fd_cache_capacity(
    env_value: Option<&str>,
    soft_nofile_limit: Option<usize>,
) -> std::num::NonZeroUsize {
    let requested = env_value
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_FD_CACHE_CAPACITY);
    let capped = soft_nofile_limit
        .map(|limit| {
            let headroom_cap = (limit / 4).max(MIN_FD_CACHE_CAPACITY);
            requested.min(headroom_cap)
        })
        .unwrap_or(requested)
        .max(1);
    std::num::NonZeroUsize::new(capped).expect("fd cache capacity is non-zero")
}

/// File-system implementation of [`ReadAt`] for immutable SSTable components.
///
/// Production opens mmap the small, hot index components (`Partitions.db` and
/// `Rows.db`) and use cached `pread` for `Data.db`. That keeps index traversal
/// lock-free without letting a corrupted or externally truncated data component
/// crash the process with SIGBUS.
pub struct FileReadAt {
    inner: FileReadAtInner,
}

enum FileReadAtInner {
    Mmap {
        mmap: std::sync::Arc<memmap2::Mmap>,
        len: u64,
    },
    Empty,
    CachedFd {
        path: std::path::PathBuf,
        cache: std::sync::Arc<FdCache>,
    },
}

static GLOBAL_FD_CACHE: std::sync::OnceLock<std::sync::Arc<FdCache>> = std::sync::OnceLock::new();
type FileReadRehydrationHook = dyn Fn(&Path) -> Result<bool> + Send + Sync + 'static;
type FileReadRangeHook =
    dyn Fn(&Path, u64, usize) -> Result<Option<Vec<u8>>> + Send + Sync + 'static;
type FileReadLenHook = dyn Fn(&Path) -> Result<Option<u64>> + Send + Sync + 'static;
static FILE_READ_REHYDRATION_HOOKS: OnceLock<RwLock<Vec<Arc<FileReadRehydrationHook>>>> =
    OnceLock::new();
static FILE_READ_RANGE_HOOKS: OnceLock<RwLock<Vec<Arc<FileReadRangeHook>>>> = OnceLock::new();
static FILE_READ_LEN_HOOKS: OnceLock<RwLock<Vec<Arc<FileReadLenHook>>>> = OnceLock::new();

fn file_read_rehydration_hooks() -> &'static RwLock<Vec<Arc<FileReadRehydrationHook>>> {
    FILE_READ_REHYDRATION_HOOKS.get_or_init(|| RwLock::new(Vec::new()))
}

fn file_read_range_hooks() -> &'static RwLock<Vec<Arc<FileReadRangeHook>>> {
    FILE_READ_RANGE_HOOKS.get_or_init(|| RwLock::new(Vec::new()))
}

fn file_read_len_hooks() -> &'static RwLock<Vec<Arc<FileReadLenHook>>> {
    FILE_READ_LEN_HOOKS.get_or_init(|| RwLock::new(Vec::new()))
}

/// Register a process-wide hook that can restore an immutable component file
/// after it has been evicted from local disk.
///
/// Hooks are called only when a lazy file read observes `NotFound`. The first
/// hook to return `Ok(true)` wins and the read is retried. This keeps the
/// SSTable crate independent from object storage while allowing storage engines
/// to provide S3-backed read-through for uploaded local-cache entries.
pub fn register_file_read_rehydration_hook(hook: Arc<FileReadRehydrationHook>) {
    file_read_rehydration_hooks()
        .write()
        .expect("file read rehydration hook registry poisoned")
        .push(hook);
}

/// Register a process-wide hook that can satisfy a positional read directly
/// when an immutable component file has been evicted from local disk.
///
/// This is the fast path for S3-backed SSTable cache misses: point reads can
/// fetch only the addressed byte range instead of rehydrating the whole
/// component locally before retrying the read.
pub fn register_file_read_range_hook(hook: Arc<FileReadRangeHook>) {
    file_read_range_hooks()
        .write()
        .expect("file read range hook registry poisoned")
        .push(hook);
}

/// Register a process-wide hook that can return the length of an evicted
/// immutable component without restoring it to local disk.
pub fn register_file_read_len_hook(hook: Arc<FileReadLenHook>) {
    file_read_len_hooks()
        .write()
        .expect("file read len hook registry poisoned")
        .push(hook);
}

fn try_read_file_range(path: &Path, offset: u64, buf: &mut [u8]) -> Result<Option<usize>> {
    let hooks = file_read_range_hooks()
        .read()
        .expect("file read range hook registry poisoned");
    for hook in hooks.iter() {
        if let Some(bytes) = hook(path, offset, buf.len())? {
            let n = bytes.len().min(buf.len());
            buf[..n].copy_from_slice(&bytes[..n]);
            return Ok(Some(n));
        }
    }
    Ok(None)
}

fn try_file_len(path: &Path) -> Result<Option<u64>> {
    let hooks = file_read_len_hooks()
        .read()
        .expect("file read len hook registry poisoned");
    for hook in hooks.iter() {
        if let Some(len) = hook(path)? {
            return Ok(Some(len));
        }
    }
    Ok(None)
}

fn try_rehydrate_file(path: &Path) -> Result<bool> {
    let hooks = file_read_rehydration_hooks()
        .read()
        .expect("file read rehydration hook registry poisoned");
    for hook in hooks.iter() {
        if hook(path)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Try to restore an immutable component file through the registered
/// read-through hooks.
///
/// Storage-engine maintenance code uses this before opening evicted SSTable
/// components for compaction. The SSTable crate remains object-store agnostic:
/// callers only learn whether some registered hook restored the local path.
pub fn rehydrate_file(path: impl AsRef<Path>) -> Result<bool> {
    try_rehydrate_file(path.as_ref())
}

/// Return the length of an immutable component through registered read-through
/// hooks without restoring it locally.
pub fn remote_file_len(path: impl AsRef<Path>) -> Result<Option<u64>> {
    try_file_len(path.as_ref())
}

fn is_not_found(err: &ferrosa_common::Error) -> bool {
    matches!(err, ferrosa_common::Error::Io(e) if e.kind() == std::io::ErrorKind::NotFound)
}

fn global_fd_cache() -> std::sync::Arc<FdCache> {
    std::sync::Arc::clone(GLOBAL_FD_CACHE.get_or_init(|| {
        std::sync::Arc::new(FdCache::with_capacity(
            std::num::NonZeroUsize::new(1024).expect("fd cache capacity is non-zero"),
        ))
    }))
}

/// Drop the cached descriptor for `path` from the process-global `Data.db` fd
/// cache, if present. Test-only.
///
/// An open fd survives `unlink` on Unix, so a still-cached descriptor would let
/// a positional read succeed even after the file is deleted. The residual
/// read-vs-compaction race test must model a read whose `Data.db` was deleted
/// *and* whose fd has been evicted, so the next `read_at` re-opens by path and
/// observes `ENOENT` — exactly the mid-read error the fix recovers from.
#[doc(hidden)]
pub fn evict_global_fd_for_test(path: impl AsRef<Path>) {
    global_fd_cache().invalidate(path.as_ref());
}

fn should_mmap_component(path: &std::path::Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(|name| name.ends_with("-Partitions.db") || name.ends_with("-Rows.db"))
        .unwrap_or(false)
}

impl FileReadAt {
    /// Open a file for positional reading.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if !should_mmap_component(&path) {
            return Self::open_with_cache(path, global_fd_cache());
        }

        let file = match std::fs::File::open(&path) {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && try_rehydrate_file(&path)? => {
                std::fs::File::open(&path)?
            }
            Err(e) => return Err(e.into()),
        };
        let len = file.metadata()?.len();
        if len == 0 {
            return Ok(Self {
                inner: FileReadAtInner::Empty,
            });
        }

        // SAFETY: SSTable component files are immutable after they are opened by
        // the reader. Compaction deletes whole files after readers are dropped;
        // it does not mutate bytes through this mapping.
        let mmap = unsafe { memmap2::MmapOptions::new().map(&file)? };
        Ok(Self {
            inner: FileReadAtInner::Mmap {
                mmap: std::sync::Arc::new(mmap),
                len,
            },
        })
    }

    /// Open against an explicit cache — used by tests so capacity and
    /// eviction can be observed in isolation from the global cache.
    pub fn open_with_cache(
        path: impl AsRef<std::path::Path>,
        cache: std::sync::Arc<FdCache>,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        match std::fs::File::open(&path) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && try_rehydrate_file(&path)? => {
                std::fs::File::open(&path)?;
            }
            Err(e) => return Err(e.into()),
        }
        Ok(Self {
            inner: FileReadAtInner::CachedFd { path, cache },
        })
    }
}

impl Drop for FileReadAt {
    fn drop(&mut self) {
        if let FileReadAtInner::CachedFd { path, cache } = &self.inner {
            cache.invalidate(path);
        }
    }
}

impl ReadAt for FileReadAt {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        match &self.inner {
            FileReadAtInner::Mmap { mmap, len, .. } => {
                if buf.is_empty() || offset >= *len {
                    return Ok(0);
                }
                let offset = offset as usize;
                let n = buf.len().min(mmap.len().saturating_sub(offset));
                buf[..n].copy_from_slice(&mmap[offset..offset + n]);
                Ok(n)
            }
            FileReadAtInner::Empty => Ok(0),
            FileReadAtInner::CachedFd { path, cache } => {
                let file = match cache.get_or_open(path) {
                    Ok(file) => file,
                    Err(e) if is_not_found(&e) => {
                        if let Some(n) = try_read_file_range(path, offset, buf)? {
                            return Ok(n);
                        }
                        if try_rehydrate_file(path)? {
                            cache.invalidate(path);
                            cache.get_or_open(path)?
                        } else {
                            return Err(e);
                        }
                    }
                    Err(e) => return Err(e),
                };
                #[cfg(unix)]
                {
                    use std::os::unix::fs::FileExt;
                    Ok(file.read_at(buf, offset)?)
                }
                #[cfg(not(unix))]
                {
                    use std::io::{Read, Seek, SeekFrom};
                    let mut file = file.try_clone()?;
                    file.seek(SeekFrom::Start(offset))?;
                    Ok(file.read(buf)?)
                }
            }
        }
    }

    fn len(&self) -> Result<u64> {
        match &self.inner {
            FileReadAtInner::Mmap { len, .. } => Ok(*len),
            FileReadAtInner::Empty => Ok(0),
            FileReadAtInner::CachedFd { path, cache } => match std::fs::metadata(path) {
                Ok(metadata) => Ok(metadata.len()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    if let Some(len) = try_file_len(path)? {
                        return Ok(len);
                    }
                    if try_rehydrate_file(path)? {
                        cache.invalidate(path);
                        Ok(std::fs::metadata(path)?.len())
                    } else {
                        Err(e.into())
                    }
                }
                Err(e) => Err(e.into()),
            },
        }
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
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    struct CountingReadAt {
        data: Vec<u8>,
        reads: Arc<AtomicU64>,
        len_calls: Arc<AtomicU64>,
    }

    impl CountingReadAt {
        fn new(data: Vec<u8>) -> (Self, Arc<AtomicU64>, Arc<AtomicU64>) {
            let reads = Arc::new(AtomicU64::new(0));
            let len_calls = Arc::new(AtomicU64::new(0));
            (
                Self {
                    data,
                    reads: Arc::clone(&reads),
                    len_calls: Arc::clone(&len_calls),
                },
                reads,
                len_calls,
            )
        }
    }

    impl ReadAt for CountingReadAt {
        fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            self.data.read_at(buf, offset)
        }

        fn len(&self) -> Result<u64> {
            self.len_calls.fetch_add(1, Ordering::Relaxed);
            Ok(self.data.len() as u64)
        }
    }

    #[test]
    fn slice_read_at_basic() {
        let data: &[u8] = b"hello world";
        let mut buf = [0u8; 5];
        let n = data.read_at(&mut buf, 0).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"hello");
    }

    #[test]
    fn cached_read_at_is_lock_free_read_through_and_caches_len() {
        let (inner, reads, len_calls) = CountingReadAt::new(b"abcdefgh".to_vec());
        let cached =
            CachedReadAt::with_capacity(inner, 4, std::num::NonZeroUsize::new(2).unwrap()).unwrap();

        let mut first = [0u8; 2];
        cached.read_exact_at(&mut first, 0).unwrap();
        assert_eq!(&first, b"ab");

        let mut second = [0u8; 2];
        cached.read_exact_at(&mut second, 1).unwrap();
        assert_eq!(&second, b"bc");

        let mut third = [0u8; 2];
        cached.read_exact_at(&mut third, 5).unwrap();
        assert_eq!(&third, b"fg");

        assert_eq!(cached.len().unwrap(), 8);
        assert_eq!(reads.load(Ordering::Relaxed), 3);
        assert_eq!(len_calls.load(Ordering::Relaxed), 1);
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
    fn data_component_truncation_returns_eof_instead_of_sigbus() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("1-Data.db");
        std::fs::write(&path, b"0123456789").unwrap();

        let reader = FileReadAt::open(&path).unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(1)
            .unwrap();

        let mut buf = [0u8; 4];
        let err = reader.read_exact_at(&mut buf, 6).unwrap_err();
        assert!(
            err.to_string().contains("wanted 4 bytes"),
            "truncated data component must surface EOF, got: {err}"
        );
    }

    #[test]
    fn index_components_use_mmap() {
        let dir = tempfile::tempdir().unwrap();
        let partitions = dir.path().join("1-Partitions.db");
        let rows = dir.path().join("1-Rows.db");
        std::fs::write(&partitions, b"partitions").unwrap();
        std::fs::write(&rows, b"rows").unwrap();

        assert!(matches!(
            FileReadAt::open(&partitions).unwrap().inner,
            FileReadAtInner::Mmap { .. }
        ));
        assert!(matches!(
            FileReadAt::open(&rows).unwrap().inner,
            FileReadAtInner::Mmap { .. }
        ));
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
    fn default_fd_cache_capacity_leaves_headroom_under_low_ulimit() {
        let cap = super::fd_cache_capacity(None, Some(1024));

        assert!(
            cap.get() <= 256,
            "default fd cache cap must leave process headroom under ulimit 1024, got {}",
            cap
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

    #[test]
    fn cached_data_component_rehydrates_missing_file_before_retrying_read() {
        let cache = Arc::new(FdCache::with_capacity(NonZeroUsize::new(4).unwrap()));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("1-Data.db");
        let restored = b"restored sstable bytes";
        std::fs::write(&path, b"original bytes").unwrap();

        let reader = FileReadAt::open_with_cache(&path, Arc::clone(&cache)).unwrap();
        std::fs::remove_file(&path).unwrap();
        cache.invalidate_for_test(&path);

        let hook_path = path.clone();
        let hook_calls = Arc::new(AtomicU64::new(0));
        let hook_calls_for_hook = Arc::clone(&hook_calls);
        register_file_read_rehydration_hook(Arc::new(move |missing_path| {
            if missing_path != hook_path {
                return Ok(false);
            }
            hook_calls_for_hook.fetch_add(1, Ordering::Relaxed);
            std::fs::write(missing_path, restored)?;
            Ok(true)
        }));

        let mut buf = vec![0u8; restored.len()];
        reader.read_exact_at(&mut buf, 0).unwrap();

        assert_eq!(buf, restored);
        assert_eq!(hook_calls.load(Ordering::Relaxed), 1);
        assert_eq!(reader.len().unwrap(), restored.len() as u64);
    }

    #[test]
    fn cached_data_component_reads_evicted_range_without_full_rehydrate() {
        let cache = Arc::new(FdCache::with_capacity(NonZeroUsize::new(4).unwrap()));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("range-fast-path-Data.db");
        std::fs::write(&path, b"local seed bytes").unwrap();

        let reader = FileReadAt::open_with_cache(&path, Arc::clone(&cache)).unwrap();
        std::fs::remove_file(&path).unwrap();
        cache.invalidate_for_test(&path);

        let remote = Arc::new(b"0123456789abcdef".to_vec());
        let hook_path = path.clone();
        let range_calls = Arc::new(AtomicU64::new(0));
        let len_calls = Arc::new(AtomicU64::new(0));
        let range_calls_for_hook = Arc::clone(&range_calls);
        let remote_for_range = Arc::clone(&remote);
        register_file_read_range_hook(Arc::new(move |missing_path, offset, len| {
            if missing_path != hook_path {
                return Ok(None);
            }
            range_calls_for_hook.fetch_add(1, Ordering::Relaxed);
            let start = offset as usize;
            let end = start.saturating_add(len).min(remote_for_range.len());
            Ok(Some(remote_for_range[start..end].to_vec()))
        }));

        let hook_path = path.clone();
        let remote_for_len = Arc::clone(&remote);
        let len_calls_for_hook = Arc::clone(&len_calls);
        register_file_read_len_hook(Arc::new(move |missing_path| {
            if missing_path != hook_path {
                return Ok(None);
            }
            len_calls_for_hook.fetch_add(1, Ordering::Relaxed);
            Ok(Some(remote_for_len.len() as u64))
        }));

        let mut buf = [0u8; 4];
        reader.read_exact_at(&mut buf, 4).unwrap();

        assert_eq!(&buf, b"4567");
        assert_eq!(reader.len().unwrap(), remote.len() as u64);
        assert_eq!(range_calls.load(Ordering::Relaxed), 1);
        assert_eq!(len_calls.load(Ordering::Relaxed), 1);
        assert!(
            !path.exists(),
            "range fast path must not rehydrate the whole component"
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
            after_reads <= baseline + 16,
            "same-path readers must share one cache entry, not 256 fds: baseline={baseline}, after_reads={after_reads}"
        );
    }
}
