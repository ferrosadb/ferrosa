//! Page-range object-store cache for quantized vector artifacts.
//!
//! Quantized `.qvec` artifacts are one durable object per SSTable/index
//! generation. Query readers need individual pages, not whole sidecars, and
//! local NVMe may be much smaller than the full artifact. This module keeps a
//! bounded per-page cache and rehydrates misses with S3-compatible byte-range
//! reads.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use ferrosa_common::{Error, Result};
use object_store::path::Path as ObjectPath;
use object_store::{GetOptions, GetRange, ObjectStore};
use parking_lot::Mutex;
use sha2::{Digest, Sha256};

/// Minimal storage-side manifest needed to resolve a quantized artifact page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuantizedArtifactManifest {
    object_path: ObjectPath,
    object_len: u64,
    page_size: u64,
}

impl QuantizedArtifactManifest {
    /// Creates a manifest for a single-object `.qvec` artifact.
    pub fn new(object_path: ObjectPath, object_len: u64, page_size: u64) -> Self {
        Self {
            object_path,
            object_len,
            page_size,
        }
    }

    /// Returns the durable object key for the artifact.
    pub fn object_path(&self) -> &ObjectPath {
        &self.object_path
    }

    /// Returns the number of pages addressed by this artifact.
    pub fn page_count(&self) -> u64 {
        self.object_len.div_ceil(self.page_size)
    }

    fn page_range(&self, page_id: u64) -> Result<std::ops::Range<usize>> {
        if self.page_size == 0 {
            return Err(Error::InvalidData(
                "quantized artifact page size must be non-zero".to_string(),
            ));
        }
        if page_id >= self.page_count() {
            return Err(Error::InvalidData(format!(
                "quantized page {page_id} out of bounds for {} pages",
                self.page_count()
            )));
        }

        let start = page_id
            .checked_mul(self.page_size)
            .ok_or_else(|| Error::InvalidData("quantized page offset overflow".to_string()))?;
        let end = (start + self.page_size).min(self.object_len);
        Ok(start as usize..end as usize)
    }
}

#[derive(Clone, Debug)]
struct PageCacheEntry {
    path: PathBuf,
    size: u64,
    last_accessed: Instant,
}

#[derive(Default)]
struct PageCacheState {
    entries: HashMap<String, PageCacheEntry>,
    bytes: u64,
}

/// Bounded local cache backed by object-store range reads for `.qvec` pages.
pub struct ObjectRangePageStore {
    object_store: Arc<dyn ObjectStore>,
    cache_dir: PathBuf,
    max_cache_bytes: u64,
    state: Mutex<PageCacheState>,
    object_range_reads: AtomicU64,
    object_bytes_read: AtomicU64,
}

impl ObjectRangePageStore {
    /// Creates a page store rooted at `cache_dir` with an LRU byte budget.
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        cache_dir: PathBuf,
        max_cache_bytes: u64,
    ) -> Result<Self> {
        fs::create_dir_all(&cache_dir)?;
        Ok(Self {
            object_store,
            cache_dir,
            max_cache_bytes,
            state: Mutex::new(PageCacheState::default()),
            object_range_reads: AtomicU64::new(0),
            object_bytes_read: AtomicU64::new(0),
        })
    }

    /// Read one artifact page, serving a cache hit locally or range-reading it.
    pub async fn read_page(
        &self,
        manifest: &QuantizedArtifactManifest,
        page_id: u64,
    ) -> Result<Bytes> {
        let range = manifest.page_range(page_id)?;
        let key = cache_key(manifest, page_id);

        if let Some(path) = self.cached_path_if_present(&key) {
            return Ok(Bytes::from(fs::read(path)?));
        }

        let options = GetOptions {
            range: Some(GetRange::Bounded(range.clone())),
            ..GetOptions::default()
        };
        let bytes = self
            .object_store
            .get_opts(manifest.object_path(), options)
            .await
            .map_err(|e| {
                Error::InvalidFormat(format!("quantized artifact range read failed: {e}"))
            })?
            .bytes()
            .await
            .map_err(|e| {
                Error::InvalidFormat(format!("quantized artifact range body failed: {e}"))
            })?;

        if bytes.len() != range.len() {
            return Err(Error::InvalidFormat(format!(
                "short quantized page read: expected {} bytes, got {}",
                range.len(),
                bytes.len()
            )));
        }

        self.object_range_reads.fetch_add(1, Ordering::Relaxed);
        self.object_bytes_read
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        self.write_cache_page(key, &bytes)?;
        Ok(bytes)
    }

    /// Returns current on-disk page bytes tracked by the bounded cache.
    pub fn cache_bytes(&self) -> u64 {
        self.state.lock().bytes
    }

    /// Returns how many object range reads this page store has issued.
    pub fn object_range_reads(&self) -> u64 {
        self.object_range_reads.load(Ordering::Relaxed)
    }

    /// Returns total bytes fetched from object range reads.
    pub fn object_bytes_read(&self) -> u64 {
        self.object_bytes_read.load(Ordering::Relaxed)
    }

    /// Removes a cached page. Test-only hook for eviction/rehydration evidence.
    pub fn delete_cached_page_for_test(
        &self,
        manifest: &QuantizedArtifactManifest,
        page_id: u64,
    ) -> Result<()> {
        let key = cache_key(manifest, page_id);
        let mut state = self.state.lock();
        if let Some(entry) = state.entries.remove(&key) {
            state.bytes = state.bytes.saturating_sub(entry.size);
            match fs::remove_file(&entry.path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    fn cached_path_if_present(&self, key: &str) -> Option<PathBuf> {
        let mut state = self.state.lock();
        let entry = state.entries.get_mut(key)?;
        if entry.path.is_file() {
            entry.last_accessed = Instant::now();
            Some(entry.path.clone())
        } else {
            let size = entry.size;
            state.entries.remove(key);
            state.bytes = state.bytes.saturating_sub(size);
            None
        }
    }

    fn write_cache_page(&self, key: String, bytes: &[u8]) -> Result<()> {
        let path = self.cache_dir.join(&key);
        fs::write(&path, bytes)?;

        let mut state = self.state.lock();
        if let Some(previous) = state.entries.remove(&key) {
            state.bytes = state.bytes.saturating_sub(previous.size);
        }
        state.bytes += bytes.len() as u64;
        state.entries.insert(
            key.clone(),
            PageCacheEntry {
                path,
                size: bytes.len() as u64,
                last_accessed: Instant::now(),
            },
        );
        self.evict_locked(&mut state, &key);
        Ok(())
    }

    fn evict_locked(&self, state: &mut PageCacheState, newest_key: &str) {
        while state.bytes > self.max_cache_bytes {
            let victim = state
                .entries
                .iter()
                .filter(|(key, _)| key.as_str() != newest_key || state.entries.len() == 1)
                .min_by_key(|(_, entry)| entry.last_accessed)
                .map(|(key, _)| key.clone());
            let Some(victim) = victim else { break };
            let Some(entry) = state.entries.remove(&victim) else {
                break;
            };
            state.bytes = state.bytes.saturating_sub(entry.size);
            let _ = fs::remove_file(entry.path);
        }
    }
}

fn cache_key(manifest: &QuantizedArtifactManifest, page_id: u64) -> String {
    let mut hasher = Sha256::new();
    hasher.update(manifest.object_path.to_string().as_bytes());
    hasher.update(b"\0");
    hasher.update(page_id.to_le_bytes());
    format!("{}.page", hex::encode(hasher.finalize()))
}
