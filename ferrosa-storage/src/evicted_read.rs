//! Ranged, paged reads of SSTables the uploaded-cache evictor removed (ST-51).
//!
//! Opening an evicted SSTable for a query used to download the whole
//! generation first: on 2026-09-30 a point read of one key waited for a
//! 479 MB SSTable (281 MB `Data.db`). A query needs the generation's small
//! index components and a few pages of `Data.db`, nothing more.
//!
//! * [`EvictedReadStore::fetch_query_components`] downloads only the small
//!   components a reader needs to open (`Partitions.db`, `Rows.db`,
//!   `Filter.db`, `Statistics.db`, `CompressionInfo.db`, `TOC.txt`,
//!   `Digest.crc32`, `CRC.db`) next to where the generation lived. Each is
//!   written to a temp file, fsynced and renamed, so a crash leaves either a
//!   whole component or none.
//! * `Data.db` (and any other component that is missing locally) is served
//!   through the process-wide `ferrosa_sstable::io` range and len hooks, which
//!   read it page by page: [`EvictedReadStore::read_range`]. Pages follow
//!   compression-chunk boundaries when `CompressionInfo.db` is local, else a
//!   fixed size. They live in a bounded in-memory LRU cache, and concurrent
//!   readers of one page share a single fetch.
//!
//! The reader above this layer still verifies the per-chunk CRCs (compressed
//! tables) and `CRC.db` (uncompressed tables) on the bytes it is handed, so a
//! corrupted object is an error, not a wrong row. Here, a page that comes back
//! short or fails is an error: a ranged read never returns fewer bytes than
//! the object holds (FMEA ST-41 / ST-51).
//!
//! Full-generation rehydration is unchanged and still owns startup restore and
//! compaction inputs (`ferrosa_sstable::io::rehydrate_file`).

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Once, RwLock};

use bytes::Bytes;
use dashmap::DashMap;
use ferrosa_common::{Error, Result};
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;

use crate::engine::StorageEngine;

/// Default page size when `FERROSA_S3_READ_PAGE_BYTES` is unset.
pub(crate) const DEFAULT_PAGE_BYTES: u64 = 1 << 20;
/// Default page-cache budget when `FERROSA_S3_PAGE_CACHE_BYTES` is unset.
pub(crate) const DEFAULT_PAGE_CACHE_BYTES: u64 = 256 << 20;
/// Smallest accepted page size: smaller pages turn one read into many requests.
const MIN_PAGE_BYTES: u64 = 4096;
/// Most adjacent missing pages fetched with one ranged GET.
const MAX_RUN_PAGES: usize = 8;
/// Object-info entries kept per store before the table is rebuilt from scratch.
const MAX_OBJECT_INFOS: usize = 4096;

/// Every component the engine uploads, as named in the object store.
const SSTABLE_COMPONENTS: &[&str] = &[
    "Data.db",
    "Partitions.db",
    "Rows.db",
    "Filter.db",
    "Statistics.db",
    "TOC.txt",
    "CompressionInfo.db",
    "Digest.crc32",
    "CRC.db",
];

/// Components needed to open a reader. The first three are required: a
/// generation missing any of them cannot be opened.
const QUERY_COMPONENTS: &[(&str, bool)] = &[
    ("Partitions.db", true),
    ("Rows.db", true),
    ("Filter.db", true),
    ("Statistics.db", false),
    ("TOC.txt", false),
    ("CompressionInfo.db", false),
    ("Digest.crc32", false),
    ("CRC.db", false),
];

/// Tunables for ranged reads of evicted SSTables.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct EvictedReadConfig {
    /// Target page size in bytes (compressed tables round up to whole chunks).
    pub page_bytes: u64,
    /// Total bytes of pages the in-memory cache may hold.
    pub cache_bytes: u64,
}

impl Default for EvictedReadConfig {
    fn default() -> Self {
        Self {
            page_bytes: DEFAULT_PAGE_BYTES,
            cache_bytes: DEFAULT_PAGE_CACHE_BYTES,
        }
    }
}

impl EvictedReadConfig {
    /// Read `FERROSA_S3_READ_PAGE_BYTES` and `FERROSA_S3_PAGE_CACHE_BYTES`. A
    /// value that does not parse (or a page size under 4 KiB) is reported and
    /// replaced by the default, never silently reinterpreted.
    pub(crate) fn from_env() -> Self {
        let default = Self::default();
        Self {
            page_bytes: env_u64(
                "FERROSA_S3_READ_PAGE_BYTES",
                default.page_bytes,
                MIN_PAGE_BYTES,
            ),
            cache_bytes: env_u64("FERROSA_S3_PAGE_CACHE_BYTES", default.cache_bytes, 0),
        }
    }
}

fn env_u64(name: &str, default: u64, min: u64) -> u64 {
    let Ok(raw) = std::env::var(name) else {
        return default;
    };
    match raw.trim().parse::<u64>() {
        Ok(value) if value >= min => value,
        _ => {
            tracing::warn!(
                variable = name,
                value = %raw,
                minimum = min,
                default,
                "ignoring an invalid evicted-SSTable read setting; using the default"
            );
            default
        }
    }
}

/// What [`EvictedReadStore::fetch_query_components`] found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QueryFetch {
    /// The index components are local; `Data.db` is served by ranged reads.
    Ready,
    /// No registered store owns this path.
    NotOwned,
    /// The object store lacks the generation (its `Data.db` or a required
    /// component), so it cannot be opened.
    Missing,
}

type PageKey = (String, u64);

#[derive(Default)]
struct PageCacheInner {
    pages: HashMap<PageKey, (Bytes, u64)>,
    /// `tick -> key`, oldest first, for O(log n) LRU eviction.
    order: BTreeMap<u64, PageKey>,
    bytes: u64,
    tick: u64,
}

/// Bounded LRU of fetched pages.
struct PageCache {
    max_bytes: u64,
    inner: Mutex<PageCacheInner>,
}

impl PageCache {
    fn new(max_bytes: u64) -> Self {
        Self {
            max_bytes,
            inner: Mutex::new(PageCacheInner::default()),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, PageCacheInner> {
        // The cache holds only immutable page bytes; a panic elsewhere cannot
        // leave it logically inconsistent, so a poisoned lock is usable.
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn get(&self, key: &PageKey) -> Option<Bytes> {
        let mut inner = self.lock();
        inner.tick += 1;
        let tick = inner.tick;
        let (page, old_tick) = {
            let (page, old) = inner.pages.get_mut(key)?;
            let old_tick = std::mem::replace(old, tick);
            (page.clone(), old_tick)
        };
        inner.order.remove(&old_tick);
        inner.order.insert(tick, key.clone());
        Some(page)
    }

    fn insert(&self, key: PageKey, page: Bytes) {
        let mut inner = self.lock();
        inner.tick += 1;
        let tick = inner.tick;
        if let Some((old, old_tick)) = inner.pages.remove(&key) {
            inner.bytes = inner.bytes.saturating_sub(old.len() as u64);
            inner.order.remove(&old_tick);
        }
        inner.bytes += page.len() as u64;
        inner.order.insert(tick, key.clone());
        inner.pages.insert(key, (page, tick));
        while inner.bytes > self.max_bytes {
            let Some((_, oldest)) = inner.order.pop_first() else {
                break;
            };
            if let Some((evicted, _)) = inner.pages.remove(&oldest) {
                inner.bytes = inner.bytes.saturating_sub(evicted.len() as u64);
            }
        }
    }

    #[cfg(test)]
    fn bytes(&self) -> u64 {
        self.lock().bytes
    }
}

/// Size and page layout of one object in the store.
struct ObjectInfo {
    size: u64,
    /// Page start offsets (strictly increasing, first is 0) when the object is
    /// a compressed `Data.db`; `None` means fixed-size pages.
    page_starts: Option<Vec<u64>>,
}

impl ObjectInfo {
    /// The page `[start, end)` holding `offset` (`offset < size`).
    fn page_range(&self, offset: u64, page_bytes: u64) -> (u64, u64) {
        match &self.page_starts {
            Some(starts) => {
                let index = starts.partition_point(|&s| s <= offset).saturating_sub(1);
                let start = starts[index];
                let end = starts.get(index + 1).copied().unwrap_or(self.size);
                (start, end)
            }
            None => {
                let start = offset / page_bytes * page_bytes;
                (start, start.saturating_add(page_bytes).min(self.size))
            }
        }
    }
}

/// Group compressed-chunk offsets into pages of at least `page_bytes`, each
/// starting on a chunk boundary, so a page never splits a compressed chunk.
fn chunk_aligned_page_starts(chunk_offsets: &[u64], page_bytes: u64) -> Vec<u64> {
    let mut starts = vec![0u64];
    for &offset in chunk_offsets.iter().skip(1) {
        let last = *starts.last().expect("starts is never empty");
        if offset - last >= page_bytes {
            starts.push(offset);
        }
    }
    starts
}

struct ObjectLocation {
    key: ObjectPath,
    table_id: String,
    dir: PathBuf,
    sstable_id: String,
    component: String,
}

/// Ranged-read state for one data directory's object store.
pub(crate) struct EvictedReadStore {
    data_dir: PathBuf,
    prefix: String,
    store: Arc<dyn ObjectStore>,
    config: EvictedReadConfig,
    cache: PageCache,
    objects: DashMap<String, Arc<ObjectInfo>>,
    /// One lock per page being fetched: the first reader fetches, the rest wait
    /// and then hit the cache.
    page_loads: DashMap<PageKey, Arc<Mutex<()>>>,
    /// One lock per generation whose index components are being downloaded.
    gen_loads: DashMap<String, Arc<Mutex<()>>>,
    gets: AtomicU64,
    page_bytes_fetched: AtomicU64,
    index_bytes_fetched: AtomicU64,
}

static REGISTRY: RwLock<Vec<Arc<EvictedReadStore>>> = RwLock::new(Vec::new());
static HOOKS: Once = Once::new();

/// Register (or replace) the ranged-read store for `data_dir`, with settings
/// from the environment, and make sure the process-wide hooks exist.
pub(crate) fn register(data_dir: PathBuf, prefix: String, store: Arc<dyn ObjectStore>) {
    register_with_config(data_dir, prefix, store, EvictedReadConfig::from_env());
}

/// [`register`] with explicit settings.
pub(crate) fn register_with_config(
    data_dir: PathBuf,
    prefix: String,
    store: Arc<dyn ObjectStore>,
    config: EvictedReadConfig,
) -> Arc<EvictedReadStore> {
    let entry = Arc::new(EvictedReadStore::new(data_dir, prefix, store, config));
    {
        let mut registry = REGISTRY.write().unwrap_or_else(|e| e.into_inner());
        registry.retain(|existing| existing.data_dir != entry.data_dir);
        registry.push(Arc::clone(&entry));
    }
    HOOKS.call_once(install_hooks);
    entry
}

/// The store owning `path`, if any data directory registered one.
pub(crate) fn store_for(path: &Path) -> Option<Arc<EvictedReadStore>> {
    let registry = REGISTRY.read().unwrap_or_else(|e| e.into_inner());
    registry
        .iter()
        .find(|entry| path.starts_with(&entry.data_dir))
        .cloned()
}

/// Download the index components of generation `gen` into `dir` so a reader
/// can open it, leaving `Data.db` to ranged reads. See [`QueryFetch`].
pub(crate) fn fetch_query_components(dir: &Path, gen: &str) -> Result<QueryFetch> {
    let data = dir.join(format!("{gen}-Data.db"));
    match store_for(&data) {
        Some(store) => store.fetch_query_components(dir, gen),
        None => Ok(QueryFetch::NotOwned),
    }
}

fn install_hooks() {
    ferrosa_sstable::io::register_file_read_range_hook(Arc::new(
        |path, offset, len| match store_for(path) {
            Some(store) => store.read_range(path, offset, len),
            None => Ok(None),
        },
    ));
    ferrosa_sstable::io::register_file_read_len_hook(Arc::new(|path| match store_for(path) {
        Some(store) => store.object_len(path),
        None => Ok(None),
    }));
}

impl EvictedReadStore {
    fn new(
        data_dir: PathBuf,
        prefix: String,
        store: Arc<dyn ObjectStore>,
        config: EvictedReadConfig,
    ) -> Self {
        Self {
            data_dir,
            prefix,
            store,
            cache: PageCache::new(config.cache_bytes),
            config,
            objects: DashMap::new(),
            page_loads: DashMap::new(),
            gen_loads: DashMap::new(),
            gets: AtomicU64::new(0),
            page_bytes_fetched: AtomicU64::new(0),
            index_bytes_fetched: AtomicU64::new(0),
        }
    }

    /// Bytes of pages currently cached.
    #[cfg(test)]
    pub(crate) fn cached_bytes(&self) -> u64 {
        self.cache.bytes()
    }

    /// Number of ranged GETs issued for `Data.db` pages.
    #[cfg(test)]
    pub(crate) fn gets(&self) -> u64 {
        self.gets.load(Ordering::Relaxed)
    }

    /// Bytes of index components downloaded for ranged opens.
    #[cfg(test)]
    pub(crate) fn index_bytes_fetched(&self) -> u64 {
        self.index_bytes_fetched.load(Ordering::Relaxed)
    }

    fn locate(&self, path: &Path) -> Option<ObjectLocation> {
        let (table_id, sstable_id, component) =
            StorageEngine::parse_local_sstable_component_path(&self.data_dir, path)?;
        if !SSTABLE_COMPONENTS.contains(&component.as_str()) {
            return None;
        }
        let hex = crate::upload::manager::hex_prefix_for(&sstable_id);
        let key = crate::upload::manager::sstable_object_key(
            &self.prefix,
            &hex,
            &table_id,
            &sstable_id,
            &component,
        );
        Some(ObjectLocation {
            key,
            table_id,
            dir: path.parent()?.to_path_buf(),
            sstable_id,
            component,
        })
    }

    /// Size and layout of `loc`, from the cache or one `HEAD`. `Ok(None)` when
    /// the object does not exist.
    fn object_info(&self, loc: &ObjectLocation) -> Result<Option<Arc<ObjectInfo>>> {
        let cache_key = loc.key.to_string();
        if let Some(info) = self.objects.get(&cache_key) {
            return Ok(Some(Arc::clone(&info)));
        }
        let store = Arc::clone(&self.store);
        let key = loc.key.clone();
        let head = StorageEngine::block_on_rehydration(async move { store.head(&key).await });
        let size = match head {
            Ok(meta) => meta.size as u64,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => {
                return Err(Error::InvalidFormat(format!(
                    "failed SSTable component head {}: {e}",
                    loc.key
                )));
            }
        };
        let info = Arc::new(ObjectInfo {
            size,
            page_starts: self.page_layout(loc, size)?,
        });
        if self.objects.len() >= MAX_OBJECT_INFOS {
            // Layouts are cheap to rebuild; bounding the table bounds memory.
            self.objects.clear();
        }
        self.objects.insert(cache_key, Arc::clone(&info));
        Ok(Some(info))
    }

    /// Chunk-aligned page starts for a compressed `Data.db` whose
    /// `CompressionInfo.db` is local; `None` (fixed pages) otherwise.
    fn page_layout(&self, loc: &ObjectLocation, size: u64) -> Result<Option<Vec<u64>>> {
        if loc.component != "Data.db" {
            return Ok(None);
        }
        let info_path = loc
            .dir
            .join(format!("{}-CompressionInfo.db", loc.sstable_id));
        let bytes = match std::fs::read(&info_path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => {
                return Err(Error::InvalidFormat(format!(
                    "cannot read {} to lay out ranged pages: {e}",
                    info_path.display()
                )));
            }
        };
        let info = ferrosa_sstable::compression::CompressionInfo::read(&bytes)?;
        if info.chunk_offsets.last().is_some_and(|&last| last >= size) {
            return Err(Error::InvalidFormat(format!(
                "{} lists a chunk at offset >= the {size}-byte Data.db object",
                info_path.display()
            )));
        }
        Ok(Some(chunk_aligned_page_starts(
            &info.chunk_offsets,
            self.config.page_bytes,
        )))
    }

    /// Length of an evicted component without downloading it.
    pub(crate) fn object_len(&self, path: &Path) -> Result<Option<u64>> {
        let Some(loc) = self.locate(path) else {
            return Ok(None);
        };
        Ok(self.object_info(&loc)?.map(|info| info.size))
    }

    /// Serve `len` bytes at `offset` of an evicted component from cached or
    /// freshly fetched pages. Returns fewer than `len` bytes only at the end of
    /// the object. `Ok(None)`: not an SSTable component of this store, or the
    /// object does not exist (the caller then fails or falls back loudly).
    pub(crate) fn read_range(
        &self,
        path: &Path,
        offset: u64,
        len: usize,
    ) -> Result<Option<Vec<u8>>> {
        let Some(loc) = self.locate(path) else {
            return Ok(None);
        };
        if len == 0 {
            return Ok(Some(Vec::new()));
        }
        let Some(info) = self.object_info(&loc)? else {
            return Ok(None);
        };
        if offset >= info.size {
            return Ok(Some(Vec::new()));
        }
        let end = offset.saturating_add(len as u64).min(info.size);
        let pages = self.pages_covering(&loc, &info, offset, end)?;
        let held = self.load_pages(&loc.key, &pages)?;
        let mut out = Vec::with_capacity((end - offset) as usize);
        for ((start, page_end), page) in pages.iter().zip(&held) {
            let from = offset.max(*start) - start;
            let to = end.min(*page_end) - start;
            out.extend_from_slice(&page[from as usize..to as usize]);
        }
        debug_assert_eq!(out.len() as u64, end - offset);
        Ok(Some(out))
    }

    /// The `[start, end)` pages that cover `[offset, end)` of the object.
    fn pages_covering(
        &self,
        loc: &ObjectLocation,
        info: &ObjectInfo,
        offset: u64,
        end: u64,
    ) -> Result<Vec<(u64, u64)>> {
        let mut pages = Vec::new();
        let mut pos = offset;
        while pos < end {
            let (start, page_end) = info.page_range(pos, self.config.page_bytes);
            if page_end <= pos || start > pos {
                return Err(Error::InvalidFormat(format!(
                    "page layout of {} does not cover offset {pos}",
                    loc.key
                )));
            }
            pages.push((start, page_end));
            pos = page_end;
        }
        Ok(pages)
    }

    /// Every page in `pages` (ascending, contiguous), from the cache or the
    /// store. The pages are held by the caller for the duration of the read, so
    /// a read larger than the cache still assembles correctly.
    ///
    /// Misses are fetched under per-page locks, taken in ascending order, so
    /// concurrent readers of one page cause one fetch and cannot deadlock. A
    /// run of adjacent misses is fetched with one ranged GET.
    fn load_pages(&self, key: &ObjectPath, pages: &[(u64, u64)]) -> Result<Vec<Bytes>> {
        let object = key.to_string();
        let page_key = |start: u64| -> PageKey { (object.clone(), start) };
        let mut held: Vec<Option<Bytes>> = pages
            .iter()
            .map(|(start, _)| self.cache.get(&page_key(*start)))
            .collect();
        let missing: Vec<usize> = (0..pages.len()).filter(|&i| held[i].is_none()).collect();
        if missing.is_empty() {
            return Ok(held.into_iter().flatten().collect());
        }
        let locks: Vec<(PageKey, Arc<Mutex<()>>)> = missing
            .iter()
            .map(|&i| {
                let k = page_key(pages[i].0);
                let lock = Arc::clone(self.page_loads.entry(k.clone()).or_default().value());
                (k, lock)
            })
            .collect();
        let _guards: Vec<_> = locks
            .iter()
            .map(|(_, lock)| lock.lock().unwrap_or_else(|e| e.into_inner()))
            .collect();
        // Another reader may have fetched some of them while this one waited.
        let mut still_missing = Vec::new();
        for &i in &missing {
            match self.cache.get(&page_key(pages[i].0)) {
                Some(page) => held[i] = Some(page),
                None => still_missing.push(i),
            }
        }
        let fetched = self.fetch_runs(key, pages, &still_missing, &mut held);
        // The cache is filled before the page locks go away, so a reader that
        // finds no lock entry always finds the page.
        for (k, _) in &locks {
            self.page_loads.remove(k);
        }
        fetched?;
        held.into_iter()
            .map(|page| {
                page.ok_or_else(|| {
                    Error::InvalidFormat(format!("ranged read of {key} left a page unloaded"))
                })
            })
            .collect()
    }

    /// Fetch `missing` page indexes, one ranged GET per run of adjacent pages,
    /// cache them and store them in `held`.
    fn fetch_runs(
        &self,
        key: &ObjectPath,
        pages: &[(u64, u64)],
        missing: &[usize],
        held: &mut [Option<Bytes>],
    ) -> Result<()> {
        let mut run_start = 0;
        while run_start < missing.len() {
            let mut run_end = run_start + 1;
            while run_end < missing.len()
                && run_end - run_start < MAX_RUN_PAGES
                && missing[run_end] == missing[run_end - 1] + 1
            {
                run_end += 1;
            }
            let run = &missing[run_start..run_end];
            let start = pages[run[0]].0;
            let end = pages[run[run.len() - 1]].1;
            let bytes = self.fetch_range(key, start, end)?;
            for &i in run {
                let (page_start, page_end) = pages[i];
                // Zero-copy: the page shares the run's allocation.
                let page = bytes.slice((page_start - start) as usize..(page_end - start) as usize);
                self.cache
                    .insert((key.to_string(), page_start), page.clone());
                held[i] = Some(page);
            }
            run_start = run_end;
        }
        Ok(())
    }

    /// One ranged GET of `[start, end)`; a short answer is an error.
    fn fetch_range(&self, key: &ObjectPath, start: u64, end: u64) -> Result<Bytes> {
        let store = Arc::clone(&self.store);
        let range = start as usize..end as usize;
        let get_key = key.clone();
        let bytes =
            StorageEngine::block_on_rehydration(
                async move { store.get_range(&get_key, range).await },
            )
            .map_err(|e| {
                Error::InvalidFormat(format!(
                    "failed ranged SSTable read {key} [{start}, {end}): {e}"
                ))
            })?;
        let expected = (end - start) as usize;
        if bytes.len() != expected {
            return Err(Error::InvalidFormat(format!(
                "short ranged SSTable read {key} [{start}, {end}): expected {expected} bytes, got {}",
                bytes.len()
            )));
        }
        self.gets.fetch_add(1, Ordering::Relaxed);
        self.page_bytes_fetched
            .fetch_add(expected as u64, Ordering::Relaxed);
        tracing::debug!(
            object = %key,
            start,
            bytes = expected,
            "served evicted SSTable range from object storage"
        );
        Ok(bytes)
    }

    /// Download the index components of generation `gen` into `dir`.
    pub(crate) fn fetch_query_components(&self, dir: &Path, gen: &str) -> Result<QueryFetch> {
        let data_path = dir.join(format!("{gen}-Data.db"));
        let Some(data_loc) = self.locate(&data_path) else {
            return Ok(QueryFetch::NotOwned);
        };
        let lock_key = data_loc.key.to_string();
        let lock = Arc::clone(self.gen_loads.entry(lock_key.clone()).or_default().value());
        let _guard = lock.lock().unwrap_or_else(|e| e.into_inner());
        let outcome = self.fetch_query_components_locked(dir, gen, &data_loc);
        self.gen_loads.remove(&lock_key);
        outcome
    }

    fn fetch_query_components_locked(
        &self,
        dir: &Path,
        gen: &str,
        data_loc: &ObjectLocation,
    ) -> Result<QueryFetch> {
        if self.object_info(data_loc)?.is_none() {
            tracing::error!(
                object = %data_loc.key,
                "evicted SSTable has no Data.db object in the store; it cannot be opened"
            );
            return Ok(QueryFetch::Missing);
        }
        std::fs::create_dir_all(dir)?;
        let mut fetched_bytes = 0u64;
        for (component, required) in QUERY_COMPONENTS {
            let local = dir.join(format!("{gen}-{component}"));
            if local.exists() {
                continue;
            }
            let key = self.component_key(data_loc, component);
            match self.download_small_component(&key, &local)? {
                Some(bytes) => fetched_bytes += bytes,
                None if *required => {
                    tracing::error!(
                        object = %key,
                        "evicted SSTable is missing a required component in the store; it cannot be opened"
                    );
                    return Ok(QueryFetch::Missing);
                }
                None => {}
            }
        }
        if fetched_bytes > 0 {
            sync_dir(dir)?;
            self.index_bytes_fetched
                .fetch_add(fetched_bytes, Ordering::Relaxed);
            tracing::info!(
                dir = %dir.display(),
                sstable = gen,
                index_bytes = fetched_bytes,
                "opened an evicted SSTable for ranged reads: fetched its index components only"
            );
        }
        Ok(QueryFetch::Ready)
    }

    fn component_key(&self, loc: &ObjectLocation, component: &str) -> ObjectPath {
        let hex = crate::upload::manager::hex_prefix_for(&loc.sstable_id);
        crate::upload::manager::sstable_object_key(
            &self.prefix,
            &hex,
            &loc.table_id,
            &loc.sstable_id,
            component,
        )
    }

    /// Download one small component to `local` atomically (temp file, fsync,
    /// rename). `Ok(None)` when the object does not exist.
    fn download_small_component(&self, key: &ObjectPath, local: &Path) -> Result<Option<u64>> {
        let store = Arc::clone(&self.store);
        let get_key = key.clone();
        let fetched = StorageEngine::block_on_rehydration(async move {
            match store.get(&get_key).await {
                Ok(result) => result.bytes().await.map(Some),
                Err(e) => Err(e),
            }
        });
        let bytes = match fetched {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return Ok(None),
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => {
                return Err(Error::InvalidFormat(format!(
                    "failed to fetch SSTable component {key}: {e}"
                )));
            }
        };
        let tmp = local.with_extension(format!(
            "{}.query-fetch.tmp",
            local
                .extension()
                .and_then(|e| e.to_str())
                .unwrap_or("component")
        ));
        write_synced(&tmp, &bytes)?;
        std::fs::rename(&tmp, local)?;
        Ok(Some(bytes.len() as u64))
    }
}

#[cfg(test)]
#[path = "evicted_read_tests.rs"]
mod tests;

fn write_synced(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;
    let mut file = std::fs::File::create(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn sync_dir(dir: &Path) -> Result<()> {
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
}
