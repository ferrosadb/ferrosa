# ferrosa-storage Part C Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete ferrosa-storage with commit log hardening, STCS compaction, S3 upload via `object_store`, and `StorageEngine` composition.

**Architecture:** Bottom-up build: harden commit log → add compaction → add S3 upload → compose StorageEngine. Each layer independently testable. Zero-copy throughout. Property tests for all invariants.

**Tech Stack:** Rust, `object_store` (S3-compatible), `tokio` (async runtime handle from caller), `bytes` (zero-copy), `proptest`

**Spec:** `docs/superpowers/specs/2026-03-12-ferrosa-storage-part-c-design.md`

---

## File Structure

### Modified files

| File | Changes |
|------|---------|
| `ferrosa-storage/Cargo.toml` | Add `object_store`, `tokio`, `bytes` dependencies |
| `ferrosa-storage/src/lib.rs` | Add `compaction`, `upload`, `manifest`, `cache`, `engine` modules + re-exports |
| `ferrosa-storage/src/commitlog/segment.rs` | Add `in_flight_writers: AtomicU64`, `file_handle: Mutex<Option<File>>`, `last_flushed: AtomicU64`; modify `flush_to_disk()` |
| `ferrosa-storage/src/commitlog/sync.rs` | Wire sync marker calls into `BatchSync::on_write()`, `PeriodicSync` flush callback, `GroupSync` flush callback |
| `ferrosa-storage/src/commitlog/mod.rs` | Update `create_sync_strategy` to pass segment ref for marker wiring |

### New files

| File | Purpose |
|------|---------|
| `ferrosa-storage/src/compaction/mod.rs` | Module declarations, re-exports |
| `ferrosa-storage/src/compaction/metadata.rs` | `SSTableMetadata`, `CompactionTask` types |
| `ferrosa-storage/src/compaction/strategy.rs` | `CompactionStrategy` trait, `SizeTieredStrategy` |
| `ferrosa-storage/src/compaction/executor.rs` | `CompactionExecutor` — background thread, channel-based |
| `ferrosa-storage/src/upload/mod.rs` | Module declarations, re-exports |
| `ferrosa-storage/src/upload/config.rs` | `ObjectStoreConfig::from_env()` |
| `ferrosa-storage/src/upload/manager.rs` | `UploadManager` — tokio task, bounded channel, retry |
| `ferrosa-storage/src/manifest.rs` | `Manifest` — S3 JSON manifest with CAS updates |
| `ferrosa-storage/src/cache.rs` | `LocalCache` — LRU eviction by access time |
| `ferrosa-storage/src/engine.rs` | `StorageEngine` — top-level composition |
| `ferrosa-storage/tests/compaction_property.rs` | Property tests for compaction |
| `ferrosa-storage/tests/engine_integration.rs` | Integration tests for StorageEngine |
| `ferrosa-storage/tests/engine_property.rs` | Property tests for StorageEngine |

---

## Chunk 1: Commit Log Hardening

### Task 1: In-flight Write Counter

**Files:**

- Modify: `ferrosa-storage/src/commitlog/segment.rs`
- Test: `ferrosa-storage/src/commitlog/segment.rs` (inline tests)

- [ ] **Step 1: Add `in_flight_writers` field to `Segment`**

Add `in_flight_writers: AtomicU64` to the `Segment` struct. Initialize to 0 in `Segment::new()`. This field tracks how many writers are between `allocate()` and `write_entry()` completion.

- [ ] **Step 2: Add `wait_for_writers()` method**

```rust
/// Spins until all in-flight writers have completed their entries.
/// Called by `flush_to_disk()` before reading the buffer.
fn wait_for_writers(&self) {
    let mut spins = 0;
    while self.in_flight_writers.load(Ordering::Acquire) > 0 {
        spins += 1;
        if spins > 1000 {
            std::thread::yield_now();
        } else {
            std::hint::spin_loop();
        }
    }
}
```

- [ ] **Step 3: Modify `write_entry()` to increment/decrement counter**

Wrap the body of `write_entry()`:

```rust
pub fn write_entry(&self, offset: u64, mutation: &Mutation) -> CommitLogPosition {
    self.in_flight_writers.fetch_add(1, Ordering::AcqRel);
    // ... existing write logic ...
    self.in_flight_writers.fetch_sub(1, Ordering::AcqRel);
    position
}
```

Use a scope guard (or manual fetch_sub) to ensure decrement happens even on panic.

- [ ] **Step 4: Modify `flush_to_disk()` to wait for writers**

Add `self.wait_for_writers();` as the first line of `flush_to_disk()`, before reading the buffer.

- [ ] **Step 5: Write property test — concurrent writes + flush never captures partial entry**

Add a test that spawns 8 writer threads and 2 flusher threads concurrently. After all complete, read back the flushed file and verify every entry has valid CRCs. Use the existing `SegmentReader` to validate.

- [ ] **Step 6: Run tests**

Run: `cargo test -p ferrosa-storage`
Expected: All tests pass including the new concurrent write+flush test.

- [ ] **Step 7: Commit**

```
feat(storage): add in-flight write counter to Segment
```

---

### Task 2: Incremental Flush

**Files:**

- Modify: `ferrosa-storage/src/commitlog/segment.rs`
- Test: `ferrosa-storage/src/commitlog/segment.rs` (inline tests)

- [ ] **Step 1: Add persistent file handle and last-flushed position**

Add two fields to `Segment`:

```rust
file_handle: Mutex<Option<std::fs::File>>,
last_flushed: AtomicU64,
```

Initialize `file_handle` to `None` and `last_flushed` to `INITIAL_POSITION` in `Segment::new()`.

- [ ] **Step 2: Rewrite `flush_to_disk()` for incremental append**

```rust
pub fn flush_to_disk(&self) -> ferrosa_common::Result<()> {
    self.wait_for_writers();

    let current_pos = self.position.load(Ordering::Acquire) as usize;
    let last_flushed = self.last_flushed.load(Ordering::Acquire) as usize;

    if current_pos <= last_flushed {
        return Ok(()); // Nothing new to flush
    }

    let buf = unsafe { &*self.buffer.get() };

    let mut handle = self.file_handle.lock();
    let file = match handle.as_mut() {
        Some(f) => f,
        None => {
            // First flush: write header + everything up to current_pos
            if let Some(parent) = self.path.parent() {
                fs::create_dir_all(parent)?;
            }
            let f = fs::OpenOptions::new()
                .create(true)
                .write(true)
                .open(&self.path)?;
            *handle = Some(f);
            // Write from beginning on first open
            let file = handle.as_mut().unwrap();
            use std::io::Write;
            file.write_all(&buf[..current_pos])?;
            file.sync_all()?;
            self.last_flushed.store(current_pos as u64, Ordering::Release);
            return Ok(());
        }
    };

    // Incremental: append only new bytes
    use std::io::Write;
    file.write_all(&buf[last_flushed..current_pos])?;
    file.sync_all()?;
    self.last_flushed.store(current_pos as u64, Ordering::Release);

    Ok(())
}
```

- [ ] **Step 3: Write test — incremental flush produces same file as full flush**

Create a segment, write 3 entries, flush after each. Read back the file and verify all 3 entries are valid. Compare with a reference segment that writes all 3 then does a single flush.

- [ ] **Step 4: Write test — flush with no new data is a no-op**

Flush, then flush again. Second flush should return Ok without writing.

- [ ] **Step 5: Run all tests**

Run: `cargo test -p ferrosa-storage`
Expected: All tests pass. Existing tests that call `flush_to_disk()` should work with the new incremental approach.

- [ ] **Step 6: Commit**

```
feat(storage): add incremental flush with persistent file handle
```

---

### Task 3: Sync Marker Wiring

**Files:**

- Modify: `ferrosa-storage/src/commitlog/sync.rs`
- Modify: `ferrosa-storage/src/commitlog/segment.rs` (if needed for marker position tracking)
- Test: `ferrosa-storage/tests/commitlog_property.rs` (add marker chain test)

- [ ] **Step 1: Wire sync markers into `BatchSync::on_write()`**

After `segment.flush_to_disk()`, call `segment.write_sync_marker(0)` to write an EOF marker at the current position. This creates a marker after each batch of writes.

Note: The sync marker needs to be allocated first (it consumes 8 bytes). Modify the approach: after flushing, allocate `SYNC_MARKER_SIZE` bytes, write the marker at that offset, then the next flush will include it.

- [ ] **Step 2: Wire sync markers into `PeriodicSync` and `GroupSync` flush callbacks**

The flush callbacks in `CommitLog::create_sync_strategy()` currently just call `segment.flush_to_disk()`. After flushing, also write a sync marker:

```rust
let flush_callback: FlushCallback = Arc::new(move || {
    let segment = active_ref.load();
    segment.flush_to_disk()?;
    // Write EOF sync marker at current position
    if let Some(_offset) = segment.allocate(SYNC_MARKER_SIZE) {
        segment.write_sync_marker(0);
    }
    Ok(())
});
```

- [ ] **Step 3: Update `write_sync_marker()` to accept allocated offset**

Currently `write_sync_marker()` reads the position atomically and advances it. Change it to accept an explicit offset (from a prior `allocate()` call) to avoid double-advancing:

```rust
pub fn write_sync_marker_at(&self, offset: u64, next_marker_offset: u32) {
    let buf = unsafe { &mut *self.buffer.get() };
    Self::write_sync_marker_to_buffer(buf, offset as usize, self.id, next_marker_offset);
}
```

Keep the old `write_sync_marker()` for backward compatibility or remove if unused.

- [ ] **Step 4: Write property test — marker chain is correctly linked**

Write a test that appends mutations with `BatchSync`, shuts down, reads the file, and walks the sync marker chain from the initial marker. Verify that each marker points to the next (or 0 for EOF), and that all marker CRCs are valid.

- [ ] **Step 5: Run all tests**

Run: `cargo test -p ferrosa-storage`
Expected: All tests pass. Reader tests should still work since the reader already handles sync markers.

- [ ] **Step 6: Commit**

```
feat(storage): wire sync marker chain into sync strategies
```

---

## Chunk 2: Compaction

### Task 4: Compaction Types and Strategy Trait

**Files:**

- Create: `ferrosa-storage/src/compaction/mod.rs`
- Create: `ferrosa-storage/src/compaction/metadata.rs`
- Create: `ferrosa-storage/src/compaction/strategy.rs`
- Modify: `ferrosa-storage/src/lib.rs`

- [ ] **Step 1: Create module skeleton**

Create `ferrosa-storage/src/compaction/mod.rs`:

```rust
pub(crate) mod metadata;
pub(crate) mod strategy;
pub(crate) mod executor;

pub use metadata::{SSTableMetadata, CompactionTask};
pub use strategy::{CompactionStrategy, SizeTieredStrategy, CompactionConfig};
pub use executor::CompactionExecutor;
```

Add `pub mod compaction;` to `lib.rs`.

- [ ] **Step 2: Define `SSTableMetadata` and `CompactionTask`**

Create `metadata.rs`:

```rust
#[derive(Debug, Clone)]
pub struct SSTableMetadata {
    pub id: String,
    pub path: PathBuf,
    pub size_bytes: u64,
    pub min_token: i64,
    pub max_token: i64,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub partition_count: u64,
}

#[derive(Debug, Clone)]
pub struct CompactionTask {
    pub inputs: Vec<SSTableMetadata>,
    pub output_dir: PathBuf,
}
```

- [ ] **Step 3: Define `CompactionStrategy` trait and `CompactionConfig`**

Create `strategy.rs`:

```rust
pub trait CompactionStrategy: Send + Sync {
    fn select(&self, sstables: &[SSTableMetadata]) -> Vec<CompactionTask>;
}

pub struct CompactionConfig {
    pub min_threshold: usize,     // default 4
    pub max_threshold: usize,     // default 32
    pub bucket_low: f64,          // default 0.5
    pub bucket_high: f64,         // default 1.5
    pub output_dir: PathBuf,
}
```

Add `CompactionConfig::from_env()` reading `FERROSA_COMPACTION_*` env vars with defaults.

- [ ] **Step 4: Implement `SizeTieredStrategy`**

```rust
pub struct SizeTieredStrategy {
    config: CompactionConfig,
}

impl CompactionStrategy for SizeTieredStrategy {
    fn select(&self, sstables: &[SSTableMetadata]) -> Vec<CompactionTask> {
        // 1. Sort SSTables by size
        // 2. Group into buckets where sizes are within [bucket_low, bucket_high] ratio of bucket median
        // 3. For each bucket with >= min_threshold SSTables, create a CompactionTask
        //    (cap at max_threshold)
        // 4. Return tasks
    }
}
```

The bucketing algorithm: iterate sorted SSTables, start a new bucket when the next SSTable's size exceeds `bucket_high * current_median`. Use a sliding window approach.

- [ ] **Step 5: Write unit tests for bucket selection**

Test cases:

- 4 similarly-sized SSTables → 1 task with all 4
- 8 SSTables in 2 size groups → 2 tasks
- 3 SSTables (below threshold) → 0 tasks
- Mix of sizes → correct bucketing
- max_threshold respected

- [ ] **Step 6: Write property test — deterministic selection**

Given the same input set (order-independent), `select()` always returns the same tasks.

- [ ] **Step 7: Run tests, commit**

```
feat(storage): add CompactionStrategy trait and SizeTieredStrategy
```

---

### Task 5: Compaction Executor

**Files:**

- Create: `ferrosa-storage/src/compaction/executor.rs`
- Modify: `ferrosa-storage/src/compaction/mod.rs`

- [ ] **Step 1: Define `CompactionExecutor` struct**

```rust
pub struct CompactionExecutor {
    task_tx: std::sync::mpsc::Sender<CompactionTask>,
    result_rx: Mutex<std::sync::mpsc::Receiver<CompactionResult>>,
    handle: Mutex<Option<std::thread::JoinHandle<()>>>,
    stop_flag: Arc<AtomicBool>,
}

pub struct CompactionResult {
    pub task: CompactionTask,
    pub output: SSTableMetadata,
}
```

- [ ] **Step 2: Implement `new()` and background thread**

The background thread:

1. Receives `CompactionTask` from channel
2. Opens input SSTables via `SSTableReader`
3. Reads all partitions from each input
4. Merges using `merge_partitions()` (existing function)
5. Writes merged output via `SSTableWriter`
6. Sends `CompactionResult` back

The executor needs access to the `TableSchema` and `WriteOptions` for the SSTableWriter. These should be passed in `CompactionTask` or stored in the executor.

- [ ] **Step 3: Implement `submit()`, `poll_results()`, `shutdown()`**

```rust
pub fn submit(&self, task: CompactionTask) -> Result<()>
pub fn poll_results(&self) -> Vec<CompactionResult>
pub fn shutdown(&self)
```

- [ ] **Step 4: Write integration test — compact 4 SSTables into 1**

Create 4 SSTables with known data (some overlapping keys), submit compaction, verify output contains merged data with correct LWW semantics.

- [ ] **Step 5: Write property test — compaction preserves all live data**

Generate N random SSTables with proptest, compact them, verify the output contains exactly the union of live data (tombstones suppress older values).

- [ ] **Step 6: Write property test — compaction is idempotent**

Compact N SSTables → output O. Compact [O] → output O'. Verify O and O' have identical logical content.

- [ ] **Step 7: Run tests, commit**

```
feat(storage): add CompactionExecutor with background merge thread
```

---

## Chunk 3: S3 Upload Manager

### Task 6: Add New Dependencies

**Files:**

- Modify: `ferrosa-storage/Cargo.toml`

- [ ] **Step 1: Add dependencies**

```toml
[dependencies]
# ... existing ...
object_store = { version = "0.11", features = ["aws"] }
tokio = { version = "1", features = ["rt", "sync", "time"] }
bytes = "1"
```

Check the latest `object_store` version and use that.

- [ ] **Step 2: Verify build**

Run: `cargo build -p ferrosa-storage`
Expected: Builds successfully with new dependencies.

- [ ] **Step 3: Commit**

```
chore(storage): add object_store, tokio, bytes dependencies
```

---

### Task 7: ObjectStoreConfig

**Files:**

- Create: `ferrosa-storage/src/upload/mod.rs`
- Create: `ferrosa-storage/src/upload/config.rs`
- Modify: `ferrosa-storage/src/lib.rs`

- [ ] **Step 1: Create module skeleton**

Create `ferrosa-storage/src/upload/mod.rs`:

```rust
pub(crate) mod config;
pub(crate) mod manager;

pub use config::ObjectStoreConfig;
pub use manager::UploadManager;
```

Add `pub mod upload;` to `lib.rs`.

- [ ] **Step 2: Implement `ObjectStoreConfig`**

```rust
pub struct ObjectStoreConfig {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub allow_http: bool,
    pub prefix: String,
    pub upload_queue_depth: usize,
}

impl ObjectStoreConfig {
    pub fn from_env() -> ferrosa_common::Result<Self> {
        // Read FERROSA_S3_* env vars
        // Required: FERROSA_S3_ENDPOINT, FERROSA_S3_BUCKET
        // Optional with defaults: region, allow_http, prefix, queue_depth
    }

    pub fn build_object_store(&self) -> ferrosa_common::Result<Box<dyn object_store::ObjectStore>> {
        // Use object_store::aws::AmazonS3Builder
    }
}
```

- [ ] **Step 3: Write unit tests**

Test `from_env()` with env vars set/unset, verify defaults, verify error on missing required vars. Use `std::env::set_var` in tests (sequential, not parallel).

- [ ] **Step 4: Run tests, commit**

```
feat(storage): add ObjectStoreConfig with 12-factor env var loading
```

---

### Task 8: Manifest

**Files:**

- Create: `ferrosa-storage/src/manifest.rs`
- Modify: `ferrosa-storage/src/lib.rs`

- [ ] **Step 1: Define `Manifest` struct**

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub format_version: u32,
    pub sstables: HashMap<String, Vec<ManifestEntry>>,  // table_id -> entries
    pub last_compacted_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub id: String,
    pub size: u64,
    pub min_token: i64,
    pub max_token: i64,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
}
```

- [ ] **Step 2: Implement `load()` and `save()` with CAS**

```rust
impl Manifest {
    pub fn new() -> Self { ... }

    /// Load manifest from object store. Returns (manifest, etag) for CAS.
    pub async fn load(store: &dyn ObjectStore, prefix: &str) -> Result<(Self, Option<String>)>

    /// Save manifest with conditional put (etag-based CAS).
    /// Returns Err if etag doesn't match (concurrent update).
    pub async fn save(&self, store: &dyn ObjectStore, prefix: &str, etag: Option<&str>) -> Result<()>

    /// Add an SSTable entry.
    pub fn add_sstable(&mut self, table_id: &str, entry: ManifestEntry)

    /// Remove SSTable entries by ID (after compaction replaces them).
    pub fn remove_sstables(&mut self, table_id: &str, ids: &[String])
}
```

- [ ] **Step 3: Write tests with `object_store::memory::InMemory`**

Test load/save round-trip, CAS conflict detection, add/remove operations.

- [ ] **Step 4: Write property test — concurrent manifest updates never lose entries**

Simulate N concurrent add operations. After all complete (with retries on CAS conflict), verify all entries are present.

- [ ] **Step 5: Run tests, commit**

```
feat(storage): add Manifest with CAS-based atomic S3 updates
```

---

### Task 9: Upload Manager

**Files:**

- Create: `ferrosa-storage/src/upload/manager.rs`

- [ ] **Step 1: Define `UploadManager` struct**

```rust
pub struct UploadManager {
    task_tx: tokio::sync::mpsc::Sender<UploadTask>,
    handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

pub enum UploadTask {
    SSTable {
        table_id: String,
        sstable_id: String,
        files: Vec<(String, Bytes)>,  // (component_name, data)
    },
    Shutdown,
}
```

- [ ] **Step 2: Implement `new()` with tokio task**

```rust
impl UploadManager {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        prefix: String,
        queue_depth: usize,
        runtime: &tokio::runtime::Handle,
    ) -> Self {
        let (tx, mut rx) = tokio::sync::mpsc::channel(queue_depth);
        let handle = runtime.spawn(async move {
            while let Some(task) = rx.recv().await {
                match task {
                    UploadTask::SSTable { table_id, sstable_id, files } => {
                        // Upload each component file with retry
                        for (name, data) in files {
                            let path = object_store::path::Path::from(
                                format!("{prefix}/sstables/{table_id}/{sstable_id}/{name}")
                            );
                            Self::put_with_retry(&store, &path, data).await;
                        }
                    }
                    UploadTask::Shutdown => break,
                }
            }
        });
        Self { task_tx: tx, handle: Mutex::new(Some(handle)) }
    }
}
```

- [ ] **Step 3: Implement `submit()`, `shutdown()`, retry logic**

`submit()` sends via the bounded channel (blocks when full = backpressure).
`shutdown()` sends `Shutdown` task, awaits the join handle.
Retry: exponential backoff (100ms, 200ms, 400ms, ...) up to 5 retries on transient errors.

- [ ] **Step 4: Write tests with `InMemory` object store**

Test upload round-trip: submit SSTable files, verify they appear in the in-memory store at the correct paths.

- [ ] **Step 5: Write backpressure test**

Create manager with queue_depth=1, submit 2 tasks, verify the second blocks until the first completes.

- [ ] **Step 6: Run tests, commit**

```
feat(storage): add UploadManager with backpressure and retry
```

---

### Task 10: Local Cache

**Files:**

- Create: `ferrosa-storage/src/cache.rs`
- Modify: `ferrosa-storage/src/lib.rs`

- [ ] **Step 1: Define `LocalCache` struct**

```rust
pub struct LocalCache {
    base_dir: PathBuf,
    max_bytes: u64,
    entries: Mutex<HashMap<String, CacheEntry>>,
}

struct CacheEntry {
    path: PathBuf,
    size: u64,
    last_accessed: Instant,
}
```

- [ ] **Step 2: Implement cache operations**

```rust
impl LocalCache {
    pub fn new(base_dir: PathBuf, max_bytes: u64) -> Self
    pub fn register(&self, id: &str, path: PathBuf, size: u64)
    pub fn touch(&self, id: &str)  // Update last_accessed
    pub fn evict_if_needed(&self) -> Vec<PathBuf>  // Returns paths deleted
    pub fn total_size(&self) -> u64
    pub fn contains(&self, id: &str) -> bool
    pub fn get_path(&self, id: &str) -> Option<PathBuf>
}
```

`evict_if_needed()`: if total_size > max_bytes, sort entries by last_accessed ascending, delete oldest until under limit. Never delete entries that are in a "pinned" set (referenced by current manifest).

- [ ] **Step 3: Write unit tests**

- Register files, verify total size
- Eviction removes oldest when over limit
- Touch updates access time, prevents eviction

- [ ] **Step 4: Write property test — eviction never reduces below max_bytes needlessly**

- [ ] **Step 5: Run tests, commit**

```
feat(storage): add LocalCache with LRU eviction
```

---

## Chunk 4: StorageEngine

### Task 11: StorageEngineConfig

**Files:**

- Create: `ferrosa-storage/src/engine.rs`
- Modify: `ferrosa-storage/src/lib.rs`

- [ ] **Step 1: Define `StorageEngineConfig`**

```rust
pub struct StorageEngineConfig {
    pub commit_log: CommitLogConfig,
    pub compaction: CompactionConfig,
    pub object_store: ObjectStoreConfig,
    pub local_cache_max_bytes: u64,
    pub flush_threshold_bytes: u64,
    pub data_dir: PathBuf,
}

impl StorageEngineConfig {
    pub fn from_env() -> ferrosa_common::Result<Self> {
        // Compose all sub-configs from FERROSA_* env vars
    }

    #[cfg(test)]
    pub fn test_config(dir: &Path) -> Self {
        // In-memory/local test config with small thresholds
    }
}
```

- [ ] **Step 2: Write tests for `from_env()`**

- [ ] **Step 3: Commit**

```
feat(storage): add StorageEngineConfig with from_env()
```

---

### Task 12: StorageEngine Core — new, write, read

**Files:**

- Modify: `ferrosa-storage/src/engine.rs`

- [ ] **Step 1: Define `StorageEngine` struct**

```rust
pub struct StorageEngine {
    config: StorageEngineConfig,
    tables: parking_lot::RwLock<HashMap<TableId, TableStore<FileFlushTarget>>>,
    commit_log: CommitLog,
    compaction_executor: CompactionExecutor,
    upload_manager: Option<UploadManager>,
    local_cache: LocalCache,
    runtime: tokio::runtime::Handle,
}
```

`upload_manager` is `Option` so tests can run without S3.

- [ ] **Step 2: Implement `new()`**

```rust
impl StorageEngine {
    pub fn new(
        config: StorageEngineConfig,
        runtime: tokio::runtime::Handle,
    ) -> ferrosa_common::Result<Self> {
        // 1. Create data directory
        // 2. Create CommitLog
        // 3. Create CompactionExecutor
        // 4. Create UploadManager (if S3 config provided)
        // 5. Create LocalCache
        // 6. Return Self
    }
}
```

- [ ] **Step 3: Implement `write()` and `read()`**

```rust
pub fn write(
    &self,
    table_id: &TableId,
    schema: &TableSchema,
    key: &DecoratedKey,
    row: Row,
) -> ferrosa_common::Result<()> {
    // 1. Append to commit log
    let mutation = Mutation { ... };
    self.commit_log.append(&mutation)?;
    // 2. Write to table's memtable
    let tables = self.tables.read();
    // Get or create TableStore for this table
    // ...
    table_store.write(key, row)?;
    Ok(())
}

pub fn read(
    &self,
    table_id: &TableId,
    key: &DecoratedKey,
) -> ferrosa_common::Result<Option<Partition>> {
    let tables = self.tables.read();
    match tables.get(table_id) {
        Some(store) => store.read(key),
        None => Ok(None),
    }
}
```

- [ ] **Step 4: Write basic test — write then read**

- [ ] **Step 5: Run tests, commit**

```
feat(storage): add StorageEngine with write and read
```

---

### Task 13: StorageEngine — flush and compaction integration

**Files:**

- Modify: `ferrosa-storage/src/engine.rs`

- [ ] **Step 1: Implement `flush()`**

```rust
pub fn flush(&self, table_id: &TableId) -> ferrosa_common::Result<()> {
    let tables = self.tables.read();
    if let Some(store) = tables.get(table_id) {
        store.flush()?;
        // Discard commit log entries for this table
        // (need to track latest position per table)
        // Check if compaction needed
        let metadata = self.collect_sstable_metadata(table_id);
        let strategy = SizeTieredStrategy::new(self.config.compaction.clone());
        let tasks = strategy.select(&metadata);
        for task in tasks {
            self.compaction_executor.submit(task)?;
        }
    }
    Ok(())
}
```

- [ ] **Step 2: Implement `poll_compactions()` to integrate results**

When a compaction completes, swap old SSTables for the new one in the TableStore's view.

- [ ] **Step 3: Write integration test — write → flush → compact → verify**

Write enough data to trigger compaction (write many small batches, flush each), then verify all data is still readable after compaction.

- [ ] **Step 4: Run tests, commit**

```
feat(storage): integrate flush and compaction into StorageEngine
```

---

### Task 14: StorageEngine — S3 upload integration

**Files:**

- Modify: `ferrosa-storage/src/engine.rs`

- [ ] **Step 1: Wire upload into flush pipeline**

After `store.flush()` produces new SSTable files, submit them to the `UploadManager`. After compaction produces output, submit that too. After uploads complete, update the manifest.

- [ ] **Step 2: Implement manifest update after upload**

```rust
async fn update_manifest_after_upload(&self, table_id: &str, entry: ManifestEntry) -> Result<()> {
    // CAS loop: load manifest, add entry, save with etag
    loop {
        let (mut manifest, etag) = Manifest::load(&*self.store, &self.config.object_store.prefix).await?;
        manifest.add_sstable(table_id, entry.clone());
        match manifest.save(&*self.store, &self.config.object_store.prefix, etag.as_deref()).await {
            Ok(()) => return Ok(()),
            Err(e) if is_precondition_failed(&e) => continue, // Retry
            Err(e) => return Err(e),
        }
    }
}
```

- [ ] **Step 3: Write integration test with `InMemory` object store**

Write data, flush, verify SSTable appears in the in-memory S3 store and manifest is updated.

- [ ] **Step 4: Run tests, commit**

```
feat(storage): integrate S3 upload and manifest into StorageEngine
```

---

### Task 15: StorageEngine — recovery and shutdown

**Files:**

- Modify: `ferrosa-storage/src/engine.rs`

- [ ] **Step 1: Implement `open()` for recovery**

```rust
pub fn open(
    config: StorageEngineConfig,
    runtime: tokio::runtime::Handle,
) -> ferrosa_common::Result<Self> {
    // 1. Load manifest from S3 (or empty if first boot)
    // 2. Ensure local cache has all SSTables listed in manifest
    //    (fetch from S3 if missing)
    // 3. Open CommitLog and replay mutations
    // 4. Replay mutations into appropriate TableStore memtables
    // 5. Return ready-to-use StorageEngine
}
```

- [ ] **Step 2: Implement `shutdown()`**

```rust
pub fn shutdown(&self) -> ferrosa_common::Result<()> {
    // 1. Flush all dirty memtables
    // 2. Upload any pending SSTables
    // 3. Stop compaction executor
    // 4. Shutdown upload manager (drain queue)
    // 5. Shutdown commit log
    Ok(())
}
```

- [ ] **Step 3: Write recovery test — write → shutdown → open → read**

Write mutations, shutdown, re-open with `open()`, verify all data is readable.

- [ ] **Step 4: Write property test — write N mutations → shutdown → recover → all readable**

Use proptest to generate random mutation sequences, verify perfect recovery.

- [ ] **Step 5: Write property test — concurrent writes + flushes + compactions → no data loss**

Multi-threaded test: writers, flushers, and compaction all running concurrently. After all complete + shutdown + reopen, all written data is readable.

- [ ] **Step 6: Run all tests**

Run: `cargo test -p ferrosa-storage`
Expected: All tests pass.

- [ ] **Step 7: Commit**

```
feat(storage): add StorageEngine recovery and shutdown
```

---

### Task 16: Update lib.rs exports and documentation

**Files:**

- Modify: `ferrosa-storage/src/lib.rs`

- [ ] **Step 1: Add module declarations and re-exports**

```rust
pub mod compaction;
pub mod upload;
pub mod manifest;
pub mod cache;
pub mod engine;

pub use compaction::{CompactionStrategy, SizeTieredStrategy, CompactionConfig, CompactionExecutor};
pub use upload::{ObjectStoreConfig, UploadManager};
pub use manifest::Manifest;
pub use cache::LocalCache;
pub use engine::{StorageEngine, StorageEngineConfig};
```

- [ ] **Step 2: Update module-level docs**

Update the `//! # Components` section to include Part C additions.

- [ ] **Step 3: Run full test suite + clippy + doc check**

```bash
cargo test --workspace
cargo clippy --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --workspace
```

- [ ] **Step 4: Commit**

```
docs(storage): update module docs and re-exports for Part C
```

---

### Task 17: Property test suite for StorageEngine

**Files:**

- Create: `ferrosa-storage/tests/compaction_property.rs`
- Create: `ferrosa-storage/tests/engine_integration.rs`
- Create: `ferrosa-storage/tests/engine_property.rs`

- [ ] **Step 1: Create compaction property tests**

Tests:

- STCS bucket selection is deterministic
- Compacted output contains all live data
- Tombstone suppression works across compaction
- Compaction idempotency

- [ ] **Step 2: Create engine integration tests**

Tests:

- Write → read round-trip
- Write → flush → read
- Write → flush → compact → read
- Multi-table isolation
- Recovery after shutdown

- [ ] **Step 3: Create engine property tests**

Tests:

- Random write sequences → recovery preserves all data
- Concurrent writers → all writes durable
- Flush threshold triggers correctly

- [ ] **Step 4: Run all tests**

Run: `cargo test -p ferrosa-storage`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```
test(storage): add comprehensive property and integration tests for Part C
```
