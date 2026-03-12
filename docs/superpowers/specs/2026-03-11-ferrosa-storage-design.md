# ferrosa-storage Design

> **Date:** 2026-03-11
> **Status:** Approved
> **Approach:** Bottom-up by component, strict TDD (red-green-refactor)
> **Methodology:** Literate programming (swdev) — module docs, doc-tests, property tests

## Goal

Implement `ferrosa-storage`: the single-node storage engine for Ferrosa. Handles writes to memtable, flush to SSTable, read-path merge, and eventually commit log, compaction, and S3 write-behind.

## Scope Decomposition

ferrosa-storage is split into three independent implementation cycles:

| Part | Scope | Key Deps |
|------|-------|----------|
| **A** | Memtable + Flush + Read-path merge | `ferrosa-common`, `ferrosa-sstable`, `arc_swap`, `parking_lot` |
| **B** | Commit log (WAL, segments, replay) | Part A + versioned segment format |
| **C** | Compaction + S3 manager + storage engine composition | Part A + B + `aws-sdk-s3`, `tokio` |

Each part has its own spec-plan-implement cycle. This document covers the full design. Implementation plans will be written per-part.

### Versioned Protocols

No versioned protocols between in-process modules — they share Rust types directly and compile together. Versioned format headers are required for **persisted artifacts** that must survive rolling upgrades:

| Artifact | Part | Versioning Strategy |
|----------|------|---------------------|
| Commit log segments | B | Version byte in segment header |
| `manifest.json` (S3) | C | `"format_version"` field in JSON |
| `checkpoint.json` (S3) | C | `"format_version"` field in JSON |

## Crate Structure

```
ferrosa-common/src/
  schema.rs               # TableSchema, ColumnDefinition (new)

ferrosa-storage/
  Cargo.toml              # ferrosa-common, ferrosa-sstable, arc_swap, parking_lot
  src/
    lib.rs                # Public API re-exports
    memtable/
      mod.rs              # Memtable trait
      sharded.rs          # ShardedBTreeMemtable (64 shards, parking_lot RwLock)
    flush.rs              # FlushTarget trait + InMemoryFlushTarget + FileFlushTarget
    store.rs              # TableStore — lock-free ArcSwap composition
    merge.rs              # Read-path merge (cell-level LWW, deletion suppression)
  tests/
    integration.rs        # Module integration tests (write → flush → read)
```

### External Dependencies

| Crate | Purpose |
|-------|---------|
| `ferrosa-common` | Token, DecoratedKey, CellValue, TableSchema, Error/Result |
| `ferrosa-sstable` | SSTableReader, SSTableWriter, ReadAt, WriteAt |
| `arc_swap` | Lock-free atomic `Arc` swaps for StoreView |
| `parking_lot` | Fast RwLock for memtable shards (shorter critical sections than std) |
| `proptest` (dev) | Property-based testing |
| `tempfile` (dev) | Temporary directories for file-backed flush tests |

## Design Decisions

### Lock-Free Read Path

The storage engine uses `ArcSwap<StoreView>` to make the read path completely wait-free. State transitions (flush, compaction) create a new immutable `StoreView` and atomically swap the pointer. Readers load the current view (single atomic load), hold an `Arc` to their snapshot, and can never be invalidated mid-read.

This eliminates `RwLock` from the read-write path between components. The only locks in the system are the per-shard `RwLock`s inside the memtable, which protect individual BTreeMap shards during concurrent writes.

### Arc\<Partition> Storage

Memtable stores `Arc<Partition>` values to avoid deep-cloning partition data on reads. A read-lock on a shard, followed by an `Arc::clone()` (pointer increment), then immediate lock release. This keeps the read-side critical section to nanoseconds.

### Memtable as a Trait

The `Memtable` trait abstracts over the backing data structure. Part A ships `ShardedBTreeMemtable` (simple, correct). The lock-free upgrade path (crossbeam-skiplist or Okasaki-style persistent structures) swaps in a new implementation without changing any consumer code.

### Parallel Flush Pipeline

Flush exploits parallelism at two points:

1. **Parallel shard drain**: 64 independent shards are snapshot concurrently via `std::thread::scope`, then k-way merged into token order.
2. **Parallel file writes**: `FileFlushTarget` writes up to 7 SSTable component files in parallel (they are independent byte buffers; `CompressionInfo.db` is omitted when compression is disabled).

## ferrosa-common: TableSchema

```rust
/// Describes a table's column structure.
/// Shared between ferrosa-storage and ferrosa-schema.
///
/// Note: ferrosa-common does NOT depend on ferrosa-sstable.
/// Conversion to SerializationHeader lives in ferrosa-storage (flush.rs),
/// which depends on both crates.
pub struct TableSchema {
    pub keyspace: String,
    pub table: String,
    pub key_type: String,                           // Cassandra type class name
    pub clustering_columns: Vec<ColumnDefinition>,
    pub static_columns: Vec<ColumnDefinition>,
    pub regular_columns: Vec<ColumnDefinition>,     // ordered by column index
}

pub struct ColumnDefinition {
    pub name: String,
    pub type_name: String,  // Cassandra type class name
}

impl TableSchema {
    /// Get clustering column type names.
    pub fn clustering_types(&self) -> Vec<String>;

    /// Look up a column's index by name.
    pub fn column_index(&self, name: &str) -> Option<u16>;
}
```

The conversion to `SerializationHeader` lives in `ferrosa-storage::flush`, not in ferrosa-common, to avoid a circular dependency (ferrosa-common cannot depend on ferrosa-sstable). The `min_timestamp`, `min_local_deletion_time`, and `min_ttl` fields required by `SerializationHeader` are computed by scanning the partition data during flush:

```rust
// In ferrosa-storage::flush
fn build_serialization_header(
    schema: &TableSchema,
    partitions: &[Partition],
) -> SerializationHeader {
    // Scan partitions to compute min_timestamp, min_local_deletion_time, min_ttl
    // These are per-SSTable statistics, not schema-level constants
    ...
}
```

## Memtable

### Trait

```rust
/// In-memory write buffer for a single table.
/// Implementations must be thread-safe for concurrent reads and writes.
///
/// The trait is designed for lock-free upgrade: ShardedBTreeMemtable now,
/// crossbeam-skiplist or persistent data structures later.
pub trait Memtable: Send + Sync {
    /// Insert or update a row. Merges with existing data by timestamp
    /// (cell-level last-write-wins).
    fn put(&self, key: &DecoratedKey, row: Row, schema: &TableSchema) -> Result<()>;

    /// Read a single partition. Returns Arc to avoid deep clones.
    fn get(&self, key: &DecoratedKey) -> Result<Option<Arc<Partition>>>;

    /// Collect all partitions in token order. Uses &self because the
    /// memtable has already been swapped out of the active view —
    /// no new writes are coming.
    fn snapshot(&self) -> Vec<Partition>;

    /// Approximate memory usage in bytes. Wait-free (AtomicUsize).
    fn size_bytes(&self) -> usize;

    /// Number of partitions stored. Wait-free (AtomicUsize).
    fn partition_count(&self) -> usize;
}
```

### ShardedBTreeMemtable

```rust
pub struct ShardedBTreeMemtable {
    shards: Vec<parking_lot::RwLock<BTreeMap<DecoratedKey, Arc<Partition>>>>,
    num_shards: usize,        // default 64
    size: AtomicUsize,        // updated on put, wait-free reads
    count: AtomicUsize,       // updated on put, wait-free reads
}
```

**Shard selection**: `key.token.0 as u64 % num_shards`

**`put()` merge semantics**:

- Write-lock target shard
- If partition exists: merge cells by `(column_index)`, newer timestamp wins. Merge row-level and partition-level deletions (newer wins). Update `Arc<Partition>` in place.
- If new: insert `Arc::new(Partition { ... })`
- Update `AtomicUsize` counters
- Release lock

**`get()`**: Read-lock shard → `Arc::clone()` → release. Nanosecond critical section.

**`snapshot()`**: Parallel drain via `std::thread::scope`:

1. Spawn a thread per shard (or batch), each read-locks its shard and collects `Vec<Partition>`
2. K-way merge the 64 sorted results into a single token-ordered `Vec<Partition>`
3. No write contention — memtable already swapped out of active view

**`size_bytes()` / `partition_count()`**: Read `AtomicUsize` with `Ordering::Relaxed`. Wait-free.

## FlushTarget

```rust
/// Abstraction over where flushed SSTables land.
pub trait FlushTarget: Send + Sync {
    type Reader: ReadAt + Send + Sync + 'static;

    /// Persist an SSTableOutput and return a reader.
    fn flush(&self, output: SSTableOutput) -> Result<SSTableReader<Self::Reader>>;
}

/// In-memory: wraps SSTable components as Vec<u8>. No filesystem.
pub struct InMemoryFlushTarget;

impl FlushTarget for InMemoryFlushTarget {
    type Reader = Vec<u8>;
    // Parses SSTableOutput into SSTableComponents<Vec<u8>>, opens reader.
}

/// File-backed: writes components to a directory, opens file handles.
pub struct FileFlushTarget {
    base_dir: PathBuf,
    generation: AtomicU64,
}

impl FileFlushTarget {
    pub fn new(base_dir: PathBuf) -> Result<Self>;
}

impl FlushTarget for FileFlushTarget {
    type Reader = FileReadAt;
    // Writes 7 component files in parallel (std::thread::scope),
    // opens SSTableReader<FileReadAt>.
}
```

**FileFlushTarget naming**: `{base_dir}/{generation}-{Component}.db` (e.g., `1-Data.db`, `1-Partitions.db`).

**Parallel file writes**: All components (up to 7 — `CompressionInfo.db` is omitted when compression is disabled) are independent byte buffers. `std::thread::scope` writes them concurrently, then opens `SSTableReader` over the written files.

## TableStore (Lock-Free Composition)

```rust
use arc_swap::ArcSwap;

/// Storage engine for a single table.
/// Read path is entirely wait-free via ArcSwap.
/// Flush is serialized via Mutex (only one flush at a time).
pub struct TableStore<F: FlushTarget> {
    schema: TableSchema,
    view: ArcSwap<StoreView<F::Reader>>,
    flush_guard: Mutex<()>,       // serializes flush operations
    flush_target: F,
    options: ferrosa_sstable::WriteOptions,  // re-used directly from sstable crate
}

/// Immutable snapshot of storage state.
/// State transitions create a new StoreView and atomically swap.
struct StoreView<R: ReadAt + Send + Sync + 'static> {
    active: Arc<dyn Memtable>,
    flushing: Option<Arc<dyn Memtable>>,
    sstables: Arc<Vec<Arc<SSTableReader<R>>>>,  // newest first
}
```

### Write Path

```rust
impl<F: FlushTarget> TableStore<F> {
    /// Write a row to the active memtable.
    /// Wait-free view load + single-shard write-lock.
    pub fn write(&self, key: &DecoratedKey, row: Row) -> Result<()> {
        let view = self.view.load();  // atomic, wait-free
        view.active.put(key, row, &self.schema)
    }
}
```

### Read Path

```rust
impl<F: FlushTarget> TableStore<F> {
    /// Read a partition by key. Merges across memtable + flushing + SSTables.
    /// Entirely wait-free at the view level.
    pub fn read(&self, key: &DecoratedKey) -> Result<Option<Partition>> {
        let view = self.view.load();  // atomic, wait-free

        let mut sources: Vec<Partition> = Vec::new();

        // 1. Check active memtable
        if let Some(p) = view.active.get(key)? {
            sources.push((*p).clone());
        }

        // 2. Check flushing memtable (if mid-flush)
        if let Some(ref flushing) = view.flushing {
            if let Some(p) = flushing.get(key)? {
                sources.push((*p).clone());
            }
        }

        // 3. Check flushed SSTables (newest first)
        // SSTableReader::get_partition() handles bloom filter checks internally,
        // returning None immediately if the bloom filter says absent.
        for sstable in view.sstables.iter() {
            if let Some(p) = sstable.get_partition(key)? {
                sources.push(p);
            }
        }

        if sources.is_empty() {
            return Ok(None);
        }

        Ok(Some(merge::merge_partitions(sources)))
    }
}
```

### Flush

```rust
impl<F: FlushTarget> TableStore<F> {
    /// Flush the active memtable to an SSTable.
    /// Takes &self — does not block reads or writes during the slow part.
    ///
    /// Flush is serialized via `flush_guard` Mutex to prevent concurrent
    /// flushes from racing on the ArcSwap (load-then-store is not atomic).
    /// The Mutex is held for the full flush duration, but reads and writes
    /// are completely unaffected — they use ArcSwap::load() which is wait-free.
    pub fn flush(&self) -> Result<()> {
        let _guard = self.flush_guard.lock();  // serialize flushes

        // Step 1: Atomic swap — install fresh memtable, move old to flushing.
        // Writes immediately resume against the new memtable.
        let old_view = self.view.load();
        let old_memtable = old_view.active.clone();
        let new_view = StoreView {
            active: Arc::new(ShardedBTreeMemtable::new(/* num_shards */)),
            flushing: Some(old_memtable.clone()),
            sstables: old_view.sstables.clone(),
        };
        self.view.store(Arc::new(new_view));

        // Step 2: Slow part — flush_guard held but NO view locks.
        // Reads and writes continue against new memtable + flushing memtable.
        let partitions = old_memtable.snapshot();

        if partitions.is_empty() {
            // Nothing to flush — clear flushing state.
            let cur = self.view.load();
            self.view.store(Arc::new(StoreView {
                active: cur.active.clone(),
                flushing: None,
                sstables: cur.sstables.clone(),
            }));
            return Ok(());
        }

        // Build SerializationHeader from schema + partition data.
        // min_timestamp etc. are computed from the data being flushed.
        let header = flush::build_serialization_header(&self.schema, &partitions);
        let mut writer = SSTableWriter::new(self.options.clone(), header);
        for partition in &partitions {
            writer.add_partition(partition)?;
        }
        let output = writer.finish()?;
        let reader = self.flush_target.flush(output)?;

        // Step 3: Atomic swap — add SSTable, clear flushing.
        // Safe because flush_guard prevents concurrent modification.
        let cur = self.view.load();
        let mut new_sstables = (*cur.sstables).clone();
        new_sstables.insert(0, Arc::new(reader));
        self.view.store(Arc::new(StoreView {
            active: cur.active.clone(),
            flushing: None,
            sstables: Arc::new(new_sstables),
        }));

        Ok(())
    }
}
```

### Public API Summary

```rust
impl<F: FlushTarget> TableStore<F> {
    pub fn new(schema: TableSchema, flush_target: F, options: ferrosa_sstable::WriteOptions) -> Self;
    pub fn write(&self, key: &DecoratedKey, row: Row) -> Result<()>;
    pub fn read(&self, key: &DecoratedKey) -> Result<Option<Partition>>;
    pub fn flush(&self) -> Result<()>;
    pub fn sstable_count(&self) -> usize;
    pub fn memtable_size(&self) -> usize;
    pub fn memtable_partition_count(&self) -> usize;
}
```

All methods take `&self`. No `&mut self` anywhere in the public API.

## Read-Path Merge (merge.rs)

```rust
/// Merge partitions from multiple sources (memtable, SSTables).
/// Cell-level last-write-wins by timestamp. Deletions suppress older data.
pub fn merge_partitions(sources: Vec<Partition>) -> Partition;

/// Merge two rows with the same clustering key.
/// Per-cell: newer timestamp wins.
fn merge_rows(a: Row, b: Row) -> Row;

/// Apply deletion semantics: partition-level deletion suppresses all rows
/// older than the deletion timestamp. Row-level deletion suppresses
/// cells in that row older than the row deletion timestamp.
fn apply_deletions(partition: &mut Partition);
```

**Merge rules** (matching Cassandra):

- Partition-level deletion: newest `DeletionTime` wins. Suppresses all rows with `primary_key_liveness.timestamp` < `marked_for_delete_at`.
- Row-level deletion: newest `DeletionTime` wins per clustering key. Suppresses cells with `timestamp` < `marked_for_delete_at`.
- Cell-level: for same `(column_index)`, cell with highest `timestamp` wins.
- Static row: merged like a regular row (cell-level LWW). When one source has a static row and another does not, the static row from the source that has one is used. When both have static rows, cells are merged by timestamp.
- Rows from multiple sources are merged by clustering key (byte-ordered).

## Concurrency Summary

| Operation | Mechanism | Contention |
|-----------|-----------|------------|
| `TableStore::read()` | `ArcSwap::load()` (wait-free) + memtable `get()` (read-lock one shard) | Near-zero |
| `TableStore::write()` | `ArcSwap::load()` (wait-free) + memtable `put()` (write-lock one shard) | One shard out of 64 |
| `TableStore::flush()` | `Mutex` serializes flushes; two brief `ArcSwap::store()` calls; slow work holds no view locks | Mutex blocks concurrent flushes only; reads/writes unaffected |
| `memtable.size_bytes()` | `AtomicUsize::load(Relaxed)` | Zero (wait-free) |
| `memtable.snapshot()` | `std::thread::scope` parallel drain of 64 shards (read-locks, no write contention) | None (memtable is retired) |
| `FileFlushTarget::flush()` | `std::thread::scope` parallel write of up to 7 files | None (independent files) |

### Lock-Free Upgrade Path

The `Memtable` trait enables swapping `ShardedBTreeMemtable` for a lock-free implementation:

1. **crossbeam-skiplist**: Lock-free concurrent skip list. Currently `0.1.x`. Would eliminate all per-shard locks.
2. **Okasaki-style persistent structures**: HAMT or persistent red-black tree from `im` crate. Immutable structure with structural sharing — reads see a consistent snapshot without any synchronization.
3. **Custom**: Investigate structures from `../research/corpus/cs-foundations/okasaki.pdf` for a Ferrosa-specific lock-free memtable.

The upgrade is a single new file implementing `Memtable` — no changes to `TableStore`, `FlushTarget`, or `merge.rs`.

## Test Strategy

### Unit Tests

| Module | Tests |
|--------|-------|
| `memtable/sharded.rs` | Put/get single row; merge-on-write (newer timestamp wins); multi-shard distribution; snapshot returns token-sorted; concurrent puts from N threads; size_bytes/partition_count accuracy; Arc\<Partition> not deeply cloned |
| `flush.rs` | InMemoryFlushTarget round-trip; FileFlushTarget writes correct component files; parallel file writes produce valid SSTable |
| `merge.rs` | Two partitions merge cells by timestamp; row deletion suppresses older cells; partition deletion suppresses all rows; disjoint partitions concatenate; empty inputs; static row merge; commutative: merge(a,b) == merge(b,a) |
| `store.rs` | Write + read (memtable only); flush + read (SSTable only); write + flush + write + read (both sources merge correctly) |

### Module Integration Tests (`tests/integration.rs`)

| Test | What It Proves |
|------|----------------|
| `write_flush_read_round_trip` | Write N partitions → flush → read each back, assert equality |
| `multiple_flushes_merge` | Write, flush, write same partitions with newer timestamps, flush again, read merges across 2 SSTables + memtable |
| `flush_does_not_block_reads` | Spawn reader thread, flush concurrently, reader always sees consistent data (never partial, never missing) |
| `deletion_suppresses_across_sources` | Write cells, flush, write partition tombstone, read returns None |
| `snapshot_produces_token_order` | Write partitions with random keys, snapshot returns sorted by token |
| `file_flush_target_creates_readable_sstables` | FileFlushTarget writes to tempdir, SSTableReader opens and reads back |
| `concurrent_writes_no_data_loss` | N threads write distinct partitions concurrently, all readable after flush |
| `merge_is_commutative` | Same data flushed in different orders produces identical read results |

### Property Tests (proptest)

| Property | Invariant |
|----------|-----------|
| Memtable round-trip | `get(put(key, row))` contains all cells from row |
| Merge commutativity | `merge([a, b]) == merge([b, a])` for arbitrary partition pairs |
| Merge associativity | `merge([merge([a, b]), c]) == merge([a, merge([b, c])])` |
| Flush preserves all data | Write N arbitrary partitions → flush → read all N back, none lost |
| Timestamp ordering | For any two cells at the same column, the one with higher timestamp survives merge |

## Build Order (Part A)

| Order | Module | Purpose | Key Tests |
|-------|--------|---------|-----------:|
| 1 | `ferrosa-common/schema.rs` | `TableSchema`, `ColumnDefinition` | Construction, `clustering_types()`, `column_index()` |
| 2 | `memtable/mod.rs` + `memtable/sharded.rs` | `Memtable` trait + `ShardedBTreeMemtable` | Put/get, merge, concurrent writes, snapshot |
| 3 | `merge.rs` | `merge_partitions` | LWW, deletions, commutativity |
| 4 | `flush.rs` | `FlushTarget` + `InMemoryFlushTarget` + `FileFlushTarget` + `build_serialization_header()` | Round-trip, parallel file writes, header construction from partition data |
| 5 | `store.rs` | `TableStore` (composes all) | Integration tests: write-flush-read |
| 6 | `tests/integration.rs` | Cross-module integration | All integration tests listed above |

## Parts B and C (Future — Separate Specs)

### Part B: Commit Log

- Segment-based WAL with versioned header (version byte)
- Append-only writes with CRC32 checksums
- Segment lifecycle: active → closed → shipped
- Replay protocol on startup
- Integration with `TableStore` flush tracking

### Part C: Compaction + S3 + Composition

- Compaction strategies: Size-Tiered, Leveled, Time-Window
- SSTable lifecycle with `Arc`-based reference counting
- S3 upload manager with async upload, backpressure, manifest
- `manifest.json` and `checkpoint.json` with format versioning
- Full `StorageEngine` composing TableStore + CommitLog + Compaction + S3

## Related Documents

- [SSTable Design](2026-03-11-ferrosa-sstable-design.md) — SSTable crate design
- [Component Architecture](../../../specs/components.md) — crate dependency graph
- [Data Flow](../../../specs/data-flow.md) — write/read paths and S3 lifecycle
- [System Overview](../../../specs/overview.md) — system overview and design principles
