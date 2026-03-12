# ferrosa-storage Part B: Commit Log Design

> **Date:** 2026-03-11
> **Status:** Approved
> **Approach:** Bottom-up by component, strict TDD (red-green-refactor)
> **Methodology:** Literate programming (swdev) -- module docs, doc-tests, property tests
> **Parent spec:** [ferrosa-storage Design](2026-03-11-ferrosa-storage-design.md)

## Goal

Implement the commit log (WAL) for ferrosa-storage. The commit log durably records every mutation before it is applied to the memtable. On crash recovery, uncommitted mutations are replayed from the log to restore memtable state.

## Scope

Part B is **local-only**. Async S3 shipping of commit log segments is deferred to Part C.

| In scope | Out of scope |
|----------|-------------|
| Segment-based WAL with Rust-native binary format | S3 segment shipping |
| Lock-free CAS allocation on the write path | io_uring I/O backend |
| Zero extra copies (serialize directly into segment buffer) | mmap segment buffers |
| Three sync strategies (Periodic, Batch, Group) with config | TableStore composition (Part C) |
| Replay from segments after checkpoint | Compaction integration |
| Explicit checkpoint file with format versioning | |
| Shared proptest generators in `ferrosa-common` | |

## Architecture Overview

The commit log is a write-ahead log that durably records every mutation before it is applied to the memtable. On crash recovery, uncommitted mutations are replayed from the log to restore memtable state.

### Data Flow

```
Write Path (with commit log):
  1. CommitLog::append(mutation) -> serialize + CRC + write to active segment
  2. SyncStrategy ensures durability (periodic fsync / batch fsync / group fsync)
  3. TableStore::write(key, row) -> memtable update
  4. Client gets ACK

Flush Path (segment cleanup):
  1. TableStore::flush() -> SSTable written
  2. CommitLog::discard_completed(table_id, position) -> mark segments clean
  3. Segments where all tables are flushed -> close + delete

Startup Path:
  1. Read checkpoint file -> last flushed position per table
  2. Scan segment files on disk, sort by ID
  3. Replay entries after checkpoint positions -> TableStore::write()
  4. Write fresh checkpoint
```

### Module Structure

The commit log lives alongside `TableStore` in `ferrosa-storage` -- no new crate. It adds these modules:

```
ferrosa-storage/src/
  commitlog/
    mod.rs          # CommitLog public API
    segment.rs      # Segment: active buffer, write, sync, close
    descriptor.rs   # Segment header: version, ID, config
    mutation.rs     # Mutation type + binary serialization
    sync.rs         # SyncStrategy trait + Periodic/Batch/Group impls
    checkpoint.rs   # CommitLogCheckpoint: persist/load flushed positions
    reader.rs       # Segment reader for replay
    config.rs       # CommitLogConfig: all tunables
```

### External Dependencies

| Crate | Purpose |
|-------|---------|
| `ferrosa-common` | `DecoratedKey`, `CellValue`, `PartitionKey` |
| `ferrosa-sstable` | `Row`, `DeletionTime`, `LivenessInfo`, `Partition` (types reused from SSTable layer) |
| `crc32fast` | CRC32 checksums for segment headers, sync markers, entries |
| `parking_lot` | `Mutex` for closed-segment list and next-segment queue |
| `arc_swap` | `ArcSwap<Segment>` for lock-free active segment access |
| `serde` + `serde_json` | Checkpoint file serialization (JSON with format versioning) |
| `proptest` (dev) | Property-based testing |
| `tempfile` (dev) | Temporary directories for segment file tests |

### Lock-Free Write Path

The write hot path is lock-free:

1. **Position allocation**: Atomic CAS loop on `AtomicU64` -- each writer reserves its slice of the segment buffer without locking. Same approach as Cassandra's `allocatePosition.compareAndSet()`.
2. **Writing to buffer**: Each writer gets an exclusive `&mut [u8]` slice at its reserved offset -- no lock, no contention.
3. **Sync/fsync**: Runs on a separate thread. Writers don't wait (Periodic), or wait on a condition variable (Batch/Group) -- but that is the sync strategy's choice, not a lock in the write path.
4. **Segment rotation**: When a segment fills up, the writer that gets the `-1` from CAS triggers rotation. A pre-allocated next-segment queue (Cassandra's approach) means the slow path is an atomic pointer swap, not allocation under contention.

### Zero-Copy Serialization

One unavoidable copy exists: serialization of the `Mutation` struct into bytes. But no *additional* copies:

1. CAS-allocate N bytes in the segment buffer
2. Serialize the Mutation **directly into the allocated slice** (no intermediate `Vec<u8>`)
3. Write CRCs in-place

One serialization pass into the final destination, no intermediate buffers, no memcpy. The segment buffer is the only place the bytes ever live until fsync writes them to disk.

## Segment Binary Format

Each segment file is a self-contained, append-only log. The format is Rust-native (inspired by Cassandra but not wire-compatible).

### Layout

```
+---------------------------------------------------+
| Segment Header (fixed)                            |
|  version: u8          (format version, starts 1)  |
|  segment_id: u64      (monotonic, millis-based)   |
|  config_flags: u32    (reserved for future use)   |
|  header_crc: u32      (CRC32 of above fields)     |
|  Total: 17 bytes                                  |
+---------------------------------------------------+
| Sync Section 0                                    |
|  +- Sync Marker ---+                              |
|  | next_marker_offset: u32                        ||
|  | marker_crc: u32  (CRC of segment_id || offset)  ||
|  +------------------------------------------------+|
|  +- Entry 0 ------+                               |
|  | entry_size: u32                                ||
|  | size_crc: u32     (CRC of entry_size)          ||
|  | payload: [u8]     (serialized Mutation)        ||
|  | payload_crc: u32  (CRC of payload)             ||
|  +------------------------------------------------+|
|  +- Entry 1..N ---+                               |
|  | (same structure)                               ||
|  +------------------------------------------------+|
+---------------------------------------------------+
| Sync Section 1..M (same structure)                |
+---------------------------------------------------+
| EOF Marker                                        |
|  next_marker_offset: 0u32                         |
|  marker_crc: 0u32                                 |
+---------------------------------------------------+
```

### Design Points

- **Two-tier CRC per entry**: Size CRC lets the reader detect corruption before allocating a buffer for the payload. Payload CRC validates the mutation data itself. Same approach as Cassandra but without the Java serialization overhead.
- **Sync marker chaining**: Each sync marker points to the next one, enabling fast skip-forward during replay. Markers are written at sync boundaries -- the positions where fsync has been called. `marker_crc` is computed over `segment_id` (from the header) concatenated with `next_marker_offset`, binding each marker to its segment.
- **17-byte header**: `header_crc` is CRC32 over `[version || segment_id || config_flags]` (13 bytes). Compare to Cassandra's ~50-200 bytes (includes JSON params blob). Version byte is sufficient for format evolution. `config_flags` is reserved for future use (compression, encryption) without a format version bump.
- **EOF marker** is all zeros -- naturally occurs if the process crashes mid-write since we pre-allocate with zeros. The EOF marker is only checked at positions indicated by the sync marker chain, so embedded zeros in payload data cannot cause false EOF detection.
- **Entry overhead** per mutation: 12 bytes (4 size + 4 size_crc + 4 payload_crc).

## Mutation Type

```rust
/// A mutation represents one or more row writes to a single table,
/// captured as an atomic unit in the commit log.
pub struct Mutation {
    pub keyspace: String,
    pub table: String,
    pub key: DecoratedKey,
    pub rows: Vec<Row>,
    pub timestamp: i64,         // wall-clock time of mutation
}
```

### Binary Serialization Format

Hand-rolled, versioned. Serialization writes directly into the segment buffer slice (zero extra copies). `Mutation::serialized_size()` computes exact size upfront for CAS allocation. Deserialization constructs `Mutation` from a `&[u8]` slice during replay.

**Why not reuse SSTable row serialization?** SSTable on-disk row format uses delta-encoding against a `SerializationHeader` (min_timestamp, column names, type metadata). The commit log has no such header context -- each entry must be self-describing. A flat, field-by-field layout is simpler and sufficient for WAL replay.

**Mutation layout:**

```
+--------------------------------------+
| keyspace_len: u16                    |
| keyspace: [u8; keyspace_len]         |
| table_len: u16                       |
| table: [u8; table_len]              |
| key_len: u16                         |
| partition_key_bytes: [u8; key_len]   |
| token: i64                           |
| timestamp: i64                       |
| row_count: u16                       |
| rows: [serialized Row x row_count]   |
+--------------------------------------+
```

**Row layout:**

```
+--------------------------------------+
| clustering_len: u16                  |
| clustering: [u8; clustering_len]     |
| deletion_marked_for_delete_at: i64   |
| deletion_local_deletion_time: u32    |
| liveness_timestamp: i64              |
| liveness_ttl: i32                    |
| liveness_local_deletion_time: i32    |
| cell_count: u16                      |
| cells: [serialized Cell x cell_count]|
+--------------------------------------+
```

**Cell layout:**

```
+--------------------------------------+
| column_index: u16                    |
| timestamp: i64                       |
| ttl: i32                             |
| local_deletion_time: i32             |
| value_len: i32 (-1 = null/tombstone) |
| value: [u8; value_len] (if >= 0)     |
+--------------------------------------+
```

Format is compact -- no field names, no padding, no alignment requirements.

**Type note:** `DeletionTime.local_deletion_time` is `u32` (from `ferrosa-sstable`), while `CellValue.local_deletion_time` is `i32` (from `ferrosa-common`). The binary format matches the actual Rust types: `u32` for row-level deletion, `i32` for cell-level deletion.

**Schema assumption:** Part B assumes the schema is static between write and replay. Schema evolution during replay (column additions/removals that change column indices) is deferred to a later phase.

## Sync Strategies

All three sync strategies are provided, selectable via configuration. Periodic is the default.

### Trait

```rust
/// Controls when segment data is fsynced to disk.
pub trait SyncStrategy: Send + Sync {
    /// Called after each mutation is written to the segment buffer.
    /// Returns when the strategy considers the write durable.
    fn on_write(&self, segment: &Segment, position: u64);

    /// Start the strategy's background work (if any).
    fn start(&self);

    /// Shut down cleanly, fsyncing any pending data.
    fn stop(&self);
}
```

### Periodic (default)

- Background thread wakes every `sync_interval` (default 10ms, configurable)
- Fsyncs all accumulated writes since last sync
- `on_write()` returns immediately -- writer never blocks
- Best throughput, durability window = `sync_interval`
- **Tradeoff:** Up to `sync_interval` ms of mutations lost on crash

### Batch

- `on_write()` blocks until fsync completes for this write
- Each write triggers its own fsync
- Safest -- zero data loss window
- **Tradeoff:** Highest latency, lowest throughput (one fsync per mutation)

### Group

- `on_write()` adds to a pending batch, then waits on a condition variable
- Background thread wakes when batch reaches size threshold OR a short timeout (default 1ms)
- Single fsync covers all pending writes, then signals all waiters
- **Tradeoff:** Bounded latency (max 1ms wait), good throughput (amortized fsync)

### Configuration

```rust
pub struct CommitLogConfig {
    /// Segment size in bytes (default 32 MB).
    pub segment_size: usize,
    /// Maximum segment age before rotation (default 5 minutes).
    pub max_segment_age: Duration,
    /// Sync strategy selection.
    pub sync_strategy: SyncStrategyConfig,
    /// Directory for commit log segments.
    pub log_dir: PathBuf,
    /// Directory for checkpoint file.
    pub checkpoint_dir: PathBuf,
}

pub enum SyncStrategyConfig {
    /// Fsync on a timer. Best throughput, small durability window.
    Periodic {
        /// Interval between fsyncs (default 10ms).
        sync_interval: Duration,
    },
    /// Fsync per write. Zero data loss, highest latency.
    Batch,
    /// Fsync batches of writes. Bounded latency, good throughput.
    Group {
        /// Max time to wait for batch (default 1ms).
        max_wait: Duration,
    },
}

impl Default for SyncStrategyConfig {
    fn default() -> Self {
        SyncStrategyConfig::Periodic {
            sync_interval: Duration::from_millis(10),
        }
    }
}
```

### User-Facing Documentation (sync strategy tradeoffs)

| Strategy | Throughput | Latency | Durability Window | When to Use |
|----------|-----------|---------|-------------------|-------------|
| **Periodic** (default) | Highest | Lowest (no blocking) | Up to `sync_interval` (10ms default) | General workloads; acceptable to lose up to 10ms of writes on crash |
| **Batch** | Lowest | Highest (blocks per write) | Zero -- every write fsynced | Financial, audit, or compliance workloads requiring zero data loss |
| **Group** | Good (amortized fsync) | Bounded (max 1ms default) | Up to `max_wait` (1ms default) | Balanced workloads needing low latency and good durability |

## CommitLog Public API

```rust
pub struct CommitLog {
    config: CommitLogConfig,
    /// Active segment accepting writes.
    active: ArcSwap<Segment>,
    /// Closed segments waiting for tables to flush past them.
    closed_segments: Mutex<Vec<Arc<Segment>>>,
    /// Tracks which tables are dirty in which segments.
    segment_tracker: SegmentTracker,
    /// Pre-allocated next segment (ready to swap in on rotation).
    next_segment: Mutex<Option<Segment>>,
    /// Background sync strategy.
    sync_strategy: Box<dyn SyncStrategy>,
    /// Monotonic segment ID generator.
    next_segment_id: AtomicU64,
}

impl CommitLog {
    /// Create a new commit log.
    pub fn new(config: CommitLogConfig) -> Result<Self>;

    /// Open an existing commit log, replaying segments after checkpoint.
    /// Returns the mutations to replay (caller applies to TableStore).
    pub fn open_and_replay(config: CommitLogConfig) -> Result<(Self, Vec<Mutation>)>;

    /// Append a mutation. Lock-free CAS allocation + direct serialization.
    /// Returns a CommitLogPosition for flush tracking.
    pub fn append(&self, mutation: &Mutation) -> Result<CommitLogPosition>;

    /// Notify that a table has been flushed up to this position.
    /// Segments where all tables are flushed get closed and deleted.
    pub fn discard_completed(
        &self,
        table_id: &TableId,
        position: CommitLogPosition,
    ) -> Result<()>;

    /// Force-close the active segment and rotate to a new one.
    pub fn force_rotate(&self) -> Result<()>;

    /// Shut down: flush pending writes, close active segment, write checkpoint.
    pub fn shutdown(&self) -> Result<()>;
}

/// Position in the commit log: segment ID + byte offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct CommitLogPosition {
    pub segment_id: u64,
    pub offset: u64,
}

/// Identifies a table for flush tracking.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableId {
    pub keyspace: String,
    pub table: String,
}
```

### Segment Lifecycle

```
  Pre-allocated         Active              Closed           Deleted
  +-----------+    +--------------+    +--------------+
  | Next seg  |--->| Accepts      |--->| Read-only,   |---> File removed
  | (ready)   |    | writes via   |    | waiting for  |
  |           |    | CAS alloc    |    | all tables   |
  +-----------+    +--------------+    | to flush     |
                   Rotates when:      +--------------+
                   - Buffer full       Deleted when:
                   - Max age reached   - All tables flushed
                                       past this segment
```

Part C will add a "Shipped" state between Closed and Deleted for S3 segment archival.

### Dirty/Clean Tracking (SegmentTracker)

Each segment tracks which tables have unflushed mutations via a `HashMap<TableId, PositionRange>`. When `discard_completed` is called:

1. For each closed segment, check if the flushed position covers the segment's dirty range for that table
1. If all tables in a segment are clean, delete the segment file
1. Update the checkpoint file with the new minimum replay position

### Checkpoint File

The checkpoint file is an explicit file on local disk with format versioning:

```json
{
  "format_version": 1,
  "flushed_positions": {
    "ks1.table1": { "segment_id": 42, "offset": 8192 },
    "ks1.table2": { "segment_id": 41, "offset": 16384 }
  },
  "timestamp": "2026-03-11T12:00:00Z"
}
```

Checkpoint writes are atomic (write to temp file, then rename) to prevent partial-write corruption.

### Integration with TableStore

Part B does NOT modify TableStore -- it provides CommitLog as a standalone component. Part C composes them:

```rust
// In the composed StorageEngine (Part C):
let mutation = Mutation { keyspace, table, key, rows, timestamp };
let position = commit_log.append(&mutation)?;
table_store.write(&key, row)?;
// On flush:
commit_log.discard_completed(&table_id, position)?;
```

## Test Strategy

### Unit Tests (per module)

| Module | Tests |
|--------|-------|
| `descriptor.rs` | Header write/read round-trip; CRC validation catches corruption; version byte forward compat |
| `mutation.rs` | Serialization round-trip; `serialized_size()` matches actual; empty rows; large payloads; null cell values |
| `segment.rs` | CAS allocation returns non-overlapping slices; rotation on full; rotation on max age; sync marker chaining; EOF marker on close |
| `sync.rs` | Periodic fires at interval; Batch blocks until fsync; Group batches and signals waiters |
| `checkpoint.rs` | Write/read round-trip; atomic update (no partial writes); format version check |
| `reader.rs` | Read valid segment; detect corrupted header CRC; detect corrupted entry CRC; skip corrupt entries; stop at EOF marker |

### Property Tests (proptest)

| Property | Invariant |
|----------|-----------|
| Append-replay round-trip | For any sequence of mutations, `append` then replay produces identical mutations in order |
| Serialization round-trip | For any `Mutation` (arbitrary keys, rows, cells, values including empty/null), `serialize` then `deserialize` is identity |
| CAS allocation non-overlapping | N concurrent allocations produce N non-overlapping `(offset, len)` ranges with no gaps |
| Flush tracking correctness | For any interleaving of appends and discards, a segment is deleted iff every table that wrote to it has flushed past it |
| Crash recovery completeness | Write N mutations, simulate crash at random position, replay recovers all mutations before the last successful sync point |
| Crash recovery no duplicates | Replay after checkpoint produces no mutations that were already flushed |
| Segment rotation preserves data | Mutations spanning a segment boundary are all recoverable |
| Sync marker chain integrity | Following the marker chain visits every sync section exactly once and terminates at EOF |
| Commutativity of discard | `discard(A); discard(B)` and `discard(B); discard(A)` produce the same segment cleanup |
| Checkpoint atomicity | Crash during checkpoint write -> old checkpoint still valid on recovery |

### Integration Tests

| Test | What It Proves |
|------|----------------|
| `append_replay_round_trip` | Write N mutations across multiple segments, close, replay, verify all recovered |
| `concurrent_appends_no_data_loss` | N threads appending simultaneously, all mutations recoverable |
| `flush_tracking_cleans_segments` | Append, flush tables, verify old segments deleted |
| `segment_rotation_on_size` | Fill a segment past capacity, verify rotation and both segments readable |
| `segment_rotation_on_age` | Wait past max_age, verify rotation |
| `crash_mid_entry` | Truncate segment mid-entry, replay recovers everything before truncation |
| `crash_mid_sync_marker` | Truncate at sync marker boundary, replay recovers previous sections |
| `periodic_sync_strategy` | Verify writes are durable after sync_interval |
| `batch_sync_strategy` | Verify each write blocks until fsynced |
| `group_sync_strategy` | Verify batched fsync, all waiters unblocked |
| `checkpoint_survives_restart` | Write checkpoint, "restart" (new CommitLog instance), verify replay starts after checkpoint |
| `multiple_tables_independent_flush` | Two tables in same segment, flush one, segment stays; flush both, segment deleted |

### Proptest Generator Strategies

Shared generators live in `ferrosa-common` behind `#[cfg(feature = "test-generators")]` so all crates can reuse them:

```rust
// ferrosa-common/src/test_generators.rs

/// Arbitrary cell value: live, null/tombstone, or with TTL
fn arb_cell_value() -> impl Strategy<Value = CellValue> {
    prop_oneof![
        // Live cell with arbitrary bytes
        (prop::collection::vec(any::<u8>(), 0..1024), 1i64..1_000_000)
            .prop_map(|(v, ts)| CellValue::live(v, ts)),
        // Tombstone (null value)
        (1i64..1_000_000, 1_700_000_000i32..1_700_100_000)
            .prop_map(|(ts, ldt)| CellValue::tombstone(ts, ldt)),
        // Live cell with TTL
        (prop::collection::vec(any::<u8>(), 0..256), 1i64..1_000_000,
         1i32..86400, 1_700_000_000i32..1_700_100_000)
            .prop_map(|(v, ts, ttl, ldt)| CellValue::expiring(v, ts, ttl, ldt)),
    ]
}

/// Arbitrary cell: column index + value
fn arb_cell() -> impl Strategy<Value = (u16, CellValue)> {
    (0u16..64, arb_cell_value())
}

/// Arbitrary row with 0-16 cells
fn arb_row() -> impl Strategy<Value = Row> {
    (
        prop::collection::vec(any::<u8>(), 0..32),       // clustering
        prop::collection::vec(arb_cell(), 0..16),        // cells
        prop_oneof![                                      // deletion
            Just(DeletionTime::LIVE),
            (1i64..1_000_000, 1u32..100_000)
                .prop_map(|(ts, ldt)| DeletionTime::new(ts, ldt)),
        ],
        1i64..1_000_000,                                  // liveness timestamp
    )
        .prop_map(|(clustering, mut cells, deletion, ts)| {
            cells.sort_by_key(|(idx, _)| *idx);
            cells.dedup_by_key(|(idx, _)| *idx);
            Row {
                clustering,
                cells,
                deletion,
                primary_key_liveness: LivenessInfo::with_timestamp(ts),
            }
        })
}

/// Arbitrary partition key
fn arb_key() -> impl Strategy<Value = DecoratedKey> {
    prop::collection::vec(any::<u8>(), 1..128)
        .prop_map(|bytes| DecoratedKey::new(PartitionKey::new(bytes)))
}

/// Arbitrary table identifier
fn arb_table_id() -> impl Strategy<Value = TableId> {
    ("[a-z]{1,8}", "[a-z]{1,8}")
        .prop_map(|(ks, tbl)| TableId { keyspace: ks, table: tbl })
}

/// Arbitrary mutation: 1-8 rows for a single table+key
fn arb_mutation() -> impl Strategy<Value = Mutation> {
    (
        arb_table_id(),
        arb_key(),
        prop::collection::vec(arb_row(), 1..8),
        1i64..1_000_000,
    )
        .prop_map(|(table_id, key, rows, timestamp)| Mutation {
            keyspace: table_id.keyspace,
            table: table_id.table,
            key,
            rows,
            timestamp,
        })
}

/// Sequence of mutations for multi-mutation property tests
fn arb_mutation_sequence() -> impl Strategy<Value = Vec<Mutation>> {
    prop::collection::vec(arb_mutation(), 1..50)
}

/// Arbitrary flush schedule: indices where flushes occur
fn arb_flush_schedule(num_mutations: usize) -> impl Strategy<Value = Vec<usize>> {
    prop::collection::hash_set(0..num_mutations, 0..num_mutations/2)
        .prop_map(|s| { let mut v: Vec<_> = s.into_iter().collect(); v.sort(); v })
}

/// Arbitrary crash point: byte offset within a segment
fn arb_crash_point(segment_size: usize) -> impl Strategy<Value = usize> {
    17..segment_size  // after header, before end
}
```

**Coverage per property test:**

| Property | Generator | Edge cases covered |
|----------|-----------|-------------------|
| Serialization round-trip | `arb_mutation()` | Empty values, tombstones, TTLs, large payloads, 0-length clustering keys |
| Append-replay | `arb_mutation_sequence()` | Multiple mutations, varying sizes, segment boundary crossings |
| CAS non-overlapping | Explicit: N threads x random sizes | Sizes from 12 bytes to 16 KB |
| Flush tracking | `arb_mutation_sequence()` + `arb_flush_schedule()` | Partial flushes, out-of-order flushes, single-table segments, multi-table segments |
| Crash recovery | `arb_mutation_sequence()` + `arb_crash_point()` | Mid-entry, mid-sync-marker, mid-header, at exact boundaries |

## Backlog (Deferred Optimizations)

### io_uring I/O Backend

Use standard `std::fs::File` with `write` + `sync_all` for Part B. Define an `IoBackend` trait so segment I/O is swappable. Add io_uring as an alternative backend later. Biggest win on Group and Batch sync strategies where fsync latency directly impacts write latency. Linux-only, requires fallback for other platforms.

### mmap Segment Buffers

mmap for the segment buffer to potentially eliminate the fsync copy. Priority order (per user): lock-free write path first, zero unnecessary copies second.

## Build Order

| Order | Module | Purpose |
|-------|--------|---------|
| 1 | `ferrosa-common/test_generators.rs` | Shared proptest generators. Add `[features] test-generators = ["proptest"]` to `ferrosa-common/Cargo.toml` and move `proptest` to `[dependencies]` with `optional = true`. |
| 2 | `commitlog/config.rs` | `CommitLogConfig`, `SyncStrategyConfig`. Add `crc32fast`, `serde`, `serde_json` to `ferrosa-storage/Cargo.toml`. |
| 3 | `commitlog/descriptor.rs` | Segment header: write, read, CRC validation |
| 4 | `commitlog/mutation.rs` | `Mutation` type, `serialize`, `deserialize`, `serialized_size` |
| 5 | `commitlog/segment.rs` | `Segment`: buffer, CAS allocation, entry writing, sync markers |
| 6 | `commitlog/sync.rs` | `SyncStrategy` trait + Periodic/Batch/Group |
| 7 | `commitlog/reader.rs` | Segment reader: parse entries, follow sync markers, skip corruption |
| 8 | `commitlog/checkpoint.rs` | Checkpoint file: write, read, atomic update |
| 9 | `commitlog/mod.rs` | `CommitLog`: append, replay, discard, rotate, shutdown |
| 10 | `tests/commitlog_integration.rs` | Cross-module integration tests |
| 11 | `tests/commitlog_property.rs` | Property-based tests with shared generators |

## Related Documents

- [ferrosa-storage Design](2026-03-11-ferrosa-storage-design.md) -- parent spec (Parts A/B/C overview)
- [Storage Spec](../../../specs/storage.md) -- architecture spec (specs/ canonical location)
- [SSTable Design](2026-03-11-ferrosa-sstable-design.md) -- SSTable crate design
- [Data Flow](../../../specs/data-flow.md) -- write/read paths and S3 lifecycle
