# Secondary Index Framework Design Specification

> Date: 2026-03-14
> Status: Draft
> Crate: ferrosa-index (new)

## Goal

Build a pluggable secondary index framework for Ferrosa that supports multiple index types behind a common trait, integrates with the storage-attached SSTable lifecycle, and provides operational visibility into index staleness. The framework keeps the hot write path free of index maintenance by decoupling index builds into an asynchronous background pipeline.

## Index Types

| Type | CQL Syntax | Use Case |
|------|-----------|----------|
| B-tree | `CREATE INDEX ... USING 'btree'` | Ordered range queries, sorted scans |
| Hash | `CREATE INDEX ... USING 'hash'` | O(1) equality point lookups |
| Composite | `CREATE INDEX ... USING 'composite'` | Multi-column prefix-based lookups |
| Phonetic | `CREATE INDEX ... USING 'phonetic'` | Fuzzy string matching (Soundex, Metaphone, Double Metaphone, Caverphone) |
| Filtered | `WHERE` clause on any index | Partial/conditional index over a row subset |
| Vector (HNSW) | `CREATE INDEX ... USING 'vector'` | Approximate nearest neighbor — best query performance, incremental build |
| Vector (IVFFlat) | `CREATE INDEX ... USING 'vector'` | Approximate nearest neighbor — faster build, k-means clustering |

### Clustered Index (Deferred)

Clustered indexes override SSTable physical sort order at flush time. Unlike all other index types, they are a constraint on how Data.db is written, not a companion file. This creates fundamental conflicts with the `SecondaryIndex` trait model:

- The `IndexBuilder` trait processes rows *after* they are written; a clustered index must influence sort order *before* writing.
- Other secondary indexes and the merge path assume rows within a partition are in clustering key order.
- Only one clustered index per table is physically possible.

Clustered indexes are deferred to a future spec where they will be modeled as a flush-time sort constraint on `SSTableWriter`, separate from the `SecondaryIndex` trait system.

## Architecture

### Crate Position

`ferrosa-index` is a new workspace crate. It depends on `ferrosa-common` for shared types. `ferrosa-storage` depends on `ferrosa-index` for the trait (index build scheduling during flush/compaction). `ferrosa-schema` depends on `ferrosa-index` for `IndexType` and metadata types. `ferrosa-cql` depends on `ferrosa-index` indirectly through schema. `ferrosa-cluster` depends on `ferrosa-schema` (which re-exports `IndexMetadata`), so `DdlOperation` can reference index types without a direct dependency on `ferrosa-index`.

```
ferrosa-common
    ↓
ferrosa-index          (trait + all implementations)
    ↓              ↘
ferrosa-schema      ferrosa-storage
    ↓         ↘         ↙
ferrosa-cql    ferrosa-cluster
```

### Module Structure

```
ferrosa-index/src/
├── lib.rs              # SecondaryIndex trait, IndexType enum, core types
├── btree.rs            # B-tree index builder + reader
├── hash.rs             # Hash index builder + reader
├── composite.rs        # Multi-column composite index
├── phonetic/           # Phonetic index + algorithm registry
│   ├── mod.rs
│   ├── soundex.rs
│   ├── metaphone.rs
│   ├── double_metaphone.rs
│   └── caverphone.rs
├── filtered.rs         # Filtered/partial index wrapper
└── vector/             # Vector index subsystem
    ├── mod.rs           # VectorDistance trait, dimension handling
    ├── hnsw.rs          # HNSW graph builder + searcher
    └── ivfflat.rs       # IVFFlat cluster builder + searcher
```

---

## Core Type Definitions

These types are used throughout the trait system and must be defined before any index implementation.

```rust
/// Locates a specific row within an SSTable.
/// Uses partition + clustering key bytes rather than byte offsets into Data.db,
/// because the existing SSTableReader provides partition-level access via
/// get_partition() and rows are iterated within partitions. This avoids
/// requiring row-level random access into Data.db.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RowPosition {
    /// Byte-comparable encoded partition key (same encoding used by partition index trie).
    pub partition_key: Vec<u8>,
    /// Byte-comparable encoded clustering key within the partition.
    pub clustering_key: Vec<u8>,
}

/// A typed, serializable value used as an index lookup key.
/// Handles type heterogeneity — the indexed column could be text, int, uuid,
/// blob, vector, or a composite of multiple columns.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IndexKey {
    /// Raw bytes in byte-comparable order (for B-tree, hash).
    Bytes(Vec<u8>),
    /// Text value (for phonetic indexes — needs the original string for encoding).
    Text(String),
    /// Composite key — concatenated byte-comparable values from multiple columns,
    /// ordered by column position in the index definition.
    Composite(Vec<Vec<u8>>),
    /// Vector embedding (for nearest-neighbor queries — not used for lookup/range).
    Vector(Vec<f32>),
}

/// Files produced by an IndexBuilder.
/// Paths are local disk paths, valid during the build and for local reads.
/// After S3 upload and local eviction, readers reconstruct paths from the
/// SSTable prefix + index name convention (e.g., <prefix>-SI_<name>.db).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexFiles {
    /// Path to the .db file containing the index data.
    pub data_path: PathBuf,
    /// Path to the .meta file containing index metadata.
    pub meta_path: PathBuf,
    /// Parsed metadata (always loaded).
    pub meta: IndexFileMeta,
}

/// Configuration passed to IndexFactory::create_builder().
#[derive(Debug, Clone)]
pub struct IndexConfig {
    /// The index type and its parameters.
    pub index_type: IndexType,
    /// Column positions (indices into the row's cell array) that this index covers.
    pub column_positions: Vec<usize>,
    /// Directory where index files should be written.
    pub output_dir: PathBuf,
    /// SSTable prefix for naming companion files.
    pub sstable_prefix: String,
    /// Index name (used in file naming: SI_<index_name>.db).
    pub index_name: String,
}

/// Bitflags indicating which query operations an IndexReader supports.
/// Used by the query planner to select appropriate indexes without downcasting.
bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct IndexCapabilities: u8 {
        /// Supports exact point lookups.
        const POINT_LOOKUP = 0b0001;
        /// Supports ordered range scans.
        const RANGE_SCAN   = 0b0010;
        /// Supports approximate nearest-neighbor queries.
        const NEAREST      = 0b0100;
        /// Supports SOUNDS LIKE phonetic matching.
        const PHONETIC     = 0b1000;
    }
}

/// A predicate for filtered indexes. Evaluated against rows at build time
/// to determine which rows are included in the index.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FilterPredicate {
    /// Column name to filter on.
    pub column: String,
    /// Comparison operator.
    pub op: FilterOp,
    /// Value to compare against (byte-comparable encoded).
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FilterOp {
    Eq,
    NotEq,
    Lt,
    Gt,
    LtEq,
    GtEq,
}
```

---

## Core Trait System

### IndexType Enum

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndexType {
    BTree,
    Hash,
    Composite { columns: Vec<String> },
    Phonetic { algorithm: PhoneticAlgorithm },
    Filtered { predicate: FilterPredicate, inner: Box<IndexType> },
    Vector { method: VectorMethod, metric: DistanceMetric, dimensions: u32 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VectorMethod {
    Hnsw { m: u16, ef_construction: u16 },
    IvfFlat { lists: u32 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DistanceMetric { L2, Cosine, InnerProduct }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PhoneticAlgorithm { Soundex, Metaphone, DoubleMetaphone, Caverphone }
```

### Traits

```rust
/// Build-side: called during background index construction.
/// Single-threaded per SSTable — !Sync by design.
pub trait IndexBuilder: Send {
    /// Feed a row from an SSTable into the builder.
    /// Receives the full row so composite and multi-column indexes can
    /// extract the columns they need. Rows arrive sorted by
    /// partition/clustering key order.
    fn add_row(
        &mut self,
        partition_key: &[u8],
        clustering_key: &[u8],
        cells: &[(u16, CellValue)],
    ) -> Result<()>;

    /// Finalize and write the index file(s) for this SSTable.
    fn finish(self: Box<Self>) -> Result<IndexFiles>;
}

/// Read-side: query an index built for one SSTable.
/// Thread-safe — shared across concurrent query threads.
pub trait IndexReader: Send + Sync {
    /// Point lookup — returns matching row positions in the SSTable data file.
    fn lookup(&self, key: &IndexKey) -> Result<Vec<RowPosition>>;

    /// Range scan (for B-tree, composite). Returns all matching positions eagerly.
    fn range(
        &self,
        start: Bound<&IndexKey>,
        end: Bound<&IndexKey>,
    ) -> Result<Vec<RowPosition>>;

    /// Nearest-neighbor query (for vector indexes).
    fn nearest(
        &self,
        query: &[f32],
        k: usize,
        ef_search: Option<u16>,
    ) -> Result<Vec<(RowPosition, f32)>>;

    /// Which capabilities this reader supports.
    fn capabilities(&self) -> IndexCapabilities;
}

/// Factory: registered per IndexType, creates builders and readers.
pub trait IndexFactory: Send + Sync {
    fn create_builder(&self, config: &IndexConfig) -> Result<Box<dyn IndexBuilder>>;
    fn open_reader(&self, files: &IndexFiles) -> Result<Box<dyn IndexReader>>;
    fn merge(
        &self,
        readers: Vec<Box<dyn IndexReader>>,
        builder: Box<dyn IndexBuilder>,
    ) -> Result<IndexFiles>;
}
```

**Design decisions:**

- `IndexBuilder` is `Send` but not `Sync` — single-threaded construction, one builder per SSTable per index.
- `IndexBuilder::add_row()` receives the full cell array `&[(u16, CellValue)]` so composite indexes can extract multiple columns and vector indexes can access the embedding column by position. The `IndexConfig::column_positions` field tells the builder which cells to extract.
- `IndexReader` is `Send + Sync` — shared across concurrent query threads via `Arc`.
- `IndexReader::range()` returns `Vec<RowPosition>` (eager collection) rather than a borrowing iterator, avoiding lifetime issues with `Box<dyn Iterator>` that would require either cloning or unsafe lifetime erasure.
- `IndexFactory` handles the full lifecycle: construction, opening persisted indexes, and merging during compaction.
- `capabilities()` returns `IndexCapabilities` bitflags so the query planner knows which operations an index supports without downcasting.
- Filtered index wraps another `IndexType` — it delegates to the inner index's factory but only feeds rows matching the predicate to the builder.
- GPU offloading is a future optimization: a `GpuHnswFactory` vs `CpuHnswFactory` can be selected at runtime behind the same `IndexFactory` trait, with no changes to storage layout or query path.

---

## Storage Layout

Each secondary index produces per-SSTable companion files stored alongside the existing SSTable components:

```
<sstable_prefix>-Data.db              # existing
<sstable_prefix>-Partitions.db        # existing (BTI partition trie)
<sstable_prefix>-Rows.db              # existing (BTI row trie)
<sstable_prefix>-Filter.db            # existing (Bloom filter)
<sstable_prefix>-SI_<index_name>.db   # secondary index data
<sstable_prefix>-SI_<index_name>.meta # index metadata (type, config, stats)
```

### Index File Metadata

The `.meta` file is small and always loaded:

```rust
pub struct IndexFileMeta {
    pub index_type: IndexType,
    pub index_name: String,
    pub row_count: u64,
    pub build_timestamp: u64,
    pub sstable_id: String,     // generation-based string ID, matching existing SSTable ID convention
    pub file_size: u64,
    pub checksum: u32,          // CRC32 of the .db file
}
```

### S3 Write-Behind

Index files follow the same S3 write-behind pattern as SSTables — built on local disk first, asynchronously uploaded to S3. The `SI_` prefix groups them visually and makes cleanup straightforward when an SSTable is removed during compaction.

Index files are built asynchronously *after* SSTable flush completes. The `IndexBuildScheduler` handles all index file I/O independently — no modifications to `FileFlushTarget` or `SSTableOutput` are required. The existing flush path writes SSTable + primary index files as before; index companion files appear later when the background build finishes.

For S3 upload, the `UploadManager` gains a new task variant:

```rust
pub enum UploadTask {
    SSTable { files: Vec<(String, Bytes)> },         // existing
    IndexFiles { files: Vec<(String, Bytes)> },      // NEW: upload index companion files
}
```

This keeps index file uploads separate from SSTable uploads since they happen at different times.

### Per-Type Internal Formats

| Type | .db File Contents |
|------|-------------------|
| B-tree | Page-based B+ tree (sorted keys → row positions) |
| Hash | Bucket array + overflow chains (key hash → row positions) |
| Composite | B+ tree with concatenated column keys |
| Phonetic | Hash map: phonetic code → list of row positions |
| Filtered | Same as inner index type, just fewer entries |
| Vector (HNSW) | Serialized graph layers + vector data |
| Vector (IVFFlat) | Centroids + inverted lists of vectors + row positions |

---

## Async Index Build Pipeline

### Write Path Separation

The hot write path never touches index structures:

```
Write Path (hot, synchronous):
  client write → commit log → memtable → return to client

Flush Path (background):
  memtable → SSTable + primary index (partition trie, Bloom filter) [synchronous]
           → enqueue secondary index build jobs [asynchronous]

Compaction Path (background):
  merge N SSTables → 1 new SSTable + primary index [synchronous]
                   → enqueue secondary index build for merged SSTable [asynchronous]
                   (or use IndexFactory::merge() if index type supports incremental merge)
                   → clean up index files from obsolete input SSTables
```

Primary indexes (partition trie, Bloom filter) remain synchronous with flush — they are essential for reads. Secondary indexes are decoupled because multiple indexes on one table could make flush latency unpredictable.

### Build Scheduler

Uses channel-based job submission (matching the existing `CompactionExecutor` pattern with `mpsc::channel`) rather than shared `Mutex<BinaryHeap>` to avoid lock contention:

```rust
pub struct IndexBuildScheduler {
    /// Dedicated worker threads pulling from the job channel.
    /// Follows the CompactionExecutor pattern: N std::threads + mpsc::channel,
    /// not an external thread pool crate.
    workers: Vec<JoinHandle<()>>,
    /// Channel sender for submitting build jobs (lock-free).
    job_sender: mpsc::Sender<IndexBuildJob>,
    /// Per-index, per-table state tracking.
    state_tracker: Arc<IndexStateTracker>,
}

pub struct IndexBuildJob {
    pub sstable_id: String,        // generation-based string ID
    pub index_name: String,
    pub index_type: IndexType,
    pub table: (String, String),   // (keyspace, table)
    pub priority: BuildPriority,
    pub enqueued_at: Instant,
}

pub enum BuildPriority {
    /// Normal priority — background build after flush/compaction.
    Normal,
    /// High priority — initial build after CREATE INDEX on existing data.
    Initial,
}
```

The thread pool workers receive jobs from the channel, which internally uses a priority queue for ordering.

**Build lifecycle:**

1. SSTable flush completes → `IndexBuildScheduler` receives notification
2. For each secondary index on the table → send `IndexBuildJob` via channel
3. Build pool worker receives job:
   - Opens SSTable reader
   - Creates `IndexBuilder` via `IndexFactory`
   - Iterates rows, calls `builder.add_row()` for each
   - `builder.finish()` → writes `SI_*.db` + `SI_*.meta`
   - Notifies `IndexStateTracker`: SSTable now indexed
   - Submits `UploadTask::IndexFiles` to `UploadManager`
4. On failure → retry with exponential backoff, max retries → `Failed` status + alert-level log

### Index Lifecycle & Cleanup

**DROP TABLE**: When a table is dropped via `DdlOperation::DropTable`:

1. All secondary indexes on the table are removed from `IndexStateTracker`.
2. Pending build jobs for the table are cancelled (builder checks for cancellation before starting).
3. Index files on disk are deleted as part of SSTable cleanup.
4. S3 index file deletion follows the same lifecycle as SSTable S3 cleanup.

**DROP INDEX**: When an index is dropped via `DdlOperation::DropIndex`:

1. The index is removed from `IndexStateTracker`.
2. Pending build jobs for that index are cancelled.
3. All `SI_<index_name>.db` and `SI_<index_name>.meta` files across all SSTables are deleted from disk and S3.

**Compaction cleanup**: When compaction merges N input SSTables into 1 output SSTable:

1. The merged SSTable gets new index build jobs enqueued.
2. Index files from the N input SSTables are deleted as part of the standard obsolete-SSTable cleanup path — no special index-specific cleanup needed since `SI_*` files share the SSTable prefix.

### Staleness Tracking

`IndexStateTracker` maintains per-index, per-table state:

```rust
pub struct IndexState {
    pub index_name: String,
    pub table: (String, String),
    pub status: IndexStatus,
    pub indexed_sstables: HashSet<String>,
    pub pending_sstables: VecDeque<String>,
    pub pending_bytes: u64,
    pub oldest_pending_timestamp: Option<u64>,
    pub last_build_duration: Option<Duration>,
    pub total_builds: u64,
    pub total_build_errors: u64,
}

pub enum IndexStatus {
    /// All SSTables indexed.
    Current,
    /// Build in progress.
    Building,
    /// One or more SSTables awaiting index build.
    Stale { lag: Duration, pending_count: u32 },
    /// Build failed, will retry.
    Failed { error: String, retry_at: Instant },
}
```

Each index has independent staleness — a fast hash index might be current while a vector HNSW index is still several SSTables behind.

### Operational Metrics

Exposed via `system_views.secondary_indexes` virtual table:

| Column | Type | Description |
|--------|------|-------------|
| `keyspace_name` | text | Keyspace |
| `table_name` | text | Table |
| `index_name` | text | Index name |
| `index_type` | text | btree, hash, vector_hnsw, etc. |
| `status` | text | current, building, stale, failed |
| `indexed_sstable_count` | int | SSTables fully indexed |
| `pending_sstable_count` | int | SSTables awaiting index build |
| `pending_bytes` | bigint | Data size not yet indexed |
| `lag_seconds` | double | Time since oldest unindexed write |
| `last_build_ms` | bigint | Duration of most recent build |
| `total_builds` | bigint | Lifetime build count |
| `build_errors` | bigint | Lifetime error count |
| `disk_size` | bigint | Total index file size on disk |

This virtual table is owned by `ferrosa-storage` (where `IndexStateTracker` lives) and exposed through the `VirtualTable` trait defined in `ferrosa-schema`.

---

## CQL Syntax

The syntax uses Cassandra-compatible `CREATE INDEX ... USING 'type'` form to maintain compatibility with existing CQL tools (cqlsh, DataStax drivers, ORM frameworks).

### CREATE INDEX

```sql
-- B-tree
CREATE INDEX idx_email ON users (email) USING 'btree';

-- Hash
CREATE INDEX idx_user_id ON users (user_id) USING 'hash';

-- Composite (multi-column)
CREATE INDEX idx_name ON users (last_name, first_name) USING 'composite';

-- Phonetic
CREATE INDEX idx_name ON users (last_name) USING 'phonetic'
    WITH OPTIONS = {'algorithm': 'double_metaphone'};

-- Filtered (wraps another index type)
CREATE INDEX idx_active ON users (email) USING 'btree'
    WHERE status = 'active';
CREATE INDEX idx_premium ON orders (product_id) USING 'hash'
    WHERE tier = 'premium';

-- Vector (HNSW)
CREATE INDEX idx_embed ON documents (embedding) USING 'vector'
    WITH OPTIONS = {'method': 'hnsw', 'metric': 'cosine', 'dimensions': '768',
                    'm': '16', 'ef_construction': '200'};

-- Vector (IVFFlat)
CREATE INDEX idx_embed_ivf ON documents (embedding) USING 'vector'
    WITH OPTIONS = {'method': 'ivfflat', 'metric': 'l2', 'dimensions': '768',
                    'lists': '100'};

-- Common modifiers
CREATE INDEX IF NOT EXISTS idx_email ON users (email) USING 'btree';
DROP INDEX idx_email;
DROP INDEX IF EXISTS idx_email;
```

When `USING` is omitted, the default index type is `btree` (matching Cassandra's behavior of defaulting to a built-in index type).

### Vector Query Syntax

```sql
-- Nearest neighbor search
SELECT * FROM documents ORDER BY embedding ANN OF [0.1, 0.2, ...] LIMIT 10;

-- With ef_search tuning
SELECT * FROM documents ORDER BY embedding ANN OF [0.1, 0.2, ...] LIMIT 10
    WITH OPTIONS = {'ef_search': '100'};
```

### Phonetic Query Syntax

```sql
-- Matches rows where phonetic(last_name) = phonetic('Smith')
SELECT * FROM users WHERE last_name SOUNDS LIKE 'Smith';
```

### Driver Compatibility

CQL drivers are fully compatible:

- Drivers send `CREATE INDEX` as a raw string — no client-side parsing or validation.
- Drivers discover indexes by querying `system_schema.indexes` and store whatever the server returns.
- The `kind` column is an opaque string — drivers don't validate its value.
- The `USING 'type'` syntax follows the existing Cassandra `CREATE CUSTOM INDEX ... USING 'class'` pattern, so tools that generate or parse CQL will handle it naturally.

**`system_schema.indexes` virtual table** (Cassandra-compatible columns):

| Column | Type | Source |
|--------|------|--------|
| `keyspace_name` | text | `index.keyspace` |
| `table_name` | text | `index.table` |
| `index_name` | text | `index.name` |
| `kind` | text | `"btree"`, `"hash"`, `"vector_hnsw"`, etc. |
| `target` | text | `"email"`, `"(last_name, first_name)"` for composite |
| `options` | map<text,text> | Serialized WITH OPTIONS |

This virtual table is owned by `ferrosa-schema` (alongside existing `system/schema_tables.rs`).

---

## Schema Integration

### IndexMetadata

New file: `ferrosa-schema/src/metadata/index.rs`

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexMetadata {
    pub keyspace: String,
    pub table: String,
    pub name: String,
    pub index_type: IndexType,        // from ferrosa-index
    pub target_columns: Vec<String>,
    pub filter_predicate: Option<FilterPredicate>,
    pub options: HashMap<String, String>,
}
```

### SchemaSnapshot Extension

```rust
pub struct SchemaSnapshot {
    pub version: Uuid,
    pub keyspaces: HashMap<String, KeyspaceMetadata>,
    pub tables: HashMap<(String, String), TableMetadata>,
    #[serde(default)]  // backward-compatible with snapshots serialized before indexes existed
    pub indexes: HashMap<(String, String, String), IndexMetadata>,  // (ks, table, index_name)
    pub roles: HashMap<String, RoleMetadata>,
    pub grants: HashMap<String, Vec<GrantEntry>>,
}
```

The `#[serde(default)]` attribute ensures existing serialized snapshots (from pair-mode schema sync) deserialize correctly — the `indexes` field defaults to an empty `HashMap` when absent.

### WireSchemaSnapshot Extension

```rust
pub struct WireSchemaSnapshot {
    pub version: Uuid,
    pub keyspaces: HashMap<String, KeyspaceMetadata>,
    pub tables: Vec<((String, String), TableMetadata)>,
    #[serde(default)]
    pub indexes: Vec<((String, String, String), IndexMetadata)>,  // NEW: flattened for JSON
    pub roles: HashMap<String, RoleMetadata>,
    pub grants: HashMap<String, Vec<GrantEntry>>,
}
```

The `apply_snapshot` method in `Schema` is extended to iterate over `snapshot.indexes` and call `create_index_internal()` for each, following the same pattern as tables. On the receiving node, `apply_snapshot` also triggers index build jobs for any tables that have data.

### Schema Registry Methods

```rust
// Public API (with auth checks)
pub fn create_index(&self, index: IndexMetadata, auth: &AuthContext) -> Result<()>;
pub fn drop_index(&self, ks: &str, table: &str, name: &str, auth: &AuthContext) -> Result<()>;

// Internal API (no auth, idempotent — for replication)
pub fn create_index_internal(&self, index: IndexMetadata) -> Result<()>;
pub fn drop_index_internal(&self, ks: &str, table: &str, name: &str) -> Result<()>;
```

Internal methods follow the established idempotency contract:

- `create_index_internal`: succeeds silently if the index already exists.
- `drop_index_internal`: succeeds silently if the index doesn't exist.

### CQL AST Additions

```rust
pub enum Statement {
    // ... existing variants ...
    CreateIndex(CreateIndexStatement),
    DropIndex(DropIndexStatement),
}

pub struct CreateIndexStatement {
    pub name: Option<String>,        // auto-generated if omitted
    pub keyspace: Option<String>,
    pub table: String,
    pub columns: Vec<String>,
    pub using: Option<String>,       // "btree", "hash", "vector", etc. — default "btree"
    pub filter: Option<FilterPredicate>,
    pub options: HashMap<String, String>,
    pub if_not_exists: bool,
}

pub struct DropIndexStatement {
    pub keyspace: Option<String>,
    pub name: String,
    pub if_exists: bool,
}
```

---

## Multi-Node Compatibility

The design integrates with the in-flight pair-integration work (feature/pair-integration branch).

### DdlOperation Extension

```rust
pub enum DdlOperation {
    CreateKeyspace(KeyspaceMetadata),
    DropKeyspace(String),
    CreateTable(Box<TableMetadata>),
    DropTable { keyspace: String, table: String },
    CreateIndex(IndexMetadata),                                    // NEW
    DropIndex { keyspace: String, table: String, index: String },  // NEW
}
```

### Coordination Flow

- `DdlCoordinator::apply_ddl_locally()` gets two new match arms calling `schema.create_index_internal()` / `schema.drop_index_internal()`.
- Wire format is JSON-serialized `DdlOperation` — serde handles new enum variants automatically. `#[serde(default)]` on `SchemaSnapshot::indexes` ensures backward compatibility during rolling upgrades.
- `PairDdlForwardHandler` and `PairSchemaSyncHandler` operate on serialized bytes and need no code changes.
- `WireSchemaSnapshot` in `ferrosa-cluster/src/pair/ddl.rs` gets an `indexes` field (with `#[serde(default)]`) alongside the existing `tables` Vec. This is a modification to `ferrosa-cluster` code, using `IndexMetadata` re-exported from `ferrosa-schema`.
- `DdlPath::Unavailable` correctly rejects index DDL in degraded mode — no special handling.

### Per-Node Index Builds

After `CREATE INDEX` propagates via DDL coordination, each node independently enqueues index build jobs for its local SSTables. There is no cross-node index build coordination — indexes are storage-attached and each node maintains its own SSTables. Index staleness is tracked per-node independently.

### Initial Index Build

When `CREATE INDEX` is executed on a table with existing data, index build jobs are enqueued for all existing SSTables on each node with `BuildPriority::Initial`. The index status starts as `Building` and transitions to `Current` once all SSTables are indexed.

---

## Query Path Integration

### Row Lookup Strategy

`RowPosition` contains `(partition_key, clustering_key)` bytes rather than byte offsets into Data.db. The query path resolves positions by:

1. Using the partition index trie to locate the partition in Data.db (existing `SSTableReader::get_partition()`).
2. Scanning rows within the partition to find the matching clustering key.

This avoids requiring row-level random access into Data.db, which the current `SSTableReader` does not support. For wide partitions, the row index trie (existing BTI row index) can accelerate intra-partition lookup. The initial implementation accepts full partition materialization via `get_partition()` as a known performance limitation — a targeted `get_row(partition_key, clustering_key)` method on `SSTableReader` should be added as a follow-up optimization for tables with wide partitions and heavy index usage.

### Scan Plan

```rust
pub enum ScanPlan {
    /// Full partition scan (current behavior).
    PartitionScan { partition_key: Vec<CellValue> },

    /// Index-accelerated scan.
    IndexScan {
        index_name: String,
        index_lookup: IndexLookup,
        fallback_sstables: Vec<String>,
    },

    /// Vector nearest-neighbor.
    VectorAnn {
        index_name: String,
        query_vector: Vec<f32>,
        k: usize,
        ef_search: Option<u16>,
        fallback_sstables: Vec<String>,
    },
}

pub enum IndexLookup {
    Point(IndexKey),
    Range { start: Bound<IndexKey>, end: Bound<IndexKey> },
}
```

### Query Execution Flow

1. Parser produces WHERE clause predicates.
2. Planner checks `IndexStateTracker` for available indexes on target columns.
3. If matching index exists:
   - Get coverage from `IndexStateTracker` (indexed vs unindexed SSTables).
   - Build `IndexScan` or `VectorAnn` plan.
4. Execute:
   a. For each indexed SSTable → `IndexReader.lookup/range/nearest`
   b. For each unindexed SSTable → brute-force scan with predicate filter
   c. Memtable scan with predicate filter
   d. Union + deduplicate by partition/clustering key
   e. Apply remaining WHERE predicates not covered by the index

### Best-Effort Coverage

Queries always return results regardless of index staleness. The query executor unions indexed SSTable results with scans of unindexed SSTables and the memtable. Staleness metrics let operators monitor and alert, but queries are never rejected due to stale indexes.

### Filtered Index Interaction

The planner verifies the query's WHERE clause is a superset of the index's filter predicate. A filtered index on `email WHERE status = 'active'` can be used by a query with `WHERE email = 'foo@bar.com' AND status = 'active'`, but not by `WHERE email = 'foo@bar.com'` alone.

### ALLOW FILTERING

Unchanged — queries on non-indexed columns still require `ALLOW FILTERING`. Implementing `ALLOW FILTERING` support is separate work.

---

## Vector Index Details

### Dimension Limits

Constrained by SSTable block size and performance:

- A single vector row must fit in one SSTable data block.
- At 64KB blocks with ~128 bytes overhead: `(65536 - 128) / 4 = 16,352` theoretical max (f32).
- Practical performance degrades significantly above ~2,048 dimensions.
- Most embedding models produce 768–1,536 dimensions.

```rust
pub const VECTOR_MAX_DIMENSIONS_F32: u32 = 4096;    // hard cap, 32-bit floats
pub const VECTOR_MAX_DIMENSIONS_F16: u32 = 8192;    // hard cap, half-precision
pub const VECTOR_PERF_WARNING_THRESHOLD: u32 = 2048; // warning emitted above this
```

### Storage Format

- `f32`: `4 * dimensions + 8` bytes (4-byte header + 4-byte dimension count)
- Future `f16` (half precision): `2 * dimensions + 8` bytes
- Vectors stored inline in SSTable data rows, not externalized.

### Distance Metrics

| Metric | Operator | Description |
|--------|----------|-------------|
| L2 (Euclidean) | `<->` | Standard distance; smaller = more similar |
| Cosine | `<=>` | Angle-based similarity; normalized vectors |
| Inner Product | `<#>` | Dot product; larger = more similar |

Distance functions are the hottest inner loop and candidates for SIMD optimization and future GPU offloading.

### HNSW Specifics

- Multi-layered hierarchical graph; each node is a vector.
- Layer assignment: `l = floor(-ln(uniform(0,1)) * mL)`.
- Search: greedy descent from entry point at top layer, expanding candidate list at base layer.
- Parameters: `m` (connections per node), `ef_construction` (build beam width), `ef_search` (query beam width, tunable at query time).
- No training phase — can build incrementally, fits flush lifecycle.
- Graph stored as: layer 0 (all vectors + adjacency arrays), upper layers (sparse subsets).

### IVFFlat Specifics

- K-means clustering partitions vector space into N clusters.
- Search: find nearest K cluster centroids to query, brute-force within selected clusters.
- Parameters: `lists` (number of clusters), `probes` (clusters to search, tunable at query time).
- Training strategy: per-SSTable centroids computed during build. During compaction merge, `IndexFactory::merge()` recomputes global centroids across the merged dataset, producing a higher-quality index. This means IVFFlat quality improves naturally as compaction consolidates SSTables.
- Stored as: centroids in header, inverted lists of (row_position, vector) pairs.

### GPU Offloading (Future)

The `IndexFactory` trait boundary enables GPU acceleration without architectural changes:

- `IndexBuilder`: GPU-accelerated k-means for IVFFlat training, parallel HNSW graph construction.
- `IndexReader::nearest()`: GPU-accelerated batch distance computations.
- Runtime hardware detection selects `GpuHnswFactory` vs `CpuHnswFactory` behind the same trait.
