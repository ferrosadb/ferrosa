# Data Flow

> Last updated: 2026-04-05
> Status: Approved

## Overview

Ferrosa uses a write-behind async S3 storage model. Writes go to local ephemeral storage first, then are asynchronously uploaded to S3. Reads check memtable, local cache, then fall back to S3 on cache miss. Transactional writes (LWT, BEGIN TRANSACTION) route through the Accord consensus protocol for serializable isolation before reaching the storage engine.

Five protocol endpoints share the same storage engine:

- **CQL** (port 9042) — CQL native protocol v4/v5 with CRC integrity (v5)
- **Bolt** (port 7687) — Neo4j driver compatibility for Cypher
- **Graph HTTP** (port 7474) — Cypher queries via HTTP/JSON
- **SPARQL** (port 8080) — SPARQL 1.1 Query/Update with RDF* and content negotiation
- **Web console** (port 9090) — Observability dashboard and metrics

## Write Path

```mermaid
sequenceDiagram
    participant C as CQL Client
    participant Coord as Coordinator Node
    participant R1 as Replica 1
    participant R2 as Replica 2
    participant S3 as S3

    C->>Coord: INSERT (CL=QUORUM)
    Coord->>R1: Forward mutation
    Coord->>R2: Forward mutation

    par On each replica
        R1->>R1: Write to commit log (local NVMe)
        R1->>R1: Write to memtable (RAM)
        R1-->>S3: Async: ship commit log segment
    and
        R2->>R2: Write to commit log (local NVMe)
        R2->>R2: Write to memtable (RAM)
        R2-->>S3: Async: ship commit log segment
    end

    R1->>Coord: ACK
    R2->>Coord: ACK
    Coord->>C: SUCCESS (QUORUM met)

    Note over R1,S3: Later: memtable flush
    R1->>R1: Flush memtable → SSTable (local)
    R1->>R1: Build FTI sidecars (per registered FullText index)
    alt pin_config.is_pinned()
        R1->>R1: Pin SSTable in LocalCache (skip S3)
    else not pinned
        R1-->>S3: Async: upload SSTable (priority queue)
    end
```

### NVMe Pinning

When `pin_config.is_pinned()` returns true for a table, flushed SSTables remain on local NVMe and are **not** uploaded to S3. The SSTable ID is added to the `LocalCache` pinned set. This is used for tables whose working set should never leave local storage (e.g., high-churn ephemeral data, node-local system tables).

### FTI Sidecar Building

After an SSTable is written during flush, for each registered FullText index on the table, a `FullTextIndexBuilder` processes the flushed partitions and writes a sidecar file named `{generation}-FTI-{index_name}.db` alongside the SSTable. These sidecars follow the same locality rules as the parent SSTable (pinned or uploaded to S3).

## Read Path

```mermaid
sequenceDiagram
    participant C as CQL Client
    participant Coord as Coordinator
    participant R as Replica
    participant Cache as Local Cache (NVMe)
    participant S3 as S3

    C->>Coord: SELECT (CL=QUORUM)
    Coord->>R: Forward read

    R->>R: Check memtable
    alt Found in memtable
        R->>Coord: Return data
    else Not in memtable
        R->>Cache: Check local SSTable cache
        alt Cache hit
            R->>R: Bloom filter → partition index → data
            R->>Coord: Return data
        else Cache miss
            R->>S3: Fetch SSTable component
            S3->>R: SSTable data
            R->>Cache: Cache locally
            R->>Coord: Return data
        end
    end

    Coord->>C: Result
```

## Index-Accelerated Read Path

When a secondary index exists for the queried column, the planner bypasses full table scan:

```mermaid
sequenceDiagram
    participant C as CQL Client
    participant P as Planner
    participant MI as MemtableIndex
    participant SC as Sidecar Files
    participant Store as TableStore

    C->>P: SELECT * FROM t WHERE val = 'x'
    P->>P: plan() → SingleIndex(idx_val)

    par Merge index sources
        P->>MI: lookup("x") → Vec<RowPosition>
        P->>SC: lookup("x") per SSTable → Vec<RowPosition>
    end

    P->>Store: Fetch rows by RowPosition
    Store->>C: Result rows
```

**Sidecar files** are per-SSTable companion files written during flush. They contain sorted `(indexed_value, RowPosition)` entries with a CRC32-checksummed header. Missing sidecars trigger a fallback to full scan for that SSTable, with startup rebuild.

**IndexIntersection**: When multiple indexed columns appear in WHERE, the planner collects RowPositions from each index and intersects them before row fetch, reducing I/O.

## Accord Transaction Flow

Accord consensus provides serializable transactions without a dedicated coordinator. The protocol uses a multi-phase approach where the transaction coordinator can be any node.

### LWT (Lightweight Transaction) Flow

```mermaid
sequenceDiagram
    participant C as CQL Client
    participant Coord as Coordinator
    participant R1 as Replica 1
    participant R2 as Replica 2
    participant R3 as Replica 3

    C->>Coord: INSERT ... IF NOT EXISTS (CL=SERIAL)
    Coord->>Coord: Generate TxnId (HLC timestamp + node)

    Note over Coord,R3: Phase 1: PreAccept
    Coord->>R1: PreAccept(txn, keys, deps)
    Coord->>R2: PreAccept(txn, keys, deps)
    Coord->>R3: PreAccept(txn, keys, deps)
    R1->>R1: ConflictIndex.check(keys)
    R2->>R2: ConflictIndex.check(keys)
    R1->>Coord: PreAcceptOk(deps)
    R2->>Coord: PreAcceptOk(deps)

    alt Fast Path (3/4 quorum agrees on deps)
        Note over Coord,R3: Phase 2: Commit (skip Accept)
        Coord->>R1: Commit(txn, deps)
        Coord->>R2: Commit(txn, deps)
        Coord->>R3: Commit(txn, deps)
    else Slow Path (deps disagree)
        Note over Coord,R3: Phase 2: Accept
        Coord->>R1: Accept(txn, merged_deps)
        Coord->>R2: Accept(txn, merged_deps)
        R1->>Coord: AcceptOk
        R2->>Coord: AcceptOk
        Note over Coord,R3: Phase 3: Commit
        Coord->>R1: Commit(txn, deps)
        Coord->>R2: Commit(txn, deps)
        Coord->>R3: Commit(txn, deps)
    end

    Note over Coord,R3: Phase 4: Execute
    Coord->>Coord: Wait for deps via DepWaitGraph
    Coord->>Coord: Execute IF condition check
    Coord->>Coord: Apply via SyncWriter

    Coord->>C: RESULT ([applied]=true/false)
```

### Multi-Statement Transaction Flow

```mermaid
sequenceDiagram
    participant C as CQL Client
    participant Coord as Coordinator
    participant Accord as AccordCoordinator
    participant SM as AccordStateMachine

    C->>Coord: BEGIN TRANSACTION
    Coord->>Coord: Create transaction context

    C->>Coord: SELECT ... (read-set)
    Coord->>Coord: Buffer read to read-set

    C->>Coord: UPDATE ... (write-set)
    Coord->>Coord: Buffer write to write-set

    C->>Coord: COMMIT
    Coord->>Accord: Submit(read-set, write-set)
    Accord->>SM: PreAccept → Accept → Commit → Execute
    SM->>SM: DepWaitGraph: wait for dependencies
    SM->>SM: Execute read-set validation
    SM->>SM: Apply write-set via SyncWriter

    Accord->>Coord: CommitResult
    Coord->>C: RESULT (committed)
```

### Cross-Shard Transaction Flow

```mermaid
sequenceDiagram
    participant Coord as Coordinator
    participant S1 as Shard 1 Electorate
    participant S2 as Shard 2 Electorate

    Coord->>Coord: Partition keys by token range → shards
    par PreAccept to both shards
        Coord->>S1: PreAccept(txn, shard1_keys)
        Coord->>S2: PreAccept(txn, shard2_keys)
    end
    S1->>Coord: PreAcceptOk(deps_1)
    S2->>Coord: PreAcceptOk(deps_2)

    Coord->>Coord: Merge deps from all shards

    par Commit to both shards
        Coord->>S1: Commit(txn, merged_deps)
        Coord->>S2: Commit(txn, merged_deps)
    end

    par Execute on each shard
        S1->>S1: Wait deps → apply shard1 writes
        S2->>S2: Wait deps → apply shard2 writes
    end
```

### Accord Recovery Flow

When a coordinator fails mid-transaction, any node can recover the transaction:

```mermaid
sequenceDiagram
    participant Rec as RecoveryCoordinator
    participant R1 as Replica 1
    participant R2 as Replica 2
    participant R3 as Replica 3

    Note over Rec: Detect stale transaction via timeout
    Rec->>R1: Recovery(txn_id, ballot)
    Rec->>R2: Recovery(txn_id, ballot)
    Rec->>R3: Recovery(txn_id, ballot)

    R1->>Rec: RecoveryOk(state: PreAccepted, deps)
    R2->>Rec: RecoveryOk(state: Committed, deps)

    alt Already committed
        Rec->>Rec: Use committed state
        Rec->>R1: Commit(txn, deps)
        Rec->>R3: Commit(txn, deps)
    else Not yet committed
        Rec->>R1: Accept(txn, merged_deps)
        Rec->>R2: Accept(txn, merged_deps)
        Note over Rec: Then proceed to commit
    end
```

### DDL Drain Flow

DDL operations (CREATE TABLE, ALTER TABLE, etc.) must drain active Accord transactions before proceeding:

```mermaid
sequenceDiagram
    participant DDL as DDL Request
    participant Drain as DdlDrain
    participant Accord as AccordCoordinator
    participant Gate as WriteGate

    DDL->>Drain: request_drain()
    Drain->>Gate: close() — block new transactions
    Drain->>Accord: wait_for_in_flight()
    Note over Accord: All active transactions complete or timeout
    Accord->>Drain: drained
    Drain->>DDL: proceed with DDL
    DDL->>DDL: Apply schema change
    DDL->>Gate: open() — resume transactions
```

## SSTable Lifecycle in S3

```mermaid
stateDiagram-v2
    [*] --> Memtable: Client write
    Memtable --> LocalSSTable: Flush threshold
    LocalSSTable --> PinCheck: pin_mode?

    state PinCheck <<choice>>
    PinCheck --> PinnedLocal: NVMe pinned
    PinCheck --> S3SSTable: Not pinned

    PinnedLocal --> Active: Pin in LocalCache (skip S3)
    S3SSTable --> Active: Async upload + manifest updated

    state "Compaction" as Compact {
        Active --> Reading: Strategy selects inputs (STCS or UCS per table)
        Reading --> Merging: Read input SSTables + merge FTI sidecars
        Merging --> NewSSTable: Write merged output
        NewSSTable --> CompactPinCheck: pin_mode?
        state CompactPinCheck <<choice>>
        CompactPinCheck --> PinnedOutput: NVMe pinned
        CompactPinCheck --> Uploaded: Not pinned → upload to S3
        PinnedOutput --> CompactDone: Pin in LocalCache (skip S3)
        Uploaded --> CompactDone: Upload complete
    }

    CompactDone --> Active: New manifest written
    Active --> GracePeriod: Superseded by compaction
    GracePeriod --> Deleted: Grace period expires (1hr)
```

### Compaction: NVMe Pinning and FTI Sidecar Merging

- **Pinned tables**: When compaction produces output for a pinned table, the compacted SSTable stays on local NVMe and S3 upload is skipped, matching flush-path behavior.
- **FTI sidecar merging**: FTI sidecars from the input SSTables are merged during compaction. The compaction output includes a new `{generation}-FTI-{index_name}.db` sidecar built from the merged index entries.

### Manifest

Each table has a `manifest.json` in S3 that lists the current set of active SSTable entries per table. The manifest is the source of truth for what SSTables are live. It is a complete document (not a diff) — each update writes a new version.

**Implementation status**: The `Manifest` struct (`ferrosa-storage/src/manifest.rs`) uses **etag-based CAS** (conditional put) via `object_store`'s `PutMode::Update(UpdateVersion { e_tag, version })` to detect stale writes. On `load()`, the current etag is captured; on `save()`, the etag is passed to the conditional put. If another node updated the manifest in the meantime, the put fails and the caller must reload and retry. The CAS retry loop is follow-on work — currently `save()` returns an error on conflict.

The manifest contains a `format_version`, a map of table name to `Vec<ManifestEntry>` (SSTable ID, path, size, token range, timestamp range, partition count), and a `last_compacted_at` timestamp.

### Safe Deletion Protocol (Follow-on)

Not yet implemented. The design:

1. Compaction completes: new SSTables uploaded, confirmed durable in S3
1. Updated `manifest.json` written (etag-based CAS — new entries in, old out)
1. Old SSTables marked for deletion with grace period (default 1 hour)
1. Background GC deletes S3 objects whose grace period has expired
1. Grace period ensures nodes reading old SSTables from S3 (cache miss during transition) have time to complete

### Orphan Cleanup (Follow-on)

Not yet implemented. The design:

A periodic sweep compares SSTable objects in S3 against all known manifests. Objects not referenced by any manifest and older than the grace period are deleted. This catches orphans from partial upload failures or interrupted compactions.

## Commit Log Lifecycle

```mermaid
stateDiagram-v2
    [*] --> Active: New segment opened
    Active --> Active: Writes appended
    Active --> Closed: Segment full (32MB)
    Active --> Shipped: Timer fires — partial upload
    Closed --> Shipped: Upload to S3
    Shipped --> Retained: SSTable flush not yet durable
    Retained --> Deletable: Corresponding SSTables confirmed in S3
    Deletable --> [*]: Cleanup
```

### Commit Log Entry Format

**Segment-level**: Each entry in a segment is framed as:

| Field | Size | Description |
|-------|------|-------------|
| Length prefix | 4 bytes | Record length (big-endian u32) |
| Mutation payload | Variable | Self-describing binary (see below) |
| CRC32 checksum | 4 bytes | CRC32 of the mutation payload |

**Mutation binary layout** (implemented in `ferrosa-storage/src/commitlog/mutation.rs`):

```text
Mutation: keyspace_len:u16 | keyspace | table_len:u16 | table
        | key_len:u16 | key_bytes | token:i64 | timestamp:i64
        | row_count:u16 | rows...

Row: clustering_len:u16 | clustering
   | deletion_marked_for_delete_at:i64 | deletion_local_deletion_time:u32
   | liveness_timestamp:i64 | liveness_ttl:i32 | liveness_local_deletion_time:i32
   | cell_count:u16 | cells...

Cell: column_index:u16 | timestamp:i64 | ttl:i32 | local_deletion_time:i32
    | value_len:i32 (-1=tombstone) | value
```

All multi-byte integers are big-endian. Each entry is fully self-describing — it carries keyspace, table, key, token, and complete row/cell data. This is unlike SSTable row format (which uses delta-encoding against a `SerializationHeader`).

Segments are configurable-size files (default 32 MB, `DEFAULT_SEGMENT_SIZE`). A segment is closed and a new one opened when it reaches capacity or max age (default 5 minutes, `DEFAULT_MAX_SEGMENT_AGE`). Segment space is allocated via compare-and-swap (CAS) on an `AtomicUsize` offset, enabling concurrent appends without holding a lock during serialization.

### Commit Log Archiving (PITR)

Commit log archiving is implemented as part of the PITR system. When `FERROSA_ARCHIVE_ENABLED=true`:

1. The `CommitLogArchiver` (tokio task) monitors closed segments
2. Closed segments are uploaded to `{prefix}/commitlog-archive/` in S3 with hex-prefix paths for throughput
3. Each segment is checksummed (SHA-256) and recorded in `archive-manifest.json` with CAS update
4. The `archived` flag on the segment tracker prevents premature local deletion
5. Retention policy respects snapshot boundaries: segments newer than the oldest snapshot's commit log position are never deleted

The active (not yet full) segment is not archived — only closed segments. The archiver uses exponential backoff retry (5 attempts) on S3 upload failure.

**Implementation**: The `CommitLogArchiver` (`ferrosa-storage/src/archiver.rs`) runs as a tokio background task. The commit log checkpoint system (`ferrosa-storage/src/commitlog/checkpoint.rs`) tracks per-table flush positions as `CommitLogPosition { segment_id, offset }`. Checkpoints are serialized as JSON and persisted to local disk. The archive manifest (`archive-manifest.json`) in S3 tracks archived segments with checksums and uses etag-based CAS for consistency.

### Replay Protocol

Commit log replay is implemented (merged PR #38). At startup, the storage engine:

1. Reads the checkpoint from local disk to find the replay starting point
1. Determines which commit log segments contain data newer than the latest checkpoint
1. Replays those segments in order, applying mutations to the memtable
1. Once replay is complete, the node is current and can begin serving

**S3 replay (follow-on)**: Downloading and replaying segments from S3 (for cold-start recovery on a new node) is not yet implemented. The current replay path is local-disk only.

### Commit Log Cleanup

Segments are deleted once the data they contain has been flushed to SSTables. The checkpoint tracks per-table flush boundaries. Currently, local segment cleanup is based on the checkpoint; S3 segment cleanup is follow-on work.

## PITR: Snapshot & Restore

```mermaid
sequenceDiagram
    participant Op as Operator
    participant Engine as StorageEngine
    participant SM as SnapshotManager
    participant S3 as S3

    Note over Op,S3: Snapshot Creation
    Op->>Engine: create_snapshot("daily")
    Engine->>Engine: Flush all memtables
    Engine->>SM: create(name, commit_log_position)
    SM->>S3: Copy manifest.json → snapshots/daily/
    SM->>S3: Copy schema.json → snapshots/daily/
    SM->>S3: Write metadata.json (name, node_id, position, SHA-256)
    SM->>Engine: SnapshotMetadata

    Note over Op,S3: Point-in-Time Restore
    Op->>Engine: open_from_snapshot("daily", restore_point)
    Engine->>S3: Load snapshot metadata.json
    Engine->>Engine: Validate node_id matches (or --force)
    Engine->>S3: Download SSTables from snapshot manifest
    Engine->>S3: Download archived segments [snapshot_pos..latest]
    Engine->>Engine: Validate segment continuity (no gaps)
    Engine->>Engine: Replay segments with timestamp <= restore_point
    Engine->>Engine: Open normally
```

### Snapshot Lifecycle

Snapshots are metadata-only references to existing SSTables — they do not duplicate data:

- **Create**: Flush memtables, copy manifest + schema to snapshot prefix, write `metadata.json` with SHA-256 of manifest and commit log position
- **SSTable GC safety**: Before deleting any SSTable, GC scans all snapshot manifests. An SSTable is deleted only when zero references exist across the live manifest and all snapshots
- **TTL cleanup**: Background task expires snapshots past their `expires_at` (configurable, default 1 hour interval)
- **Archive retention**: Retention cleanup never deletes segments newer than the oldest snapshot's commit log position

### S3 Layout (PITR)

```text
s3://ferrosa-data/{cluster_id}/
  commitlog-archive/
    {hex_prefix}/{node_id}/{segment_id}.log
  snapshots/
    {snapshot_name}/
      manifest.json
      schema.json
      metadata.json
```

## S3 Upload Backpressure

```mermaid
flowchart TD
    A[SSTable flushed to local disk] --> B{Upload queue depth}
    B -->|Normal| C[Queue for async upload]
    B -->|Queue growing| D{What type?}
    D -->|Fresh SSTable| C
    D -->|Compaction output| E[Deprioritize or drop]
    C --> F{Local disk usage}
    F -->|Below 80%| G[Continue accepting writes]
    F -->|Above 80%| H[Backpressure: WriteTimeout to clients]
    E --> F
```

### Backpressure Details

**Implementation status**: The `UploadManager` (`ferrosa-storage/src/upload/manager.rs`) uses a bounded `tokio::sync::mpsc` channel for backpressure — when the channel is full, senders block. Exponential backoff retry on upload failure is implemented. Priority shedding and disk space threshold monitoring are follow-on work.

1. **Queue depth monitoring**: Track pending uploads by count and total bytes. *(follow-on)*
1. **Priority shedding**: Drop compaction output uploads first — they can be re-compacted. Freshly-flushed SSTables always get priority. *(follow-on)*
1. **Disk space threshold**: When local ephemeral disk usage exceeds a configurable threshold (default 80%), begin rejecting writes with backpressure (return `WriteTimeout` to clients). *(follow-on)*
1. **S3 outage behavior**: Writes continue locally as long as disk space permits. Uploads queue and retry with exponential backoff. When S3 returns, the queue drains in priority order. *(backoff retry implemented; priority drain follow-on)*

## Node Recovery

```mermaid
sequenceDiagram
    participant New as New Node
    participant Raft as Raft Leader
    participant S3 as S3

    New->>Raft: Join cluster request
    Raft->>New: Token assignment + Raft snapshot
    New->>S3: Download SSTable manifest
    New->>S3: Fetch Bloom filters + partition indices
    Note over New: Can serve reads now (S3 fallback)
    New->>S3: Download commit log checkpoint
    New->>S3: Replay recent commit log segments

    loop Background cache warming
        New->>S3: Fetch hot SSTables based on access
    end
```

## Data Loss Mitigations

| Layer | Mechanism | Window Covered |
|-------|-----------|---------------|
| 1. Quorum writes | Data on >= 2 nodes before ACK (RF=3, CL=QUORUM) | Node death before S3 upload |
| 2. Commit log shipping | Async S3 upload on configurable interval (`commitlog_ship_interval`) | Node death before SSTable flush |
| 3. SSTable upload priority | Fresh flushes before compaction output | Node death between flush and upload |
| 4. Replica coordination | Track S3 upload confirmation per replica | Multi-node failure before any upload |
| 5. Increased quorum (optional) | CL=ALL or higher RF | Catastrophic multi-node failure |

## S3 Object Layout (Design Target)

The intended S3 layout once upload wiring is complete:

```
s3://ferrosa-data/{cluster_id}/
  {keyspace}/{table}/
    sstables/{generation}-{component}.db
    manifest.json
  commitlog/
    {node_id}/{segment_id}.log
    {node_id}/checkpoint.json
  metadata/
    schema.json
    topology.json
```

**Implementation status**: The `ObjectStoreConfig` (`ferrosa-storage/src/upload/config.rs`) configures the S3 bucket, region, endpoint, and prefix via `FERROSA_S3_*` environment variables. The `UploadManager` can upload files to S3 via the `object_store` crate. The actual path structure and wiring from flush/compaction to upload is follow-on work.

## S3 Object Metadata (per SSTable component)

| Key | Value | Purpose |
|-----|-------|---------|
| `x-amz-meta-ferrosa-table` | `keyspace.table_name` | Quick identification |
| `x-amz-meta-ferrosa-generation` | `42` | SSTable generation |
| `x-amz-meta-ferrosa-format` | `bti-1.0` | Format version |
| `x-amz-meta-ferrosa-min-token` | `-9223372036854775808` | Partition range start (cache warming) |
| `x-amz-meta-ferrosa-max-token` | `3074457345618258602` | Partition range end |
| `x-amz-meta-ferrosa-level` | `0` | Compaction level |
| `x-amz-meta-ferrosa-checksum` | `sha256:abc123...` | Integrity verification |
| `x-amz-meta-ferrosa-uploaded-by` | `node-3` | Source node |
| `x-amz-meta-ferrosa-created-at` | `2026-03-11T...` | Lifecycle policies |

## Observability

### Virtual Table Read Path

Virtual tables are code-backed (not SSTable-backed) and served entirely in-process. The router intercepts SELECTs targeting virtual table keyspaces before the storage engine is consulted.

```mermaid
sequenceDiagram
    participant C as CQL Client
    participant Router as CQL Router
    participant VTR as VirtualTableRegistry
    participant VT as VirtualTable impl

    C->>Router: SELECT * FROM system_observability.connections
    Router->>VTR: get("system_observability", "connections")
    alt Found in registry
        VTR->>Router: Arc<dyn VirtualTable>
        Router->>VT: table.read(predicate)
        VT->>Router: Vec<VirtualRow>
        Router->>C: CQL RESULT frame (rows)
    else Not found
        Router->>Router: Fall through to user table lookup
    end
```

**Implementation**: The `VirtualTableRegistry` (`ferrosa-schema/src/virtual_registry.rs`) stores `Arc<dyn VirtualTable>` in an `ArcSwap<HashMap>` for lock-free reads. The router (`ferrosa-cql/src/router.rs`) checks `state.schema.virtual_tables().get(ks, &s.table)` before attempting storage engine lookups.

### Connection Tracking Flow

Every CQL TCP connection is registered with a `ConnectionTracker` on accept and deregistered on disconnect. State transitions are tracked as the connection progresses through the protocol handshake.

```mermaid
sequenceDiagram
    participant TCP as TCP Accept
    participant Server as CQL Server
    participant CT as ConnectionTracker
    participant VT as ConnectionsTable

    TCP->>Server: New connection (SocketAddr)
    Server->>CT: register(addr, ConnectionInfo{state: "startup"})

    Server->>Server: Receive STARTUP frame
    Server->>CT: update_state(addr, "authenticating")

    Server->>Server: Auth handshake completes
    Server->>CT: update_state(addr, "ready")
    Server->>CT: update_username(addr, "alice")

    loop Each request
        Server->>CT: increment_requests(addr)
    end

    Note over VT: SELECT * FROM system_observability.connections
    VT->>CT: read(None) — snapshot all ConnectionInfo

    Server->>Server: Connection closes
    Server->>CT: deregister(addr)
```

**Implementation**: `ConnectionTracker` (`ferrosa-cql/src/virtual_tables/connections.rs`) uses `RwLock<HashMap<SocketAddr, ConnectionInfo>>`. Each `ConnectionInfo` carries `peer_address`, `peer_port`, `state` (`"startup"` / `"authenticating"` / `"ready"`), `username`, `connected_at`, `requests_served`, and `protocol_version`. The `ConnectionsTable` exposes this as `system_observability.connections` with primary key `(peer_address, peer_port)`.

### Query Tracking Flow

Every query is tracked from arrival to completion via `QueryTracker`. The RAII `QueryGuard` ensures automatic deregistration even if query execution panics.

```mermaid
sequenceDiagram
    participant C as CQL Client
    participant Server as CQL Server
    participant QT as QueryTracker
    participant VT as ActiveQueriesTable

    C->>Server: QUERY frame
    Server->>QT: begin_guarded(query, keyspace, client, username)
    QT->>Server: QueryGuard (holds query_id)

    Server->>Server: Execute query

    Note over VT: SELECT * FROM system_observability.active_queries
    VT->>QT: snapshot() — all in-flight QueryInfo

    Server->>Server: Query completes (or panics)
    Note over QT: QueryGuard dropped → auto-calls complete(id)
    QT->>QT: Remove from active map, increment total_executed
```

**Implementation**: `QueryTracker` (`ferrosa-cql/src/virtual_tables/active_queries.rs`) uses `RwLock<HashMap<u64, QueryInfo>>` with `AtomicU64` for ID generation and total-executed counting. `QueryGuard` implements `Drop` to call `complete()`. The `ActiveQueriesTable` exposes this as `system_observability.active_queries` with primary key `query_id`. Columns: `query_id`, `client_address`, `username`, `query_text`, `keyspace`, `start_time` (epoch ms), `elapsed_ms`, `state`.

### Prometheus Scrape Flow

The Prometheus exporter converts all virtual tables in `system_observability` into text exposition format. Text columns become labels; numeric columns (Int, BigInt, Double) become metric values named `ferrosa_<table>_<column>`.

```mermaid
sequenceDiagram
    participant P as Prometheus
    participant HTTP as HTTP Server (port 9090)
    participant Prom as prometheus::render_metrics()
    participant VTR as VirtualTableRegistry

    P->>HTTP: GET /metrics
    HTTP->>Prom: render_metrics(registry)
    Prom->>VTR: list("system_observability")
    VTR->>Prom: Vec<Arc<dyn VirtualTable>>

    loop Each virtual table
        Prom->>Prom: table.read(None)
        Prom->>Prom: Text columns → labels, numeric columns → metric values
        Prom->>Prom: format_metric("ferrosa_{table}_{column}", labels, value)
    end

    Prom->>HTTP: Prometheus text exposition string
    HTTP->>P: HTTP 200 text/plain
```

**Implementation**: `render_metrics()` (`ferrosa-cql/src/prometheus.rs`) iterates `registry.list("system_observability")`, calls `table.read(None)` on each, and emits metric lines. Tombstoned cells are skipped. The `/metrics` route wiring is follow-on work — the renderer is complete.

### Web Dashboard Flow

An Axum-based web server (default port 9090) serves both a static HTML frontend and JSON API endpoints backed by the `VirtualTableRegistry`. The frontend auto-refreshes every 5 seconds.

```mermaid
sequenceDiagram
    participant Browser as Browser
    participant Axum as Axum Web Server (port 9090)
    participant API as API Handler
    participant VTR as VirtualTableRegistry
    participant VT as VirtualTable impl

    Browser->>Axum: GET /api/connections
    Axum->>API: get_connections(State(registry))
    API->>VTR: get("system_observability", "connections")
    VTR->>API: Arc<dyn VirtualTable>
    API->>VT: table.read(None)
    VT->>API: Vec<VirtualRow>
    API->>API: virtual_table_to_json() — decode cells by DataType
    API->>Axum: Json(Value)
    Axum->>Browser: HTTP 200 application/json

    Note over Browser: Auto-refresh every 5 seconds
```

**Endpoints**:

| Route | Handler | Virtual Table |
|-------|---------|---------------|
| `GET /api/connections` | `get_connections` | `system_observability.connections` |
| `GET /api/storage_stats` | `get_storage_stats` | `system_observability.storage_stats` |
| `GET /api/active_queries` | `get_active_queries` | `system_observability.active_queries` |
| `GET /api/tables` | `list_tables` | Lists all tables in `system_observability` |

**Implementation**: `ferrosa/src/web/mod.rs` binds on `FERROSA_WEB_BIND` (default `0.0.0.0:9090`). `ferrosa/src/web/api.rs` converts virtual table rows to JSON: Text columns become strings, Int/BigInt become numbers, Double uses `from_f64`, Boolean checks first byte, tombstones become `null`, and unrecognized types render as `"<binary>"`. Static files are served via rust-embed on the fallback route.

### TUI Monitor Flow

`ferrosa-ctl monitor` provides a terminal dashboard using ratatui. It connects as a CQL client and polls virtual tables via standard CQL queries.

```mermaid
sequenceDiagram
    participant User as User
    participant CTL as ferrosa-ctl monitor
    participant CQL as CQL Server
    participant VT as Virtual Tables

    User->>CTL: ferrosa-ctl monitor --node 127.0.0.1:9042

    CTL->>CQL: CqlClient::connect(addr)
    CTL->>CTL: Enter raw terminal mode (crossterm)

    loop Event loop (poll every 100ms)
        alt Refresh due (every 2 seconds)
            CTL->>CQL: SELECT * FROM system_observability.connections
            CQL->>VT: route_select → VirtualTable.read()
            VT->>CQL: rows
            CQL->>CTL: QueryResult

            CTL->>CQL: SELECT * FROM system_observability.active_queries
            CQL->>CTL: QueryResult

            CTL->>CQL: SELECT * FROM system_observability.storage_stats
            CQL->>CTL: QueryResult
        end

        CTL->>CTL: ratatui renders panels (Connections, Queries, Storage)

        alt Keyboard input
            Note over CTL: Tab → next panel, Up/Down → scroll, q → quit
        end
    end

    CTL->>CTL: Leave raw terminal mode
```

**Implementation**: `ferrosa-ctl/src/tui.rs` uses three panels (`Connections`, `Queries`, `Storage`). The event loop polls crossterm events with a 100 ms timeout and refreshes CQL data every 2 seconds via `AppState::refresh()`. Panel switching resets scroll position. The CQL client runs on a tokio runtime handle with `block_on()` for synchronous integration with the ratatui render loop.

### Subscription Flow (Deferred)

SUBSCRIBE allows clients to receive notifications when watched tables are written to. The observer hooks into the storage write path and filters mutations by subscription interest.

```mermaid
sequenceDiagram
    participant C as CQL Client
    participant Parser as CQL Parser
    participant SO as SubscriptionObserver
    participant Engine as Storage Engine

    C->>Parser: SUBSCRIBE SELECT * FROM ks.users
    Parser->>Parser: Produce Subscribe AST

    Parser->>SO: register(SubscriptionFilter{tables: [ks.users]})
    SO->>SO: Increment table_watch_counts[ks.users]
    SO->>Parser: SubscriptionId

    Note over SO: SubscriptionObserver implements WriteObserver (Async mode)

    loop On each write to ks.users
        Engine->>SO: on_write(table, mutation)
        SO->>SO: watches_table(table) → true
        Note over SO: Notification delivery via tokio channels (T19 — follow-on)
    end

    C->>C: Disconnect
    Note over SO: deregister(subscription_id)
    SO->>SO: Decrement table_watch_counts[ks.users]
```

**Implementation status**: The `SubscriptionObserver` (`ferrosa-storage/src/subscription_observer.rs`) implements `WriteObserver` with `ObserverMode::Async`. It maintains ref-counted `table_watch_counts` so multiple subscriptions to the same table are handled correctly — only when the last subscription is removed does `watches_table()` return false. Currently, `on_write()` returns an empty vec; notification delivery via tokio channels is deferred (T19). The `SubscriptionState` per-connection tracking and `SUBSCRIBE` AST parsing are also follow-on work.

## Cluster Formation Flow

Progressive join: Standalone → Pair → Forming → Cluster. No config flag — mode transitions automatically as peers connect.

```mermaid
sequenceDiagram
    participant N1 as Node 1 (Seed)
    participant N2 as Node 2
    participant N3 as Node 3

    Note over N1: Standalone mode

    N2->>N1: Connect (handshake + CQL broadcast exchange)
    N1->>N1: transition_to_pair() — N1=Primary, N2=Secondary
    N1-->>N2: Reverse connection pool

    Note over N1,N2: Pair mode — write forwarding, DDL replication

    N3->>N1: Connect (handshake + CQL broadcast exchange)
    N1->>N1: transition_to_forming() — DDL blocked
    N1->>N2: ClusterInvite{initiator=N1, peers=[N1,N2,N3]} (Data lane, 10 retries)
    N1->>N3: ClusterInvite{initiator=N1, peers=[N1,N2,N3]} (Data lane, 10 retries)

    par Hub-and-spoke → full mesh
        N2->>N3: Connect (learned from invite)
        N3->>N2: Connect (learned from invite)
    end

    Note over N1,N3: Forming mode — DDL unavailable

    N1->>N1: Register Raft handlers via LazyRaft (before init)
    N1->>N1: Raft initialize (seed only)
    N1-->>N2: AppendEntries (N2 joins via Raft, not initialize)
    N1-->>N3: AppendEntries (N3 joins via Raft, not initialize)

    rect rgb(230,245,255)
        Note over N1,N3: Phase A — Schema convergence
        N1->>N1: Replay user keyspaces/tables through Raft
        N1-->>N2: Raft commit schema
        N1-->>N3: Raft commit schema
    end

    rect rgb(230,255,230)
        Note over N1,N3: Phase B — Bootstrap streaming
        N1->>N2: Stream mutations for N2's token ranges
        N1->>N3: Stream mutations for N3's token ranges
        N2->>N1: Stream mutations for N1's token ranges
        N2->>N3: Stream mutations for N3's token ranges
        N3->>N1: Stream mutations for N1's token ranges
        N3->>N2: Stream mutations for N2's token ranges
    end

    rect rgb(255,245,230)
        Note over N1,N3: Phase C — Promotion (5s delay, TODO: RPC barrier)
        N1->>N1: Raft command: promote Joining → Normal
    end

    Note over N1,N3: Cluster mode — Raft consensus, tunable CL, Accord
```

### CQL Broadcast Address Resolution

Three-tier fallback for `system.peers.native_address`:

```
1. NodeInfo.cql_broadcast (set at ring construction, local node only)
   ↓ (not available for remote peers)
2. PeerManager.get_peer_cql_broadcast_sync(host_id) (learned from handshake)
   ↓ (try_read — non-blocking to avoid stalling CQL queries)
3. Fallback: internode_address:9042
```

`FERROSA_CQL_BROADCAST` env var enables container/NAT scenarios where the CQL listen address differs from the internode address (e.g., port-mapped Docker: internal `:9042` → external `:19042`). Hostname resolution is supported.

### Connection Lifecycle

CQL connections use RAII `IpSlotGuard` — per-IP connection slots are released on any exit (normal, panic, task cancellation). TCP keepalive is configured at 30s probe / 10s interval to detect dead peers within ~60s instead of the OS default 2+ hours.

## Related Specs

- [Overview](overview.md) — system overview
- [Components](components.md) — crate architecture
- [Accord](accord.md) — Accord consensus protocol specification
- [Cluster Formation Architecture](cluster-formation-architecture.md) — detailed formation spec
- [Cluster Formation State Machine](cluster-formation-state-machine.md) — state machine design
- [Testing](testing.md) — data integrity and chaos tests
