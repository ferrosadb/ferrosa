# ferrosa-graph Engine Design — Phase 0 + Phase 1 with Security Mitigations

> **Status:** Draft
> **Date:** 2026-03-13
> **Prerequisite:** ferrosa-graph parser (merged, PR #23)
> **Parent spec:** `2026-03-12-ferrosa-graph-design.md` (architecture overview)
> **Threat model:** `specs/threat-model-graph.md` (T1–T11)
> **Security issues:** GitHub #11–#20 (T2–T11)
>
> **Note:** This spec supersedes the parent spec's adjacency table definition.
> The parent spec defines `system_graph.adjacency` as a single global table
> with `UUID` vertex IDs. This spec uses per-keyspace tables
> (`system_graph_<ks>.adjacency`) with `BLOB` vertex IDs (raw partition key
> bytes). The parent spec should be updated to match.

## Goal

Build the ferrosa-graph engine from storage hooks through HTTP endpoint, with
all 10 remaining security mitigations (T2–T11) baked into each vertical slice.
The result is a working Cypher query endpoint that can read and write graph data
stored in CQL tables, backed by an async adjacency index.

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Implementation strategy | Vertical slices (6 slices) | Each slice produces testable functionality; security is built in, not bolted on |
| Observer model | Push (WriteObserver trait), async mode | Low latency, out of CQL hot path; reconciliation catches drops |
| HTTP framework | Axum (on hyper/tokio) | Minimal, routing + middleware + extractors; shared tokio runtime with CQL |
| Adjacency scope | Per-keyspace | Tenant isolation via keyspace boundaries; simpler permission model |
| RBAC model | Reuse existing CQL permissions | `Select` for reads, `Modify` for writes, `Create` on keyspace for `graph.*` extensions |
| Audit | Audit everything (no sampling) | Simple, complete; sampling can be added at the sink layer later |

## Vertical Slices

| Slice | Crate(s) | Delivers | Mitigations |
|-------|----------|----------|-------------|
| 1 | ferrosa-schema | Table extensions, `is_system` flag, `graph.*` validation | T6, T7, T10 (partial) |
| 2 | ferrosa-storage | WriteObserver trait, registration, async dispatch, backpressure | T9 |
| 3 | ferrosa-graph | Adjacency index schema, observer impl, reconciliation | T5 |
| 4 | ferrosa-graph | Planner, executor, per-hop auth, resource limits | T3, T4 |
| 5 | ferrosa-graph | HTTP endpoint, auth, TLS, error sanitization, audit | T2, T8, T10, T11 |
| 6 | ferrosa | Binary integration, startup wiring, graceful shutdown | — |

---

## Slice 1: Schema Hooks (ferrosa-schema)

### Table extensions map

Add `extensions: HashMap<String, String>` to `TableMetadata`. Opaque key-value
store on every table, wired through DDL:

```sql
CREATE TABLE graph.person (
    id UUID PRIMARY KEY, name TEXT
) WITH extensions = {'graph.type': 'vertex', 'graph.label': 'person'};
```

- Stored in `TableMetadata.extensions`
- Passed through `CREATE TABLE` and `ALTER TABLE` DDL handlers
- `TableUpdates` gains an `extensions: Option<HashMap<String, String>>` field
  for ALTER TABLE support
- No interpretation by ferrosa-schema itself — just storage and validation hooks

### Graph extension validation (T6 — extension poisoning)

When any key starting with `graph.` is set via DDL:

- Require `Permission::Create` on the keyspace (not just `ALTER` on the table)
- Validate `graph.type` is one of `vertex` or `edge`
- If `graph.type = 'edge'`, validate:
  - `graph.source_label` and `graph.target_label` reference existing tables in
    the same keyspace with `graph.type = 'vertex'`
  - `graph.source` and `graph.target` reference existing columns in the table
- Emit `AuditEventKind::TableAltered` (existing event — extensions are part of
  ALTER TABLE)

### System table flag (T7 — cross-protocol leakage)

Add `is_system: bool` to `TableMetadata` (default `false`). When true:

- `DROP TABLE` and `ALTER TABLE` are rejected via the schema registry
- CQL `SELECT` requires checking permissions on referenced user tables
  (enforced at query routing time in ferrosa-cql, not in schema itself)
- The adjacency table will be created with `is_system: true`

### Testing

- Unit test: create table with extensions, read them back
- Unit test: setting `graph.type` without `Permission::Create` on keyspace fails
- Unit test: setting `graph.source_label` to nonexistent table fails
- Unit test: `is_system: true` rejects DROP and ALTER

---

## Slice 2: WriteObserver (ferrosa-storage)

### WriteObserver trait

```rust
pub enum ObserverMode {
    /// StorageEngine awaits on_write before returning to caller.
    Sync,
    /// StorageEngine spawns on_write as a background task.
    Async,
}

/// Called by ferrosa-storage on every write to observed tables.
///
/// **Important:** `on_write` must be non-blocking. Implementations should only
/// perform CPU-bound work: read schema from `ArcSwap` (lock-free), extract
/// keys from the mutation, and generate derived mutations. Do not perform
/// async I/O, disk reads, or network calls inside `on_write`. For async
/// observers, the method runs on the drain task's thread — blocking it stalls
/// the entire observer pipeline.
pub trait WriteObserver: Send + Sync {
    fn mode(&self) -> ObserverMode;

    /// Which tables this observer watches.
    ///
    /// Called on every write to check if the observer should fire. For dynamic
    /// table sets (e.g., the adjacency observer watches all edge tables), query
    /// the schema snapshot here — `ArcSwap::load()` is lock-free and fast.
    fn tables(&self) -> Vec<TableId>;

    fn on_write(&self, table: TableId, mutation: &Mutation) -> Vec<Mutation>;
}
```

Note: `tables()` returns `Vec<TableId>` (owned) rather than `&[TableId]` to
support dynamic table sets. The adjacency observer queries the schema snapshot
on each call to discover newly created edge tables without requiring a
re-registration hook.

### Registration and dispatch

Add to `StorageEngine`:

- `observers: Vec<Arc<dyn WriteObserver>>` field
- `register_observer(Arc<dyn WriteObserver>)` method
- In `write()` and `batch_write()`: after commit log + memtable, iterate
  observers. For each observer whose `tables()` matches, dispatch based on mode.

### Bounded queue and backpressure (T9 — observer amplification)

Async observers use a bounded channel instead of inline dispatch:

- Each async observer gets a `tokio::sync::mpsc` channel
  (capacity configurable, default 10,000)
- Write path sends `(TableId, Mutation)` to the channel. If the channel is
  full, the mutation is **dropped** (not blocking the write path) and a drop
  counter is incremented
- A dedicated tokio task drains the channel, calls `on_write()`, and writes
  resulting mutations back via `batch_write()`
- Batching: the drain task accumulates mutations for up to 10ms before flushing
- Metrics exposed: queue depth, drop count, flush count

**Why drop instead of block:** The write path is the CQL hot path. Blocking it
because the observer is slow would degrade CQL performance. Dropped mutations
are recovered by background reconciliation (Slice 3).

### Testing

- Unit test with mock `WriteObserver` that records calls
- Test that async observer doesn't block the write path
- Test backpressure: fill the channel, verify writes succeed and drop counter
  increments

---

## Slice 3: Adjacency Index + Observer (ferrosa-graph)

### Per-keyspace adjacency table schema

When the first `graph.type = 'edge'` table is created in a keyspace,
automatically create:

```sql
-- system_graph_<keyspace>.adjacency (auto-created, is_system: true)
CREATE TABLE system_graph_social.adjacency (
    vertex_id BLOB,
    direction TINYINT,    -- 0=OUT, 1=IN
    edge_label TEXT,      -- edge table name
    neighbor_id BLOB,     -- the other vertex
    edge_table TEXT,      -- fully qualified table for property lookups
    PRIMARY KEY (vertex_id, direction, edge_label, neighbor_id)
);
```

- Created via `Schema` with `is_system: true` — protected from user DDL
- `vertex_id` and `neighbor_id` are raw partition key bytes (not UUID-specific)
- One adjacency table per keyspace

### AdjacencyIndexObserver

Implements `WriteObserver` with `ObserverMode::Async`. Watches all tables where
`extensions["graph.type"] == "edge"`.

On each mutation:

1. Read table extensions to get `graph.source`, `graph.target`,
   `graph.source_label`, `graph.target_label`
2. Extract source and target key bytes from the mutation
3. Generate two adjacency mutations:
   - OUT: `(vertex_id=src, dir=0, edge_label=table_name, neighbor=dst,
     edge_table=ks.table)`
   - IN: `(vertex_id=dst, dir=1, edge_label=table_name, neighbor=src,
     edge_table=ks.table)`
4. For DELETE mutations: generate tombstones with timestamp >= the edge
   deletion timestamp (T5)

### Background reconciliation (T5 — adjacency inconsistency)

Periodic background task (default: every 5 minutes, configurable):

1. For each edge table with `graph.type = 'edge'`:
   - Scan edge table partitions
   - For each edge row, verify corresponding OUT and IN adjacency entries exist
   - Repair missing entries
   - Remove orphaned adjacency entries
2. Record reconciliation metrics: entries checked, repaired, orphaned

Safety net for dropped observer mutations (backpressure) and crash recovery
gaps. Runs in its own tokio task. "Low priority" is achieved by yielding between
partition scans (`tokio::task::yield_now()`) and limiting concurrent partition
reads to 1 at a time, so reconciliation does not compete with query workloads.

### Crash recovery

Observer mutations go through `StorageEngine::write()` → commit log. On
restart, the commit log replays all pending mutations including adjacency
writes. The reconciliation job catches anything that was in the observer's
in-memory queue at crash time.

### Testing

- Integration test: write edge via `StorageEngine::write()`, verify adjacency
  entries appear after async drain
- Test DELETE produces tombstones with correct timestamps
- Test reconciliation detects and repairs a missing adjacency entry
- Test reconciliation detects and removes orphaned adjacency entries (edge
  deleted but adjacency entry persists)
- Test `is_system: true` prevents CQL DROP/ALTER on adjacency table

---

## Slice 4: Planner + Executor (ferrosa-graph)

### Logical planner

`parse(query) -> Statement -> validate(statement, schema) -> LogicalPlan`

Validation phase:

- Resolve labels to tables: `Person` -> `graph.person` (via
  `extensions["graph.label"]`)
- Verify all referenced labels exist as vertex/edge tables in the keyspace
- Verify all property references map to columns in the resolved tables
- **Per-hop permission check (T3):** For every table in the pattern, call
  `check_permission(snap, auth, Permission::Select, Resource::Table(ks, table))`.
  Fail fast at plan time — don't start executing and then error mid-traversal
- For mutations (CREATE/SET/DELETE): check `Permission::Modify`

Output: `LogicalPlan` — a `PatternGraph` with resolved table bindings,
predicates, and projections.

### Physical planner

`LogicalPlan -> PhysicalPlan`

Phase 1 strategy is always `Expand`:

- Choose anchor node (most selective: partition key filter > label > unfiltered)
- Order hops from anchor outward
- Attach WHERE predicates as filters at the appropriate hop

Output: `PhysicalPlan::Expand { anchor, hops, filters, projection }`

### Expand executor

Walks the graph hop-by-hop:

1. **Anchor lookup:** Read anchor vertex table with any property filters
2. **Expand each hop:** For each vertex from previous step, read adjacency
   index (`vertex_id, direction, edge_label`) -> neighbor IDs
3. **Property fetch:** For RETURN columns, read vertex/edge tables by
   partition key
4. **Filter:** Apply WHERE predicates after property fetch
5. **Project:** Extract RETURN columns, apply aliases, ORDER BY, LIMIT

All reads go through `StorageEngine::read()` — partition key point lookups
only in Phase 1.

### Resource limits (T4 — DoS via expensive queries)

```rust
pub struct GraphEngineConfig {
    pub query_timeout: Duration,        // default 30s
    pub max_result_rows: usize,         // default 10,000
    pub max_fan_out_per_hop: usize,     // default 10,000
}
```

Enforcement:

- Wrap executor in `tokio::time::timeout(config.query_timeout, execute(...))`
- After each hop expansion, check neighbor count against
  `max_fan_out_per_hop`. If exceeded, return error
- Track total result rows. If `max_result_rows` exceeded, stop and return
  partial results with truncation warning in response stats

### Testing

- Unit tests for logical planner: valid queries resolve, invalid
  labels/properties error
- Unit tests for permission denial at plan time (T3)
- Integration test: write vertices + edges, run expand executor, verify results
- Test timeout enforcement
- Test fan-out limit with a supernode

---

## Slice 5: HTTP Endpoint (ferrosa-graph)

### Routes

| Method | Path | Auth | Purpose |
|--------|------|------|---------|
| POST | `/graph/query` | Yes | Execute Cypher, return JSON rows |
| POST | `/graph/explain` | Yes | Return physical plan as JSON |
| GET | `/graph/schema` | Yes | List vertex/edge tables with labels |
| GET | `/graph/health` | No | Liveness check |

Keyspace is a required field in the JSON request body for `/graph/query` and
`/graph/explain`:

```json
{
  "query": "MATCH (a:Person)-[:KNOWS]->(b) RETURN b.name",
  "keyspace": "social"
}
```

The `/graph/schema` endpoint takes keyspace as a query parameter:
`GET /graph/schema?keyspace=social`.

### GraphEngine API

All query methods take `AuthContext` for permission enforcement:

```rust
pub struct GraphEngine {
    schema: Arc<Schema>,
    storage: Arc<StorageEngine>,
    config: GraphEngineConfig,
}

impl GraphEngine {
    pub fn new(schema: Arc<Schema>, storage: Arc<StorageEngine>, config: GraphEngineConfig) -> Self;
    pub fn execute(&self, query: &str, keyspace: &str, auth: &AuthContext) -> Result<GraphResult>;
    pub fn explain(&self, query: &str, keyspace: &str, auth: &AuthContext) -> Result<PhysicalPlan>;
    pub fn graph_schema(&self, keyspace: &str, auth: &AuthContext) -> Result<GraphSchema>;
}
```

The HTTP layer extracts `AuthContext` from the auth middleware and passes it
through to every engine method.

### Authentication middleware (T2 — unauthenticated access)

Axum middleware layer on all routes except `/health`:

- Extract `Authorization` header — supports `Basic` and `Bearer`
- Call `Schema::authenticate(username, password)` -> `AuthContext`
- Rate limit via schema's `AuthRateLimiter`
- On failure: 401, emit `AuditEventKind::AuthFailed`
- On success: inject `AuthContext` into request extensions
- **No unauthenticated mode** — dev deployments use a default dev role

### TLS (T11 — unencrypted transport)

```rust
pub struct GraphHttpConfig {
    pub bind_addr: SocketAddr,          // default 0.0.0.0:7474
    pub tls_cert_path: Option<PathBuf>,
    pub tls_key_path: Option<PathBuf>,
    pub require_tls: bool,              // default true in production
    pub max_request_body_bytes: usize,  // default 1MB
}
```

Behavior:

- Cert + key provided: bind with `axum_server::tls_rustls`
- `require_tls` true, no cert: **fail startup** with clear error
- `require_tls` false, no cert: bind plaintext, log warning

### Axum security considerations

- `tower_http::limit::RequestBodyLimitLayer(config.max_request_body_bytes)`
  enforces request body size (default 1MB). Requests exceeding the limit get
  413 Payload Too Large before the body is read into memory.
- Request timeout at server level (separate from query timeout)
- No path traversal risk — static routes, no file serving
- `tower_http::catch_panic::CatchPanicLayer` catches handler panics -> 500
- No CORS needed in Phase 1 (API-only, no browser clients)

### Error sanitization (T8 — info disclosure)

| Internal error | HTTP status | Client message |
|----------------|-------------|----------------|
| `ParseError` | 400 | `"Syntax error at position N: <message>"` |
| Permission denied (read) | 403 | `"Access denied"` |
| Permission denied (write) | 403 | `"Access denied"` |
| Validation error (bad label/property) | 400 | `"Invalid query: <message>"` |
| Mutation target not found | 400 | `"Invalid query: unknown label or property"` |
| Timeout | 408 | `"Query exceeded time limit"` |
| Resource limit | 400 | `"Query exceeded resource limit"` |
| Request body too large | 413 | `"Payload too large"` |
| Internal / panic | 500 | `"Internal server error"` |

- Full details logged server-side via `tracing`
- Parse errors include position (useful) but not internal parser state
- Panics never leak to client

### Audit events (T10 — audit gap)

Two new variants in `AuditEventKind`:

```rust
GraphQueryExecuted {
    query: String,
    keyspace: String,
    rows_returned: usize,
    execution_ms: u64,
    status: GraphAuditStatus,  // Ok, Timeout, Denied, Error
}

GraphMutationExecuted {
    query: String,
    keyspace: String,
    vertices_affected: usize,
    edges_affected: usize,
    status: GraphAuditStatus,
}
```

`GraphAuditStatus` distinguishes successful operations from failures:

- `Ok` — query completed successfully
- `Timeout` — query killed by timeout
- `Denied` — permission check failed
- `Error` — internal error

Emitted after every request (including failures). `AuditEvent` wrapper carries
`actor` (from AuthContext) and `source` (client IP from TCP connection).

### Testing

- Integration test: start Axum server, POST query, verify JSON response
- Test auth: missing header -> 401
- Test auth: bad credentials -> 401 + `AuthFailed` audit event
- Test permission denied -> 403, no internal details
- Test parse error -> 400, position info but no internals
- Test panic recovery -> 500 generic message
- Test TLS required but no cert -> startup failure
- Test audit: verify `GraphQueryExecuted` emitted

---

## Slice 6: Binary Integration (ferrosa)

### Startup sequence

1. Create `GraphEngine::new(schema.clone(), storage.clone(), graph_config)`
2. GraphEngine constructor:
   - Scans schema for keyspaces with `graph.type` edge tables
   - Creates `system_graph_<ks>.adjacency` tables if needed
   - Creates `AdjacencyIndexObserver` for each keyspace
   - Calls `storage.register_observer(observer)` for each
   - Starts background reconciliation task
3. Start HTTP server: `start_graph_http(graph_engine, http_config)`
4. Both CQL and graph servers share the same tokio runtime

### Configuration

```rust
pub struct GraphConfig {
    pub http: GraphHttpConfig,
    pub engine: GraphEngineConfig,
    pub observer: ObserverConfig,        // queue_capacity, batch_interval_ms
    pub reconciliation_interval: Duration, // default 5 min
    pub enabled: bool,                   // feature flag
}
```

When `enabled: false`: no observer registered, no HTTP server, no adjacency
tables. Zero overhead.

### Graceful shutdown

On SIGTERM:

1. Stop accepting new HTTP connections
2. Drain in-flight graph queries (respect query timeout)
3. Flush observer queue (best-effort, bounded by timeout)
4. CQL server shuts down independently

### Testing

- Integration test: start both servers, write via CQL, read via Cypher HTTP
- Test `enabled: false` — no graph endpoint, no observer

---

## Security Mitigation Coverage

| Threat | Risk | Slice | Mitigation |
|--------|------|-------|------------|
| T1: Parser exploitation | 4 High | (done) | Proptests, depth limit (PR #23). Panic catch in Slice 5 |
| T2: Unauthenticated access | 9 Critical | 5 | HTTP auth middleware, no unauth mode |
| T3: Auth bypass via traversal | 6 High | 4 | Per-hop permission check at plan time |
| T4: DoS expensive queries | 6 High | 4 | Timeout, max rows, max fan-out |
| T5: Adjacency inconsistency | 4 High | 3 | Commit log recovery, background reconciliation, tombstone timestamps |
| T6: Extension poisoning | 4 High | 1 | `Permission::Create` required, label validation |
| T7: Cross-protocol leakage | 4 High | 1 | `is_system` flag, CQL access restricted |
| T8: HTTP info disclosure | 2 Medium | 5 | Error sanitization, panic catch, server-side logging |
| T9: Observer amplification | 4 High | 2 | Bounded queue, drop + reconcile, metrics |
| T10: Audit gap | 6 High | 1 + 5 | Extension audit via existing events, new graph query/mutation events |
| T11: Unencrypted HTTP | 6 High | 5 | TLS via rustls, require in production |

## New Crate Dependencies

| Crate | Purpose | Used in |
|-------|---------|---------|
| `axum` | HTTP routing, middleware, extractors | ferrosa-graph |
| `axum-server` | TLS support (rustls backend) | ferrosa-graph |
| `tower-http` | `CatchPanicLayer`, request body limits | ferrosa-graph |
| `serde` + `serde_json` | JSON request/response | ferrosa-graph |
| `hyper` | (transitive via axum) | ferrosa-graph |

## File Structure (new and modified files)

```
ferrosa-schema/src/
  metadata/table.rs          # +extensions field, +is_system field
  registry.rs                # +graph.* validation, +system table protection
  audit/event.rs             # +GraphQueryExecuted, +GraphMutationExecuted

ferrosa-storage/src/
  observer.rs                # NEW: WriteObserver trait, ObserverMode
  engine.rs                  # +observers field, +register_observer, +dispatch
  lib.rs                     # +pub mod observer

ferrosa-graph/src/
  lib.rs                     # +pub mod engine, planner, executor, adjacency, http
  engine.rs                  # NEW: GraphEngine, GraphEngineConfig
  error.rs                   # NEW: GraphError enum
  planner/
    mod.rs                   # NEW: plan() entry point
    logical.rs               # NEW: validate, resolve labels, PatternGraph
    physical.rs              # NEW: Expand plan, anchor selection
  executor/
    mod.rs                   # NEW: execute() entry point
    expand.rs                # NEW: hop-by-hop expansion
    result.rs                # NEW: GraphResult rows
  adjacency/
    mod.rs                   # NEW: AdjacencyIndex read helpers
    observer.rs              # NEW: AdjacencyIndexObserver (WriteObserver impl)
    schema.rs                # NEW: adjacency table schema definition
  http.rs                    # NEW: Axum server, routes, auth, TLS, errors

ferrosa/src/
  main.rs                    # +GraphEngine init, +HTTP server start
```
