# Ferrosa Observability Design

## Overview

Observability for Ferrosa built on a virtual table abstraction as the single source of truth. All observability data is modeled as tables in a `system_observability` keyspace, queryable via CQL and graph queries. Four view layers consume the same data: CQL SELECT/SUBSCRIBE, Prometheus metrics endpoint, a CLI tool (`ferrosa-ctl`), and a web operations console.

## Virtual Table Framework

### Core Trait

The `VirtualTable` trait lives in `ferrosa-schema`, extending the existing schema system:

```rust
pub trait VirtualTable: Send + Sync {
    fn name(&self) -> &str;
    fn keyspace(&self) -> &str; // "system_observability"
    fn columns(&self) -> &[VirtualColumnDef];

    // Pull: returns latest cached snapshot, must never block.
    // DemandDriven tables maintain a background buffer populated
    // by the subscription infrastructure; read() returns whatever
    // is currently cached (may be empty before first collection).
    async fn read(&self, predicate: Option<&RowPredicate>) -> Vec<VirtualRow>;

    // Push: SUBSCRIBE support
    fn subscription_mode(&self) -> SubscriptionMode;
}

/// Lightweight row representation for virtual tables.
/// Lives in ferrosa-schema to avoid a dependency on ferrosa-cql types.
pub struct VirtualRow {
    pub cells: Vec<CellValue>,  // reuses ferrosa-common::CellValue
}

/// Column filter for virtual table reads.
pub struct RowPredicate {
    pub column: String,
    pub op: PredicateOp,
    pub value: CellValue,
}

pub enum PredicateOp {
    Eq,
    Gt,
    Lt,
    Gte,
    Lte,
}

pub struct VirtualColumnDef {
    pub name: String,
    pub data_type: CellValueType,
}

pub enum SubscriptionMode {
    /// Table supports polling at caller-specified interval
    Pollable,
    /// Activates collection on first subscriber, deactivates on last unsubscribe
    DemandDriven { default_interval: Duration },
    /// Does not support subscriptions
    None,
}
```

### Virtual Table Registry

A `VirtualTableRegistry` holds `Arc<dyn VirtualTable>` instances keyed by `(keyspace, table_name)`. The CQL router and graph executor check this registry before hitting storage — if a virtual table matches, they call `read()` instead of performing a storage lookup.

### Tables

| Table | Subscription Mode | Data Source |
|-------|-------------------|-------------|
| `connections` | Pollable | CQL server connection state |
| `storage_stats` | Pollable | Storage engine (memtable sizes, SSTable counts, S3 bucket stats) |
| `active_queries` | Pollable | CQL query executor |
| `cluster_peers` | Pollable | Cluster membership / Raft state |
| `cluster_topology` | Pollable | Token ring, rack/DC layout |
| `host_metrics` | DemandDriven(5s) | CPU/memory/network/disk — collected from peers only when subscribed |

### Column Schemas

**`connections`:**
peer_address (text), peer_port (int), state (text: startup/authenticating/ready), username (text), idle_seconds (int), requests_served (bigint), protocol_version (int)

**`storage_stats`:**
keyspace (text), table_name (text), memtable_size_bytes (bigint), memtable_count (int), sstable_count (int), sstable_size_bytes (bigint), s3_object_count (int), s3_bytes (bigint), pending_compactions (int)

**`active_queries`:**
query_id (uuid), client_address (text), username (text), query_text (text), keyspace (text), start_time (timestamp), elapsed_ms (bigint), state (text: parsing/planning/executing)

**`cluster_peers`:**
host_id (uuid), address (text), dc (text), rack (text), state (text: up/down/joining/leaving), raft_role (text: leader/follower/learner), schema_version (uuid), tokens (int)

**`cluster_topology`:**
token (bigint), host_id (uuid), address (text), dc (text), rack (text)

**`host_metrics`:**
host_id (uuid), address (text), cpu_percent (double), memory_used_bytes (bigint), memory_total_bytes (bigint), disk_used_bytes (bigint), disk_total_bytes (bigint), net_rx_bytes_sec (bigint), net_tx_bytes_sec (bigint), open_files (int), uptime_seconds (bigint)

### Graph Engine Integration

The graph executor's hop-by-hop expansion recognizes virtual table sources and calls `read()` instead of storage lookups. Since virtual tables are registered in `ferrosa-schema` and the graph engine already reads from the schema layer, this requires the graph planner to resolve virtual tables the same way it resolves regular tables.

## SUBSCRIBE Mechanism

SUBSCRIBE is a general-purpose streaming query extension for both CQL and graph queries. It works with virtual tables (observability) and regular tables (reactive data queries).

### Modes

| Mode | Trigger | Syntax | Use Case |
|------|---------|--------|----------|
| Change-driven (default) | WriteObserver detects matching mutation | `SUBSCRIBE SELECT ...` | Reactive applications, live dashboards on real data |
| Polling | Timer-based re-execution | `SUBSCRIBE SELECT ... EVERY 5s` | Virtual tables, periodic snapshots |

### Result Delivery

| Mode | Behavior | Syntax |
|------|----------|--------|
| Full (default) | Re-executes query, sends complete result set | `SUBSCRIBE SELECT ...` |
| Delta | Sends only changed rows with `change_type` column (`INSERT`, `UPDATE`, `DELETE`) | `SUBSCRIBE SELECT ... DELTA` |

### CQL Syntax

```sql
-- Change-driven, full result set (default)
SUBSCRIBE SELECT * FROM users WHERE active = true;

-- Change-driven, delta mode
SUBSCRIBE SELECT * FROM users WHERE active = true DELTA;

-- Polling fallback, full result set
SUBSCRIBE SELECT * FROM users WHERE active = true EVERY 5s;

-- Polling fallback, delta mode
SUBSCRIBE SELECT * FROM users WHERE active = true EVERY 5s DELTA;

-- Virtual tables (same syntax)
SUBSCRIBE SELECT * FROM system_observability.host_metrics EVERY 5s;

-- Cancel specific or all subscriptions
UNSUBSCRIBE <stream_id>;
UNSUBSCRIBE;
```

- New `SUBSCRIBE` statement type in the CQL parser, carrying the inner SELECT + optional interval + optional delta flag
- Server enforces a minimum interval floor (500ms) for polling mode to prevent abuse
- Maximum 8 concurrent subscriptions per connection

### Graph Query Syntax

Backward-compatible prefix keyword on existing graph queries:

```
-- Change-driven (default)
SUBSCRIBE MATCH (u:User {active: true})-[:FOLLOWS]->(f) RETURN u, f;

-- Delta mode
SUBSCRIBE MATCH (u:User {active: true})-[:FOLLOWS]->(f) RETURN u, f DELTA;

-- Polling fallback
SUBSCRIBE MATCH (u:User {active: true})-[:FOLLOWS]->(f) RETURN u, f EVERY 5s;
```

AST representation:

```rust
pub enum GraphStatement {
    Match(MatchQuery),
    Create(CreateMutation),
    Subscribe {
        inner: Box<MatchQuery>,  // only read queries can be subscribed to
        interval: Option<Duration>,  // None = change-driven, Some = polling
        delta: bool,
    },
    Unsubscribe { stream_id: Option<u16> },
    // ...
}
```

The parser rejects `SUBSCRIBE CREATE ...` and `SUBSCRIBE SUBSCRIBE ...` at parse time — only read queries (SELECT / MATCH) are valid subscription targets.

### Change-Driven Implementation via WriteObserver

Change-driven subscriptions use the existing `WriteObserver` infrastructure:

1. **Registration:** On `SUBSCRIBE` (without `EVERY`), a `SubscriptionObserver` registers the subscription's filter — table ID + predicate columns + partition key scope.
2. **Observer mode:** `SubscriptionObserver` implements `WriteObserver` with `ObserverMode::Async`. One `SubscriptionObserver` instance handles all active change-driven subscriptions.
3. **Filter matching in `on_write()`:** For each mutation, checks table ID and affected columns against registered filters. This is a cheap check — no query execution in the write path.
4. **Async re-execution:** When a filter matches, the async drain task re-executes the subscription's query and pushes the result set (or delta) to the subscriber's streaming connection.
5. **Backpressure:** Bounded channel (existing WriteObserver infrastructure). If a subscription can't keep up with write rate, mutations are dropped and the subscriber gets the next consistent snapshot when the queue drains.

**Delta mode specifics:** The async drain task compares the new result set against the subscription's last-sent result set (held in memory per subscription) and emits only changed rows with a `change_type` column. For full mode, it sends the complete re-executed result set.

### Wire Protocol

SUBSCRIBE responses reuse the existing CQL stream ID for the duration of the subscription. Each push sends a standard `ROWS` result frame on the original stream ID with a custom flag (0x10, `HAS_MORE_PAGES` repurposed as `STREAMING`) indicating more frames will follow. The final frame (on UNSUBSCRIBE or disconnect) omits this flag.

For delta mode, the result set includes an additional first column `change_type` (text: `INSERT`, `UPDATE`, `DELETE`) not present in the original query's column spec.

**UNSUBSCRIBE** takes an optional stream ID: `UNSUBSCRIBE <stream_id>` cancels a specific subscription, bare `UNSUBSCRIBE` cancels all subscriptions on the connection.

**Driver compatibility:** SUBSCRIBE is a Ferrosa extension. Standard CQL drivers will not understand the streaming response. Consumers are `ferrosa-ctl`, the web interface, and custom clients that opt in. Standard SELECT against virtual tables works with any CQL driver.

No changes to existing grammar rules. `SUBSCRIBE` is a new production that delegates to the existing query parser for its body. The executor checks if the statement is wrapped in `Subscribe` and, if so, loops on a timer pushing results instead of returning once.

### Subscription Manager

The `SubscriptionManager` lives in `ferrosa-net` since it drives internode communication. CQL and graph layers tell `ferrosa-net` "someone wants to subscribe to X" and it handles ref-counting and peer coordination.

### Demand-Driven Collection Lifecycle

```
First SUBSCRIBE arrives for host_metrics
  → SubscriptionManager increments ref count (0 → 1)
  → Triggers internode "start collecting" request to all peers
  → Peers begin sending stats at the table's default_interval

More subscribers join → ref count increases, no new internode work
  (collection always uses the table's default_interval, not per-subscriber intervals;
   the subscriber's EVERY clause controls how often they receive frames,
   which may be a multiple of the collection interval)

Last subscriber disconnects
  → Ref count drops to 0
  → Sends internode "stop collecting" to peers
  → Peers stop sending, no data flows
```

**Failure semantics:**

- Unreachable peers: reported in `host_metrics` with null metric values and `state = 'unreachable'` in `cluster_peers`. Collection continues for reachable peers.
- Buffer limits: each DemandDriven table maintains a bounded ring buffer (configurable, default 1000 entries). Oldest entries are evicted on overflow.
- Peer reconnection: when a previously unreachable peer becomes reachable, collection resumes automatically if there are active subscribers.

## Prometheus Exporter

A `/metrics` endpoint that reads from virtual tables and formats as Prometheus text exposition:

- Runs on its own port (e.g. `:9091`), managed by `ferrosa-net`
- On each Prometheus scrape, iterates registered virtual tables and converts rows to Prometheus metric lines
- Scalar tables (`storage_stats`, `host_metrics`) map to gauges/counters
- Tabular data (`connections`, `active_queries`) emitted as gauges with labels (e.g. `ferrosa_connections_active{peer="10.0.1.5", state="ready"}`)
- Pull model — just calls `read()` on each scrape, no subscription needed for Pollable tables
- For DemandDriven tables: if the Prometheus exporter is enabled, it acts as a persistent subscriber. This is the pragmatic choice since Prometheus scrapes are inherently periodic — a cold-start-per-scrape approach would return stale/empty data. Operators who want zero-overhead can disable the Prometheus exporter entirely
- Metric naming convention: `ferrosa_<table>_<column>`

## CLI Tool (`ferrosa-ctl`)

New `ferrosa-ctl` crate in the workspace. Connects to a Ferrosa node using `ferrosa-cql`'s frame codec for wire encoding/decoding, with a thin client module added to `ferrosa-cql` that handles connection establishment, STARTUP handshake, and authentication from the client side. This reuses existing codec types rather than depending on an external CQL driver.

### Subcommand Mode

Query and exit:

```bash
ferrosa-ctl status                    # cluster health summary
ferrosa-ctl connections list          # active connections
ferrosa-ctl connections --sort=idle   # sorted/filtered
ferrosa-ctl queries --long-running    # queries over threshold
ferrosa-ctl storage                   # bucket sizes, SSTable stats
ferrosa-ctl topology                  # token ring, rack/DC layout
ferrosa-ctl peers                     # cluster members + status
```

All subcommands issue `SELECT` against virtual tables and format output as table/JSON.

### TUI Monitor Mode

```bash
ferrosa-ctl monitor                   # full dashboard
ferrosa-ctl monitor --panel=queries   # single panel
```

Uses `ratatui` for the terminal UI. Issues `SUBSCRIBE` queries for live-updating panels. Panels map 1:1 to virtual tables:

- Connections panel
- Active queries panel (highlights long-running)
- Storage stats panel
- Cluster peers + health panel
- Host metrics panel (CPU/mem/net/disk bars)

Keyboard navigation to switch panels, drill into details, and eventually kill queries or manage connections.

## Web Interface

Runs on its own port (e.g. `:9090`), separate from CQL (`:9042`) and Prometheus (`:9091`). Lightweight JS app compiled into the binary via `rust-embed`, served from the HTTPS interface.

### Backend

Axum server in `ferrosa-net` exposing virtual table data as a JSON API. WebSocket endpoint for live updates — server-side SUBSCRIBE drives WebSocket push.

### Incremental Delivery

**Phase 1 — Status Dashboard (read-only):**

- Cluster overview: peers, health indicators, topology summary
- Connection list with basic filtering
- Storage stats (table sizes, SSTable counts, S3 bucket usage)
- Active queries list with duration highlighting

**Phase 2 — Live Monitoring:**

- WebSocket-driven auto-updating panels
- Host metrics charts (CPU, memory, network, disk over time)
- Simple time-series retention in-memory for sparklines (5-minute window at 1-second resolution, ring buffer per metric per host)

**Phase 3 — Operations Console:**

- Kill long-running queries
- Drain/undrain nodes
- Connection management (force disconnect)
- Topology visualization (token ring diagram, rack/DC map)

**Phase 4 — Advanced:**

- Query plan visualization for graph traversals
- Storage drilldown (per-SSTable, per-partition stats)
- Audit log viewer

## Authorization

Virtual table access uses the existing permission system:

- `SELECT` permission on `system_observability` keyspace required for reads
- `SUBSCRIBE` requires `SELECT` permission — no new permission type needed, but subscription count is bounded (max 8 per connection) to limit resource consumption
- Prometheus endpoint: configurable auth — either unauthenticated (firewall-protected) or basic auth via config
- Web interface: requires authentication. Phase 1 uses basic auth backed by CQL credentials. Phase 3+ operations (kill query, drain node) require `SUPERUSER` role

## Relationship to CQL REGISTER/EVENT

CQL v5 already has `REGISTER`/`EVENT` for schema changes, topology changes, and status changes. `SUBSCRIBE` is more general — it streams arbitrary query results at intervals. The two coexist: `REGISTER` remains for push-based event notifications (fires on change), while `SUBSCRIBE` is for periodic polling of virtual table state. A `SUBSCRIBE` on `cluster_peers` gives a periodic snapshot; a `REGISTER` for `STATUS_CHANGE` fires only when a peer goes up/down.

## Crate Responsibilities

| Crate | New Responsibilities |
|-------|---------------------|
| `ferrosa-schema` | `VirtualTable` trait, `VirtualTableRegistry`, `system_observability` keyspace + table definitions |
| `ferrosa-storage` | Implements `storage_stats` virtual table, hosts `SubscriptionObserver` (implements `WriteObserver`) for change-driven subscriptions |
| `ferrosa-cql` | `SUBSCRIBE`/`UNSUBSCRIBE` parsing (with `EVERY`/`DELTA` modifiers), streaming frame type, query executor calls virtual table `read()`, implements `connections` and `active_queries` tables |
| `ferrosa-cql` (client module) | Thin CQL client: frame codec reuse, connection establishment, STARTUP handshake, auth — used by `ferrosa-ctl` |
| `ferrosa-net` | **Not yet created.** `SubscriptionManager`, internode metrics collection protocol, Prometheus `/metrics` endpoint, web interface (HTTPS + embedded JS), demand-driven peer coordination |
| `ferrosa-cluster` | **Not yet created.** Implements `cluster_peers`, `cluster_topology`, `host_metrics` virtual tables |
| `ferrosa-graph` | Graph executor recognizes virtual table sources, `SUBSCRIBE`/`UNSUBSCRIBE` parsing (with `EVERY`/`DELTA` modifiers) for graph queries |
| `ferrosa-ctl` | **New crate** — CLI binary with subcommands + `ratatui` TUI monitor mode |

## Dependency Flow

```
ferrosa-ctl ──▶ ferrosa-cql (client module: codec + handshake)

ferrosa (binary)
  ├── ferrosa-net (subscription mgr, prometheus, web)
  ├── ferrosa-cql (SUBSCRIBE parsing, streaming)
  ├── ferrosa-cluster (peer/topology/host tables)
  ├── ferrosa-storage (storage stats table)
  ├── ferrosa-graph (virtual table reads, SUBSCRIBE parsing)
  └── ferrosa-schema (VirtualTable trait, registry)
```

## Design Patterns

This design follows existing Ferrosa patterns:

- **Pluggable trait abstraction** — `VirtualTable` follows the same pattern as `AuditSink` (trait + registry + multiple implementations)
- **Config-driven behavior** — subscription intervals, port bindings, minimum interval floors all configurable via config structs with `Default` + `from_env()`
- **Lock-free reads** — `VirtualTableRegistry` uses `ArcSwap` for lock-free lookups on the read path, consistent with schema snapshot reads
- **Demand-driven collection** — `DemandDriven` subscription mode ensures no internode overhead unless actively observed, following the project's efficiency-first approach
- **Typed errors with `#[non_exhaustive]`** — subscription errors, virtual table errors follow existing error patterns
- **Backward-compatible extensions** — `SUBSCRIBE` is a prefix keyword, not a grammar change; existing CQL and graph queries parse identically
