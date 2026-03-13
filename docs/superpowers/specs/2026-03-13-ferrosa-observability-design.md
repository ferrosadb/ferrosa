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
    fn columns(&self) -> &[ColumnDefinition];

    // Pull: standard SELECT
    fn read(&self, predicate: Option<&RowPredicate>) -> Vec<Row>;

    // Push: SUBSCRIBE support
    fn subscription_mode(&self) -> SubscriptionMode;
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

### Graph Engine Integration

The graph executor's hop-by-hop expansion recognizes virtual table sources and calls `read()` instead of storage lookups. Since virtual tables are registered in `ferrosa-schema` and the graph engine already reads from the schema layer, this requires the graph planner to resolve virtual tables the same way it resolves regular tables.

## SUBSCRIBE Mechanism

### CQL Extension

```sql
SUBSCRIBE SELECT * FROM system_observability.host_metrics EVERY 5s;
SUBSCRIBE SELECT * FROM system_observability.connections EVERY 1s;
UNSUBSCRIBE;  -- on same connection, stops streaming
```

- New `SUBSCRIBE` statement type in the CQL parser, carrying the inner SELECT + interval
- Response uses a streaming frame type — repeated `ROWS` frames on the same stream ID with a flag indicating "more coming"
- `UNSUBSCRIBE` or client disconnect terminates the stream
- Server enforces a minimum interval floor (500ms) to prevent abuse

### Graph Query Extension

Backward-compatible prefix keyword on existing graph queries:

```
SUBSCRIBE MATCH (h:Host)-[:RUNS]->(p:Process)
  WHERE h.cpu_percent > 80
  RETURN h.name, p.name, h.cpu_percent
  EVERY 5s;
```

AST representation:

```rust
pub enum GraphStatement {
    Match(MatchQuery),
    Create(CreateMutation),
    Subscribe {
        inner: Box<GraphStatement>,
        interval: Duration,
    },
    Unsubscribe,
    // ...
}
```

No changes to existing grammar rules. `SUBSCRIBE` is a new production that delegates to the existing query parser for its body. The executor checks if the statement is wrapped in `Subscribe` and, if so, loops on a timer pushing results instead of returning once.

### Subscription Manager

The `SubscriptionManager` lives in `ferrosa-net` since it drives internode communication. CQL and graph layers tell `ferrosa-net` "someone wants to subscribe to X" and it handles ref-counting and peer coordination.

### Demand-Driven Collection Lifecycle

```
First SUBSCRIBE arrives for host_metrics
  → SubscriptionManager increments ref count (0 → 1)
  → Triggers internode "start collecting" request to all peers
  → Peers begin sending stats at requested interval

More subscribers join → ref count increases, no new internode work

Last subscriber disconnects
  → Ref count drops to 0
  → Sends internode "stop collecting" to peers
  → Peers stop sending, no data flows
```

## Prometheus Exporter

A `/metrics` endpoint that reads from virtual tables and formats as Prometheus text exposition:

- Runs on its own port (e.g. `:9091`), managed by `ferrosa-net`
- On each Prometheus scrape, iterates registered virtual tables and converts rows to Prometheus metric lines
- Scalar tables (`storage_stats`, `host_metrics`) map to gauges/counters
- Tabular data (`connections`, `active_queries`) emitted as gauges with labels (e.g. `ferrosa_connections_active{peer="10.0.1.5", state="ready"}`)
- Pull model — just calls `read()` on each scrape, no subscription needed for Pollable tables
- For DemandDriven tables, Prometheus scrape activates collection briefly or is treated as a persistent subscriber if scrape interval is configured
- Metric naming convention: `ferrosa_<table>_<column>`

## CLI Tool (`ferrosa-ctl`)

New `ferrosa-ctl` crate in the workspace. Connects to a Ferrosa node as a CQL client.

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
- Simple time-series retention in-memory for sparklines

**Phase 3 — Operations Console:**

- Kill long-running queries
- Drain/undrain nodes
- Connection management (force disconnect)
- Topology visualization (token ring diagram, rack/DC map)

**Phase 4 — Advanced:**

- Query plan visualization for graph traversals
- Storage drilldown (per-SSTable, per-partition stats)
- Audit log viewer

## Crate Responsibilities

| Crate | New Responsibilities |
|-------|---------------------|
| `ferrosa-schema` | `VirtualTable` trait, `VirtualTableRegistry`, `system_observability` keyspace + table definitions |
| `ferrosa-storage` | Implements `storage_stats` virtual table |
| `ferrosa-cql` | `SUBSCRIBE`/`UNSUBSCRIBE` parsing, streaming frame type, query executor calls virtual table `read()`, implements `connections` and `active_queries` tables |
| `ferrosa-net` | `SubscriptionManager`, internode metrics collection protocol, Prometheus `/metrics` endpoint, web interface (HTTPS + embedded JS), demand-driven peer coordination |
| `ferrosa-cluster` | Implements `cluster_peers`, `cluster_topology`, `host_metrics` virtual tables |
| `ferrosa-graph` | Graph executor recognizes virtual table sources, `SUBSCRIBE`/`UNSUBSCRIBE` parsing for graph queries |
| `ferrosa-ctl` | **New crate** — CLI binary with subcommands + `ratatui` TUI monitor mode |

## Dependency Flow

```
ferrosa-ctl ──▶ ferrosa-cql (as client library)

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
