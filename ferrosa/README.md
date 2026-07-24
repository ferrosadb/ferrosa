# ferrosa

> The top-level binary and **composition root**: it constructs every subsystem
> crate, wires them together (one `StorageEngine`, one `Schema`, one
> `ModeController`, one `PeerManager`, one `CdcBus`), and starts every network
> listener. Nothing depends on this crate — it is where the platform comes alive.

## What this crate is

`ferrosa` is the `[[bin]]` that becomes the running database. It owns **no
domain logic of its own** — it imports the engine (`ferrosa-storage`), the
metadata layer (`ferrosa-schema`), the cluster control plane
(`ferrosa-cluster`), the internode transport (`ferrosa-net`), the change-data
bus (`ferrosa-cdc`), the session core (`ferrosa-session`), and every query
front-end (`ferrosa-cql`, `ferrosa-postgres`, `ferrosa-graph`, `ferrosa-sparql`,
`ferrosa-flight`, plus `ferrosa-udf`), then composes them in a fixed startup
order in `src/main.rs`. Its job is **wiring, ordering, configuration resolution,
and lifecycle** — not re-implementing any subsystem.

It is the only crate in the workspace that depends on all the front-ends at once.

## Process layout (`src/`)

| Module | Responsibility |
|--------|----------------|
| `main.rs` (~2.8k LoC) | The whole startup sequence: config → host_id → storage → schema → cluster → listeners → maintenance loop → graceful shutdown |
| `runtime.rs` | `RuntimeManager` — dedicated tokio runtimes (raft, data, cql, background) so subsystems don't contend on one shared pool |
| `repair_wiring.rs` | `BinaryRepairContext` / `build_repair_executor` — binds the self-heal + anti-entropy repair scheduler to the live ring |
| `cql_broadcast.rs` | `parse_cql_broadcast` — resolves the externally-advertised CQL address/port for `system.local` |
| `web/` | Axum observability console + cluster REST API + auth middleware + readiness probe + WebSocket + embedded UI + PITR snapshot/restore endpoints |

The `web/` subtree is the only HTTP surface that lives *in this crate*; all other
listeners are owned by their subsystem crates and merely started here.

## Listeners & ports

Ports are resolved as **TOML (`/etc/ferrosa/ferrosa.toml`) → env var → built-in
default**. The config file is authoritative when it sets a value. Defaults below
apply when neither source sets the listener.

| Listener | Default bind | Owning crate | Enable / config |
|----------|--------------|--------------|-----------------|
| Internode RPC | `0.0.0.0:17000` | `ferrosa-net` | `FERROSA_INTERNODE_BIND` / `[internode].bind` — note **17000**, not Cassandra's 7000 (BUG-001: 7000 collides with macOS ControlCenter) |
| CQL native v5 | `127.0.0.1:9042` | `ferrosa-cql` | `FERROSA_CQL_BIND` / `[cql].bind` |
| Postgres wire | `127.0.0.1:5432` | `ferrosa-postgres` | `FERROSA_POSTGRES_BIND` / `[postgres].bind` — always started; query execution is a fail-loud stub until the relational engine lands |
| Arrow Flight (gRPC) | `127.0.0.1:8815` | `ferrosa-flight` | **`--features flight`** + `FERROSA_FLIGHT_BIND` / `[flight].bind`; per-RPC signed bearer tokens |
| Graph HTTP | `127.0.0.1:7474` | `ferrosa-graph` | only if graph enabled; `FERROSA_GRAPH_BIND` / `[graph].bind` |
| Bolt v5 | `127.0.0.1:7687` | `ferrosa-graph` | only if graph enabled; `FERROSA_BOLT_PORT` / `[graph].bolt_port`; uses the host resolved for Graph HTTP |
| SPARQL HTTP | `127.0.0.1:8080` | `ferrosa-sparql` | enabled by default; `FERROSA_SPARQL_BIND` / `[sparql].bind` |
| Web console + `/metrics` | `127.0.0.1:9090` | this crate (`web/`) | `FERROSA_WEB_BIND` / `[web].bind` |

## Startup order (`main`)

The sequence is strict because later steps consume the handles produced earlier
(the engine before schema restore, the `PeerManager` before the self-heal
cluster view, the `SharedState` before the CQL/Flight servers). See
[specs/data-flow.md](specs/data-flow.md) for the full diagram.

1. **Tracing** — non-blocking writer (`tracing-appender`); optional OTel layer when `FERROSA_TELEMETRY_ENABLED=true` (`--features otel`).
2. **Config** — load `FERROSA_CONFIG` TOML (default `/etc/ferrosa/ferrosa.toml`); file values win over environment values, which win over built-in defaults.
3. **host_id** — load/generate/validate `{data_dir}/host_id` (`classify_host_id_state`: loaded / override / empty-regenerated / invalid-regenerated / generated-new — each path logs a breadcrumb, BUG-008).
4. **StorageEngine** — `open()` (replay commit log) if segments exist, else `new()`; probe S3 CAS; **attach `CdcBus`** (capacity 1024) to the commit log; register system tables; replay pending S3 uploads.
5. **Schema** — `Schema::new` (composes audit sinks); seed default roles if auth enabled; restore schema from local `schema.json` → S3 bootstrap → fresh; re-register secondary indexes, UDTs, UDFs, role permissions from `system_schema.*`; replay pending commit-log mutations.
6. **ModeController** — `ClusterConfig`/`NetConfig` (with TOML overrides, BUG-006); build the `HandlerRegistry` (ping, pair-catchup, mutation/truncate forward, three repair handlers); construct controller in standalone mode.
7. **PeerManager** — wire as `ModeController`'s `PeerEventListener`; start the heartbeat loop; spawn the self-heal controller with a **live** peer-health probe.
8. **Internode RPC** (`:17000`) — `RpcServer::start_and_get_addr`.
9. **CQL server** (`:9042`) — build `SharedState` (`SessionCore` + Accord HLC + prepared cache + observability trackers + virtual tables) and `start_background`.
10. **Arrow Flight** (`:8815`, `flight` feature) — signing key from `FERROSA_FLIGHT_SIGNING_KEY` (ephemeral if unset — warns).
11. **Web console** (`:9090`).
12. **Automatic repair** — self-heal controller with verified-replica cluster view + quarantine→refill trigger; periodic anti-entropy scheduler.
13. **Graph** (HTTP `:7474` + Bolt `:7687`) if enabled; **Postgres** (`:5432`); **SPARQL** (`:8080`) if enabled.
14. **Seeds** — background connect to `FERROSA_SEED` peers with exponential backoff.
15. **Maintenance loop** — periodic + urgent flush, compaction polling, commit-log GC, schema persist (local + S3).
16. **Shutdown** — `SIGINT`/`SIGTERM` → 30 s graceful drain: stop cluster tasks → drain internode → flush memtables → persist schema (local + S3).

## How the subsystems compose

- **One engine, one schema, one cluster brain.** A single `Arc<StorageEngine>`,
  `Arc<Schema>`, `Arc<ModeController>`, and `Arc<PeerManager>` are shared by
  every front-end and background task — so a row written over Postgres is read
  over CQL, and a DDL over CQL replicates over the same `DdlPath` the graph
  engine uses.
- **CdcBus injection.** The `ferrosa-cdc` bus is attached to the engine's commit
  log at step 4, *before* any front-end starts, so live CQL `SUBSCRIBE` and the
  Arrow Flight stream observe the same change events.
- **SessionCore as the execution hub.** `ferrosa-session::SessionCore` bundles
  engine + schema + write/DDL paths + UDF executor + `ModeController` + peer
  manager + Accord HLC; the CQL router and the Flight service share one instance.
- **Per-subsystem runtimes.** `RuntimeManager` gives raft / data / cql /
  background their own tokio runtimes; the main runtime stays supervisor-only so
  bulk CQL writes can't starve Raft heartbeats.

## Feature flags

| Feature | Effect |
|---------|--------|
| `flight` *(off by default)* | Pulls in `ferrosa-flight` and starts the Arrow Flight gRPC endpoint on `:8815` |
| `otel` *(off by default)* | Pulls in OpenTelemetry/OTLP and installs the tracing export layer |

Allocator: on non-MSVC targets the binary links **jemalloc** with
`dirty_decay_ms:0,muzzy_decay_ms:0` (immediate page return to the OS — keeps RSS
flat under tight cgroups; override with `MALLOC_CONF`).

## Key environment variables

| Variable | Purpose |
|----------|---------|
| `FERROSA_CONFIG` | TOML config path (default `/etc/ferrosa/ferrosa.toml`) |
| `FERROSA_DATA_DIR` | data directory (default `/var/lib/ferrosa`) — holds `host_id`, `schema.json`, commit log, hints |
| `FERROSA_HOST_ID` | authoritative host-id override (wins over disk) |
| `FERROSA_AUTH_ENABLED` | single source of truth for CQL role auth; `[cql].auth_enabled` is authoritative when configured |
| `FERROSA_AUTH_DISABLED` | **deprecated** direct override — honored with a warning |
| `FERROSA_SEED` | comma-separated seed peers (`host:port`, DNS-resolved) |
| `FERROSA_GRAPH_ENABLED` / `FERROSA_SPARQL_ENABLED` | enable graph (HTTP+Bolt) / SPARQL front-ends |
| `FERROSA_FLIGHT_BIND` / `FERROSA_FLIGHT_SIGNING_KEY` / `FERROSA_FLIGHT_TOKEN_TTL_SECS` | Flight endpoint (when `flight` feature is built) |
| `FERROSA_TELEMETRY_ENABLED` | install the OTel tracing layer (when `otel` feature is built) |
| `FERROSA_SELFHEAL_ENABLED` | self-heal quarantine controller (default on) |
| `FERROSA_FLUSH_INTERVAL_SECS`, `FERROSA_URGENT_*` | maintenance-loop cadences |

See `ferrosa.example.toml` for the file form (`[cql] [internode] [storage] [s3]
[graph] [web]`).

## Dependencies

**Calls** (subsystem crates this composes — verbatim):
`ferrosa-cdc`, `ferrosa-cluster`, `ferrosa-common`, `ferrosa-cql`,
`ferrosa-flight`, `ferrosa-graph`, `ferrosa-net`, `ferrosa-postgres`,
`ferrosa-schema`, `ferrosa-session`, `ferrosa-sparql`, `ferrosa-storage`,
`ferrosa-udf`.

**Called by**: **NONE** — it is the top-level binary.

## Tests

~265 in-crate tests (`src/main.rs` + `web/*` + `cql_broadcast.rs` +
`repair_wiring.rs`). They cover the *pure* composition helpers — config
precedence (TOML → env → default), `host_id` classification, internode/graph/auth
TOML resolution, hinted-handoff dir resolution, schema local persist/load, web
config and auth bypass. The end-to-end boot path itself is exercised by the
cluster/integration suites, not from here. One in-code `TODO` remains
(`web/api.rs:475`). See [specs/fmea.md](specs/fmea.md).

## Specs

- [Architecture overview](specs/overview.md) — composition-root model
- [Data flow](specs/data-flow.md) — startup sequence wiring the crates + listeners
- [FMEA / known issues](specs/fmea.md) — startup-ordering, auth kill-switch, port-binding, partial-boot risks
- [Roadmap](specs/roadmap.md) — Now / Next / Later
