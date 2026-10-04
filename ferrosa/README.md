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
| `maintenance.rs` | The storage maintenance loop (periodic + urgent flush, compaction polling, commit-log GC, schema persist, S3 sync), run under `supervisor` |
| `supervisor.rs` | OTP-style supervision of the maintenance loop and its flusher: restart within an intensity (`FERROSA_SUPERVISOR_MAX_RESTARTS`, default 3, per `FERROSA_SUPERVISOR_PERIOD_SECS`, default 3600), stall detection (`FERROSA_FLUSH_STALL_DEADLINE_SECS`, default 300), `/readyz` + `ferrosa_supervised_task_*` metrics, and escalation (commit-log sync, then abort) |
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
| Internode RPC | `0.0.0.0:17000` | `ferrosa-net` | bind: `FERROSA_INTERNODE_BIND` / `[internode].bind`; advertised endpoint: `FERROSA_INTERNODE_BROADCAST` / `[internode].broadcast`. The exact advertised host/port is preserved for peer handshakes, including same-host clusters whose nodes use different ports. Note **17000**, not Cassandra's 7000 (BUG-001: 7000 collides with macOS ControlCenter). |
| CQL native v5 | `127.0.0.1:9042` | `ferrosa-cql` | `FERROSA_CQL_BIND` / `[cql].bind` |
| Postgres wire | `127.0.0.1:5432` | `ferrosa-postgres` | `FERROSA_POSTGRES_BIND` / `[postgres].bind` — always started; query execution is a fail-loud stub until the relational engine lands |
| Arrow Flight (gRPC) | `127.0.0.1:8815` | `ferrosa-flight` | **`--features flight`** (in release `full` builds) + `FERROSA_FLIGHT_BIND` / `[flight].bind`; `[flight] enabled` (default on); per-RPC signed bearer tokens; TLS from `[flight] tls_cert/tls_key/require_tls` |
| Graph HTTP | `127.0.0.1:7474` | `ferrosa-graph` | only if graph enabled; `FERROSA_GRAPH_BIND` / `[graph].bind` |
| Bolt v5 | `127.0.0.1:7687` | `ferrosa-graph` | only if graph enabled; `FERROSA_BOLT_PORT` / `[graph].bolt_port`; uses the host resolved for Graph HTTP |
| SPARQL HTTP | `127.0.0.1:8080` | `ferrosa-sparql` | enabled by default; `FERROSA_SPARQL_BIND` / `[sparql].bind` |
| Web console + `/metrics` | `127.0.0.1:9090` | this crate (`web/`) | `FERROSA_WEB_BIND` / `[web].bind`. `/readyz` and `/health`: when `FERROSA_EXPECTED_CLUSTER_SIZE` is set they return 503 `waiting_for: declared_topology` until that topology is met (the same gate CQL uses). They also return 503 `waiting_for: listeners` while a background client listener (graph HTTP, Bolt, SPARQL, Postgres) has failed to bind or exited; `ferrosa_listener_up{listener="…"}` on `/metrics` is 1/0 for each. A bind failure there used to be one ERROR log line while the node kept probing ready. A vector index whose generations are being rebuilt keeps them 200 but adds `degraded_recall: [{table, index, generations}]` (ANN over it refuses, retryable, until rebuilt; ST-81). A secondary index that is not current also keeps them 200 and adds `stale_indexes: [{table, index, pending_generations, state, last_error}]` (`state` is `backfilling` or `failed`; reads through it are refused until its backfill completes, and the maintenance loop retries a failed one; `ferrosa_index_not_current` / `ferrosa_index_backfill_failed` on `/metrics`; storage ST-85). They return 503 `waiting_for: background_tasks` while a supervised task (`storage_flush`, `maintenance_loop`) is failing, stalled or restarting, naming the task and its last failure; `ferrosa_supervised_task_up{task="…"}` is 1/0, with `_failures_total{kind}` and `_restarts_total` |

### TLS and production mode (t_d5d122ba)

**One certificate for the whole node.** `[tls]` (env `FERROSA_TLS_*`) sets a
single certificate that every listener and internode use unless they set their
own, so a typical node needs only:

```toml
[tls]
cert    = "/etc/ferrosa/tls/node.crt"   # FERROSA_TLS_CERT
key     = "/etc/ferrosa/tls/node.key"   # FERROSA_TLS_KEY
ca      = "/etc/ferrosa/tls/ca.crt"     # FERROSA_TLS_CA (internode peer verification)
require = true                          # FERROSA_TLS_REQUIRE: every listener + internode refuse plaintext
```

That satisfies the production gate for every listener (Arrow Flight included)
and internode. The certificate needs the SANs clients use for each port
(internode verifies the peer IP). A section's own keys override `[tls]`: its
`tls_cert`/`tls_key` pair (taken as a pair, never mixed with the `[tls]` key),
and its `require_tls` (so `[sparql] require_tls = false` opts one listener out
— and production then refuses it by name).

Every client listener can also take TLS from its own section with the same three keys
(TOML wins; env fallback `FERROSA_<PREFIX>_TLS_CERT` / `_TLS_KEY` /
`_REQUIRE_TLS`): `[cql]`, `[postgres]`, `[graph]` (graph HTTP **and** Bolt),
`[sparql]`, `[web]`, `[flight]` (Arrow Flight, `flight` builds) — `tls_cert`,
`tls_key`, `require_tls`. Internode uses
`[internode] tls_cert/tls_key/tls_ca/require_tls` (env `FERROSA_INTERNODE_*`).
A non-boolean `require_tls` stops startup; every configured certificate is
loaded once before anything binds, so a bad path names its listener.
All TLS configs are built by `ferrosa_net::tls` with one crypto provider.

`FERROSA_MODE=production` refuses to start (exit 1, before any listener binds)
unless every **enabled** listener requires TLS and `[internode] require_tls =
true`. Per listener: CQL, PostgreSQL, graph HTTP, Bolt, SPARQL and the web
console have TLS and must require it; SPARQL and graph/Bolt are exempt only
when disabled (`[sparql] enabled = false`, `[graph] enabled = false` — the graph
HTTP port then serves only a fixed 503 stub, over TLS if a certificate is set).
Arrow Flight (t_58db6320) follows the same rule: an enabled Flight listener
must set `[flight] require_tls = true` with `[flight] tls_cert` / `tls_key`
(env `FERROSA_FLIGHT_TLS_CERT` / `_TLS_KEY` / `_REQUIRE_TLS`). It serves gRPC
over TLS with ALPN `h2`, terminated with tokio-rustls on a config from
`ferrosa_net::tls` (not tonic's own TLS), and advertises replica locations as
`grpc+tls://`. Release `full` builds no longer need `[flight] enabled = false`
to start in production; that key now only turns the port off. Mutual TLS is not
required yet (t_b6c820f4). With `[web] require_tls`, `/readyz` and `/metrics` are HTTPS too.


## Startup order (`main`)

The sequence is strict because later steps consume the handles produced earlier
(the engine before schema restore, the `PeerManager` before the self-heal
cluster view, the `SharedState` before the CQL/Flight servers). See
[specs/data-flow.md](specs/data-flow.md) for the full diagram.

0. **CLI meta flags** — `--version`/`-V` and `--help`/`-h` print one line (`ferrosa <semver>`) and exit *before* tracing or config, so the output is parseable rather than interleaved with startup logs. Any other argument falls through to normal startup, so existing wrappers that pass extra flags are unaffected. Previously these flags were ignored and the **daemon started**, which meant anything probing the binary for its version silently launched a database.
1. **Tracing** — non-blocking writer (`tracing-appender`); optional OTel layer when `FERROSA_TELEMETRY_ENABLED=true` (`--features otel`). Log lines carry ANSI colour only when stdout is a terminal (`FERROSA_LOG_ANSI=true|false` overrides; `NO_COLOR` turns it off): `tracing-subscriber` coloured by default even into a pipe, which put escape codes in container logs and broke anchored patterns in log stores.
2. **Config** — load `FERROSA_CONFIG` TOML (default `/etc/ferrosa/ferrosa.toml`); file values win over environment values, which win over built-in defaults.
3. **Schema preflight** — load the size-bounded, discriminated local `schema.json` before storage or any listener. Legacy arrays, corrupt/oversized documents, and unknown formats are quarantined and abort startup; they never become an empty registry.
4. **host_id** — load/generate/validate `{data_dir}/host_id` (`classify_host_id_state`: loaded / override / empty-regenerated / invalid-regenerated / generated-new — each path logs a breadcrumb, BUG-008).
5. **StorageEngine** — `open()` (replay commit log) if segments exist, else `new()`; probe S3 CAS; **attach `CdcBus`** (capacity 1024) to the commit log; register system tables; replay pending S3 uploads. Storage-only recovery metadata lives in `storage-schema.json`, never `schema.json`.
6. **Schema** — `Schema::new` (composes audit sinks); apply the preflighted local snapshot or use S3 bootstrap/fresh startup; replay pending commit-log mutations; restore roles before seeding only missing defaults; then reconstruct secondary indexes, UDTs, UDFs, and role permissions from the system tables.
7. **ModeController** — `ClusterConfig`/`NetConfig` (with TOML overrides, BUG-006); preserve `[internode].broadcast` as both the resolved local address and the raw peer-handshake advertisement; build the `HandlerRegistry` (ping, pair-catchup, mutation/truncate forward, three repair handlers); construct controller in standalone mode. Then `check_startup_jsonb` (T-300, D24): a node that will not stay standalone (seeds configured, or a former cluster member) and whose restored schema holds jsonb columns exits non-zero with the table names and D15a; there is no bypass.
8. **PeerManager** — wire as `ModeController`'s `PeerEventListener`; start the heartbeat loop; spawn the self-heal controller with a **live** peer-health probe.
9. **Production gate** — resolve every listener's TLS settings, load each configured certificate, and run `enforce_production_requirements` (auth, listener + internode TLS, default admin password); exit 1 on a blocking violation.
10. **Internode RPC** (`:17000`) — `RpcServer::start_and_get_addr`.
11. **CQL server** (`:9042`) — build `SharedState` (`SessionCore` + Accord HLC + prepared cache + observability trackers + virtual tables) and `start_background`.
12. **Arrow Flight** (`:8815`, `flight` feature, after the listener-status registry so a bind or TLS failure shows in `/readyz` as `flight`) — signing key from `FERROSA_FLIGHT_SIGNING_KEY` (ephemeral if unset — warns); `FERROSA_FLIGHT_TOKEN_TTL_SECS` must be a positive integer (a typo is fatal, it used to fall back to 3600 silently); TLS when `[flight] tls_cert/tls_key` are set.
13. **Web console** (`:9090`).
14. **Automatic repair** — self-heal controller with verified-replica cluster view + quarantine→refill trigger; periodic anti-entropy scheduler.
15. **Graph** (HTTP `:7474` + Bolt `:7687`) if enabled; **Postgres** (`:5432`); **SPARQL** (`:8080`) if enabled.
16. **Seeds** — background connect to `FERROSA_SEED` peers with exponential backoff.
17. **Maintenance loop** (`maintenance.rs`) — periodic + urgent flush, compaction polling, commit-log GC, schema persist (local + S3), supervised by `supervisor.rs` (t_7681b32b):
    - Each flush runs on its own thread behind `catch_unwind`. A panic is that attempt's crash; the next tick restarts it. A flush that outlives the stall deadline is reported as a stall once per deadline and no longer blocks the loop's other arms; no second flush starts until it reports.
    - The loop itself is restarted if it panics or returns. The last persisted schema version survives a restart, so a pending persist is not skipped.
    - While a flush is failing, stalled or restarting, `/readyz` answers 503 and `ferrosa_supervised_task_up{task="storage_flush"}` is 0; ERROR lines mark the edges.
    - Panics and stalls count toward the restart intensity; a flush that returns an error does not (it is impaired, not dead). Past the intensity the process syncs the commit log (10 s bound), prints a `FATAL: supervised task …` line naming the task and last failure, and aborts (SIGABRT, which launchd `KeepAlive { Crashed }` restarts). Not-ready alone was rejected: nothing gates CQL writes on storage health, so the node would keep acknowledging writes it could never flush.
    - A hang the loop cannot report (an await that never completes, a blocked GC) is caught by `MaintenanceWatchdog`: the loop beats a heartbeat each iteration and the watchdog, on its own thread, counts a stall per `FERROSA_MAINTENANCE_STALL_DEADLINE_SECS` (600) of silence and escalates past the intensity. The flush restart window is shared across loop restarts.
    - The commit log's fsync thread (`commitlog_sync`, P0-6 t_88479cda) is a third child: `CommitLogSyncSupervisor` samples its health every 100 ms, restarts it when it died, counts a stall per elapsed `FERROSA_COMMITLOG_SYNC_STALL_DEADLINE_MS` (2000), and escalates past the intensity. The commit log refuses writes by itself while the thread is dead, failing or stalled (storage FMEA ST-71); the supervisor restores service and reports it. A stall names its cause (ST-86): the log line carries `cause=` and `/metrics` counts `ferrosa_commitlog_sync_stalls_total{cause}`, so a thread that never ran (`no_attempt_issued`) is distinguishable from a slow device (`device_slow`).
18. **Shutdown** — `SIGINT`/`SIGTERM` → 30 s graceful drain: stop cluster tasks → drain internode → flush memtables → persist schema (local + S3).

## How the subsystems compose

- **One engine, one schema, one cluster brain.** A single `Arc<StorageEngine>`,
  `Arc<Schema>`, `Arc<ModeController>`, and `Arc<PeerManager>` are shared by
  every front-end and background task — so a row written over Postgres is read
  over CQL, and a DDL over CQL replicates over the same `DdlPath` the graph
  engine uses.
- **CdcBus injection.** The `ferrosa-cdc` bus is attached to the engine's commit
  log at step 5, *before* any front-end starts, so live CQL `SUBSCRIBE` and the
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
flat under tight cgroups; override at process startup with `_RJEM_MALLOC_CONF`.

## Key environment variables

| Variable | Purpose |
|----------|---------|
| `FERROSA_CONFIG` | TOML config path (default `/etc/ferrosa/ferrosa.toml`) |
| `FERROSA_DATA_DIR` | data directory (default `/var/lib/ferrosa`) — holds `host_id`, registry-owned `schema.json`, storage-only `storage-schema.json`, commit log, hints |
| `FERROSA_HOST_ID` | authoritative host-id override (wins over disk) |
| `FERROSA_INTERNODE_BROADCAST` | host/port advertised during internode handshakes; file equivalent `[internode].broadcast` is authoritative and preserves the exact endpoint |
| `FERROSA_AUTH_ENABLED` | single source of truth for CQL role auth; `[cql].auth_enabled` is authoritative when configured |
| `FERROSA_AUTH_DISABLED` | **deprecated** direct override — honored with a warning |
| `FERROSA_SEED` | comma-separated seed peers (`host:port`, DNS-resolved) |
| `FERROSA_GRAPH_ENABLED` / `FERROSA_SPARQL_ENABLED` | enable graph (HTTP+Bolt) / SPARQL front-ends |
| `FERROSA_TLS_CERT` / `_KEY` / `_CA` / `FERROSA_TLS_REQUIRE` | node-wide certificate for every listener + internode (`[tls]`), overridden per section |
| `FERROSA_FLIGHT_BIND` / `FERROSA_FLIGHT_ENABLED` / `FERROSA_FLIGHT_SIGNING_KEY` / `FERROSA_FLIGHT_TOKEN_TTL_SECS` / `FERROSA_FLIGHT_PORT` / `FERROSA_FLIGHT_TLS_CERT` / `_TLS_KEY` / `_REQUIRE_TLS` | Flight endpoint (when `flight` feature is built); `FERROSA_FLIGHT_PORT` is the port advertised for remote replicas, default the node's own `[flight] bind` port |
| `FERROSA_TELEMETRY_ENABLED` | install the OTel tracing layer (when `otel` feature is built) |
| `FERROSA_SELFHEAL_ENABLED` | self-heal quarantine controller (default on) |
| `FERROSA_FLUSH_INTERVAL_SECS`, `FERROSA_URGENT_*` | maintenance-loop cadences |

`[jsonb]` sets the jsonb ingest limits (TOML over `FERROSA_JSONB_*` env). Startup
validates them against their hard ceilings and the commit-log segment size and
exits with a FATAL message naming the key on a violation. The resolved limits are
passed to the Postgres front end (`QueryContext::jsonb_limits`, T-161a) and gate
jsonb INSERT/UPDATE input there.

See `ferrosa.example.toml` for the file form (`[cql] [udf] [jsonb] [internode]
[storage] [s3] [graph] [web]`).

## Dependencies

**Calls** (subsystem crates this composes — verbatim):
`ferrosa-cdc`, `ferrosa-cluster`, `ferrosa-common`, `ferrosa-cql`,
`ferrosa-flight`, `ferrosa-graph`, `ferrosa-net`, `ferrosa-postgres`,
`ferrosa-schema`, `ferrosa-session`, `ferrosa-sparql`, `ferrosa-storage`,
`ferrosa-udf`, `ferrosa-jsonb`.

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

### Pump wiring acceptance (T-045)

The CQL wiring acceptance test writes compressed and plain tables through a real client/server, includes a row larger than a pump segment, flushes and compacts, tears down the first runtime, and verifies all rows through a reopened engine and fresh server.

### Compaction operator API

`POST /api/compaction/stop` uses the existing authenticated admin/operator routes.
JSON `{}` selects current tasks on this node; `{"keyspace":"ks","table":"t"}`
selects one registered table. Partial/empty scope, unknown fields, malformed JSON
and query parameters return 400; unknown tables return 404; bodies above 4 KiB
return 413. HTTP 202 returns `status: cancellation_requested`, `node_id`, echoed
`keyspace`/`table`, `matched_tasks` and `already_cancelled_tasks`. This is a request
acknowledgement: committed replacements continue, and future scheduling remains
active. `ferrosa-ctl compaction stop` calls this endpoint.

### Ring health (`GET /api/cluster/ring`)

Alongside `nodes`, the ring endpoint reports membership health so a degraded
ring is never silent:

- `ring_healthy: bool` — false when any member is not `NodeState::Normal`.
- `non_normal_members: [{node_id, address, state}]` — the reason it is false.
- `data_scatter_risk: [node_id]` — members that own tokens yet are excluded
  from `TokenRing::replicas()` because of their state, so their token ranges
  are being served by other nodes.

This exists because a node stuck in `Joining` looked healthy: it served CQL and
reported peers, while `replicas()` skipped it. On a 3-node cluster that produced
a reproducibly short paged scan on the stuck node and `ferrosa-ctl repair` with
zero owned ranges. The controller now also resumes the Promote phase on the
recovered-topology path (it previously skipped promotion on every restart, so a
mid-join node stayed `Joining` forever). Invariants and boundaries are pinned in
`ferrosa-cluster/tests/joining_node_health.rs`.
