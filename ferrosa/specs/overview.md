---
crate: ferrosa
status: implemented
last_updated: 2026-06-19
executive_summary: >
  The top-level binary and composition root. It constructs one StorageEngine,
  one Schema, one ModeController, one PeerManager, and one CdcBus, then wires
  every query front-end (CQL, Postgres, Arrow Flight, graph HTTP+Bolt, SPARQL)
  and the web console onto them in a fixed startup order. It owns no domain
  logic of its own — its responsibility is wiring, configuration resolution,
  port binding, and process lifecycle. Nothing depends on this crate.
---

# ferrosa — Architecture Overview

## Purpose & boundary

`ferrosa` is the `[[bin]]` target that becomes the running database. Its
boundary is deliberately thin: it imports the subsystem crates and **composes**
them. The hard correctness work — storage, schema, consensus, transport, query
execution — lives in those crates. This crate decides *what order* they come up
in, *which* shared instances they share, *what config* they read, and *how* they
drain on shutdown.

It is the single place in the workspace where all front-ends meet, so it is also
the only natural home for cross-cutting wiring: the `CdcBus` that feeds both CQL
`SUBSCRIBE` and Arrow Flight, the `SessionCore` shared by the CQL router and the
Flight service, and the per-subsystem tokio runtimes that keep Raft heartbeats
off the bulk-write path.

## Module map

| Module | Responsibility |
|--------|----------------|
| `main.rs` (~2.8k LoC) | The full startup sequence + the maintenance loop + graceful shutdown; pure config/host_id helpers are unit-tested here |
| `runtime.rs` | `RuntimeManager` — dedicated `raft` / `data` / `cql` / `background` tokio runtimes |
| `repair_wiring.rs` | `BinaryRepairContext`, `build_repair_executor` — binds self-heal + anti-entropy repair to the live ring/peer manager |
| `cql_broadcast.rs` | `parse_cql_broadcast` — externally-advertised CQL address for `system.local` |
| `web/` | Axum console: `api`, `auth`, `debug`, `observability`, `readiness`, `snapshots`, `static_files`, `ws` |

## Composition model

```
                 ┌──────────── Arc<StorageEngine> ────────────┐
 CdcBus ───────► │  (write-behind cache + S3 + commit log)    │
                 └───────────────┬────────────────────────────┘
                                 │ shared Arc
   Arc<Schema> ──────────────────┼───────────────► virtual tables, DDL
   Arc<ModeController> ──────────┤
   Arc<PeerManager> ─────────────┤
                                 ▼
                       ferrosa-session::SessionCore
                                 │
        ┌────────────┬───────────┼───────────┬────────────┐
       CQL        Postgres     Flight       Graph        SPARQL
      :9042        :5432       :8815      :7474/:7687     :8080
```

A single `Arc` of each core service is cloned into every consumer. There is
exactly one engine, one schema, one cluster brain, one peer map, and one HLC per
process — which is what makes cross-front-end reads consistent and DDL
replication uniform.

## Key invariants

1. **Strict startup ordering.** The engine must exist before schema restore;
   schema (system tables + user tables + indexes/UDTs/UDFs) must be registered
   before commit-log mutations are replayed; the `PeerManager` must exist before
   the self-heal cluster view (which reads live membership) and before
   `SessionCore` (so LWT routes over real peers). Re-ordering silently breaks
   recovery (e.g. "table not registered" during Raft replay).
2. **Single CdcBus, attached pre-listener.** The change bus is attached to the
   commit log in step 4, before any front-end binds, so no early write is missed
   by `SUBSCRIBE`/Flight consumers.
3. **Config precedence is TOML → env → default, everywhere.** All resolvers
   (`config_val`, `apply_internode_toml_overrides`, `resolve_graph_enabled`,
   `resolve_auth_enabled_toml`) honor this order; BUG-006 was TOML being ignored
   for internode/graph/auth.
4. **Auth is driven from one switch.** `FERROSA_AUTH_ENABLED` (or
   `[cql].auth_enabled`) is the source of truth; `resolve_auth_disabled` derives
   the CQL/web/graph/Bolt `auth_disabled` flag. `FERROSA_AUTH_DISABLED` is a
   deprecated override that logs a warning.
5. **Graceful shutdown flushes before exit.** `SIGTERM` (container stop) and
   `SIGINT` both trigger a 30 s drain that flushes memtables and persists schema
   — skipping it is 100 % data loss on restart.

## Configuration resolution

`config_val` / `config_val_opt` implement the TOML→env→default ladder and are
the most-tested code here. `host_id` resolution is a pure state machine
(`classify_host_id_state` → `HostIdResolution`) so a corrupt/empty/missing id
file produces an actionable log line instead of a silent identity change
(BUG-008). Internode, graph-enabled, and auth-enabled each have a dedicated
TOML-aware resolver (BUG-006).

The binary resolves `[web].bind` and `[graph].bind` through that same ladder.
Their built-in defaults are loopback-only (`127.0.0.1:9090` and
`127.0.0.1:7474`). Bolt derives its host from the resolved graph HTTP bind and
uses `[graph].bolt_port` / `FERROSA_BOLT_PORT` for its port, defaulting to
`127.0.0.1:7687` when neither source is configured.

## Lifecycle

Boot wires everything (see [data-flow.md](data-flow.md)); a background
maintenance loop on the `background` runtime handles periodic + urgent flush,
compaction polling, commit-log GC, and schema persistence (local + S3); shutdown
drains cluster tasks → internode → memtables → schema in a 30 s timeout window.

## Position in the dependency graph

**Root.** Depends on `ferrosa-cdc`, `ferrosa-cluster`, `ferrosa-common`,
`ferrosa-cql`, `ferrosa-flight` (feature `flight`), `ferrosa-graph`,
`ferrosa-net`, `ferrosa-postgres`, `ferrosa-schema`, `ferrosa-session`,
`ferrosa-sparql`, `ferrosa-storage`, `ferrosa-udf`. Depended on by nothing — it
is the binary. See the [root crate index](../../specs/crates.md) for the full
graph.
