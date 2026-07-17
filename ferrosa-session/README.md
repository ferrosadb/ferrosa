# ferrosa-session

> The protocol-agnostic **engine-state bundle** (`SessionCore`) shared by every
> Ferrosa query front-end, so a new front-end can reuse the neutral state without
> depending on the ~54k-LOC `ferrosa-cql` crate.

## What this crate is

A tiny crate (one module, ~69 LoC) that owns a single struct, [`SessionCore`] —
the neutral subset of the former `ferrosa-cql` `SharedState`: the storage engine,
schema, node config, cluster-mode routing (`write_path` / `ddl_path` /
`cluster_state`), UDF executor, HA mode controller, the Accord HLC, and the peer
manager. It was carved out per blueprint decision **D10** so that
`ferrosa-postgres` (and any future front-end) can share the *identical* engine
state via composition, without pulling in `ferrosa-cql`'s protocol machinery.

## Extraction status (honest, current state)

**This is a partial extraction, in progress** — tracked on the board as "land
ferrosa-session extraction as standalone soaked PR".

- **Extracted here:** only the *neutral field bundle* — the `SessionCore` struct
  plus the single method `accord_enabled()`. That is the entire crate.
- **Still in `ferrosa-cql`:** all *behavior*. `SharedState` (which now wraps
  `Arc<SessionCore>` and `Deref`s to it) still lives in `ferrosa-cql/src/router.rs`,
  along with every request handler, the prepared-statement cache, the EVENT
  broadcast channel, CQL metrics, full-scan / index-usage trackers, and the
  client topology policy. None of the routing, write-path, or DDL *logic* moved —
  only the field that *holds* those handles.
- **No constructor:** there is no `SessionCore::new`. Every consumer builds the
  struct with a literal (12+ call sites across `ferrosa`, `ferrosa-cql`,
  `ferrosa-ctl`, and their tests). Field construction is therefore duplicated
  rather than centralized — see [specs/fmea.md](specs/fmea.md).
- **`accord_enabled()` has no in-tree callers yet.** It is documented as the
  precondition the Postgres front-end will check (D11, explicit `BEGIN…COMMIT`
  routed to Accord) but nothing calls it today.

## Public API

| Item | Kind | Notes |
|------|------|-------|
| `SessionCore` | struct (12 pub fields) | neutral engine state; built by literal at each call site |
| `SessionCore::accord_enabled(&self) -> bool` | method | `peer_manager.is_some() && accord_clock.is_some()`; no callers yet |

### `SessionCore` fields

| Field | Type | Purpose |
|-------|------|---------|
| `engine` | `Arc<StorageEngine>` | local read/write path (memtable, SSTable, S3) |
| `schema` | `Arc<Schema>` | keyspace/table/role metadata + authorization |
| `node_config` | `Arc<NodeConfig>` | node identity, replication policy, cluster settings |
| `cluster_state` | `Arc<ArcSwap<ClusterStateHolder>>` | Standalone / Pair / Cluster topology |
| `write_path` | `Arc<ArcSwap<WritePath>>` | direct vs Accord-coordinated write routing |
| `ddl_path` | `Arc<ArcSwap<DdlPath>>` | direct / pair / Raft DDL replication routing |
| `udf_executor` | `Arc<UdfExecutor>` | WASM UDF execution |
| `mode_controller` | `Arc<ModeController>` | pair-mode HA readiness |
| `auth_warn` | `bool` | soak mode: log+allow permission failures instead of deny |
| `peer_manager` | `Option<Arc<PeerManager>>` | Accord fan-out; `None` standalone/unit-test |
| `accord_clock` | `Option<Arc<HybridLogicalClock>>` | monotone txn timestamps; `None` when no peer manager |
| `accord_state` | `AccordStateSlot` | shared slot the controller fills at formation with this node's live `AccordState`; the transaction committer reads it so the coordinator votes its own PreAccept locally (a node is never in its own peer map). Empty in standalone/tests |

The three `ArcSwap`-wrapped paths are hot-swappable so cluster-mode transitions
(Standalone → Pair → Cluster) can re-point routing without rebuilding the struct.

## How it's consumed

`ferrosa-cql`'s `SharedState` holds an `Arc<SessionCore>` and implements
`Deref<Target = SessionCore>`, so existing CQL handlers reach neutral fields
(`self.engine`, `self.schema`, …) unchanged. CQL-specific state is composed
*alongside* the core, not mixed into it.

## Dependencies

**Calls** (ferrosa crates this depends on):

- **`ferrosa-cluster`** — `ClusterStateHolder`, `WritePath`, `DdlPath`, `ModeController`.
- **`ferrosa-common`** — `accord::HybridLogicalClock`.
- **`ferrosa-net`** — `peer::PeerManager`.
- **`ferrosa-schema`** — `Schema`, `NodeConfig`.
- **`ferrosa-storage`** — `StorageEngine`.
- **`ferrosa-udf`** — `UdfExecutor`.

External: `arc-swap`.

**Called by** (crates that depend on this):

- **`ferrosa`** — builds the `SessionCore` literal in `main.rs` when wiring up the node.
- **`ferrosa-cql`** — wraps it in `SharedState` and `Deref`s to it.
- **`ferrosa-ctl`** — constructs it in integration tests.

## Tests

**Zero in-crate tests.** `ferrosa-session` has no `#[test]`/`mod tests` of its own —
the struct is exercised only transitively through `ferrosa-cql` / `ferrosa` /
`ferrosa-ctl` tests that construct it. This is a tracked gap; see
[specs/fmea.md](specs/fmea.md) and [specs/roadmap.md](specs/roadmap.md).

## Specs

- [Architecture overview](specs/overview.md) — boundary, field map, data flow
- [FMEA / known issues](specs/fmea.md) — extraction-completeness and coupling gaps
- [Roadmap](specs/roadmap.md) — Now / Next / Later
