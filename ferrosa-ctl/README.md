# ferrosa-ctl

> The operator CLI + ratatui TUI for a Ferrosa node: live observability over
> CQL, cluster lifecycle over the web admin API, and offline SSTable / Raft-log
> recovery straight against the data directory.

## What this crate is

`ferrosa-ctl` is the **single binary** (`[[bin]] name = "ferrosa-ctl"`) operators
use to inspect and administer a Ferrosa node. It is a leaf in the dependency
graph — nothing depends on it. It talks to a node through three distinct
channels, and several subcommands operate with **no live node at all** (offline
recovery against a stopped node's files):

1. **CQL native protocol (`--host`, default `127.0.0.1:9042`)** — read-only
   `SELECT`s against the `system_observability.*` and `system.peers` virtual
   tables, plus `ALTER ROLE` for `auth set-password`.
2. **Web admin HTTP API (`--web-port`, default `9090`)** — cluster membership,
   ring, repair, snapshot/restore, learner lifecycle, leader transfer.
3. **Offline filesystem / sled / S3** — SSTable corruption triage & salvage and
   Raft-log inspect/truncate/reset, run against a **stopped** node's data dir.

## What's implemented

### Observability (CQL, read-only)

| Subcommand | What it does |
|------------|--------------|
| `status` | Node up + active-connection count (`system_observability.connections`). |
| `connections [--sort <col>]` | Active CQL connections; optional client-side sort. |
| `queries [--long-running]` | Running queries (`active_queries`); `--long-running` sorts by `elapsed_ms` desc. |
| `storage` | Storage engine stats (`storage_stats`). |
| `topology` | Ring topology from `system.peers`. |
| `peers` | Peer node list from `system.peers`. |
| `monitor [--panel <name>]` | ratatui TUI dashboard (Connections / Queries / Storage), 2 s refresh. |

### Auth (CQL)

- `auth set-password [--user --admin-user --admin-password-env --ssl --force --no-confirm]`
  — reads the new password with no echo, connects as the admin role (env →
  seed-default → prompt fallback), issues `ALTER ROLE "<u>" WITH PASSWORD = '…'`.
  Identifier/literal escaping is applied; the **server** hashes the cleartext.
  `--ssl` is rejected (TLS not yet supported by `CqlClient`).

### Cluster lifecycle (web admin HTTP API, port 9090)

| Subcommand | Endpoint |
|------------|----------|
| `add-node <host_id>` | `POST /api/cluster/add-node` |
| `decommission [host_id]` | `POST /api/cluster/decommission` |
| `ring` | `GET /api/cluster/ring` |
| `rebalance` | `POST /api/cluster/rebalance` |
| `repair --keyspace --table [--rf 3]` | `POST /api/cluster/repair?…` |
| `raft transfer-leader --to <host_id>` | `POST /api/cluster/raft/transfer-leader` |
| `cluster add-learner <host_id> <addr> [--owns-tokens]` | `POST /api/cluster/add-learner` |
| `cluster promote-to-voter <host_id>` | `POST /api/cluster/promote-to-voter` |
| `cluster demote-to-learner <host_id>` | `POST /api/cluster/demote-to-learner` |
| `snapshot create\|list\|delete` | `…/api/snapshots[…]` |
| `restore <name> [--point-in-time --force]` | `POST /api/restore` |

Several of these (`raft transfer-leader`, the three `cluster` learner-lifecycle
commands, and `snapshot`/`restore`) call endpoints that are **not yet served by
the node**; they surface the upstream `404`/`501` with a spec pointer rather
than faking success.

### Offline Raft recovery (sled, node must be stopped)

- `raft reset --data-dir [--dry-run]` — atomically replaces the raft dir so the
  node rejoins as a fresh learner (does **not** open sled, so no lock contention).
- `raft log-inspect --data-dir [--json]` — decodes every persisted log entry,
  reports readable/unreadable split + vote/committed/purge markers.
- `raft log-truncate --data-dir --from <idx> [--dry-run] [--yes]` — destructive;
  drops entries `>= --from`, clamps committed. `--from` is mandatory; mutation
  requires `--yes`.
- `cluster bootstrap-dc --dc --seeds` — Sprint-6 scaffolding: derives and prints
  the per-DC `RaftGroupId`; the live wire-up is deferred (Sprint 7).

### Offline SSTable triage & recovery (filesystem / live CQL / S3)

All verdicts delegate to the engine's own `StorageEngine::smoke_test_generation`
so ctl can never disagree with what the node decides at boot.

- `sstable scan <dir> [--include-quarantine --json]` — classify every generation OK/CORRUPT.
- `sstable inspect <dir> <gen> [--json]` — deep per-generation report + corruption-class hint.
- `sstable quarantine <dir> [--apply --json]` — move CORRUPT live gens into `quarantine/` (dry-run default; node stopped to apply; files moved, never deleted).
- `sstable salvage <dir> [--include-quarantine --json]` — measure recoverable-row yield (pure read).
- `sstable reingest <dir> [--user --password-env --apply --include-quarantine --limit]` — salvage CORRUPT gens and re-insert through the **live** write path (`--host`), preserving original timestamps; dry-run default.
- `sstable s3-clean <dir> [--apply]` — delete CORRUPT generations' objects from the object store so a cold restart can't re-download them; dry-run default.

## Dependencies

**Calls** (ferrosa crates this depends on): ferrosa-cluster, ferrosa-common,
ferrosa-cql, ferrosa-schema, ferrosa-session, ferrosa-sstable, ferrosa-storage,
ferrosa-udf.

Runtime dependencies are `ferrosa-cluster`, `ferrosa-common`, `ferrosa-cql`,
`ferrosa-sstable`, `ferrosa-storage`. `ferrosa-schema`, `ferrosa-session`, and
`ferrosa-udf` are **dev-dependencies** used by the integration test harness.
External: `clap`, `tokio`, `reqwest` (rustls), `ratatui`, `crossterm`,
`tabled`, `object_store` (aws), `rpassword`, `sled`, `serde`/`serde_json`.

**Called by**: NONE — it is a CLI binary, the leaf of the dependency graph.

## Tests

178 test functions across the crate: argument parsing (`src/main.rs`, 46),
command formatting / URL+body construction / offline raft (`src/commands.rs`,
70), TUI state machine & rendering helpers (`src/tui.rs`, 12), password flow
(`src/auth.rs`, 17), SSTable triage (`src/commands/sstable.rs`, 22), and a CQL
virtual-table integration suite (`tests/integration.rs`, 11).

## Specs

- [Architecture overview](specs/overview.md) — channels, module map, data flow
- [FMEA / known issues](specs/fmea.md) — failure modes + real gaps
- [Roadmap](specs/roadmap.md) — Now / Next / Later
