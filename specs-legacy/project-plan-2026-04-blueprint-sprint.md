---
type: plan
priority: P1
status: draft
created: 2026-04-20
updated: 2026-04-20
related:
  - specs/ARCHITECTURE.md
  - specs/dsm-analysis.md
  - specs/threat-model.md
  - specs/fmea.md
  - specs/hazards.md
  - specs/decisions/design-cql-role-auth-rollout.md
---

# Post-blueprint sprint plan — 2026-04-20

Draws from the full-system review completed on 2026-04-20 (matklad-style
`ARCHITECTURE.md`, refreshed `dsm-analysis.md`, `threat-model.md`,
`fmea.md`, `hazards.md`). Orders work by intersection of STRIDE severity
and FMEA RPN. Scope covers both `ferrosa` and `ferrosa-memory`; items
labeled (M) live in `ferrosa-memory`, all others in `ferrosa`.

## Critical path — finish inside one week

The threat model and FMEA converge on the same root cause: **every
client is a superuser on the podman cluster**. Four STRIDE-CRITICAL
threats (T-S1, T-S2, T-T1, T-E1) and three FMEA top-5 items (AUTH-1,
AUTH-3, SEC-1) all collapse into one remediation program.

| Rank | Item | Spec | Notes |
|-----:|------|------|-------|
| 1 | CQL role-auth rollout | `specs/todo/todo-enable-cql-role-auth-for-graph-table-isolation.md` + `specs/decisions/design-cql-role-auth-rollout.md` | Closes T-S1, T-S2, T-E1, T-E2, AUTH-3 (RPN 486). 5-sprint plan in the design doc; estimate ~10 calendar days. |
| 2 | Graph-table write bypass | `ferrosa-memory/specs/todo/bug-ferrosa-memory-bypasses-graph-api-for-writes.md` + sibling `todo-extend-ferrosa-memory-graph-client-with-cypher-writes.md` (M) | Closes T-T1, AUTH-1 (RPN 315). Must land **before** role-auth enforcement flip or ferrosa-memory hard-breaks. |
| 3 | Rotate MinIO credentials out of compose | new — file as `todo-rotate-minio-credentials.md` | Closes SEC-1 (RPN 560, highest in FMEA). Move to secrets volume; never commit. |
| 4 | Default-password bootstrap guard | folded into item 1 | Seed-role must force password change on first successful auth; alarm after 5 min if still default. |

## Parallel workstreams

### Workstream A — data-path hardening (in-flight)

- (done 2026-04-19) phantom-ID `format!("{}", i+1)` removed from
  `ferrosa-storage/src/store.rs:sstable_metadata`.
- (done 2026-04-19) SSTableWriter Gate A + Gate B (`writer.rs`
  `validate_clustering_shape` + `verify_output_readable`).
- (done 2026-04-19) jemalloc + debug-symbol rebuild — glibc arena
  retention eliminated.
- (done 2026-04-19) legacy `tool_usage_log` corruption quarantined
  from host + S3 + manifest.
- **P1 open**: `specs/todo/todo-startup-smoke-test-for-corrupt-sstables.md`
  — closes the "open succeeds but read fails" gap that the 2026-04-19
  manual quarantine had to paper over.
- **P2 open**: `specs/todo/bug-ddl-forward-handler-stale-leader-spam.md`
  — cosmetic ERROR spam during pair→cluster transitions.

### Workstream B — audit + observability (tied to the auth work)

- **T-R1** (threat model) — audit sink in production is `LogAuditSink`,
  not `SystemTableAuditSink`. Ring-buffer audit is lost on restart.
  File as `todo-wire-persistent-audit-sink.md`; required as part of
  item 1 above (denials must land in `system_auth.audit_log`).
- **OBS-OF1** — telemetry self-write feedback loop, already captured
  in `specs/observability-fmea.md`.
- Writer self-readback `FERROSA_WRITE_VERIFY=true` stays on by default;
  soak for 2 weeks, then decide whether to flip OFF once the class of
  bugs is believed dead.

### Workstream C — structural debt (background)

Items below stay as P2/P3; schedule after items 1–3 above land.

- **INF-1** (FMEA RPN 432) — single podman VM host is a correlated
  failure domain. Medium-term: migrate to separate VMs or reserve
  resources. File `todo-deploy-cluster-cross-vm.md`.
- **DSM extraction** — `ferrosa-cluster` is the fan-out giant (42k LOC,
  fan-in 7). DSM suggests extracting `accord` as its own crate and
  freezing `ferrosa-storage`'s public API. File
  `todo-extract-accord-crate.md`.
- **Split keyspace** — `agent_memory` keyspace today conflates
  app-owned and graph-owned tables. File
  `todo-split-keyspaces-application-vs-graph.md`; unblocks cleaner
  per-keyspace grants.

### Workstream D — correctness hazard sweep

From `specs/hazards.md`:

- **P0**: ban `unwrap()`/`expect()` on async task results — use
  clippy lint workspace-wide. Quick win; wire into
  `ferrosa/Cargo.toml` `[lints]` block.
- **P1**: audit unbounded channels (`tokio::sync::mpsc::unbounded_channel`
  callsites); replace with bounded where the producer can outpace the
  consumer.
- **P1**: `Arc<Mutex<...>>` hotspots — document invariants for any
  mutex held across `.await`.

## Sprint rhythm (proposed)

**Sprint 1 (2026-04-21 → 2026-04-28)** — items 1–3 critical path.
Deliverable: `FERROSA_AUTH_DISABLED` deleted from compose; three roles
seeded; ferrosa-memory writes land through Cypher. Flag: soak mode only
(`FERROSA_AUTH_WARN=true`), no enforcement yet.

**Sprint 2 (2026-04-28 → 2026-05-05)** — enforcement flip + persistent
audit sink (workstream B). Deliverable: denials produce
`Unauthorized` + audit row; rollback-lever tested.

**Sprint 3 (2026-05-05 → 2026-05-12)** — workstream A remaining +
correctness hazards (P0 lint). Deliverable: startup smoke-test landed;
`#[deny(clippy::unwrap_used)]` clean on all crates.

**Sprint 4+ (background)** — workstream C (structural). Owner-free
until Sprint 3 lands.

## Exit criteria for this blueprint sprint

- [ ] No STRIDE-CRITICAL threat remains open.
- [ ] No FMEA entry with RPN ≥ 300 remains open.
- [ ] `FERROSA_AUTH_DISABLED` is not set anywhere in shipped config.
- [ ] `ferrosa-memory` does not name graph-owned tables in any
      CQL statement.
- [ ] Writer self-readback is enabled in default build; decision on
      keeping it on or adding a feature flag recorded as an ADR.
- [ ] Audit log is persisted to `system_auth.audit_log` in
      production, not just a ring buffer.
