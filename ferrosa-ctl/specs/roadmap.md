---
crate: ferrosa-ctl
doc: roadmap
last_updated: 2026-06-19
---

# ferrosa-ctl — Roadmap

Sourced from in-code "not yet" markers, the FMEA gaps ([fmea.md](fmea.md)), and
the channel/dependency review.

## Now (highest value)

- **Wire the deferred web-API endpoints to the node, or label them clearly.**
  `raft transfer-leader`, `cluster add-learner|promote-to-voter|demote-to-learner`,
  and `snapshot`/`restore` POST to endpoints the node does not yet serve (they
  return `404`/`501` with spec pointers: sprint-03-openraft-patches W3.8/W3.9,
  sprint-08-learners-endurance W8.5). Until the server side lands, surface
  "not yet implemented" in `--help`, not just at runtime (FMEA CTL-2).
- **Surface a recovery-loss tally in `sstable reingest`.** Print salvaged vs.
  unrecoverable rows per generation so a partial recovery cannot read as
  complete (FMEA CTL-6). The counters (`reconstructed`, `inserted`, `failed`)
  already exist internally — promote them into the summary and into `--json`.

## Next

- **TLS for CQL commands.** `auth set-password --ssl` errors out because
  `CqlClient` has no TLS; every CQL command therefore sends credentials in
  cleartext off-localhost (FMEA CTL-5). Add TLS to `CqlClient` and honor `--ssl`.
- **Confirmation guard for `sstable s3-clean --apply`.** It's the only offline
  command that *deletes* (durable S3 objects) and depends on the dir being named
  exactly `<keyspace>.<table>`; add an interactive confirmation or `--yes` gate
  to match `raft log-truncate` (FMEA CTL-3).
- **Land `cluster bootstrap-dc` HTTP wire-up (Sprint 7).** Today it only derives
  and prints the per-DC `RaftGroupId` (Sprint 6 scaffolding); connect it to the
  node so it actually creates the group (FMEA CTL-8).

## Later

- **Multi-node `raft log-truncate` guard.** Truncation correctness depends on
  the operator applying the right `--from` on every damaged node; explore a
  cluster-wide inspect that reports which nodes share the same log damage
  (FMEA CTL-1).
- **Structured (`--json`) output for the observability commands.** `status`,
  `connections`, `queries`, `storage`, `topology`, `peers` print human tables
  only; a machine-readable mode would help scripting and monitoring.
- **TUI panel parity with CLI.** The dashboard shows only Connections / Queries
  / Storage; topology/peers/ring panels would unify the two surfaces.

## Non-goals

- Becoming a server. `ferrosa-ctl` stays a leaf client; node-side logic
  (membership changes, PITR execution, password hashing) lives in the node,
  reached over CQL or the web admin API — ctl never reimplements it.
- Re-implementing corruption detection. SSTable verdicts must always delegate to
  `StorageEngine::smoke_test_generation` so ctl can never diverge from startup.
