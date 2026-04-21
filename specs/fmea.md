# FMEA — ferrosa + ferrosa-memory System

> Date: 2026-04-18
> Scope: Whole-system Failure Mode and Effects Analysis covering the
>        ferrosa storage/SSTable/Raft/CQL engine and the ferrosa-memory
>        MCP service that depends on it.
> Supersedes (but does not delete): the scoped FMEAs below. Where an ID
>        from this document overlaps a scoped doc, the scoped doc remains
>        the authoritative deep-dive for that subsystem.
>
>        - [specs/fmea-cluster-formation.md](fmea-cluster-formation.md) — cluster
>          formation state machine (F1..F29). Not re-entered here; see CF-*
>          back-reference row.
>        - [specs/observability-fmea.md](observability-fmea.md) — telemetry pipeline
>          (OF1..OF12). Not re-entered here; see OBS-* back-reference row.
>        - [ferrosa-memory/specs/fmea.md](../../ferrosa-memory/specs/fmea.md) —
>          MCP-internal modes (F01..F69). The storage/boundary-level modes
>          that affect ferrosa-memory are re-surfaced here under the MEM-*
>          prefix; MCP-internal logic stays in that file.
>
> Method: RPN = Severity × Occurrence × Detection on a 1–10 scale each
>        (max 1000). Action required for RPN ≥ 50. Every row with
>        RPN ≥ 50 has at least one recommended test.

## Scoring Criteria

| Score | Severity (S)                | Occurrence (O)      | Detection (D)                        |
|-------|-----------------------------|---------------------|--------------------------------------|
| 1     | No effect                   | < 1 in 10,000       | Always caught pre-release            |
| 2–3   | Minor degradation           | ~1 in 1,000         | Unit/integration tests catch it      |
| 4–6   | Significant degradation     | ~1 in 100           | Moderate — only specific tests catch |
| 7–8   | Major failure / data loss   | ~1 in 10            | Low — requires targeted audit        |
| 9–10  | Critical / security breach  | > 1 in 10 / chronic | None — only detected post-incident   |

Status legend: **fixed** = verified resolved in code; **mitigated** = partial
fix in place, residual risk remains; **open** = no code-level mitigation yet.

---

## Top-10 by RPN

| Rank | ID      | Mode                                                                              | Status     | RPN   |
|-----:|---------|-----------------------------------------------------------------------------------|------------|------:|
| 1    | SEC-1   | MinIO credentials committed in `docker-compose.yml`                               | open       | **560** |
| 2    | AUTH-3  | Default-password seed role left unchanged → unauthenticated superuser access      | open       | **486** |
| 3    | INF-1   | Single podman VM host — hypervisor/panic loses whole cluster simultaneously       | open       | **432** |
| 4    | OPS-3   | S3 manifest references locally quarantined files → download 404 storm at boot     | mitigated  | **360** |
| 5    | AUTH-1  | Direct CQL writes to graph-owned tables produce graph-consistency violations      | open       | **315** |
| 6    | STO-5   | Legacy corrupt SSTables in S3 survive restart via bootstrap download              | mitigated  | **280** |
| 7    | AUTH-2  | Roll-forward `FERROSA_AUTH_DISABLED=false` locks out backup/MCP                   | open       | **252** |
| 8    | INF-2   | 2 GB cgroup spikes during writer-verify parse pass on large flush                 | open       | **224** |
| 9    | AUTH-4  | `system_auth.roles` not replicated before role DDL returns                        | open       | **216** |
| 10   | OPS-1   | Backup retries mask partial data loss (22 FAILED tables/cycle)                    | mitigated  | **192** |

Back-references (not in top-10 but still owned elsewhere):

- **CF-F3** (Cluster Formation DDL race) — RPN 378, see `fmea-cluster-formation.md`.
- **OBS-OF1** (telemetry self-write loop) — RPN 504, see `observability-fmea.md`.
- **MEM-F62** (effective rule set divergence) — RPN 280, see `ferrosa-memory/specs/fmea.md`.

---

## 1. 2026-04-19 Incident-log modes

### STO-1 — Phantom SSTable IDs via `format!("{}", i+1)` fallback

- **Mode:** `store.rs::sstable_metadata` synthesized an SSTable ID from the
  loop index when the real ID was missing, producing identifiers that did
  not exist on disk.
- **Cause:** Fallback branch papered over missing metadata instead of
  failing loud. Violation of the "never fake success" rule.
- **Local effect:** `ListSSTables` / compaction planner reported
  non-existent file IDs.
- **System effect:** Compaction attempted to open files that didn't exist
  → errors in metric path; confused operator dashboards; possible
  mis-targeting if a later SSTable happened to be created with that ID.
- **Current detection:** Fixed — fallback removed; missing metadata is a
  hard error.
- **Scores:** S=7, O=2, D=3 → **RPN 42**. Status: **fixed**.
- **Recommended test:** Unit test in `ferrosa-storage` that feeds
  `sstable_metadata` a directory with a missing-metadata SSTable and
  asserts a typed error (not a phantom ID).
- **Mitigation:** Already applied. Add `#[deny(clippy::unwrap_used)]`
  enforcement to prevent regression.

### STO-2 — Writer accepts `row.clustering` that doesn't match schema `clustering_types` length

- **Mode:** BTI writer accepted rows with a clustering vector of the wrong
  arity, producing Partitions.db rows that could not be read back.
- **Cause:** Missing shape validation in `add_partition`.
- **Local effect:** On-disk partition is unreadable.
- **System effect:** Reads of any key in that partition fail; compaction
  input becomes poisoned.
- **Current detection:** Fixed via `add_partition` validation — wrong
  arity now rejected at write time.
- **Scores:** S=8, O=2, D=3 → **RPN 48**. Status: **fixed**.
- **Recommended test:** Unit test that passes a `Row` with N-1 and N+1
  clustering components against a schema of arity N and asserts
  `Err(InvalidClustering)`.
- **Mitigation:** Validation in place; add property-based (`proptest`)
  test that the clustering arity always matches the schema.

### STO-3 — Writer emits Partitions.db referencing offsets past Data.db

- **Mode:** Offset bookkeeping drift produced Partitions.db entries that
  pointed past end-of-Data.db.
- **Cause:** Writer bug where a partition boundary was recorded before
  the corresponding Data.db flush.
- **Local effect:** Readback of that partition returns EOF / parse error.
- **System effect:** Entire SSTable quarantined; data from the affected
  flush not available until recovery from commit log or replica.
- **Current detection:** Defensive readback now walks the full
  Partitions.db at finalize and asserts every offset ≤ Data.db length.
- **Scores:** S=9, O=2, D=3 → **RPN 54**. Status: **fixed**.
- **Recommended test:** Integration test in `ferrosa-storage/src/engine.rs`
  that writes N partitions, truncates Data.db by 1 byte before finalize,
  and asserts the writer refuses to close.
- **Mitigation:** Readback check already committed. Add a secondary
  `zero-byte Rows.db` quarantine check (already done per commit 6aaf56d).

### STO-4 — glibc thread-arena retention → RSS bloat → cgroup OOM → CQL wedge

- **Mode:** glibc malloc retained per-thread arenas indefinitely; sustained
  workloads grew RSS far beyond heap live set. Under a 2 GB cgroup, the
  process was killed; on restart, the CQL listener appeared to wedge
  until the cgroup recovered.
- **Cause:** Default glibc allocator behavior with many tokio worker
  threads.
- **Local effect:** RSS grows without bound, no corresponding heap growth.
- **System effect:** Node OOM-killed; cluster sees a missing replica;
  CQL clients see connection drops.
- **Current detection:** Fixed by switching to jemalloc.
- **Scores:** S=8, O=3, D=4 → **RPN 96**. Status: **fixed**.
- **Recommended test:** Soak test: run `ferrosa-loadgen` for 60 min
  under a 2 GB cgroup with jemalloc; assert RSS stable within ±10%
  of steady-state.
- **Mitigation:** jemalloc shipped. Consider adding `jeprof` dump-on-SIGUSR2
  so future arena issues are diagnosable without a restart.

### STO-5 — Legacy corrupt SSTables in S3 survive restart via bootstrap download

- **Mode:** A previously quarantined-local SSTable is re-fetched from S3
  on bootstrap and becomes live again.
- **Cause:** Quarantine is local-only; S3 is treated as authoritative. No
  pre-bootstrap smoke test validates the SSTable set.
- **Local effect:** Re-introduction of corrupt partitions; reads fail or
  return corrupt data.
- **System effect:** Node crashes, or silently returns corrupted rows to
  clients. If the other replicas are also stale, corrupt data propagates
  via read repair.
- **Current detection:** Mitigated by manual quarantine procedure; no
  automatic smoke test on bootstrap (filed follow-up).
- **Scores:** S=10, O=4, D=7 → **RPN 280**. Status: **mitigated**.
- **Recommended tests:**
  1. Integration test: place a known-bad SSTable in S3, start node, assert
     startup smoke test marks it invalid and does NOT put it on the live
     list.
  2. Integration test: verify quarantine marker is written to S3 so other
     nodes also skip the file.
- **Mitigation:** Implement startup smoke test (parse Partitions.db +
  random-sample Data.db reads). Persist quarantine markers to S3.

### STO-6 — Raft AppendEntries 600 ms timeout during mode-transition bootstrap

- **Mode:** During the Forming→Cluster transition, AppendEntries
  occasionally exceed the 600 ms heartbeat-derived deadline, producing
  transient error spam.
- **Cause:** Bootstrap-time disk I/O bursts and log-store catch-up.
- **Local effect:** Log spam and metric spikes.
- **System effect:** No correctness effect; elevated alert noise.
- **Current detection:** Filed; not yet silenced.
- **Scores:** S=3, O=6, D=4 → **RPN 72**. Status: **open**.
- **Recommended test:** Integration test asserting AppendEntries failures
  during Forming are classified as `transient_bootstrap` in metrics
  rather than `error`.
- **Mitigation:** Tag AppendEntries errors with a `during_bootstrap`
  label; downgrade their log level to DEBUG for the first N seconds
  after mode change.

### STO-7 — `ClusterDdlForwardHandler` "not Raft leader" spam from stale pair-primary cache

- **Mode:** After a leader change, the pair-primary cache keeps returning
  the old leader, producing repeated "not Raft leader" log entries on
  DDL forwarding.
- **Cause:** Cache invalidation not wired to Raft leader-change events.
- **Local effect:** Log noise; one DDL round-trip is wasted before the
  client retries.
- **System effect:** No correctness effect. Operator alert fatigue.
- **Current detection:** Filed.
- **Scores:** S=3, O=5, D=4 → **RPN 60**. Status: **open**.
- **Recommended test:** Integration test: force leader change, issue
  DDL on the stale-cache node, assert at most one "not leader" redirect
  log line per DDL (not a burst).
- **Mitigation:** Subscribe the pair-primary cache to Raft
  `LeaderChanged` events; invalidate synchronously.

---

## 2. Auth-related modes (CQL role-auth rollout)

### AUTH-1 — Direct CQL writes to graph-owned tables produce consistency violations

- **Mode:** A CQL client with write permission inserts directly into
  `typed_edges`, `graph_vertices`, or related graph-owned tables,
  bypassing the graph engine's invariants (degree caps, edge type
  schema, audit hooks).
- **Cause:** Table-level authorization does not distinguish
  "graph-managed" from "user-managed" tables. Role-auth grants blanket
  INSERT/UPDATE at the table level.
- **Local effect:** Rows appear in graph tables without matching edge
  type registration or inverse-edge bookkeeping.
- **System effect:** Graph traversals return partial/corrupted results;
  Cypher queries silently skip dangling rows; audit log is missing
  provenance for those writes. Cross-tenant leakage possible if row
  keys encode other tenants.
- **Current detection:** None — writes are indistinguishable from legal
  ones at the storage layer.
- **Scores:** S=9, O=5, D=7 → **RPN 315**. Status: **open**.
- **Recommended tests:**
  1. Integration test: with a CQL role that has `MODIFY` on a graph-owned
     table, attempt a direct INSERT; assert rejection via role-auth
     policy (deny-list of graph-owned tables).
  2. Static/CI test: enumerate graph-owned tables and assert each is
     listed in the deny-list config.
- **Mitigation:** Add a `graph-owned` table attribute; role-auth policy
  denies direct DML on any `graph-owned` table regardless of role.
  Route all graph mutations through the graph API.

### AUTH-2 — Roll-forward `FERROSA_AUTH_DISABLED=false` locks out backup/MCP

- **Mode:** An operator flips `FERROSA_AUTH_DISABLED=false` to enable
  CQL role auth; the backup script and/or `ferrosa-memory` MCP have no
  configured credentials and are immediately denied.
- **Cause:** Migration lacks a rollout gate requiring dependent-service
  credentials to exist and validate before auth is turned on.
- **Local effect:** Backup job fails; MCP returns auth errors.
- **System effect:** Silent data-protection gap (no fresh backups);
  ferrosa-memory tool calls fail for real users.
- **Current detection:** Only when the next backup cycle fails or a user
  reports broken memory.
- **Scores:** S=7, O=6, D=6 → **RPN 252**. Status: **open**.
- **Recommended tests:**
  1. Integration test: pre-flight script validates that backup role +
     MCP role exist with correct grants before the node accepts
     `FERROSA_AUTH_DISABLED=false`.
  2. Integration test: startup with auth enabled and backup role missing
     exits non-zero with a clear error message.
- **Mitigation:** Guarded rollout: `enable_auth` only succeeds if
  (a) a named "bootstrap-required" role set authenticates successfully
  from this node within N seconds, or (b) an operator passes
  `--allow-lockout`.

### AUTH-3 — Default-password seed role left unchanged → unauthenticated superuser access

- **Mode:** First-boot seeding creates a superuser role with a known
  default password; operator forgets to rotate.
- **Cause:** No "must-rotate-on-first-login" marker; no startup refusal
  for the default password after N seconds.
- **Local effect:** Anyone with network reach to 9042 has superuser on
  the cluster.
- **System effect:** Full data compromise, full DDL compromise,
  pivot-to-OS via UDF if UDF auth is weak.
- **Current detection:** Only via external audit.
- **Scores:** S=9, O=6, D=9 → **RPN 486**. Status: **open**.
- **Recommended tests:**
  1. Unit test: default password is not accepted after the initial
     "force rotate" window (e.g., 10 minutes) unless explicitly
     unblocked.
  2. Integration test: attempting to log in with the default password
     after seeding completes returns
     `AuthError::DefaultPasswordMustRotate`.
  3. Smoke test: startup emits WARN every 60s until the default
     password is rotated.
- **Mitigation:** Random per-install default password printed once to
  stdout and persisted to an operator-readable file with 0600;
  account locked until rotated. No static default.

### AUTH-4 — `system_auth.roles` not replicated before a role DDL returns

- **Mode:** `CREATE ROLE` returns success to the client before the role
  row is committed on a quorum of replicas. Client connects to a
  different coordinator and fails to authenticate.
- **Cause:** Role DDL path uses local write + fire-and-forget replication
  instead of Raft/quorum commit.
- **Local effect:** Authentication succeeds on node A, fails on node B
  for the same role.
- **System effect:** Intermittent auth failures during rollout; load
  balancers see half-broken cluster.
- **Current detection:** Only via client-side error-rate alerts.
- **Scores:** S=8, O=6, D=4.5 → **RPN 216**. Status: **open**.
- **Recommended tests:**
  1. Integration test (3-node Firecracker): `CREATE ROLE` on node 1,
     immediately authenticate against node 2; assert success.
  2. Jepsen-style test: partition node 3, create role on quorum,
     heal partition, assert node 3 converges before its next client
     auth call succeeds.
- **Mitigation:** Route `system_auth` writes through Raft (or through
  the same DDL path as schema) and gate the DDL response on quorum
  commit.

### AUTH-5 — Encrypted client-auth credentials mount fails silently → anonymous fallback

- **Mode:** ferrosa-memory (or another client) reads credentials from an
  encrypted secret mount; mount is missing or decryption fails; client
  falls back to anonymous auth and is denied.
- **Cause:** Fallback-to-anonymous on credential-load failure instead of
  fail-fast. Violation of the "never silently degrade" rule.
- **Local effect:** Client sees generic "auth denied" instead of
  "credentials failed to load".
- **System effect:** Operator cannot distinguish wrong-password from
  missing-mount; MTTD spikes during rollout.
- **Current detection:** Only if operator inspects the client logs
  carefully.
- **Scores:** S=6, O=5, D=5 → **RPN 150**. Status: **open**.
- **Recommended tests:**
  1. Unit test: credential loader with missing file returns typed
     `CredentialLoadError`, not `None`.
  2. Integration test: start MCP with no credentials mount; assert
     process exits non-zero with a clear error and does not attempt
     anonymous auth.
- **Mitigation:** Credential loader must return `Result`; callers must
  NOT coerce to anonymous. Startup log line must distinguish
  "no-credentials-configured" from "credentials-failed-to-load".

---

## 3. Structural / operational modes

### INF-1 — Single podman VM host — VM panic loses whole cluster

- **Mode:** All ferrosa nodes run as containers on one podman VM on one
  physical host; host panic, kernel upgrade, or disk failure takes the
  entire cluster down simultaneously.
- **Cause:** Infrastructure co-location. No blast-radius isolation.
- **Local effect:** All nodes stop.
- **System effect:** Total outage. Data is safe in S3, but availability
  is zero until the VM is rebuilt. RF>1 provides no protection when all
  replicas share a physical host.
- **Current detection:** Only when users report outage.
- **Scores:** S=9, O=6, D=8 → **RPN 432**. Status: **open**.
- **Recommended tests:**
  1. Game-day: manually `systemctl stop podman` on the host; measure
     recovery time.
  2. Integration test in `ferrosa-jepsen`: simulate whole-cluster loss;
     assert S3 state is sufficient to rebuild from scratch.
- **Mitigation:** Spread nodes across at least two VMs on different
  hosts (preferably AZs). Until then, document the known SPOF in the
  operator runbook and in `specs/threat-model.md`.

### SEC-1 — MinIO credentials committed in `docker-compose.yml`

- **Mode:** Root `docker-compose.yml` contains literal MinIO access key
  and secret key; anyone with repo read has object-store creds.
- **Cause:** Convenience during early development; never rotated out.
- **Local effect:** Creds usable by anyone who has cloned the repo.
- **System effect:** Full data-plane compromise (read and write) to the
  S3 bucket used by this deployment. The credentials are long-lived and
  static.
- **Current detection:** None; `frg secret_scan` or pre-commit would
  catch future ones but the existing ones are already published.
- **Scores:** S=10, O=8, D=7 → **RPN 560**. Status: **open**.
- **Recommended tests:**
  1. Pre-commit hook / CI: `frg secret_scan` fails the build on any
     access-key-shaped string under a 0-length deny-list.
  2. Unit test in the compose wrapper: required env vars must be set;
     startup fails if the literal placeholder values are present.
- **Mitigation:** Rotate MinIO keys immediately. Move to `${VAR}` env
  interpolation in compose. Add `.env` to `.gitignore` (check it isn't
  already tracked). Add a CI scanner. Consider short-lived STS creds.

### INF-2 — 2 GB cgroup spikes during writer-verify parse pass on large flush

- **Mode:** Writer-verify reads back the full Partitions.db + samples
  Data.db after flush; on a large flush, parse buffers plus
  tokio-task stacks spike RSS above the 2 GB cgroup → OOM kill.
- **Cause:** Verify pass is not size-bounded; runs on the same task
  as the flush; no backpressure.
- **Local effect:** Node killed mid-flush.
- **System effect:** SSTable left in tmp state (renamed path only on
  success, so usually clean) but commit log must replay on restart; if
  the OOM recurs, the node cannot make progress.
- **Current detection:** OOM killer after the fact.
- **Scores:** S=8, O=4, D=7 → **RPN 224**. Status: **open**.
- **Recommended tests:**
  1. Integration test under a 2 GB cgroup: flush a 1.5 GB memtable;
     assert peak RSS stays ≤ 1.8 GB.
  2. Unit test: writer-verify parses Partitions.db in streaming mode
     (bounded buffer) — not load-all-then-parse.
- **Mitigation:** Stream Partitions.db in bounded chunks; cap verify
  memory use; fall back to cheaper verify (checksum only) when free
  RAM is below a watermark.

### OPS-1 — Backup retries mask partial data loss (22 FAILED tables/cycle)

- **Mode:** The backup script retries failed tables; retry eventually
  succeeds for most but consistently fails for ~22 tables per cycle.
  The top-line "backup succeeded" message hides the per-table failures.
- **Cause:** Aggregated success/failure is computed from the final retry,
  not the per-table count. Violation of "fallbacks require disclosure".
- **Local effect:** Backup report shows green; 22 tables are not
  actually backed up.
- **System effect:** Recovery would find those tables missing or stale.
- **Current detection:** Only by reading the full log.
- **Scores:** S=8, O=6, D=4 → **RPN 192**. Status: **mitigated**.
- **Recommended tests:**
  1. Unit test: a backup cycle with any FAILED table at final retry
     exits non-zero and emits a `backup_tables_failed_total` metric.
  2. Integration test: quarantine a table; run backup; assert clear
     FAILED line per table and overall non-zero exit.
- **Mitigation:** Script now tracks per-table status; add the non-zero
  exit and metric, plus an alerting rule.

### OPS-2 — Compose stop during graceful flush may leave partial SSTables on disk

- **Mode:** `docker compose stop` during flush can kill the process
  between `write_tmp` and `rename`; partial files remain.
- **Cause:** Graceful flush is best-effort under compose's default
  timeout.
- **Local effect:** `.tmp` files left on disk.
- **System effect:** Already largely mitigated by tmp→rename convention
  (readers never see partial files). Startup must still clean up stale
  `.tmp` files.
- **Current detection:** Verified tmp→rename catches most cases.
- **Scores:** S=5, O=4, D=3 → **RPN 60**. Status: **mitigated**.
- **Recommended tests:**
  1. Integration test: SIGKILL the node mid-flush; assert no `.tmp`
     files remain after next startup.
  2. Unit test: startup directory scan removes stale `.tmp` files older
     than N minutes and logs the cleanup.
- **Mitigation:** Increase compose stop timeout to 30s; add a startup
  tmp-file sweeper with logging.

### OPS-3 — S3 manifest references quarantined files → 404 storm at boot

- **Mode:** `manifest.json` lists files that have been locally
  quarantined; a fresh node downloads per the manifest and gets 404s
  for every quarantined entry.
- **Cause:** Manifest updated from one node's view; quarantine decisions
  made on other nodes don't propagate to the manifest.
- **Local effect:** Repeated download attempts; elevated S3 request
  cost; node startup stalls.
- **System effect:** Bootstrap time inflated; may cross Raft
  `formation_timeout`; in the worst case, the node cannot start.
- **Current detection:** Only via S3 4xx metrics.
- **Scores:** S=8, O=5, D=9 → **RPN 360**. Status: **mitigated**.
- **Recommended tests:**
  1. Integration test: quarantine an SSTable on node A; refresh manifest
     from node B; fresh node C boots; assert no 404 storm (manifest no
     longer references the file OR node C marks as quarantined without
     retrying).
  2. Unit test: manifest writer excludes locally quarantined files.
- **Mitigation:** Write quarantine markers to S3 (shared view); manifest
  generation filters out anything with a quarantine marker; bootstrap
  treats 404 on a manifest-listed file as a hard quarantine signal,
  not a retryable error.

---

## 4. ferrosa-memory boundary modes re-surfaced

Modes below are caused at the ferrosa boundary but manifest inside
ferrosa-memory. Deep internal modes (F01..F69) remain in
`ferrosa-memory/specs/fmea.md`.

### MEM-1 — Graph writes via direct CQL bypass the graph API

Duplicate of AUTH-1 from the ferrosa-memory angle. See
`ferrosa-memory/specs/fmea.md` F66. No separate row here — fix AUTH-1
and F66 together.

### MEM-2 — Storage error surfaces as "empty result" to MCP client

- **Mode:** CQL query errors (e.g., coordinator-side timeout, schema
  mismatch) are caught by the MCP tool layer and converted to an empty
  array response.
- **Cause:** Over-broad `rescue` in the memoization / retrieval path.
- **Local effect:** Tool call reports "no results".
- **System effect:** Agent makes decisions on non-data.
- **Current detection:** Sampled tool-call logs.
- **Scores:** S=7, O=4, D=5 → **RPN 140**. Status: **open**.
- **Recommended test:** Integration test: block CQL port during a
  `hybrid_search` call; assert the tool returns an error result object,
  not an empty hit list.
- **Mitigation:** Tool handlers must return `Result<...>`; only the
  outermost MCP serializer converts errors to JSON-RPC error responses.
  No intermediate `unwrap_or(empty)`.

---

## 5. FMEA table (sorted by RPN)

| ID     | Mode (short)                                               | Status     | S | O | D | RPN   |
|--------|------------------------------------------------------------|------------|--:|--:|--:|------:|
| SEC-1  | MinIO creds in docker-compose.yml                          | open       |10 | 8 | 7 | **560** |
| AUTH-3 | Default-password superuser seed left unrotated             | open       | 9 | 6 | 9 | **486** |
| INF-1  | Single podman VM host — VM panic = total outage            | open       | 9 | 6 | 8 | **432** |
| OPS-3  | S3 manifest references locally quarantined files           | mitigated  | 8 | 5 | 9 | **360** |
| AUTH-1 | Direct CQL writes to graph-owned tables                    | open       | 9 | 5 | 7 | **315** |
| STO-5  | Legacy corrupt SSTables persist via S3 bootstrap           | mitigated  |10 | 4 | 7 | **280** |
| AUTH-2 | Auth roll-forward locks out backup/MCP                     | open       | 7 | 6 | 6 | **252** |
| INF-2  | 2 GB cgroup spikes during writer-verify                    | open       | 8 | 4 | 7 | **224** |
| AUTH-4 | system_auth.roles not replicated before role DDL returns   | open       | 8 | 6 |4.5| **216** |
| OPS-1  | Backup retries mask partial data loss                      | mitigated  | 8 | 6 | 4 | **192** |
| AUTH-5 | Encrypted cred mount fails → anonymous fallback            | open       | 6 | 5 | 5 | **150** |
| MEM-2  | Storage errors surface as empty MCP results                | open       | 7 | 4 | 5 | **140** |
| STO-4  | glibc thread-arena RSS bloat → cgroup OOM                  | fixed      | 8 | 3 | 4 |   96  |
| STO-6  | AppendEntries 600ms timeouts during bootstrap              | open       | 3 | 6 | 4 |   72  |
| STO-7  | ClusterDdlForwardHandler "not leader" spam                 | open       | 3 | 5 | 4 |   60  |
| OPS-2  | Compose stop leaves partial .tmp SSTables                  | mitigated  | 5 | 4 | 3 |   60  |
| STO-3  | Partitions.db offsets past Data.db                         | fixed      | 9 | 2 | 3 |   54  |
| STO-2  | Writer accepts mismatched clustering arity                 | fixed      | 8 | 2 | 3 |   48  |
| STO-1  | Phantom SSTable IDs from `format!("{}", i+1)`              | fixed      | 7 | 2 | 3 |   42  |

See `fmea-cluster-formation.md` for CF-F1..F29 (top: CF-F3 = 378) and
`observability-fmea.md` for OF1..OF12 (top: OF1 = 504).

---

## 6. Cross-cutting observations

1. **Silent-degradation culture remnants.** STO-1, OPS-1, MEM-2, AUTH-5
   all share the same failure philosophy violation: an error is caught
   and replaced with a default. The project-wide fix is a lint policy
   (`#[deny(clippy::unwrap_used)]`, `clippy::ok_expect`) plus code
   review emphasis on "never fake success."
2. **Boundary-level auth design gaps.** AUTH-1..AUTH-5 are all a single
   design pattern: auth assumes storage-level role is sufficient;
   domain-level semantics (graph-owned tables, role replication,
   rotation, credential-load failure) are not modeled.
3. **S3 as "authoritative" is load-bearing and leaky.** STO-5 and OPS-3
   both exploit the asymmetry between local quarantine decisions and
   S3 bootstrap. Write quarantine markers to S3.
4. **Single-host blast radius.** INF-1 is a config/deploy decision, not
   a code decision, but it dominates every correctness win above.
5. **Cgroup sizing isn't tested against worst-case paths.** INF-2 is a
   known path where RSS exceeds the cgroup. Add a soak test that
   exercises writer-verify at realistic sizes.

## 7. Recommended actions

| Priority | Item                                                              | Effort |
|----------|-------------------------------------------------------------------|--------|
| P0       | SEC-1: rotate MinIO keys, move to env interpolation, add scanner  | S      |
| P0       | AUTH-3: remove static default password; random per-install       | S      |
| P0       | INF-1: split nodes across at least two VMs                        | M      |
| P0       | AUTH-1: table-level deny on graph-owned tables                    | M      |
| P1       | OPS-3: quarantine markers in S3 + manifest filter                 | M      |
| P1       | STO-5: startup SSTable smoke test + S3 quarantine markers         | M      |
| P1       | AUTH-2: guarded rollout check; AUTH-4: quorum-commit role DDL     | M      |
| P1       | INF-2: bounded writer-verify with cgroup-aware backpressure       | M      |
| P2       | OPS-1: per-table failure counter + non-zero exit                  | S      |
| P2       | STO-6/STO-7: classify transient bootstrap errors; invalidate
             pair-primary cache on leader change                              | S      |
| P2       | MEM-2: tool layer returns `Result`; no empty-array fallbacks      | S      |
| P2       | AUTH-5: credential loader returns typed errors; no anonymous
             fallback                                                         | S      |

## 8. Related documents

- `specs/fmea-cluster-formation.md` — F1..F29 (top item F3, RPN 378)
- `specs/observability-fmea.md` — OF1..OF12 (top item OF1, RPN 504)
- `specs/hazards.md` — paired Rust-level hazard scan (same sprint)
- `specs/hazards-cluster-formation.md` — paired scoped hazard scan
- `ferrosa-memory/specs/fmea.md` — F01..F69 (MCP internal)
