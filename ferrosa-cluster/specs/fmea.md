---
crate: ferrosa-cluster
doc: fmea
last_updated: 2026-06-19
---

# ferrosa-cluster — FMEA / Known Issues

Failure modes are ranked by **RPN = Severity × Occurrence × Detection** (1–10
each; higher = worse). This is the distribution layer, so safety failures are
cluster-wide and severities run high. Several entries are *evidence* gaps
(insufficient validation) rather than known-broken code — those are called out.

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation / status |
|----|--------------|--------|---|---|---|-----|---------------------|
| CL-1 | **No external/public Jepsen validation of Accord + Raft.** All consensus/transaction tests are in-crate, deterministic harnesses (`TestCluster`, simulated nemesis). Real partitions, clock skew, fsync faults, and Byzantine timing are not exercised end-to-end. | A linearizability/strict-serializability violation that only manifests under real-world faults would ship undetected | 10 | 4 | 8 | 320 | **Open evidence gap.** `ferrosa-jepsen` harness designed + approved (`specs/todo/jepsen-e2e-test-plan.md`) but **not built**. Strong in-crate property/recovery tests reduce occurrence but cannot close detection. Treat correctness as *tested, not proven*. |
| CL-2 | **Raft election storm on log divergence.** A follower whose log falls behind bumps its term unboundedly (no pre-vote in upstream openraft 0.9), burning CPU and inflating terms while the cluster is stable (observed T18,348 vs leader T8). | Hot node, term inflation, replication backoff; can block snapshot delivery | 8 | 5 | 4 | 160 | **Mitigated.** Pinned openraft fork adds PreVote + CheckQuorum (ADR-012, default on). Belt-and-suspenders `election_guard` watchdog (P0-17/P0-19) suppresses storms (`elect(false)` 60 s) and exposes `ELECTION_STORM_TERM_JUMPS_TOTAL`; `snapshot_pusher` (P0-20) pushes snapshots to lagging followers. Guard/pusher slated for removal (W4.11/W4.12) only after a clean Jepsen window — which depends on CL-1. |
| CL-3 | **Write backpressure exhaustion starves the data path.** `WRITE_CONCURRENCY_LIMIT = 128` semaphore protects Raft from bulk-insert runtime saturation, but a saturated cluster returns `Unavailable` once all 128 permits are held. | Bulk writers see `Unavailable`/`WriteTimeout` under load; client must retry/throttle | 6 | 5 | 3 | 90 | **By design, fail-loud.** Limit prevents the worse failure (Raft heartbeat starvation → election storms). Constant is fixed, not tunable; batch path has a separate `DEFAULT_BATCH_CONCURRENCY = 32`. Tuning guidance + per-tenant limits are roadmap items. |
| CL-4 | **Accord coordinator-failure recovery edge cases.** Recovery must re-propose by highest `accepted_ballot` (not `max_ballot_seen`); supersession ordering and cross-DC recovery are exercised only on the deterministic harness. | Wrong recovery decision → committed value lost or duplicated → serializability violation | 10 | 2 | 7 | 140 | Recovery rule + ballot invariant covered by `recovery_scenarios.rs` (13) and `proptests.rs`. Cross-DC adapter is thin glue with **no in-crate cross-DC failure test**. Depends on CL-1 for real validation. |
| CL-5 | **Accord storage-apply seam untested under crashes.** Apply path dep-waits then writes via `StorageApplier`; in-crate tests use `NoopStorageApplier`, so real durability-under-crash on the engine-backed applier is unproven here. | A transaction reported applied may not be durable after a crash at the wrong moment | 9 | 2 | 7 | 126 | Dep-wait + idempotent re-apply implemented; production wires the engine applier. Crash-durability of the seam is a CL-1-adjacent evidence gap. |
| CL-6 | **Read-repair / re-fetch failure on digest mismatch.** If the newest replica cannot be re-fetched, the read fails loud (`ReadTimeout`) rather than serving a possibly-stale copy. | Reads error transiently under replica flakiness instead of returning data | 5 | 4 | 3 | 60 | **By design, fail-loud** to protect linearizability. Inline repair writes the resolved value to stale replicas before returning. Acceptable trade vs. silent staleness. |
| CL-7 | **Hinted-handoff eviction loses mutations.** Hints are byte-budget-capped per peer (`max_per_peer_mb`, default 1 GiB); a long-down peer overflows the budget and new hints are rejected. **No time-based TTL.** | Mutations destined for a long-down replica are dropped; replicas diverge until repair | 7 | 3 | 3 | 63 | **Fail-loud, designed.** Overflow rejected at append (no silent loss), `needs_repair` flag set + ERROR logged; anti-entropy repair is the documented backstop. TTL-based expiry not implemented. |
| CL-8 | **Anti-entropy repair queue overflow under sustained corruption.** Read-path corrupt-SSTable detections feed a bounded queue (cap 1024); on overflow requests coalesce (global counter still increments, enqueue skipped). | Some refill requests dropped; affected ranges wait for the periodic scheduler | 6 | 3 | 4 | 72 | Bounded by design (serve-now/repair-later LOCKED DESIGN). Global metric always increments so overflow is observable; 24 h `AutoRepairScheduler` is the backstop. |
| CL-9 | **Formation timeout falls back to Pair (silent durability reduction).** If a leader is not elected within `formation_timeout_secs`, the node reverts to `DegradedPair` and spawns `attempt_rejoin` instead of crashing. | Cluster may run with fewer replicas than configured RF; writes during the window have reduced durability | 7 | 3 | 4 | 84 | **Disclosed fallback.** Counters `LEADER_ELECTION_TIMEOUTS`, `FORMATION_REDUCED_DURABILITY_WRITES`, `RAFT_PUBLISH_NO_SUBSCRIBERS`, `RAFT_INITIALIZE_FAILURES` surface the condition; `cluster_rejoin` (P0-21) retries with capped backoff and counts `CLUSTER_REJOIN_FAILURES_TOTAL`. Operator must REPAIR after full membership. |
| CL-10 | **Merkle XOR hash collision (false-negative divergence).** Depth-15 leaves XOR 64-bit content hashes; a collision can mask real divergence (~2⁻³² per diverging content, Cassandra-equivalent). | Two replicas with different data hash equal → repair skips them → permanent silent divergence | 8 | 1 | 8 | 64 | Accepted trade-off matching Cassandra. Hash widened 32→64 bit and made content-aware (was key-only). Residual risk inherent to Merkle anti-entropy. |
| CL-11 | **Multi-DC Accord apply reorder-buffer stall.** Cross-DC `AccordApply` entries buffer by HLC; if writes outrun the `max_skew` watermark (default 200 ms), the buffer grows past `REORDER_BUFFER_ALARM_DEPTH = 100`. | Cross-DC apply latency rises; buffer pressure under clock skew | 5 | 3 | 4 | 60 | Alarm metric on buffer depth; applied-txn ledger dedupes; GC bounds memory. Cross-DC paths are newest and least battle-tested (ADR-015). |
| CL-12 | **Bincode Raft-log wire fragility.** `RaftOp` variant reordering silently corrupts the persisted log. | A careless enum edit bricks log replay across a rolling upgrade | 9 | 1 | 5 | 45 | `raft_op_variant_tag_stability` test pins discriminants; recovery tooling `ferrosa-ctl raft log-inspect`/`log-truncate`; legacy format auto-migrated on load. |
| CL-13 | **Degraded pair read rejection (fixed).** `transition_to_degraded()` previously replaced `WritePath` with `Unavailable`, blocking both writes AND reads. The `WritePath` is also the read path. `is_cql_ready()` returned `true` for `DegradedPair` (intending stale reads), but the read methods on `Unavailable` returned errors. | After primary failure, follower could not serve reads of replicated data until operator promotion — violating the pair-mode design rule that reads work without promotion. | 8 | 4 | 5 | 160 | **Fixed:** `DegradedPair(Arc<PairCoordinator>)` variant preserves `local_storage()` for reads while rejecting writes. Regression test `degraded_pair_serves_stale_reads` verifies the `WritePath` variant after peer loss. |

## Top risks to act on

1. **CL-1 (RPN 320)** — the dominant risk is *evidence*, not a known bug: there is
   no external Jepsen run. Until `ferrosa-jepsen` lands, every "strict
   serializability holds" claim rests on deterministic in-crate tests. Build the
   harness; it also gates removal of the CL-2 election-guard/snapshot-pusher
   safety nets.
2. **CL-2 (RPN 160)** — election storms are well-mitigated (PreVote + CheckQuorum +
   guard + pusher), but the mitigations are *layered band-aids* awaiting Jepsen
   confirmation before the band-aids come off. Keep the guard until CL-1 closes.
3. **CL-4 / CL-5 (RPN 140 / 126)** — Accord recovery and the storage-apply seam are
   the correctness-critical paths least covered by real-fault testing; prioritise
   them in the Jepsen workload set and add cross-DC failure cases.

## Detection assets

- `ELECTION_STORM_TERM_JUMPS_TOTAL`, `INSTALLSNAPSHOT_PUSHES_TOTAL` (Raft health).
- `LEADER_ELECTION_TIMEOUTS`, `FORMATION_REDUCED_DURABILITY_WRITES`,
  `RAFT_PUBLISH_NO_SUBSCRIBERS`, `RAFT_INITIALIZE_FAILURES` (formation).
- `CLUSTER_REJOIN_ATTEMPTS_TOTAL` / `_FAILURES_TOTAL` (rejoin).
- `ferrosa_auto_repair_*`, `ferrosa_anti_entropy_refills_{scheduled,no_source}_total`,
  timestamp-tie counters (repair); hint `needs_repair` ERROR logs.
- In-crate suites: `failure_mode_matrix` (44), `raft_election_storm` (36),
  `leader_snapshot_push` (31), `accord_lwt_concurrent` (21), `accord_nemesis` (15),
  `recovery_scenarios` (13), `proptests`, `repair_fuzz` (7).
- **Missing:** external Jepsen/Knossos/Elle history checking (CL-1).
