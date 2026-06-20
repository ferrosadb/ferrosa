---
crate: ferrosa-cluster
doc: roadmap
last_updated: 2026-06-19
---

# ferrosa-cluster — Roadmap

Sourced from the FMEA gaps ([fmea.md](fmea.md)), in-code deprecation markers,
reference/decision specs, and the dependency/usage review. Ordered by value.

## Recently addressed

- **Multi-key Accord — additive V2 wire foundation + `new_multi` API (Phase 1).**
  Multi-key (multi-partition) transactions will travel on new
  `AccordPreAcceptV2`/`AccordApplyV2` message codes (bincode is not
  self-describing, so the single-key payloads keep their exact bytes). Landed so
  far: `WriteSetEntry` + `ApplyV2Payload` wire types, the reserved net message
  codes (round-trip tested in `ferrosa-net`), and
  `AccordCoordinatorDriver::new_multi(write_set)` (with `new` delegating as the
  one-entry degenerate case) so the CQL/Postgres front-ends can be built against
  the API in parallel. Multi-key *execution* is **deliberately fail-loud for now**
  (`AccordDriverError::MultiKeyNotYetExecutable`): a `(txn_id, t)`-idempotent
  applier would no-op writes 2..N of one transaction, so correct multi-key apply
  needs the participant-set/per-shard quorum (Phase 2) and the `(txn_id, key)`
  apply keying (Phase 3) before it can run — fail loud rather than silently drop
  writes. Single-key LWT is unchanged.
- **Replica apply path is now dep-ordered (Phase 0, t_59629c9b).** `handle_apply`
  used to call the storage applier **directly**, ignoring the transaction's
  dependency set — so an `Apply(B)` delivered before its dependency `A` applied
  would persist `B` out of order. It now routes every real write through the
  `DepWaitApplier`: a mutation persists only once all of its dependencies have
  applied on this replica, otherwise it parks and the cascade applies it (in
  dependency order) when the last dependency lands. `try_apply`/`cascade`/
  `notify_applied` return the `(txn_id, data)` of every transaction actually
  persisted, and the state machine runs its post-apply bookkeeping
  (`bookkeep_applied`: protocol-log marker → `Applied` flag → conflict-index GC →
  ReadVote wake) for exactly those. The storage write precedes the marker and the
  applier is idempotent on `(txn_id, t)`, so a crash between them is recovered by
  the per-txn Apply retry — never a falsely-`Applied` txn. This is the single-key
  prerequisite for the multi-key/multi-partition transaction work (Phases 1+).
  Regression test: `sm_apply_is_dep_ordered_parks_until_dependency_applies`.
- **Dep-wait cascade replayed queued txns with empty data — fixed (PR #159).**
  `DepWaitApplier` parked dependency waiters but never stored their mutation, so
  when the last dependency resolved the cascade re-applied each waiter with
  `ApplyMutation { data: vec![] }` — silently dropping the write. It now parks the
  real mutation and replays it (transitively); applier errors fail loud rather than
  committing a lost write. `NoopStorageApplier` now records payloads so this is
  regression-tested. (The previously-open follow-up — route `handle_apply` itself
  through `DepWaitApplier` — is now done; see the dep-ordered apply entry above.)
- **Cluster-wide `fts_match` scatter-gather (BUG-F-007 / t_0d08aa43).** `fts_match`
  carries no partition key, so its hits span every token range, but the served
  path consulted only the coordinator's local FTI — returning 0/1
  non-deterministically depending on which node coordinated. Added a
  `FulltextSearchRequest`/`Response` internode RPC + `FulltextSearchHandler`, a
  `ClusterCoordinator::coordinate_fulltext_search` that fans out to every node and
  unions/de-dupes the matching keys, and `WritePath::fulltext_search`
  (Direct/Pair → local, Cluster → fan-out). The CQL router now routes the FTI
  lookup through the write path. In-process 2-node fan-out + dedup test in
  `coordinator/read.rs`.

## Now (highest value)

- **Build the external Jepsen harness (FMEA CL-1).** `ferrosa-jepsen` is designed
  and approved (`specs/todo/jepsen-e2e-test-plan.md`) but not built. It is the only
  thing that converts "Accord/Raft is *tested*" into "*validated under real
  partitions, clock skew, and disk faults*". This is the single highest-leverage
  item — it also gates Now-item #2.
- **Add cross-DC + apply-seam failure cases to the transaction tests (CL-4, CL-5).**
  The cross-DC adapter has no in-crate failure test, and the storage-apply seam is
  tested only with `NoopStorageApplier`. Add deterministic crash-at-apply and
  cross-DC partition scenarios now (cheap), pending the full Jepsen run.

## Next

- **Retire the election-storm safety nets once Jepsen is clean (CL-2).**
  `election_guard.rs` (W4.11) and `snapshot_pusher.rs` (W4.12) are explicitly
  marked for deletion after a clean Jepsen window against the PreVote+CheckQuorum
  build. Do not remove before CL-1 closes; track the two-week clean-window gate.
- **Make write backpressure observable and tunable (CL-3).** `WRITE_CONCURRENCY_LIMIT`
  (128) is a hard constant. Add an admission/queue-depth metric and consider a
  configurable / per-tenant limit so operators can trade Raft-protection headroom
  against bulk-write throughput instead of hitting an opaque `Unavailable`.
- **Finish the `ApplyError` migration (raft state machine).** `ApplyError` types
  the engine-register path; remaining schema/system-writer failure sites still use
  `Other(String)`. Type them so apply failures are classified, not stringly.
- **Cross-DC Accord hardening (CL-11).** Reorder-buffer back-pressure policy beyond
  the alarm depth, and clock-skew runbook tie-in for the watermark stall path.

## Later

- **Hint TTL / time-based expiry (CL-7).** Today hints are only byte-budget-capped;
  a long-down peer overflows and triggers `needs_repair`. A time bound would let
  operators reason about hint freshness independent of volume.
- **Anti-entropy queue sizing + adaptive scheduling (CL-8).** Tune the 1024-deep
  queue and let the scheduler prioritise ranges with recent corruption signals
  rather than pure round-robin.
- **Merkle false-negative reduction (CL-10).** Investigate a second hash or
  configurable depth for high-divergence tables where the ~2⁻³² per-leaf
  false-negative rate is unacceptable.
- **Multi-DC SERIAL / LWT routing.** `cl_routing` currently returns
  `NotImplementedCrossDc` for multi-DC serial consistency (deferred). Implement
  cross-DC LWT when a consumer needs it.

## Non-goals

- **User-data durability/storage** — owned by `ferrosa-storage`; this crate routes
  and reconciles, it does not persist SSTables.
- **CQL / Bolt / SPARQL / Flight protocol framing and query planning** — owned by
  the front-end crates that call this one.
- **Putting user data through Raft** — Raft is metadata-only by design (keeps the
  log small and protects heartbeats); the data path stays on the coordinator.
