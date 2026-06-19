---
crate: ferrosa-cluster
doc: roadmap
last_updated: 2026-06-19
---

# ferrosa-cluster — Roadmap

Sourced from the FMEA gaps ([fmea.md](fmea.md)), in-code deprecation markers,
reference/decision specs, and the dependency/usage review. Ordered by value.

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
