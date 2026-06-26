---
crate: ferrosa-sim
doc: roadmap
last_updated: 2026-06-19
---

# ferrosa-sim — Roadmap

Sourced from the in-code module docs (each carries its Sprint 5/7/8 work-item
provenance), the dependency/usage review, and the "what is modelled vs. not"
section of [overview.md](overview.md). There are no `TODO`/`FIXME` markers in the
source.

## Now (highest value)

- **Close the mirror-drift gap.** `deployment::DeploymentMode`,
  `bootstrap::BootstrapPhase`, and `multi_dc`'s apply types are hand-copies of
  `ferrosa-cluster` shapes kept in lock-step *manually*. Nothing fails loudly if
  the originals change. Add a compile-time or test-time assertion that pins each
  mirror to its source (e.g. a `ferrosa-cluster` test that imports the sim enum,
  or a shared definition lifted into `ferrosa-common` — the `deployment` module
  doc already floats this).

## Next

- **Model AppendEntries log replication.** Today `SimulatedNode::log_len` is a
  counter and `commit_index` is never advanced by the loop, so `LogMatching`,
  `LeaderCompleteness`, and the non-degenerate `StateMachineSafety` /
  `LeaderAppendOnly` invariants can only be checked in snapshot/degenerate form.
  Extend the cluster to a real `Vec<LogEntry>` so the spec's replication
  invariants get exercised end-to-end.
- **Wire the Apalache path.** The refinement check is currently a Rust
  re-implementation of `specs/tla/raft.tla`. Add the optional
  `apalache-mc check --simulate` export (the `refinement` module doc describes
  the intended `apalache.json` hand-off) so the canonical model checker and the
  Rust interpreter are cross-validated.

## Later

- **Expand the nemesis catalogue.** ADR-017 lists 19 nemeses; three are
  implemented (`PartitionHalves`, `KillMinority`, `AddNode`). Add the rest
  (clock skew, message duplication/delay, asymmetric partitions) as the Raft
  model grows.
- **Drive nemeses from the event loop.** The `Nemesis` trait is applied manually
  between `run_for` calls in tests; a scheduled nemesis injector keyed off the
  seed would let the nightly sweep explore fault timing automatically.
- **Property-test the determinism contract** with `proptest` (already a
  dev-dependency) over random seeds and voter counts, beyond the current
  hand-picked `same_seed`/`different_seed` cases.

## Non-goals

- Running the real `FerrosRaft` (sled, networking, schema replay) — that is the
  job of the in-process harness in `ferrosa-cluster/tests/`, not this crate.
- Depending on any other ferrosa crate — ADR-017's in-house choice is load-bearing;
  the mirror-drift work above is the price of keeping this boundary.
