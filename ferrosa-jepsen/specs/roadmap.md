---
crate: ferrosa-jepsen
doc: roadmap
last_updated: 2026-07-16
---

# ferrosa-jepsen — Roadmap

Sourced from in-code TODOs/stubs, the wired-vs-aspirational gaps found in the
run path, and the dependency/usage review. The harness's correctness logic
(linearizability + membership checkers, registries, the sim endurance run) is
implemented and tested; the gaps are mostly in cluster-backend wiring and the
external checkers.

## Now (highest value)

- **Wire (or remove) Firecracker provisioning.** `firecracker.rs` and
  `cluster.rs` (microVM provisioning) are still unreachable; `cluster.rs`
  carries a `// TODO: SSH into each node, run setup-guest.sh, start ferrosa`.
  T3 now accepts externally provisioned contacts and fails loud without them,
  and a Fly caller can select Fly-SSH WAN faults; what remains is automated
  Firecracker lifecycle ownership.
- **Implement the `report` CLI subcommands.** `report list`/`compare`/`render`
  log "not yet implemented" (`main.rs`) despite `report/{comparison,timeline,
  anomaly}` modules existing. Finish wiring them to the archive.

## Next

- **Decide Elle's fate.** `checker/elle.rs` types exist but `UnifiedChecker`
  hard-codes `elle_result = None`. Either wire Elle against a running Jepsen/lein
  cluster or document it as explicitly out of scope so it isn't mistaken for an
  active checker.
- **Exercise the container drivers in a tier.** `phase2` registers
  Python/Go/Node/Java/C# `ContainerDriver`s, but `resolve_driver_registry`
  returns `phase1` (Rust only) for every tier. Add a tier (or flag) that runs
  the polyglot drivers so the per-language Docker images are actually tested.
- **Promote the linearizability search bound to a config.** `SEARCH_LIMIT` is a
  hard-coded 100k-node cap; a long history that exceeds it is reported as
  non-linearizable with a node-count explanation. Make the bound configurable
  and distinguish "no linearization" from "search exhausted".

## Later

- **Automate real multi-DC (T3/T4) endurance on Fly.io.** The run path now
  accepts a caller-provisioned Fly topology and executes WAN faults over Fly
  SSH, but it does not create/destroy the machine fleet itself. Add an explicit
  cost-gated launcher and run the real 24h tier alongside the sim gate.
- **Broaden checker models.** The native checker models a single-value register
  (read/write/CAS/serial-read); the `bank` and LWT workloads rely on
  workload-specific invariants. Consider a list/set/transactional model for
  richer anomaly detection.

## Non-goals

- Database internals — this crate drives a cluster over CQL/SSH and the
  simulator; it does not own storage, consensus, or query execution.
- Replacing `ferrosa-sim` — the simulator is the modeling layer; this crate
  consumes it for the endurance path.
