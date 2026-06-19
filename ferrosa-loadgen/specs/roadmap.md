---
crate: ferrosa-loadgen
doc: roadmap
last_updated: 2026-06-19
---

# ferrosa-loadgen — Roadmap

Sourced from a read of the crate (no in-code `TODO`/`FIXME` markers exist today)
plus the dependency/usage review. This is a testing/tooling crate, so the
roadmap is intentionally small.

## Now (highest value)

- **Cluster-mode integrity parity.** The in-process path runs a full
  `IntegrityVerifier::verify_all` scan at the end; confirm the cluster path
  applies the same end-of-run oracle scan over CQL (re-reading every
  ground-truth key) rather than relying only on inline `record_read`
  classification. If it does not, add it — a load run that does not verify is not
  evidence.

## Next

- **Custom profiles from the CLI.** Profiles are five hard-coded constructors;
  only `--duration` and `--cache-max-bytes` are overridable. Allow ratios,
  key-space size, and worker counts to be set on the command line (or loaded from
  a file) for ad-hoc workloads without recompiling.
- **Soak failure artifacts.** On a compaction-soak mismatch, persist the failing
  corpus + seed (the work dir is currently removed unconditionally) so a failure
  can be replayed and minimized offline.

## Later

- **Tombstone/GC-grace assertions.** The oracle currently accepts either outcome
  for a deleted key during compaction (tombstone may or may not be applied yet).
  Once GC-grace semantics are pinned, tighten the deleted-key check to assert the
  expected post-grace state rather than accepting both.
- **Latency budget gates.** `LoadStats` reports HDR percentiles but nothing fails
  on them; optionally fail a run when p99 exceeds a per-profile budget so the tool
  catches latency regressions, not just correctness ones.

## Non-goals

- Distributed-systems fault injection (partitions, node kills, clock skew) — that
  is `ferrosa-jepsen`'s domain, not this crate's.
- Being a benchmark of record — this is a correctness-under-load and
  resource-leak tool first; throughput numbers are diagnostic, not certified.
