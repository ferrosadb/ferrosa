import Ferrosa

/-!
# What the specification does not cover

These claims from ferrosa.ai have no theorem behind them yet. Some are measurements, which a proof
cannot establish; the rest are behaviour the specification has not modelled. Until a claim has a
theorem, its evidence is the test suite and the published benchmarks, not this site.

## Behaviour that is not yet specified

Each item is the claim, then what a specification would have to prove to back it.

- **Losing a node is a cache miss, not a restore.** Every acknowledged row reaches object storage and
  is published by the manifest, and a node rebuilt from the bucket serves every acknowledged row.
- **NVMe table pinning.** A pinned table's acknowledged rows survive what its sync mode promises, and
  unpinning a table loses nothing.
- **Vector indexes (HNSW, IVFFlat, HVQ).** What an approximate nearest-neighbour query may omit, and
  that a filtered query never returns a row outside its filter.
- **Full-text search with BM25 and a pluggable analyzer.** Query terms and indexed terms pass through
  the same analyzer, so a match is never lost to analysis.
- **Phonetic, B-tree, hash, composite and filtered indexes.** Each index answers exactly the rows a
  table scan with the same predicate would.
- **Cypher queries and graph traversals.** Pattern-matching semantics, including variable-length
  paths and `LIMIT` push-down. Only the Bolt session is modelled today.
- **Hinted handoff.** A hint takes effect at most once, and a write outside the hint window is
  reported for repair rather than dropped.
- **Anti-entropy repair.** After repair, replicas agree on every row whose newest write predates it.
- **Operator-gated join, decommission and token rebalancing.** A key stays readable at its level
  throughout a membership change.
- **Raft for cluster metadata.** `Ferrosa.TlaCheck` audits the existing TLA+ model and finds
  invariants that are tautologies; nothing models the openraft integration itself.
- **Sandboxed WebAssembly functions.** A function cannot reach outside its sandbox, and exceeding a
  memory or CPU limit fails the query rather than the node.
- **Memtable backpressure.** Resident memtable bytes never exceed the configured budget plus one write.
- **Lock-free reads.** No reader waits on a writer on the read path.
- **Observability, Prometheus metrics and traces.** Outside a behavioural specification.

## Measurements

Latency (no GC pauses, P99), throughput, the Fly write-path A/B, HVQ's bytes-per-query and speed-up,
recovery cost and storage cost are measurements. They belong to benchmarks with stated workloads, and
the specification does not, and cannot, prove them.
-/
