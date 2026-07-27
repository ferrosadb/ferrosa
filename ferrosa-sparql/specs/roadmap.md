---
crate: ferrosa-sparql
doc: roadmap
last_updated: 2026-07-25
---

# ferrosa-sparql — Roadmap

Sourced from the code (planner/executor gaps, in-code `warn!`s and fail-loud
stubs), the FMEA ([fmea.md](fmea.md)), and the dependency/usage review.

## Now (highest value)

- **Decide the graph/keyspace partition-key contract (FMEA SP-11, t_af4eb9f0).**
  `update.rs` writes the graph component as the literal `"default"`; the planner
  reads it from the keyspace. On the deployed keyspace (`rdf`) point reads miss
  data full scans find, and `DELETE` reports success while deleting nothing.
  This is a **decision**, not a patch: changing the read side is
  backward-compatible with data already on disk, changing the write side orphans
  it. Four invariant tests in `tests/sparql_executor_invariants.rs` are red
  against this and must not be weakened.
- **Authentication + authorization (FMEA SP-1).** The endpoint is fully
  unauthenticated read+write. Wire a real auth layer (token / mTLS), make
  `auth_disabled` actually gate request handling, and return
  `SparqlError::AccessDenied` (already defined) on failure. Decide whether
  `X-Keyspace` is authorized per-principal rather than client-asserted.
- **Fix OPTIONAL (FMEA SP-2).** `LeftJoin` drops its right side with only a
  `warn!`. Either implement the optional join (bind right-side vars where
  compatible, keep left rows otherwise) or fail loud — a silent incomplete
  result is the worst outcome.
- **True UNION semantics (FMEA SP-3).** Evaluate each branch independently and
  set-union the solutions instead of concatenating `TripleOp`s into one join.

## Next

- **Pipeline the multi-pattern join (FMEA SP-6).** The scan streams, but the
  nested-loop join still rebuilds a materialized solution set per pattern, so
  peak memory for a multi-pattern BGP is set by the intermediates rather than by
  the scan. They are now capped at `max_rows` and fail loud past it, so this is
  a scalability limit rather than an OOM — but a pattern that could stream
  end-to-end still cannot. Needs pattern reordering (most-selective first) plus
  either a pipelined or hash join.
- **Index-driven property-path expansion (FMEA SP-4).** The adjacency read
  streams and its buffer is bounded, but BFS still materializes every edge for
  the predicate before traversing. Expand neighbours through the index instead,
  with a hop cap.
- **Paginate instead of failing at the bound.** Crossing `max_rows` is now a
  loud error, which is correct but blunt: the right answer for a large honest
  result set is a continuation token over the streaming scan (`ScanResume` in
  `WritePath` already supports it) rather than asking the operator to raise a
  limit.

## Later

- **Full Turtle / RDF-XML serialization (FMEA SP-8).** Route CONSTRUCT/DESCRIBE
  output through `oxrdf`/`sparesults` for prefixed, grouped Turtle and correct
  RDF/XML instead of the current N-Triples-subset and ad-hoc XML.
- **RDF\* annotation evaluation (FMEA SP-7).** Requires a storage encoding for
  quoted-triple terms in key position, insert support for quoted-triple
  subjects, and a join planner that binds the inner pattern before the outer
  annotation property. Today it fails loud (correct, but unimplemented).
- **Aggregation / GROUP BY / sub-SELECT.** `GraphPattern::Group` and friends are
  not handled by `collect_ops` and currently hit the catch-all
  `unsupported graph pattern` error.
- **Named-graph support.** Lift the one-graph-per-keyspace restriction so
  `GRAPH <g> { … }`, `ADD`/`MOVE`/`COPY`, and named-graph insert/delete targets
  work instead of failing loud.

## Non-goals

- Storage durability, replication, secondary-index construction, or schema
  management — owned by `ferrosa-cluster` / `ferrosa-storage` / `ferrosa-schema`.
- Cassandra wire compatibility for RDF — this is an HTTP SPARQL endpoint, not a
  CQL surface.
