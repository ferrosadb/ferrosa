---
crate: ferrosa-sparql
doc: roadmap
last_updated: 2026-06-19
---

# ferrosa-sparql — Roadmap

Sourced from the code (planner/executor gaps, in-code `warn!`s and fail-loud
stubs), the FMEA ([fmea.md](fmea.md)), and the dependency/usage review.

## Now (highest value)

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

- **Bound property-path cost (FMEA SP-4).** Replace the full-adjacency
  `range_read` + in-memory BFS with index-driven neighbor expansion and a
  hop/row cap; surface truncation instead of OOM risk.
- **Surface scan truncation (FMEA SP-5).** When a scan hits `SCAN_ROW_CAP`,
  return a machine-readable "results truncated" signal (or paginate) rather than
  a 200 OK with a silently partial body.
- **Join planning (FMEA SP-6).** Add pattern reordering (most-selective first)
  and a hash join to escape the nested-loop O(rows^patterns) blowup.

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
