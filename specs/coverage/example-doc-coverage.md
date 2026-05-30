# Example Documentation Coverage

Audit of which user-facing, CQL-reachable features have a runnable example under
`examples/` (executed in CI and rendered to `docs/database/examples/*.html`).

Last audited: 2026-05-30 (branch `docs/hvq-vector-indexes`).

## Background

Generated example HTML drifted from its AsciiDoc sources because the
`docs-examples.yml` post-merge auto-commit to a protected `main` silently failed
(`timeseries-rrd.html` lagged its `.adoc` by several feature commits). A
pull-request drift gate now blocks merging stale HTML. See that workflow.

## Covered

Vector / ANN (HNSW + HVQ — `vector-indexes`), counters, LWT, batch, TTL, UDT,
UDF/WASM aggregates, Cypher/graph, Bolt SUBSCRIBE.

## Gaps — supported features with no runnable example

| Feature | CQL surface | Suggested home |
|---------|-------------|----------------|
| Auth / RBAC | `CREATE ROLE`, `GRANT`, `CREATE USER` | new `security-rbac` example |
| Phonetic index | `USING 'phonetic'` | extend an existing example or `secondary-indexes` |
| Filtered index | `USING 'filtered'` | `secondary-indexes` |
| Full-text index | `USING 'fulltext'` | `secondary-indexes` |
| Composite / multi-column index | `CREATE INDEX … (a, b)` | `secondary-indexes` |
| SPARQL | HTTP SPARQL endpoint (reference page exists) | new `sparql-basics` example |

## Not a gap

Materialized views — parser reports "CREATE MATERIALIZED VIEW is not yet
supported", so there is nothing to document yet.
