# Prepared SELECT `LIMIT ?` is ignored outside the exact-key fast path

Forge task: `t_fb3aa9d9`

Status: in progress

## Failure

`SELECT ... WHERE tenant_id = ? AND server_fingerprint = ? AND cursor_bucket = ? AND cursor > ? LIMIT ?`
returns every matching row in the partition. Replacing the final marker with the same trusted integer literal honors the limit. Ferrosa Memory PR #245 had to interpolate the literal to keep durable mobile-control replay pages bounded.

The prepared metadata and wire decoder recognize the synthetic `[limit] int` value. The exact-partition prepared fast path also consumes it. The generic fallback substitutes only `WHERE` terms, however, leaving the AST's `Limit::BindMarker` unresolved. Routing interprets a non-literal limit as absent.

## Acceptance checklist

- [x] A real PREPARE/EXECUTE regression matching the composite partition key plus `cursor > ? LIMIT ?` shape fails before the fix and returns exactly the bound count after it.
- [x] Generic SELECT substitution consumes markers in CQL order: WHERE, ANN, LIMIT.
- [x] A present LIMIT marker that is missing, null, or not a positive integer returns a typed error before routing; it never becomes an unlimited scan.
- [x] Literal LIMIT behavior and the exact-partition prepared fast path remain green.
- [x] Paging never returns more than the query limit in total and does not turn the limit into a per-page allowance.
- [x] No partition-sized collection, growing queue, or unbounded diagnostic is introduced. Each memtable/SSTable source drops the clustering prefix and stops after `page_size + 1`; later pages never retain a cumulative prefix or the query-wide LIMIT.
- [x] Source LIMIT pushdown is restricted to row-preserving projections. DISTINCT, ANN, aggregates, scalar functions, ordering, geo shapes, and static-row projections stay on their complete-input plans.
- [x] The suffix read uses the exact partition token's replica/consistency path (including NTS/LOCAL routing), never the global range-scan fan-out.
- [x] Rolling upgrades preserve the legacy `ReadRequest` bytes. Suffix reads use an additive message type, so an old peer rejects unsupported semantics instead of silently returning a prefix.
- [x] NTS/LOCAL coordination carries the selected replica identities into the read path rather than recomputing SimpleStrategy replicas from their count.
- [x] The HMAC-protected continuation binds the partition key and total LIMIT, carries the last clustering key and rows already returned, and rejects mismatched or malformed reuse before storage access.
- [ ] Focused tests, `cargo fmt --check`, clippy with warnings denied, and an independent diff review pass before the dedicated PR opens.

## Failure modes

| Failure mode | Required guard |
|---|---|
| Bound LIMIT is skipped during substitution | Rewrite it to `Limit::Literal` before routing and assert marker consumption order. |
| Missing or NULL LIMIT silently means unlimited | Return `CqlError::Invalid` before `route`. |
| Range SELECT bypasses the exact-key fast path | Exercise the cached generic EXECUTE path over a partition larger than the bound. |
| LIMIT is applied per page rather than per query | Encode and validate rows already returned in the opaque cursor; every page decrements the same total query allowance. |
| Fix materializes the partition to truncate later | Push an exclusive clustering cursor and `page_size + 1` into each point-read source, then stop decoding the SSTable tail. |
| Source truncation changes DISTINCT/ANN/aggregate output | Admit only plain row-preserving column projections to the suffix optimization. |
| Global range fan-out violates partition/NTS placement | Route through `pk_read_limited_rows_from`, which shares the keyed replica and CL path. |
| A positional bincode field breaks rolling reads | Keep `ReadRequestPayload` byte-compatible and put the suffix cursor in a versioned additive message that old peers reject. |
| NTS replica IDs collapse to an RF count | Pass the exact strategy-selected node IDs through the shared coordinator and exercise a two-DC ring where SimpleStrategy would choose the wrong DC. |
