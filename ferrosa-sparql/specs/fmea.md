---
crate: ferrosa-sparql
doc: fmea
last_updated: 2026-06-19
---

# ferrosa-sparql — FMEA / Known Issues

Failure modes ranked by **RPN = Severity × Occurrence × Detection** (1–10 each;
higher = worse). This crate is a public-facing query endpoint with broad SPARQL
surface area, so both correctness-coverage and security gaps dominate.

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation / status |
|----|--------------|--------|---|---|---|-----|---------------------|
| SP-1 | **No authentication or authorization.** `SparqlError::AccessDenied` and `AppState::auth_disabled` exist but no handler ever checks credentials; `base64` is a declared-but-unused dep. `auth_disabled` only toggles health-check verbosity. | Any network client can `SELECT`, `INSERT DATA`, `DELETE WHERE`, or `DROP` against any keyspace. Full read+write data exposure. | 10 | 8 | 3 | **240** | **Open gap.** No mitigation in this crate. The `X-Keyspace` header is attacker-controlled. Must add an auth layer (token/mTLS) before any untrusted exposure. |
| SP-2 | **OPTIONAL (LeftJoin) right side silently dropped.** `collect_ops` evaluates only the left side of a `LeftJoin` and logs `warn!`; the optional pattern never binds. | `OPTIONAL { … }` returns rows missing the optional variables — a silent wrong (incomplete) result, not an error. | 8 | 6 | 7 | **336** | **Open gap.** Only a `tracing::warn!` fires; the HTTP client sees a 200 with wrong bindings. Should fail loud or implement the left-join. |
| SP-3 | **UNION is concatenation, not a true two-branch union.** Both `Union` arms append their `TripleOp`s into one flat `ops` list, then run through the same nested-loop join. | UNION of two BGPs is joined rather than unioned; cross-branch variable bindings can over- or under-constrain. Wrong result set for any non-trivial UNION. | 8 | 5 | 7 | **280** | **Open gap.** Tested only for parse/plan, not for join semantics. Needs branch-isolated evaluation then set union. |
| SP-4 | **Property paths load the entire predicate adjacency into memory.** `fetch_triples_for_predicate` does a full `range_read` of the table, filters by predicate in-process, then BFS. No bound on adjacency size; `range_read` itself is capped at 10k partitions elsewhere but not here. | A `knows+` over a large graph buffers all `knows` edges in a `Vec` and BFS over them — unbounded memory + latency; a single query can OOM or stall the node. | 7 | 5 | 5 | **175** | **Partial.** BFS has cycle detection and is correct; cost is unbounded. Needs a hop/row cap and index-driven neighbor fetch (violates Power-of-10 Rule 2/3). |
| SP-5 | **Scan results truncated at a 10k row cap, returning partial answers as success.** `SCAN_ROW_CAP = 10_000`; on `PredicateScan`/`FullScan`/ObjectScan fallback the executor logs `warn!` and returns the truncated set. | A query over >10k partitions returns an incomplete result with a 200 OK — silent under-reporting. | 7 | 4 | 7 | **196** | **Partial.** Logged, but not surfaced to the client. Should return a "results truncated" signal or paginate. |
| SP-6 | **Multi-pattern joins are O(rows^patterns) nested-loop with no index-driven join.** `evaluate_standard_op` cross-products existing bindings against every fetched row per pattern. | A 3+ pattern BGP over moderate data degrades quadratically/cubically; latency spikes, possible request timeout. | 6 | 5 | 5 | **150** | **Open gap.** Correct but not scalable. No join reordering or hash join. |
| SP-7 | **RDF\* annotation queries unsupported but parseable.** `<< s p o >> :prop ?v` parses (sparql-12) but `plan_triple_pattern` / `rdf_star.rs` reject it. | Users expecting RDF-star annotations get a 400. Feature appears available (parses) but is not. | 4 | 3 | 2 | **24** | **By design, fail-loud.** Documented in `rdf_star.rs`; returns `SparqlError::Plan`, never a silent wrong binding. Lowest-risk gap. |
| SP-8 | **Turtle output is an N-Triples subset; CONSTRUCT XML is ad-hoc RDF/XML.** `to_turtle` delegates to `to_ntriples`; CONSTRUCT XML is hand-built, not via `oxrdf`/`sparesults` serializers. | Clients requesting `text/turtle` get valid-but-unprefixed/ungrouped output; RDF/XML may mishandle literals with special structure. | 3 | 5 | 4 | **60** | **Partial.** Output is valid N-Triples (a valid Turtle subset). Full Turtle/RDF-XML deferred. |
| SP-9 | **`datatype`/`language` columns read with `from_utf8_lossy`; lossy on non-UTF-8 bytes.** Object/predicate/datatype decoding uses `String::from_utf8_lossy` throughout the executor. | A non-UTF-8 stored value is silently mangled (replacement chars) rather than erroring. | 4 | 2 | 6 | **48** | **Partial.** RDF terms are UTF-8 by construction here, so occurrence is low; still a silent-corruption path. |
| SP-10 | **`ObjectScan` index fallback can't distinguish empty index from no-index.** `fetch_by_object_index` treats an empty `index_read` as "no index" and falls back to a full scan. | If the object genuinely has no matches, the engine still pays for a full range scan; correctness OK, cost surprise. | 3 | 4 | 5 | **60** | **Partial.** Documented in code; performance-only, not a correctness bug. |

## Top risks to act on

1. **SP-2 (RPN 336) — OPTIONAL silently dropped.** The highest-RPN correctness
   bug: a common, well-formed query returns wrong (incomplete) results with a
   200. Either implement the left-join or fail loud like the other gaps.
2. **SP-1 (RPN 240) — no auth.** Severity-10 security gap: the endpoint is fully
   unauthenticated read+write across keyspaces. The `X-Keyspace` header is the
   only tenancy boundary and it is client-supplied.
3. **SP-3 (RPN 280) — UNION semantics.** Plausible queries silently get join
   instead of union semantics.

## Detection assets

- `tests/sparql_m3_completeness.rs` (14) — CONSTRUCT/DESCRIBE, INSERT WHERE,
  ORDER BY expressions, RDF\* fail-loud, XML/ASK serialization shape.
- `tests/sparql_update_pattern_delete.rs` (6) — DELETE WHERE / DELETE-INSERT,
  LOAD fail-loud.
- `tests/sparql_update_s02_mgmt.rs` (11) — CLEAR/DROP/CREATE, atomicity,
  cross-graph rejection.
- 79 in-crate unit tests (executor join/decode, planner access selection,
  property-path BFS, filter evaluation).
- **No** test today covers OPTIONAL semantics, true UNION semantics, auth, or
  property-path memory bounds — the SP-1/2/3/4 blind spots.
