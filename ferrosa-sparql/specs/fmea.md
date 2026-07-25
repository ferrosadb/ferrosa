---
crate: ferrosa-sparql
doc: fmea
last_updated: 2026-07-25
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
| SP-4 | **Property-path BFS buffers the predicate adjacency.** `fetch_triples_for_predicate` now STREAMS the table (`range_read_stream_all`) and keeps only matching `(subject, object)` pairs, but BFS is a blocking operator so the adjacency list itself is still buffered. | A `knows+` over a very large graph buffers all `knows` edges before traversing. Memory is now capped, not unbounded. | 5 | 4 | 3 | **60** | **Mitigated.** The read streams; the adjacency buffer is bounded by `SparqlConfig::max_rows` and exceeding it returns `SparqlError::Execution` rather than traversing a partial graph (a partial adjacency would give a wrong reachability answer that looks complete). Index-driven neighbour expansion is still the right long-term fix. |
| SP-5 | ~~**Scan results truncated at a 10k row cap.**~~ The `SCAN_ROW_CAP = 10_000` constant truncated **nothing**: both uses were a `tracing::warn!` fired *after* `range_read` had already materialized the whole table, and the warning text ("scan results truncated at row cap") was false. `SparqlConfig::max_results` was likewise dead — zero reads workspace-wide. | Unbounded materialization of the full table (the real OOM), plus a log line asserting a truncation that never happened. | 8 | 6 | 8 | **384** | **FIXED.** Scans stream via `range_read_stream_all`; both fictions are deleted and replaced by one real bound, `SparqlConfig::max_rows`, enforced at the source and on every operator buffer. Crossing it returns `SparqlError::Execution` — never a silent truncation. `LIMIT` is pushed into the scan when provably safe. Covered by `tests/sparql_scan_bound_invariants.rs`. |
| SP-6 | **Multi-pattern joins are O(rows^patterns) nested-loop with no index-driven join.** `evaluate_standard_op` cross-products existing bindings against every fetched row per pattern. | A 3+ pattern BGP over moderate data degrades quadratically/cubically; latency spikes, possible request timeout. | 6 | 5 | 5 | **150** | **Open gap.** Correct but not scalable. No join reordering or hash join. |
| SP-7 | **RDF\* annotation queries unsupported but parseable.** `<< s p o >> :prop ?v` parses (sparql-12) but `plan_triple_pattern` / `rdf_star.rs` reject it. | Users expecting RDF-star annotations get a 400. Feature appears available (parses) but is not. | 4 | 3 | 2 | **24** | **By design, fail-loud.** Documented in `rdf_star.rs`; returns `SparqlError::Plan`, never a silent wrong binding. Lowest-risk gap. |
| SP-8 | **Turtle output is an N-Triples subset; CONSTRUCT XML is ad-hoc RDF/XML.** `to_turtle` delegates to `to_ntriples`; CONSTRUCT XML is hand-built, not via `oxrdf`/`sparesults` serializers. | Clients requesting `text/turtle` get valid-but-unprefixed/ungrouped output; RDF/XML may mishandle literals with special structure. | 3 | 5 | 4 | **60** | **Partial.** Output is valid N-Triples (a valid Turtle subset). Full Turtle/RDF-XML deferred. |
| SP-9 | **`datatype`/`language` columns read with `from_utf8_lossy`; lossy on non-UTF-8 bytes.** Object/predicate/datatype decoding uses `String::from_utf8_lossy` throughout the executor. | A non-UTF-8 stored value is silently mangled (replacement chars) rather than erroring. | 4 | 2 | 6 | **48** | **Partial.** RDF terms are UTF-8 by construction here, so occurrence is low; still a silent-corruption path. |
| SP-10 | **`ObjectScan` index fallback can't distinguish empty index from no-index.** `fetch_by_object_index` treats an empty `index_read` as "no index" and falls back to a full scan. | If the object genuinely has no matches, the engine still pays for a full range scan; correctness OK, cost surprise. | 3 | 4 | 5 | **60** | **Partial.** Documented in code; performance-only, not a correctness bug. |
| SP-11 | **Writes and reads disagree about the graph partition-key component (t_af4eb9f0).** `update.rs` maps `GraphName::DefaultGraph` to the literal string `"default"` when building the partition key, while the planner sets the graph component from the KEYSPACE. On any keyspace other than `default` — **including `rdf`, the keyspace the HTTP endpoint defaults to** — a point read computes a key that no row was written under. | A `SubjectLookup` returns nothing for data a full scan finds, so two access paths disagree about whether a triple exists. `DELETE` reports a non-zero `triples_deleted` while tombstoning keys that hold nothing, so the data survives — a silent data-integrity lie. | 9 | 8 | 4 | **288** | **OPEN — needs a decision, not a patch.** Fixing the read side (use `"default"`) is backward-compatible with existing data; fixing the write side (use the keyspace) orphans every row already written. Someone must decide whether the default graph is named by the keyspace or by the constant. Four failing tests in `tests/sparql_executor_invariants.rs` pin the invariants: `constant_subject_matches_exactly_that_subjects_triples`, `constant_subject_and_object_are_both_enforced`, `point_lookup_and_full_scan_agree_on_existence`, `reported_delete_actually_removes_the_data`. The existing behavioural suites hide this by pinning `KS = "default"`. |
| SP-12 | ~~**Constant terms in a triple pattern were never enforced (t_c3a2d3e4).**~~ `try_bind_triple` handled only the `Variable` arms, so a constant in a position the access path was not chosen on was silently dropped: `?s <ex:status> "active"` planned a `PredicateScan` and returned subjects with ANY status. Separately, the planner keyed `ObjectScan` on `Literal::to_string()` — the quoted N-Triples form `"Alice"` — against the bare stored lexical `Alice`, so a literal-object pattern matched nothing. | Silent wrong results in both directions: too many rows (dropped constant) and zero rows (quoted literal). | 8 | 7 | 8 | **448** | **FIXED.** Constants are enforced in all three positions, with term-kind checking (an IRI constant no longer matches a literal spelling the same characters), and the planner keys `ObjectScan` on `Literal::value()`. Covered by the I1 invariants in `tests/sparql_executor_invariants.rs`. |
| SP-13 | ~~**`OFFSET k LIMIT n` computed `k + n` as an unchecked `usize` add.**~~ Both values come straight from attacker-controlled query text. `OFFSET 1 LIMIT 18446744073709551615` panicked in debug and wrapped to 0 in release, silently returning no rows. | Debug panic / release silent-empty from a 60-byte query. | 7 | 5 | 6 | **210** | **FIXED** with `saturating_add`. This was a prerequisite for streaming: the accidental `.min(len)` clamp that masked the wrap only worked because a materialized `Vec` had a `len()` to clamp against. Covered by `limit_and_offset_never_overflow_for_any_usize`. |

## Top risks to act on

1. **SP-11 (RPN 288) — read/write graph-key mismatch.** The highest open
   correctness risk, and it fires on the DEPLOYED default keyspace (`rdf`).
   Point reads miss data that scans find, and `DELETE` reports success while
   deleting nothing. Needs an owner decision on which side to change.
2. **SP-2 (RPN 336) — OPTIONAL silently dropped.** A common, well-formed query
   returns wrong (incomplete) results with a 200. Either implement the left-join
   or fail loud like the other gaps.
3. **SP-1 (RPN 240) — no auth.** Severity-10 security gap: the endpoint is fully
   unauthenticated read+write across keyspaces. The `X-Keyspace` header is the
   only tenancy boundary and it is client-supplied.
4. **SP-3 (RPN 280) — UNION semantics.** Plausible queries silently get join
   instead of union semantics.

Resolved this cycle: **SP-5** (fictional scan cap → real streaming + fail-loud
bound), **SP-12** (constant terms never enforced), **SP-13** (LIMIT/OFFSET
overflow). **SP-4** downgraded from unbounded to bounded.

## Detection assets

- `tests/sparql_m3_completeness.rs` (14) — CONSTRUCT/DESCRIBE, INSERT WHERE,
  ORDER BY expressions, RDF\* fail-loud, XML/ASK serialization shape.
- `tests/sparql_update_pattern_delete.rs` (6) — DELETE WHERE / DELETE-INSERT,
  LOAD fail-loud.
- `tests/sparql_update_s02_mgmt.rs` (11) — CLEAR/DROP/CREATE, atomicity,
  cross-graph rejection.
- `tests/sparql_executor_invariants.rs` (21) — the first END-TO-END tests of
  `executor::execute`. Constant-term enforcement in every position, read-path
  agreement, delete honesty, LIMIT/OFFSET totality, DISTINCT exactness. Run on
  the DEPLOYED keyspace (`rdf`), not on `default`, so SP-11 is visible rather
  than hidden. **4 fail today** — that is SP-11, deliberately not weakened.
- `tests/sparql_scan_bound_invariants.rs` (10) — completeness and boundedness:
  a scan past the bound errors instead of truncating, a scan within it returns
  the complete result, `LIMIT n` stops early, and blocking operators
  (ORDER BY / DISTINCT) and `FILTER` correctly refuse the pushdown.
- 83 in-crate unit tests (executor join/decode/filter, planner access selection,
  property-path BFS, filter evaluation), including a source tripwire that fails
  the build if the `Vec`-returning `WritePath::range_read` is reintroduced.
- **No** test today covers OPTIONAL semantics, true UNION semantics, or auth —
  the SP-1/2/3 blind spots.
