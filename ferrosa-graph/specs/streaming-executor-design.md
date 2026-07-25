---
title: Graph streaming (pull-based) executor — design + increment plan
status: Proposed
last_revised: 2026-07-25
executive_summary: >
  The ferrosa-graph query executor materializes its full result set (and full
  per-hop fan-out) into Vecs, then applies ORDER BY / DISTINCT / LIMIT by
  truncation — the OOM risk for unbounded / high-degree-hub queries (t_4ce82a3e).
  This spec designs the replacement: a pull-based (Volcano) model where each
  operator is a lazy `RowStream`, LIMIT short-circuits upstream everywhere, and
  per-hop hydration runs at bounded concurrency WITHOUT cloning WritePath /
  Schema / Hop / Row per neighbor (defeating the documented "Send is not general
  enough" wall via inline-driven `FuturesUnordered` of borrowed futures, not
  `buffer_unordered`'s `'static` clones). Rows stream to the transport (HTTP
  chunked, Bolt incremental PULL) so an unbounded hub query cannot OOM. Delivered
  as 7 behavior-preserving increments, each keeping the existing ~95 integration
  tests green.
---

# Graph streaming (pull-based) executor — design + increment plan

> Task: `t_4ce82a3e`. Owner requirement (Ben, 2026-07-08): a FULL streaming fix —
> no per-neighbor data clones, no materialized result sets. Do not hand-build
> clone/concurrency hacks in the current loop.

## 1. Current model and why it must change

`executor::execute()` (`expand.rs:115`) dispatches to `execute_*` fns that each
build a full `GraphResult { columns, rows: Vec<Vec<Value>>, stats }`
(`result.rs:1`). The spine is **materialize-then-truncate**:

- The frontier is a `Vec<ExpandState>` (`expand.rs:33`, `:1061`); each hop reads
  every frontier vertex's adjacency, collects `pairs`/`next_states` Vecs
  (`:1164-1262`), hydrates *every* neighbor, then swaps into `current_states`.
- Result rows are built into a `rows` Vec (`:1328-1354`); ORDER BY
  (`sort_projected_rows_by_bindings` `:1358`), DISTINCT (sort+dedup `:1361`),
  and LIMIT (`rows.truncate` `:1368`) are all **post-hoc Vec operations**.
- The one exception is the last-hop LIMIT push-down (bc098906, `:1141-1260`),
  which stops hydrating once `limit` rows accumulate — but only when
  `order_by.is_empty() && !distinct && with_pipeline.is_none() &&
  post_filters.is_empty() && optional_hops.is_empty()` and only on the final hop.

Two consequences: (a) an unbounded / high-degree-hub query holds the entire
result and full fan-out in memory → OOM; (b) concurrent per-hop hydration is
blocked by the **Send wall** (`:1220-1227`): `hydrate_hop_neighbor` (`:2595`)
borrows `&WritePath / &Schema / &Hop / &ExpandState / &Row` (the last two alias
the frontier Vec being iterated), so a `buffer_unordered` on the multi-threaded
runtime would need `Send + 'static` futures — achievable only by cloning that
data per neighbor, which the task rejects. The loop is therefore **sequential**.

No query-path row sink exists: HTTP returns `Json(result)` (`http.rs:286`) and
Bolt buffers the whole result in `pending_result`, replying to PULL with all
rows at once and ignoring `n`/`has_more` (`bolt/server.rs:391,413`). The
SUBSCRIBE SSE path (`http.rs:456-560`, an mpsc → `ReceiverStream` → `Sse`) is the
only streaming sink and is the reusable template.

## 2. Target model — pull-based Volcano operators

Every operator is a lazy asynchronous row source:

```rust
/// A pull-based row source. `next()` yields the next row or None at end;
/// dropping the stream stops all upstream work (the LIMIT short-circuit).
type RowStream<'a> = Pin<Box<dyn Stream<Item = Result<RowVals>> + Send + 'a>>;
type RowVals = Vec<serde_json::Value>;
```

Operators compose as a tree; the transport pulls the root. Two operator classes:

- **Pipelineable (stream-through, O(1) memory):** AnchorScan, Expand(hop),
  OptionalExpand, Filter, Project, Unwind, Limit, Union(all). These hold no
  result buffer; they transform/forward one row (or one frontier element) at a
  time.
- **Pipeline-breakers:** OrderBy, Distinct, Aggregate, VarPath, WcoJoin. These
  must see multiple/all inputs. Rather than capping them in memory, they follow
  the approach **ferrosa-cql already proved for the same problem**:
  - **ORDER BY + LIMIT k → bounded top-k** (a k-sized heap), not a full sort.
  - **ORDER BY without LIMIT → SPILL, don't cap.** Reuse
    [`ferrosa_storage::ExternalSorter`] (`ferrosa-storage/src/external_sort.rs`):
    accumulate to a byte threshold, spill sorted runs to a temp dir, k-way merge
    on `finish()`, fail loud on any spill/merge I/O error. The temp dir is a
    [`TempSortTableReservation`] whose `Drop` does `remove_dir_all`, so a
    cancelled or aborted query cleans up automatically — the cancel-friendly
    property this executor needs, for free. The CQL side already classifies this
    shape as `OrderByExecutionPlan::SpillableTempTable { estimated_scan_bytes }`
    (`ferrosa-cql/src/router.rs`); the graph planner should classify the same way.
    *Gap to close:* `ExternalSorter` sorts `Row`/`CqlValue` while graph rows are
    `Vec<serde_json::Value>`, so this needs either a value bridge or a small
    generic-ification of the sorter — the spill/merge/cleanup machinery itself is
    reused untouched.
  - **DISTINCT** emits each row the first time it is seen (streaming dedup). Its
    seen-set is subject to the same argument: a spilling/sort-based dedup rather
    than an unbounded in-memory `HashSet`.
  - **Aggregate** stays bounded by `max_groups`; a spilling group-by is a later
    option if that cap proves too restrictive in practice.
  - VarPath / WcoJoin keep their existing `max_var_path_visited` / `max_results`
    caps (they bound *traversal*, not result buffering).

  The principle: an in-memory cap that fails a legitimate query is a worse answer
  than spilling, when the spill machinery already exists and is cancel-safe.

`Limit(k)` is `upstream.take(k)`: when the transport (or an outer Limit) stops
pulling, `take` drops the upstream stream, which drops the Expand operator's
in-flight hydration — LIMIT short-circuits **everywhere**, for free, not just the
last hop. This subsumes and generalizes bc098906.

## 3. Defeating the Send wall without clones

The wall is `buffer_unordered`/`buffered` requiring `Send + 'static`. The fix is
**inline-driven `FuturesUnordered`** inside the Expand operator's `poll_next`:

- The operator OWNS the shared read context as `Arc` handles
  (`Arc<WritePath>`, `Arc<Schema>`) — an `Arc::clone` is a refcount bump, NOT the
  per-neighbor DATA clone the task forbids (`Hop`, `Row`, `bindings` are never
  cloned into the future; `Hop` is borrowed from the operator, the neighbor id is
  the only owned input, exactly as `hydrate_hop_neighbor` already takes it).
- `FuturesUnordered<F>` driven by the operator's own `poll_next` (not
  `tokio::spawn`) does **not** require `F: 'static` — it polls the futures in
  place, so `F` may borrow from the operator (`&self.hop`, `&Arc<WritePath>`).
  Bounded concurrency = keep at most `N` futures in the set (push a new neighbor
  future each time one completes), yielding hydrated rows as they resolve.
- This gives concurrent hydration with a fixed in-flight budget, zero data
  clones, and no `'static` requirement — the exact shape the task calls for.

`hydrate_hop_neighbor` (`:2595`) is reused verbatim as the per-neighbor future
body (it already borrows everything and owns only `neighbor_id`).

## 4. Streaming transport

A row sink is added at the `execute` boundary. `execute()` gains a streaming
sibling returning `(columns, RowStream, StatsHandle)`; the buffered `GraphResult`
is kept for internal callers (CALL join, aggregate inner, tests) by `collect`ing
the stream — behavior-identical.

- **HTTP:** replace `Json(result)` (`http.rs:286`) with a chunked
  `application/x-ndjson` (or a JSON array streamed element-by-element) body over
  the SUBSCRIBE mpsc/`ReceiverStream` template (`:456-560`). Header row first,
  then one row per chunk, then a trailing stats object.
- **Bolt:** honor `Pull { n, qid }` (`server.rs:408`): pull `n` rows from the
  `RowStream`, reply `Record` per row, then SUCCESS with `has_more` when the
  stream is not exhausted — incremental paging instead of the whole-result batch.

## 5. Increment plan (each behavior-preserving; existing tests stay green)

1. **RowStream plumbing (no behavior change).** Introduce `RowStream` +
   `collect_to_graph_result`. `execute()` keeps returning `GraphResult` (built by
   collecting an internally-produced stream for one simple path). Pure scaffold.
2. **Streaming AnchorScan + Project + Limit** for the anchor-only / no-hop path
   (`execute_return_only`, virtual-anchor). LIMIT short-circuits; O(1) memory.
3. **Streaming single-hop Expand** with inline `FuturesUnordered` bounded
   hydration (§3) + LIMIT short-circuit across the hop. This is the core win and
   must keep `http_limit_short_circuits_expansion_hydration`'s `vertices_read`
   bound (`tests:6615`).
4. **Multi-hop Expand** — pull chains hop→hop; LIMIT/`take` propagates upstream.
5. **Pipeline-breakers:** OrderBy (top-k on LIMIT; SPILLING external sort via
   `ferrosa_storage::ExternalSorter` + `TempSortTableReservation` otherwise),
   streaming/spilling Distinct, Aggregate (bounded groups). Unwind,
   OPTIONAL MATCH, WITH-pipeline as streaming stages.
6. **VarPath + Leapfrog** streaming (bounded by their visited/result caps).
7. **Transport streaming:** HTTP chunked body + Bolt incremental PULL honoring
   `n`/`has_more`. Only here does the full result stop being buffered end-to-end.

Increments 1–4 deliver the headline OOM/LIMIT win for the common
expand-with-limit shape; 5–7 complete coverage and end-to-end streaming.

## 6. Verification

- The ~95 `graph_http_integration.rs` tests are the behavior oracle; run the full
  suite after every increment. Key guards: `http_limit_short_circuits_expansion_hydration`
  (LIMIT bounds `vertices_read`), DISTINCT/ORDER BY/LIMIT pipeline tests, aggregate
  semantics, `varpath_budget_exceeded` + `leapfrog_join_respects_max_results`
  (the memory/DoS caps must not regress).
- Add: a high-degree-hub streaming test asserting bounded peak `vertices_read`
  (and, once transport streams, bounded response buffering) for
  `MATCH (h)<-[r]-(n) RETURN n LIMIT k` where the hub degree ≫ k.
- `cargo test -p ferrosa-graph` + clippy `-D warnings` green each increment.

## 6b. Materialization inventory

`frg materialization-scan ferrosa-graph/src` reports 45 findings; most are
**false positives** — Vecs sized by the *query* or *schema* (parser items,
planner hops, merge column shapes, `column_names_for_table`, param binding), not
by the data. The ones that actually scale with graph data:

| Site | Holds | Bound today | Retired by |
|---|---|---|---|
| `expand.rs:1079` anchor `states` | every anchor partition (**full table scan**) | none | inc 2 — use `WritePath::range_read_stream_all*` (already exists) |
| `expand.rs:1225` `pairs` collect | one vertex's full adjacency (**hub degree**) | `max_fan_out_per_hop` | inc 3/4 (concurrency+early-stop done; still collects) |
| `expand.rs:1384-85` `rows`/`result_states` | **entire result set** | `max_result_rows` (silent truncate — see §7) | inc 7 (transport) |
| `expand.rs:1328` `kept` (post-filters) | full frontier | none | inc 5 |
| `expand.rs:471` `next_frontier` (pattern predicate) | BFS frontier | none | inc 5 |
| `expand.rs:1857` edge-anchored `states` | edge-anchor scan | none | inc 2 |
| `varpath.rs` frontier / `result_keys` / `visited` | BFS frontier + visited set | `max_var_path_visited` | inc 6 |
| `leapfrog.rs` `result_rows` / `AdjacencyIterator` | join output + sorted adjacency | `max_results` | inc 6 |
| `engine.rs:910-915` CALL subquery `out_rows` | **outer result + every inner result** (nested-loop join) | none | inc 5 |
| `http.rs:510` SUBSCRIBE diff | **two full result sets** (previous + current) | none | see §7 — needs a design decision |
| `stream.rs:69/91` collect bridges | migration scaffolding | caller's | inc 7 (they disappear) |

Note the storage layer already went through this discipline and has a source
tripwire enforcing it (`ferrosa-cluster/src/write_path.rs` test: *"unbounded local
range reads must be exposed as streams, not collected into `Vec<Partition>`"*).
The graph executor should end up subject to an equivalent guard.

## 7. Non-goals / risks

- Not changing query semantics, result ordering, or the traversal DoS caps
  (`max_fan_out_per_hop`, `max_var_path_visited`, `max_results`).
- ORDER BY without LIMIT and global Aggregate are pipeline-breakers — they must
  see their whole input. Streaming does not remove that, but per §2 the answer is
  to **spill** (reusing the CQL `ExternalSorter` + `TempSortTableReservation`
  RAII cleanup), not to cap in memory and fail a legitimate query.
- The recursive `Box::pin(execute(...))` children (Union/Subscribe/Set/Remove/
  Delete/Aggregate inner) each currently await a full child `GraphResult`;
  increment 5+ converts them to consume the child `RowStream`.
