---
title: "Graph: hydrate opposite endpoints for label-agnostic relationship expansion"
status: implemented
crate: ferrosa-graph
task: t_8c506227
date: 2026-07-08
executive_summary: >
  Ferrosa's Cypher engine hydrates the opposite endpoint node only when the query
  labels it (typed traversal `(a:Actor)-[r:DONATED_TO]->(c:Committee)`). A
  label-agnostic expansion `(c {id:…})-[r]->(n)` returns the relationship's raw
  adjacency internals (`_src`, `_dst`, `col_0`) but leaves `n` un-hydrated (null),
  blocking a generic D3 recenter / n+2 explorer from using Ferrosa directly. Root
  cause: the physical planner leaves `hop.vertex_table` (and `edge_table`/`edge_label`)
  `None` for unlabeled patterns, so the executor's vertex-hydration call is gated off.
  Fix: at execution, resolve each adjacency row's edge table (already recorded in the
  row), read its source/target label metadata, resolve the opposite vertex table
  direction-aware, and hydrate the neighbor through the existing `find_vertex_match`
  path — hydrating edge properties too. Fail loud (not null) when an edge table lacks
  valid source/target metadata, per the task's acceptance criteria.
---

# Graph — hydrate opposite endpoints for label-agnostic relationship expansion

## 1. Problem (current behavior)

The property-graph engine over CQL edge/vertex tables supports **typed** traversals
but not **label-agnostic** expansion of the opposite endpoint.

Works — the query labels the neighbor, so the planner can resolve its table:

```cypher
MATCH (a:Actor)-[r:DONATED_TO]->(c:Committee {filer_id: '211776936'})
RETURN a.name_raw, r, c.name
```

Broken — the neighbor `(n)` and the relationship `[r]` are unlabeled:

```cypher
MATCH (c:Committee {id: 'f680…'})-[r]->(n)  RETURN c.name, r, n.id   -- n.id is NULL
MATCH (c:Committee {id: 'f680…'})<-[r]-(n)  RETURN c.name, r, n.id   -- n.id is NULL
```

Returned rows carry `r._src`, `r._dst`, and `r.col_0` (e.g. `sf_influence_query.donated_to`)
but `n.id` is null; the incoming form's endpoint reporting is also surprising.

**Impact:** a generic explorer cannot issue a single label-agnostic 1-hop / 2-hop
expansion from an arbitrary node and render the neighbors; sf-elections must keep an
app-side SQLite adjacency projection for recenter.

## 2. Root cause (grounded, `ferrosa-graph`)

| Stage | Location | Behavior for unlabeled `-[r]->(n)` |
|---|---|---|
| Physical plan | `planner/physical.rs:1440-1446` | `edge_label = None`, `edge_table = None` (no `rel_type`). |
| Physical plan | `planner/physical.rs:1451-1465` | `vertex_table` resolves **only** if the neighbor has a label or var binding. Unlabeled `(n)` → `vertex_table = None`. |
| Execute | `executor/expand.rs:1214-1231` | Vertex hydration is gated on `if let (Some(vertex_tid), Some(meta)) = …`. With `vertex_table = None` the block is skipped → `neighbor_json = None`. |
| Execute | `executor/expand.rs:1243-1245` | Neighbor becomes a bare hex-id string only. |
| Execute | `executor/expand.rs:1891-1949` (`edge_binding_json`) | With `edge_match = None`, `r` falls back to raw adjacency cells → `_id/_src/_dst/_type` + `col_0` (the edge-table name), no real edge props. |

The typed path works because the neighbor's **label comes from the query**
(`(c:Committee)`), which lets the planner resolve `vertex_table`, which enables
`find_vertex_match` (`expand.rs:2268-2330`) → `row_to_json` (`expand.rs:1963+`).

**What the executor already has at expansion time but the unlabeled path ignores:**
each adjacency row records the originating **edge table** (`adjacency/observer.rs`,
surfaced as `col_0`), and the neighbor id (`_src`/`_dst`). That is enough to resolve
the opposite vertex table — *if the edge table declares its endpoint labels*.

### Metadata reality (important correction)

Real edge tables carry `graph.type=edge`, `graph.label`, `graph.source` /
`graph.target` (the **column names** holding the endpoint ids). The endpoint
**labels** `graph.source_label` / `graph.target_label` are **read** by the executor in
9 non-test spots (always via graceful `if let Some(...)`) but are currently **set only
in test fixtures** (`engine.rs:2718/2966`). So generic hydration depends on edge
tables declaring their source/target labels; where they do not, the engine must fail
loud rather than silently return null (task acceptance #4).

## 3. Decision record

- **D1 — Resolve the opposite vertex table at execution, per adjacency row, from the
  edge table's endpoint-label metadata.** Not at plan time: an unlabeled pattern can
  span multiple edge tables (`donated_to`, `owed_to`, …), and only the adjacency row
  knows which edge produced a given neighbor. Reuse `resolve_table_by_graph_label`
  (`expand.rs:2482-2496`) → `find_vertex_match` → `row_to_json`. *Rejected alt:*
  scan every vertex table for the neighbor id (ambiguous across labels, O(V) per hop);
  *rejected alt:* infer the table from a global id→table index (does not exist).
- **D2 — Direction-aware endpoint selection.** For `-[r]->` (OUT) the neighbor is the
  edge's **target** vertex (`graph.target_label`); for `<-[r]-` (IN) it is the
  **source** vertex (`graph.source_label`); `Direction::Both` tries the far end per
  the adjacency row's stored direction. `_src`/`_dst` must reflect true edge
  orientation regardless of query direction (fixes the "surprising incoming" report).
- **D3 — Fail loud on missing/invalid endpoint metadata.** If a matched edge table
  lacks a usable `graph.source_label`/`graph.target_label` (or the label resolves to
  no vertex table, or the vertex row is unreadable), the query returns a **clear
  error** naming the edge table and missing extension — never a null endpoint. This is
  acceptance #4 and matches the fail-loud rule.
- **D4 — Hydrate `r` too.** When the edge table is resolved, load the edge row and
  emit real relationship properties (via the existing edge-hydration path) instead of
  the `col_N` fallback — so `RETURN r` matches the typed traversal's shape.
- **D5 — Typed path unchanged.** The new resolution only engages when
  `hop.vertex_table` (and `hop.edge_table`) are `None`; labeled/typed patterns keep
  their existing, test-covered behavior byte-for-byte.

**RESOLVED (2026-07-08).** The loaded `sf_influence_query` edge tables **do**
declare `graph.source_label`/`graph.target_label`, verified by running the fixed
build against the live graph on the local sf node (`OWED_TO`/`DONATED_TO`
neighbors hydrate with non-null ids + full edge props, both directions). The
executor fix works on the existing data with **no re-ingest**. Moreover the
write-side guarantee is **already enforced**: `ferrosa-schema` (`registry.rs`)
rejects any `graph.type=edge` table lacking `graph.source_label`/`target_label`
or whose labels don't reference existing vertex tables — so the metadata this
relies on cannot be absent for a normally-created edge, and the query-time
fail-loud is defense-in-depth.

**Implementation note.** The fix resolves the edge and the opposite vertex
**independently**, so it covers all four label combinations, not just the fully
unlabeled one: `-[r]->(n)`, `<-[r]-(n)`, `-[r:T]->(n)` (typed edge, unlabeled
node → vertex resolved from the edge's target label), and `-[r]->(n:L)`
(unlabeled edge, labeled node → relationship hydrated from the adjacency edge).

## 4. Design

Execution change localized to the unlabeled branch of `execute_expand`
(`expand.rs` ~1141-1260), reusing existing primitives:

1. **Per-row edge resolution.** When `hop.edge_table.is_none() && hop.edge_label.is_none()`,
   read the edge-table identifier from the adjacency row (the value surfaced today as
   `col_0`, `keyspace.table`) and look up its `TableMetadata` from the schema snapshot.
2. **Endpoint label selection (D2).** From the resolved edge metadata choose the
   opposite label:
   - OUT → `graph.target_label`; IN → `graph.source_label`;
   - Both → pick per the adjacency row's stored direction component.
3. **Vertex table resolution.** `resolve_table_by_graph_label(schema, keyspace, label)`
   → the neighbor vertex table + metadata.
4. **Hydrate neighbor (D1).** Call the existing `find_vertex_match` with the resolved
   vertex table, neighbor id, and any inline props → full property map for `n`
   (`row_to_json`).
5. **Hydrate edge (D4).** Load the edge row for the adjacency entry and build the
   relationship JSON via the typed-path edge hydration instead of the `col_N` fallback.
6. **Fail loud (D3).** Any of {edge table not found, missing source/target label,
   label→table unresolved, vertex row missing} returns an error naming the edge table
   and the specific missing/invalid piece.

No planner change is required for the executor fix; the planner already emits `None`
for these fields, which the executor now treats as "resolve at run time" rather than
"skip hydration".

## 5. FMEA (failure modes)

| ID | Failure mode | Effect | Sev | Detect | Mitigation |
|---|---|---|---|---|---|
| F1 | Edge table lacks `source_label`/`target_label` | Cannot resolve neighbor table | High | test + live | **Fail loud** (D3) with edge-table name + missing key; document the edge-table contract. |
| F2 | Direction inverted for `<-[r]-` | Wrong neighbor / wrong `_src`/`_dst` | High | inverse test (acceptance #2) | D2 selects source-label for IN; assert `_src`/`_dst` orientation in tests. |
| F3 | Heterogeneous edge tables in one hop | Neighbor hydrated from wrong table | High | multi-label test (acceptance #1) | Resolve per-row from the adjacency edge table, not once per hop. |
| F4 | N+1 vertex lookups on wide fan-out | Latency / load on big nodes | Med | perf test | Reuse the existing per-neighbor `find_vertex_match` (same cost as typed path); consider batching later — out of scope for correctness fix. |
| F5 | Self-loop / same source+target label | Neighbor = anchor | Low | unit test | Direction + id still resolves correctly; add a self-loop case. |
| F6 | Neighbor id present in adjacency but vertex row deleted | Dangling edge | Med | unit test | D3: fail loud OR emit null with an explicit `_dangling` marker — **decide in review** (default: fail loud to match #4). |
| F7 | Regression to typed path | Existing queries change output | High | existing suite | D5 gate: new path only when `vertex_table`/`edge_table` are `None`; run full `ferrosa-graph` tests. |

## 6. Project plan (TDD, acceptance-driven)

1. **RED — multi-label expansion test** (`ferrosa-graph/tests` or `expand.rs` unit):
   fixture with ≥2 edge labels between ≥2 vertex labels; assert
   `MATCH (a {id:…})-[r]->(n) RETURN n.id, r` returns **non-null** `n.id` and real
   `r` props. (acceptance #1)
2. **RED — inverse incoming test:** `… <-[r]-(n) RETURN n.id, r` hydrates the **source**
   neighbor; assert `_src`/`_dst` orientation. (acceptance #2)
3. **RED — fail-loud test:** an edge table with missing/invalid source/target metadata
   → query errors with a useful message, not null endpoints. (acceptance #4)
4. **GREEN — implement** the §4 execution change in `execute_expand`, reusing
   `resolve_table_by_graph_label` / `find_vertex_match` / `row_to_json` / edge
   hydration.
5. **REGRESSION — typed path unchanged:** `-[r:DONATED_TO]->` returns the same node +
   edge props (acceptance #3); run the full `ferrosa-graph` test suite.
6. **Docs:** update `ferrosa-graph/specs/overview.md` + `fmea.md` with the edge-table
   endpoint-label contract (source/target labels are required for generic expansion);
   note in `ferrosa-graph/README.md`.
7. **Live check (§7):** confirm whether `sf_influence_query` edge tables carry the
   labels; if not, file the ingest-side schema addition + backfill.

## 7. Open questions / verification

- **Live schema:** query `system_schema` (or graph metadata) on the loaded
  `sf_influence_query` keyspace to confirm each edge table's extensions include
  `graph.source_label`/`graph.target_label`. If absent, the executor fix is correct but
  the *data* needs those extensions before generic recenter works — a small addition to
  `sf_campaign_donor_graph_ingest`'s edge-table DDL plus re-ingest/backfill.
- **F6 dangling-edge policy:** fail loud vs explicit null marker — confirm in review
  (default: fail loud).
- **Scope guard:** this is a correctness fix, not a fan-out perf change (F4 batching is
  a follow-up).
