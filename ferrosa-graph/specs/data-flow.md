---
crate: ferrosa-graph
doc: data-flow
last_updated: 2026-10-15
---

# ferrosa-graph — Data Flow

> Updated 2026-10-15 against `origin/main` (commit `9e32e82a`, release v0.22.3).
> The read path now reflects the streaming entry points, incremental aggregation,
> and external-sort spill; see [overview.md](overview.md) for the module map.

Two paths matter for understanding the crate: a **read** (a Cypher `MATCH` that
expands through the adjacency index) and a **write** (an edge create that keeps
the adjacency index consistent). Both go through the same composition root
(`GraphEngine`) and delegate persistence to `ferrosa-cluster`'s `WritePath`.

## 1. Read: a `MATCH` expand traversal

`MATCH (a:Person {name:'alice'})-[:KNOWS]->(b) RETURN b.name`

The engine resolves and authorizes the statement, looks up the anchor vertex,
then reads `system_graph_<ks>.adjacency` once per hop to find neighbors — it never
scans the edge table to traverse.

```mermaid
sequenceDiagram
    participant C as Client (HTTP 7474 / Bolt 7687)
    participant E as GraphEngine.execute_stream_with_params
    participant P as parser + planner
    participant A as auth (check_permission)
    participant X as expand executor (streaming)
    participant W as WritePath (ferrosa-cluster)
    participant S as StorageEngine

    C->>E: Cypher MATCH ... RETURN count(b)
    E->>E: parse + bind_statement_params; FOREACH / CALL {} materialized
    E->>P: validate: resolve labels, authorize (Select)
    P->>A: check_permission (per statement)
    A-->>P: ok (AuthContext permits)
    P->>P: logical plan then physical plan (Expand / Aggregate)
    E->>X: execute_streaming_owned -> execute_streaming_inner
    X->>W: range_read_stream_all(Person)  %% anchor scan, lazy
    W->>S: pull one anchor partition
    S-->>X: anchor rows (filter name='alice'), one partition at a time
    loop each hop
        X->>W: read(adjacency, key=src, dir=OUT, label=KNOWS)
        W->>S: read adjacency partition
        S-->>X: neighbor_id rows (capped by max_fan_out_per_hop)
        X->>W: read(Person, key=neighbor_id)  %% hydrate b
        W-->>X: b row
    end
    Note over X: aggregate folds rows incrementally (per-group state, bounded by max_groups);<br/>ORDER BY + LIMIT spills to disk when over the memory budget
    X-->>E: RowStream (columns, rows, QueryStats)
    E-->>C: streamed JSON body / Bolt RECORD stream
```

Notes: the entry point is `execute_stream_with_params`, which first expands and
**materializes** `FOREACH` and `CALL {}` (engine.rs:976, 982) before a single
statement flows through `execute_statement_streaming` → `validate` → `plan` →
`execute_streaming_owned` (engine.rs:1016, executor/expand.rs:280). The anchor is
**streamed** with `range_read_stream_all` — a partition that fails the anchor
predicates is dropped before the next is read, so an unanchored `MATCH` costs one
partition of memory rather than the whole table (t_bc5f0e6f). Each hop reads the
adjacency index for `(direction, edge_label, neighbor_id)`. Limits
(`max_fan_out_per_hop`, `max_result_rows`, `query_timeout`, and
`max_var_path_visited` for `[*]`) bound cost. Aggregation does **not** materialize
its input: `execute_aggregate` (executor/expand.rs:6046) folds each row into
per-group `Accumulator`s, so `RETURN count(n)` keeps state proportional to the
number of groups (bounded by `config.max_groups`), not the input row count.
`ORDER BY` (with optional `LIMIT`) spills to disk via `executor::spill`
(spill.rs:303). Because traversal reads the index rather than the edge table, an
adjacency-index desync (FMEA G-1/G-2) shows up here as **missing rows, not an
error** — which is exactly why the consistency invariant below matters.

### Measured read-path cost (2026-10-15)

On a local development cluster on 2026-10-15 (v0.22.3, `9e32e82a`):
`MATCH (n:Entity) RETURN count(n)` returned 105,873 in 5.88 s and
`MATCH (n:Fold) RETURN count(n)` returned 1,784 in 0.17 s — roughly linear per
returned row. The aggregate node is not the cost (it folds incrementally,
executor/expand.rs:6046); the dominant cost is in the scan/hydration path, though
these numbers do not attribute its cause. See [overview.md](overview.md) Open
Questions.

## 2. Write: an edge create keeping the adjacency index consistent

`CREATE (a)-[:KNOWS]->(b)` (or any edge-table write)

The edge row is written through `WritePath`. The `AdjacencyIndexObserver` is
registered on the edge table as a **synchronous** `WriteObserver`, so the storage
engine asks it to derive the matching adjacency mutations and applies them in the
**same** write — the OUT row under `src` and the IN row under `tgt`. The index is
therefore consistent with the committed edge, not eventually.

```mermaid
sequenceDiagram
    participant E as GraphEngine (Create/Merge plan)
    participant W as WritePath (ferrosa-cluster)
    participant S as StorageEngine
    participant O as AdjacencyIndexObserver (Sync)
    participant R as reconciler (background fallback)

    E->>W: write(edge_table, src, row, ts)
    W->>S: apply edge mutation
    S->>O: on_write(edge_table, mutation)
    Note over O: requires graph.source + graph.target ext;<br/>else returns empty (FMEA G-2)
    O-->>S: derived OUT row (under src) + IN row (under tgt)
    S->>S: apply adjacency mutations in the same write
    S-->>W: committed (edge + adjacency together)
    W-->>E: ok

    Note over R: fallback only - not the primary path
    loop every reconciliation_interval (0 = disabled by default)
        R->>W: range_read_stream_all(edge_table)
        R->>W: read(adjacency) per edge row
        alt OUT/IN entry missing
            R->>W: write make_adjacency_mutation (repair)
        end
        R->>W: range_read_stream_all(adjacency) then read(edge_table)
        alt adjacency row has no surviving edge
            R->>W: write tombstone (remove orphan)
        end
    end
```

### The adjacency-consistency invariant

For every live edge `(src)-[label]->(tgt)` the index holds exactly two rows:

- OUT: partition `src`, clustering `(OUT, label, tgt)`
- IN: partition `tgt`, clustering `(IN, label, src)`

Each clustering component is wire-encoded as `[u16 BE length][bytes]` for
`(direction:1B, edge_label:text, neighbor_id:blob)` — the layout the SSTable
composite parser requires. The **synchronous observer** is the primary
enforcement (index commits with the edge). The **reconciler is the fallback**: it
repairs entries the observer missed (dropped mutation under backpressure, crash
between edge and index visibility) and tombstones orphans whose edge is gone. It
is idempotent and observable (`ReconcileMetrics`), but a safety net — never the
source of truth. Caveat (FMEA): it is disarmed by the default config and repairs
at hardcoded RF=1, so the fallback is currently weaker than the invariant it
guards.


## Same-engine write ownership

Bound AST -> recursive write classification -> FIFO GraphEngine writer admission
-> publication read guard -> MATCH/predicates/RHS/nested execution -> row stream.
BEGIN/read-only RUN skips admission. Explicit writes retain admission in the
transaction overlay across RUNs; COMMIT takes publication write under admission
and publishes through the unchanged BatchTxn. Autocommit retains admission in the
outer stream until exhaustion/error/drop. Publication releases before admission.
Waiters hold no publication guard; readers bypass writer admission. Deadlines
bound setup/admission and finite stream polling. RESET/EOF cannot be observed
while Bolt is awaiting RUN; prompt cancellation there remains separate work.

Ownership spans every key/database in this engine. Independent-key writes
serialize (roughly 1/H throughput ceiling for ownership duration H), so baseline
results must disclose this cost. No CQL/cross-engine/schema-incarnation guarantee
follows from this local gate; narrower validated update-read ownership is next.
