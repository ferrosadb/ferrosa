# Query Engine Completeness — Cypher, Bolt, and SPARQL

> Last updated: 2026-06-12
> Status: Draft (proposed)
> Repo: ferrosa (ferrosa-graph, ferrosa-sparql)

## Goal

Make Ferrosa's three graph-facing query interfaces **fully compliant and
consistent**:

- **Cypher** (openCypher dialect) — the property-graph query/mutation language.
- **Bolt** — Neo4j's binary wire protocol (port 7687) carrying Cypher.
- **SPARQL 1.1** — query + update over the RDF triple store.

**First milestone: delete completeness** across all three. Today every interface
has a delete gap (detailed below); closing those is the highest-value, lowest-risk
step and unblocks the downstream `ferrosa-memory` "forget" feature. Full
language/protocol compliance follows as later milestones.

## Architecture (established by code audit)

Ferrosa runs **two independent graph data models on one shared `StorageEngine`**:

| Model | Interfaces | Storage | Key tables |
| --- | --- | --- | --- |
| **Property graph** | Cypher (HTTP `/graph/query` + Bolt) | `StorageEngine` SSTables | `typed_edges`, vertex tables, `system_graph_<ks>.adjacency` |
| **RDF triples** | SPARQL (HTTP `/sparql`) | `StorageEngine` SSTables | `rdf_triples` (PK `((graph, subject), predicate, object)`) |

**There is no bridge between them.** A Cypher-created edge is invisible to SPARQL
and vice versa (`ferrosa-graph` writes `typed_edges`; `ferrosa-sparql` writes
`rdf_triples`; no code syncs the two). They converge only at the low-level
`StorageEngine` write/tombstone path.

Implications:
- Cypher and Bolt **share one executor** — a Cypher fix corrects both transports.
- SPARQL is a **separate path** and must be fixed independently.
- "Delete an entity everywhere" means *each store the entity lives in*. For
  ferrosa-memory that is **only the property graph** (it never writes RDF).
- The unification is **discipline + a shared low-level delete primitive**, not a
  single logical node: every interface maps its delete to the same
  `StorageEngine` tombstone path with the same guarantees (atomic adjacency
  cleanup, fail-loud, no orphans). Whether to add a property-graph↔RDF bridge is
  an explicit open question, not assumed.

## Milestone 1 — Delete completeness (priority)

### Current gaps

| Interface | Works | Broken / missing |
| --- | --- | --- |
| Cypher edge `DELETE r` | ✅ durable, no orphans | — |
| Cypher node `DELETE n` | deletes node row | **No constraint check** — orphans edges silently |
| Cypher `DETACH DELETE n` | parsed, planned | **Executor ignores the `detach` flag** (`executor/expand.rs:3730` `_detach`) — no edge cascade |
| Adjacency cleanup | eventual (`adjacency/reconcile.rs`) | **Not atomic** with delete — stale index entries until reconciliation |
| Cypher `REMOVE` | — | **Absent** (no parser/AST variant) |
| SPARQL `INSERT DATA` / `DELETE DATA` (ground) | ✅ | — |
| SPARQL `DELETE WHERE`, `DELETE/INSERT … WHERE` | parsed (spargebra) | **Stubbed** (`update.rs:48-52`) |
| SPARQL `CLEAR`, `DROP` | parsed | **Stubbed** |
| Bolt delete | ✅ (shares Cypher executor) | inherits the Cypher node/DETACH gap |

### Requirements

| ID | Requirement |
| --- | --- |
| URS-QEC-D01 | `DETACH DELETE n` (Cypher/Bolt) shall delete the node **and all incident relationships** (both directions) and their adjacency entries, atomically, leaving no orphans. |
| URS-QEC-D02 | Plain `DELETE n` with surviving relationships shall **fail loud** with a Neo4j-style constraint error, not orphan edges. |
| URS-QEC-D03 | Adjacency-index cleanup shall happen **within the delete**; reconciliation is only a crash-window backstop. |
| URS-QEC-D04 | SPARQL pattern deletes — `DELETE WHERE`, `DELETE/INSERT … WHERE`, `CLEAR`, `DROP` — shall be implemented over `rdf_triples`, writing tombstones via the same `StorageEngine` path as `DELETE DATA`. |
| URS-QEC-D05 | All deletes shall be durable and immediately invisible to subsequent reads on their own interface (Cypher MATCH; SPARQL SELECT/ASK) and to direct CQL reads of the underlying tables. |
| URS-QEC-D06 | Cypher `REMOVE n.prop` / `REMOVE n:Label` shall be implemented. |
| URS-QEC-D07 | A shared low-level `delete_with_adjacency(node/edge ids)` primitive in `StorageEngine`/`ferrosa-graph` shall back the Cypher cascade so the logic is defined once. |

### Design

- **Cypher `execute_delete`** (`ferrosa-graph/src/executor/expand.rs:3725-3853`):
  honor `detach` — enumerate incident edges via the adjacency index (forward +
  backward), tombstone each edge + its adjacency entries, then the vertex, in one
  write batch (URS-QEC-D01/D03/D07). For `detach == false`, probe adjacency and
  error if any edge exists (URS-QEC-D02).
- **SPARQL update** (`ferrosa-sparql/src/update.rs`): implement the
  pattern-delete operations by planning the WHERE clause (reuse the SELECT
  executor to bind solutions), then deleting each resulting ground triple via the
  existing `delete_ground_quad` tombstone path; `CLEAR`/`DROP` tombstone the
  graph/table (URS-QEC-D04).
- **Bolt**: no work — inherits the corrected Cypher executor (URS-QEC-D05 over
  Bolt).
- **REMOVE**: add `Remove` AST/parser/planner/executor mirroring `SET`.

### Verification (Milestone 1)

| Test ID | Type | Given / When / Then |
| --- | --- | --- |
| T-QEC-D01 | Integration | Node with N inbound + M outbound edges; `DETACH DELETE n` → node + all N+M edges gone; adjacency scan finds zero references (MATCH + CQL confirm). |
| T-QEC-D02 | Integration | Node with edges; `DELETE n` (no DETACH) → fails loud; nothing deleted. |
| T-QEC-D03 | Integration | Post-DETACH-DELETE adjacency scan is clean **without** running reconciliation. |
| T-QEC-D04 | Integration | Same DETACH DELETE result via Bolt and HTTP. |
| T-QEC-D05 | Integration | SPARQL `DELETE WHERE { ?s :p ?o }` removes all matching triples; SELECT no longer returns them. |
| T-QEC-D06 | Integration | SPARQL `DELETE/INSERT … WHERE` and `CLEAR GRAPH <g>` behave per SPARQL 1.1. |
| T-QEC-D07 | Unit | `REMOVE n.prop` unsets the property. |

## Milestone 2 — Full Bolt compliance

Bolt v5 is real and solid (handshake, PackStream, HELLO/LOGON/RUN/PULL/DISCARD/
RESET/GOODBYE, auth, streaming, errors). Gap to "full":

| ID | Requirement |
| --- | --- |
| URS-QEC-B01 | Explicit transactions — `BEGIN` (0x11) / `COMMIT` (0x12) / `ROLLBACK` (0x13) message types implemented (decode/encode, `process_message` wiring). |
| URS-QEC-B02 | A connection transaction state machine (`tx_id`, open-tx, queued statements) that defers execution until `COMMIT` and aborts on `ROLLBACK`, backed by a `StorageEngine` transaction/batch API. |
| URS-QEC-B03 | Per-query timeout enforcement and (optionally) bookmark/causal-consistency metadata for driver compatibility. |

(Transaction support here is the same storage-batch capability Milestone 1's
atomic delete wants and that the `ferrosa-memory` forget feature wants — see the
Accord stub bug `specs/bug-accord-lwt-acks-phantom-write.md`; these should share a
real multi-write batch/transaction primitive.)

## Milestone 3 — Full SPARQL 1.1 compliance

SPARQL has a real engine (spargebra parser, CQL-backed triple store, SELECT/ASK,
property paths). Gaps to "full":

| ID | Requirement |
| --- | --- |
| URS-QEC-S01 | `CONSTRUCT` and `DESCRIBE` query forms (planner + executor + graph result serialization). |
| URS-QEC-S02 | Full SPARQL UPDATE beyond Milestone 1 deletes: `INSERT … WHERE`, `LOAD`, `CREATE`, graph management. |
| URS-QEC-S03 | RDF* annotation evaluation (currently returns inner bindings + warning — a silent gap) and SPARQL XML results serialization. |
| URS-QEC-S04 | ORDER BY on expressions (currently variables only — silent ignore). |

## Milestone 4 — Full openCypher compliance (Cypher)

Beyond delete/REMOVE: audit the executor against the openCypher surface
(`MERGE`/`SET`/`CREATE` are done) and close remaining gaps (e.g. `FOREACH`,
`CALL {}` subqueries, list/map comprehensions, `UNION`, aggregation edge cases) —
enumerate via a conformance pass. Tracked but lower priority than deletes.

## Cross-cutting requirements

| ID | Requirement |
| --- | --- |
| URS-QEC-X01 | No silent no-ops: any unimplemented clause/message **fails loud** with a clear, interface-appropriate error (Cypher error / Bolt FAILURE / SPARQL protocol error) — never acknowledges a mutation it didn't perform (cf. the Accord LWT phantom-write bug). |
| URS-QEC-X02 | Multi-write atomicity for delete-cascade, Bolt transactions, and forget shall use one real `StorageEngine` batch/transaction primitive, not three divergent ad-hoc paths. |

## Open Questions

- **Property-graph ↔ RDF bridge:** should Ferrosa offer an optional view so the
  same data is queryable as both a property graph (Cypher/Bolt) and RDF (SPARQL)?
  Today they are disjoint. If yes, "delete everywhere" and the bridge become a
  fourth milestone; if no, document that the two models are independent stores.
- **Transaction primitive:** what `StorageEngine` API backs atomic multi-row
  writes (for D03/B02/X02)? Is it the same path the Accord work will land on?
- **Conformance bar:** which openCypher / SPARQL 1.1 conformance suites do we
  gate on for "fully compliant"?

## Supersedes / relates

- Folds in and supersedes the standalone `feat-graph-node-delete-detach.md` draft.
- Relates to `specs/bug-accord-lwt-acks-phantom-write.md` (shared need for a real
  multi-write transaction primitive).
- Downstream consumer: `ferrosa-memory/specs/todo/feat-forget-memory.md`.
