---
crate: ferrosa-sparql
status: implemented
last_updated: 2026-07-25
executive_summary: >
  The SPARQL 1.1 Query + Update endpoint for ferrosa. Parses SPARQL with
  spargebra/oxrdf, plans algebra into TripleOps over a single CQL-backed
  rdf_triples table per keyspace, executes via the cluster write path with a
  nested-loop join, and serializes results as SPARQL JSON/XML/N-Triples/Turtle
  over an axum HTTP surface. The model is one RDF graph per keyspace; cross-graph
  operations fail loud rather than silently retarget. Significant gaps remain:
  no authentication, no OPTIONAL/MINUS, UNION is concat-only, and property paths
  load the full predicate adjacency into memory.
---

# ferrosa-sparql — Architecture Overview

## Purpose & boundary

`ferrosa-sparql` is the W3C SPARQL front-end. It owns SPARQL parsing, algebra →
storage planning, query execution, SPARQL UPDATE, and result serialization. It
does **not** own storage durability, replication, indexing, or schema — it
delegates reads to `ferrosa-cluster`'s `WritePath` and writes to
`ferrosa-storage`'s `StorageEngine`.

The RDF data model is mapped onto exactly one CQL table per keyspace:

```
rdf_triples : ((graph, subject), predicate, object)
              regular cols: object_type, datatype, language
```

The partition key is the composite `(graph, subject)` where `graph` is the
keyspace name — so there is **one logical RDF graph per keyspace**. This is the
crate's defining boundary: anything that addresses a distinct named graph is
rejected, never silently mapped onto the default graph.

## Module map

| Module | Responsibility |
|--------|----------------|
| `engine` (`src/engine.rs`, ~600 LoC) | `SparqlEngine`: parse→plan→execute orchestration, `SparqlResult`, ASK/CONSTRUCT/DESCRIBE graph assembly, JSON/XML/NT/Turtle dispatch |
| `planner` (`src/planner.rs`, ~770 LoC) | `spargebra` algebra → `QueryPlan` of `TripleOp`s; per-pattern access-method selection; CONSTRUCT/DESCRIBE mode detection |
| `executor` (`src/executor.rs`, ~1150 LoC) | Streaming triple-pattern scans + `ExecutionLimits` row bound, LIMIT pushdown, nested-loop join, constant-term enforcement, FILTER/ORDER BY/DISTINCT/LIMIT, composite-key decode, tombstone skipping, ObjectScan index |
| `update` (`src/update.rs`, ~730 LoC) | INSERT/DELETE DATA, DELETE WHERE, DELETE/INSERT WHERE, CLEAR, DROP, CREATE; up-front atomicity validation; one shared tombstone primitive |
| `property_path` (`src/property_path.rs`, ~440 LoC) | `+ * ? ^` evaluation via BFS with cycle detection over a streamed, bounded adjacency read |
| `filter` (`src/filter.rs`, ~490 LoC) | `Expression` evaluation: comparisons, boolean ops, arithmetic, a function subset; `unsupported_expr` gate for ORDER BY fail-loud |
| `results` (`src/results.rs`, ~415 LoC) | `Binding`, `SparqlJsonResults`, `ResultFormat`; JSON / W3C XML / N-Triples serializers |
| `http` (`src/http.rs`, ~234 LoC) | axum router, content negotiation, 1 MiB body limit, error→status mapping |
| `triple_store` (`src/triple_store.rs`) | `rdf_triples` schema, composite partition-key encoding, `ObjectType` |
| `rdf_star` (`src/rdf_star.rs`) | RDF\* annotation evaluation stub — fail-loud, not implemented |
| `namespace` (`src/namespace.rs`) | Standard prefix table (rdf, rdfs, owl, xsd, foaf, …) |

## Data flow

```mermaid
flowchart TD
    HTTP["axum handlers<br/>POST/GET /sparql, /sparql/update"] --> ENG["SparqlEngine::execute / execute_update"]
    ENG --> PARSE["spargebra parse<br/>query / update"]
    PARSE --> PLAN["planner::plan_query<br/>algebra to Vec&lt;TripleOp&gt;"]
    PLAN --> EXEC["executor::execute<br/>nested-loop join"]
    EXEC --> WP["ferrosa-cluster WritePath<br/>read / range_read_stream_all / index_read"]
    WP --> POST["FILTER to ORDER BY to DISTINCT to LIMIT/OFFSET"]
    POST --> SHAPE{"query form?"}
    SHAPE -->|SELECT| SER["results.rs serialize"]
    SHAPE -->|ASK| BOOL["boolean from non-empty bindings"]
    SHAPE -->|CONSTRUCT/DESCRIBE| GRAPH["instantiate template / SubjectLookup per IRI"]
    BOOL --> SER
    GRAPH --> SER
    SER --> RESP["JSON / XML / N-Triples / Turtle"]
    ENG -->|UPDATE| UVAL["update::validate_update<br/>up-front atomicity check"]
    UVAL --> UWRITE["StorageEngine write / tombstone"]
```

**Read path.** A bound subject plans to `SubjectLookup` (point read on the
partition key); a bound predicate to `PredicateScan` (streaming range scan +
filter); a bound object to `ObjectScan` (secondary index
`rdf_triples_object_idx`, falling back to a streaming range scan); nothing bound
to `FullScan`. Range scans pull one partition at a time from
`WritePath::range_read_stream_all` and bind each row as it arrives — no
intermediate `Vec` of fetched triples exists. The executor folds each
`TripleOp`'s rows into the running binding set with a compatibility check on both
value and `binding_type`, and enforces every constant term in the pattern (the
access path is a pushdown, not the whole match).

**Write path.** UPDATE first validates **every** operation (atomicity), then
applies them sequentially. Inserts write a live row; every delete form funnels
through one `tombstone_triple` primitive that writes a deletion-marker row with
`LivenessInfo::NONE` and clustering identical to the live row it must shadow.

## Key invariants

1. **One graph per keyspace.** Operations addressing a different named graph
   fail loud, never retarget (`check_quad_graph_pattern`,
   `check_pattern_graph_reads`).

   > **This invariant is VIOLATED today (FMEA SP-11 / t_af4eb9f0).** The read
   > side sets the `graph` partition-key component from the KEYSPACE, but
   > `update.rs` writes it as the literal string `"default"`. The two agree only
   > when the keyspace *is* `default` — and the HTTP endpoint defaults the
   > keyspace to `rdf`. On any other keyspace a point read computes a key
   > nothing was written under, so `SubjectLookup` misses data a `FullScan`
   > finds and `DELETE` reports success while tombstoning empty keys. Resolving
   > it requires deciding which side is authoritative; see the roadmap.
2. **UPDATE atomicity by up-front rejection.** Because execution has no rollback,
   `validate_update` rejects the whole request before any mutation if any op is
   unaddressable — preventing a desugared `Drop` from wiping data before a later
   op fails.
3. **Tombstone clustering must shadow the live row exactly.** Inserts and deletes
   share `encode_triple_clustering` (strict `[u16 len][bytes]`, no separator), so
   a tombstone matches the row it deletes; `LivenessInfo::NONE` prevents
   resurrection.
4. **Fail loud over silent wrong answers.** RDF\* annotation, `LOAD`,
   cross-graph ops, and unsupported ORDER BY expressions return
   `SparqlError::Plan`, not approximate results.
5. **Deleted rows never surface.** The executor skips any row whose deletion
   marker is non-live (URS-QEC-D05).
6. **Complete result, or an error — never a silent truncation.**
   `ExecutionLimits::max_rows` (from `SparqlConfig::max_rows`) is the one row
   bound. It is enforced at the SOURCE, as storage rows arrive from the
   partition stream, and on every operator's solution buffer. Crossing it
   returns `SparqlError::Execution`. Blocking operators (`ORDER BY`, `DISTINCT`,
   property-path BFS) may buffer but not exceed it.
7. **A constant is a match constraint in every position.** The access method is
   chosen from ONE bound term, but `try_bind_triple` re-checks subject,
   predicate, and object — including term kind, so an IRI constant never matches
   a literal spelling the same characters. A blank node in a pattern is a
   non-selectable variable and constrains nothing.
8. **LIMIT pushdown only where it is provably safe.** The scan stops after
   `offset + limit` solutions only for a single triple pattern with no
   `ORDER BY`, `DISTINCT`, `FILTER`, or graph query form above it; those
   operators either block or drop rows after binding, so an early stop would
   return the wrong rows or too few. LIMIT/OFFSET arithmetic saturates — both
   values are attacker-controlled `usize`.

## Position in the dependency graph

Front-end leaf: depends on `ferrosa-cluster`, `ferrosa-storage`,
`ferrosa-common`, `ferrosa-sstable`, `ferrosa-index`, `ferrosa-schema`; depended
on only by the `ferrosa` binary. See the
[root crate index](../../specs/crates.md) for the full graph.
