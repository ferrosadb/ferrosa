# SPARQL Endpoint for Ferrosa

## Summary

Add SPARQL 1.1 Query + Update as a fourth protocol endpoint in Ferrosa, alongside CQL (port 9042), Bolt/Cypher (port 7687), and Graph HTTP (port 7474). This gives ferrosa native RDF*/SPARQL support for semantic repository workloads.

## Motivation

ferrosa-memory-mcp is building an MCP evaluation framework that grades semantic repository maturity (inference, ontological consistency, graph completeness, multi-hop reasoning). SPARQL is the standard query language for semantic repositories. Rather than implementing a SPARQL-to-CQL translator in the client (ferrosa-memory), SPARQL belongs as a server-side module in ferrosa — it parallels CQL, Bolt, and Cypher as a query protocol.

Additionally:
- SPARQL property paths (`?s foaf:knows+ ?o`) require efficient server-side graph traversal — doing N client round-trips is prohibitive
- SPARQL CONSTRUCT enables RDF graph export without client-side assembly
- Federated SPARQL (SERVICE) becomes possible in future as a server feature
- Any ferrosa user gets SPARQL, not just ferrosa-memory

## Architecture

### New Crate: `ferrosa-sparql`

```
ferrosa-sparql/
    Cargo.toml          (depends on spargebra, ferrosa-common, ferrosa-storage, ferrosa-graph)
    src/
        lib.rs
        server.rs       # HTTP endpoint: GET/POST /sparql on configurable port
        parser.rs       # SPARQL text -> algebra (via spargebra crate)
        planner.rs      # Algebra -> execution plan (storage reads, graph traversals)
        executor.rs     # Run plan against StorageEngine + graph indexes
        update.rs       # SPARQL UPDATE: INSERT DATA, DELETE DATA, MODIFY
        rdf_star.rs     # RDF* annotation queries (<< ?s ?p ?o >> ?prop ?val)
        results.rs      # Serialization: SPARQL JSON, Turtle, N-Triples, JSON-LD
        namespace.rs    # Standard prefix management (foaf, dc, prov, rdf, rdfs, owl)
        property_path.rs # Transitive closure via graph engine BFS/DFS
```

### Integration with Existing Architecture

```
                    ┌──────────────┐
                    │  ferrosa     │
                    │  (main.rs)   │
                    └──────┬───────┘
           ┌───────┬───────┼───────┬──────────┐
           │       │       │       │          │
        CQL:9042  Bolt:7687 HTTP:7474 Web:9090 SPARQL:8080
           │       │       │       │          │
     ferrosa-cql  ferrosa  ferrosa  ferrosa   ferrosa-sparql
                  -graph   -graph   (web)     (NEW)
           │       │       │                  │
           └───────┴───────┴──────────────────┘
                          │
                   ferrosa-storage
                   (StorageEngine)
```

SPARQL queries execute against the same StorageEngine that CQL and Cypher use. No data duplication.

### Port and Configuration

```
FERROSA_SPARQL_ENABLED=true
FERROSA_SPARQL_BIND=0.0.0.0:8080
```

Disabled by default (like Bolt). Enabled via env var or config.

## SPARQL Feature Matrix

### Query (Read)

| Feature | Priority | Implementation |
|---|---|---|
| SELECT | P0 | Triple patterns → StorageEngine edge/entity queries |
| WHERE (basic graph patterns) | P0 | Join on shared variables across triple patterns |
| FILTER | P0 | Predicate evaluation on bindings |
| ORDER BY / LIMIT / OFFSET | P0 | Post-processing on result set |
| OPTIONAL | P1 | Left-join semantics |
| UNION | P1 | Concat result sets |
| ASK | P1 | Boolean existence (SELECT + limit 1) |
| CONSTRUCT | P2 | Build RDF graph from query results |
| DESCRIBE | P2 | Entity neighborhood (uses graph adjacency index) |
| Property paths (`+`, `*`, `?`) | P1 | Server-side BFS/DFS via graph engine — this is WHY SPARQL belongs in ferrosa |
| Aggregates (COUNT, SUM, AVG, GROUP BY) | P2 | Post-processing on bindings |
| Subqueries | P3 | Nested evaluation |
| Federated (SERVICE) | P3 | Future — proxy to external SPARQL endpoints |

### RDF* Extensions

| Feature | Priority | Implementation |
|---|---|---|
| `<< ?s ?p ?o >> ?prop ?val` | P1 | Join typed_edges with edge_annotations table |
| Annotated CONSTRUCT | P2 | Emit RDF* Turtle with annotations |
| Nested annotations | P3 | Recursive annotation queries |

### Update (Write)

| Feature | Priority | Implementation |
|---|---|---|
| INSERT DATA | P0 | StorageEngine::write for entities + edges + annotations |
| DELETE DATA | P0 | Scoped deletion by triple pattern |
| DELETE/INSERT (MODIFY) | P1 | Atomic: query bindings → delete matched → insert new |
| LOAD | P2 | Parse Turtle/N-Triples file → batch insert |
| CLEAR GRAPH | P2 | Delete all triples in a named graph (session) |
| DROP GRAPH | P3 | Remove graph metadata |

### Serialization

| Format | Content-Type | Priority |
|---|---|---|
| SPARQL JSON Results | `application/sparql-results+json` | P0 |
| Turtle | `text/turtle` | P1 |
| N-Triples | `application/n-triples` | P1 |
| JSON-LD | `application/ld+json` | P2 |

## Prerequisites from Ferrosa

### 1. Reverse Edge Index (Required for SPARQL)

SPARQL triple patterns can bind any position: `?s ?p ?o`, `?s :knows ?o`, `:alice ?p ?o`, `?s ?p :bob`.

Current `typed_edges` partition key is `(tenant_id, session_id, src_id, edge_type, dst_id)` — efficient for forward lookups (given src, find dst) but requires full scan for reverse (given dst, find src).

**Needed:** A reverse-edge materialized view or secondary index:
```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS agent_memory.typed_edges_by_dst AS
    SELECT * FROM agent_memory.typed_edges
    WHERE tenant_id IS NOT NULL AND session_id IS NOT NULL AND dst_id IS NOT NULL
    AND src_id IS NOT NULL AND edge_type IS NOT NULL
    PRIMARY KEY ((tenant_id, session_id, dst_id), edge_type, src_id);
```

Or alternatively, a secondary index on `dst_id` if ferrosa supports efficient secondary index scans.

### 2. Edge Annotations Table (Required for RDF*)

New table for structured metadata on edges (statement-about-statement):

```sql
CREATE TABLE IF NOT EXISTS agent_memory.edge_annotations (
    tenant_id uuid,
    session_id uuid,
    src_id uuid,
    edge_type text,
    dst_id uuid,
    property_name text,
    property_value text,
    value_type text,
    created_at timestamp,
    PRIMARY KEY ((tenant_id, session_id, src_id, edge_type, dst_id), property_name)
);
```

This is DDL that ferrosa-memory will issue via CQL. No ferrosa engine changes needed — just standard table creation.

### 3. Server-Side Graph Traversal API (Required for Property Paths)

SPARQL property paths (`?s foaf:knows+ ?o`) require multi-hop traversal. The existing graph HTTP endpoint supports Cypher queries which can express this:

```cypher
MATCH (a)-[:KNOWS*1..10]->(b) RETURN a, b
```

The SPARQL planner should translate property paths to Cypher traversals via the graph engine, or use a direct BFS/DFS on the adjacency index.

**Needed:** Expose the graph engine's BFS/DFS traversal as an internal API (not just HTTP) so `ferrosa-sparql` can call it directly without HTTP overhead.

### 4. Content Negotiation on HTTP

The SPARQL endpoint needs to return different formats based on Accept header. The existing web console (port 9090) uses axum — `ferrosa-sparql` should also use axum with content negotiation middleware.

## Docker Compose Integration

Add SPARQL port to the ferrosa-memory docker-compose.yml:

```yaml
node1:
    ports:
      - "19042:9042"    # CQL
      - "17474:7474"    # Graph HTTP
      - "17687:7687"    # Bolt
      - "19090:9090"    # Web console
      - "18080:8080"    # SPARQL (NEW)
```

## Relationship to ferrosa-memory

Once SPARQL is a ferrosa server module:
- ferrosa-memory-eval's Semantic Analyzer queries via SPARQL instead of raw CQL
- ferrosa-memory-sparql crate becomes unnecessary — just a thin client
- The eval framework gains SPARQL as a verification tool (L3 scenarios use SPARQL queries)
- smart_ingest and run_consolidation gain SPARQL INSERT for provenance-annotated writes

## Implementation Plan

This can be developed in parallel with ferrosa-memory eval work:

| Sprint | Focus | Deliverable |
|---|---|---|
| S1 | Parser + planner + basic SELECT | `ferrosa-sparql` crate, triple pattern queries work |
| S2 | FILTER, OPTIONAL, property paths, RDF* | Full read support with annotations |
| S3 | INSERT DATA, DELETE DATA, MODIFY, serialization | Full read/write + Turtle/N-Triples output |

**Dependencies:**
- Reverse edge index (S1 prerequisite)
- Edge annotations table (S2 prerequisite, but can be created via CQL DDL)
- Graph traversal internal API (S2, for property paths)
