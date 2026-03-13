# ferrosa-graph Design

> **Date:** 2026-03-12
> **Status:** Approved — pending CQL merge before implementation begins
> **Approach:** Phased, bottom-up. Phase 0 hooks in existing crates, Phase 1 new crate MVP, Phase 2–3 optimization.
> **Methodology:** Literate programming (swdev) — module docs, doc-tests, property tests
> **Research Corpus:** `../research/corpus/graph-databases/` — WCO joins, schema optimization, similarity joins

## Goal

Add a graph query endpoint to ferrosa alongside CQL. Data lives in normal CQL tables (accessible via both CQL and Cypher). A system-managed adjacency index enables fast graph traversals. The design ensures `ferrosa-storage` and `ferrosa-schema` are graph-compatible from day one without introducing graph-specific coupling.

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Data model | Property graph + schema-aware optimization (Hybrid) | Property graph maps naturally to CQL tables; schema optimization (Sharma et al. SIGMOD '25) adds 2.5–6x speedups as a later phase without changing the data model |
| Storage mapping | Table-per-label + system adjacency index (Dual-Model) | CQL users see clean tables; graph traversals use the adjacency index for fast multi-label neighbor lookup |
| Query language | Cypher subset (openCypher), GQL-aligned | Declarative pattern matching enables WCO join optimization; ISO standard (GQL) trajectory; openCypher spec is Apache 2.0 |
| Protocol | HTTP/JSON first, Bolt later | HTTP is trivial to implement; all effort goes into the query engine. Bolt adds driver compatibility later |
| Index maintenance | Async WriteObserver (eventual consistency) | No write latency impact; adjacency index is eventually consistent. Sync mode available via trait if needed |
| Query execution | Phased: naive expand → WCO/Leapfrog → schema optimization | Working queries fast (Phase 1), provably optimal joins later (Phase 2), schema acceleration last (Phase 3) |

## Graph-to-CQL Storage Mapping

### Vertex Tables

Each vertex label maps to a CQL table. The partition key is the vertex ID.

```sql
CREATE TABLE graph.person (
    id UUID PRIMARY KEY,
    name TEXT,
    age INT
) WITH extensions = {
    'graph.type': 'vertex',
    'graph.label': 'person'
};

CREATE TABLE graph.company (
    id UUID PRIMARY KEY,
    name TEXT,
    founded INT
) WITH extensions = {
    'graph.type': 'vertex',
    'graph.label': 'company'
};
```

### Edge Tables

Each edge label maps to a CQL table. Partitioned by source vertex ID, clustered by destination.

```sql
CREATE TABLE graph.knows (
    src_id UUID,
    dst_id UUID,
    since INT,
    PRIMARY KEY (src_id, dst_id)
) WITH extensions = {
    'graph.type': 'edge',
    'graph.source': 'src_id',
    'graph.target': 'dst_id',
    'graph.source_label': 'person',
    'graph.target_label': 'person'
};

CREATE TABLE graph.works_at (
    src_id UUID,
    dst_id UUID,
    role TEXT,
    PRIMARY KEY (src_id, dst_id)
) WITH extensions = {
    'graph.type': 'edge',
    'graph.source': 'src_id',
    'graph.target': 'dst_id',
    'graph.source_label': 'person',
    'graph.target_label': 'company'
};
```

### System Adjacency Index

Auto-managed system table. One partition per vertex, clustering by direction + label + neighbor.

```sql
-- system_graph.adjacency (auto-created, hidden from user DDL)
CREATE TABLE system_graph.adjacency (
    vertex_id UUID,
    direction TINYINT,    -- 0=OUT, 1=IN
    edge_label TEXT,
    neighbor_id UUID,
    edge_table TEXT,      -- source table for full edge data
    PRIMARY KEY (vertex_id, direction, edge_label, neighbor_id)
);
```

Access patterns (all served by partition key + clustering prefix scans):

| Query | Clustering prefix |
|-------|-------------------|
| All outgoing edges from vertex | `vertex_id = ? AND direction = 0` |
| Outgoing KNOWS edges from vertex | `vertex_id = ? AND direction = 0 AND edge_label = 'knows'` |
| All incoming edges to vertex | `vertex_id = ? AND direction = 1` |
| Check specific edge exists | `vertex_id = ? AND direction = 0 AND edge_label = 'knows' AND neighbor_id = ?` |

### Dual Access — Same Data, Two Languages

```
Via CQL (port 9042):                    Via Cypher (port 7474):

SELECT name, age FROM graph.person      MATCH (a:Person {name: 'Alice'})
  WHERE id = ?;                               -[:KNOWS]->(b:Person)
                                        RETURN b.name, b.age
SELECT dst_id, since FROM graph.knows
  WHERE src_id = ?;
```

Same underlying tables. CQL sees normal rows; Cypher sees graph patterns. Both go through ferrosa-storage.

## Crate Structure

```
ferrosa-graph/
  Cargo.toml
  src/
    lib.rs                  # Public API: GraphEngine, start_http_server
    engine.rs               # GraphEngine — composes schema + storage + observer
    error.rs                # GraphError enum
    parser/
      mod.rs                # Lexer + Parser entry point
      lexer.rs              # Zero-alloc tokenizer (Cypher keywords)
      ast.rs                # Cypher AST types
    planner/
      mod.rs                # LogicalPlan → PhysicalPlan
      logical.rs            # Pattern graph, variable bindings
      physical.rs           # Expand, Filter, Project operators
    executor/
      mod.rs                # Execute PhysicalPlan against storage
      expand.rs             # Phase 1: naive neighbor expansion
      result.rs             # GraphResult — rows of bindings
    adjacency/
      mod.rs                # AdjacencyIndex — read/write helpers
      observer.rs           # AdjacencyIndexObserver (WriteObserver impl)
      schema.rs             # Adjacency table schema definition
    http.rs                 # Phase 1: HTTP/JSON endpoint
    bolt.rs                 # Phase 2: Bolt protocol (future)
  tests/
    integration/            # End-to-end: CQL write → Cypher read
```

### External Dependencies

| Crate | Purpose |
|-------|---------|
| `ferrosa-common` | Token, DecoratedKey, CellValue, Error/Result |
| `ferrosa-schema` | SchemaSnapshot, TableMetadata, table extensions |
| `ferrosa-storage` | StorageEngine, WriteObserver trait |
| `hyper` or `axum` | HTTP server |
| `serde`, `serde_json` | Request/response serialization |
| `tokio` | Async runtime (shared with CQL server) |

### Crate Dependency Graph

```
ferrosa-common (no dependencies)
├── ferrosa-sstable
├── ferrosa-schema
├── ferrosa-storage → ferrosa-sstable
├── ferrosa-cql → ferrosa-schema, ferrosa-storage
├── ferrosa-graph → ferrosa-schema, ferrosa-storage       # NEW
└── ferrosa (binary) → ferrosa-cql + ferrosa-graph + ferrosa-cluster
```

`ferrosa-graph` and `ferrosa-cql` are siblings — both depend on schema and storage, neither depends on the other.

## Gap Analysis — Changes to Existing Crates

All gaps serve dual purposes (graph + CQL). No graph-only additions to existing crates.

### Must Build for Phase 1

| Gap | Crate | Change | Also Enables |
|-----|-------|--------|-------------|
| WriteObserver trait | ferrosa-storage | `trait WriteObserver { fn mode() -> ObserverMode; fn tables() -> &[TableId]; fn on_write(table, mutation) -> Vec<Mutation>; }`. Add `Vec<Arc<dyn WriteObserver>>` to StorageEngine. Dispatch in write(). | CDC, secondary indexes, materialized views |
| Table extensions map | ferrosa-schema | Add `extensions: HashMap<String, String>` to TableMetadata. Wire through DDL (`WITH extensions = {...}`). | Any future table annotations |
| System table flag | ferrosa-schema | Add `is_system: bool` to TableMetadata. Schema registry rejects DROP/ALTER on system tables. | system.local, system.peers persistence |
| Register observer API | ferrosa-storage | `StorageEngine::register_observer(Arc<dyn WriteObserver>)`. Called at startup by ferrosa-graph. | Any future observer registration |

### Needed for Phase 2 (also needed for CQL range queries)

| Gap | Crate | Change | Also Enables |
|-----|-------|--------|-------------|
| Trie floor/ceiling/next | ferrosa-sstable | Extend walker.rs with ordered traversal. Dense nodes: binary search transitions. Sparse nodes: linear scan. | CQL token range scans |
| PartitionIterator | ferrosa-sstable | Lazy seek + next over partition index trie. Yields (DecoratedKey, data_offset). | CQL full table scans |
| SortedIterator trait | ferrosa-storage | Merge iterator: memtable BTreeMap + SSTable PartitionIterators. Min-heap ordered merge. | CQL SELECT with clustering ranges |

### No Changes Needed

These existing components work for graph support unchanged:

| Component | Why It Works |
|-----------|-------------|
| Memtable (ShardedBTreeMemtable) | Adjacency index rows are just rows. BTreeMap gives sorted order. |
| CommitLog | Already table-aware (TableId per mutation). Observer mutations get log entries. |
| TableStore (ArcSwap) | Lock-free reads work for adjacency index queries. |
| SSTableWriter | Adjacency index flushes to SSTables like any table. BTI format works. |
| SSTableReader point lookups | Phase 1 graph queries use point lookups (partition key = vertex_id). |
| Compaction (STCS) | Adjacency index compacts like any table. |
| S3 upload + cache | Adjacency index SSTables upload to S3 and cache locally. |
| Auth/RBAC | Graph endpoint reuses AuthContext and permission checking. |
| Audit | Graph DDL and queries emit audit events through existing AuditSink. |

## Parser — Cypher Subset

Hand-rolled recursive descent, same architecture as ferrosa-cql's parser.

### Supported Statements (Phase 1)

```
// Read
MATCH (pattern) RETURN expr [, expr]*
MATCH (pattern) WHERE predicate RETURN expr
MATCH (pattern) RETURN expr ORDER BY expr LIMIT n

// Write
CREATE (n:Label {prop: val, ...})
CREATE (a)-[:TYPE {prop: val}]->(b)
MATCH ... SET n.prop = val
MATCH ... DELETE n
MATCH ... DETACH DELETE n
```

### AST Types

```rust
/// Top-level Cypher statement.
pub enum Statement {
    Match { pattern: Vec<Pattern>, where_: Option<Expr>, return_: ReturnClause },
    Create { elements: Vec<Pattern> },
    Delete { pattern: Vec<Pattern>, detach: bool },
    Set { pattern: Vec<Pattern>, assignments: Vec<(PropertyRef, Expr)> },
}

/// A graph pattern element.
pub enum Pattern {
    Node { var: Option<String>, label: Option<String>, props: Vec<(String, Expr)> },
    Edge { src: Box<Pattern>, rel_type: Option<String>, dst: Box<Pattern>,
           direction: Direction, var: Option<String>, props: Vec<(String, Expr)> },
    Path { elements: Vec<Pattern> },
}

/// An expression (property access, literal, comparison, function call).
pub enum Expr {
    Property { var: String, name: String },
    Literal(LiteralValue),
    Function { name: String, args: Vec<Expr> },
    Comparison { left: Box<Expr>, op: CompareOp, right: Box<Expr> },
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
}
```

### Deferred Syntax

OPTIONAL MATCH, UNION, WITH, UNWIND, variable-length paths (`[:KNOWS*1..3]`), aggregations (count, sum, collect). Added as the executor can handle them.

## Planner

Two-stage planner with a stable interface between logical and physical plans. The physical plan strategy is swappable without changing the logical layer.

### Pipeline

```
Cypher text
  → parse()
  → AST (Statement)
  → validate(ast, schema_snapshot)
      — verify labels exist as vertex/edge tables
      — verify properties exist as columns
      — resolve table extensions for src/dst columns
  → LogicalPlan
      PatternGraph { nodes, edges, predicates, projections }
      — variables bound to vertex/edge table + column mappings
      — edge direction (OUT/IN/BOTH) resolved
  → PhysicalPlan (strategy selected here)
      Phase 1: Expand { anchor, hops }
      Phase 2: LeapfrogJoin { iterators, variable_order }
      Phase 3: SchemaOptimized { enriched_plan }
  → Execute against storage
  → GraphResult { columns, rows, stats }
```

### Anchor Selection (Phase 1)

The planner chooses the most selective node as the traversal anchor:

1. Node with property filter on partition key → single partition read (best)
2. Node with label → full table scan with filter
3. Unfiltered node → worst case, defer to later optimization

## Executor — Phase 1 Naive Expand

Walks the graph hop-by-hop using the adjacency index.

```
MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b)-[:WORKS_AT]->(c:Company) RETURN b.name, c.name

Step 1: Anchor — find starting vertices
  scan graph.person WHERE name = 'Alice' → [a₁]

Step 2: Expand hop 1 — KNOWS edges from anchor
  for each aᵢ:
    read system_graph.adjacency(vertex_id=aᵢ, dir=OUT, label='knows')
    → [b₁, b₂, ...]  (clustering key scan within partition)

Step 3: Expand hop 2 — WORKS_AT edges from hop 1 results
  for each bⱼ:
    read system_graph.adjacency(vertex_id=bⱼ, dir=OUT, label='works_at')
    → [c₁, c₂, ...]
    filter: c must exist in graph.company (label check)

Step 4: Project — fetch properties for RETURN clause
  for each (b, c) pair:
    read graph.person(id=bⱼ) → b.name
    read graph.company(id=cₖ) → c.name
    emit row [b.name, c.name]
```

All reads are partition-key point lookups — Phase 1 needs only what ferrosa-storage already provides.

## AdjacencyIndexObserver

Async WriteObserver implementation. Fires on tables tagged with `graph.type = 'edge'`.

```
User writes edge:
  INSERT INTO graph.knows (src_id, dst_id, since) VALUES (alice, bob, 2024)

Storage write path:
  commit_log.append(knows mutation)
  memtable(graph.knows).put(alice, {dst:bob, since:2024})
  spawn async: observer.on_write(knows_table_id, mutation)
    │
    ├─ read table extensions → graph.source=src_id, graph.target=dst_id
    ├─ extract src=alice, dst=bob from mutation
    ├─ generate 2 adjacency mutations:
    │    OUT: (vertex_id=alice, dir=0, label=knows, neighbor=bob, edge_table=graph.knows)
    │    IN:  (vertex_id=bob,   dir=1, label=knows, neighbor=alice, edge_table=graph.knows)
    └─ write both to system_graph.adjacency via storage engine
```

Both OUT and IN entries enable bidirectional traversal. DELETE mutations generate tombstones in the adjacency index.

## HTTP/JSON Endpoint

### Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| POST | `/graph/query` | Execute Cypher query |
| POST | `/graph/explain` | Return query plan without executing |
| GET | `/graph/schema` | List vertex/edge tables and their labels |
| GET | `/graph/health` | Health check |

### Request/Response Format

```json
// POST /graph/query
// Request:
{
  "query": "MATCH (a:Person)-[:KNOWS]->(b) RETURN b.name",
  "params": {"name": "Alice"},
  "keyspace": "social"
}

// Response:
{
  "columns": ["b.name"],
  "rows": [["Bob"], ["Charlie"]],
  "stats": {
    "vertices_read": 3,
    "edges_read": 2,
    "execution_ms": 12
  }
}
```

## GraphEngine

Composition root. Shares Schema and StorageEngine with CQL server via `Arc`.

```rust
/// Central coordinator for graph query processing.
/// Constructed by ferrosa binary alongside CqlServer.
pub struct GraphEngine {
    schema: Arc<Schema>,              // Shared with CQL
    storage: Arc<StorageEngine>,      // Shared with CQL
    adjacency: AdjacencyIndex,        // Read helpers for system_graph.adjacency
}

impl GraphEngine {
    pub fn new(schema: Arc<Schema>, storage: Arc<StorageEngine>) -> Self
    pub fn execute(&self, query: &str, keyspace: &str) -> Result<GraphResult>
    pub fn explain(&self, query: &str, keyspace: &str) -> Result<PhysicalPlan>
    pub fn graph_schema(&self, keyspace: &str) -> Result<GraphSchema>
}
```

A CQL INSERT into an edge table triggers the adjacency observer, and the next Cypher MATCH sees the new edge (after async propagation).

## WriteObserver Trait (ferrosa-storage addition)

```rust
/// Observer mode determines whether the storage engine awaits or spawns.
pub enum ObserverMode {
    /// Storage engine awaits on_write before returning to caller.
    Sync,
    /// Storage engine spawns on_write as a background task.
    Async,
}

/// Called by ferrosa-storage on every write to observed tables.
pub trait WriteObserver: Send + Sync {
    /// Whether this observer blocks writes or runs in the background.
    fn mode(&self) -> ObserverMode;

    /// Which tables this observer watches. Empty = all tables.
    fn tables(&self) -> &[TableId];

    /// Process a mutation and return additional mutations to apply.
    fn on_write(&self, table: TableId, mutation: &Mutation) -> Vec<Mutation>;
}
```

## Phasing

### Phase 0 — Storage & Schema Hooks

Changes to existing crates. No new crate. Prerequisite: CQL merge (brings ferrosa-storage and ferrosa-schema into workspace).

| Task | Crate | Details |
|------|-------|---------|
| WriteObserver trait | ferrosa-storage | Trait + `Vec<Arc<dyn WriteObserver>>` on StorageEngine + dispatch in write() |
| Table extensions map | ferrosa-schema | `extensions: HashMap<String, String>` on TableMetadata + DDL wiring |
| System table flag | ferrosa-schema | `is_system: bool` on TableMetadata + reject user DROP/ALTER |
| Register observer API | ferrosa-storage | `StorageEngine::register_observer()` called at startup |

**Milestone:** `cargo test` passes with a mock WriteObserver that logs mutations.

### Phase 1 — ferrosa-graph MVP

New crate. Cypher parser, naive expand executor, HTTP endpoint, adjacency index observer.

| Task | Module | Details |
|------|--------|---------|
| Cypher lexer + parser | parser/ | MATCH, CREATE, DELETE, SET, RETURN, WHERE, ORDER BY, LIMIT. Hand-rolled RD. LL(2). |
| AST types | parser/ast.rs | Statement, Pattern (Node/Edge/Path), Expr, SortOrder |
| Logical planner | planner/logical.rs | AST → PatternGraph. Validate labels/properties against schema. Resolve table extensions. |
| Physical planner | planner/physical.rs | PatternGraph → Expand plan. Choose anchor. Order hops. |
| Expand executor | executor/expand.rs | Walk adjacency index hop-by-hop. Fetch vertex properties for RETURN. Apply WHERE filters. |
| Adjacency index | adjacency/ | Schema definition, read helpers, AdjacencyIndexObserver (async WriteObserver impl). |
| HTTP endpoint | http.rs | /graph/query, /graph/explain, /graph/schema, /graph/health |
| GraphEngine | engine.rs | Compose schema + storage. Wire observer on startup. |

**Milestone:** End-to-end test — CREATE vertex/edge via CQL, MATCH via Cypher HTTP, verify results. Also: CREATE/MATCH via Cypher endpoint alone.

### Phase 2 — WCO Joins & Leapfrog

Optimization phase. Requires sorted iteration (also needed for CQL range queries).

| Task | Crate | Details |
|------|-------|---------|
| Trie floor/ceiling/next | ferrosa-sstable | Extend walker.rs with ordered traversal |
| PartitionIterator | ferrosa-sstable | Lazy seek + next over partition index trie |
| SortedIterator trait | ferrosa-storage | Merge iterator: memtable BTreeMap + SSTable PartitionIterators |
| Leapfrog executor | ferrosa-graph | LeapfrogTriejoin over SortedIterators on adjacency index |
| Planner: cycle detection | ferrosa-graph | Detect cyclic patterns; route to Leapfrog when cyclic, Expand when acyclic |

**Milestone:** Triangle query benchmark — compare naive expand vs Leapfrog on synthetic social graph. Verify O(N^{3/2}) vs O(N³) scaling.

### Phase 3 — Schema-Aware Query Optimization

Implements Sharma et al. SIGMOD '25 techniques.

| Task | Module | Details |
|------|--------|---------|
| Graph schema model | ferrosa-graph | Extract structural constraints from table extensions |
| Type inference pass | ferrosa-graph | Enrich LogicalPlan with schema-derived type constraints |
| TC elimination | ferrosa-graph | Eliminate recursive expansion when schema proves TC unnecessary |
| Semi-join filters | ferrosa-graph | Reduce intermediate result sizes using schema constraints |

**Milestone:** Benchmark against Phase 2 on YAGO-like dataset. Target: 2.5x average speedup on recursive queries.

### Future — Beyond Phase 3

| Feature | Details |
|---------|---------|
| Bolt protocol | PackStream encoding, Neo4j driver compatibility |
| Variable-length paths | `[:KNOWS*1..5]` syntax, BFS/DFS, shortest path |
| Aggregations | count(), collect(), sum(), avg() in RETURN clauses |
| Partition co-location | Hint vertex + adjacency data to same node (requires ferrosa-cluster) |
| Ring data structure | Compressed WCO from Arroyuelo et al. SIGMOD '24 |
| k-NN similarity joins | WCO similarity joins (Arroyuelo et al. SIGMOD '24) |

## Research Corpus Mapping

| Paper | Phase | Application |
|-------|-------|-------------|
| Hogan & Vrgoč, SIGMOD '24 — Querying Graph Databases at Scale | Phase 2 | WCO joins, Leapfrog Triejoin algorithm, variable ordering |
| Sharma et al., SIGMOD '25 — Schema-Based Query Optimisation | Phase 3 | Type inference, transitive closure elimination, semi-join filters |
| Arroyuelo et al., SIGMOD '24 — WCO Similarity Joins | Future | Ring data structure, k-NN similarity constraints |
| Franz Inc. — AllegroGraph 8.5.0 | Reference | RDF reasoning patterns (informational, not directly implemented) |
