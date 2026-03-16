# Threat Model — ferrosa-graph Extensions

> **Date:** 2026-03-12
> **Scope:** New attack surface introduced by the graph query endpoint (ferrosa-graph)
> **Methodology:** STRIDE per element
> **Design Spec:** `superpowers/specs/2026-03-12-ferrosa-graph-design.md`

## System Overview

ferrosa-graph adds a Cypher/GQL query endpoint alongside CQL. Key new components:

- **HTTP/JSON endpoint** (port 7474) — new network-facing service
- **Cypher parser** — new input parsing surface accepting arbitrary user text
- **Adjacency index** — system-managed table with async write consistency
- **WriteObserver** — hook in the storage write path generating derived mutations
- **Table extensions** — metadata annotations controlling graph behavior
- **Dual-access model** — same data accessible via CQL (port 9042) and Cypher (port 7474)

## Data Flow Diagram

```mermaid
graph TB
    subgraph "Untrusted"
        CQLClient[CQL Client]
        GraphClient[Graph Client / Browser]
        Attacker[Attacker]
    end

    subgraph "Trust Boundary: Network Edge"
        CQLPort["CQL Endpoint\nport 9042\n(Tokio TCP)"]
        HTTPPort["Graph Endpoint\nport 7474\n(HTTP/JSON)"]
    end

    subgraph "Trust Boundary: Application"
        CQLParser[CQL Parser]
        CypherParser[Cypher Parser]
        AuthN["AuthN\n(SASL/HTTP)"]
        AuthZ["AuthZ\n(RBAC)"]
        Planner[Graph Planner]
        Executor[Graph Executor]
        CQLExec[CQL Executor]
    end

    subgraph "Trust Boundary: Storage"
        Schema["Schema\n(ArcSwap)"]
        Storage["StorageEngine\n(Memtable + SSTable)"]
        Observer["WriteObserver\n(async)"]
        AdjIndex[("system_graph\n.adjacency")]
        UserTables[("User Tables\n(vertex + edge)")]
        CommitLog[("Commit Log")]
        S3[("S3 / Object Store")]
    end

    CQLClient -->|"CQL v5 binary"| CQLPort
    GraphClient -->|"HTTP/JSON"| HTTPPort

    CQLPort --> AuthN
    HTTPPort --> AuthN
    AuthN --> AuthZ
    AuthZ --> CQLParser
    AuthZ --> CypherParser

    CypherParser --> Planner
    Planner -->|"validate labels"| Schema
    Planner --> Executor
    Executor -->|"read adjacency"| AdjIndex
    Executor -->|"read vertex props"| UserTables
    CQLParser --> CQLExec
    CQLExec -->|"read/write"| UserTables

    Storage -->|"edge write"| Observer
    Observer -->|"async write"| AdjIndex
    Storage --> CommitLog
    Storage --> S3
    UserTables --- Storage
    AdjIndex --- Storage
```

## Assets

| Asset | Type | Impact if Compromised |
|-------|------|----------------------|
| Graph data (vertices, edges, properties) | Confidentiality, Integrity | Data breach, incorrect query results |
| Adjacency index | Integrity, Availability | Incorrect traversals, missing edges, graph corruption |
| Table extensions metadata | Integrity | Attacker controls which tables are treated as graph edges |
| Authentication credentials | Confidentiality | Full system access via either CQL or graph endpoint |
| Audit trail | Integrity | Actions via graph endpoint untracked |
| System availability | Availability | Graph queries or observer overload degrades CQL |
| S3-stored SSTables | Confidentiality, Integrity | Long-term data exposure if adjacency index leaks |

## Threat Inventory

### T1: Cypher Injection / Parser Exploitation

| Field | Value |
|-------|-------|
| **STRIDE** | Tampering, Elevation of Privilege |
| **Component** | Cypher parser (parser.rs) |
| **Threat** | Malformed Cypher input causes parser panic, stack overflow (deeply nested expressions), or unexpected AST that bypasses planner validation |
| **Likelihood** | 2 — Parser accepts arbitrary user text; hand-rolled parsers are error-prone |
| **Impact** | 2 — Panic crashes the connection task; crafted AST could bypass authorization |
| **Risk** | **4 (High)** |
| **Mitigation** | (1) Proptest fuzz testing on all parser entry points (already in plan). (2) Limit expression nesting depth (max 64 levels) to prevent stack overflow. (3) Catch panics at the HTTP handler boundary (tokio task). (4) Parser produces AST only — planner validates against schema before execution. |
| **Status** | **Mitigated.** Proptests implemented, expression depth capped at 64 (returns ParseError). Remaining: catch panics at HTTP handler boundary (Phase 1). |

### T2: Unauthenticated Graph Endpoint Access

| Field | Value |
|-------|-------|
| **STRIDE** | Spoofing |
| **Component** | HTTP endpoint (http.rs) |
| **Threat** | Graph HTTP endpoint lacks authentication, allowing unauthenticated queries. CQL uses SASL handshake; HTTP has no equivalent unless explicitly implemented. |
| **Likelihood** | 3 — If auth is not implemented on the HTTP endpoint, it's trivially exploitable |
| **Impact** | 3 — Full read/write access to all graph data |
| **Risk** | **9 (Critical)** |
| **Mitigation** | (1) HTTP endpoint MUST authenticate before processing queries. Use HTTP Basic Auth or Bearer token mapped to the same RBAC system (ferrosa-schema AuthContext). (2) Reuse the same `Schema::authenticate()` with rate limiting. (3) Never ship an unauthenticated endpoint, even in dev mode — use a dev token. |
| **Status** | **Mitigated.** HTTP Basic Auth middleware implemented in `http.rs`. Maps to `Schema::authenticate()` with rate limiting. All routes except `/graph/health` require auth. |

### T3: Authorization Bypass via Graph Endpoint

| Field | Value |
|-------|-------|
| **STRIDE** | Elevation of Privilege |
| **Component** | Graph executor, planner |
| **Threat** | A user with CQL SELECT on `graph.person` but NOT on `graph.company` uses a Cypher traversal `MATCH (a:Person)-[:WORKS_AT]->(c:Company) RETURN c.name` to read Company data they shouldn't access. The graph executor must enforce per-table permissions at each hop. |
| **Likelihood** | 2 — If per-hop auth checking is missed, traversals leak data across permission boundaries |
| **Impact** | 3 — Unauthorized data access across tables |
| **Risk** | **6 (High)** |
| **Mitigation** | (1) Executor MUST check `Permission::Select` on each vertex/edge table accessed during traversal — not just the anchor table. (2) Planner should pre-check all tables in the pattern at plan time and fail fast. (3) Adjacency index reads should also check permission on the referenced `edge_table`. |
| **Status** | **Mitigated.** Logical planner checks `Permission::Select` on every table in the pattern at plan time via `check_table_permission()`. Executor verifies per-hop during traversal. |

### T4: Denial of Service via Expensive Graph Queries

| Field | Value |
|-------|-------|
| **STRIDE** | Denial of Service |
| **Component** | Graph executor (expand.rs) |
| **Threat** | Unbounded graph traversals exhaust memory or CPU. Examples: (a) `MATCH (a)-[:FOLLOWS]->() RETURN a` on a social graph with a supernode (millions of edges). (b) Multi-hop expansion without LIMIT produces cartesian blowup. (c) No query timeout — long-running traversals block executor threads. |
| **Likelihood** | 3 — Trivial to craft; even well-meaning queries can be expensive |
| **Impact** | 2 — Degrades both graph AND CQL performance (shared StorageEngine) |
| **Risk** | **6 (High)** |
| **Mitigation** | (1) Default query timeout (e.g., 30 seconds). (2) Max result set size (e.g., 10,000 rows default). (3) Max expansion fan-out per hop (configurable). (4) Query memory budget tracking. (5) Separate thread pool or Tokio task budget for graph queries so CQL is not starved. |
| **Status** | **Mitigated.** `GraphEngineConfig` implements: query timeout (default 30s), max result rows (10,000), max fan-out per hop (10,000). Configurable. |

### T5: Adjacency Index Inconsistency (Eventual Consistency Window)

| Field | Value |
|-------|-------|
| **STRIDE** | Tampering, Information Disclosure |
| **Component** | AdjacencyIndexObserver (observer.rs) |
| **Threat** | The async WriteObserver creates a consistency window where edge data exists in the edge table but not yet in the adjacency index. During this window: (a) Cypher queries miss recently written edges. (b) DELETE via CQL removes the edge but adjacency index still has the entry — phantom edges. (c) Observer crash/restart could leave permanent inconsistency. |
| **Likelihood** | 2 — Async by design; crash scenarios are realistic |
| **Impact** | 2 — Incorrect query results; phantom edges could leak data that was "deleted" |
| **Risk** | **4 (High)** |
| **Mitigation** | (1) Observer mutations go through the commit log for crash recovery. (2) Periodic background reconciliation job compares edge tables against adjacency index. (3) `DELETE` tombstones in the adjacency index must have timestamps ≥ the edge deletion. (4) Document the consistency model clearly for users. (5) Consider a "consistency check" graph query (`EXPLAIN CONSISTENCY`). |
| **Status** | **Partially mitigated.** Background `spawn_reconciliation()` task runs periodic scans. Commit log recovery implicit. Full reconciliation scan logic is a stub — compares edge table count but does not yet do row-level verification. |

### T6: Table Extension Metadata Poisoning

| Field | Value |
|-------|-------|
| **STRIDE** | Tampering, Elevation of Privilege |
| **Component** | ferrosa-schema (table extensions) |
| **Threat** | A user with ALTER TABLE permission sets `graph.type = 'edge'` and `graph.source = 'src_id'` on a table they control, causing the WriteObserver to generate adjacency index entries for their table. This could: (a) Pollute the adjacency index with attacker-controlled edges. (b) Create edges that point to vertices in tables the attacker can't normally read. |
| **Likelihood** | 2 — Requires ALTER TABLE permission, but that's a common grant |
| **Impact** | 2 — Adjacency index pollution, possible data leakage via graph traversals |
| **Risk** | **4 (High)** |
| **Mitigation** | (1) Setting `graph.*` extensions requires a new `Permission::GraphAdmin` or at minimum `Permission::Create` on the keyspace (not just ALTER on the table). (2) Validate that `graph.source_label` and `graph.target_label` reference existing vertex tables in the same keyspace. (3) Audit `graph.*` extension changes as a distinct event type. |
| **Status** | **Mitigated.** Schema validates `graph.*` extensions on CREATE/ALTER TABLE, requiring `Permission::Create` on the keyspace. System tables protected from DROP/ALTER. |

### T7: Cross-Protocol Data Leakage

| Field | Value |
|-------|-------|
| **STRIDE** | Information Disclosure |
| **Component** | Dual-access model |
| **Threat** | The adjacency index (`system_graph.adjacency`) is a system table. If readable via CQL, any user with access to the `system_graph` keyspace can enumerate all edges in the graph, bypassing per-table permissions. The adjacency index stores `vertex_id`, `edge_label`, `neighbor_id` — enough to reconstruct the graph structure without accessing the actual edge/vertex tables. |
| **Likelihood** | 2 — System keyspaces are often granted broader access |
| **Impact** | 2 — Graph structure leak even for users without edge table access |
| **Risk** | **4 (High)** |
| **Mitigation** | (1) `system_graph.adjacency` must be protected: CQL SELECT on it requires the same permissions as on the underlying edge tables. (2) Or: make `system_graph` completely inaccessible via CQL (internal-only). (3) The adjacency index should not store edge properties — only vertex IDs and table references. |
| **Status** | **Mitigated.** `system_graph` tables marked `is_system: true`. Schema rejects DROP/ALTER on system tables (`SystemTableProtected` error). CQL access to system_graph tables follows standard permission checks. |

### T8: HTTP Response Information Disclosure

| Field | Value |
|-------|-------|
| **STRIDE** | Information Disclosure |
| **Component** | HTTP endpoint (http.rs) |
| **Threat** | Error responses from the graph endpoint leak internal details: stack traces, table names, file paths, schema structure, Rust panic messages. |
| **Likelihood** | 2 — Common in HTTP APIs without explicit error sanitization |
| **Impact** | 1 — Information aids further attacks but is not directly exploitable |
| **Risk** | **2 (Medium)** |
| **Mitigation** | (1) Return generic error messages to clients; log details server-side. (2) Never expose Rust panic backtraces in HTTP responses. (3) Parse errors should show the query position but not internal parser state. |
| **Status** | **Mitigated.** `error_to_response()` in `http.rs` maps errors to appropriate HTTP status codes (400/403/408/500) with sanitized messages. `CatchPanicLayer` prevents panic backtraces reaching clients. |

### T9: WriteObserver Amplification Attack

| Field | Value |
|-------|-------|
| **STRIDE** | Denial of Service |
| **Component** | WriteObserver, StorageEngine |
| **Threat** | A user bulk-inserts into an edge table, triggering the async WriteObserver for every row. Each edge write generates 2 adjacency mutations (OUT + IN). A batch of 1M edge inserts generates 2M observer writes, potentially overwhelming the storage engine or commit log. The observer shares the same StorageEngine as CQL. |
| **Likelihood** | 2 — Bulk loads are normal operations |
| **Impact** | 2 — Observer backlog degrades all storage operations |
| **Risk** | **4 (High)** |
| **Mitigation** | (1) Observer write queue with bounded capacity and backpressure. (2) Rate-limit observer mutations (e.g., max 10K/sec per table). (3) Batch observer mutations (accumulate for 10ms, write as batch). (4) Monitor observer queue depth as a health metric. |
| **Status** | **Mitigated.** `WriteObserver` async mode uses bounded `mpsc` channel with backpressure. Observer mutations dispatched via `StorageEngine` with configurable queue depth. |

### T10: Audit Gap on Graph Operations

| Field | Value |
|-------|-------|
| **STRIDE** | Repudiation |
| **Component** | GraphEngine, HTTP endpoint |
| **Threat** | Graph read queries (MATCH) and write operations (CREATE/SET/DELETE via Cypher) are not audited. An attacker performs actions via the graph endpoint that don't appear in the audit log. CQL has comprehensive audit events; graph operations should too. |
| **Likelihood** | 3 — If audit events are not implemented for the graph endpoint, there's a 100% gap |
| **Impact** | 2 — Compliance failure, inability to trace data access |
| **Risk** | **6 (High)** |
| **Mitigation** | (1) Graph DDL operations (creating vertex/edge tables with extensions) should emit existing `TableCreated`/`TableAltered` audit events (they go through Schema). (2) Add new audit event types: `GraphQueryExecuted { query, keyspace, actor }`, `GraphMutationExecuted { query, keyspace, actor, vertices_affected, edges_affected }`. (3) Log source IP from HTTP connection. |
| **Status** | **Mitigated.** `GraphQueryExecuted` and `GraphMutationExecuted` audit event variants added to `AuditEventKind`. HTTP endpoint emits audit events with query, keyspace, and actor. Schema mutations emit existing `TableCreated`/`TableAltered` events. |

### T11: Unencrypted HTTP Transport

| Field | Value |
|-------|-------|
| **STRIDE** | Information Disclosure, Tampering |
| **Component** | HTTP endpoint |
| **Threat** | Graph endpoint serves plain HTTP. Credentials (if using HTTP Basic Auth) and query results travel in cleartext. Network observers can read graph data and steal credentials. |
| **Likelihood** | 2 — Depends on deployment; internal networks may be less exposed |
| **Impact** | 3 — Credential theft + data leakage |
| **Risk** | **6 (High)** |
| **Mitigation** | (1) Support TLS on the HTTP endpoint (shared TLS config with CQL). (2) Production mode (DeploymentMode::Production) should reject unencrypted HTTP. (3) Development mode can allow plaintext with a warning. |
| **Status** | **Mitigated.** `axum-server` with `tls-rustls` feature. `GraphHttpConfig` has `tls_cert_path`/`tls_key_path` fields. Production mode rejects plaintext via `require_tls` flag. |

## Risk Summary

| ID | Threat | Risk | Priority |
|----|--------|------|----------|
| T2 | Unauthenticated graph endpoint | **9 Critical** | **Mitigated** — HTTP Basic Auth middleware |
| T3 | Authorization bypass via traversal | **6 High** | **Mitigated** — per-hop auth in planner |
| T4 | DoS via expensive graph queries | **6 High** | **Mitigated** — timeout + fan-out limits |
| T10 | Audit gap on graph operations | **6 High** | **Mitigated** — audit event variants added |
| T11 | Unencrypted HTTP transport | **6 High** | **Mitigated** — TLS via axum-server + rustls |
| T1 | Parser exploitation | **4 High** | **Mitigated** — proptests + depth limit + CatchPanicLayer |
| T5 | Adjacency index inconsistency | **4 High** | **Mitigated** — full reconciliation: edge scan + orphan cleanup |
| T6 | Table extension poisoning | **4 High** | **Mitigated** — Permission::Create required for graph.* extensions |
| T7 | Cross-protocol data leakage | **4 High** | **Mitigated** — system tables protected, is_system flag |
| T9 | Observer amplification | **4 High** | **Mitigated** — bounded channel backpressure |
| T8 | HTTP error info disclosure | **2 Medium** | **Mitigated** — error sanitization in http.rs |
| T13 | Variable-length path explosion | **9 Critical** | **Open** — max hops cap + visited budget needed |
| T12 | SUBSCRIBE resource exhaustion | **4 High** | **Open** — per-connection + global subscription limits |
| T14 | Aggregation memory amplification | **4 High** | **Open** — group count + collect size limits |
| T15 | Bolt protocol injection | **6 High** | **Open** — deferred until Bolt implementation |
| T16 | Expression evaluator type confusion | **4 High** | **Open** — NULL propagation + type-safe comparison |

## Implementation Status

All critical and high-priority mitigations from Phase 1 have been implemented. Remaining work:

### Completed (Phase 1)

1. **HTTP authentication** — HTTP Basic Auth middleware, mapped to `Schema::authenticate()` with rate limiting
1. **Per-hop authorization** — Planner pre-validates all tables; executor checks per-hop
1. **Query resource limits** — Timeout (30s), max result rows (10K), max fan-out per hop (10K)
1. **Graph audit events** — `GraphQueryExecuted` and `GraphMutationExecuted` event variants
1. **TLS on HTTP** — `axum-server` with rustls, `require_tls` flag for production mode
1. **Parser depth limit** — Expression nesting capped, `CatchPanicLayer` at HTTP boundary
1. **Observer backpressure** — Bounded `mpsc` channel for async observer dispatch
1. **Extension permission guard** — `Permission::Create` required for `graph.*` extensions
1. **System table protection** — `is_system` flag, `SystemTableProtected` error on DROP/ALTER
1. **Error sanitization** — `error_to_response()` maps to HTTP status codes without leaking internals

### Completed (Phase 2 — Gap Closure Prep)

1. **Full adjacency reconciliation scan** — Two-phase: edge table scan + orphan cleanup with repair mutations
1. **CREATE/SET/DELETE execution** — Mutation paths through storage engine with proper auth checks

### Remaining (Follow-on)

1. **Graceful reconciliation shutdown** — `CancellationToken` for clean task termination
1. **Query memory budget tracking** — Per-query memory accounting not yet implemented
1. **Separate thread pool for graph queries** — Currently shares Tokio runtime with CQL

### New Threats from Gap Closure (T12–T16)

### T12: SUBSCRIBE Resource Exhaustion

| Field | Value |
|-------|-------|
| **STRIDE** | Denial of Service |
| **Component** | SUBSCRIBE executor, SSE endpoint |
| **Threat** | Attacker opens many SUBSCRIBE streams, each spawning a background re-query task. Unbounded subscriptions exhaust memory and executor threads. |
| **Likelihood** | 2 — Requires authenticated access but trivial to exploit |
| **Impact** | 2 — Degrades graph+CQL performance |
| **Risk** | **4 (High)** |
| **Mitigation** | (1) Per-connection subscription limit (e.g., 8, matching CQL). (2) Global subscription limit. (3) Subscription timeout/TTL. (4) Subscription registry with cleanup on disconnect. |

### T13: Variable-Length Path Explosion

| Field | Value |
|-------|-------|
| **STRIDE** | Denial of Service |
| **Component** | Variable-length path executor |
| **Threat** | `MATCH (a)-[*1..100]->(b)` on a dense graph produces exponential expansion. Even with per-hop fan-out limits, total work is `fan_out^max_hops`. |
| **Likelihood** | 3 — Trivial to craft |
| **Impact** | 3 — Memory exhaustion, CQL starvation |
| **Risk** | **9 (Critical)** |
| **Mitigation** | (1) Hard cap on max hops (e.g., 10). (2) Total visited-vertex budget (not just per-hop). (3) Existing query timeout applies. (4) BFS with cycle detection (visited set prevents infinite loops). |

### T14: Aggregation Memory Amplification

| Field | Value |
|-------|-------|
| **STRIDE** | Denial of Service |
| **Component** | Aggregation executor |
| **Threat** | `collect()` aggregation on a large result set materializes all values in memory. High-cardinality GROUP BY creates many accumulator instances. |
| **Likelihood** | 2 — Requires crafted query on large dataset |
| **Impact** | 2 — OOM or degraded performance |
| **Risk** | **4 (High)** |
| **Mitigation** | (1) Max group count limit. (2) `collect()` size limit. (3) Existing max_result_rows applies to pre-aggregation input. |

### T15: Bolt Protocol Injection

| Field | Value |
|-------|-------|
| **STRIDE** | Tampering, Elevation of Privilege |
| **Component** | Bolt codec (future) |
| **Threat** | Malformed PackStream messages cause deserialization panics or buffer overflows in the Bolt codec. Binary protocols have larger attack surface than HTTP/JSON. |
| **Likelihood** | 2 — Binary parsing is error-prone |
| **Impact** | 3 — Process crash or RCE |
| **Risk** | **6 (High)** |
| **Mitigation** | (1) Message size limits at frame layer. (2) Proptest/fuzz testing on PackStream decoder. (3) Catch panics at connection boundary. (4) Same auth+authz as HTTP path. |

### T16: Expression Evaluator Type Confusion

| Field | Value |
|-------|-------|
| **STRIDE** | Tampering |
| **Component** | Expression evaluator (eval.rs) |
| **Threat** | WHERE/RETURN expressions with type mismatches (e.g., `n.age > 'text'`) cause panics or incorrect filtering, leaking rows that should be excluded. |
| **Likelihood** | 2 — Type errors in dynamic evaluation are common |
| **Impact** | 2 — Information disclosure via incorrect filtering |
| **Risk** | **4 (High)** |
| **Mitigation** | (1) Type-safe comparison with explicit NULL handling (NULL propagation). (2) Type mismatch returns NULL (not panic). (3) Test coverage for all type combinations. |

## Assumptions

- ferrosa is deployed in a VPC with network-level controls (not directly internet-facing)
- TLS termination may happen at a load balancer, but internal traffic should also be encrypted
- Users have distinct RBAC roles (not everyone is superuser)
- The async observer will eventually catch up under normal load
- S3 bucket policies provide an additional access control layer for data at rest

## Open Questions

1. **Should the graph endpoint share the CQL TCP port?** A single port with protocol detection would simplify TLS configuration but add complexity.
2. **Should graph RBAC use existing CQL permissions or add graph-specific ones?** E.g., `Permission::Traverse` as distinct from `Permission::Select`.
3. **How does Bolt protocol authentication work?** When Bolt is added in a later phase, it has its own auth handshake. Plan for this now?
4. **Should the adjacency index be per-keyspace or global?** Per-keyspace isolates tenants; global enables cross-keyspace traversals.
5. **Is there a need for query audit sampling?** High-traffic graph endpoints may generate too many audit events. Configurable sampling rate?
