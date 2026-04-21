---
type: todo
priority: P1
status: draft
created: 2026-04-20
updated: 2026-04-20
---

# Implement public Cypher mutations so clients can stop writing graph tables directly

## Why

Ferrosa already exposes public graph **reads** through the Cypher HTTP
endpoint, but graph **writes** are still effectively internal-table
mutations.

That blocks the intended client boundary for `ferrosa-memory` and any
other application that wants to write graph state through Ferrosa at
the correct abstraction level:

- direct CQL to **app-owned** tables is fine
- direct CQL to **graph-owned** backing tables is not
- graph writes should go through a public graph mutation API owned by
  Ferrosa

Today, `ferrosa-memory` still writes graph-owned tables like
`typed_edges`, `folded_into`, `mentioned_in`, `co_occurs_with`, and
`supersedes` directly because there is no complete public mutation path
it can use instead.

This is not just a client ergonomics problem. It is an abstraction and
correctness problem:

- graph invariants live in Ferrosa, not in clients
- schema evolution of graph backing tables should not silently break
  clients
- auth/role isolation depends on keeping graph-table `MODIFY`
  permissions inside Ferrosa-owned components
- audit, telemetry, and reconciliation hooks should observe writes at
  the graph layer

The current role-auth rollout makes this concrete: `app_reader` should
not have `MODIFY` on graph-owned tables. That means clients need a
public mutation path if graph-writing features are to keep working.

## Proposed

Implement a public Cypher mutation path in Ferrosa, owned by the graph
engine, and make it suitable for application clients such as
`ferrosa-memory`.

The target is not “full Cypher overnight.” The target is the minimum
public mutation surface needed to replace direct graph-table writes
safely, starting with the idempotent operations clients actually need.

### Phase 1: minimal public mutation support

Support these operations through the existing public graph surface:

1. `CREATE` node
2. `CREATE` relationship
3. `SET` properties on created/matched bindings
4. `DELETE` relationship / node where already supported by planner shape
5. `MERGE` node
6. `MERGE` relationship

`MERGE` is the real unblocker. Without it, clients have to issue
non-atomic `MATCH` + `CREATE` sequences and rebuild graph-level
correctness themselves.

### Phase 2: client-safe semantics

The mutation path should:

- run through the same public graph HTTP endpoint family as reads
- enforce auth/permissions at the graph layer
- maintain adjacency/reconciliation/index invariants inside Ferrosa
- fail loudly with explicit graph/Cypher errors
- avoid exposing graph backing table names as part of the contract

### Phase 3: client migration proof

Add end-to-end proof that a client can perform the write shapes needed
by `ferrosa-memory` without naming graph-owned tables directly:

- idempotent typed edge upsert
- `FOLDED_INTO`
- `MENTIONED_IN`
- `SUPERSEDES`
- repeated relationship writes that should not duplicate graph state

## Required API/behavior

At minimum, the public graph API should support requests like:

```cypher
MERGE (a:Entity {entity_id: $src})
MERGE (b:Entity {entity_id: $dst})
MERGE (a)-[r:TYPED_EDGE {edge_type: $edge_type}]->(b)
SET r.weight = $weight,
    r.session_id = $session_id,
    r.tenant_id = $tenant_id,
    r.created_at = $created_at,
    r.metadata = $metadata
RETURN r
```

and

```cypher
MERGE (child:Fold {fold_id: $child_fold_id})
MERGE (parent:Fold {fold_id: $parent_fold_id})
MERGE (child)-[r:FOLDED_INTO]->(parent)
SET r.session_id = $session_id,
    r.tenant_id = $tenant_id,
    r.created_at = $created_at
RETURN r
```

Exact syntax can vary, but the contract must provide:

- parameterized public mutation queries
- idempotent upsert semantics for common client write shapes
- graph-engine-owned invariant maintenance

## Non-goals

- Full openCypher/GQL mutation coverage in one pass
- Replacing direct CQL for app-owned tables
- Moving Datalog into Ferrosa
- Exposing graph backing table layout as public API

## Acceptance criteria

- [ ] Public graph HTTP/Cypher mutation support exists for the minimum
      client-unblocking set: `CREATE`, `SET`, and `MERGE` for nodes and
      relationships.
- [ ] `MERGE` is implemented end-to-end in the AST, parser, planner,
      and executor, not only documented.
- [ ] Graph mutations through the public path preserve graph-engine
      invariants and adjacency/index maintenance; clients do not need to
      touch graph backing tables directly.
- [ ] Auth/permission checks apply at the graph mutation layer so a
      role like `app_reader` can use the public graph API without
      needing direct `MODIFY` on `typed_edges`, `folded_into`,
      `mentioned_in`, `co_occurs_with`, `supersedes`, or derived-edge
      tables.
- [ ] Integration tests prove repeated `MERGE` requests are idempotent
      and do not create duplicate edges.
- [ ] Integration tests prove a client can create the graph write shapes
      needed by `ferrosa-memory` through public Cypher alone.
- [ ] Public docs stop implying “mutations coming soon” once the
      supported subset is actually live, and they describe the supported
      mutation subset precisely.

## Suggested TDD slices

1. Parse `MERGE` into a real AST variant.
2. Plan `MERGE` for node upsert.
3. Plan `MERGE` for relationship upsert.
4. Execute `MERGE` with idempotent semantics.
5. Support `SET` over matched/merged bindings.
6. Add auth checks for graph mutations.
7. Add end-to-end HTTP tests using the public `/graph/query` path.
8. Add migration-proof client-shape tests for typed edges and core
   relationship kinds.

## Test scenarios

### Parser / planner

- [ ] `MERGE (n:Entity {entity_id: $id}) RETURN n` parses successfully.
- [ ] `MERGE (a)-[r:TYPED_EDGE {edge_type: $edge_type}]->(b)` parses
      successfully.
- [ ] Unsupported mutation syntax fails with explicit parser/planner
      errors rather than silent fallback.

### Executor semantics

- [ ] Repeating the same node `MERGE` twice yields one node.
- [ ] Repeating the same relationship `MERGE` twice yields one edge.
- [ ] `SET` updates relationship properties after `MERGE`.
- [ ] Missing bound endpoints for relationship creation return a clear
      execution error.

### Public API integration

- [ ] `POST /graph/query` accepts the supported mutation subset and
      returns a successful JSON response.
- [ ] Authenticated mutation requests fail with `Unauthorized` when the
      caller lacks graph-mutation rights.
- [ ] Mutations become visible through subsequent public Cypher reads.

### Client migration proof

- [ ] A typed-edge write used by `ferrosa-memory` can be expressed as a
      public Cypher mutation with no direct table references.
- [ ] `FOLDED_INTO`, `MENTIONED_IN`, and `SUPERSEDES` relationship
      creation work through the same public path.
- [ ] A regression test proves no client migration recipe requires
      direct `INSERT INTO typed_edges` or sibling tables.

## Related

- `specs/todo/todo-enable-cql-role-auth-for-graph-table-isolation.md`
- `specs/coverage/multimodel-coverage.md`
- `/Users/bkearns/src/ferrosa-memory/specs/todo/bug-ferrosa-memory-bypasses-graph-api-for-writes.md`
- `/Users/bkearns/src/ferrosa-memory/specs/todo/todo-extend-ferrosa-memory-graph-client-with-cypher-writes.md`
