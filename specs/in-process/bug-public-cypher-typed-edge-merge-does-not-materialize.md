# Bug: Public Cypher `TYPED_EDGE` writes persist to CQL but are not readable back through graph queries

## Summary

Ferrosa's public graph HTTP mutation path now accepts canonical typed-edge
`MERGE` writes and persists rows into `agent_memory.typed_edges`, but follow-up
public graph reads still do not return those relationships correctly.

This blocks `ferrosa-memory` Sprint 9 graph-write cutover. The client is
already routing typed-edge writes through the public graph API and failing
loudly instead of falling back to direct CQL writes.

## Expected

This canonical public mutation shape should create or upsert a typed edge:

```cypher
MERGE (a:Entity {entity_id: '11111111-2222-3333-4444-555555555555'})
MERGE (b:Entity {entity_id: '66666666-7777-8888-9999-aaaaaaaaaaaa'})
MERGE (a)-[r:TYPED_EDGE {edge_type: 'related_to'}]->(b)
SET r.weight = 1.0
RETURN r
```

Expected results:

1. `POST /graph/query` returns a success payload.
2. A row exists in `agent_memory.typed_edges` for `(src_id, edge_type, dst_id)`.
3. A follow-up Cypher `MATCH` with the same identifiers returns the written
   edge and properties.

## Actual

Observed against the local three-node auth-enabled cluster used by
`ferrosa-memory`:

1. `ferrosa-memory` tool `create_edge` returns success.
2. Direct execution of the canonical Cypher `MERGE` shape returns success.
3. Follow-up CQL shows the expected row in `agent_memory.typed_edges`.
4. Follow-up Cypher `MATCH ...-[r:TYPED_EDGE]->... RETURN r.edge_type, r.weight`
   still returns no matching rows, or returns unrelated edge labels.

### Re-verified on `d6573c1`

Retested on the local three-node auth-enabled cluster rebuilt from Ferrosa
commit `d6573c141071716eeade7ec87213c2decb8d0b33`, with `ferrosa-memory`
running successfully on `18765/18766`.

Live observations:

- `SPARQL` passthrough works through `ferrosa-memory`.
- direct canonical public typed-edge `MERGE` returns success:

```json
{"columns":["status"],"rows":[["merged 5 vertices, 1 properties updated"]]}
```

- exact CQL readback shows the `related_to` row exists:

```json
{"count":1,"rows":[{"src_id":"41753309-7297-454e-8f2d-c6546740cf2b","edge_type":"related_to","dst_id":"f6ffe258-9194-470d-9811-5b3e23b33103","weight":1.0}]}
```

- `ferrosa-memory` `create_edge` also returns success:

```json
{"created":true,"dst_id":"f6ffe258-9194-470d-9811-5b3e23b33103","edge_type":"references","src_id":"41753309-7297-454e-8f2d-c6546740cf2b","weight":0.75}
```

- exact CQL readback also shows that `references` row exists:

```json
{"count":1,"rows":[{"src_id":"41753309-7297-454e-8f2d-c6546740cf2b","edge_type":"references","dst_id":"f6ffe258-9194-470d-9811-5b3e23b33103","weight":0.75}]}
```

- but public graph readback is still wrong:

```json
{"columns":["r.edge_type","r.weight"],"rows":[]}
```

and a broader read on the same endpoints returned unrelated graph edges:

```json
{"columns":["r.edge_type","r.weight"],"rows":[["TAGGED_AS",1.0],["TAGGED_AS",1.0]]}
```

That narrows the current bug to:

> public typed-edge writes now persist correctly, but public graph query
> readback for those typed edges is still not honoring the expected
> `TYPED_EDGE` relationship semantics.

### Re-verified on `23ca6c4`

Retested after rebuilding the local three-node cluster from Ferrosa commit
`23ca6c4`.

The failure mode is narrower now:

- `ferrosa-memory` no longer sends the old invalid `SET r.tenant_id ...`
  shape for typed edges.
- Direct canonical graph HTTP mutation returns:

```json
{"columns":["status"],"rows":[["merged 5 vertices, 1 properties updated"]]}
```

- `ferrosa-memory` `create_edge` also returns success:

```json
{"created":true,"dst_id":"...","edge_type":"related_to","src_id":"...","weight":0.75}
```

- But readback still shows no row in `agent_memory.typed_edges`.

So the remaining bug is no longer a client validation mismatch. It is now:

> canonical public `TYPED_EDGE` `MERGE` acknowledges success but does not
> materialize a typed edge row.

### Re-verified on `02b0629`

Retested after rebuilding the local three-node cluster from Ferrosa commit
`02b0629`.

The new negative-path validation is present now:

- canonical typed-edge `MERGE` against fresh UUIDs returns:

```json
{"error":"validation error: MERGE on 'agent_memory.entity_store' is missing required scoped key columns; match existing scoped vertices or set the missing key properties explicitly"}
```

So the old false-positive "success with no row" path for unscoped entities is
improved.

But the positive inference path is still not working from the public graph
surface:

- existing `Entity` vertices are visible with scoped properties:

```json
{"columns":["a.entity_id","a.tenant_id","a.session_id"],"rows":[["41753309-7297-454e-8f2d-c6546740cf2b","6792702e-2a9c-4465-ba65-ba100b5aaafa","909e2671-aea0-534a-83bc-bb5efc544b0f"],["f6ffe258-9194-470d-9811-5b3e23b33103","6792702e-2a9c-4465-ba65-ba100b5aaafa","909e2671-aea0-534a-83bc-bb5efc544b0f"]]}
```

- canonical public merge against those existing IDs still returns success:

```json
{"columns":["status"],"rows":[["merged 5 vertices, 1 properties updated"]]}
```

- but follow-up public graph readback still returns no matching edge:

```json
{"columns":["r.edge_type","r.weight"],"rows":[]}
```

That narrows the remaining bug further:

> `02b0629` fixes the validation path for unscoped entities, but canonical
> typed-edge `MERGE` still does not materialize a readable relationship when
> scope should be inferable from existing scoped entity rows.

## Reproduction

### Environment

- Ferrosa cluster from `../ferrosa`
- `ferrosa-memory` local smoke stack on `28765/28766`
- graph auth: `ferrosa_admin / ferrosa_admin`
- workbench auth proxy via `smoke / smoke-pass`

### Repro 1: `ferrosa-memory` MCP path

Create two entities:

```json
{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"upsert_entity","arguments":{"entity_name":"sprint-9-smoke-src3","entity_type":"concept","context_snippet":"Unique src entity for sprint 9 graph smoke","session_id":"11111111-1111-1111-1111-111111111111","confidence":1.0}}}
```

```json
{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"upsert_entity","arguments":{"entity_name":"sprint-9-smoke-dst3","entity_type":"concept","context_snippet":"Unique dst entity for sprint 9 graph smoke","session_id":"11111111-1111-1111-1111-111111111111","confidence":1.0}}}
```

Observed IDs:

- src: `a5710d35-8dec-4ac5-b862-1abcd500954c`
- dst: `e41d40b3-dc55-4b76-8793-cfaf5ca4e1b3`

Create the typed edge:

```json
{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"create_edge","arguments":{"src_entity_id":"a5710d35-8dec-4ac5-b862-1abcd500954c","dst_entity_id":"e41d40b3-dc55-4b76-8793-cfaf5ca4e1b3","edge_type":"related_to","weight":0.75,"session_id":"11111111-1111-1111-1111-111111111111"}}}
```

Observed result:

```json
{"created":true,"dst_id":"e41d40b3-dc55-4b76-8793-cfaf5ca4e1b3","edge_type":"related_to","src_id":"a5710d35-8dec-4ac5-b862-1abcd500954c","weight":0.75}
```

Now query CQL through the workbench:

```sql
SELECT src_id, edge_type, dst_id, weight, session_id
FROM agent_memory.typed_edges
WHERE src_id = a5710d35-8dec-4ac5-b862-1abcd500954c
  AND dst_id = e41d40b3-dc55-4b76-8793-cfaf5ca4e1b3
ALLOW FILTERING
```

Observed:

```json
{"count":0,"rows":[]}
```

### Repro 2: direct canonical graph mutation

Execute:

```cypher
MERGE (a:Entity {entity_id: '11111111-2222-3333-4444-555555555555'})
MERGE (b:Entity {entity_id: '66666666-7777-8888-9999-aaaaaaaaaaaa'})
MERGE (a)-[r:TYPED_EDGE {edge_type: 'related_to'}]->(b)
SET r.weight = 1.0
RETURN r
```

Then check:

```sql
SELECT src_id, edge_type, dst_id, weight
FROM agent_memory.typed_edges
WHERE src_id = 11111111-2222-3333-4444-555555555555
  AND dst_id = 66666666-7777-8888-9999-aaaaaaaaaaaa
ALLOW FILTERING
```

Observed:

```json
{"count":0,"rows":[]}
```

And the public graph read:

```cypher
MATCH (a:Entity {entity_id: '11111111-2222-3333-4444-555555555555'})
      -[r:TYPED_EDGE {edge_type: 'related_to'}]->
      (b:Entity {entity_id: '66666666-7777-8888-9999-aaaaaaaaaaaa'})
RETURN count(r)
```

Observed:

```json
{"rows":[[0]]}
```

## Why this matters

`ferrosa-memory` can no longer safely validate graph-write cutover if public
typed-edge writes persist to CQL but the public graph read surface cannot read
them back correctly. Falling back to direct reads against internal graph tables
would violate the public-boundary design.

## Acceptance criteria

- [ ] The canonical `MERGE (a)-[r:TYPED_EDGE {edge_type: ...}]->(b)` shape
      materializes a row in `agent_memory.typed_edges`.
- [ ] A follow-up Cypher `MATCH` sees the relationship immediately.
- [ ] `ferrosa-memory` `create_edge` succeeds and the edge is visible through
      both CQL readback and graph readback.
