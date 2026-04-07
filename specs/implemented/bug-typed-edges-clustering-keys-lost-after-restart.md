---
type: bug
priority: P1
reported-by: agent
implemented-by: ""
verified-by: ""
created: 2026-04-06
updated: 2026-04-06
source: ferrosa-memory DIKW pipeline test
source-location: "ferrosa-memory/scripts/test-dikw-pipeline.sh"
branch: "fix/compaction-data-loss @ 286c9f0"
---

# typed_edges clustering keys return NULL after restart

## Description

The `typed_edges` table has 3 clustering key columns: `(src_id uuid, edge_type text, dst_id uuid)`. After data is flushed to SSTables and the node restarts, all clustering key values read back as NULL. Regular columns (weight, metadata, created_at) and partition keys (tenant_id, session_id) are correct.

This is the same class of bug as the P0 entity_store corruption (unsorted cells) but for a different table with a different clustering key structure (3 columns: UUID, text, UUID).

## Proof: Not a client bug

Direct CQL INSERT reads back correctly from memtable but returns NOT FOUND after restart:

```python
# Before restart:
INSERT INTO typed_edges (..., src_id, edge_type, dst_id, ...) VALUES (..., 'aaaaaaaa-...', 'post_flush_test', 'bbbbbbbb-...', ...)
SELECT ... WHERE src_id = 'aaaaaaaa-...' → FOUND, all values correct

# After restart:
SELECT ... WHERE src_id = 'aaaaaaaa-...' → NOT FOUND
SELECT ... LIMIT 3 → src_id=None, edge_type=None, dst_id=None
```

## Evidence

68,134 typed_edges created by `skilltools ingest`. All have NULL clustering keys after restart:

```python
{'tenant_id': '9a5f8fbf-...', 'session_id': '00000000-...', 
 'src_id': None, 'edge_type': None, 'dst_id': None,  # ALL NULL
 'weight': 0.6, 'metadata': '', 'created_at': '2026-04-06T23:13:27'}
```

entity_store (single UUID CK) was fixed by `e9703f8` (cell sorting). typed_edges has a different CK structure: `(uuid, text, uuid)` — the mixed types may require different sorting or serialization handling.

## Reproduction

```bash
# Fresh cluster on 286c9f0
rm -rf ~/data/ferrosa-memory/node{1,2,3}/*
podman compose up -d
# Wait for healthy

python3 -c "
from cassandra.cluster import Cluster
import uuid
c = Cluster(['localhost'], port=19042, protocol_version=4)
s = c.connect('agent_memory')
t = uuid.UUID('9a5f8fbf-d842-4d30-8ea5-1aa931e618a8')
z = uuid.UUID(int=0)
src = uuid.uuid4()
dst = uuid.uuid4()
s.execute('INSERT INTO agent_memory.typed_edges (tenant_id, session_id, src_id, edge_type, dst_id, weight, metadata, created_at) VALUES (%s,%s,%s,%s,%s,%s,%s,toTimestamp(now()))', (t, z, src, 'test', dst, 1.0, ''))
# Verify in memtable
row = s.execute('SELECT src_id, edge_type, dst_id FROM agent_memory.typed_edges WHERE tenant_id = %s AND session_id = %s AND src_id = %s AND edge_type = %s AND dst_id = %s', (t, z, src, 'test', dst)).one()
print(f'Before restart: src={row.src_id} type={row.edge_type} dst={row.dst_id}')  # Works
c.shutdown()
"

podman compose stop && podman compose up -d
# Wait for healthy

python3 -c "
from cassandra.cluster import Cluster
import uuid
c = Cluster(['localhost'], port=19042, protocol_version=4)
s = c.connect('agent_memory')
t = uuid.UUID('9a5f8fbf-d842-4d30-8ea5-1aa931e618a8')
z = uuid.UUID(int=0)
rows = list(s.execute('SELECT src_id, edge_type, dst_id FROM agent_memory.typed_edges WHERE tenant_id = %s AND session_id = %s LIMIT 3', (t, z)))
for r in rows:
    print(f'After restart: src={r.src_id} type={r.edge_type} dst={r.dst_id}')  # All None
c.shutdown()
"
```

## Table Schema

```cql
CREATE TABLE typed_edges (
    tenant_id uuid,
    session_id uuid,
    src_id uuid,          -- clustering key 1
    edge_type text,       -- clustering key 2  (TEXT, not UUID)
    dst_id uuid,          -- clustering key 3
    weight double,
    metadata text,
    created_at timestamp,
    PRIMARY KEY ((tenant_id, session_id), src_id, edge_type, dst_id)
);
```

The mixed clustering key types (uuid, text, uuid) may be relevant — the cell sorting fix in `e9703f8` may only handle homogeneous CK types or the text column may serialize differently.

## Root Cause Hypothesis

The cell sorting fix (`e9703f8`) sorts cells by column index for `build_row`. For entity_store (single UUID CK), this works. For typed_edges with 3 CK columns of mixed types (uuid, text, uuid), the CK component serialization or BTI index construction may have a different code path that still produces corrupt indices.

The `wanted 97 bytes, got 34` error in tool_usage_log (P2 bug) may also be related — different table, different CK structure, same symptom.

## Impact

- **P1**: 68k edges written but unreadable after restart — entire knowledge graph connectivity lost
- **Viz shows no connections** between entities
- **Consolidation returns 0 connections** — graph appears disconnected
- **Blocks DIKW pipeline** Information→Knowledge→Wisdom layers
