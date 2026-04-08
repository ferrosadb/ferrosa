---
type: bug
priority: P0
reported-by: human
implemented-by: ""
verified-by: ""
created: 2026-04-05
updated: 2026-04-05
source: manual
source-location: "tools/forge/specs/bug-ingest-dangling-edge-references.md"
---

# Entity store loses entities from prior ingests

## Description

When multiple codebases are ingested sequentially via `frg ingest --cql`, entities from earlier ingests are lost. Only the last codebase's entities survive. All ingests use the same `(tenant_id, session_id)` partition, with unique `entity_id` clustering keys, so Cassandra upsert semantics should not cause overwrites.

## Evidence

3 codebases ingested sequentially into the same cluster with session `00000000-...`:
- ferrosa-memory: 2,865 entities reported inserted
- ferrosa: 11,029 entities reported inserted
- ferrosa-dbaas: 2,015 entities reported inserted

**Post-ingest:** Only 2,482 entities survive. Breakdown:
- 14 crates — **all from ferrosa-dbaas** (the last ingest)
- 76 modules, 1,300 functions — match ferrosa-dbaas counts
- 0 crates from ferrosa (expected ~30) or ferrosa-memory (expected ~10)
- 74 person entities from paper ingestion (survived)
- 19,138 edges — most pointing to entities that no longer exist

**13,427 entities were lost.** The Python loader reported successful INSERT for all 15,909 but only the last batch persists.

## Root Cause Hypotheses

1. **Ferrosa storage engine compaction/GC**: The LSM-tree storage may be dropping older SSTables during compaction, especially under heavy write load from sequential bulk ingests. If compaction runs between ingests, it could discard tombstoned data incorrectly.

2. **S3 tiering race condition**: If entity_store data is being tiered to S3 between ingests, a subsequent compaction might not see the S3-resident data and treat the partition as empty.

3. **Write-ahead log truncation**: If the commit log is truncated before the first ingest's SSTables are flushed, those writes are lost on restart.

4. **Python cassandra-driver consistency**: The loader uses default consistency level (likely ONE). If the local replica acknowledges the write but fails to replicate before the next ingest overwrites, data is lost.

5. **Partition size limit**: 15,909 entities in one partition `(tenant_id, session_id)` may hit a ferrosa partition size limit, causing silent truncation.

## Expected Behavior

All entities from all ingests should persist. The `entity_store` partition should hold the union of all ingested entities since the entity_ids are unique UUIDv5 values.

## Reproduction (confirmed 2026-04-05)

```bash
# Step 1: Insert 100 test entities via direct CQL — all persist
# Step 2: Insert 500 more via direct CQL — all persist (total 634)
# Step 3: frg ingest ferrosa-memory (2,873 entities) — all prior data survives (total 3,847)
# Step 4: frg ingest ferrosa (11,579 entities) — PRIOR DATA LOST

# Results after step 4:
#   Total entities: 2,210 (was 3,191 before step 4)
#   Batch 1 (100 direct CQL): 100/100 survived
#   Batch 2 (500 direct CQL): 116/500 survived (384 LOST)
#   ferrosa-memory-core: present but deduped
```

**Key finding:** Small writes (100, 500 entities) survive across ingests. But a LARGE write (11,579 entities) to the same partition causes ~1,000 existing entities to disappear. This points to a memtable flush or SSTable compaction bug where the new SSTable REPLACES instead of MERGING with existing SSTables.

The loss is selective — not all prior data is lost, just a portion. This is consistent with an SSTable replacement during compaction where one input SSTable is dropped instead of merged.

## Impact

- Knowledge graph is structurally incomplete — only the last-ingested codebase is queryable
- 76.8% of edges are dangling
- Re-ingesting doesn't fix it — it just overwrites again

## Re-test After Fix d6b9dcd (manifest CAS retry)

**Result: FIX DID NOT RESOLVE THE BUG.**

```
Step 1: 100 canaries                    → 100 entities
Step 2: + fmem ingest (2,872)           → 2,864 entities, canaries: 100/100 ✓
Step 3: + ferrosa ingest (11,071)       → 1,250 entities, canaries: 3/100 ✗ LOST 97
Step 4: + dbaas ingest (2,015)          → 3,228 entities, canaries: 3/100 ✗
```

Worse than the pre-fix run. The large ferrosa ingest (Step 3) dropped entity count from 2,864 to 1,250 and destroyed 97 of 100 canary entities.

The manifest CAS fix was a real bug but not the root cause of this data loss. The root cause is still active — likely in the LSM compaction merge or memtable flush path.

## Re-test After Fix b83c3b4 (compaction swap empty path)

**Result: FIX DID NOT RESOLVE THE BUG.**

```
Step 1: 100 canaries inserted
Step 2: + fmem ingest → 2,864 entities, canaries: 100/100 ✓
Step 3: MID-INGEST snapshot → 6,141 entities, canaries: 3/100 ✗

Data loss happens DURING the large write, not after compaction completes.
Canaries drop from 100 to 3 while the ferrosa ingest is still running.
```

This rules out post-compaction manifest issues. The loss occurs during concurrent flush+compaction while writes are active. The bug is in the memtable→SSTable flush or the compaction merge that runs concurrently with writes.

## Diagnostic Logging Request

We need debug-level logging added to the following hot paths to capture the exact moment data disappears. The reproduction is reliable: run `frg ingest --cql localhost:19042 /path/to/ferrosa` (~11K entities) while prior data exists in the same partition.

### 1. Memtable flush path
Log BEFORE and AFTER each memtable flush:
```
DEBUG flush_memtable_start: table={table} partition=({tenant_id},{session_id}) memtable_size={rows} new_sstable_id={id}
DEBUG flush_memtable_done: table={table} sstable_id={id} rows_written={n} file_size={bytes} path={path}
```

### 2. Compaction merge path
Log every compaction merge showing inputs and outputs:
```
DEBUG compaction_start: table={table} strategy={strategy} input_sstables=[{id1},{id2},...] total_input_rows={n}
DEBUG compaction_merge_read: sstable={id} rows_read={n}
DEBUG compaction_done: table={table} output_sstable={new_id} rows_written={n} input_sstables_deleted=[{id1},{id2}]
```

### 3. SSTable registration/deregistration
Log when SSTables are added to or removed from the active set:
```
DEBUG sstable_register: table={table} id={id} rows={n} path={path}
DEBUG sstable_deregister: table={table} id={id} reason={flush_replace|compaction_replace|drop_table}
```

### 4. Manifest updates
Log every manifest CAS attempt:
```
DEBUG manifest_update: table={table} action={add|remove|swap} sstable_ids=[...] cas_attempt={n} success={bool}
```

### 5. Canary probe (specific key tracing)
Add an env var `FERROSA_CANARY_PROBE_ID` that traces a single entity_id through all paths:
```rust
// In memtable write:
if key matches canary_probe_id {
    tracing::warn!(table, entity_id, "CANARY: memtable write");
}
// In SSTable read:
if key matches canary_probe_id {
    tracing::warn!(table, entity_id, sstable_id, "CANARY: sstable read");
}
// In compaction input:
if key matches canary_probe_id {
    tracing::warn!(table, entity_id, input_sstable, "CANARY: compaction input");
}
// In compaction output:
if key matches canary_probe_id {
    tracing::warn!(table, entity_id, output_sstable, "CANARY: compaction output");
}
// MISSING from compaction output:
// If key was in input but NOT in output, this is the data loss:
tracing::error!(table, entity_id, "CANARY: MISSING from compaction output — DATA LOSS HERE");
```

### How to run reproduction with logging

```bash
# canary-0 UUID: python3 -c "import uuid; print(uuid.uuid5(uuid.NAMESPACE_DNS, 'canary-0'))"
export FERROSA_CANARY_PROBE_ID=$(python3 -c "import uuid; print(uuid.uuid5(uuid.NAMESPACE_DNS, 'canary-0'))")
export RUST_LOG=debug

podman compose up -d
# Wait for cluster formation (~45s)

# Step 1: Insert canaries
python3 -c "..." # (see reproduction steps above)

# Step 2: Small ingest (canaries survive this)
frg ingest --cql localhost:19042 /path/to/ferrosa-memory

# Step 3: Large ingest (canaries lost during this)
frg ingest --cql localhost:19042 /path/to/ferrosa

# Step 4: Check logs
podman logs ferrosa-memory_node1_1 2>&1 | grep "CANARY"
# Expected: see CANARY write, CANARY sstable read... then either
# CANARY compaction input WITHOUT CANARY compaction output (dropped in merge)
# or CANARY sstable deregister without re-register (SSTable deleted)
```

### What we expect to find

The canary entity is written and flushed to SSTable A. During the large ingest, a memtable flush creates SSTable B. Compaction merges A+B into C. The bug is one of:
- **A is not included in compaction input** (not in active set when compaction starts)
- **A's rows are dropped during merge** (merge logic error)  
- **A is deregistered but C doesn't include its rows** (swap bug)
- **The memtable containing the canary is flushed with empty/partial data** (flush race)

The logging will show exactly which step loses the data.

## Diagnostic Log Evidence (2026-04-06, commit bc50a29)

**ROOT CAUSE FOUND: Corrupt SSTable**

```
WARN ferrosa_storage::store: skipping corrupt SSTable partition during read — data may be incomplete
  error=I/O error: cell value length 3064389195864 exceeds maximum (268435456), likely corrupt SSTable
  sstable_index=0
```

SSTable index 0 for `typed_edges` has a corrupt cell value length of **3,064,389,195,864 bytes** (3 trillion). This is clearly garbage — either an uninitialized length field, a pointer written as a length, or byte-order corruption.

The storage engine correctly detects the corruption and skips the partition, but this means ALL data in that SSTable is silently dropped on reads. The data was written correctly (it's readable immediately after write) but becomes corrupt after flush/compaction writes a bad SSTable.

**This also explains the entity_store loss** — if the entity_store SSTable has similar corruption, entire partitions are skipped during reads.

### Next Step

The SSTable WRITER is producing corrupt files. The bug is in:
- `ferrosa-sstable` crate — the SSTable serialization code
- Specifically the cell value length encoding during flush or compaction output
- The value `3064389195864` in hex is `0x2C9B3D22D58` — doesn't match any obvious sentinel, suggesting uninitialized memory or a buffer overrun

Need to add assertions in the SSTable writer to validate cell value lengths before writing, and in the flush path to verify SSTable integrity after write (read-back verification).

## Additional Diagnostic Evidence (compaction log)

```
[compaction] skipping corrupt SSTable 2: invalid data: corrupted DeletionTime flags: 0x9d
[compaction] skipping corrupt SSTable 5: invalid data: corrupted DeletionTime flags: 0xca
[compaction] skipping corrupt SSTable 3: invalid data: corrupted DeletionTime flags: 0xbd
[compaction] skipping corrupt SSTable 4: invalid data: corrupted DeletionTime flags: 0xe6
[compaction] task failed: no partitions to compact
```

FOUR SSTables (2, 3, 4, 5) have corrupt DeletionTime flags. Compaction skips all of them and fails. Data in those SSTables is lost on subsequent reads.

The DeletionTime flags (0x9d, 0xca, 0xbd, 0xe6) are all invalid — valid flags should be 0x00 (live) or specific Cassandra-defined values. These look like arbitrary byte values, confirming the SSTable writer is producing corrupt output.

**Root cause is in ferrosa-sstable serialization**: the DeletionTime flags field is being written with garbage values during flush. This corrupts the SSTable, making it unreadable by both the read path and compaction.

## Replication Factor

agent_memory uses NetworkTopologyStrategy RF=3 — data is replicated to all 3 nodes. The corruption is NOT from replication loss. All 3 nodes have the same corrupt SSTables because the corruption happens during flush on the coordinator BEFORE replication.

RF=3 cannot protect against SSTable writer bugs — all replicas get the same corrupt data.

## Fix Verified (2026-04-06, commit a6d1006)

**fix: P0 data scatter — peers marked Joining caused every node to write locally**

Root cause: peers stuck in `Joining` state caused the coordinator to treat all nodes as unavailable for token-aware routing, falling back to local writes. Data scattered across nodes instead of replicating to the correct token owners.

Reproduction result:
```
Step 1: 100 canaries inserted
Step 2: + fmem ingest → 2,863 entities, canaries: 100/100 ✓
Step 3: + ferrosa ingest (11K) → 13,350 entities, canaries: 100/100 ✓
```

No corrupt SSTables. No data loss. No "skipping corrupt" messages.
**Status: VERIFIED FIXED**
