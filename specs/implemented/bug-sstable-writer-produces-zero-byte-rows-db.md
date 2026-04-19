---
type: bug
priority: P0
reported-by: ferrosa-memory codebase ingest (2026-04-17)
implemented-by: ""
verified-by: ""
created: 2026-04-17
updated: 2026-04-18
---

# SSTable writer produces 0-byte `Rows.db` — reader then parses corrupt DeletionTime flags

## Observed

On a **fresh** 3-node cluster (image built from `main @ 45fc1fe`, no carried-
over data), after ~1 hour of normal write traffic (seed 79 skills via
`ingest_skill` + ingest 3 codebases via forge's `ingest`), each node has
exactly **15 SSTable directories where `Rows.db` is a 0-byte file**:

```
$ find ~/data/ferrosa-memory/node1/sstables -type f -size 0 | \
    awk -F- '{print $NF}' | sort | uniq -c
  15 Rows.db
```

All 15 are on the `Rows.db` component specifically — never `Data.db`,
`Filter.db`, `Partitions.db`, `Statistics.db`, or `TOC.txt` (those are
non-zero). All three replicas have exactly 15 each, meaning the
corruption was produced at the coordinator and faithfully replicated.

Per-table distribution on node1 (same on node2/3):

| Table | 0-byte Rows.db |
|---|---|
| agent_memory.entity_store | 4 |
| agent_memory.typed_edges | 3 |
| agent_memory.entity_types | 3 |
| agent_memory.edge_types | 3 |
| agent_memory.tool_usage_log | 1 |
| agent_memory.schema_version | 1 |

## Silent data loss

Not just a display issue — this is **primary data loss**. Post-ingest:

| Source | Client ack'd inserts | In storage (all 3 replicas agree) |
|---|---|---|
| 79 skills via `ingest_skill` | 79 | 79 |
| `forge ingest ../ferrosa` | 5,154 | fraction |
| `forge ingest ../ferrosa-dbaas` | 1,040 | fraction |
| `forge ingest ../ferrosa-memory` | 1,247 | fraction |
| **total** | **7,520** | **1,242** |

So **≈ 83% of entity_store inserts were silently dropped**. The writer
returned success to the client, the entries showed up in memtable reads
during the ingest session (forge's ingest tool would have errored if
entity_put failed), but after the memtable flushed to an SSTable with a
0-byte `Rows.db`, the entries were effectively unwriteable.

Typed_edges — likely written through a different code path or in
larger batches — landed correctly (13,345 ≈ 13,400 reported).

Entity types that survived vs. died:

| entity_type | count in storage |
|---|---|
| section | 1,109 (from markdown section flushes) |
| document | 82 |
| module | 47 (code modules — should be hundreds) |
| crate | 4 (should be dozens) |
| skill | 0 present here (lives in global-session partition) |

## Downstream symptom

Every subsequent read from these tables (range scan, point lookup) trips
the SSTable reader on a 0-byte `Rows.db`:

```
WARN ferrosa_storage::store: read_range: skipping corrupted SSTable:
  invalid data: corrupted DeletionTime flags: 0xe3
WARN … corrupted DeletionTime flags: 0xc3
WARN … corrupted DeletionTime flags: 0xbc
```

Flag values vary (`0xe3`, `0xc3`, `0xbc` observed across nodes) because
the parser reads uninitialized memory / leftover buffer contents when the
backing file has no bytes. Post-normalization the reader skips the file
and continues, but every affected scan pays: per-file log WARN, per-file
error-handling cost, and nodes' memory grew by hundreds of MB during a
14k-row scan in the earlier Phase-0 dry-run (filed as
`specs/todo/bug-read-path-memory-growth-bloats-coordinator.md` — likely
a duplicate root cause).

## Hypothesis

The SSTable writer creates all component files up-front and streams
content into them as rows are flushed. For SSTables where **no rows are
materialized** — e.g. an INSERT that only sets partition-level tombstones,
a DELETE, an UPDATE SETting columns to NULL, or a small-batch flush that
emits a partition header but no row cells — `Rows.db` is created but
nothing is ever written to it. The writer then finalizes the SSTable
(emits TOC.txt etc.) leaving `Rows.db` as a 0-byte artifact.

The reader assumes `Rows.db` is non-empty: it `read_exact`s a
DeletionTime record and the resulting byte buffer is either
uninitialized or a zero-length slice that the DeletionTime codec
interprets as flags = 0-ish XORed with whatever preceding parser state
happens to be on the stack.

## Reproducer

```bash
# Fresh cluster, no data
cd ferrosa-memory
rm -rf ~/data/ferrosa-memory/node{1,2,3}/* ~/data/ferrosa-memory/minio/*
podman compose up -d
# Wait for healthy (3/3)

# Normal write workload
python3 /tmp/seed-skills.py /Users/bkearns/src/research/skills   # 79 skills
# via forge MCP:
#   mcp__forge__ingest(path="/Users/bkearns/src/ferrosa")
#   mcp__forge__ingest(path="/Users/bkearns/src/ferrosa-dbaas")
#   mcp__forge__ingest(path="/Users/bkearns/src/ferrosa-memory")

# Inspect
find ~/data/ferrosa-memory/node1/sstables -type f -size 0 | wc -l
#   ~15

podman logs ferrosa-memory-node1-1 | grep -c 'corrupted DeletionTime'
#   ~23
```

## Expected

- The writer either emits a valid empty-rows Rows.db (a single zero-byte
  marker that the reader interprets as "no rows" without trying to parse
  DeletionTime), or does not create Rows.db at all for SSTables with
  zero row payloads.
- The reader treats a missing or 0-byte Rows.db as "no rows" rather
  than "corrupted DeletionTime flags".

Either fix closes the loop. Preferred direction: the writer should never
finalize a SSTable with inconsistent components — if Rows.db is empty,
the TOC should not list Rows.db, or the row count in Statistics.db should
be 0 and the reader should short-circuit based on that before touching
Rows.db.

## Acceptance Criteria

- [ ] After 1 hour of mixed INSERT/UPDATE/DELETE traffic on a fresh
      cluster, `find .../sstables -type f -size 0` returns 0 files
      across all nodes.
- [ ] Reader no longer logs `corrupted DeletionTime flags` during any
      normal scan.
- [ ] Regression test: write a partition-level tombstone, trigger
      flush, assert the resulting Rows.db is either valid (parseable as
      zero rows) or absent.
- [ ] If the writer legitimately needs to produce 0-byte Rows.db for
      some case, the reader's short-circuit check (file length == 0 →
      skip without WARN) is added and unit-tested.

## Diagnostics from disk (2026-04-17)

Inspected corrupt SSTables on `~/data/ferrosa-memory/node1/`. Each had
a 0-byte `Rows.db` + a non-empty `Data.db`. The **first** partition in
each Data.db has deletion first-byte `0x80` (LIVE) — not the `0xe3 /
0xc3 / 0xbc` seen in WARN logs. So the reader's error is not on the
first partition. It must happen on a **subsequent** partition after
parse-drift accumulates through the intervening rows.

So the sequence is: writer produces correct partition #0, then drift
somewhere in row serialization (varint deltas, clustering-key encoding,
cell packing) puts later `read_partition_header` reads at the wrong
offset. The "corrupted DeletionTime flags" byte is whatever happened to
be at that drifted offset.

## Root cause (identified 2026-04-17)

**Schema drift between ALTER TABLE and the storage engine.** Flow:

1. `register_table_inner` (engine.rs:789) captures `state.schema` and
   `TableStore.schema` at table-creation time.
2. `ALTER TABLE ADD COLUMN` applies via `RaftOp::AlterTable`
   (raft/state_machine.rs:405) which updates the cluster-level schema
   (`schema.alter_table_internal`) but **does not** propagate to
   `state.schema` or `TableStore.schema`. `register_table_inner` is a
   no-op when the table exists (engine.rs:796-799), and there is no
   other mutation path — `grep "state.schema ="` returns zero hits.
3. The CQL bridge learns the new schema via `schema.alter_table_internal`
   and emits writes with `col_idx` values for post-ALTER columns.
4. Those writes land in the memtable untouched.
5. At flush, `TableStore.flush` calls `build_serialization_header(
   &self.schema, &partitions)` (store.rs:462) with the **stale**
   pre-ALTER schema. Header's `regular_columns.len()` is too small
   for the new cells.
6. `SSTableWriter::serialize_row` built its missing-column bitmap
   from a `HashSet<col_idx>` but then iterated all cells in order,
   writing the body bytes for every cell — including those whose
   `col_idx >= num_columns` that the bitmap could not represent. Body
   size is correct; the bitmap is a subset of the body.
7. The reader reads bitmap bits, consumes exactly that many cells,
   and leaves the remainder of the body unparsed. Its position is
   now inside real cell bytes. It interprets subsequent bytes as the
   next row's flags byte, eventually hitting a byte with bit 0 set
   and terminating the partition early — or drifting far enough to
   misread a partition header and report "corrupted DeletionTime flags".

Confirmed by `ferrosa-sstable/tests/p0_production_disk_replay.rs` on
the on-disk files under `~/data/ferrosa-memory/node1/sstables/
agent_memory.entity_store/` (the 8 `ALTER TABLE ADD` statements in
`ferrosa-memory/ddl/020_rich_entity_schema.cql` line up exactly with
the 7-vs-15 column gap observed in the on-disk bitmaps).

## Implementation Notes

Two-layer fix:

1. `ferrosa-sstable/src/writer.rs` — `serialize_row` asserts each cell's
   `col_idx` is in range for the header and that cells are strictly
   sorted by `col_idx`. Any violation is a hard fail at write time
   instead of producing silently corrupt SSTables.
2. `ferrosa-storage/src/store.rs` — `TableStore.schema` is now
   `ArcSwap<TableSchema>`; new `update_schema` method atomically swaps
   the schema. `ferrosa-storage/src/engine.rs` exposes
   `StorageEngine::update_table_schema(tid, new_schema)` that updates
   both `TableState.schema` and `TableStore.schema`.
3. All `AlterTable` apply paths
   (`ferrosa-cluster/src/raft/state_machine.rs`,
   `ferrosa-cluster/src/ddl_path.rs`,
   `ferrosa-cluster/src/pair/ddl.rs`) now call
   `engine.update_table_schema` with the post-ALTER `to_storage_schema()`
   so subsequent flushes build the correct `SerializationHeader`.

Regression coverage:

- `ferrosa-sstable/src/writer.rs`: `writer_rejects_cell_with_out_of_range_column_index`
  and `writer_rejects_duplicate_column_index_cells`.
- `ferrosa-sstable/tests/p0_production_disk_replay.rs`: diagnostic
  replay of on-disk corrupt SSTables (ignored by default).
- `ferrosa-storage/src/engine.rs::tests::read_survives_schema_evolution_across_sstables`:
  re-enabled and green; exercises the post-ALTER path with explicit
  `engine.update_table_schema`.
- `ferrosa-cluster/src/ddl_path.rs::tests::direct_alter_table_propagates_schema_to_storage`:
  end-to-end DDL→write→ALTER→write→flush→read.

## Possibly Related

- `specs/todo/bug-small-sstable-index-corruption-tool-usage-audit.md` —
  a different corruption signature (`wanted N bytes, got M`) on small
  SSTables, also dominated by `tool_usage_log` / `audit_log`. Different
  component (Data.db / Index), same overall "the writer is producing
  inconsistent small SSTables" pattern. Likely share a root cause in
  the multi-component finalize path.
- `specs/implemented/bug-sstable-flush-corruption-index-mismatch.md`
  and `specs/in-process/bug-sstable-corruption-survives-689e404.md` —
  historical corruption bugs in the same family.
- `specs/todo/bug-read-path-memory-growth-bloats-coordinator.md` — the
  memory bloat I filed earlier is almost certainly downstream of this:
  every range read iterates dozens of empty-Rows.db SSTables and the
  per-file WARN + error path allocation dominates.
