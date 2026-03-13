# Commit Log Replay & Compaction Execution Design

> Last updated: 2026-03-13
> Status: Draft

## Goal

Wire commit log replay into StorageEngine startup and implement compaction merge I/O, enabling data durability across restarts and bounded SSTable growth on a single node.

## Architecture

Both features are **integration tasks** — the building blocks (replay reader, merge algorithm, SSTable writer, STCS strategy, executor thread) are fully implemented. The work is wiring them together and adding the missing glue code.

```mermaid
graph TB
    subgraph "Startup Path (Part 1)"
        Boot[StorageEngine::new] --> Replay[CommitLog::open_and_replay]
        Replay --> Filter[Filter mutations by checkpoint]
        Filter --> Apply[Replay mutations into TableStore memtables]
        Apply --> Flush[Flush replayed tables to SSTable]
        Flush --> Discard[discard_completed per table]
    end

    subgraph "Compaction Path (Part 2)"
        Trigger[Periodic check] --> Strategy[STCS select candidates]
        Strategy --> Submit[Submit CompactionTask]
        Submit --> Exec[CompactionExecutor thread]
        Exec --> Read[SSTableReader per input]
        Read --> Merge[merge_partitions]
        Merge --> Write[SSTableWriter]
        Write --> Swap[Atomic swap: new SSTable replaces inputs]
        Swap --> Cleanup[Delete old SSTable files]
    end
```

## Dependencies

- `ferrosa-storage`: CommitLog, TableStore, merge, flush, compaction modules
- `ferrosa-sstable`: SSTableReader, SSTableWriter
- `ferrosa-common`: Partition, Row, CellValue types

No new crate dependencies.

## Part 1: Commit Log Replay on Startup

### What Exists

| Component | Status |
|-----------|--------|
| `CommitLog::open_and_replay()` | Implemented — scans segments, validates CRCs, filters by checkpoint |
| `SegmentReader` | Implemented — follows marker chain, skips corruption |
| `Checkpoint` | Implemented — atomic JSON saves, per-table position tracking |
| `CommitLog::discard_completed()` | Implemented — removes tables from segments, deletes old files |
| `Mutation` serialization | Implemented — round-trip tested with property tests |

### What Needs Wiring

#### 1.1 StorageEngine startup calls replay

`StorageEngine::new()` currently calls `CommitLog::new(config)`. Change to:

```
let (commit_log, mutations) = CommitLog::open_and_replay(config)?;
```

Then replay each mutation into the appropriate `TableStore`:

```
for mutation in mutations {
    let table_id = TableId::new(&mutation.keyspace, &mutation.table);
    if let Some(store) = self.tables.get(&table_id) {
        store.write_from_replay(mutation);
    }
    // Unknown tables are silently skipped — schema may have changed
}
```

`write_from_replay()` is a new method on `TableStore` that writes directly to the memtable without appending to the commit log (avoiding circular writes).

#### 1.2 Post-replay flush

After replay, flush all tables that received mutations to SSTable. This converts replayed data from memtable (volatile) to SSTable (durable), then updates the checkpoint:

```
for (table_id, store) in &self.tables {
    if store.has_unflushed_data() {
        let position = store.flush()?;
        commit_log.discard_completed(&table_id, position)?;
    }
}
```

This is optional for correctness (the commit log is the durability guarantee) but important for performance — without it, the memtable holds all replayed data in memory until the next natural flush threshold.

#### 1.3 Flush-time checkpoint updates

When `TableStore::flush()` completes during normal operation (not just replay), the engine must call `commit_log.discard_completed(table_id, position)`. This ensures old segments are cleaned up as data becomes durable in SSTables.

The `StorageEngine::flush()` method already calls `TableStore::flush()`. Add the `discard_completed` call after it succeeds.

#### 1.4 Track commit log position per write

Each `StorageEngine::write()` call appends to the commit log and gets back a `CommitLogPosition`. This position must be associated with the table so that `discard_completed` can report the correct position at flush time.

`TableStore` needs a `last_commit_log_position: Mutex<Option<CommitLogPosition>>` field, updated on each write, read at flush time.

### Error Handling

- **Corrupted entries**: Already handled by `SegmentReader` — silently skipped, next valid entry used. This is WAL standard practice.
- **Unknown tables**: Mutations for tables not in the current schema are skipped. This handles schema changes between crash and restart.
- **Empty replay**: If no segments exist or all are checkpointed, `open_and_replay` returns an empty vec. No special handling needed.
- **Checkpoint write failure**: Fatal — startup fails. Without a valid checkpoint, we cannot safely skip already-flushed entries on next restart.

### Testing

- **Replay round-trip**: Write mutations via engine, kill without flush, restart, verify data is present.
- **Checkpoint filtering**: Write, flush (updates checkpoint), write more, restart — only unflushed mutations should replay.
- **Corruption tolerance**: Corrupt a segment file, restart — corrupted entries skipped, valid entries replayed.
- **Empty replay**: Fresh start with no segments — engine starts normally.
- **Schema change**: Write to table, drop table from schema, restart — mutations for dropped table silently skipped.

## Part 2: Compaction Execution (STCS)

### What Exists

| Component | Status |
|-----------|--------|
| `SizeTieredStrategy::select()` | Implemented — bucket grouping, threshold logic |
| `CompactionExecutor` | Implemented — background thread, channel, task/result types |
| `merge_partitions()` | Implemented — LWW, deletion suppression, static row merge |
| `SSTableWriter` | Implemented — all 7 components, delta-encoding, bloom, trie |
| `SSTableReader` | Implemented — reads all 7 components |
| `SSTableMetadata` | Implemented — lightweight carrier for strategy decisions |
| `FlushTarget` / `FileFlushTarget` | Implemented — parallel file writes with generation counter |

### What Needs Wiring

#### 2.1 CompactionExecutor: implement merge I/O

`execute_task()` currently returns placeholder metadata. Replace with:

1. **Read**: Open each input SSTable via `SSTableReader::open(path)`
2. **Collect**: Read all partitions from each reader into memory
3. **Group**: Group partitions by decorated key across all inputs
4. **Merge**: For each key group, call `merge_partitions()` to produce a single merged partition
5. **Sort**: Ensure output partitions are in token order (SSTableWriter requires this)
6. **Header**: Build `SerializationHeader` by scanning merged partitions (reuse `build_serialization_header()` from flush)
7. **Write**: Create `SSTableWriter`, add all merged partitions, call `finish()`
8. **Output**: Write the 7 component files to `task.output_dir` via `FileFlushTarget`
9. **Return**: `CompactionResult` with output SSTable metadata

Memory concern: Loading all partitions into memory works for small-to-medium SSTables. For very large tables, a streaming merge with a priority queue would be needed — but that's a future optimization. The current merge.rs API already works on `Vec<Partition>` and the flush path loads entire memtables, so this is consistent.

#### 2.2 StorageEngine: compaction trigger loop

Add a periodic background task that runs every N seconds (configurable, default 60s):

1. For each `TableStore`, collect SSTable metadata (size, path, token range, timestamps)
2. Run `SizeTieredStrategy::select()` on the metadata
3. If candidates returned, submit a `CompactionTask` to the executor
4. Poll executor for completed results
5. On completion: swap new SSTable in, remove old SSTables from the table's list, delete old files

The trigger loop runs on the existing tokio runtime (async task with `tokio::time::interval`).

#### 2.3 Atomic SSTable swap after compaction

When compaction completes, the `TableStore` must atomically replace the input SSTables with the output SSTable. This uses the same `ArcSwap` pattern as flush:

1. Load current SSTable list
2. Remove input SSTables (by ID/path matching)
3. Insert output SSTable at the correct position (sorted by generation)
4. Store new list via `ArcSwap::store()`

Old SSTable files are deleted after the swap. If deletion fails, log a warning but don't fail — orphan cleanup can handle it later.

#### 2.4 SSTable metadata collection

`TableStore` needs a method to collect `SSTableMetadata` for its current SSTables. This reads the stored statistics (partition count, size, token range, timestamp range) from each `SSTableReader`.

### Error Handling

- **Reader failure**: If an input SSTable cannot be opened (corrupt, missing), the compaction task fails. The executor reports the error; the trigger loop retries on the next cycle. Input SSTables are not removed.
- **Writer failure**: If the output SSTable cannot be written (disk full, I/O error), the task fails. Input SSTables are preserved — no data loss.
- **Concurrent flush**: Flush and compaction can run concurrently on different tables. For the same table, the ArcSwap pattern ensures atomic visibility — readers always see a consistent snapshot.
- **Shutdown**: `CompactionExecutor::shutdown()` already drains the channel and joins the thread. In-progress tasks complete before shutdown.

### Testing

- **Round-trip compaction**: Flush 4 SSTables (STCS min_threshold), trigger compaction, verify merged output contains all data.
- **LWW correctness**: Write conflicting values across multiple SSTables, compact, verify latest-timestamp wins.
- **Deletion suppression**: Write + delete across SSTables, compact, verify tombstones suppress older values.
- **Concurrent read during compaction**: Start a read, trigger compaction, verify read completes with correct data (ArcSwap atomicity).
- **File cleanup**: After compaction, verify old SSTable files are deleted and new ones exist.
- **Strategy selection**: Verify STCS only triggers when bucket reaches min_threshold.
- **Error recovery**: Corrupt an input SSTable, trigger compaction, verify graceful failure with no data loss.

## Implementation Order

1. **Part 1 first**: Commit log replay. This is prerequisite for meaningful compaction testing — without replay, restarting the engine between test phases loses data.
2. **Part 2 second**: Compaction execution. With replay working, we can write data, restart, verify persistence, then test compaction across multiple flush cycles.

## Crate Structure Changes

No new files needed. All changes are in existing files:

```
ferrosa-storage/src/
├── engine.rs          # Startup replay, flush signaling, compaction trigger
├── store.rs           # write_from_replay(), position tracking, SSTable metadata
├── compaction/
│   └── executor.rs    # Merge I/O implementation
└── (no new files)
```

## Related Specs

- [Storage Part A](2026-03-11-ferrosa-storage-design.md) — memtable, flush, merge
- [Storage Part B](2026-03-11-ferrosa-storage-part-b-design.md) — commit log format and sync
- [Storage Part C](2026-03-12-ferrosa-storage-part-c-design.md) — compaction, S3, engine composition
