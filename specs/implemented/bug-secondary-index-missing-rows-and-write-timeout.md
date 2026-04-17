---
type: bug
priority: P1
reported-by: ferrosa-memory 2i validation suite
implemented-by: ""
verified-by: ""
created: 2026-04-16
updated: 2026-04-16
implemented-by: claude-code
source: ferrosa-memory/crates/ferrosa-memory-core/tests/ferrosa_2i_validation.rs
---

# Secondary index drops rows for non-unique labels + CL=ONE writes time out under concurrency

Two related failures discovered while running the ferrosa-memory 2i validation suite against a freshly-booted 3-node cluster (docker-compose.test.yml, +500 port offset). Reproducible. See `../ferrosa-memory/crates/ferrosa-memory-core/tests/ferrosa_2i_validation.rs` for the harness.

## Environment

- Ferrosa 3-node cluster (node1-test, node2-test, node3-test) on ports 19542-19544.
- Replication: `SimpleStrategy` with `replication_factor = 1`.
- cdrs-tokio client, default consistency level (`ONE`).
- Test keyspace: `agent_memory_test`.
- One simple table with a secondary index on a text column.

## Failure 1 — secondary index returns only a subset of matching rows

### Reproduction (C5 in the validation suite)

1. Create table with a 2i on a `text` column `label`.
2. Insert 8 rows, all with `label = 'shared'` and distinct `uuid` primary keys.
3. Run `SELECT id FROM t WHERE label = 'shared'`.

### Expected

All 8 row ids.

### Observed

Only 3 row ids consistently. The returned ids are always a subset of the inserted ids, but different ids are returned across runs.

### Example

```text
inserted: {4cd4705c, 1ec55d01, de7d5b8c, 9dea9e60, 45ac93ff, 5076f253, e1722727, 94ea96f3}
returned: {5076f253, 1ec55d01, 9dea9e60}
```

### Hypotheses

- 2i entries for a given label may be collapsing to a single partition (by label), and the SSTable compaction may be dropping clustering rows.
- Index writes may only be going to one node, and at CL=ONE reads from a different coordinator miss them.
- The index may be storing value → primary-key associations in a way that's not collision-tolerant when multiple primary keys share the indexed value.

### Impact on ferrosa-memory

Blocks Sprint 3 of the skills layer (O(1) skill name lookup via a 2i on `entity_type + entity_name`). Today ferrosa-memory works around this with phonetic name match — functionally correct but O(n) per session.

## Failure 2 — CL=ONE writes time out under modest concurrency

### Reproduction (C2 + the early phase of C6)

1. Same setup as above.
2. Spawn 16 concurrent tokio tasks, each inserting a row with a unique label.

### Expected

All 16 writes complete.

### Observed

Intermittent write timeouts:

```
server error: storage error: invalid data: cluster: write timeout:
CL=ONE, received=0, required=1
```

`received=0` means the coordinator didn't get an ack from any replica, despite `replication_factor = 1` and `CL=ONE`. The cluster has ~5 minutes of warmup (Raft leader elected, state machine Normal on all nodes).

### Timing

Occurs most often when the cluster has just finished bootstrapping. Even after a 30-second wait for stability, concurrent N=16 inserts still trip a subset of failures.

### Hypotheses

- Raft leader election/ack lag after bootstrap leaves a node unable to serve writes briefly, but the coordinator doesn't fail over to another replica.
- Internode RPC may have a bootstrap delay even after the state machine transitions to Normal.

### Impact on ferrosa-memory

Doesn't block skills (ingest_skill is single-call, not bulk). Would block any batched-write path — `batch_create_edges`, `batch_ingest`, or the forge-side bulk ingest (already flagged by the pre-existing P0 data-loss bug `bug-entity-store-session-partitioning.md`).

## Reproducing Locally

```bash
cd ferrosa-memory
scripts/start-test-cluster.sh
export $(scripts/start-test-cluster.sh --env)
cargo test -p ferrosa-memory-core --test ferrosa_2i_validation \
  -- --ignored --nocapture --test-threads=1 \
  c5_index_returns_all_matches c2_concurrent_writers
```

## Current Workaround in ferrosa-memory

None. Per `ferrosa-memory/CLAUDE.md` the project policy is to fix DB bugs upstream, not work around them. Skill name lookup uses `entity_find_phonetic` (O(n) partition scan) until 2i is reliable.

## Acceptance Criteria

- [ ] C5 returns all 8 rows consistently.
- [ ] C2 completes without timeouts under N=16 concurrency on a cluster ≤60s post-boot.
- [ ] Both cases also pass with `replication_factor = 3` and `CL = LOCAL_QUORUM` (separate follow-up after the single-replica case is stable).

## Implementation Notes

**Root cause (C5):** Secondary index reads were local-only — the CQL router called `engine.read_by_index()` directly instead of going through the `WritePath` which handles cluster coordination. In a 3-node cluster with RF=1, each node only had index entries for rows whose partition key hashed to that node (~2-3 of 8). The query returned only the local node's entries.

**Fix:** Added `WritePath::index_read()` that scatter-gathers to all ring nodes in cluster mode:
- `ferrosa-cluster/src/write_path.rs` — new `index_read()` method (Direct/Pair/Cluster dispatch)
- `ferrosa-cluster/src/coordinator/read.rs` — new `coordinate_index_read()` (fans out to all nodes, merges, deduplicates by token)
- `ferrosa-net/src/codec.rs` + `message.rs` — new `IndexReadRequest`/`IndexReadResponse` (0x62/0x63)
- `ferrosa-cluster/src/raft/handlers.rs` — new `IndexReadHandler` + payload types
- `ferrosa-cluster/src/controller/cluster.rs` — handler registration
- `ferrosa-cql/src/router.rs:1230-1234` — changed from `engine.read_by_index()` to `write_path.index_read()` (both SingleIndex and IndexIntersection paths)
- `ferrosa-cluster/Cargo.toml` — moved `ferrosa-index` from dev-dependencies to dependencies

**C2 (write timeout):** Not addressed in this fix. The intermittent CL=ONE write timeouts under N=16 concurrency are a separate issue, likely related to Raft leader warmup timing or backpressure. Tracked separately.
