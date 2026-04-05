# Bug: Bulk CQL Writes Cause Raft Election Storm via Runtime Starvation

**Severity:** High — cluster becomes unavailable for all writes  
**Discovered:** 2026-04-05  
**Component:** ferrosa-cluster (coordinator write path + ferrosa-net lane actors)  
**Reproducer:** Restore ~2000 entities via sequential CQL INSERTs to a 3-node cluster  

## Symptoms

1. First ~2000 INSERT statements succeed (entity_store restore).
2. Subsequent writes fail with `NoHostAvailable` / `OperationTimedOut`.
3. Node1 (leader) logs continuous `timeout after 300ms when AppendEntries` errors.
4. Node2 and Node3 enter an election storm — terms climb rapidly (T284 → T392 → T417) with no leader elected.
5. Cluster never self-heals; requires full restart.
6. Inter-node TCP connectivity is fine (`</dev/tcp/node2/7000` succeeds, DNS resolves).

## Root Cause Analysis

### Key Fact: Raft Is NOT on the Data Write Path

CQL data writes (INSERT/UPDATE/DELETE) do **not** flow through Raft consensus. They use the coordinator fan-out pattern:

```
CQL INSERT → WritePath::Cluster → ClusterCoordinator::coordinate_write()
           → FuturesUnordered fan-out to replicas via Lane::Data
           → Each replica: PeerManager::send(MutationForward, Lane::Data)
```

Raft (`Lane::Raft`) is only used for DDL and topology changes.

### The Starvation Chain

Despite Raft and Data being on separate lanes with separate actors, bulk writes cause Raft starvation through **tokio runtime contention**:

1. **Sequential CQL inserts arrive** — each goes through `coordinate_write()`, which fans out `MutationForward` to 2 remote replicas on `Lane::Data`.

2. **Lane actor processes sequentially** (`lane_actor.rs:255`): each `handle_send()` awaits the full network round-trip before processing the next command. With `LANE_CHANNEL_CAPACITY = 64`, at most 64 concurrent messages are in-flight per peer.

3. **Follower nodes are saturated**: each inbound `MutationForward` triggers a synchronous `storage.write()` (memtable insert + commitlog append). With 2000+ writes arriving back-to-back, the follower's tokio runtime is busy executing storage writes.

4. **Raft heartbeat processing is delayed**: although `Lane::Raft` has its own actor and channel, the actor's `recv().await` and `handle_send().await` compete for tokio worker threads. When workers are saturated with storage writes from the Data lane handler, the Raft lane actor can't get scheduled in time.

5. **Election timeout fires**: the Raft follower hasn't processed a heartbeat within `election_timeout_min` (1000ms). It starts an election.

6. **Election storm**: the election sends Vote RPCs through `Lane::Raft`, but ALL nodes are now saturated. Vote RPCs timeout. No candidate wins. Elections repeat with incrementing terms. The leader keeps trying AppendEntries, further loading the Raft lane.

7. **Unrecoverable**: even after the bulk writes stop, the election storm continues because:
   - Each failed election triggers a new one (1-2s timeout)
   - Vote RPCs add load to already-stressed Raft lanes  
   - No node can achieve quorum to become leader
   - Without a Raft leader, the CQL write path reports `NoHostAvailable`

### Contributing Factors

| Factor | Location | Issue |
|--------|----------|-------|
| Sequential lane processing | `lane_actor.rs:255-298` | Actor loop processes one command at a time — can't prioritize |
| Small lane capacity | `lane_actor.rs:25` | `LANE_CHANNEL_CAPACITY = 64` — fills up quickly under burst |
| No CQL write backpressure | `write_path.rs` / `batch.rs` | CQL accepts writes at line rate with no regard for replication lag |
| Sequential batch mutations | `batch.rs:50-64` | `coordinate_logged_batch` awaits each mutation serially in a loop |
| Shared tokio runtime | all async code | Raft and Data lane actors compete for the same worker thread pool |
| Short heartbeat interval | `config.rs:75` | 300ms heartbeat with no adaptation under load |

## Proposed Fixes

### P0: Raft Lane Priority (Prevents Election Storm)

Give the Raft lane actor a dedicated tokio runtime or use `tokio::task::spawn_blocking` isolation so storage writes on the Data lane can never starve Raft processing.

Alternatively, use a dedicated OS thread for the Raft lane actor (not a tokio task).

### P1: CQL Write Backpressure (Prevents Saturation)

Add a bounded semaphore to `coordinate_write()` that limits concurrent in-flight mutations. When the semaphore is full, CQL writes get backpressure (slow down rather than overwhelm the system).

```rust
// In ClusterCoordinator:
let _permit = self.write_semaphore.acquire().await?;
// ... proceed with coordinate_write ...
```

### P2: Adaptive Heartbeat Interval

Increase the heartbeat interval dynamically when the system is under write load. Alternatively, increase `election_timeout_min` to give more headroom (e.g., 3000ms instead of 1000ms).

### P3: Parallel Batch Processing

In `coordinate_logged_batch()`, replace the sequential loop:
```rust
// Current (sequential):
for m in &mutations {
    for row in &m.rows {
        self.coordinate_write(...).await;  // blocks on each
    }
}

// Proposed (parallel with bounded concurrency):
let semaphore = Arc::new(Semaphore::new(32));
let mut futs = FuturesUnordered::new();
for m in &mutations {
    for row in &m.rows {
        let sem = semaphore.clone();
        futs.push(async move {
            let _permit = sem.acquire().await;
            self.coordinate_write(...).await
        });
    }
}
while let Some(result) = futs.next().await { ... }
```

### P4: Increase Lane Capacity

Increase `LANE_CHANNEL_CAPACITY` from 64 to 256 or 512 to reduce backpressure under burst writes. The memory cost is negligible (each slot is a `LaneCommand` enum).

## Test

See `ferrosa-cluster/tests/bulk_write_stability.rs` for three tests:
1. `sequential_bulk_writes_complete_without_timeout` — reproduces the exact restore pattern
2. `concurrent_burst_writes_measure_lane_saturation` — measures throughput at varying concurrency levels
3. `bulk_writes_do_not_starve_probe_latency` — detects runtime starvation by measuring probe latency under sustained write load

## Workaround (User-Facing)

Until fixed, bulk restores must throttle writes:
- Add `time.sleep(0.01)` between INSERT statements in restore scripts
- Or use CQL BATCH statements to reduce round-trips
- Or restore to a single-node cluster (standalone mode) then form the cluster after
