# Bug: node OOM during cluster recovery — Raft snapshot install/build materializes the full dataset in memory (non-streaming)

**Status**: Open. Filed from ferrosa-memory cluster rebuild on `main@673f9ada` (2026-06-14).
**Severity**: High — blocks a 3-node cluster from re-forming under its provisioned per-node memory budget.
**Component**: `ferrosa-cluster` — Raft snapshot path (`raft/state_machine.rs`, `raft/snapshot_pusher.rs`, `raft/snapshot_transport.rs`), and possibly `repair/cluster_view.rs` / peer bootstrap streaming.

## Contract being violated

A ferrosa node in the ferrosa-memory test deployment is provisioned with a **hard 2 GB per-node
`mem_limit`** (`ferrosa-memory/docker-compose.yml`, all of node1/node2/node3). Policy: a node must
**never** need more than 2 GB. If it does, that is a streaming bug — memory use during any operation
(including recovery) must be bounded and independent of total dataset size, not proportional to it.

## Symptom

During cluster re-formation (any `compose down` + `up`, or a node restart that leaves a peer
lagging), one node spikes memory to ≥2 GB and is **OOM-killed by the cgroup** (`SIGKILL`, exit
`137`, `podman inspect … State.OOMKilled=true`). With `on-failure:5` it exhausts retries and stays
down, so the cluster is stuck at 2/3 and reads/writes that need the down node's replica time out.
Observed on node1, node2, and node3 across attempts — whichever node is doing the heavy recovery
work at the time.

Concrete signatures observed:

- `OOMKilled=true ExitCode=137` on multiple nodes after `down`/`up`.
- Leader (node3) log immediately before a victim OOM:
  `ferrosa_cluster::raft::snapshot_pusher: snapshot_pusher: triggered snapshot + heartbeat for 1
  lagging peer(s) (P0-20) lagging_peers=1 leader_committed=5986 INSTALLSNAPSHOT_PUSHES_TOTAL=1`
  → the victim was the **lagging peer receiving a full `InstallSnapshot`**.
- Victim startup also runs self-heal scanning **every** table:
  `ferrosa_cluster::repair::cluster_view: self-heal: no reachable replica peer for table —
  posture=SingleNode (quarantine will be refused; FMEA #1) keyspace=… table=…` (repeated per table).

## Key evidence: memory use is proportional to dataset, not bounded

- On-disk data is ~equal across nodes: node1 1.8 G, node2 2.0 G, node3 1.8 G SSTables; commit logs
  are trivial (12–28 KiB). So replay is not the cost.
- **Steady-state RSS is tiny.** With the cgroup cap temporarily lifted to 10 GB (for measurement
  only — the cap was restored to 2 GB afterward), node1 and node3 ran a full recovery and settled at
  **idle ~240–270 MB, peak ~450–500 MB** — ~0.13:1 RSS:data. No OOM when they did **not** need a
  snapshot install.
- The OOM only appears on the node that must **receive an `InstallSnapshot`** (lagging peer) or
  otherwise reconstruct state. That node approaches **~1:1 RSS:data (~1.8 GB)** — i.e. it pulls
  roughly its entire dataset into memory at once.

A 13-entry lag (committed 5973 → 5986) triggered a full snapshot install that OOM'd the receiver.
A tiny lag forcing a full-dataset-in-memory transfer is the bug.

## Root cause: CONFIRMED via jemalloc heap profiling (2026-06-14)

Built ferrosa with `tikv-jemallocator` `profiling` feature (branch
`debug/recovery-oom-heap-profiling`), ran the cluster with
`MALLOC_CONF=...,prof:true,lg_prof_interval:28,prof_prefix:/var/lib/ferrosa/jeprof` (incremental
dumps that survive the SIGKILL), reproduced the OOM, and analyzed the near-OOM dump with `jeprof`.

**Result: 1.35 GB live heap, ~82% of it in a single call path:**

```
ferrosa_net::rpc::server::RpcServer::handle_connection
 → HandlerRegistry::dispatch
  → ferrosa_cluster::repair::rpc::RepairMerkleHandler::handle   (repair/rpc.rs:161)
   → ferrosa_cluster::repair::build_tree_for_range              (repair/mod.rs:126)
    → StorageEngine::walk_token_range_for_digest                (ferrosa-storage/src/store.rs:3376)
     → ferrosa_sstable::reader::partition_token_offsets / partition_offsets  (reader.rs:269,308)
       → OnceLock::get_or_init → Arc<Vec<(i64,u64)>> / Arc<Vec<u64>>  (RawVec::grow_one)
```

**The allocation is the SSTable reader's per-partition offset indexes**, NOT the memtable, the Raft
snapshot, or `cluster_view`:

- `reader.rs:308 partition_token_offsets() -> Arc<Vec<(i64, u64)>>` — one `(token, byte-offset)`
  pair for **every partition** in the SSTable, built by walking the whole file, cached in `OnceLock`
  for the reader's lifetime. Added as the seek optimization that turns repair's "partitions in token
  range [a,b)" from O(table_size) into O(log N + matches) — but the **index itself is
  O(num_partitions) resident memory**.
- `reader.rs:269 partition_offsets() -> Arc<Vec<u64>>` — same shape for the skip-dedup path.
- ~24 B/partition × tens of millions of small partitions (agent_memory term/edge tables) ≈ the
  observed ~1 GB, and it is built **per reader**. `repair::build_tree_for_range` and the storage walk
  themselves correctly stream rows (one partition in flight); the unbounded memory is purely these
  cached whole-table offset vectors. `bounded_overlap_readers` bounds the reader *count*, not each
  reader's offset-index size.

### Fix direction

The partition offset/token index is **already on disk** (Cassandra-style `Index.db` / `Summary.db`
+ `partition_index` trie used by `get_partition`). `seek_to_token` and `skip_to_next_partition`
should resolve positions via the **on-disk index / a bounded sampled summary** (binary search on
disk, or a fixed-size sampled in-memory summary à la Cassandra's index summary), instead of
materializing and caching `Vec`s sized to the whole partition count. Peak memory for a digest scan
must be O(sample/window), not O(num_partitions). This keeps the O(log N) seek win without the
O(partitions) resident cost.

### TDD test list (target: ferrosa-sstable reader + storage digest walk)
- [ ] A reader over an SSTable with N partitions answers `seek_to_token`/range scan **without**
      retaining an O(N) offset Vec (assert resident structure size is bounded / sampled).
- [ ] `walk_token_range_for_digest` over a narrow sub-range of a large table touches memory bounded
      by the sub-range + sample, not the whole table.
- [ ] Digest output is **byte-identical** to the current full-index implementation (regression
      guard — the Merkle XOR must not change).
- [ ] Empty/torn-index fallback path still works (linear scan from byte 0).
- [ ] Property: for random partition counts and random token sub-ranges, peak resident index memory
      stays under a fixed bound and the digest equals the reference full-scan digest.

### Chosen plan
- **Approach 1 (LANDED — branch `fix/sstable-sampled-partition-index`, TDD):** `partition_token_offsets`
  is now a bounded downsampled summary (`build_token_summary`, ≤ `PARTITION_TOKEN_SUMMARY_MAX_ENTRIES`
  = 65 536 → ~1 MB hard cap; stride 1 for small tables so behaviour is unchanged). `seek_to_token`
  binary-searches the summary to the floor anchor then forward-scans (bounded by stride) to the exact
  landing via `next_partition_metadata` — the **index-free** advance, which also stops the build from
  calling `skip_to_next_partition`, so a repair seek no longer materializes the full `partition_offsets`
  either. Same partition landing → byte-identical digest. Tests: `ferrosa-sstable/src/reader.rs`
  `token_summary_is_downsampled_yet_seek_lands_exactly` (new) + `seek_to_token_starts_at_or_after_target`
  (regression). Green: ferrosa-sstable lib 247, ferrosa-storage digest 5 / walk_token 1, ferrosa-cluster
  repair 80; clippy clean. Not yet rebuilt+verified live on the cluster.
- **Approach 2 (DEFERRED follow-up):** on-disk index seek — resolve `seek_to_token` against the
  on-disk `Partitions.db` BTI trie + a file-order cursor for `skip_to_next_partition`, for O(1)
  resident memory. Larger blast radius (needs an ordered range/floor-seek walker on the trie, which
  is point-lookup only today). Do after Approach 1 lands and re-measure. *(Could not file to the
  forge task board — its CQL was unreachable while the cluster is degraded; tracked here instead.)*
- **Also deferred:** apply the same bounding to `partition_offsets` (the `skip_to_next_partition`
  merge/compaction path) — same O(num_partitions) shape, wider blast radius than repair.
- **Separate work item:** start/stop **property/fuzz (Jepsen-style) tests** asserting the cluster
  always re-forms a single stable leader with all voters, no OOM, bounded election churn.

---

## Earlier hypotheses (investigated and disproven — kept for the record)

Two initial hypotheses were investigated and **disproven** (do not fix these without evidence):

1. **Raft snapshot materialization** (`raft/state_machine.rs`) — the snapshot *is* a single
   in-memory `Vec<u8>` (`current_snapshot: Option<(SnapshotMeta, Vec<u8>)>`,
   `PersistedSnapshot { bytes: Vec<u8> }`, `bincode` over the whole blob). **But** the snapshotted
   payload is `SnapshotData { state: RaftState, last_applied, last_membership }` — that is **cluster
   metadata** (schema, members, config, index-state map, Accord ledgers/buffers), **not** the
   ~1.8 GB of row data, which lives in SSTables *outside* Raft. For the ferrosa-memory workload this
   blob should be KB–low-MB, so it is an unlikely source of a ~1.8 GB spike. (Streaming it is still
   good hygiene, but it is not the confirmed OOM cause.)
2. **Startup self-heal scan** (`repair/cluster_view.rs`) — uses **repair Merkle/digest RPCs** (root
   hash per `(table, range)`), not full-table reads. Does not materialize row data. Not the cause.

**What is actually confirmed:**
- A plain `podman compose down && up` of the 3-node cluster **reliably OOM-kills 1–2 nodes**
  (`exit 137`, `State.OOMKilled=true`) at the 2 GB cap during recovery. **Which** node(s) OOM is
  non-deterministic across runs (observed node1; node1+node2; node3) — correlates with whichever
  node performs the heavier "catch-up / SingleNode" recovery in that run's ordering.
- It is a **transient spike**, not retained memory: with the cap temporarily lifted to 10 GB the
  same nodes recover and settle at **idle ~250 MB / peak ~450–500 MB**, and a node that does **not**
  need catch-up rejoins cleanly under 2 GB.
- Spike approaches ~1:1 with dataset size (~1.8 GB) on the affected node ⇒ a recovery path pulling
  ~the full dataset into memory at once (non-streaming), per the original thesis — **but the exact
  path is not yet identified.**

**Next step (prerequisite to any fix): capture a heap profile of the over-allocation.** ferrosa uses
`tikv-jemallocator` and honors `MALLOC_CONF` (`ferrosa/src/main.rs:48`) but is built **without** the
`profiling` feature. To capture:
1. `ferrosa/Cargo.toml`: add `"profiling"` to `tikv-jemallocator` features; rebuild the image.
2. Run the node with `MALLOC_CONF="prof:true,prof_active:true,lg_prof_sample:17,lg_prof_interval:28,prof_prefix:/var/lib/ferrosa/jeprof/jeprof"`
   (dumps a profile every ~256 MiB allocated to the mounted volume — survives the SIGKILL).
3. Reproduce the OOM; analyze the last `jeprof.*.heap` with `jeprof --show_bytes --cum --text
   /usr/local/bin/ferrosa <heap>` to attribute the allocation by call stack.

Candidate paths to keep on the audit list until the profile points somewhere (all in
`ferrosa-cluster`): peer bootstrap / catch-up data streaming (sender reading whole tables into a
`Vec`), the storage-engine startup load on the catch-up node, and any repair path that materializes
rows (vs digests).

## Related instability (separate, likely same recovery flow)

After recovery a node can enter a **CheckQuorum step-down / re-election loop** ("leader has lost
quorum contact, stepping down") even with peers reachable (0 TCP refused) — i.e. a node cannot
re-stabilize into the existing quorum after start/stop. A distributed DB must tolerate individual
node start/stop and re-converge without destabilizing healthy peers. This needs its own
investigation and **property-based / fuzz (Jepsen-style) tests** that randomize start/stop/restart
orderings and assert the cluster always re-forms a single stable leader with all voters, no OOM, and
bounded election churn.

(Operational note, not a ferrosa bug: `podman restart <one node>` cascades through compose
`depends_on: service_healthy` and SIGKILLs healthy dependents. Use `podman-compose up/down`, not
per-container restart, when operating this stack.)

## Fix direction

Make snapshot build and install **streaming and bounded**: serialize/deserialize and persist the
snapshot to/from disk in bounded chunks (the 3 MiB transport chunk is a natural unit) without ever
holding the full snapshot as one `Vec<u8>` in RAM. Peak memory for snapshot transfer must be O(chunk
size), not O(dataset). Same requirement for any repair/bootstrap scan: stream rows to the
network/disk, never collect a full table into memory.

## Not fixed by PR#131

PR#131 (`fix/cql-range-read-offload`, OPEN) offloads the **CQL range/streaming read** off the tokio
worker to cure **keepalive starvation** ("pool is broken / keepalive timed out") on heavy
`SELECT`s. It changes which thread runs the scan but still materializes results into a `Vec` — it
does not reduce memory, and it touches the CQL read path, not the Raft recovery/snapshot path. PR#131
is still wanted for the `cql_live.rs` heavy-read timeout symptom, but it will **not** stop these
recovery OOMs.

## Reproduction

1. 3-node ferrosa cluster on `main@673f9ada`, ~1.8–2 GB data/node, per-node `mem_limit: 2g`.
2. `podman compose down && podman compose up -d` (or restart one node so it lags by a few entries).
3. The lagging node receives an `InstallSnapshot`, RSS climbs to ≥2 GB, cgroup OOM-kills it
   (`ExitCode=137`, `OOMKilled=true`); it cannot rejoin within the 2 GB budget.
