# Anti-Entropy Repair Architecture

> Last updated: 2026-05-19
> Status: Implemented (operator-triggered); scheduling, multi-DC policy, and Jepsen verification still open

## Overview

Anti-entropy repair reconciles divergent replica content that survives the
write-path's quorum + hinted-handoff safety net — for example after a node
restarts mid-write, after a partial-failure window where one replica fell
behind, or whenever the operator wants explicit convergence evidence. The
repair pipeline is built around the Cassandra-shape "compare two Merkle trees
on the same token range, stream only the partitions that differ" pattern, with
two extensions for bounded-memory operation on a constrained-container
deployment:

1. **Merkle-then-stream**: each side builds the Merkle tree by *streaming*
   the local SSTable contents row-at-a-time through a digest, never
   materialising a partition into memory.
2. **Chunked Fetch + Apply**: once divergent leaves are identified, partitions
   are fetched and applied in fixed-size chunks (64 partitions per RPC) so
   peak in-flight memory is bounded by chunk size × max-partition-size, not
   by table size.

Repair is currently **operator-initiated** (HTTP `POST /api/cluster/repair`
or `ferrosa-ctl repair`). A scheduled / continuous-repair scheduler is
explicitly out of scope for this revision — see "Limits / open work".

Source files referenced throughout:

- `ferrosa-cluster/src/repair/mod.rs` — algorithm core, `partition_merkle_hash`,
  `compute_repair_plan`, `build_tree_for_range`, streaming diff
- `ferrosa-cluster/src/repair/coordinator.rs` — `RepairCoordinator`,
  `SessionExecutor` trait, per-`(range, peer)` fan-out
- `ferrosa-cluster/src/repair/executor.rs` — `LocalRepairExecutor`,
  `RepairStore` trait, `StorageEngineRepairStore`, `InMemoryRepairStore`,
  chunked Fetch/Apply
- `ferrosa-cluster/src/repair/merkle.rs` — `MerkleTree`,
  `divergent_leaf_ranges`
- `ferrosa-cluster/src/repair/rpc.rs` — wire protocol, `RemoteRepairStore`,
  `RepairMerkleHandler`, `RepairFetchHandler`, `RepairApplyHandler`
- `ferrosa-cluster/src/raft/handlers.rs` — `PartitionDigestStream`,
  `compute_partition_digest_streaming`, `serialize_partition_to_wire_borrowed`
- `ferrosa-storage/src/engine.rs` — `read_token_range`,
  `walk_token_range_for_digest`
- `ferrosa/src/web/api.rs` — HTTP `/repair` handler
- `ferrosa-ctl/src/main.rs` + `commands.rs` — `Repair` subcommand

## Phases

A repair run for a single `(table, range, peer)` triple executes four
phases, all driven by `LocalRepairExecutor::run_session`:

### 1. Merkle build (each side, in parallel)

Both replicas call `RepairStore::build_merkle(table, range_start,
range_end)`. The `StorageEngineRepairStore` impl walks the local SSTables
via `StorageEngine::walk_token_range_for_digest`, feeding each partition's
header + rows into a `PartitionDigestStream` (CRC32 over a bincode-encoded
wire shape, byte-identical to the read-path digest). The 64-bit Merkle leaf
hash is computed as `(hi << 32) | lo` with `hi = crc.rotate_left(16)`.

Tree depth is `TREE_DEPTH = 15` → 32 768 leaves. At that depth a
production table with O(10⁴–10⁶) partitions averages well under one
partition per non-empty leaf, so the leaf-level diff is fine-grained.

Concurrent Merkle builds are throttled process-wide by
`REPAIR_BUILD_SEMAPHORE` (capacity `REPAIR_BUILD_CONCURRENCY = 2`). The
same semaphore covers the initiator path and the RPC-responder path so a
3-node RF=3 cluster doesn't stack four concurrent full-table walks on the
largest replica.

### 2. Leaf diff

`MerkleTree::divergent_leaf_ranges` is a tree walk that prunes any subtree
whose root hashes agree. The output is a list of `(start, end)` token
sub-ranges, each one leaf wide, where the two trees disagree. When the
replicas already agree, this list is empty and the session short-circuits
with zero partition data crossing the wire.

Adjacent divergent leaves are coalesced into maximal-contiguous spans by
`merge_contiguous_token_ranges`, capped at `MERGED_SPAN_MAX_LEAVES = 8`
leaves per span. The cap bounds the per-span working set under dense-diff
scenarios; without it a fully-divergent table would collapse to one giant
span and materialise the whole replica per session.

### 3. Chunked Fetch + streaming diff (per span)

For each merged span, the executor walks parallel cursors on local and
remote via `RepairStore::read_range_chunked(table, span_start, span_end,
cursor, REPAIR_FETCH_CHUNK_PARTITIONS, REPAIR_FETCH_CHUNK_BYTES)`. Each call
returns at most `REPAIR_FETCH_CHUNK_PARTITIONS = 64` partitions and at most
`REPAIR_FETCH_CHUNK_BYTES = 32 MiB` of encoded partition content, plus a
`next_cursor` for the follow-up chunk.

The two per-side chunks are clipped to a common `sub_end` cursor frontier,
sorted by `(token, key_bytes)`, then fed into
`diff_partition_sets_streaming`. The streaming diff consumes both vectors
in token order and emits one `RepairDecision` per partition (`AToB(p)`,
`BToA(p)`, or `Tie(key)`) — partitions are *moved* out of the input
vectors into per-direction apply queues, never cloned. The legacy
`compute_repair_plan` shape (which built a `RepairPlan { a_to_b:
Vec<Partition>, b_to_a: Vec<Partition> }` by cloning every divergent
partition) remains only for tests and one-shot callers; the live executor
path uses the streaming form.

`SkippedTimestampTie` decisions (identical max timestamp, different
content) are counted and surfaced in `SessionStats.timestamp_ties` but
**never auto-resolved** — last-write-wins with equal timestamps is
undefined in the Cassandra data model, so the operator gets the count and
must reconcile manually.

### 4. Chunked Apply (per direction)

When a per-direction apply queue reaches `REPAIR_APPLY_CHUNK_PARTITIONS =
64` partitions, the executor flushes it via `RepairStore::apply_partitions`.
For the remote direction this becomes a `RepairApplyRequest` RPC; for the
local direction it lands directly into `StorageEngine::write` (one row at a
time, with the maximum cell timestamp as the write timestamp so the
per-cell LWW merge in the memtable picks the newer copy). A final flush
runs at end-of-span.

The Apply path is the trigger that motivates write-path memtable
backpressure (see `memtable-backpressure.md`): a span with 1 000+
divergent partitions, each 1–4 MB on the fmem `entity_store`, can drive
the receiver memtable past 1 GiB inside a single session if writes outpace
the maintenance-loop's async flush drain.

## Memory model

Anti-entropy repair on a multi-GB replica in a 2 GiB cgroup is the
forcing function for the bounded-memory shape. Peak in-flight memory **per
session** is the sum of four bounded buffers:

| Buffer | Bound | Source |
|--------|-------|--------|
| Local Fetch chunk | `REPAIR_FETCH_CHUNK_PARTITIONS` partitions (64) or `REPAIR_FETCH_CHUNK_BYTES` (32 MiB), whichever comes first | `executor.rs` |
| Remote Fetch chunk | `REPAIR_FETCH_CHUNK_PARTITIONS` partitions (64) or `REPAIR_FETCH_CHUNK_BYTES` (32 MiB), whichever comes first | `executor.rs` / `rpc.rs` |
| A→B apply queue | `REPAIR_APPLY_CHUNK_PARTITIONS` partitions (64) | `executor.rs` |
| B→A apply queue | `REPAIR_APPLY_CHUNK_PARTITIONS` partitions (64) | `executor.rs` |

So the worst-case per-session in-flight working set is governed by the fetch
byte budget and apply queue count, not by table size. With defaults, each
side's fetch response is capped at 32 MiB even when a divergent range contains
many large partitions; apply queues are still partition-count bounded.

The Merkle-build path has its own, independent bound:

| Buffer | Bound | Source |
|--------|-------|--------|
| Per-build page | `MERKLE_BUILD_BATCH` partitions (16) | `repair/mod.rs` |
| Concurrent builds per node | `REPAIR_BUILD_CONCURRENCY` (2) | `repair/mod.rs` |

The build path never materialises a `Partition` struct: the SSTable
walker calls `next_partition_header_only` to park the iterator at the
header, then `stream_clustered_rows` to feed rows one at a time into
`PartitionDigestStream::update_row`. So the build's peak working set is
the bincode scratch state per row, not the partition. The
`MERKLE_BUILD_BATCH` constant bounds the cursor-page size of the
underlying `read_token_range`, which is the only place a `Vec<Partition>`
is materialised in the build path — even there, the partitions are
hashed and dropped before the next page is read.

A node hosting an in-flight repair session and serving as the
RPC-responder for another peer's session in parallel sees both budgets
simultaneously, but the build semaphore caps the total Merkle-build
budget regardless of whether the builds came from the initiator or
RPC-handler path.

Coordinator-level concurrency is capped by
`RepairCoordinator::max_concurrent_sessions` (default 4). So the
absolute upper bound on a single node's repair working set is roughly:

```
peak_repair_rss ≈ max_concurrent_sessions × (2 × REPAIR_FETCH_CHUNK_BYTES
                + 2 × REPAIR_APPLY_CHUNK_PARTITIONS × max_partition_size)
                + REPAIR_BUILD_CONCURRENCY × MERKLE_BUILD_BATCH × max_partition_size
```

In practice the per-session bound is dominated by *actual* divergence (chunks
empty out quickly on converged replicas) and the working set rarely approaches
this limit; the bound exists so a worst-case run cannot OOM the container in a
way that hides the symptom.

## RPC types

Three request/response pairs, all in `ferrosa-cluster/src/repair/rpc.rs`,
serialised with bincode (matching the existing `ReadRequest` /
`RangeReadRequest` convention) and sent on `Lane::Bulk`:

| Request | Response | Purpose |
|---------|----------|---------|
| `RepairMerkleRequest` | `RepairMerkleResponse` | Build Merkle tree for `(keyspace, table, range_start, range_end)` |
| `RepairFetchRequest` | `RepairFetchResponse` | Fetch ≤`limit` partitions and ≤`max_bytes` in `[cursor, range_end)`, return `next_cursor` |
| `RepairApplyRequest` | `RepairApplyResponse` | Apply received `Vec<PartitionWire>`, return `applied` + optional per-row error |

`RepairFetchRequestPayload` carries the chunked-iteration fields `cursor:
Option<i64>`, `limit: u32` (default `REPAIR_FETCH_CHUNK_PARTITIONS`), and
`max_bytes: u64` (default `REPAIR_FETCH_CHUNK_BYTES` for older callers).
`RepairFetchResponsePayload` returns
`next_cursor: Option<i64>` — `None` indicates the server returned every
remaining partition in `[cursor, range_end)`. The server probes for
`limit + 1` partitions and uses the extra match's token as the next
cursor, so the chunked walk doesn't need an extra round-trip to detect
"more remaining".

Wire-side partition representation is `PartitionWire` (defined in
`raft/handlers.rs`), shared with the read path. The Merkle tree itself is
serialised in `RepairMerkleResponse` — `MerkleTree` derives `Serialize` /
`Deserialize` over a `Vec<u64>` of node hashes plus depth + range bounds,
so the wire payload is bounded by tree depth (`2^TREE_DEPTH × 8 bytes ≈
256 KiB` for `TREE_DEPTH = 15`).

`Lane::Bulk` choice is load-bearing. An earlier (pre-#47) draft routed
Fetch/Apply over `Lane::Data`, which gave the bulk transfer a 3-second
coordinator timeout and caused every coordinated repair RPC to fail on a
multi-MB partition payload. `Lane::Bulk` carries the 60-second envelope
timeout sized for high-throughput latency-tolerant transfers. See the
archived bug spec
[`bug-bulk-lane-send-timeouts-on-coordinated-reads.md`](archive/bugs-verified/bug-bulk-lane-send-timeouts-on-coordinated-reads.md)
for the original symptom that drove the lane choice.

## Coordinator / executor split

The coordinator and executor are intentionally factored apart so the
fan-out logic can be unit-tested without standing up the RPC machinery.

`RepairCoordinator::repair_table` enumerates every token range the local
node is a replica of (`owned_token_ranges`), looks up the peer
participants via `repair_participants` (W8.7 — includes voters and
`owns_tokens=true` learners; excludes `owns_tokens=false` learners and
non-Normal states), collapses adjacent ranges that share the same replica
set into super-ranges, and issues one session per `(merged_range, peer)`.
Concurrency is bounded by a tokio semaphore sized at
`max_concurrent_sessions` (default 4).

The `SessionExecutor` trait abstracts "do a single repair session
between local and `peer` over `[range_start, range_end)`". Two
implementations:

- `LocalRepairExecutor` (production): drives `RepairStore` calls in the
  Merkle-then-stream shape described above. Local side is an
  `Arc<StorageEngineRepairStore>` wrapping `Arc<StorageEngine>`; remotes
  are `Arc<RemoteRepairStore>` instances pointed at each peer's
  `host_id` via `PeerManager`.
- Test mocks (e.g. `MockExecutor` in `coordinator::tests`): record the
  scheduled `(table, range, peer)` triples without doing any I/O. Used
  to pin the merge-adjacent-ranges and concurrency-cap contracts in
  isolation.

The `RepairStore` trait further abstracts the local vs remote side. Four
implementations:

| Impl | Side | Storage | Used by |
|------|------|---------|---------|
| `StorageEngineRepairStore` | local | `Arc<StorageEngine>` | production initiator |
| `RemoteRepairStore` | remote | RPC via `PeerManager` | production initiator |
| `LocalRepairExecutor` (composed) | both | trait objects | production wiring |
| `InMemoryRepairStore` | both | `Vec<Partition>` in `Mutex` | unit tests |

The HTTP handler in `ferrosa/src/web/api.rs` does the wiring: builds the
local store from `state.storage`, builds one `RemoteRepairStore` per
non-self node in the ring, composes them into a `LocalRepairExecutor`,
and hands that to `RepairCoordinator::repair_table`.

## Operator interfaces

### HTTP

`POST /api/cluster/repair?keyspace=<ks>&table=<t>&rf=<n>` on port 9090
(the cluster admin web surface). `rf` defaults to 3. Response is JSON:

```
{
  "total_sessions": N,
  "ok": ok,
  "failed": fail,
  "partitions_streamed_in": X,
  "partitions_streamed_out": Y,
  "timestamp_ties": Z
}
```

Returns 503 if the node isn't in cluster mode (no token ring, no peer
manager), 400 on missing query parameters.

### CLI

`ferrosa-ctl repair --keyspace=<ks> --table=<t> [--rf=<n>]` shells out to
the HTTP endpoint via the configured web host/port. Source:
`ferrosa-ctl/src/main.rs` (`Repair` subcommand),
`ferrosa-ctl/src/commands.rs` (`repair` function building the URL +
issuing the POST).

## Configuration knobs

All constants live in `ferrosa-cluster/src/repair/`:

| Constant | Default | Effect |
|----------|---------|--------|
| `TREE_DEPTH` | 15 | Merkle leaves per range = `2^15 = 32 768` |
| `MERKLE_BUILD_BATCH` | 16 | partitions decoded per `read_token_range` page during a build |
| `REPAIR_BUILD_CONCURRENCY` | 2 | concurrent Merkle builds per node (initiator + responder share this budget) |
| `REPAIR_FETCH_CHUNK_PARTITIONS` | 64 | partitions per `RepairFetchRequest` chunk |
| `REPAIR_FETCH_CHUNK_BYTES` | 32 MiB | encoded partition bytes per `RepairFetchRequest` chunk |
| `REPAIR_APPLY_CHUNK_PARTITIONS` | 64 | partitions per `RepairApplyRequest` chunk |
| `MERGED_SPAN_MAX_LEAVES` | 8 | cap on adjacent divergent leaves coalesced into one span |
| `RepairCoordinator::max_concurrent_sessions` | 4 | sessions in flight per node |

None of these are operator-tunable today — they're compile-time
constants chosen to fit the 2 GiB fmem cgroup. A configuration surface is
listed under "Limits / open work".

## Limits / open work

The current shape is a working operator-driven baseline. Known gaps:

- **No scheduler.** Repair runs only when an operator triggers
  `POST /repair` or `ferrosa-ctl repair`. A periodic / continuous-repair
  loop with per-keyspace cadence is not implemented. The
  `coordinator.rs` module header still references a `RepairScheduler`
  background task in its design comment; that piece has not landed.
- **No per-keyspace policy.** `RepairCoordinator::default()` applies the
  same concurrency cap to every table. No way to mark
  high-priority / low-priority tables, exclude system keyspaces, or set
  a maintenance window.
- **No tunable constants.** All seven knobs above are compile-time. A
  configuration surface (`FERROSA_REPAIR_*` env vars) is straightforward
  to add once the operator workflow stabilises.
- **No Jepsen-verified convergence.** Unit tests cover the algorithm
  (`diff_partition_sets_*`, `compute_repair_plan`,
  `repair_table_*`) and the in-memory executor exercises end-to-end
  convergence. The full RF=3 + nemesis story tracked in
  `specs/jepsen-e2e-test-plan.md` is still pending the Jepsen
  infrastructure work (S5 in the sprint plan).
- **No multi-DC awareness.** `repair_participants` resolves replicas
  through `TokenRing::replicas`, which is SimpleStrategy today. NTS
  multi-DC repair (only repair within a DC unless explicitly fanned
  cross-DC) is open work alongside the rest of the multi-DC track in
  `specs/todo/todo-multi-dc-node-dc-assignment.md`.
- **No artifact preservation.** Successful repair runs are not recorded
  durably — the `SessionStats` only surface in the HTTP response. A
  `system.repair_history` virtual table or persisted log would be the
  obvious next step for operator audit.
- **Streaming range-read floor.** The Fetch RPC walks
  `StorageEngine::read_token_range` per chunk. That path is bounded
  memory but currently ~50× slower than the arithmetic floor on the
  fmem cluster — tracked in
  [`specs/todo/bug-streaming-range-read-perf-50x-floor.md`](todo/bug-streaming-range-read-perf-50x-floor.md).
  Fix that and the per-session repair time drops by the same factor.

## Related Specs

- [Memtable Backpressure](memtable-backpressure.md) — the receiver-side
  flow control that prevents repair's apply phase from OOM'ing the
  receiver
- [Storage](storage.md) — `read_token_range`, `walk_token_range_for_digest`,
  and the streaming read primitives repair builds on
- [Components](components.md) — `ferrosa-cluster` public types,
  `ferrosa-ctl` subcommands
- [Data Flow](data-flow.md) — end-to-end repair flow diagram
- [Jepsen E2E Test Plan](jepsen-e2e-test-plan.md) — the deferred
  convergence-under-nemesis verification
