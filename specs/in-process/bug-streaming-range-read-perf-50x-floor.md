---
type: todo
priority: P2
status: draft
created: 2026-05-16
updated: 2026-05-16
affected-versions: ferrosa-cluster v0.10.0 + feat/idle-timeout-watchdog (PR #43)
---

# Bug: streaming range-read wall time is ~50× the arithmetic floor on a local-only 3-node cluster

## Why this is a Ferrosa bug

The ADR-020 streaming range-read path (PR #43) is architecturally
correct: memory bounded at every layer (handler bounded-memory test
passes, no OOM at the storage iterator level), dedup correct
(10 800 vs 30 901), no per-RPC wall-clock timeout. But the
**observed wall time is 50–110 s for a 9 774-partition table on a
3-node cluster where every container is on the same host with no
network at all**.

That's wildly higher than the arithmetic floor. With ~30 µs per
partition for bincode + mpsc + TCP-loopback overhead, an entire
local-only fan-out of 30 000 partition transfers (RF=3) should
finish in under a second. We're at 50 s+. **No single hot path
from code reading explains the gap.**

## Observed on

- Ferrosa: `feat/idle-timeout-watchdog` branch (PR #43), commit
  `65c0406`, image `ferrosa-memory-node:adr020-lazy`.
- Live ferrosa-memory cluster, 3 nodes (Docker, 2 GiB mem_limit
  each), MinIO local S3, RF=3, `FERROSA_BULK_STREAMING_RANGE_READ=1`.
- Test query: `SELECT COUNT(*) FROM agent_memory.entity_store`
  against the 9 774-partition `agent_memory.entity_store` table.

## Measurements

5 back-to-back runs of the same query:

| Run | Result | Wall  |
|-----|--------|-------|
| 1   | 10 800 ✓ | 113 s |
| 2   | 10 800 ✓ | 51 s  |
| 3   | idle timeout 30 s | 120 s |
| 4   | 10 800 ✓ | 67 s  |
| 5   | 10 800 ✓ | 104 s |

Aggregate: ~10 800 partitions × 3 replica fan-out / 51 s minimum
= **~635 partition transfers / second**, or **~1.6 ms per
partition transfer**. The arithmetic floor is somewhere around
20–40 µs (bincode + mpsc hop + TCP loopback for a small frame).

**~50× the floor.**

SSTable count per node for this table:

| Node  | SSTables |
|-------|---------|
| node1 | 167     |
| node2 | 85      |
| node3 | 35      |

Average SSTable size: 4.3 MB (uncompressed — 0 `CompressionInfo.db`
files for this table).

## Suspected scope (none confirmed; need profiling)

1. **SSTable iterator open cost.** `RangeMerger::new` opens
   `partitions_iter()` on every SSTable in the view at once. With
   167 readers, that's 167× (bloom filter alloc + partition_index
   trie load). Even at 10 ms each, that's 1.7 s just for setup.
   The merger keeps all 169 iterator slots populated; per-source
   peek-ahead means we touch all 167 SSTables before yielding the
   first partition.

2. **Per-partition file I/O syscall count.** Each
   `read_partition_limited_rows()` call probably issues several
   `pread` syscalls (header → metadata → cells). Page cache makes
   each syscall ~µs but the count adds up. With 169 sources and
   peek-ahead, every yielded partition might involve syscalls on
   multiple SSTables.

3. **Coordinator-side `Vec` amplification.** In
   `coordinate_range_read_stream_limited_rows`:
   - Local read still uses the **materializing**
     `read_local_range_limited_rows`, not the lazy iterator.
   - Remote results are `extend`-ed into a `Vec<Partition>` before
     dedup — holds 3× replica copies (~30 000 partitions) at
     peak.
   - `dedup_by_token` builds a `BTreeMap<i64, Vec<Partition>>`
     intermediate.
   The streaming on the wire doesn't bound the coordinator's
   memory because the CQL caller signature is `Vec<Partition>` —
   can't stream out without folding (COUNT) or paging (SELECT).

4. **Concurrent compaction load.** The cluster is mid-flush /
   compaction much of the time. The data runtime is shared
   between handler streaming and compaction tasks. Heartbeat
   ticker scheduling can be starved (one run did trip the 30 s
   idle timeout). Storage I/O contends with the merger.

5. **tokio scheduler overhead.** Per-partition pull goes through
   `spawn_blocking → mpsc::Receiver → Stream::next() →
   tokio::select! → emit_chunk → bincode → PeerManager::fire →
   inbound handler → StreamRouter::route → mpsc → consumer's
   IdleTimeoutWatchdog → bincode decode → batch extend`. That's
   ~10 await/poll points per partition. Even 10 µs each = 100 µs
   per partition, ~1 s for 10K.

6. **`HeapEntry` deep clones in the merger.** Each peek pushes a
   full `Partition` into the heap entry. For wide partitions, the
   deep clone is non-trivial. With 169 sources peeking ahead,
   that's 169 Partition clones held at any moment, plus
   replacement on every pop.

7. **OOM under repeated load.** Node1 (167 SSTables, leader) hit
   `OOMKilled: true exit 137` again after a few queries even with
   the lazy storage iterator. The lazy iterator bounds the
   handler-side Vec, but the coordinator-side Vec (#3) and the
   per-SSTable iterator state (#1) accumulate.

## Repro

1. Cluster: `cd ferrosa-suite/ferrosa-memory && docker compose up -d`
   with `FERROSA_BULK_STREAMING_RANGE_READ: "1"` in each node env.
2. Wait for Raft leader (~30 s).
3. `cqlsh 127.0.0.1 19042 -u ferrosa_admin -p ferrosa_admin
   --request-timeout=120 -e "SELECT COUNT(*) FROM
   agent_memory.entity_store;"` — observe 50–110 s wall on a
   table that local-only should finish in <1 s.

## 2026-05-17 third update: spread-the-data experiment confirms data layout is the bottleneck

To test the hypothesis that the cold-cache 32 s wall is caused by
**data layout** (overlapping SSTable runs forcing many same-key
dedup pops), I re-INSERTed every row from the cluster at CL=ALL
through node1's coordinator. The CL=ALL fan-out forces the write
to land on every replica.

Before (drifted state from organic load):
| Node  | Rows  | Size    | SSTables |
|-------|-------|---------|----------|
| node1 | 13807 | 1.77 GB | 47       |
| node2 |  9774 | 0.33 GB | 47       |
| node3 |  9774 | 0.15 GB | 19       |

After re-INSERTing 10800 rows at CL=ALL (took 7.5 min at 24 inserts/s):
| Node  | Rows  | Size     | SSTables |
|-------|-------|----------|----------|
| node1 | 39984 | 1.22 GB  | 42       |
| node2 | 10801 | 0.32 GB  | 47       |
| node3 | 10801 | 0.14 GB  | 19       |

Per-node `SELECT pk,ck LIMIT 5` after spread (was 32 s on node1):
| Node  | Run 1  | Run 2  | Run 3  |
|-------|--------|--------|--------|
| node1 | 6.80s  | 3.55s  | 3.61s  |  ← FIXED
| node2 | 21.99s | 21.99s | 22.07s |  ← regressed
| node3 |  3.39s | 22.21s |  3.37s |

**node1 went from 32 s to 3.5 s consistent.** node2/node3 got
slower because they absorbed fresh overlapping writes into their
existing SSTables, increasing the merger-pop dedup work for their
local reads.

**Diagnosis confirmed**: the bottleneck is data layout, not the
merger code. When SSTables have less overlap (fewer overlapping
runs per token range), the merger does fewer dedup pops per
emitted partition. Compaction is the cure.

The bigger architectural lesson is that **the cluster's
data-plane replication is operationally fragile**: organic load
produced a 12× size imbalance, the CL=ALL forced-fanout fixed
node1 but stirred the same imbalance on node2/node3.

`SELECT COUNT(*)` per-node is now also inconsistent across
replicas (39984 vs 10801 vs 10801) — that's because the
per-node aggregation isn't deduplicating across replicas, AND
node1 is now holding more recent versions of rows it merged
through the CL=ALL coordinator.

## 2026-05-17 second update: peek/pop merger, non-blocking logs, LIMIT pushdown shipped — and three NEW concrete findings

Three architectural improvements landed in commit 9030f1d:

1. **Peek/pop merger split.** `MergeSource::peek_key()` returns the
   next key WITHOUT decoding the partition body (new
   `DataReader::peek_partition_key` reads only the partition header).
   Heap is primed via peeks; pop_partition decodes the body only
   when popped. This eliminates the eager 38-source body decode at
   merger construction time.

2. **Non-blocking logging.** `tracing_subscriber::fmt()` was
   writing to stdout SYNCHRONOUSLY. With the cluster emitting
   hundreds of `info!()` lines per minute (compaction +
   schema-sync + raft heartbeats), a slow stdout consumer
   (docker json-file driver under host disk pressure)
   back-pressured every emitter — including the storage hot
   path. Switched to `tracing_appender::non_blocking`.

3. **LIMIT pushdown.** `range_iter_projected` now takes
   `partition_limit: Option<usize>` and the producer breaks the
   loop after N emissions. Without this, the bounded mpsc
   buffer (was 64, now 4) caused the producer to race ahead by
   ~buffer body decodes after the consumer had already read
   enough, turning a 5-decode scan into a 69-decode scan.

**The 32 s cold-cache wall persists on node1 even after all three
fixes.** The remaining bottleneck is per-emit cost: for the
smallest-token dedup group, the merger pops every overlapping
source (~30 of node1's 47 SSTables on entity_store) and each pop
is a full wide-partition body decode on cold cache.

### Three NEW concrete bugs identified during this investigation

**1. Write-path replication is broken / unbalanced.**

| Node  | entity_store size | SSTable count |
|-------|------------------|---------------|
| node1 | 1.77 GB          | 47            |
| node2 | 0.33 GB          | 47            |
| node3 | 0.15 GB          | 19            |

RF=3 but writes are not landing on peers symmetrically. Writes
either land only on one replica or hinted handoff isn't
delivering. node3 has 1/12 of node1's data. Cassandra-style data
plane writes don't go through Raft (which only handles schema /
topology) — this is in the WritePath / coordinator. Reproducible:
direct query to each node returns DIFFERENT first 5 partitions.

**2. Read repair is not implemented.**

`coordinator::metrics::ReadRepairMetrics` exists with
`read_repairs_attempted / succeeded / failed` counters, but a
codebase-wide grep for `.attempted()` / `.succeeded()` /
`ReadRepairMetrics::` shows only the struct constructor — no
production code increments these counters. The scaffolding is
present, the read repair LOGIC is absent. Without it, divergent
replicas (see issue 1) never reconcile.

**3. `system.peers` advertises `rpc_address=127.0.0.1`.**

```
peer       | rpc_address | native_address
172.20.0.4 |   127.0.0.1 |      127.0.0.1
172.20.0.5 |   127.0.0.1 |      127.0.0.1
```

All three replicas advertise themselves on the same loopback
address. The cqlsh driver discovers three hosts and round-robins
between them at 127.0.0.1:9042 — which maps only to node1
externally. Inside docker the published ports are
19042/19043/19044 mapped to nodes 1/2/3 respectively. This caused
an early misdiagnosis where every 3rd query (round-robin
host-3) appeared slow; in reality all three "hosts" resolve to
the same coordinator, but data asymmetry across nodes (issue 1)
made node1's coordinator-side reads pathological.

These three issues are independent of the cold-cache scan
bottleneck but were ALL surfaced by the perf investigation.

---

## 2026-05-17 update: cold-cache root cause identified

The **cold-cache 32 s wall** for `SELECT pk, ck FROM
agent_memory.entity_store LIMIT 5` (separate symptom from the
50× streaming gap above, but in the same code path) has a
single mechanical cause: the k-way merger's heap-priming loop
in `RangeMerger::new` calls `refill_source(src)` for every
source up-front, and each `refill_source` for an SSTable source
**reads and decodes the full first partition body** just to
extract its key for the heap entry.

For `entity_store` (47 SSTables, disjoint-runs grouping
collapses to 38 runs because token ranges are heavily
overlapping), 38 cold-cache full-partition decodes ≈ 38 × ~800
ms = ~30 s wall before the merger emits a single partition.
Per-source timing (instrumented via `PERF slow refill_source`
trace on a host-page-cache-evicted run):

```
src=5  iters=1 ms=1257
src=6  iters=1 ms=1251
src=7  iters=1 ms=1254
src=8  iters=1 ms=1252
src=9  iters=1 ms=713
src=10 iters=1 ms=688
src=11 iters=1 ms=2635
src=12 iters=1 ms=1313
...
```

`iters=1` confirms each source pays for exactly one partition
decode and the cost is per-decode, not per-iteration. The wide
entity rows (768-dim float32 embedding cell + clustering)
amplify per-partition I/O even with projected/byte-skip mode
(`next_partition_projected(&[])`) because cells must still be
*read* to be skipped.

### Why disjoint-runs barely helps for this table

`PERF disjoint runs computed sstable_count=47 run_count=38
run_sizes=[4,2,2,2,1,1,1,1,1,1,1,1,1,1,1,1,2,2,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,2,1]`

Most runs are size 1 — the SSTables overlap pairwise in token
space, so concatenation reduces only 47 → 38, not 47 → ~log N.
A properly-compacted LSM would have far fewer runs; this
cluster's UCS state is highly fragmented.

### Proposed fix: key-cursor over partition_index

The merger reads partition bodies to learn keys. The
`PartitionIndex` already has every key cached
(`smallest_key`/`largest_key` and walkable trie). The fix is to
add a `PartitionKeyCursor` abstraction that walks the trie
producing `DecoratedKey` only, and to make the merger heap key
the `(next_key, source_id)` pair — decoding the partition body
only when the source is popped.

Then for `LIMIT 5` on cold cache:
- Init: 38 sources × *trie-walk-only* key lookup ≈ 38 × few ms = <200 ms
- Per emit: pop min, decode that one partition body
- Total cold: 5 partition decodes + ~38 trie walks ≈ <2 s

Trade-off: requires extending `SSTableReader` /
`PartitionIndex` with a key-iteration API (`iter_keys() -> impl
Iterator<Item = DecoratedKey>`). The trie supports it but the
current code only exposes lookup by key, not in-order walk.

### Why this is independent of the disjoint-runs work

The disjoint-runs collapse is correct and helpful when the LSM
is compacted (the typical case). For the cold-cache pathology
on fragmented LSMs, the *runs themselves* still each pay the
priming cost. Both optimizations stack — compacted LSM →
fewer runs → fewer priming reads → fast even cold.

Short-term mitigation while the key-cursor change lands: drive
UCS to actually compact `entity_store` down to <10 SSTables.
That alone reduces 30 s init → ~6 s on cold cache without any
code change.

## Investigation next steps

- **Profile inside the container.** `perf record -p <ferrosa-pid>
  -F 99 -g sleep 60` during a query → flamegraph. Probably
  pinpoints where the 1.6 ms/partition goes (S3? syscalls? heap?
  bincode? Drop?).
- **Per-stage tracing.** Add per-chunk wall-clock spans on the
  producer (merger → bincode → fire), wire (TCP), consumer
  (recv → decode → batch). Tells us if the bottleneck is
  storage, network, or aggregation.
- **Single-replica path measurement.** Run COUNT with CL=ONE so
  only the local node is consulted; compare wall time to the
  fan-out result. Isolates fan-out cost.
- **Single SSTable test.** Build a synthetic 10K-partition table
  with one large SSTable and measure storage-iterator throughput
  alone (no merge across 167 sources).
- **Coordinator-side fold/stream.** For COUNT(*), the coordinator
  could fold a count rather than collect every partition into a
  Vec. ADR-020 next-step: thread a fold/yield callback through
  the coordinator entry so the CQL layer can choose how to
  consume.

## Why it's load-bearing (and why P2 not P1)

The streaming path now works *correctly* — bounded handler
memory, correct dedup, no per-RPC timeout, validated against
the live cluster. The perf gap doesn't *break* anything that
worked before; the legacy path with the 3 s `BULK_READ_TIMEOUT`
was strictly worse (failed entirely). But:

- 50 s for a 10 K-row table is unusable for any interactive
  workload.
- The OOMs under repeated load mean the cluster can't sustain
  even moderate streaming-COUNT load with `RF=3` and 2 GiB nodes
  until the coordinator-side Vec is addressed.
- The MCP workbench summary endpoint (which uses
  `coordinate_index_read`, still on the legacy path) keeps
  504-ing because the MCP HTTP timeout is 30 s while legitimate
  reads take much longer.

Related: [[bug-bulk-lane-send-timeouts-on-coordinated-reads]],
[[020-streaming-internode-range-read]].
