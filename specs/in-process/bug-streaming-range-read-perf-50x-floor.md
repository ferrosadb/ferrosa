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
