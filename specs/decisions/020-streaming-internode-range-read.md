# ADR-020: Streaming internode range read with idle-timeout watchdog

> Date: 2026-05-16
> Status: Proposed
> Scope: `ferrosa-cluster::coordinator::read` (range / index reads),
> `ferrosa-cluster::raft::handlers::{RangeReadHandler, IndexReadHandler}`,
> `ferrosa-net` (multi-message RPC over the `Bulk` lane), and the
> `ferrosa-storage` range iterator surface.
> Non-goal: changing the CQL-client paging protocol; replacing
> single-partition reads (`coordinate_read`) — those stay request-response.

## Context

Cross-node range and index reads currently route through a single
request-response RPC on the `Bulk` lane:

- `RangeReadRequest { keyspace, table, limit, row_limit }` →
- `RangeReadResponse { partitions: Vec<PartitionWire>, truncated: bool }`

Three magic-number caps and timeouts make this architecturally
unscalable, and they all couple together to surface the same symptom:

1. **Wire payload is fully materialized.** The receiver collects every
   matching partition into a `Vec<Partition>`, serializes the whole
   thing with `bincode`, then sends it as one
   `Message::RangeReadResponse` blob. Memory is `O(table_window)` on
   both sides; first byte to the coordinator arrives only after the
   last byte was produced by the handler.
2. **Storage layer enforces a 10 000-partition cap.**
   `RANGE_READ_MATERIALIZATION_CAP = 10_000` in
   `ferrosa-storage/src/store.rs` errors with
   `"range read limit … exceeds materialization cap …; use a paged/streaming read path"`
   — but no such paged/streaming read path exists at the internode
   layer today.
3. **Coordinator deadline is a wall-clock timeout.**
   `BULK_READ_TIMEOUT` (currently 3 s, proposed 8 s in PR #41) is a
   single deadline for the entire RPC. There is no notion of "the
   peer is still streaming, just slowly" — either the full vec is on
   the wire within the deadline or the coordinator gives up.

### Empirical baseline (2026-05-16, ferrosa-memory cluster)

Three back-to-back `SELECT COUNT(*) FROM agent_memory.entity_store`
runs (9 774 partitions, 3 nodes, RF=3, no concurrent load):

| Run | Wall-clock |
|-----|------------|
| 1   | 7.28 s     |
| 2   | 37.15 s    |
| 3   | 7.32 s     |

A bounded `SELECT … LIMIT 5` against the same table completes in
0.65 s. The same query that returns 5 rows in <1 s takes 7-37 s when
the LIMIT is removed — confirming that materializing the full vec on
both ends is what's slow, not the underlying reads.

PB-scale tables make this categorically impossible: a 1 TB table
serialized through a 10 Gbps link is 800+ seconds on the network
alone, before storage, serialization, or deserialization.

### What the user is observing

> "what if the tables were PB in size … should the read timeout … be
> a monitor that allows for streaming recovery? … there should not be
> magic number caps that is an antipattern"

Both observations are correct. The wall-clock timeout and the
10 000-partition cap are independent symptoms of the same architectural
gap — there is no chunked, observable, back-pressured streaming path
between the coordinator and the storage-owning node.

## Decision

Replace the single-RPC range read with a **multi-message streaming
RPC**, gated by an **idle-timeout watchdog** instead of a wall-clock
deadline, with the storage layer producing partitions through a lazy
iterator. No hardcoded partition cap; no hardcoded total-wall-clock
timeout.

```
                Coordinator                        Storage-owning node
        ┌─────────────────────────┐               ┌──────────────────────────┐
        │  range_read_stream(...) │ ── Request ──▶│  storage.range_iter(...) │
        │                         │               │                          │
        │  ┌──────────────────┐   │  Chunk(0..N) │  for batch in iter {     │
        │  │ idle_deadline    │ ◀─────────────────│      send(Chunk(batch)); │
        │  │   = NOW + IDLE   │   │   Heartbeat   │      if slow:            │
        │  │  reset on chunk  │ ◀─────────────────│         send(Hb);        │
        │  │  reset on hb     │   │     Done      │  }                       │
        │  └──────────────────┘ ◀─────────────────│  send(Done(total))       │
        │                         │               │                          │
        │  yield partitions       │               │                          │
        └─────────────────────────┘               └──────────────────────────┘
```

### Required pieces

1. **Storage layer: lazy range iterator.**
   Replace
   ```
   fn read_range_limited_rows(...) -> Result<Vec<Partition>>
   ```
   with (additionally; the old call site keeps working for single-node
   use):
   ```
   fn range_iter(start, end) -> impl Iterator<Item = Result<Partition>>
   ```
   The iterator merges memtable + flushing memtable + SSTables in
   token order, applies deletions on the fly, and yields one partition
   at a time. No vec materialization, no `RANGE_READ_MATERIALIZATION_CAP`.

2. **Wire protocol: chunked response messages.**
   Replace the single response with three message variants on the
   Bulk lane:
   ```
   Message::RangeReadChunk {
       request_id: u32,
       seq: u32,
       partitions: Vec<PartitionWire>, // small batch, ~64-256 partitions
   }
   Message::RangeReadHeartbeat { request_id: u32, seq: u32 } // sent when a chunk takes >IDLE/2 to produce
   Message::RangeReadDone {
       request_id: u32,
       total_chunks: u32,
       truncated: bool,
   }
   ```
   `request_id` correlates a stream; `seq` lets the coordinator
   detect gaps and reorders (Bulk lane is TCP, so reorders are
   excluded, but `seq` is cheap insurance against future lane
   changes). The chunk size is **a configurable target**, not a
   hardcoded constant — exposed via `NetConfig` and chosen at runtime
   to fit the MTU/buffer budget.

3. **Bulk lane: multi-message RPC support.**
   The existing `dispatch_send` in `ferrosa-net::lane_actor` is
   request-response — one Message in, one Message out, then drop.
   Adding streaming responses needs the existing bootstrap-streaming
   machinery (`ferrosa-cluster::streaming`) generalized so any RPC on
   the Bulk lane can return a stream of frames keyed by `request_id`.
   The sender API becomes:
   ```
   pool.send_stream(msg, Lane::Bulk) -> impl Stream<Item = Result<Message>>
   ```
   That stream completes when a `Done` frame arrives, errors on idle
   timeout, or errors on explicit `Error` frame.

4. **Coordinator: idle-timeout watchdog, not wall-clock deadline.**
   Drop `BULK_READ_TIMEOUT` entirely. Introduce:
   ```
   NetConfig {
       bulk_stream_idle_timeout: Duration,   // default: 10s, no chunk = abort
       bulk_stream_heartbeat_interval: Duration, // default: 4s, sender pings
   }
   ```
   The coordinator's stream consumer resets its watchdog every time a
   `RangeReadChunk` *or* `RangeReadHeartbeat` arrives. If the gap
   between any two messages exceeds `bulk_stream_idle_timeout`, the
   coordinator cancels the stream (sends `CancelStream { request_id }`,
   peer aborts the iteration). Total wall-clock can be hours for a PB
   scan; the watchdog only fires when the peer is actually stuck.

5. **Heartbeat policy on the sender.**
   The handler iterates the storage iterator and batches partitions.
   It maintains its own `last_send_at` timestamp; if more than
   `heartbeat_interval` elapses while waiting for the next batch
   (e.g. S3 fetch, compaction back-pressure, large partition decode),
   it sends a `RangeReadHeartbeat` and continues iterating. The
   coordinator treats heartbeats as activity but discards them from
   the result set.

6. **Cancellation path.**
   `CancelStream { request_id }` lets the coordinator stop the
   handler when the CQL client disconnects, when read-quorum is
   already satisfied by other replicas, or when the user issues
   `KILL`. The handler must check a cancellation flag between batches
   so a runaway scan can be stopped cheaply.

7. **No magic-number partition cap.**
   `RANGE_READ_MATERIALIZATION_CAP` and `DEFAULT_RANGE_READ_LIMIT`
   are removed from the internode path. Limits flow from the CQL
   layer (the client's `PAGE_SIZE` and `LIMIT` clauses) down through
   the coordinator into the storage iterator, which simply stops
   pulling partitions when the upstream consumer stops asking. The
   storage iterator itself bounds memory by definition — one
   partition at a time.

## Consequences

### Wins

- **Scales to PB-sized tables.** Memory is `O(chunk_size)` regardless
  of table size. First chunk reaches the coordinator in tens of
  milliseconds.
- **No spurious timeouts.** A slow scan that produces a chunk every
  few seconds runs to completion. Only genuine stalls (peer crash,
  network partition) abort the stream.
- **Back-pressure is natural.** TCP flow control on the Bulk lane
  paces the sender; the storage iterator stops decoding ahead of the
  consumer.
- **Removes two existing antipatterns** — the 3 s/8 s wall-clock
  timeout magic number and the 10 000-partition materialization cap.

### Costs

- **Three new message variants** on the Bulk lane and matching
  CapnProto schemas if the framing gate (ADR-019) is on. Each variant
  carries a `request_id` correlation field.
- **Bulk lane RPC machinery needs a multi-message mode.** Today it's
  strict request-response. Adding streams touches `lane_actor`,
  `rpc::client`, and `rpc::server`. The bootstrap streaming code path
  is the right reference — its frame correlation, completion, and
  cancellation primitives should be generalized rather than
  duplicated.
- **Backwards compatibility.** Mixed-version clusters during rolling
  upgrade: a v0.10.0 coordinator talking to a v0.11.0 handler (or
  vice versa) must negotiate. Two options:
  1. Add the new RPC under a new `MsgType` (`RangeReadStreamRequest`,
     `RangeReadStreamChunk`, …) and fall back to the legacy
     `RangeReadRequest` when the peer's announced protocol version is
     older. This is the safer path — old path keeps working until all
     nodes are upgraded.
  2. Replace in place behind a config flag.
  Choose (1).
- **Coordinator code complexity.** Today a range read is one
  `send_remote_with_reconnect_timeout` per peer plus a join. The new
  path is a stream consumer with per-peer state machines (chunk
  reassembly, dedup-on-the-fly, cancellation). The complexity is
  intrinsic to the problem, but it lands somewhere new in the
  coordinator codebase.
- **Tests need stream-aware fixtures.** Existing
  `DelayedRangeReadHandler` test pattern verifies a 1 s delay
  succeeds inside the 3 s timeout. New tests need:
  - A slow-but-steady producer (chunks every 1 s for 30 s) succeeding
    with idle_timeout=10 s.
  - A genuinely-stuck producer (heartbeats stop) aborting via the
    watchdog.
  - A cancellation case where the coordinator drops the stream
    mid-flight and the handler observes the cancel within one
    batch boundary.

## Migration plan

This is a multi-PR change; each PR lands behind a feature flag and is
independently testable.

1. **ADR + design lock-in** (this doc).
2. **Storage iterator.** Add `range_iter` returning
   `impl Iterator<Item = Result<Partition>>`. Existing
   `read_range_limited_rows` stays as a thin wrapper that collects N
   items. Unit tests cover memtable + flushing + SSTable interleaving
   without materialization.
3. **Bulk lane stream primitive.** Generalize the bootstrap streaming
   correlation/completion machinery into a `pool.send_stream` API.
   Tests cover happy-path stream, idle-timeout fires, cancellation
   propagates.
4. **New range-read RPC** with the chunk / heartbeat / done frames,
   gated by a `bulk_streaming_range_read = false` config flag.
   Coordinator probes peer protocol version; falls back to the
   existing `RangeReadRequest` when the peer is older or the flag is
   off.
5. **Enable by default** once a release cycle of bake-time confirms
   no regressions. Remove the legacy
   `RangeReadRequest`/`RangeReadResponse` path and
   `BULK_READ_TIMEOUT` constant in the release after that.
6. **Remove `RANGE_READ_MATERIALIZATION_CAP`** when the legacy path
   is gone — the cap is only there because the legacy path
   materializes.

The same shape generalizes to `IndexReadRequest` (also currently a
single-response RPC) and `RangeWriteRequest` (the inverse direction
for bulk imports). Those follow as separate ADRs once this one lands.

## Windowed continuation + fail-loud terminal contract (t_a0f922a3)

The paged multi-replica scan layers **flow control** and a **fail-loud
completion contract** on top of the base streaming RPC. Both are
correctness-critical: a violation silently truncates a full-table scan
(data loss), which is strictly worse than a crash.

**Windowed continuation (flow control).** A `RangeReadStreamRequest`
carries `max_chunks`: the producer stops after that many chunk frames and
reports the position after its last emitted row in
`RangeReadStreamDonePayload.resume`. The coordinator's
`WindowedReplicaForwarder` fires the next window (a fresh `request_id`,
resumed at that position) only after its consumer drains the previous one.
Un-drained frames per stream never exceed one window, so the bounded route
buffer cannot overflow no matter how large the scan is — no server-side
result cap, bounded memory. A `Done` with `resume: None` means the
producer's local range is genuinely exhausted.

**Fail-loud terminal contract (t_a0f922a3 bug #2).** The N-way merge
concludes the whole scan is complete once *every* source's stream ends. A
source stream ends when its per-replica `remote_tx` drops. That inference
— "channel closed ⇒ replica exhausted" — is only sound when the forwarder
closed for a reason that makes a silent close *correct*:

- the producer signalled genuine exhaustion (`Completed { resume: None }`), or
- the consumer *deliberately* abandoned the merged output (a paged read
  filled its page).

The forwarder sets a per-source `clean_end` flag at exactly those two
terminations (and at the window-boundary `remote_tx.is_closed()` abandon).
Every source stream is wrapped by `clean_end_guarded_stream`: if the
channel closes **without** `clean_end` set — a panicked forwarder task, or
any future refactor that drops `remote_tx` without delivering an error or
signalling exhaustion — the wrapper emits one final **loud, retryable
`Internal` error** instead of a silent `None`. The scan then fails loudly
(and drivers retry the idempotent range read) rather than returning a
partial result with `has_more = false`. A forwarder that already delivered
its own error item (the `Failed` path) is unaffected: the merge sees that
error first and the fallback never fires.

Observability: `forwarder_diag::{error_send_dropped, continuations_fired}`
are process-global counters. `error_send_dropped` counts loud replica
errors that could not be delivered because the merged output was already
gone — benign under a deliberate page abandon, but a non-zero value on a
full-drain scan is the silent-partial signature and is asserted on by the
`range_scan_multi_replica_paging` harness.

**Not reproduced in-process.** The `range_scan_multi_replica_paging`
harness drives the exact live shape — 3-node loopback cluster, RF=3,
CL=ALL, disjoint per-replica data, wide partitions, and
`FERROSA_RANGE_READ_ROWS_PER_FRAGMENT=1` to force ~hundreds of window
continuations per page — and pages to an **exact** union every time, with
`error_send_dropped == 0` and `route_closures == 0`. The live 21160-of-50807
truncation did not reproduce in-process, consistent with the historical
observation that fresh-data reproductions all page complete; the fail-loud
guard closes the class of silent-complete structurally rather than by
matching the exact (environmental/timing) live trigger.

## Alternatives considered

- **Just bump the timeout** (PR #41). Stopgap: hides the symptom at
  small scale, fails at any scale.
- **Bump the partition cap.** Same problem, larger blast radius —
  10K → 100K means O(table_size) memory pressure grows 10× before
  failing.
- **Force every client to page via CQL `PAGE_SIZE`.** Pushes the
  problem onto every caller and breaks `SELECT COUNT(*)` /
  full-table aggregates which inherently need a fan-out.
- **Stream over a side channel** (e.g. HTTP/2 separate from the
  Bulk lane). Introduces a new transport and credential path; the
  Bulk lane already has the right framing and TLS — just needs
  multi-frame correlation.

## Related

- [[bug-bulk-lane-send-timeouts-on-coordinated-reads]] — the symptom
  this design addresses.
- [[019-capnproto-internode-protocol]] — the envelope work that makes
  adding new typed message variants safe.
- `ferrosa-cluster::streaming::*` — the bootstrap-streaming primitives
  to generalize.
- `ferrosa-cql` paging — the existing client-side paging model whose
  shape this internode design mirrors.
