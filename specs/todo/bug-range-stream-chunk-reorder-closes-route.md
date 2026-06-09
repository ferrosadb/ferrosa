---
type: todo
priority: P0
status: fixed
created: 2026-06-09
updated: 2026-06-09
affected-versions: ferrosa main @ 029544d0 (introduced by #91/#92)
fixed-by: fix/range-stream-chunk-reorder-closes-route
---

# Bug: multi-chunk streaming range read fails with `ChannelClosedBeforeDone` (chunk frames dispatched out of wire order)

## Symptom

Any streaming range read whose response carries **more than one chunk
frame** fails intermittently-to-deterministically with:

```
DbError(ServerError, "server error: cluster error: internal:
  streaming range read: ChannelClosedBeforeDone { delivered_done: 0, expected_done: 1 }")
```

Single-chunk responses (small tables, narrow partitions) always
succeed; the failure rate climbs with the chunk count, so a wide
partition or a large scan fails almost every time.

Reproduced by the downstream ferrosa-memory CI test
`derived_cache_count_streams_past_one_hundred_thousand_live_rows`,
which seeds **100 001 rows into a single partition** and then does a
paged `SELECT ... ALLOW FILTERING`. It failed 3/3 runs across two
unrelated PRs; ferrosa-memory `main` last passed on 2026-06-06, the day
before #91/#92 landed (2026-06-07).

## Why this is a Ferrosa bug

`ferrosa-net`'s connection read loop
(`rpc/server.rs::handle_connection`) reads frames off the socket in
order, but **spawns one tokio task per frame**
(`data_task_pool().spawn(handler)`). Those tasks run concurrently and
can complete in any order.

The coordinator's `StreamFrameRouter::accept_chunk_seq`
(`coordinator/stream_frame_router.rs`) enforces a **strict, contiguous
chunk `seq`**: the first time it observes `seq != next_chunk_seq` it
declares a gap/reorder and **closes the route**
(`router.unregister(request_id)`), dropping the per-request channel's
sender. The consumer's forwarder
(`coordinator/range_read_stream.rs::forward_remote_range_stream_inner`)
then sees the channel close with no `Done` and returns
`ChannelClosedBeforeDone { delivered_done: 0, expected_done: 1 }`.

So two chunk frames whose handler tasks are scheduled out of order
(`seq=1` wins the `seq_state` mutex before `seq=0`) trip the check and
kill an otherwise-healthy scan. `delivered_done` is 0 because the
forwarder counts only `Done` frames — any number of chunks may already
have been forwarded before the route closed.

## Why it regressed in #91/#92

The seq-strictness in `StreamFrameRouter` is older, but it was latent:
before #91 the producer (`stream_producer::stream_range_response`)
chunked by **partition count**, so a single wide partition was emitted
as exactly **one chunk** — no second frame, no reorder, no trip.

#91 ("bound full-scan memory — intra-partition row streaming") replaced
that with the row-fragmented producer
(`stream_request_handler::handle_stream_request`), which splits one wide
partition into many `~4096`-row chunks
(`DEFAULT_STREAM_CHUNK_ROW_CAP`). A single 100k-row partition now
produces ~25 chunk frames — and ~25 concurrently-dispatched handler
tasks — making the reorder near-certain. The memory fix exposed the
pre-existing ordering bug.

## Root cause, precisely

Frames of one streaming range-read response (`RangeReadStreamChunk` /
`RangeReadStreamHeartbeat` / `RangeReadStreamDone`) form a **single
ordered, per-`request_id` stream**, but the network layer dispatched
them with the same one-task-per-frame concurrency it uses for
independent request/response RPCs. Concurrency on an ordered stream is
pure harm: it reorders chunks (and would corrupt the coordinator's
token-ordered N-way merge input even if the seq check were relaxed).

## Fix

`ferrosa-net`: dispatch the three streaming-range-read **response**
frame types in wire order — run the handler inline in the read loop
instead of spawning a task per frame. The handler for these types is
non-blocking (`bincode` header decode + `StreamRouter::route` via
`try_send`), so inline dispatch cannot stall frame reading the way the
producer-side `RangeReadStreamRequest` storage read would. The
producer-side request and `RangeReadStreamCancel` keep spawning.

- `ferrosa-net/src/codec.rs` — `MsgType::is_ordered_stream_response()`.
- `ferrosa-net/src/rpc/server.rs` — inline-dispatch branch + regression
  test `stream_response_frames_dispatch_in_wire_order` (records the
  arrival order of three deliberately-skewed chunk handlers; asserts
  `[0,1,2]`. Fails as `[2,1,0]` on the old spawn path).

This preserves every existing guarantee: the seq check still fails loud
on genuine wire corruption/loss, and the token-ordered merge still
receives chunks in order.

## Alternatives considered

- **Reorder-tolerant buffer in `StreamFrameRouter`** (hold out-of-order
  chunks, release the contiguous prefix): more code on a hot path, and
  needs a bounded reorder window — too small spuriously fails huge
  scans, too large defeats #91's memory bound. Rejected.
- **Relax the seq check**: would let chunks reach the consumer out of
  token order and silently corrupt the N-way merge. Rejected (violates
  fail-loud).
