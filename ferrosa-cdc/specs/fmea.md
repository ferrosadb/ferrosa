---
crate: ferrosa-cdc
doc: fmea
last_updated: 2026-06-19
---

# ferrosa-cdc — FMEA / Known Issues

Failure modes are ranked by **RPN = Severity × Occurrence × Detection** (1–10
each; higher = worse). This crate is tiny but sits on the live write-path-to-
streaming-consumer boundary, so correctness-of-delivery severities are high.

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation / status |
|----|--------------|--------|---|---|---|-----|---------------------|
| CDC-1 | Slow subscriber's bounded queue overflows | Events dropped from that subscriber's queue | 7 | 6 | 2 | 84 | **By design, surfaced.** Drop is per-subscriber only; next `recv`/`try_recv` returns `Lagged { skipped }` so the consumer resyncs from a checkpoint. Covered by `overflow_emits_gap_signal_not_silent_drop`. Residual risk: consumer that ignores `Lagged` silently loses data — owned by the consumer, not this crate. |
| CDC-2 | Capacity chosen too small for real write rate | Frequent `Lagged`, consumers in constant resync, effective stream starvation | 6 | 5 | 4 | 120 | **Open tuning gap.** `main.rs` hard-codes `CdcBus::new(1024)`; there is no per-stream sizing, backpressure metric, or `Lagged`-rate counter. No way to observe how often the gap signal fires in production. |
| CDC-3 | Arrow Flight consumer not wired | The `CommittedToCluster`/`WrittenOnNode` streams have no Flight reader despite lib docs naming `ferrosa-flight` as a consumer | 4 | 8 | 2 | 64 | **Known gap, not a defect.** `ferrosa-flight` exists but does not subscribe. CQL `SUBSCRIBE` is the only live consumer today. Track Flight integration in roadmap. |
| CDC-4 | Producer publishes without honoring `has_subscribers`, or builds events when nobody listens | Wasted allocation/clone on the hot write path | 3 | 3 | 5 | 45 | Both producers guard with `has_subscribers` before constructing a `CdcEvent` (storage commit-log append; cluster `committed_cdc_event`/`CdcPublishingApplier`). A new producer that forgets the guard would regress this silently — convention, not enforced. |
| CDC-5 | At-least-once delivery yields a duplicate `CdcEvent` | Consumer applies a change twice | 5 | 3 | 4 | 60 | Delivery is at-least-once; `mutation_id` (`[u8; 16]`) is the documented dedup key. Dedup is the **consumer's** responsibility; this crate provides the key but does not deduplicate. |
| CDC-6 | Lib docs cite a missing spec (`specs/proposed/arrow-flight-endpoint/subscribe-cdc-architecture.md`) | Dangling doc reference; design rationale not discoverable | 2 | 7 | 1 | 14 | **Documentation gap.** The referenced file is absent in this checkout. `specs/overview.md` here now carries the design rationale; the lib-doc link should be repointed or the spec restored. |
| CDC-7 | Zero capacity bus constructed | A stream that can never deliver | 8 | 1 | 1 | 8 | `CdcBus::new` asserts `capacity > 0` and panics otherwise (fail-loud at construction). Covered by `zero_capacity_panics`. |
| CDC-8 | Ordering assumption violated (consumer assumes total order across streams) | Consumer reorders/correlates incorrectly | 5 | 3 | 5 | 75 | `timestamp` orders within a stream; `accord_ts` is present only for Accord-committed events. There is **no** cross-stream global order and no in-crate test asserting per-stream ordering under concurrency. Document the ordering contract; consumers must not assume cross-stream order. |

## Top risks to act on

1. **CDC-2 (RPN 120)** — capacity is an un-tuned, unobservable constant. Add a
   `Lagged`-event / dropped-count metric and make per-stream capacity
   configurable, so operators can see and fix starvation rather than guess.
2. **CDC-8 (RPN 75)** — the ordering contract is implicit. Pin it down (per-stream
   timestamp order; no cross-stream order) in docs and add a concurrency ordering
   test, so consumers don't build on an unguaranteed assumption.
3. **CDC-1 (RPN 84)** — the gap-signal mechanism is correct and tested, but its
   *value* depends on every consumer handling `Lagged`. Keep the "must resync on
   `Lagged`" contract loud in consumer docs.

## Detection assets

- In-crate: `overflow_emits_gap_signal_not_silent_drop`, `streams_are_isolated`,
  `fans_out_to_all_subscribers`, `has_subscribers_tracks_live_subscriptions_per_stream`,
  `publish_without_subscribers_is_not_an_error`, `async_recv_delivers_event`,
  `closed_bus_reports_closed`, `zero_capacity_panics`.
- End-to-end (consuming crates): `ferrosa-storage` commit-log append-publishes-CDC
  tests, `ferrosa-cluster` `committed_cdc_event_only_when_subscribed`, and
  `ferrosa-cql` `cdc_subscription_delivers_frame_on_write`.
