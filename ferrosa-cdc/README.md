# ferrosa-cdc

> The change-data-capture event types and bounded, multi-subscriber bus that
> power Ferrosa's `SUBSCRIBE` change streams — shared by every producer (storage,
> cluster) and consumer (CQL push, planned Arrow Flight) without a dependency cycle.

## What this crate is

A small, foundation-layer crate that owns the **`CdcEvent`** payload type and the
**`CdcBus`** that carries those events from write-path producers to streaming
consumers. `SUBSCRIBE` was deliberately scoped as two *optional, event-driven*
change streams, not a polled `SELECT` snapshot, so the engine needs one shared
push channel that sits *below* `ferrosa-storage` in the dependency graph. This
crate is that channel.

It depends on only `ferrosa-common` and `ferrosa-sstable`. That constraint is
load-bearing: `ferrosa-storage` (a producer) depends on `ferrosa-cdc`, so
`ferrosa-cdc` must **not** depend back on `ferrosa-storage`. Consequently
`CdcEvent` reuses `ferrosa_sstable::types::Row` and never embeds
`ferrosa_storage::Mutation`.

## The two streams

A subscriber selects exactly one stream; to follow both, open two subscriptions.

- **`CdcStream::WrittenOnNode`** — every mutation durably written to *this* node's
  commit log, ordered by mutation timestamp. Published by `ferrosa-storage`'s
  commit-log append path.
- **`CdcStream::CommittedToCluster`** — mutations the cluster has agreed/acked
  (an Accord commit, or a regular-CL quorum ack on the coordinator). Published by
  `ferrosa-cluster`.

## What's implemented

- **`CdcEvent`** — `stream`, `keyspace`, `table`, decorated `key`, mutated `rows`
  (verbatim `Row`s), `timestamp` (microseconds, the ordering key), optional
  `accord_ts` (present only for Accord-committed events), and `mutation_id`
  (`[u8; 16]`, the dedup key for at-least-once delivery).
- **`CdcBus`** — one `tokio::sync::broadcast` channel per stream behind a single
  bus. `new(capacity)` (panics on `capacity == 0`), `publish` (returns the live
  subscriber count; zero subscribers is a normal non-error case), `has_subscribers`
  (hot-path guard so producers skip building an event when nobody listens), and
  `subscribe`.
- **`CdcSubscription`** — a consumer handle with its own bounded queue. `recv`
  (async) / `try_recv` (non-blocking) return `Result<CdcEvent, CdcRecvError>`.
- **`CdcRecvError`** — `Lagged { skipped }` (the **gap signal**: the queue
  overflowed and `skipped` events were dropped — never silent), `Empty`
  (`try_recv` only), `Closed` (bus dropped).

## Bounded-queue / gap semantics

The bus is bounded **per subscriber**. A slow subscriber can never block a
producer (the write path): when its queue overflows, the oldest events are
dropped from *its* queue only, and the next `recv`/`try_recv` returns
`CdcRecvError::Lagged { skipped: N }`. This is an explicit gap signal — the
consumer must resync from a checkpoint rather than assume continuity. Delivery
then resumes from the oldest retained event. (FMEA F16/F18: a dropped event is
never silently skipped.)

## Wiring status (honest)

- **Producers — wired.** `ferrosa-storage` publishes `WrittenOnNode` from
  commit-log append (`commitlog/mod.rs`), guarded by `has_subscribers`.
  `ferrosa-cluster` publishes `CommittedToCluster` for regular-CL writes
  (`write_path.rs::committed_cdc_event`) and for Accord-committed transactions
  (`accord/apply.rs::CdcPublishingApplier`, installed in
  `accord/state_machine.rs`).
- **Shared bus — wired.** `ferrosa/src/main.rs` constructs one
  `CdcBus::new(1024)` and attaches it to the engine via `storage.set_cdc_bus(...)`,
  so both producer paths and the CQL consumer share the same bus instance.
- **CQL consumer — wired.** `ferrosa-cql::subscribe::spawn_cdc_subscription`
  subscribes to the selected stream(s), converts each `CdcEvent` to a CQL RESULT
  frame, and surfaces `Lagged` to the client.
- **Arrow Flight consumer — NOT wired.** The `ferrosa-flight` crate exists but
  does not yet subscribe to the bus. The lib docs name `ferrosa-flight` as an
  intended consumer; that integration is still pending.

## Public API

| Area | Item |
|------|------|
| Event | `CdcEvent`, `CdcStream::{WrittenOnNode, CommittedToCluster}` |
| Bus | `CdcBus::{new, publish, has_subscribers, subscribe}` |
| Subscription | `CdcSubscription::{stream, recv, try_recv}` |
| Errors | `CdcRecvError::{Lagged, Empty, Closed}` |

## Dependencies

**Calls** (ferrosa crates this depends on):

- **`ferrosa-common`** — `DecoratedKey`, `accord::Timestamp` (the `accord_ts`
  field type).
- **`ferrosa-sstable`** — `types::Row` (the mutated-row payload, reused verbatim).

External: `tokio` (the `sync` feature, for `broadcast`). **Never** depends on
`ferrosa-storage` (would create a cycle).

**Called by** (crates that depend on this):

- **`ferrosa`** — owns the shared `CdcBus` and injects it at startup.
- **`ferrosa-cluster`** — publishes `CommittedToCluster` events.
- **`ferrosa-cql`** — subscribes and pushes events to `SUBSCRIBE` clients.
- **`ferrosa-storage`** — publishes `WrittenOnNode` events from the commit log.

## Tests

8 in-crate unit tests in `src/lib.rs`: stream isolation, fan-out to multiple
subscribers, overflow → `Lagged` gap signal (not a silent drop), publish with no
subscribers, per-stream `has_subscribers` tracking, async `recv`, closed-bus
reporting, and the zero-capacity panic. End-to-end producer/consumer wiring is
exercised in the consuming crates (`ferrosa-storage` commit-log tests,
`ferrosa-cluster` write-path tests, `ferrosa-cql` subscribe tests).

## Specs

- [Architecture overview](specs/overview.md) — types, streams, bus, data flow, invariants
- [FMEA / known issues](specs/fmea.md) — failure modes + gaps
- [Roadmap](specs/roadmap.md) — Now / Next / Later
