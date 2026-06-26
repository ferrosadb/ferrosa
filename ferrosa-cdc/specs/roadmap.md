---
crate: ferrosa-cdc
doc: roadmap
last_updated: 2026-06-19
---

# ferrosa-cdc — Roadmap

Sourced from the FMEA gaps ([fmea.md](fmea.md)), the dependency/usage review, and
the wiring audit of the consuming crates.

## Now (highest value)

- **Observability for the gap signal (FMEA CDC-2).** Add a dropped-event /
  `Lagged` counter per stream so operators can see when subscribers fall behind.
  Today overflow is correct and surfaced to the consumer, but invisible to
  monitoring — there is no way to know the `1024` capacity is too small until a
  client complains.
- **Make per-stream capacity configurable.** Replace the hard-coded
  `CdcBus::new(1024)` in `ferrosa/src/main.rs` with a config-driven value, and
  allow `WrittenOnNode` and `CommittedToCluster` to be sized independently.

## Next

- **Pin and test the ordering contract (FMEA CDC-8).** Document that events are
  ordered by `timestamp` *within* a stream and that there is no cross-stream
  global order; add a concurrency test asserting per-stream ordering so consumers
  don't build on an unguaranteed assumption.
- **Wire the Arrow Flight consumer (FMEA CDC-3).** `ferrosa-flight` is named as a
  consumer in the lib docs but does not subscribe to the bus. Add the Flight
  subscription path (subscribe → encode `CdcEvent` to Arrow record batches →
  stream), mirroring `ferrosa-cql::spawn_cdc_subscription`.
- **Repair the dangling spec reference (FMEA CDC-6).** Restore or repoint
  `specs/proposed/arrow-flight-endpoint/subscribe-cdc-architecture.md` referenced
  from `src/lib.rs`; the design rationale now lives in
  [specs/overview.md](overview.md).

## Later

- **Resync/checkpoint helper.** After a `Lagged`, consumers must resync from a
  checkpoint. A shared helper (or a documented protocol) for turning a `Lagged`
  into a bounded backfill would keep the gap-handling logic out of every consumer.
- **Consumer-side dedup helper.** Delivery is at-least-once with `mutation_id` as
  the dedup key; a small shared dedup window/utility would prevent each consumer
  reimplementing it.
- **Property-test fan-out + overflow** across many subscribers and stream mixes as
  a regression net independent of the consuming crates.

## Non-goals

- Event production (commit-log append, Accord/Raft apply) — owned by
  `ferrosa-storage` / `ferrosa-cluster`.
- Protocol framing / transport (CQL RESULT frames, Arrow Flight gRPC) — owned by
  the consumers (`ferrosa-cql`, `ferrosa-flight`).
- Durable/replayable CDC log — this is an in-memory, at-least-once push bus, not a
  persisted change log.
