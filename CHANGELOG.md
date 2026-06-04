# Changelog

All notable changes to Ferrosa are documented in this file. Format loosely
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions
follow [SemVer](https://semver.org/spec/v2.0.0.html).

## [0.13.0] - 2026-06-04

### Added

- Engine-wide capped SSTable reader pool with descriptor-backed table views, so
  flushed SSTable metadata stays lightweight and open reader residency is bounded
  by `FERROSA_SSTABLE_READER_CACHE_CAP`.
- Byte-bounded anti-entropy repair fetches. Repair fetch requests now cap both
  partition count and bytes so a single wide partition or broad divergent range
  cannot exceed the configured response budget.
- Compaction input reader routing through the shared reader pool plus a global
  `FERROSA_MAX_CONCURRENT_COMPACTIONS` gate.
- Storage metrics for pooled compaction input opens and max concurrently running
  compactions.
- Shared repair/storage fuzz generator scaffolding for the repair hardening
  test harness.

### Changed

- Startup SSTable validation opens, checks, and drops readers one at a time,
  then publishes descriptors instead of keeping all readers resident.
- Large range and repair digest walks stream partition data one source step at a
  time instead of materializing intermediate tiers.
- Length-prefixed SSTable value decoding now enforces allocation bounds before
  reserving buffers.
- Ferrosa Memory-facing repair and compaction paths are documented as part of
  the bounded-storage memory model.

### Known Limitations

- Strict repair reader fan-in under full token overlap remains an explicit
  acceptance gate for the next repair-hardening pass.
- Automated/self-healing repair remains proposed; `ferrosa-ctl repair` and the
  HTTP repair endpoint are still operator-triggered.

## [0.12.0] - 2026-05-22

### Added

- RRD-style time-series consolidation for built-in streaming rollups:
  DDL-registered consolidators, bounded ring buffers, materialization queues,
  background worker draining, derived target-table writes, and cascade
  propagation.
- Time-series materialization observability via
  `system_observability.materialization_queues`,
  `system_observability.materialization_status`, and
  `system_observability.rrd_runtime_settings`.
- Runnable `examples/timeseries-rrd` sensor demo with built-in
  min/max/avg/stddev rollups and a static smoke contract.

### Changed

- Derived rollup rows now dispatch storage observers so downstream RRD tiers can
  enqueue and materialize cascade windows.
- Time-series write observers use the row clustering timestamp as the event
  timestamp, so materialized rollup rows cascade by window start rather than by
  worker write time.

### Known Limitations

- WASM aggregate loading syntax is documented, but live RRD materialization
  rejects `wasm:` aggregate functions until the streaming aggregate ABI lands.
- Late-data recomputation currently covers values still present in the
  in-memory ring; disk-backed keyed window streaming remains tracked as
  follow-up work.

## [0.11.0] - 2026-05-22

### Fixed

- **macOS port 7000 conflict.** Default internode bind address changed from
  `:7000` to `:17000` to avoid colliding with macOS ControlCenter, which
  listens on 7000 in macOS 13+. Startup now emits a diagnostic error message
  if a bind on 7000 is attempted, identifying the conflict and suggesting
  `:17000`.
- **TOML config not honored for core settings.** `ferrosa.toml` values for
  internode bind, graph, storage, and auth were silently ignored when the
  corresponding `FERROSA_*` environment variables were not set. All four
  setting groups now read from TOML when env vars are absent.
- **Corrupt or empty `host_id` on startup.** Stale, corrupt, or zero-byte
  single-node `host_id` files are now detected at startup and regenerated
  automatically with an actionable log message, rather than crashing or
  silently diverging into split-brain state.

### Changed

- Default internode bind port: **7000 → 17000**. Update any firewall rules,
  launchd plists, or `[internode] bind_addr` TOML overrides that reference the
  old port. The `FERROSA_INTERNODE_BIND` environment variable and TOML key
  still accept arbitrary values.

### Docs

- Shipped config templates and source defaults updated to `:17000` internode
  port and current auth role names (`ferrosa_admin`, `ferrosa_user`).

[0.13.0]: https://github.com/ferrosadb/ferrosa/compare/v0.12.2...v0.13.0
[0.12.0]: https://github.com/ferrosadb/ferrosa/compare/v0.11.0...v0.12.0
[0.11.0]: https://github.com/ferrosadb/ferrosa/compare/v0.10.0...v0.11.0

## [0.10.0] - 2026-05-16

### Added

- Cap'n Proto internode protocol scaffold: typed envelope, schema-version
  negotiation, framing gate, cluster recovery adapters, and conformance
  smokes. Replaces the hand-written 44-byte frame header and ad-hoc
  `Message` enum incrementally. See `specs/decisions/019-capnproto-internode-protocol.md`.
- Bounded bootstrap stream Cap'n Proto contracts.
- Bootstrap phase runner skeleton; planner now bounds peer streaming.
- Reconnect cluster invite planning and reservation.

### Fixed

- **Compaction startup memory pressure.** Compaction fan-in is now bounded
  by input bytes so startup work stays memory- and resource-bounded.
- **Bound compaction fan-in by input bytes.**
- **SSTable upload streaming.** Keep streaming from disk instead of
  materializing full multipart payloads.
- **Recovered topology token assignment** after topology refresh.
- **File-descriptor pressure on startup.** `FileReadAt` is now
  path-backed/lazy-open instead of pinning Data.db/Partitions.db/Rows.db
  descriptors for every idle SSTable — eliminates the `Too many open
  files` failure on broad test sweeps.
- **Graph adjacency registration.** Skipped for scalar-only returns.
- **Sled handle release** before raft reset returns.

### Changed

- Refactor: isolate compaction finalization.
- Refactor: isolate pending-upload replay backpressure.
- Refactor: extract peer event / peer recovery event planners.
- Refactor: bound bootstrap streaming planner.

### CI / Build

- Install `capnproto` on every GitHub Actions job that compiles the
  workspace (clippy, test, musl, jepsen-smoke, integration, docs, and
  every job in `release.yml`). `dtolnay/rust-toolchain` only installs
  Rust; `ferrosa-net/build.rs` needs the `capnp` executable.
- Add `Cross.toml` so the aarch64-musl cross build installs `capnproto`
  inside the cross container.
- `.gitignore`: add Python build artifacts (`__pycache__/`, `*.pyc`,
  `.pytest_cache/`).
- Container Dockerfile installs `capnproto`.

### Docs

- ADR-019: Cap'n Proto internode protocol envelope.
- Bug specs: `system_schema.keyspaces` PREPARE rejects `?` bind markers;
  `system_schema.views` row shape breaks scylla-rust-driver auto
  schema-agreement.
- HVQ vector index spec made S3-durable; S3 spill tier specified.
- Plans: cluster recovery and storage OOM seam refactor; storage seam
  follow-through.

[0.10.0]: https://github.com/ferrosadb/ferrosa/compare/v0.9.0...v0.10.0

## [0.9.0] - 2026-05-02

Release-prep cut after correctness sprints (raft PreVote/CheckQuorum +
multi-DC scaffolding + learners + sim), CQL compatibility work
(NoSQLBench gap closure, `system_schema.views` SELECT projection,
ALTER TABLE Direct path), graph adjacency replication via Raft, and
performance work on commit-log dirty tracking and network lane
multiplexing.

For the full commit list see
<https://github.com/ferrosadb/ferrosa/compare/v0.8.8+8...v0.9.0>.

[0.9.0]: https://github.com/ferrosadb/ferrosa/releases/tag/v0.9.0
