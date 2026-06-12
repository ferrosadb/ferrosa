# Changelog

All notable changes to Ferrosa are documented in this file. Format loosely
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions
follow [SemVer](https://semver.org/spec/v2.0.0.html).

<!-- NIGHTLY releases are cut automatically on every merged PR; they are
     prereleases on the nightly channel. A STABLE release is promoted from a
     nightly build by a maintainer via the Promote Release workflow. -->

## [0.20.0] - 2026-06-12 <!-- NIGHTLY -->

### Added

- Multi-column conjunction filter predicates for partial indexes: a single
  partial index can now accelerate queries that combine multiple equality and
  range predicates in the same `WHERE` clause.

## [0.19.1] - 2026-06-12 <!-- NIGHTLY -->

### Fixed

- **Compaction eager-index rebuild used wrong IndexType.** The compaction path
  now resolves the real per-column `IndexType` instead of falling back to a
  generic placeholder, so rebuilt indexes match the original index definition.

## [0.19.0] - 2026-06-12 <!-- NIGHTLY -->

### Added

- WKB `GEOMETRY` marshaling (geospatial P2-b): geometry values are serialized
  to and from Well-Known Binary format for storage and wire transport.
- `ST_CONTAINS` and `ST_INTERSECTS` as stored geometric predicates (P2-c),
  evaluated directly against WKB-encoded geometry columns.

## [0.18.2] - 2026-06-12 <!-- NIGHTLY -->

### Changed

- Internal onboarding docs updated: `/op-init` skill references renamed to
  `/warp`. No functional changes to the server or CLI.

## [0.18.1] - 2026-06-12 <!-- NIGHTLY -->

### Fixed

- **Stale-view reads during compaction could lose data.** When a compaction
  task opened an SSTable reader view that had been superseded, the storage
  engine now retries the read against a fresh view instead of surfacing a
  not-found or returning stale data.

## [0.18.0] - 2026-06-11 <!-- NIGHTLY -->

### Added

- `system_schema.types` is now a persisted storage table backed by Ferrosa's
  own storage engine, completing the system_schema dogfooding arc for UDTs.
  UDT definitions survive restarts without a separate serialization path.

## [0.17.0] - 2026-06-11 <!-- NIGHTLY -->

### Added

- Filtered partial index range implication: the query planner now recognizes
  when a range predicate is implied by the partial index filter and skips
  redundant post-filtering.
- Native remote-builder predicate support: the remote index builder now
  receives and evaluates the partial index predicate, reducing round-trip
  data volume for remote builds.

## [0.16.0] - 2026-06-11 <!-- NIGHTLY -->

### Added

- `ST_WITHIN` polygon candidate pruning through the R-tree: geospatial queries
  using `ST_WITHIN` now use the R-tree to eliminate non-intersecting cells
  before running the exact polygon test, significantly reducing evaluation cost
  on large datasets.

## [0.15.0] - 2026-06-11 <!-- NIGHTLY -->

### Added

- `FullTextIndex` is now reported in `EXPLAIN` output, closing the 2i
  acceleration visibility gap for full-text queries.
- `FilteredIndex` accelerates queries end-to-end as a partial index: the query
  planner selects it, the execution engine routes through it, and results are
  post-filtered only when the partial predicate is not fully implied.

### Fixed

- Broken intra-doc links in `add_index_with_predicate` storage documentation.

## [0.14.1] - 2026-06-11 <!-- NIGHTLY -->

### Added

- Index subsystem major refactor: typed per-`IndexType` dispatch replaces the
  previous uniform dispatch path, enabling per-type query planning and
  optimization.
- `system_schema.indexes` dogfooded as a persisted storage table (previously
  in-memory only).
- Vector ANN index consulted during query planning: the HNSW/IVFFlat backends
  now participate in the query planner's index-selection path, including a
  fix for big-endian vector decoding in the planner.
- Geospatial queries via CQL surface: `GEO_NEAREST`, `WITHIN`, and `BBOX`
  predicates; `ST_WITHIN` with an R-tree-backed candidate index.

## [0.14.0] - 2026-06-10 <!-- STABLE -->

### Added

- Token-aware N-way paged range merge across replicas: the coordinator now
  merges range-read results from N replicas in a single paged pass, with
  token-aware routing.
- Automated release pipeline: releases are now cut automatically on every
  merged PR via Conventional Commit history (nightly channel), and promoted
  to stable by a maintainer workflow.

### Fixed

- **OOM P0: full-scan memory unbounded.** Intra-partition row streaming and
  coordinator paging are now bounded; wide-partition or broad range scans no
  longer accumulate unbounded memory.
- **SSTable writes not crash-atomic.** Writes now go through an fsync barrier
  and WAL ordering so a crash mid-write cannot produce a corrupt SSTable.
- **Accord commit not gated on fsync.** `handle_commit` now requires the fsync
  barrier before advancing committed state to disk, preventing phantom commits
  on disk failure.
- **Internode reconnect used connect-time IP.** The internode layer now
  reconnects to the peer's advertised broadcast hostname instead of the IP
  observed at connect time, fixing stale membership after podman IP churn.
- **ClusterInvite connect storms.** Cooldown added between connect attempts to
  unreachable peers during cluster formation.
- **Streaming range-read response frame ordering.** Response frames are now
  dispatched in wire order instead of completion order.

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

[0.20.0]: https://github.com/ferrosadb/ferrosa/compare/v0.19.1...v0.20.0
[0.19.1]: https://github.com/ferrosadb/ferrosa/compare/v0.19.0...v0.19.1
[0.19.0]: https://github.com/ferrosadb/ferrosa/compare/v0.18.2...v0.19.0
[0.18.2]: https://github.com/ferrosadb/ferrosa/compare/v0.18.1...v0.18.2
[0.18.1]: https://github.com/ferrosadb/ferrosa/compare/v0.18.0...v0.18.1
[0.18.0]: https://github.com/ferrosadb/ferrosa/compare/v0.17.0...v0.18.0
[0.17.0]: https://github.com/ferrosadb/ferrosa/compare/v0.16.0...v0.17.0
[0.16.0]: https://github.com/ferrosadb/ferrosa/compare/v0.15.0...v0.16.0
[0.15.0]: https://github.com/ferrosadb/ferrosa/compare/v0.14.1...v0.15.0
[0.14.1]: https://github.com/ferrosadb/ferrosa/compare/v0.14.0...v0.14.1
[0.14.0]: https://github.com/ferrosadb/ferrosa/compare/v0.13.0...v0.14.0
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
