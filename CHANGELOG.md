# Changelog

All notable changes to Ferrosa are documented in this file. Format loosely
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions
follow [SemVer](https://semver.org/spec/v2.0.0.html).

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
