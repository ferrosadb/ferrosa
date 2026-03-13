# ADR-001: Write-Behind Async S3 Storage Model

> Date: 2026-03-11
> Status: Accepted

## Context

Ferrosa replaces Cassandra's local-disk storage model with S3-backed durability. Three approaches were considered: write-through (sync to both local + S3), write-behind (async S3 upload), and S3-primary (no local persistence).

## Decision

Write-behind async S3. Writes go to local commit log and memtable, ACK to client based on tunable CL, then SSTables are uploaded to S3 asynchronously.

## Rationale

- Minimal write latency impact — S3 is never on the synchronous write path
- Quorum writes across replicas mitigate the data loss window before S3 upload
- Commit log shipping to S3 every 5 seconds further narrows the window
- Compatible with any S3-compatible store (no conditional writes required)

## Implementation Status (2026-03-12)

The write-behind model is implemented in `ferrosa-storage`:

- **Commit log**: CAS-allocated segments, CRC32-checksummed entries, configurable sync strategies (Periodic/Batch/Group), checkpoint tracking. S3 shipping of segments is follow-on work.
- **S3 upload**: `UploadManager` with bounded `tokio::sync::mpsc` channel and exponential backoff retry, using `object_store` crate 0.11 (not `aws-sdk-s3` as originally considered). Wiring from flush/compaction output to upload is follow-on work.
- **Manifest**: Etag-based CAS via `object_store`'s `PutMode::Update` for consistent manifest updates. CAS retry loop is follow-on work.
- **Local cache**: LRU eviction with pinned entries and size tracking. S3 fetch-on-miss is follow-on work.
- **Backpressure**: Bounded channel provides basic backpressure. Priority shedding and disk threshold monitoring are follow-on work.

## Consequences

- Data loss window exists between local write and S3 upload on any single node
- Requires disciplined backpressure when upload queue grows
- Local disk space management becomes critical (upload queue, cache eviction)
- Five-layer durability mitigation strategy documented in the design spec

## Alternatives Rejected

- **Write-through**: S3 PUT latency on every write. Unacceptable for p99 targets.
- **S3-primary**: Truly stateless but requires S3 conditional writes (portability concern) and adds latency to every flush.
- **`aws-sdk-s3`**: Initially considered, but `object_store` crate (Apache Arrow project) chosen instead for multi-backend portability (S3, MinIO, GCS, Azure) and simpler API.
