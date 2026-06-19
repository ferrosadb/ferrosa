# FMEA — HVQ S3 Spill Tier

> Date: 2026-05-13
> Scope: Hierarchical vector quantization artifacts, S3-durable vector index
>        persistence, NVMe read-through cache, remote index builder, and
>        multi-fidelity ANN query planning.
> Method: RPN = Severity × Occurrence × Detection on a 1–10 scale. Action
>        required for RPN >= 50. Every row with RPN >= 50 has a detection,
>        exploitation, and recovery test.

## Executive Summary

The highest-risk failure modes are not codec math bugs; they are accidental
local-residency seams that make a node require more disk or RAM than it owns.
The HVQ implementation must make S3/object storage authoritative, publish
artifacts only after checksum validation, and prove queries still work when the
NVMe cache is smaller than the index.

Top actions:

1. Replace whole-sidecar `Vec<u8>` paths with page/range-readable index
   artifacts before enabling production HVQ.
1. Make remote-builder local fallback fail-loud for HVQ unless explicitly
   configured with enough scratch space.
1. Add MinIO integration tests that wipe local vector cache and prove query
   correctness through S3 range reads.
1. Add recall gates against an exact `f32` baseline before exposing Q1/Q2
   routing to user traffic.

## Scope

In scope:

- `.qvec` index artifact format and page table.
- Q1/Q2/Q4/Q8/F32 or residual tier storage.
- Object-store publish and manifest visibility.
- Bounded NVMe page cache and S3 range reads.
- Remote builder scratch-space and upload behavior.
- Query planner staged narrowing and final rerank.

Out of scope:

- Scalar BTree/hash/full-text sidecar failures, except where their APIs would
  be reused incorrectly by HVQ.
- Cassandra compatibility behavior unrelated to vector indexes.

## Scoring Criteria

| Score | Severity | Occurrence | Detection |
|-------|----------|------------|-----------|
| 1     | No user-visible effect | Rare | Always caught by type/unit tests |
| 2-3   | Minor latency or cost regression | Uncommon | Caught by focused integration tests |
| 4-6   | Query degraded or stale until retry | Plausible under load | Metrics or integration tests catch it |
| 7-8   | Incorrect ANN results or node instability | Likely at scale | Requires targeted tests/audits |
| 9-10  | Silent corruption, unavailable index, or data loss | Chronic if seam exists | Only production symptoms catch it |

## Top Risks

| Rank | ID | Failure Mode | Status | RPN |
|-----:|----|--------------|--------|----:|
| 1 | HVQ-02 | Builder exhausts temp disk/RAM on production-size SSTables | open | 280 |
| 2 | HVQ-03 | Whole-sidecar read path accidentally used for `.qvec` artifacts | open | 252 |
| 3 | HVQ-04 | Partial upload or stale manifest exposes corrupt `.qvec` | open | 240 |
| 4 | HVQ-08 | Remote builder writes artifacts that engine cannot discover/validate | open | 216 |
| 5 | HVQ-01 | Q1/Q2 routing drops true neighbors too early | open | 180 |

## Failure Modes

| ID | Function | Failure Mode | Effect | S | Cause | O | Detection | D | RPN | Required Tests |
|----|----------|--------------|--------|---|-------|---|-----------|---|-----|----------------|
| HVQ-01 | Query planning | Q1/Q2 routing eliminates true neighbors too early | Fast but wrong ANN results; recall below SLO | 9 | Low-bit quantization used for graph/list pruning without enough candidates or rerank | 4 | Recall benchmark against exact `f32` baseline | 5 | 180 | Detection: recall@k gate by dimension/metric. Exploitation: adversarial close-vector corpus. Recovery: widen candidate multiplier and exact rerank restores recall. |
| HVQ-02 | Index build | Builder exhausts temp disk/RAM on production-size SSTables | Build backlog, stale indexes, node instability | 8 | Current builder downloads full components and materializes partitions/indexes locally | 5 | Temp-disk watermark and build memory metrics | 7 | 280 | Detection: enforce `max_temp_bytes` before download. Exploitation: build with cache smaller than SSTable/index. Recovery: streaming/range build fails loud or completes bounded. |
| HVQ-03 | Query read path | Whole-sidecar `Vec<u8>` path used for `.qvec` | Node tries to read entire index from S3/NVMe; OOM or disk exhaustion | 9 | Reusing legacy `read_vector_sidecar` instead of page reader | 4 | Metrics: bytes/query and range-get count; API type separation | 7 | 252 | Detection: test asserts bounded bytes read. Exploitation: `.qvec` larger than local cache. Recovery: range reader succeeds with fixed page budget. |
| HVQ-04 | Publish | Partial upload or stale manifest exposes corrupt `.qvec` | Decode failures or wrong row references | 10 | Manifest visible before all pages/checksums are committed | 4 | Checksum/object-size validation before CAS publish | 6 | 240 | Detection: manifest validator rejects missing/short object. Exploitation: kill builder mid-upload. Recovery: old artifact remains live, new build is retried. |
| HVQ-05 | Cache coherency | Stale cached pages used after compaction | Queries return candidates from obsolete SSTables | 9 | Cache key omits generation/build/checksum or liveness check | 3 | Cache key/version tests and manifest liveness checks | 6 | 162 | Detection: stale-page lookup rejected. Exploitation: compact, reuse object range/page id. Recovery: old cache invalidated, new artifact queried. |
| HVQ-06 | S3 access | Range-read amplification / cold-page storm | High p99 latency and S3 cost | 7 | Poor page layout, no prefetch bounds, too many survivor pages | 5 | `s3_range_gets_per_query`, bytes/query, timeout metrics | 5 | 175 | Detection: cold-cache benchmark. Exploitation: scatter candidate pages. Recovery: page clustering, prefetch, query budget cutoff. |
| HVQ-07 | Format validation | Codec/dimension/metric mismatch | Wrong scores or decode panics | 9 | Missing magic/version/codec/dim/metric validation | 3 | Header validation and property tests | 3 | 81 | Detection: malformed header tests. Exploitation: decode Q4 as Q8 or cosine as L2. Recovery: fail loud with typed error. |
| HVQ-08 | Builder contract | Engine cannot discover or validate remote-builder `.qvec` | Index silently stale or ignored | 8 | Response only reports scalar sidecar path; no artifact manifest entry | 3 | Manifest reconciliation and builder response validation | 9 | 216 | Detection: missing artifact manifest alert. Exploitation: builder returns object key without checksum. Recovery: engine keeps old index/stale state and schedules rebuild. |
| HVQ-09 | Fallback policy | Remote builder failure falls back to local full build | Compute node exhausts disk despite tiered design | 8 | Existing `RemoteBackend` local fallback reused for HVQ | 4 | Config test for fail-loud HVQ fallback policy | 6 | 192 | Detection: all builders down with HVQ job. Exploitation: local scratch smaller than input. Recovery: job queued/stale, node remains healthy. |
| HVQ-10 | Row identity | Candidate positions collide across SSTable generations | Wrong row rerank or duplicate suppression | 8 | Legacy `RowPosition { offset }` lacks SSTable/object generation | 3 | Type-level `VectorRowRef` with generation/object key | 5 | 120 | Detection: two SSTables with identical offsets. Exploitation: merge candidates by offset only. Recovery: candidate identity includes generation/build id. |

## Required Test Matrix

| Layer | Test | Evidence |
|-------|------|----------|
| Unit | `.qvec` header/page-table validation rejects unknown versions, bad checksums, dimension mismatch, codec mismatch | Typed errors, no panic |
| Property | Q8/Q4/Q2/Q1 encode/decode error bounds across dimensions and metrics | Regression seeds committed |
| Integration | Build `.qvec`, upload to MinIO, publish manifest only after checksum validation | Object metadata and manifest CAS verified |
| Integration | Delete local NVMe vector cache and query through S3 range reads | Non-empty results, bounded bytes/query |
| Integration | Local cache smaller than total vector index still returns correct top-k after rerank | Recall gate passes |
| Chaos | Kill remote builder mid-upload | No visible partial artifact; old index remains live |
| Chaos | S3 short read, missing object, stale etag, and checksum mismatch | Fail-loud error and metric increment |
| Compaction | Replacement `.qvec` published before old artifact GC; stale cached pages rejected | Old generation no longer returned |
| Performance | Cold/warm p50/p95/p99, S3 range gets/query, bytes/query, cache hit ratio | Baseline stored for regression detection |

## Mitigation Requirements

1. Durable artifact visibility must be publish-after-validate:
   object upload -> checksum/size validation -> manifest CAS -> query planner visibility.
1. Reader APIs must distinguish local full-file sidecars from object-range HVQ
   artifacts at the type level.
1. Remote builder fallback policy must be explicit for HVQ:
   `fail_loud`, `queue`, or `local_if_capacity_reserved`; never silent local
   fallback by default.
1. Cache keys must include object key, generation/build id, byte range/page id,
   tier, checksum, and format version.
1. Query planner must expose hard budgets for page reads, bytes read, survivor
   count, and exact-rerank fanout.

## Related Specs

- [Hierarchical Vector Quantization](hierarchical-vector-quantization.md)
- [Project Plan — HVQ S3 Spill Tier](project-plan-hvq-s3-spill-tier.md)
- [Remote Index Build Backend](remote-index-build-backend.md)
- [Storage Engine](storage.md)
- [Testing Strategy](testing.md)
