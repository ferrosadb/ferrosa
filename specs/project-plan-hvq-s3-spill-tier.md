# Project Plan — HVQ S3 Spill Tier

> Last updated: 2026-05-13
> Status: Draft / ready for implementation decomposition
> Source: [Hierarchical Vector Quantization](hierarchical-vector-quantization.md)

## Executive Summary

This plan turns hierarchical vector quantization into an S3-durable, bounded
NVMe-cache implementation. The implementation order deliberately starts with
measurement and artifact contracts before ANN algorithms so no code path can
quietly require all vector tiers to fit on compute-node disk.

Critical path:

1. Define `.qvec` artifact metadata and page/range reader contracts.
1. Prove S3 read-through with a cache smaller than the total vector index.
1. Add Q8/Q4 and quantized IVFFlat first; defer quantized HNSW until recall and
   storage contracts are stable.
1. Extend remote builder to stream/spill and publish manifest entries, not just
   scalar sidecar paths.
1. Gate rollout on recall@k, bytes/query, range-get/query, and build scratch
   watermarks.

## Non-Negotiable Invariants

1. S3-compatible object storage is authoritative for `.qvec` artifacts.
1. NVMe is a bounded, evictable cache; correctness cannot depend on full local
   residency.
1. Build uses unquantized `f32` input for graph/list quality, then persists
   quantized tiers for search.
1. Low-bit tiers are routing maps, not proof of final nearest neighbors.
1. The planner narrows with quantized tiers, then optionally loads higher-bit or
   `f32` pages for rerank.
1. Missing, corrupt, stale, or dimension-mismatched artifacts fail loud.
1. Remote-builder failure must not silently fall back to local full-resident
   builds for HVQ.

## Current Seams to Remove

| Seam | Evidence | Required Direction |
|------|----------|--------------------|
| Monolithic HNSW JSON | `ferrosa-index/src/vector/hnsw.rs` stores graph, positions, and `Vec<Vec<f32>>` in one JSON blob | New page-addressable `.qvec` format |
| Monolithic IVFFlat JSON | `ferrosa-index/src/vector/ivfflat.rs` stores centroids/lists/full vectors in one JSON blob | Keep centroids hot; page list entries and vector codes |
| Whole-sidecar API | `FlushTarget::read_vector_sidecar` returns `Option<Vec<u8>>` | Replace HVQ path with object-range reader |
| File-backed vector read gap | `FileFlushTarget` writes vector sidecars but does not provide matching read-through path | Add vector artifact resolver keyed by manifest/object refs |
| Local startup assumption | `register_table()` discovers SSTables/sidecars via local directory scans | Make manifest/object refs authoritative |
| Builder scratch pressure | `ferrosa-index-builder` downloads whole components and uses local `LocalBackend` | Stream/range-read inputs and reserve scratch before download |
| Remote fallback | `RemoteBackend` can call local fallback when builders are down | Fail-loud/queue by default for HVQ |
| Row identity | `RowPosition { offset }` lacks generation/object identity | Introduce `VectorRowRef` with SSTable/artifact generation |

## Work Streams

### S0 — Baseline and Repro Cases

Goal: quantify current behavior and lock in regression tests before designing
around assumptions.

Deliverables:

- Current HNSW sidecar size and decode time by row count and dimension.
- Current IVFFlat sidecar size and build/query profile.
- Exact `f32` brute-force recall baseline for benchmark corpora.
- Metrics skeleton for ANN bytes/query and candidates/query.
- Repro test showing legacy whole-sidecar path fails or exceeds budget when the
  index is larger than the cache budget.

Acceptance criteria:

- `cargo test -p ferrosa-index vector_baseline` or equivalent focused tests pass.
- Benchmark output records sidecar bytes, decode ms, search ms, recall@k.
- Test harness can run with an artificial local-cache cap smaller than index
  artifact size.

### S1 — `.qvec` Artifact Container and Manifest Contract

Goal: define durable index artifacts before implementing new ANN logic.

Deliverables:

- Binary `.qvec` header: magic, format version, index method, dimensions,
  metric, build id, SSTable generation, row count, tier set, checksum scheme.
- Page table with object key, byte range, compressed length, uncompressed length,
  checksum, tier, and logical page id.
- `IndexArtifactManifestEntry` schema for storage/remote-builder handoff.
- Fail-loud validation errors for missing, short, stale, or corrupt pages.

Acceptance criteria:

- Unit tests validate good and malformed headers/page tables.
- Property tests cover page table bounds and checksum mismatch cases.
- No query code can accept a `.qvec` without validated artifact metadata.

### S2 — Cache-Backed Object Range Reader

Goal: make object storage readable without local full-file staging.

Deliverables:

- `ObjectRangeReader` or equivalent trait with local-hit/S3-range-miss behavior.
- NVMe page cache keys include object key, generation/build id, range/page id,
  tier, checksum, and format version.
- Cache admission and eviction policy with pinned-hot-metadata support that does
  not make full artifacts unevictable.
- Metrics: range gets/query, bytes/query, cache hit/miss/fill/evict bytes,
  checksum failures, stale-page rejections.

Acceptance criteria:

- MinIO integration test queries an artifact after deleting local cache.
- Test with cache cap < artifact size succeeds without full artifact download.
- S3 unavailable or corrupt page returns a typed error and increments metrics.

### S3 — Quantization Codecs

Goal: persist compact tiers with bounded decode error.

Deliverables:

- Q8 and Q4 first; Q2/Q1 behind feature flags until recall is proven.
- Per-block or per-list scale/zero-point metadata.
- Optional residual or PQ extension point without changing container identity.
- SIMD-friendly decode path where practical; scalar reference path for tests.

Acceptance criteria:

- Property tests cover encode/decode error bounds across dimensions and metrics.
- Golden fixtures validate backwards-compatible decode by format version.
- Codec mismatch and dimension mismatch fail loud.

### S4 — Quantized IVFFlat v1

Goal: ship the first production HVQ method on the simpler list-based ANN shape.

Deliverables:

- Build centroids/lists from `f32` input.
- Persist list pages as quantized codes and row refs.
- Query flow: centroid routing -> Q4/Q8 candidate pages -> optional `f32` or
  residual rerank -> top-k.
- Hard query budgets for pages read, bytes read, survivor count, and rerank
  fanout.

Acceptance criteria:

- Recall@k meets configured SLO against exact `f32` baseline.
- Cold-cache and warm-cache latency/bytes metrics are recorded.
- Query still succeeds with total `.qvec` bytes > local cache cap.

### S5 — Storage Engine Integration

Goal: integrate `.qvec` artifacts as first-class storage objects.

Deliverables:

- Extend index metadata/options for `quantized_ivf_flat`.
- Add vector artifact resolver that uses manifest/object refs rather than local
  sidecar directory scans.
- Ensure publish order: upload objects -> validate -> CAS manifest -> planner
  visibility.
- Preserve legacy HNSW/IVFFlat sidecar methods as compatible local/dev paths.

Acceptance criteria:

- `TableStore::ann_search` can route legacy sidecars and `.qvec` artifacts
  through distinct readers.
- Compaction replacement publishes new artifacts before old artifact GC.
- Stale cache pages cannot be used after manifest generation changes.

### S6 — Remote Builder and Spill-Safe Build Path

Goal: offload large vector index builds without moving the disk-pressure problem
onto the engine or builder.

Deliverables:

- Extend `POST /internal/index/build` request with vector method, tiers,
  dimensions, metric, max temp bytes, max build memory bytes, and build id.
- Extend response with artifact manifest entries, object keys, sizes, checksums,
  row count, page/tier summary, and format version.
- Stream/range-read SSTable input; do not download all components before work
  starts.
- Reserve scratch before fetch and fail loud if a build cannot fit configured
  limits.
- Disable silent local fallback for HVQ unless `local_if_capacity_reserved` is
  explicitly configured and proven.

Acceptance criteria:

- Builder crash mid-build leaves no visible manifest entry.
- Temp-disk limit exceeded returns a typed build failure and no partial publish.
- Engine validates returned object metadata before marking index current.
- `backend=off` pull mode discovers missing `.qvec` artifacts from manifest
  state, not fixed sidecar paths.

### S7 — Query Planner Multi-Fidelity Search

Goal: make quantized indexes usable as map tiles that refine on demand.

Deliverables:

- Planner uses low-bit tiers for broad routing only.
- Higher-bit pages are loaded for survivors according to query/index budget.
- Optional exact `f32` rerank is default for high-recall profiles.
- Query options expose recall/cost controls without leaking storage internals.

Acceptance criteria:

- Low-bit-only path is never the default for user-visible top-k unless recall
  gate allows it.
- Query plan explains selected tiers, budgets, and rerank policy.
- Metrics distinguish coarse-routing misses from final-rerank misses.

### S8 — Quantized HNSW Research Track

Goal: adapt HNSW after the storage contract is proven on IVFFlat.

Deliverables:

- Split adjacency pages from vector payload pages.
- Build graph edges from `f32` input, not low-bit approximations.
- Test graph quality degradation across Q2/Q4/Q8 traversal policies.
- Decide whether HNSW uses quantized traversal, quantized payload only, or stays
  full-precision at upper layers.

Acceptance criteria:

- HNSW recall degradation is measured against legacy HNSW and exact baseline.
- Graph traversal does not require all adjacency/vector pages locally resident.
- If quality is unacceptable, HNSW remains a non-goal and IVFFlat remains the
  production path.

## Parallel Implementation Batches

| Batch | Tasks | Dependencies | Suggested Agents |
|-------|-------|--------------|------------------|
| A | S0 baseline, exact recall harness, metric names | none | 2 agents |
| B | S1 `.qvec` format, S2 range reader prototype | A | 2 agents |
| C | S3 codecs, S4 quantized IVFFlat builder/query | B | 3 agents |
| D | S5 storage manifest integration, S6 remote builder API | B | 3 agents |
| E | S7 planner policies, MinIO integration tests, docs | C + D | 3 agents |
| F | S8 HNSW research, performance tuning | E | 1-2 agents |

## Verification Gates

| Gate | Command or Evidence | Required Result |
|------|---------------------|-----------------|
| Format | `cargo fmt --all -- --check` | pass |
| Rust lint | `cargo clippy --workspace --all-targets -- -D warnings` | pass |
| Unit | `cargo test -p ferrosa-index -p ferrosa-storage` | pass |
| Builder | `cargo test -p ferrosa-index-builder` | pass |
| Integration | MinIO-backed HVQ cache-miss/read-through suite | pass |
| Recall | exact baseline vs quantized top-k | meets configured SLO |
| Capacity | local cache cap smaller than total `.qvec` bytes | query succeeds |
| Failure | missing/corrupt/stale object | typed fail-loud error, no empty success |

## Open Design Decisions

| ID | Decision | Recommended Default | Owner Action |
|----|----------|---------------------|--------------|
| D1 | First production method | `quantized_ivf_flat`; defer HNSW | Confirm unless HNSW is must-ship first |
| D2 | Q1 exposure | Planner-internal only; do not expose until recall gate passes | Confirm |
| D3 | Exact rerank | Default on for high-recall profiles | Confirm target latency/recall tradeoff |
| D4 | Artifact shape | One `.qvec` manifest per SSTable/index, with multiple objects allowed per tier | Confirm object count/cost preference |
| D5 | Remote failure behavior | Queue/fail-loud for HVQ, no silent local fallback | Confirm operator preference |
| D6 | Cache budgets | Per-node hard byte cap plus per-query bytes/range cap | Define initial defaults |

## Documentation Updates Required

- [x] `hierarchical-vector-quantization.md` — S3 durability and NVMe-cache
  invariant.
- [x] `fmea-hvq-s3-spill-tier.md` — scoped failure analysis.
- [x] `project-plan-hvq-s3-spill-tier.md` — implementation plan.
- [x] `overview.md` — add HVQ to system architecture summary.
- [x] `components.md` — add `.qvec`, artifact resolver, range reader, remote
  builder contract.
- [x] `data-flow.md` — add query read-through flow and publish-after-validate.
- [x] `storage.md` — add vector artifact manifest/cache semantics.
- [x] `remote-index-build-backend.md` — add HVQ request/response and fallback
  semantics.
- [x] `testing.md` — add HVQ integration/performance test gates.

## Related Artifacts

- [Hierarchical Vector Quantization](hierarchical-vector-quantization.md)
- [FMEA — HVQ S3 Spill Tier](fmea-hvq-s3-spill-tier.md)
- [Remote Index Build Backend](remote-index-build-backend.md)
- [Storage Engine](storage.md)
- [Testing Strategy](testing.md)
