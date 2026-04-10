# Remote Index Build Backend

> Created: 2026-04-09
> Priority: P2
> Source: ferrosa-dbaas managed-vector-spec.md (section 11.1A, 16A)
> Type: feature

## Summary

Implement a `RemoteBackend` for the existing `IndexBuildBackend` trait that offloads index building from the main Ferrosa read/write engine to an external sidecar process. The sidecar fetches SSTable components from S3, builds sidecar index files, and writes them back — freeing the engine from index-build CPU pressure.

## Motivation

The ferrosa-dbaas managed vector service introduces a vector gateway sidecar process (Firecracker/Docker/Podman) that handles embedding, search, and index building independently of the control plane. This sidecar already needs to build vector indexes (HNSW, IVFFlat) for ingested records.

Since all Ferrosa index types (BTree, Hash, Composite, Phonetic, FullText, Vector) use the same sidecar file format and the same `IndexBuildBackend` trait, the vector gateway sidecar can offload **any** index build — not just vector indexes. This is especially valuable for:

- Full-text/BM25 inverted index builds (CPU-intensive)
- Bulk import scenarios where many SSTables need indexing simultaneously
- Re-index operations after schema changes
- Pro/Enterprise tier customers who want to scale index-build capacity independently

## Existing Architecture

The index build pipeline is already designed for this (`ferrosa-storage/src/index/`):

```
IndexBuildScheduler
  ├── receives IndexBuildJob (sstable_id, index_name, index_type, priority)
  ├── dispatches to IndexBuildBackend::build()
  └── currently uses LocalBackend (in-process)

IndexBuildBackend trait
  fn build(&self, job: &IndexBuildJob) -> Result<IndexBuildResult, String>

LocalBackend
  ├── reads SSTable from local disk
  ├── iterates rows, extracts indexed column values
  └── produces sidecar entries: Vec<(IndexKey, RowPosition)>
```

Key files:
- `ferrosa-storage/src/index/scheduler.rs` — `IndexBuildScheduler`, `with_backend()` (line ~257)
- `ferrosa-storage/src/index/sidecar.rs` — sidecar file format (FXSI magic, sorted entries)
- `ferrosa-storage/src/index/tracker.rs` — `IndexStatus` (Current, Building, Stale, Failed)
- `ferrosa-storage/src/memtable/eager_index.rs` — flush/compaction hooks that enqueue jobs

## Proposed Changes

### 1. `RemoteBackend` implementation

New file: `ferrosa-storage/src/index/remote_backend.rs`

Implements `IndexBuildBackend` by sending jobs to a remote sidecar process:

```text
RemoteBackend {
  sidecar_endpoints: Vec<Url>     // one or more sidecar instances
  s3_config: S3Config             // where SSTables live
  timeout: Duration
  retry_policy: RetryPolicy
}

fn build(&self, job: &IndexBuildJob) -> Result<IndexBuildResult> {
  1. Resolve S3 path for the SSTable components
  2. POST job to sidecar: { sstable_id, index_name, index_type, s3_path, column_position }
  3. Sidecar fetches SSTable from S3, runs LocalBackend::build() logic
  4. Sidecar writes sidecar file back to S3 alongside the SSTable
  5. Return IndexBuildResult with sidecar file path
}
```

### 2. Sidecar HTTP API

The vector gateway sidecar (in ferrosa-dbaas) exposes an internal endpoint for index build requests:

```text
POST /internal/index/build
{
  "sstable_id": "...",
  "index_name": "...",
  "index_type": "btree|hash|fulltext|hnsw|ivfflat|...",
  "s3_prefix": "s3://ferrosa/tenant-a/...",
  "table": ["keyspace", "table"],
  "column_position": 2,
  "priority": "high|normal|initial"
}

Response:
{
  "status": "completed|failed",
  "sidecar_s3_path": "s3://ferrosa/tenant-a/.../-Index-name.db",
  "entries_built": 42000,
  "elapsed_ms": 1200
}
```

### 3. Scheduler configuration

Add a config option to `IndexBuildScheduler` to select backend:

```text
[index_build]
backend = "local"           # default: in-process
# backend = "remote"        # offload to sidecar
# sidecar_endpoints = ["http://sidecar-1:8090", "http://sidecar-2:8090"]
```

### 4. Staleness tracking integration

`IndexStateTracker` already tracks `IndexStatus::Stale { lag, pending_count }`. When using `RemoteBackend`:
- Jobs in-flight to the sidecar show as `Building`
- Network failures to the sidecar result in `Failed { error, retry_at }`
- Fallback to `LocalBackend` if all sidecar endpoints are unhealthy (circuit breaker)

## Acceptance Criteria

1. `RemoteBackend` implements `IndexBuildBackend` and passes the same test suite as `LocalBackend`
1. Sidecar can build all index types: BTree, Hash, Composite, Phonetic, FullText, HNSW, IVFFlat
1. Sidecar reads SSTable components from S3 and writes sidecar files back to S3
1. `IndexBuildScheduler` can be configured to use `RemoteBackend` via config
1. Fallback to `LocalBackend` when sidecar is unreachable
1. `IndexStateTracker` correctly reports status for remote builds
1. No regression in local-only index build performance

## Dependencies

- Ferrosa S3 storage layer (existing)
- `IndexBuildBackend` trait (existing, `ferrosa-storage/src/index/scheduler.rs`)
- Sidecar process implementation (ferrosa-dbaas, `dbaas-vector-gateway` crate)

## Related

- `ferrosa-dbaas/specs/managed-vector-spec.md` — section 11.1A (sidecar dashboard), section 16A (crate structure)
- `ferrosa/specs/secondary-index-pipeline.md` — current index pipeline architecture
- `ferrosa/specs/fulltext-index-architecture.md` — full-text index build details
