---
crate: ferrosa-index-builder
doc: roadmap
last_updated: 2026-06-19
---

# ferrosa-index-builder — Roadmap

Sourced from the code, the FMEA gaps ([fmea.md](fmea.md)), and the reference
spec `specs/reference/remote-index-build-backend.md`.

## Now (highest value)

- **Fix pull-mode index typing** (FMEA IB-2). Pull mode hard-codes
  `index_type = "btree"` for every manifest index, which silently produces wrong
  sidecars for hash/vector/fulltext/filtered/geo indexes. Either (a) extend the
  engine's `GET /internal/manifest` `ManifestEntry` to carry the real index type
  + `filter_predicate`, or (b) explicitly restrict pull mode to btree-only
  tables and reject the rest loudly.
- **Add a build-pipeline integration test** (FMEA IB-1). Use
  `object_store::memory::InMemory`, seed the six SSTable components, run
  `WorkerPool::execute`, and assert the uploaded sidecar bytes (and the
  filtered-vs-unfiltered entry count). Today the green suite never touches the
  download→build→upload path.

## Next

- **Authn on `/internal/index/build`** (FMEA IB-7). The route is "internal" by
  naming only; add a shared-secret/bearer token or mTLS so it is not an open S3
  read/write trigger for anyone on the network.
- **Temp-disk robustness** (FMEA IB-4, IB-5). Reconcile `temp_bytes_used`
  against actual disk, reap the `ferrosa-index-builder/` temp dir on startup, and
  ensure a `spawn_blocking` panic releases both the temp dir and the byte count.
- **Observability beyond `/health`** (FMEA IB-8). Emit Prometheus counters
  (builds completed/failed by index type, bytes downloaded, build latency) and a
  metric for pull-loop fetch failures with backoff + a cap.

## Later

- **Quantized `.qvec` direct upload** (FMEA IB-6). Implement the streamed
  build/upload with object-size + sha256 validation and return a populated
  `artifact_manifest_entry`, replacing the current fail-closed stub.
- **Concurrency-tunable downloads.** Components are fetched sequentially per job;
  parallelizing the six-component fetch (bounded) would cut tail latency for
  large SSTables.
- **Graceful shutdown / drain.** On SIGTERM, stop accepting new builds, let
  in-flight permits drain, and exit — so a rolling restart never abandons a
  half-uploaded sidecar.

## Non-goals

- Owning the index encoding. This service wraps the engine's `LocalBackend`
  on purpose; a second encoder would risk remote/in-process divergence.
- Scheduling/priority policy. Which SSTables to index and when is the engine's
  job (`ferrosa-storage` scheduler); this service just executes the jobs it is
  handed.
