# ferrosa-index-builder

> A **standalone HTTP service binary** that offloads secondary-index
> construction from the Ferrosa engine. The engine talks to it over HTTP when
> `FERROSA_INDEX_BACKEND=remote`; it is **not** a library other crates link.

## What this crate is

`ferrosa-index-builder` is a `[[bin]]` service, not a reusable library. It reads
SSTable components from S3, runs the engine's own
[`ferrosa_storage::index::LocalBackend`] to build sidecar index files, and
writes the sidecars back to S3 — keeping the CPU/IO of index construction off
the engine nodes. The engine's `RemoteBackend` (in `ferrosa-storage`) is the
client: it POSTs build requests to this service and consumes the JSON response.

Because the coupling is **over HTTP**, no ferrosa crate has a cargo path-dep on
this one. The wire contract (the `BuildRequest` / `BuildResponse` JSON shapes
and the `/internal/index/build` route) *is* the interface — see
[Public HTTP API](#public-http-api).

## What's implemented

- **Push mode** (default, `--mode push`): an `axum` HTTP server exposing
  `POST /internal/index/build` and `GET /health`. The engine's `RemoteBackend`
  drives builds synchronously over this route.
- **Pull mode** (`--mode pull`): a manifest watcher for `backend=off`
  deployments. Polls the engine's `GET /internal/manifest` endpoint on an
  interval, diffs declared indexes against existing sidecars in S3, and enqueues
  builds for the missing ones. Pull mode builds **`btree` indexes only** with no
  partial predicate.
- **Bounded worker pool** (`worker::WorkerPool`): a `tokio::sync::Semaphore`
  caps concurrent builds (`--workers`, default 4). Each job downloads the six
  SSTable components from S3 to a per-job temp dir, runs `LocalBackend::build()`
  on a blocking thread, writes/uploads each sidecar, then cleans up. A
  `max_temp_bytes` budget (default 10 GiB) guards local disk.
- **Filtered (partial) index support**: a `filtered` build carries the
  fully-encoded `FilterPredicate` on the wire and threads it into the
  `IndexBuildJob`, so the remote sidecar contains exactly the matching rows —
  never an unfiltered sidecar.
- **Clustering-column index support**: build requests may carry
  `clustering_source` (`component`, `total`) for indexes on CLUSTERING columns;
  the worker threads it into `IndexBuildJob` so `LocalBackend` extracts the
  value from the composite clustering key instead of looking for a row cell.
- **Fail-closed for quantized vectors**: a `direct_upload` + `hvq_qvec` request
  returns `status: "failed"` with an explicit "not implemented" message rather
  than silently producing an unpublishable artifact.
- **S3 client construction**: `AmazonS3Builder` with `ETagMatch` conditional
  put, env-var configuration, optional static credentials (falls back to
  instance profile), and `--s3-allow-http` for local MinIO.

## How it works

Four modules under a thin `lib.rs` + `main.rs`:

| Module | Responsibility |
|--------|----------------|
| `main.rs` | `clap` CLI, tracing init, S3 client build, dispatch to push/pull |
| `server` (`src/server.rs`) | `axum` router; `/internal/index/build` + `/health` handlers |
| `worker` (`src/worker.rs`) | `WorkerPool`, `BuildRequest`/`BuildResponse`, download→build→upload→cleanup, type/priority parsing |
| `pull` (`src/pull.rs`) | manifest poll loop, sidecar-existence diff, build enqueue |

**Push flow**: engine `RemoteBackend` → `POST /internal/index/build` →
`handle_build` → `WorkerPool::execute` (acquire permit → download components →
`spawn_blocking(LocalBackend::build)` → `SidecarWriter::write` → S3 `put` →
cleanup) → JSON `BuildResponse`.

**Pull flow**: timer → `fetch_manifest` → for each declared index, `head` the
sidecar key; on `NotFound`, spawn a `WorkerPool::execute` for it.

## Public HTTP API

This service has **no Rust public API surface that other crates link** — its
contract is HTTP + JSON.

| Route | Method | Body / Response |
|-------|--------|-----------------|
| `/internal/index/build` | POST | `BuildRequest` JSON in; `BuildResponse` JSON out, **always HTTP 200** for app-level outcomes |
| `/health` | GET | `{status, workers_active, jobs_completed, jobs_failed}` |

`BuildResponse.status` is `"completed"` or `"failed"`; the engine treats HTTP
5xx as a transport error and `status: "failed"` as an application error. A
`completed` vector/quantized build carries an `artifact_manifest_entry`; sidecar
builds carry `sidecar_s3_path` + `entries_built`.

## Dependencies

**Calls** (ferrosa crates this depends on):

- **`ferrosa-common`** — shared types.
- **`ferrosa-index`** — `IndexType`, `FilterPredicate`/`FilterOp` (the index
  taxonomy and the partial-index predicate carried on the wire).
- **`ferrosa-sstable`** — SSTable component shapes consumed during a build.
- **`ferrosa-storage`** — `LocalBackend`, `IndexBuildJob`, `BuildPriority`,
  `ClusteringComponentRef`, `SidecarWriter`, `ArtifactManifestEntry`,
  `upload::hex_prefix_for` (the actual index-build engine this service wraps).

External: `axum`, `tokio`, `object_store` (aws), `reqwest`, `clap`, `serde`,
`serde_json`, `bytes`, `tracing`.

**Called by** (cargo path-deps on this crate):

- **NONE.** This is a standalone service binary, not a library. The Ferrosa
  engine reaches it **over HTTP** via `ferrosa-storage`'s `RemoteBackend`
  (`POST /internal/index/build`) when `FERROSA_INDEX_BACKEND=remote`. The HTTP
  request/response JSON is the integration boundary, not a compile-time
  dependency.

## Tests

7 in-crate unit/async tests (`src/server.rs`, `src/worker.rs`): the `/health`
route, index-type and priority parsing, filter-predicate threading into the job,
the quantized fail-closed path, and the quantized manifest response shape. There
is **no `tests/` integration dir** and no end-to-end S3 download→build→upload
test — a tracked gap (see [FMEA](specs/fmea.md), [roadmap](specs/roadmap.md)).

## Specs

- [Architecture overview](specs/overview.md) — module map, data flow, invariants
- [FMEA / known issues](specs/fmea.md) — failure modes + gaps
- [Roadmap](specs/roadmap.md) — Now / Next / Later
