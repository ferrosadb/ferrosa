---
crate: ferrosa-index-builder
doc: fmea
last_updated: 2026-06-19
---

# ferrosa-index-builder — FMEA / Known Issues

Failure modes ranked by **RPN = Severity × Occurrence × Detection** (1–10 each;
higher = worse). This is an out-of-engine service on the index-build path, so a
silent-wrong-result failure is more dangerous than a crash (the engine sees a
crash as HTTP 5xx and can retry; it cannot see a wrong sidecar).

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation / status |
|----|--------------|--------|---|---|---|-----|---------------------|
| IB-1 | No end-to-end test — the download→`LocalBackend::build`→sidecar→S3 path is never exercised in CI (no `tests/` dir; unit tests stop at request parsing and `/health`) | A regression in the actual build pipeline passes `cargo test -p ferrosa-index-builder` green; only caught in a live cluster | 8 | 4 | 7 | 224 | **Open gap.** Add an `object_store::memory` integration test that seeds components, runs `execute`, and asserts the sidecar bytes. |
| IB-2 | Pull mode hard-codes `index_type = "btree"` for every manifest index | Non-btree indexes (hash, vector, fulltext, filtered, geo) are rebuilt as btree → wrong/unusable sidecar, or filtered indexes rebuilt with NO predicate → unfiltered superset | 9 | 5 | 6 | 270 | **Open gap.** `pull.rs` ignores per-index type and predicate; the manifest entry carries only `(name, col_pos)`. Either restrict pull mode to btree-only tables explicitly or extend the manifest to carry index type + predicate. |
| IB-3 | App-level build failure returns HTTP 200 with `status:"failed"` (by design) | If the engine ever forgets the status split, a failed build looks like success | 8 | 2 | 4 | 64 | **Designed**: documented invariant; `RemoteBackend` checks `resp.status == "completed"`. Keep the two HTTP-vs-app error channels in sync. |
| IB-4 | `temp_bytes_used` accounting races / leaks under concurrency | Disk budget under- or over-counts → either spurious "budget exceeded" rejects or unbounded temp growth | 6 | 4 | 6 | 144 | Partial: `fetch_add`/`fetch_sub` on an `AtomicU64`, decremented on each early-return path. But the add-then-check window is racy across workers and a `spawn_blocking` panic would leak the temp dir + the byte count. No periodic reconciliation. |
| IB-5 | Orphaned temp files on crash / panic mid-build | Local disk fills across restarts; `max_temp_bytes` guard is per-process, not a startup sweep | 5 | 4 | 6 | 120 | **Open gap.** Cleanup is best-effort (`let _ = remove_dir_all`) on the happy/early-return paths only; no startup reaping of `ferrosa-index-builder/` temp dir. |
| IB-6 | Quantized `.qvec` direct-upload unimplemented | Vector-index quantized artifacts cannot be built remotely | 6 | 3 | 2 | 36 | **Fail-closed (good).** `execute` returns an explicit "not implemented" failure; covered by `quantized_remote_builder_fails_closed_*` test. Tracked feature gap, not a silent bug. |
| IB-7 | No auth / no TLS termination on `/internal/index/build` | Anyone who can reach the listen address can trigger arbitrary S3 reads/writes against the configured prefix | 7 | 3 | 5 | 105 | **Open gap.** The route is named `/internal/...` but the server enforces nothing; it relies entirely on network placement. No token, no mTLS. |
| IB-8 | Pull mode swallows manifest/`head` errors and retries forever with no backoff cap | A persistently failing engine endpoint logs a `warn` every interval indefinitely; no alerting signal, no circuit-break | 4 | 5 | 5 | 100 | Partial: errors are logged (`tracing::warn!`), so it is observable, but there is no metric, no max-retry, no backoff. |
| IB-9 | S3 build / `clap` startup uses `.expect()` (`build S3 client`, `bind listener`, `create temp dir`) | A misconfig panics the process on boot | 4 | 3 | 2 | 24 | Acceptable fail-loud at startup; crashes immediately with a clear message rather than running degraded. |

## Top risks to act on

1. **IB-2 (RPN 270)** — pull mode builds every index as `btree`. For any
   deployment with non-btree or filtered indexes this produces wrong sidecars
   *silently*. Either gate pull mode to btree-only or carry the index type +
   predicate in the manifest.
2. **IB-1 (RPN 224)** — the core build pipeline has zero automated coverage. The
   green test suite only proves request parsing and the health route; a real
   download/build/upload regression would ship undetected.

## Detection assets

- 7 in-crate unit/async tests: `/health`, `parse_index_type`, `parse_priority`,
  filter-predicate threading (`build_request_threads_filter_predicate_into_job`),
  the quantized fail-closed path, and the quantized manifest response shape.
- `tracing` structured logs on every build (sstable_id, index_name, error) and
  every pull-loop fetch.
- Engine-side `RemoteBackend` tests in `ferrosa-storage/src/index/remote_backend.rs`
  validate the client half of the contract.

## Gaps without a test or metric

- No integration test for the build pipeline (IB-1).
- No metrics endpoint beyond `/health` counters (no Prometheus, no per-index-type
  histograms); pull-loop failures are log-only (IB-8).
- No startup temp-dir reaping (IB-5); no reconciliation of `temp_bytes_used`
  against actual disk (IB-4).
