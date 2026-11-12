# PR #601 follow-on exploration — 2026-10-23

The reusable harness and 80 unit tests are implemented on
`bench/compaction-exploration`. No database behavior was changed.

## Revisions and environment

- PR head: `5f88f8ec28e6a674c9098c9161062a7a0ec9facc`.
- Executed CI test merge: `839eabe075508100ca2c1ee4656de5521857471e`.
  Its complete source tree is identical to that PR head.
- CI run `40029492091`, node-image artifact `12534064046`, default-feature
  release binary. The evidence manifest records the artifact and script hashes.
- Three reused LAX Fly machines: 2 performance CPUs, 4 GiB RAM and 10 GB
  volumes each; separate 2-performance-CPU generator.
- RF=3, QUORUM, TLS, STCS, no driver retries, one compaction worker,
  64 MiB flush threshold, 5-second flush interval, 384 MiB configured cache.
- Compaction readback verification enabled; write verification retained at
  the existing configuration's `false`. This differs from the stopped machines'
  original compaction-verification setting, which was also `false`.
- Isolated data/Raft/S3 paths; the original volume data was preserved.

## Evidence classification

The first attempt reused credentials for a bucket the user had intentionally
cleared. Failing to recreate it was an experiment setup mistake. That run is
excluded from healthy performance conclusions. A dedicated bucket was then
created; direct HEAD/PUT/GET/readback/DELETE and actual uploads from every node
passed before the healthy reruns.

| Trial | Observation | Interpretation |
|---|---|---|
| Small healthy smoke | Preload, mixed traffic, drain, full readback and collection passed | Harness integration verified |
| 64 KiB values, all-write preload at 300 ops/s | 10/4,096 writes rejected by write-pump admission | Diagnostic failure; no capacity claim |
| 64 KiB values, preload at 100 ops/s, mixed at 300 ops/s | Preload passed; 8/36,000 mixed operations rejected, zero payload mismatches; one metrics TLS timeout | Diagnostic failure; no accepted benchmark headline |
| 64 KiB values, mixed at 600 ops/s | Fly user-source stop events interrupted the run | Excluded; connection errors cannot be attributed to overload alone |
| Initial wide/many-table attempts | Could not connect after those stops | Not executed |

The user confirmed the baseline run associated with the stops. Fly recorded
`requested_stop=true`, `oom_killed=false` at 22:24:20–22 UTC. Baseline activity
and retained cold tables are additional possible confounders for the first
pilot. These observations do not establish sustainable capacity or a causal
effect of #601. Subsequent shape runs use fresh experiment data after the
baseline finished.

The fresh runs both completed successfully with no operation errors, payload
mismatches, or collector errors. Each executed 4,096 preload writes, 72,000
mixed operations, 6,000 drain reads and 4,096 final verification reads:

| Fresh case, 16 KiB values | Achieved mixed ops/s | Scheduled-response p99 | Service p99 | Drain p99 |
|---|---:|---:|---:|---:|
| 4 wide partitions, 1,024 rows each | 599.7 | 551.9 ms | 102.8 ms | 7.38 ms |
| 16 tables, point partitions | 599.7 | 26.9 ms | 26.2 ms | 7.13 ms |

These are pooled read/write percentiles from one trial per case. The wide
case's scheduled-response latency includes substantial admission queueing:
dispatch-lag p99 was 502.1 ms. The worst one-second bins occurred around
103–111 seconds into mixed traffic; node 1 was sampled compacting around
105–111 seconds. That correlation is a useful follow-up target, not proof of
causality. Sampled database RSS stayed below 263 MiB across both fresh cases.

**Successful traffic was not sufficient compaction stress.** Global counters
advanced by only one completed compaction per node in each case, with no
failures/cancellations. The many-table case ended with three SSTables per table,
below STCS's four-input trigger; its tiny global compaction timing does not
demonstrate meaningful work on the workload tables. Increase the working set
relative to the per-table flush threshold, extend duration, and require
demonstrated compaction generations before testing fairness or storm prevention.

## Best follow-on work

1. **Measure metadata-based output readback verification.** During the 300
   ops/sec diagnostic case, cumulative `output_verify` grew 21–28 seconds per
   node; `local_write_sstable` grew 29–30 seconds and `s3_upload_await` 29–32
   seconds. These timers are not disjoint percentages or isolated A/B results.
   The executor's readback loop calls `next_clustered_row_into` but only uses
   partition and row counts. Reusing metadata cursors could avoid decoding
   values. Preserve unconditional digest verification, structural validation,
   cancellation and corruption refusal. Test 64-byte, 64-KiB and 8-MiB cells,
   wide/static rows, allocations, RSS, and actual I/O. This is the closest
   follow-on to #601. Forge: `t_61ebd0fe`.

2. **Test shared I/O admission and pressure sampling.** The eight rejections
   clustered within roughly 100 ms and reported `write-pump blocked rate
   reached hard limit`. The pressure sampler aggregates producer-blocked time
   globally over a minimum 100-ms interval and clamps it to one; admission
   rejects at one. Logs also contain slow commit-log syncs. Test fixed total
   bytes/sec across different table/pump counts, and compaction/readback/upload
   concurrency while protecting foreground fsync. Assess smoothing and
   hysteresis before changing thresholds. Do not weaken write admission merely
   to improve a benchmark. Forge: `t_0e97b46b`.

3. **Fix selection progress and common task budgets.** Recompiling the actual
   selector modules at this head still produces:

   | Inputs | Selected work |
   |---|---|
   | STCS, 4 × 128 MiB | One task |
   | STCS, 4 × 129/200/512 MiB | No task |
   | UCS, 5 × 200 MiB | One 1,000 MiB task |

   This was an isolated selector characterization with schema/logging stubs,
   not a distributed engine test. More workers cannot repair empty selection.
   Keep the progress/budget fix separate from cursor optimizations.
   Forge: `t_5f246338`.

4. **Separate attempted uploads from successful persistence.** The missing-
   bucket attempt increased upload byte/file counters despite `NoSuchBucket`.
   Both upload paths call `observe_upload_file` before returning the upload
   result. Add explicit success/failure counters and pending-age measurements;
   never use attempted bytes as an auto-tuning progress signal.
   Forge: `t_b1c2eec6`.

5. **Run fan-in scaling before changing merge selection.** The streaming merge
   scans source heads to choose the next partition/row. Measure 4/16/32/64
   inputs with both fixed total input and fixed per-input work, overlap versus
   disjoint keys, and 1/2/4 workers. This distinguishes linear head-selection
   cost from simply processing more data. The pilot does not establish that
   this is the dominant hotspot.

   `fanin.py` now runs the existing allocation probe in fresh processes with
   randomized 4/16/32/64-way sweeps, fixed total input rows and fixed per-input
   rows, strict correctness/shape validation, binary hashes and raw evidence.
   Its orchestration was tested with executable fixtures; the real Rust fan-in
   matrix was **not run** in this session. The existing fixture overlaps keys;
   a disjoint-key extension is still needed to isolate head-selection cost.

There is still no evidence here justifying automatic STCS/LCS/TWCS switching.
The selector/configuration gaps from the earlier review remain relevant,
including unsupported strategy names falling back to STCS and ignored UCS
scaling parameters. Adaptive resource limits are the more plausible direction
after progress, instrumentation and admission behavior are made explicit.

## Limits and reproducibility

This is a diagnostic pilot, not a randomized comparison with Cassandra. The
same-value overwrite fixture checks missing/corrupt values, not LWW ordering,
TTL/tombstone safety, or repair. No repeated cancellation storm was established.
Raw samples and annotations are retained at
`/home/bkearns/src/ferrosa-suite/compaction-evidence-20261023`.
Use `case-annotations.json` alongside raw manifests: a failed or interrupted
run must not become a successful performance result during later analysis.

See [README.md](README.md) for the runnable matrix and verification gates.

Cleanup is complete: all three machines' original configurations and images
were compared against the private backups and restored exactly, with all three
stopped. The three task-created data directories were removed, and the
disposable generator and experiment bucket were destroyed. Raw evidence remains
local; no PR or commit was created. Rust workspace gates were not run because
the changes are Python test tooling and documentation; the 80 targeted tests,
data-directory guard, whitespace check and secret scan passed.
