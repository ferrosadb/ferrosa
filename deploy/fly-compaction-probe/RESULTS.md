# Fly compaction verification — final V5

The measured candidate reduces allocations and live heap on every shape. Six
shapes improve median elapsed time by 10–49%; the two I/O-dominated shapes are
at parity (16 KiB compressed values and 8 MiB values). Prior sustained direct-I/O
regressions were rejected and fixed before this final run. The final correctness and performance gates pass; executable size grows
0.29% after stripping, the third-priority tradeoff recorded below.

The baseline is `e2bc833dd2249abacf397f25bb49011f78da4962` (v0.27.0). The measured
candidate is `f3d687d5d82af73fe676195d3122a464482687cd`, the frozen V5 source on
`perf/compaction-cursors`; all 18 source/test/probe
hashes match the reviewed local tree. [Source and environment manifest](evidence/source-manifest.json).

Both arms run the same real public `CompactionExecutor` harness, default output
verification enabled, Rust 1.98.0 release profile on the same Fly `sjc`
performance-4-CPU / 8 GiB machine. Each shape has seven alternating fresh-process
pairs, direct reads enabled, with no compiler/test/trace activity during timing.
Each process verifies exact partition/key/row/cell/value/path/tombstone content
and BTI partition/clustering reads after measurement. All 112 samples passed.

This follows the allocation/cursor ideas discussed in [CASSANDRA-20918](https://issues.apache.org/jira/browse/CASSANDRA-20918).

Input generation, the idle executor, and final exact-content assertions are
outside the measured submit-to-completion window. The counting allocator wraps `System` and includes all worker
threads. The production node uses jemalloc; these bounded probe timings are not
a production-allocator throughput claim. Peak is live heap above the starting live heap, not RSS. Requested
allocation bytes count reallocations at their full requested size. No wall-time
assertions were added to normal CI.

| Workload | Median ms | Time change | Allocations | Allocated MiB | Peak live MiB |
|---|---:|---:|---:|---:|---:|
| 256 rows | 10.573→9.480 | -10.34% | 8,903→1,057 | 19.54→17.97 | 13.47→12.72 |
| 2,048 rows | 31.089→20.347 | -34.55% | 64,557→1,684 | 35.40→22.16 | 22.82→16.40 |
| 16,384 rows | 358.904→301.563 | -15.98% | 509,504→6,680 | 136.82→30.26 | 63.37→20.24 |
| 128 partitions ×64 rows +purge | 88.174→62.037 | -29.64% | 292,480→7,371 | 72.65→26.82 | 24.76→20.24 |
| 2,048 complex +purge +LZ4 | 58.558→36.045 | -38.44% | 134,496→3,238 | 40.90→18.70 | 22.08→12.81 |
| 256 rows ×16 KiB +LZ4 | 78.225→77.908 | -0.40% | 19,838→2,084 | 101.04→23.09 | 52.31→12.70 |
| 2,048 rows ×fan-in16 | 85.305→43.603 | -48.89% | 238,344→3,052 | 82.60→39.10 | 51.33→33.44 |
| 1 row ×8 MiB | 884.145→884.579 | +0.05% | 13,798→982 | 153.72→41.45 | 84.19→28.25 |

Except where shown, rows are in one partition, fan-in is four, and cell payloads
are 128 B. The many-partition and fan-in16 cases use 64 B. Each row has two
columns; every seventh newest first-column cell is a tombstone. The one-row
8 MiB shape contains seven live input payloads across four inputs and emits one
winning 8 MiB payload plus the first-column tombstone. It does not have 16 MiB of
winning output.

For 16,384 rows, events fall 98.69%, allocated bytes 77.88%, and peak live bytes
68.05% (66,444,557→21,227,712 B). For the large-value shape, events fall 92.88%,
allocated bytes 73.03%, and peak live bytes 66.44% (88,275,072→29,626,284 B).

[Baseline samples](evidence/baseline-paired.jsonl),
[candidate samples](evidence/candidate-paired.jsonl), and
[min/median/max summaries](evidence/paired-summary.json) retain all raw values.
The large-value +0.05% and compressed +16 KiB −0.40% changes support parity,
not a speedup claim. This is a bounded single-worker matrix, not a production
throughput or durability benchmark.

The modern path keeps source row metadata, an owned winning output row, two
reusable equal-timestamp value buffers, and fixed I/O/writer buffers. Static rows
remain owned per input; memory depends on winning/static row width and fan-in.
Output index/Bloom metadata and validation state also remain. The measured
curve demonstrates removal of partition row materialization, not constant
whole-task heap or a universal peak budget.
The new merge state and buffers are task-owned; decode scratch bypasses the
shared chunk cache and adds no shared merge locks. Existing scheduler,
read-ahead and publication synchronization remains. Whole compaction is not
claimed lock-free. Legacy unsorted rows retain fully owned one-row heads and bounded sorter/spill
buffers (4 MiB plus one row); the metadata-only losing-payload property applies
to the modern path. The all-purged fallback is disk-spooled. Unresolved exact metadata ties use canonical `task.inputs` order,
consistent with `merge_partitions`; this does not preserve incidental old heap
pop order. Timestamp/value/local-deletion-time comparisons are unchanged.

Correctness evidence, in priority order:

- Local final full affected suites: SSTable 510/510 and storage 2,082/2,082,
  zero failures or skips. Full Fly release SSTable suite: 508 unit/integration tests plus seven doctests
  (515 total), zero failures or ignores, exit 0. Fly targeted release gates: normal scan 16/16,
  cursor 1/1, public physical-I/O cursor 1/1, compaction 272/272, allocation 1/1,
  process-isolated two-shape peak guard 1/1; all exit 0.
- The exact-content executor guards captured behavioral baseline REDs before
  streaming changes: 2,048×64 B peak 19,751,083 B >16 MiB; old owned-row large
  shape 88,347,248 B >32 MiB. Final default CI checks require ≤16 MiB and
  ≤10,000 events for the former and ≤32 MiB for the latter. Final Fly guard
  invocations measure 15,106,462 B /1,621 events and 29,626,284 B respectively.
- Equal timestamps, adjacent same-source duplicate keys, schema remapping and
  dropped columns, winner and discarded-value CRC errors, cancellation and
  publication validation are covered. Disabling unread-span validation made
  the discarded-corruption regression fail; restored code passes.
- Public pending metadata defers CRC checking until winner fetch or source
  advance. Original raw spans survive schema remapping, including dropped
  columns, so every unread span is validated before a merged row is published.
  Public eager metadata APIs retain eager validation.
- The sparse-read regression captured 12,582,967 physical bytes for 8,388,663
  logical bytes before the change; final code reads 8,421,431 B with both sync
  and prefetched readers, within logical size +two windows. Existing normal
  two-buffer/backpressure pointer contracts pass unchanged.

The earlier V4 final paired matrix was rejected: direct 16,384-row elapsed time
regressed 28.9%, and 8 MiB values regressed 24.8%. Worker+prefetch `pread` tracing
attributed repeated metadata/value seeks to 27.80 vs16.71 MiB source reads for
16,384 rows and 70.00 vs56.00 MiB for large values. Explicit sparse-read hints
now preserve the sequential read-ahead window and pending prefetch. Final
three-pair controls read 17.934 vs17.777 MiB process input for 16,384 rows and
56.238 vs56.180 MiB for large values; the cached controls also improve.
[Final bounded direct/cached controls](evidence/v5-io-controls.jsonl).

Two harness failures were rejected rather than attributed to the product:
worker concurrency=1 was accidentally exported into the full correctness suite;
its default-configuration control passed. A refreshed baseline shared target
cache reused candidate SSTable artifacts; changed allocation counts exposed it.
Those measurements were discarded, every local workspace crate artifact and
fingerprint was removed, and each arm now uses its own target directory.
Original pre-candidate baseline artifacts were valid. An earlier 16 MiB budget
check mistakenly used 128 B rather than its calibrated 64 B payload; it failed
and remains in raw evidence. The 128 B workload is reported above without that
budget. An initial trace summary omitted truncated prefetch-thread names; it was
rejected and reparsed including `sstable-read` threads.

The actual `ferrosa` executable built successfully with identical target and
release flags (376 s final build, exit 0):

| Executable | Baseline bytes | Candidate bytes | Change |
|---|---:|---:|---:|
| Release with line tables | 461,070,664 | 462,224,656 | +1,153,992 (+0.250%) |
| Separate stripped copy | 82,320,304 | 82,558,048 | +237,744 (+0.289%) |

This is a small size increase, accepted as the third-priority tradeoff after
correctness and measured performance. No binary-size reduction is claimed.
[Size and SHA-256 manifest](evidence/binary-size.json). Probe size is not substituted.

Local supporting checks: full-workspace all-target/all-feature Clippy with
`-D warnings`, docs with `-D warnings`, formatting, build-all-targets, scoped
pre-commit and P0 OOM audit passed. Final all-targets build exited 0; it emits the
existing Apple linker `__eh_frame` compact-unwind warning, separate from Clippy. Stream coverage is 543/608 lines (89.31%);
the scoped 80% gate passed. Two 60 s SSTable fuzz runs passed on V4 (before the sparse-read hint). Pure DataReader and
new sparse-failure Miri checks passed. The full cursor Miri run remains blocked
by an existing crossbeam/Rayon Stacked Borrows retag failure (`t_71b33c44`).
At the original performance snapshot, full workspace tests and the exact CI
mirror were blocked by the isolated port-band assertion (`t_97fb1f1c`).
Subsequent CI repairs cache the macOS kernel query before listener probes and
keep deliberate collision listeners inside their allocator slice. The original
validation manifest remains a historical snapshot; final combined-branch
validation is recorded separately below. Unchanged dependency audit reports
32 vulnerabilities and 17 warnings (`t_8ec7ed26`). No dependency
versions, security allowances, or assertions were weakened. Forge's 300 s
response timeout on long local suites was tooling-only; terminal exit/count
records confirm the affected suites completed. Refactoring dense new stream code
and removing unused modern-path sorter filename formatting remain follow-up
`t_8badc9dc`.

Raw verbose build/test/trace logs and rejected controls are preserved off-tree at
`/Users/bkearns/src/ferrosa-suite/compaction-probe-evidence-20261023`.
[Validation manifest](evidence/validation.json) records counts, exit statuses and
raw-log/archive hashes. Remote Forge was unavailable; raw Cargo saved complete
logs without truncation.
Disposable Fly app `ferrosa-compaction-probe-1023` and its machine were destroyed
after final evidence collection; app-list verification confirms absence. The
existing builder remains present and untouched. [Cleanup record](evidence/cleanup.json).
No production app was modified. No commit was made by the verifier.

## Combined branch CI verification

The final CI source is `5a67df9138ce7f46ac2459e3b7566321fcbade78`, based on
main `6ba60f5705fc5da675b0cd0cfa84581e5e2c5c12` (v0.27.1). The readiness fix
and release on main were merged before this final run. All 18 measured
compaction source/probe hashes still match the performance candidate above.
The merged executable was not rebenchmarked or remeasured for size; the tables
above refer to the frozen performance candidate.

Unprivileged Linux x86_64 on a disposable Fly performance-16 / 32 GiB host,
Rust 1.98.0, cargo-nextest 0.9.149, the pinned ASC bundle and CI debug profile:

- Exact CI general command: **9,827 passed**, zero failures, 93 existing policy skips.
- Exact six-binary process command: **14 passed**, zero failures or skips.
- Workspace all-target/all-feature Clippy with `-D warnings`, docs with
  `RUSTDOCFLAGS=-D warnings`, formatting, and **155 CI contract tests**: all exit 0.
- Real Linux STATX_DIOALIGN, permission-denied DROP, six-process cross-DC DDL,
  exclusive graph scale/heal and new publication regressions all pass.

The initial merged nextest control passed 9,822 tests and failed two. Its raw
archive is retained: cross-DC CREATE was acknowledged at least 36 ms before
Cluster DDL publication, and the graph scale/heal fixture exceeded its unchanged
2,000 ms unsynced-write admission bound at 2,029 ms. The first is repaired by
retaining Forming DDL until publication and gating admission/readiness; three
real-behavior regressions captured RED then GREEN. The second fixture reserves
all nextest slots. This prevents competition from other tests; its original
log does not distinguish device latency, scheduling starvation or a missed
sync wakeup. No assertion, production deadline, retry or test selection was
weakened.

A subsequent run found that the new CL-77 documentation section needed a table
row and ID-registry entry. Both were corrected without changing any existing
FMEA record. The complete three-test table target passed, and the full Linux
selection was rerun on that documentation commit. Runtime and test code stayed
at `428b0393aee30db4522ca00ef4dd9dc907feff0e`.

Earlier repairs cache the macOS kernel port query before listener probes,
keep deliberate collision listeners within their allocator's leased slice,
establish Phase 10 QUORUM readiness before its unchanged single write, restore
loadgen property selection in nightly, and build the pinned ASC prerequisite
in the local mirror. The smoke cluster-formation wait now also requires the
published Cluster DDL path. Behavioral Python negative controls reproduce both
premature completion and permanent-publication failures. The restored loadgen
property passed four generated cases; the full nightly fuzz budget was not
rerun locally.

The actual Phase 1–13 smoke stack passed using the current script and nightly
`v2026.10.23.0449` on linux/amd64 under local Podman. This validates the harness
against that nightly binary; the final source is exercised by the Fly process
tests above. Its isolated containers and volumes were removed.

After raw evidence collection, disposable Fly app `ferrosa-ci-probe-1023`, its
machine and 120 GiB scratch volume were destroyed. Empty machine/volume lists
were verified before app deletion; the app is absent afterward. The existing
builder remains present and untouched. No production application was modified.

The earlier root-permission control is rejected: UID 0 bypasses the chmod
failure fixture. The final run uses UID 1000 and verifies the permission denial
and real DROP behavior. macOS deliberately fails the Linux-only STATX_DIOALIGN
probe; that assertion remains intact. The push suppresses only a duplicate Mac
mirror with `FERROSA_SKIP_PREPUSH=1` after the supported Linux gates pass; the
global author guard remains enabled.

The original performance [validation snapshot](evidence/validation.json) is
preserved. [CI repair validation](evidence/ci-repair-validation.json) records
final source identity, test counts, rejected controls, raw-log/archive hashes,
independent review and task-infrastructure cleanup. Raw CI evidence is retained
at `/tmp/ferrosa-ci-repair-20261023`; original performance evidence remains at
the path above. The historical pre-taken-PG flake (`t_97fb1f1c`), dependency
audit and cursor Miri limitations remain tracked; passing these runs is not a
claim that every historical flake or nightly budget has been eliminated.
