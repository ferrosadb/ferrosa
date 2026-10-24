# Compaction allocation probe

This harness compares the production `CompactionExecutor::submit` / completion
path using the same example source, Rust toolchain, release profile, machine and
input data on both revisions. It includes default-enabled output verification.
The baseline is `e2bc833dd2249abacf397f25bb49011f78da4962` (v0.27.0).

The disposable Fly app is `ferrosa-compaction-probe-1023`, machine
`68342d1f206778`, in `sjc`, performance 4 CPUs and 8192 MiB. No ports, production
data or production configuration are used. Destroy this task-owned app after
collecting both arms' artifacts.

Run correctness first: the crate's compaction unit/differential/cancellation
tests, followed by the probe's exact comparisons of every emitted partition,
clustering key, cell value, collection path and tombstone, plus BTI partition
and clustering point reads. Probe success alone is not the full suite.

The probe prints JSONL only after those value assertions pass. Its arguments
are rows per partition, partition count, fan-in, bytes per simple cell,
repetitions, and optional `complex`, `purge` and `lz4` substrings:

```sh
cargo run --release -p ferrosa-storage --example compaction_allocation_probe -- 2048 1 4 128 3 complex-purge-lz4
```

Input construction and readback assertions are outside the measurement window.
Global allocator atomics include worker-thread allocations. The measured window
starts immediately before submission and ends after completion/result receipt.
`peak_live_delta_bytes` is the peak absolute live heap above the initial live
heap; `allocated_bytes` includes every allocation/reallocation's requested size,
and `alloc_events` counts those calls. These are heap measures, not RSS. Input
metadata and the idle executor already exist at window start. There is one task
at a time and one worker; the example joins workers on shutdown.

Compare the 256/2048/16384 row curve to distinguish partition-sized residency
from fixed decode buffers. Also exercise many small partitions with purge,
collection paths with purge/compression, and large values. Report per-shape
raw samples plus median/min/max timing; avoid wall-clock assertions in general
parallel unit tests. Keep raw logs and manifests in the evidence directory.

The final matrix also includes fan-in 16 and one row with two columns per
input (four inputs), containing seven live 8 MiB input values in total. The
newest first-column tombstone wins; one second-column 8 MiB value is emitted. Explicit controls run that large-row fixture with direct
reads both enabled and disabled, exercising spans larger than the read-ahead
windows. Deterministic 16 MiB and 32 MiB peak gates use the separately calibrated
2048-row/64-byte and single-large-row shapes respectively.

Only after correctness and speed evidence is collected, compare the actual
`ferrosa` release executable byte counts, both with debug line tables and after
`strip`, using identical flags and target. Probe executable size is auxiliary
and must not be presented as the database executable's size.

The remote image has no `frg` CLI. The runner records this verified absence and
uses raw Cargo with complete logs saved under `/work/logs`, rather than
truncating compiler/test output. Build output stays outside both source trees.
**Use a distinct target directory per arm:**
`/work/baseline-target` and `/work/candidate-target`. Never reuse a target directory
between these relocated source trees. A refreshed baseline initially reused
candidate SSTable artifacts through Cargo's freshness cache; the changed
allocation count exposed it. That run was rejected. Dependency caches may be
seeded only after removing every local workspace crate's artifacts/fingerprints.
The pinned repository toolchain is installed by rustup.

For a fresh reproduction, populate `/work/baseline` from the baseline commit and
`/work/candidate` from the recorded source, copy the same probe example into both,
and install the pinned toolchain. Upload `paired.py` as `/tmp/paired.py` and
`run-final-v5.sh`, then run the latter with Bash. It builds each arm in its own
target directory, runs targeted correctness before timing, and then runs the full
SSTable suite and final node size build. Build the baseline node with the same
profile beforehand to produce `/work/results/baseline-ferrosa[.stripped]`.
`attribute.py` and `trace-io.py` reproduce optional diagnostic controls; do not
run them concurrently with timing. See [RESULTS.md](RESULTS.md) and
[evidence/](evidence/) for final data and hashes.
