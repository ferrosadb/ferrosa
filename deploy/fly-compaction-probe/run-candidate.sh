#!/usr/bin/env bash
# Final candidate gate: correctness, then measurements, then application size.
set -euo pipefail
export PATH="/usr/local/cargo/bin:$PATH"
export CARGO_BUILD_JOBS=4 CARGO_TARGET_DIR=/work/candidate-target
mkdir -p /work/logs /work/results /work/candidate
# Keep build traffic out of timing samples and preserve baseline executables.
for ((i = 0; i < 360; i++)); do
  if [[ $(cat /work/results/baseline-status) == complete ]]; then break; fi
  if ! pgrep -f '[c]argo build --release -p ferrosa --bin ferrosa' >/dev/null; then
    echo 'Baseline node build did not complete; inspect baseline-node-build.log' >&2
    exit 1
  fi
  sleep 5
done
[[ $(cat /work/results/baseline-status) == complete ]]
tar xf /tmp/candidate.tar -C /work/candidate
# The final probe differs only by lint cleanups from the initial calibration;
# rebuild/rerun that exact source on the baseline before comparing candidates.
cp /work/candidate/ferrosa-storage/examples/compaction_allocation_probe.rs /work/baseline/ferrosa-storage/examples/
cd /work/baseline
CARGO_TARGET_DIR=/work/baseline-target cargo build --release -p ferrosa-storage --example compaction_allocation_probe > /work/logs/baseline-final-probe-build.log 2>&1
cp /work/baseline-target/release/examples/compaction_allocation_probe /work/results/baseline-probe
mv /work/results/baseline.jsonl /work/results/baseline-initial-calibration.jsonl
for rows in 256 2048 16384; do
  /work/results/baseline-probe "$rows" 1 4 128 7 >> /work/results/baseline.jsonl
done
/work/results/baseline-probe 64 128 4 64 7 purge >> /work/results/baseline.jsonl
/work/results/baseline-probe 2048 1 4 128 7 complex-purge-lz4 >> /work/results/baseline.jsonl
/work/results/baseline-probe 256 1 4 16384 7 lz4 >> /work/results/baseline.jsonl
/work/results/baseline-probe 2048 1 16 64 7 >> /work/results/baseline.jsonl
/work/results/baseline-probe 1 1 4 8388608 7 >> /work/results/baseline.jsonl
cd /work/candidate
rustc -vV > /work/results/candidate-manifest.txt
cargo test --release -p ferrosa-sstable -- --test-threads=1 > /work/logs/candidate-sstable-correctness.log 2>&1
cargo test --release -p ferrosa-storage --lib compaction -- --test-threads=1 > /work/logs/candidate-correctness.log 2>&1
cargo test --release -p ferrosa-storage --test compaction_alloc -- --test-threads=1 > /work/logs/candidate-allocation-guards.log 2>&1
echo correctness_passed > /work/results/candidate-status
cargo build --release -p ferrosa-storage --example compaction_allocation_probe > /work/logs/candidate-build.log 2>&1
cp /work/candidate-target/release/examples/compaction_allocation_probe /work/results/candidate-probe
: > /work/results/candidate.jsonl
for rows in 256 2048 16384; do
  /work/results/candidate-probe "$rows" 1 4 128 7 >> /work/results/candidate.jsonl
done
/work/results/candidate-probe 64 128 4 64 7 purge >> /work/results/candidate.jsonl
/work/results/candidate-probe 2048 1 4 128 7 complex-purge-lz4 >> /work/results/candidate.jsonl
/work/results/candidate-probe 256 1 4 16384 7 lz4 >> /work/results/candidate.jsonl
/work/results/candidate-probe 2048 1 16 64 7 >> /work/results/candidate.jsonl
/work/results/candidate-probe 1 1 4 8388608 7 >> /work/results/candidate.jsonl
COMPACTION_MAX_PEAK_BYTES=16777216 /work/results/candidate-probe 2048 1 4 64 1 > /work/logs/candidate-budget-green.log 2>&1
COMPACTION_MAX_PEAK_BYTES=33554432 /work/results/candidate-probe 1 1 4 8388608 1 > /work/logs/candidate-large-budget-green.log 2>&1
# Explicitly exercise both raw I/O modes with values much larger than a window.
FERROSA_COMPACTION_DIRECT_READ=1 /work/results/candidate-probe 1 1 4 8388608 1 > /work/logs/candidate-large-direct-control.log 2>&1
FERROSA_COMPACTION_DIRECT_READ=0 /work/results/candidate-probe 1 1 4 8388608 1 > /work/logs/candidate-large-cached-control.log 2>&1
echo measurements_passed > /work/results/candidate-status
cargo build --release -p ferrosa --bin ferrosa > /work/logs/candidate-node-build.log 2>&1
cp /work/candidate-target/release/ferrosa /work/results/candidate-ferrosa
stat -c '%s' /work/results/candidate-ferrosa > /work/results/candidate-binary-bytes.txt
cp /work/results/candidate-ferrosa /work/results/candidate-ferrosa.stripped
strip /work/results/candidate-ferrosa.stripped
stat -c '%s' /work/results/candidate-ferrosa.stripped > /work/results/candidate-binary-stripped-bytes.txt
echo complete > /work/results/candidate-status
