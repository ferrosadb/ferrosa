#!/usr/bin/env bash
# Reproduce final V5 gates on frozen baseline/candidate source trees.
set -euo pipefail
export PATH="/usr/local/cargo/bin:$PATH" CARGO_BUILD_JOBS=4 CARGO_TARGET_DIR=/work/candidate-target
stage=correctness
trap 'code=$?; if ((code)); then echo "failed:$stage:$code" > /work/results/final-v5-status; fi' EXIT
mkdir -p /work/logs /work/results
cd /work/candidate
echo correctness > /work/results/final-v5-status
cargo test --release -p ferrosa-sstable --lib scan:: -- --test-threads=1 > /work/logs/v5-scan.log 2>&1
cargo test --release -p ferrosa-sstable --test scan_cursor_io --test scan_cursor -- --test-threads=1 > /work/logs/v5-cursor-io.log 2>&1
cargo test --release -p ferrosa-storage --lib compaction -- --test-threads=1 > /work/logs/v5-compaction.log 2>&1
cargo test --release -p ferrosa-storage --test compaction_alloc --test compaction_streaming_peak -- --test-threads=1 --nocapture > /work/logs/v5-allocation-guards.log 2>&1
cp ferrosa-storage/examples/compaction_allocation_probe.rs /work/baseline/ferrosa-storage/examples/
for arm in baseline candidate; do
  cd "/work/$arm"
  CARGO_TARGET_DIR="/work/$arm-target" cargo build --release -p ferrosa-storage --example compaction_allocation_probe > "/work/logs/final-v5-$arm-probe-build.log" 2>&1
  cp "/work/$arm-target/release/examples/compaction_allocation_probe" "/work/results/$arm-final-probe"
done
cmp /work/baseline/ferrosa-storage/examples/compaction_allocation_probe.rs /work/candidate/ferrosa-storage/examples/compaction_allocation_probe.rs
stage=paired_speed
echo paired_speed > /work/results/final-v5-status
python3 /tmp/paired.py > /work/logs/final-v5-paired.log 2>&1
COMPACTION_MAX_PEAK_BYTES=16777216 COMPACTION_MAX_ALLOC_EVENTS=10000 /work/results/candidate-final-probe 2048 1 4 64 1 > /work/logs/final-v5-64B-green.log 2>&1
COMPACTION_MAX_PEAK_BYTES=33554432 /work/results/candidate-final-probe 1 1 4 8388608 1 > /work/logs/final-v5-large-green.log 2>&1
stage=full_sstable
echo full_sstable > /work/results/final-v5-status
cd /work/candidate
cargo test --release -p ferrosa-sstable -- --test-threads=1 > /work/logs/final-v5-sstable.log 2>&1
stage=binary_size
echo binary_size > /work/results/final-v5-status
cargo build --release -p ferrosa --bin ferrosa > /work/logs/final-v5-node-build.log 2>&1
cp /work/candidate-target/release/ferrosa /work/results/candidate-ferrosa
stat -c '%s' /work/results/candidate-ferrosa > /work/results/candidate-binary-bytes.txt
cp /work/results/candidate-ferrosa /work/results/candidate-ferrosa.stripped
strip /work/results/candidate-ferrosa.stripped
stat -c '%s' /work/results/candidate-ferrosa.stripped > /work/results/candidate-binary-stripped-bytes.txt
sha256sum /work/results/baseline-ferrosa /work/results/candidate-ferrosa /work/results/baseline-ferrosa.stripped /work/results/candidate-ferrosa.stripped > /work/results/final-v5-binary-sha256.txt
sha256sum /work/baseline/ferrosa-storage/examples/compaction_allocation_probe.rs /work/candidate/ferrosa-storage/examples/compaction_allocation_probe.rs > /work/results/final-v5-harness-sha256.txt
echo complete > /work/results/final-v5-status
