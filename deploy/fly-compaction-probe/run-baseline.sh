#!/usr/bin/env bash
# Run on the disposable Fly host. Each gate must pass before the next starts.
set -euo pipefail
export PATH="/usr/local/cargo/bin:$PATH"
export CARGO_BUILD_JOBS=4 CARGO_TARGET_DIR=/work/baseline-target
cd /work/baseline
mkdir -p /work/logs /work/results
printf 'baseline=%s\n' e2bc833dd2249abacf397f25bb49011f78da4962 > /work/results/baseline-manifest.txt
rustc -vV >> /work/results/baseline-manifest.txt
# frg is absent on this disposable rust image; keep complete raw cargo logs.
if command -v frg >/dev/null; then echo 'frg=available'; else echo 'frg=unavailable; raw cargo fallback' >> /work/results/baseline-manifest.txt; fi
cargo test --release -p ferrosa-storage --lib compaction -- --test-threads=1 > /work/logs/baseline-correctness.log 2>&1
echo correctness_passed > /work/results/baseline-status
cargo build --release -p ferrosa-storage --example compaction_allocation_probe > /work/logs/baseline-build.log 2>&1
cp /work/baseline-target/release/examples/compaction_allocation_probe /work/results/baseline-probe
# Every invocation asserts full output values/order/tombstones and BTI point reads.
for rows in 256 2048 16384; do
  /work/results/baseline-probe "$rows" 1 4 128 7 >> /work/results/baseline.jsonl
done
/work/results/baseline-probe 64 128 4 64 7 purge >> /work/results/baseline.jsonl
/work/results/baseline-probe 2048 1 4 128 7 complex-purge-lz4 >> /work/results/baseline.jsonl
/work/results/baseline-probe 256 1 4 16384 7 lz4 >> /work/results/baseline.jsonl
/work/results/baseline-probe 2048 1 16 64 7 >> /work/results/baseline.jsonl
/work/results/baseline-probe 1 1 4 8388608 7 >> /work/results/baseline.jsonl
echo measurements_passed > /work/results/baseline-status
# Application release binary size uses identical profile/toolchain/target per arm.
cargo build --release -p ferrosa --bin ferrosa > /work/logs/baseline-node-build.log 2>&1
cp /work/baseline-target/release/ferrosa /work/results/baseline-ferrosa
stat -c '%s' /work/results/baseline-ferrosa > /work/results/baseline-binary-bytes.txt
cp /work/results/baseline-ferrosa /work/results/baseline-ferrosa.stripped
strip /work/results/baseline-ferrosa.stripped
stat -c '%s' /work/results/baseline-ferrosa.stripped > /work/results/baseline-binary-stripped-bytes.txt
echo complete > /work/results/baseline-status
