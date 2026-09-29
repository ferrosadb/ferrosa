#!/usr/bin/env bash
# Workspace-wide test-runner comparison: cargo nextest vs cargo test (libtest).
#
# Same selection as ci.yml's `test` job, same order, same flags — only the
# runner differs. Sequential on purpose: running them concurrently would make
# each measure the other's contention, not its own scheduling.
#
# Usage: bash scripts/bench-test-runners.sh
#
# Reports wall clock for each and the delta. Compilation is included in both
# numbers (they share a warm target dir, so the second pays less compile cost;
# note that when reading the delta).

set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

SELECT=(--all-features --workspace --lib --tests
        --exclude ferrosa-jepsen --exclude ferrosa-loadgen)

# Exactly the ci.yml "Run tests" skip list. Keep in sync if that changes.
SKIP=(--skip ::slow:: --skip accord::perf --skip batch_atomicity --skip pause_resume
      --skip recovery_coordinator --skip cassandra_reads_compacted
      --skip compaction_end_to_end_pipeline --skip dep_wait_ordering
      --skip lwt_batch_atomicity_all --skip clock_skew_large_preaccept --skip binary_
      --skip concurrent_write --skip many_flushes --skip flush_2000 --skip single_writer
      --skip write_flush_compact --skip reads_never_panic_on_arbitrary_content
      --skip corrupt_sstable_bytes_never_panic_never_oom
      --skip peak_resident_readers_within_cap --skip streaming_equals_single_pass
      --skip bounded_fetch_reassembles_to_single_pass --skip read_merge_is_lww_no_data_loss
      --skip digest_walk_is_deterministic --skip differential_oracle
      --skip fly_multi_node_streaming_scan
      --skip real_typed_edges_paged_scan_delivers_every_distinct_row
      --skip count_range_metadata_merger_dedups_real_typed_edges_sstables
      --skip promote_dir_fsync_lazyfs_crash_loses_neither_copy)

echo "=== warming the build (shared target dir) ==="
cargo build --all-features --workspace --lib --tests \
  --exclude ferrosa-jepsen --exclude ferrosa-loadgen >/dev/null 2>&1
echo "warm."

echo
echo "=== cargo nextest run (parallel) ==="
nextest_start=$(date +%s)
cargo nextest run "${SELECT[@]}" "${SKIP[@]}" >/tmp/nextest-run.log 2>&1
nextest_rc=$?
nextest_end=$(date +%s)
nextest_secs=$((nextest_end - nextest_start))
echo "exit=$nextest_rc  wall=${nextest_secs}s"
grep -E '^\s+Summary' /tmp/nextest-run.log | tail -1

echo
echo "=== cargo test (libtest) ==="
libtest_start=$(date +%s)
cargo test "${SELECT[@]}" -- "${SKIP[@]}" >/tmp/libtest-run.log 2>&1
libtest_rc=$?
libtest_end=$(date +%s)
libtest_secs=$((libtest_end - libtest_start))
echo "exit=$libtest_rc  wall=${libtest_secs}s"
grep -E '^test result:' /tmp/libtest-run.log | awk '{p+=$4} END {print "tests passed (summed):", p}'

echo
echo "=== comparison ==="
printf 'nextest: %ss\nlibtest: %ss\n' "$nextest_secs" "$libtest_secs"
python3 - "$nextest_secs" "$libtest_secs" <<'PY'
import sys
n, l = int(sys.argv[1]), int(sys.argv[2])
if n and l:
    print(f"nextest is {l/n:.2f}x vs libtest ({l-n:+}s difference)")
PY
