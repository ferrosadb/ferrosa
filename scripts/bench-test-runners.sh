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
      --skip count_range_metadata_merger_dedups_real_typed_edges_sstables)

echo "=== warming the build (shared target dir) ==="
cargo build --all-features --workspace --lib --tests \
  --exclude ferrosa-jepsen --exclude ferrosa-loadgen >/dev/null 2>&1
echo "warm."

# ci.yml builds the asc bundle before testing and points FERROSA_ASC_BUNDLE at it.
# Without it, `route_create_function_assemblyscript_compiles_and_runs` panics ON
# PURPOSE ("FERROSA_ASC_BUNDLE is not set"), which aborts the whole ferrosa-cql
# libtest binary and truncates that run's numbers. A benchmark that skips CI's
# setup steps is not measuring CI.
echo
echo "=== building the asc bundle (ci.yml does this before testing) ==="
if [ -f ferrosa-udf/examples/asc-poc/build-bundle.sh ]; then
  if bash ferrosa-udf/examples/asc-poc/build-bundle.sh /tmp/asc-host/asc-bundle.mjs >/tmp/asc-build.log 2>&1; then
    export FERROSA_ASC_BUNDLE=/tmp/asc-host/asc-bundle.mjs
    echo "ok: FERROSA_ASC_BUNDLE=$FERROSA_ASC_BUNDLE"
  else
    echo "WARNING: asc bundle build failed; ferrosa-cql will abort early."
    echo "         Last lines of /tmp/asc-build.log:"
    tail -3 /tmp/asc-build.log
  fi
else
  echo "WARNING: build-bundle.sh not found; ferrosa-cql will abort early."
fi

echo
echo "=== cargo nextest run (parallel) ==="
nextest_start=$(date +%s)
# NOTE: the skip list must come AFTER `--`. Those are libtest test-NAME
# filters, not nextest flags; before `--` nextest exits 2 with
# "unexpected argument '--skip' found" and runs NOTHING, which silently
# turns this benchmark into a 0-second nextest measurement.
cargo nextest run "${SELECT[@]}" -- "${SKIP[@]}" >/tmp/nextest-run.log 2>&1
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
printf 'nextest: exit=%s wall=%ss\llibtest: exit=%s wall=%ss\n' \
  "$nextest_rc" "$nextest_secs" "$libtest_rc" "$libtest_secs"
python3 - "$nextest_secs" "$libtest_secs" "$nextest_rc" "$libtest_rc" <<'PY'
import sys
n, l = int(sys.argv[1]), int(sys.argv[2])
nrc, lrc = int(sys.argv[3]), int(sys.argv[4])
bad = []
if n < 5:
    bad.append(f"nextest wall={n}s — too short to be real (check /tmp/nextest-run.log)")
if l < 5:
    bad.append(f"libtest wall={l}s — too short to be real (check /tmp/libtest-run.log)")
if nrc != 0:
    bad.append(f"nextest exited {nrc} (non-zero: some tests failed or args were rejected)")
if lrc != 0:
    bad.append(f"libtest exited {lrc} (non-zero: a test FAILED, which aborts that binary "
               f"and TRUNCATES its wall clock — the number below is not the full suite)")
if bad:
    print("\n!! MEASUREMENT NOT TRUSTWORTHY — do not report a ratio from this run:")
    for b in bad:
        print(f"   - {b}")
    print("\n   A benchmark that measures a partial or failed run is worse than none.")
else:
    if n and l:
        print(f"nextest is {n/l:.2f}x the speed of libtest ({l-n:+}s difference)")
PY
