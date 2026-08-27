#!/usr/bin/env bash
# pre-push-mirror-ci.sh — Run the same `cargo test` invocation CI runs.
#
# Mirrors the "Run tests" step in .github/workflows/ci.yml so that
# anything pushed has already passed locally what CI is about to
# re-run. Specifically:
#
#   cargo test --all-features --workspace --lib --tests \
#       --exclude ferrosa-jepsen --exclude ferrosa-loadgen \
#       -- --skip <infrastructure-gated-test> ...
#
# The skipped tests run in the CI integration job under
# FERROSA_TEST_CONTAINERS=1 with the cluster already up. Locally,
# that's gated behind FERROSA_PRE_PUSH_INTEGRATION=1 in
# .pre-commit-config.yaml.
#
# Speed: ~30-50% faster via cargo-nextest when installed (parallel
# by binary AND by test). Falls back to `cargo test` otherwise.
#
# Bypass: FERROSA_SKIP_PREPUSH=1 git push  (or git push --no-verify)

set -euo pipefail

# Bypass when explicitly requested. Use sparingly — the whole point of
# this hook is "don't push something that will fail CI".
if [ "${FERROSA_SKIP_PREPUSH:-0}" = "1" ]; then
  echo "FERROSA_SKIP_PREPUSH=1 — skipping pre-push test gate"
  exit 0
fi

# Skip list — must match `.github/workflows/ci.yml` step "Run tests".
# Tests gated by FERROSA_TEST_CONTAINERS / FERROSA_TEST_FIRECRACKER /
# FERROSA_TEST_CLUSTER_NODES per CLAUDE.md test policy.
SKIPS=(
  --skip accord::perf_regression
  --skip batch_atomicity
  --skip pause_resume
  --skip recovery_coordinator
  --skip cassandra_reads_compacted
  --skip compaction_end_to_end_pipeline
  --skip dep_wait_ordering
  --skip disk_fail_no_phantom
  --skip lwt_batch_atomicity_all
  --skip clock_skew_large_preaccept
  --skip binary_
  --skip concurrent_write
  --skip many_flushes
  --skip flush_2000
  --skip single_writer
  --skip write_flush_compact
  --skip reads_never_panic_on_arbitrary_content
  --skip corrupt_sstable_bytes_never_panic_never_oom
  --skip peak_resident_readers_within_cap
  --skip streaming_equals_single_pass
  --skip bounded_fetch_reassembles_to_single_pass
  --skip read_merge_is_lww_no_data_loss
  --skip digest_walk_is_deterministic
  --skip differential_oracle
  --skip fly_multi_node_streaming_scan
  --skip real_typed_edges_paged_scan_delivers_every_distinct_row
  --skip count_range_metadata_merger_dedups_real_typed_edges_sstables
)

# `cargo nextest run` hangs on this workspace's lib targets on macOS
# arm64 (no progress for 30+ min after `Finished` compile). cargo
# test does not exhibit the hang. See task #15. Until the nextest
# hang is fixed, force the cargo-test path.
#
# Use the default thread pool (one per core) — CI does the same. The
# previous `--test-threads=1` here turned a ~10–15 min mirror into a
# ~3 h sequential run with no documented justification, and CI itself
# never passed that flag (see .github/workflows/ci.yml).
echo "=== cargo test --workspace --lib --tests (mirrors CI) ==="
exec cargo test \
  --all-features \
  --workspace --lib --tests \
  --exclude ferrosa-jepsen --exclude ferrosa-loadgen \
  -- "${SKIPS[@]}"
