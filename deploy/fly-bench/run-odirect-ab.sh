#!/usr/bin/env bash
# O_DIRECT A/B for Phase 3 (epic t_29f6b948, task t_d1af57de).
#
# Same image (this worktree HEAD, which carries DirectWriter + the runtime-stall
# detector), two arms differing ONLY by FERROSA_SSTABLE_DIRECT_IO:
#   - buffered arm (=0): Data.db written through the page cache (today's default)
#   - direct   arm (=1): Data.db written O_DIRECT (page-cache bypass)
# on the fly-bench infra (ext4 volumes at /var/lib/ferrosa + performance CPUs —
# the O_DIRECT-valid substrate; the shared-cpu regression box is NOT, its rootfs
# is overlay and its CPU throttle would swamp the disk signal).
#
# Workload: cql_iot_append (write-heavy) at t128 — sustained memtable flushes at
# the 64 MiB threshold + compaction, the page-cache flooder O_DIRECT targets.
#
# We compare, per arm:
#   1. ferrosa_sstable_direct_write_fallbacks_total — MUST be 0 on the direct arm
#      (else the volume fs rejected O_DIRECT and the arm is invalid), and the
#      _files_total must be > 0 (the writer actually ran).
#   2. ferrosa_sched_runtime_stall_{events_total,max_micros} — the freeze signal;
#      expect it materially lower on the direct arm.
#   3. nosqlbench write/read latency p99/p100 from the result bundle.
#
# Dry-run by default (prints the plan, no billing). Pass --i-will-pay to execute.
#
#   run-odirect-ab.sh [--i-will-pay]
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH="${HERE}/fly-lax-benchmark.sh"

PAY=0
[ "${1:-}" = "--i-will-pay" ] && PAY=1

# One shared build for both arms (WORKTREE = this branch HEAD, not origin/main).
BUILD_RUN_ID="${BUILD_RUN_ID:-odirect-ab-$(git -C "${HERE}/../.." rev-parse --short HEAD)}"
IMAGE_TAG="bench-${BUILD_RUN_ID}"

# Keep the run economical + the disk signal clean: no Cassandra arm, no perf
# profiling, no memory snapshots (they add load/noise). A single write-heavy
# stage per arm, not the full 5-stage ramp.
export ORG="${ORG:-ferrosa}"
export REGION="${REGION:-lax}"
export BENCH_GIT_REF=WORKTREE
export FERROSA_IMAGE_TAG="${IMAGE_TAG}"
export FERROSA_USE_VOLUMES=true
export FERROSA_CPU_KIND="${FERROSA_CPU_KIND:-performance}"
export FERROSA_CPUS="${FERROSA_CPUS:-2}"
export FERROSA_MEMORY_MB="${FERROSA_MEMORY_MB:-4096}"
export FERROSA_VOLUME_GB="${FERROSA_VOLUME_GB:-5}"
export PROFILE_FERROSA=false
export FERROSA_MEMORY_SNAPSHOTS=false
export RAMP_WORKLOAD="${RAMP_WORKLOAD:-/usr/local/share/nosqlbench/cql_iot_append.yaml}"

RESULTS_ROOT="${HERE}/../../target/fly-odirect-ab"
mkdir -p "${RESULTS_ROOT}"

log() { printf '\033[36m[odirect-ab %s]\033[0m %s\n' "$(date -u +%H:%M:%S)" "$*"; }
run() {
  if [ "${PAY}" -eq 1 ]; then
    log "RUN: BENCH $*"; "${BENCH}" "$@"
  else
    log "DRY: BENCH $* (env FERROSA_SSTABLE_DIRECT_IO=${FERROSA_SSTABLE_DIRECT_IO:-0} RUN_ID=${RUN_ID:-unset})"
  fi
}

log "build tag=${IMAGE_TAG} region=${REGION} cpus=${FERROSA_CPUS}x${FERROSA_CPU_KIND} mem=${FERROSA_MEMORY_MB} vol=${FERROSA_VOLUME_GB}G"
log "workload=${RAMP_WORKLOAD} (write-heavy, t128)"

# ── 0. Preflight + one shared image build (the ~20 min pole) ──────────────────
RUN_ID="${BUILD_RUN_ID}" run preflight
RUN_ID="${BUILD_RUN_ID}" run build-images
# Bench driver node (separate app), built + created once, reused by both arms.
RUN_ID="${BUILD_RUN_ID}" run create-bench

run_arm() {  # $1 = arm name, $2 = FERROSA_SSTABLE_DIRECT_IO value
  local arm="$1" direct="$2"
  local run_id="${BUILD_RUN_ID}-${arm}"
  log "════════ ARM ${arm} (FERROSA_SSTABLE_DIRECT_IO=${direct}) ════════"
  # Fresh volumes + nodes each arm so compaction state is comparable and S3
  # (prefix keyed by RUN_ID) does not carry data across arms.
  RUN_ID="${run_id}" FERROSA_SSTABLE_DIRECT_IO="${direct}" run recreate-ferrosa
  RUN_ID="${run_id}" FERROSA_SSTABLE_DIRECT_IO="${direct}" run run-ferrosa-t128
  RUN_ID="${run_id}" FERROSA_SSTABLE_DIRECT_IO="${direct}" run teardown-ferrosa
  RUN_ID="${run_id}" FERROSA_SSTABLE_DIRECT_IO="${direct}" run teardown-ferrosa-volumes
}

# ── 1. Buffered arm first (baseline = today's default) ────────────────────────
run_arm buffered 0
# ── 2. Direct arm (the change under test) ─────────────────────────────────────
run_arm direct 1

# ── 3. Extract the comparison from the collected node /metrics + nb bundles ───
if [ "${PAY}" -eq 1 ]; then
  log "extracting comparison → ${RESULTS_ROOT}/COMPARISON.txt"
  "${HERE}/odirect-compare.sh" "${BUILD_RUN_ID}" | tee "${RESULTS_ROOT}/COMPARISON.txt"
else
  log "DRY-RUN complete — no machines created, no billing. Re-run with --i-will-pay to execute."
fi
