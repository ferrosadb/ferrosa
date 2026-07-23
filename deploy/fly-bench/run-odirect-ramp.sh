#!/usr/bin/env bash
# O_DIRECT load-RAMP + concurrent full-table-SCAN A/B (Phase 3, t_b37afe53).
#
# Instead of jumping to saturation (which collapses the cluster into a chaotic,
# placement-dependent state — see the earlier saturation runs), this RAMPS the
# offered write rate through stages and measures achieved throughput + write
# latency at each, so we can see the KNEE of the performance curve — where the
# cluster stops keeping up. Comparing the two arms' whole CURVES is far more
# robust to noisy-neighbor placement than comparing a single saturation point.
#
# Concurrently, a scanner runs full-table ALLOW FILTERING scans (ferrosa-loadgen
# --scan-storm) for the whole ramp, so we also measure reads-under-write-load —
# O_DIRECT's plausible real benefit is keeping the OS page cache free of write
# data so scan reads stay cached.
#
# Per arm (buffered = FERROSA_SSTABLE_DIRECT_IO=0, direct = 1):
#   1. fresh 3-node cluster + a scanner machine (ferrosa image w/ ferrosa-loadgen)
#   2. seed data (nb rampup, rate-limited)
#   3. start the scan-storm for the whole ramp window
#   4. ramp offered write rate through STAGES; per stage capture achieved
#      throughput + write p50/p99/p100 (nb --report-csv-to) + node metrics
#   5. collect scan stats; teardown
# Then ramp-analyze.sh prints both curves + the knee + scan latency.
#
# Dry-run by default; pass --i-will-pay to execute.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH="${HERE}/fly-lax-benchmark.sh"
ROOT="${HERE}/../.."

PAY=0; [ "${1:-}" = "--i-will-pay" ] && PAY=1

# Reuse the ramp images built under this tag (ferrosa w/ loadgen + nb w/ cyclerate).
export BUILD_RUN_ID="${BUILD_RUN_ID:-odirect-ramp}"
export FERROSA_IMAGE_TAG="bench-${BUILD_RUN_ID}"
export ORG="${ORG:-ferrosa}" REGION="${REGION:-lax}"
export BENCH_GIT_REF=WORKTREE
export FERROSA_USE_VOLUMES=true FERROSA_CPU_KIND=performance FERROSA_CPUS=2
export FERROSA_MEMORY_MB=4096 FERROSA_VOLUME_GB=12
# Small block cache so the working set exceeds it → scan reads depend on the OS
# page cache, which is what write I/O pollutes (the cache-preservation test).
export FERROSA_CACHE_MAX_BYTES="${FERROSA_CACHE_MAX_BYTES:-67108864}"
export PROFILE_FERROSA=false FERROSA_MEMORY_SNAPSHOTS=false
export FERROSA_APP="${FERROSA_APP:-ferrosa-lax}" BENCH_APP="${BENCH_APP:-ferrosa-bench-lax}"

# Offered write-rate stages (ops/sec) + per-stage measure window. Geometric ramp
# brackets the knee; earlier saturation runs suggest the cluster tops out at a
# few k ops/s, so start at 1k. Override with RAMP_STAGES.
STAGES=(${RAMP_STAGES:-1000 2000 4000 8000 16000 32000})
STAGE_SECS="${STAGE_SECS:-75}"
SEED_CYCLES="${SEED_CYCLES:-500000}"
SCAN_CONCURRENCY="${SCAN_CONCURRENCY:-8}"
THREADS="${THREADS:-256}"     # enough workers to offer the top stage rate
CONTACT_PORT=9042

OUT="${ROOT}/target/fly-odirect-ramp"; mkdir -p "$OUT"
log(){ printf '\033[35m[ramp %s]\033[0m %s\n' "$(date -u +%H:%M:%S)" "$*"; }
fly(){ if [ "$PAY" -eq 1 ]; then "$@"; else echo "DRY: $*"; fi; }

mid(){ flyctl machines list --app "$1" --json 2>/dev/null | jq -r ".[]|select(.name==\"$2\")|.id"; }
node_dns(){ flyctl machines list --app "$FERROSA_APP" --json 2>/dev/null \
  | jq -r "[.[]|select(.name|startswith(\"ferrosa-\"))|.id+\".vm.${FERROSA_APP}.internal:${CONTACT_PORT}\"]|join(\",\")"; }

# Run an nb command on the bench node (blocking).
nb_on_bench(){ # $1 = extra nb args string
  local bid; bid="$(mid "$BENCH_APP" nosqlbench-1)"
  flyctl ssh console --app "$BENCH_APP" --machine "$bid" --command \
    "sh -lc \"nb5 /usr/local/share/nosqlbench/cql_iot_append.yaml default hosts=$(node_dns) localdc=datacenter1 threads=${THREADS} rf=3 read_cl=LOCAL_ONE write_cl=LOCAL_QUORUM errors=count driver.advanced.protocol.compression=lz4 $1\"" </dev/null
}

# Reads-under-load via a SECOND, detached nosqlbench activity on the bench node
# (scenario `scanonly` = full-table ALLOW FILTERING scans of the written table).
# Right table (baselines.iot), no scanner machine, no ferrosa-loadgen dependency.
start_scan(){ # $1 = arm
  local bid; bid="$(mid "$BENCH_APP" nosqlbench-1)"
  fly flyctl ssh console --app "$BENCH_APP" --machine "$bid" --command \
    "sh -lc 'nohup nb5 /usr/local/share/nosqlbench/cql_iot_append.yaml scanonly hosts=$(node_dns) localdc=datacenter1 scan_threads=${SCAN_CONCURRENCY} scan-cycles=100000000 errors=count driver.advanced.protocol.compression=lz4 --report-csv-to /results/scan-$1 > /tmp/scan-$1.log 2>&1 & echo scan started'" </dev/null || true
}
stop_scan(){ local bid; bid="$(mid "$BENCH_APP" nosqlbench-1)"
  flyctl ssh console --app "$BENCH_APP" --machine "$bid" --command "sh -lc 'pkill -f scanonly 2>/dev/null || true'" </dev/null 2>/dev/null || true; }
collect_scan(){ # $1 arm, $2 dir — the scan activity's full-table read latency
  local bid; bid="$(mid "$BENCH_APP" nosqlbench-1)"
  flyctl ssh console --app "$BENCH_APP" --machine "$bid" --command \
    "sh -lc 'echo count,csvrow; cat /results/scan-$1/*cycles_servicetime*.csv 2>/dev/null | tail -1'" </dev/null 2>/dev/null > "${2}/scan.servicetime.csvrow" || true
}
snapshot_nodes(){ local tag="$1" dir="$2"; mkdir -p "$dir"
  flyctl machines list --app "$FERROSA_APP" --json 2>/dev/null | jq -r '.[]|select(.name|startswith("ferrosa-"))|.id+" "+.name' \
  | while read -r id name; do
      flyctl ssh console --app "$FERROSA_APP" --machine "$id" --command \
        "sh -lc 'curl -sg http://[::1]:9090/metrics'" </dev/null 2>/dev/null > "${dir}/${name}.${tag}.metrics" || true
    done
}

run_arm(){ # $1 arm, $2 direct(0/1)
  local arm="$1" direct="$2"
  local adir="${OUT}/${arm}"; mkdir -p "$adir"
  log "════════ ARM ${arm} (DIRECT_IO=${direct}) ════════"
  RUN_ID="${BUILD_RUN_ID}-${arm}" FERROSA_SSTABLE_DIRECT_IO="${direct}" fly "$BENCH" recreate-ferrosa
  if [ "$PAY" -eq 1 ]; then
    log "seeding ${SEED_CYCLES} rows"; nb_on_bench "rampup-cycles=${SEED_CYCLES} main-cycles=0 main_cyclerate=20000" || true
    snapshot_nodes preseed "$adir"
    log "starting concurrent full-table scan load (${SCAN_CONCURRENCY} threads) across the ramp"
    start_scan "$arm"
    for X in "${STAGES[@]}"; do
      log "ramp stage: offered ${X} ops/s for ${STAGE_SECS}s"
      nb_on_bench "rampup-cycles=0 main-cycles=$(( X * STAGE_SECS )) main_cyclerate=${X} --report-csv-to /results/ramp-${arm}-${X}" \
        > "${adir}/stage-${X}.log" 2>&1 || true
      # pull this stage's servicetime CSV + a node metric snapshot
      local bid; bid="$(mid "$BENCH_APP" nosqlbench-1)"
      flyctl ssh console --app "$BENCH_APP" --machine "$bid" --command \
        "sh -lc 'cat /results/ramp-${arm}-${X}/*cycles_servicetime*.csv 2>/dev/null | tail -1'" </dev/null 2>/dev/null \
        > "${adir}/stage-${X}.servicetime.csvrow" || true
      snapshot_nodes "stage-${X}" "$adir"
    done
    stop_scan; collect_scan "$arm" "$adir"
    RUN_ID="${BUILD_RUN_ID}-${arm}" fly "$BENCH" teardown-ferrosa
    RUN_ID="${BUILD_RUN_ID}-${arm}" fly "$BENCH" teardown-ferrosa-volumes
  fi
}

log "app=${FERROSA_APP} tag=${FERROSA_IMAGE_TAG} stages=[${STAGES[*]}] stage_secs=${STAGE_SECS} scan_conc=${SCAN_CONCURRENCY}"
RUN_ID="${BUILD_RUN_ID}" fly "$BENCH" create-bench
run_arm buffered 0
run_arm direct 1
# tear the shared bench driver down (not arm-scoped)
if [ "$PAY" -eq 1 ]; then bid="$(mid "$BENCH_APP" nosqlbench-1)"; [ -n "$bid" ] && flyctl machine destroy "$bid" --app "$BENCH_APP" --force || true
  "${HERE}/ramp-analyze.sh" | tee "${OUT}/RAMP-COMPARISON.txt"
else log "DRY-RUN complete — pass --i-will-pay to execute."; fi
