#!/usr/bin/env bash
# Assess one arm's collected evidence and decide PASS/FAIL against the
# expectation. Fail-loud: no data => FAIL (never a vacuous pass).
#
#   assess.sh <arm-name> <expect: green|repro> [--i-will-pay]
#
# Reads scrape.node*.csv (epoch,storm_jumps,headroom_cores,readyz) and
# logs.node*.txt. Skips the first 2 baseline rows per node.
#
#   green  (post-fix): every node stayed /readyz ready, storm-jumps == 0, NO
#          step-down log lines, and the post-fix metrics were actually present.
#          NB: headroom is NOT gated — on shared-cpu boxes available_parallelism
#          clamps to 1 so headroom sits at 0 during a scan; the load-bearing
#          proof is the differential vs the pre-fix arm, not an absolute headroom.
#   repro  (pre-fix):  a step-down WAS observed — any node went notready during
#          the storm, or a step-down log substring appeared. (Pre-fix images lack
#          the ferrosa_* metrics, so detection is readiness + log based.) This arm
#          is the non-vacuity proof that the workload can starve an unbounded build.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ARM="${1:?arm name required}"; EXPECT="${2:?expect required (green|repro)}"; shift 2 || true
# shellcheck source=lib.sh
source "${HERE}/lib.sh" "$@"

ARM_OUT="${OUT_DIR}/${ARM}"
REPORT="${ARM_OUT}/VERDICT.txt"
mkdir -p "${ARM_OUT}"

# Summarize one node CSV: "rows notready smax hmin hcount". Column indices are
# resolved from the HEADER by name, so this works across both image layouts
# (the pre-fix image predates the storm/headroom columns; the post-fix image
# emits epoch,storm_jumps,headroom_cores,readyz).
summarize_csv() {
  awk -F, '
    NR==1 {
      for (i = 1; i <= NF; i++) {
        if ($i == "storm_jumps")    cs = i
        if ($i == "headroom_cores") ch = i
        if ($i == "readyz")         cr = i
      }
      next
    }
    { dr++ }                                         # data-row index
    dr<=2 { next }                                   # skip 2 baseline rows
    {
      rows++
      if (cr && $cr == "notready") nr++
      if (cs && $cs != "") { if ($cs > smax) smax = $cs }
      if (ch && $ch != "") { if (hc == 0 || $ch < hmin) hmin = $ch; hc++ }
    }
    END { printf "%d %d %d %s %d\n", rows+0, nr+0, smax+0, (hc?hmin:"-"), hc+0 }
  ' "$1"
}

# scans_completed reported by the scan-storm driver (non-vacuity of the workload).
scans_completed() {
  local f="${ARM_OUT}/scan-storm.stats.txt"
  [ -s "${f}" ] || { printf '0'; return; }
  grep -oE '[0-9]+ scans' "${f}" | grep -oE '^[0-9]+' | tail -n1 | grep -E '^[0-9]+$' || printf '0'
}

{
  echo "=== scheduler-B0 regression verdict: arm=${ARM} expect=${EXPECT} ==="
  echo "app=${FLY_APP} scan_concurrency=${SCAN_CONCURRENCY} storm_secs=${STORM_SECS} vm=${VM_CPUS}"
  echo
} > "${REPORT}"

total_rows=0; any_notready=0; max_storm=0; min_headroom=""; metrics_present=0; log_hits=0

for i in $(seq 0 $((NODE_COUNT - 1))); do
  csv="${ARM_OUT}/scrape.node${i}.csv"
  if [ ! -s "${csv}" ]; then
    echo "node${i}: NO CSV DATA (${csv} missing/empty)" | tee -a "${REPORT}" >&2
    continue
  fi
  read -r rows nr smax hmin hc <<<"$(summarize_csv "${csv}")"
  total_rows=$(( total_rows + rows ))
  [ "${nr}" -gt 0 ] && any_notready=1
  [ "${smax}" -gt "${max_storm}" ] && max_storm="${smax}"
  if [ "${hc}" -gt 0 ]; then
    metrics_present=1
    if [ -z "${min_headroom}" ] || [ "${hmin}" -lt "${min_headroom}" ]; then min_headroom="${hmin}"; fi
  fi
  printf 'node%d: rows=%d notready=%d storm_jumps_max=%d headroom_min=%s(n=%s)\n' \
    "${i}" "${rows}" "${nr}" "${smax}" "${hmin}" "${hc}" | tee -a "${REPORT}"

  logf="${ARM_OUT}/logs.node${i}.txt"
  if [ -s "${logf}" ]; then
    hits="$(grep -Ec "${STEPDOWN_LOG_PATTERNS}" "${logf}" 2>/dev/null || true)"; hits="${hits:-0}"
    [ "${hits}" -gt 0 ] && { log_hits=$(( log_hits + hits )); \
      printf '  log step-down matches: %d\n' "${hits}" | tee -a "${REPORT}"; }
  fi
done

scans="$(scans_completed)"; scans="${scans:-0}"
{
  echo
  echo "aggregate: total_storm_rows=${total_rows} any_notready=${any_notready} \
storm_jumps_max=${max_storm} min_headroom=${min_headroom:--} metrics_present=${metrics_present} \
log_stepdown_hits=${log_hits} scans_completed=${scans}"
} | tee -a "${REPORT}"

verdict="FAIL"; reason=""
if [ "${total_rows}" -eq 0 ]; then
  reason="no storm-window samples collected (harness failure — not a vacuous pass)"
elif [ "${EXPECT}" = "repro" ]; then
  if [ "${any_notready}" -eq 1 ] || [ "${log_hits}" -gt 0 ]; then
    verdict="PASS"; reason="step-down reproduced (notready=${any_notready}, log_hits=${log_hits})"
  else
    reason="expected a pre-fix step-down but saw none — workload too weak or box too fast"
  fi
elif [ "${EXPECT}" = "green" ]; then
  # Green = no step-down. headroom is reported but NOT gated (see header note);
  # non-vacuity comes from the pre-fix arm reproducing on the same box+workload.
  ok=1; why=""
  [ "${metrics_present}" -eq 1 ] || { ok=0; why="${why} ferrosa_sched/raft metrics absent (post-fix image not deployed?);"; }
  [ "${any_notready}" -eq 0 ]     || { ok=0; why="${why} leader dropped (readyz notready);"; }
  [ "${max_storm}" -eq 0 ]        || { ok=0; why="${why} storm_jumps=${max_storm} (>0);"; }
  [ "${log_hits}" -eq 0 ]         || { ok=0; why="${why} step-down log lines=${log_hits};"; }
  if [ "${ok}" -eq 1 ]; then
    verdict="PASS"; reason="no step-down: leader stayed ready, storm_jumps=0, no step-down logs (headroom_min=${min_headroom:--}, scans=${scans}, informational)"
  else
    reason="green expectation violated:${why}"
  fi
else
  reason="unknown expectation '${EXPECT}'"
fi

echo "VERDICT[${ARM}]: ${verdict} — ${reason}" | tee -a "${REPORT}" >&2
[ "${verdict}" = "PASS" ]
