#!/usr/bin/env bash
# Assess one arm's collected evidence and decide PASS/FAIL against the
# expectation. Fail-loud: no data => FAIL (never a vacuous pass).
#
#   assess.sh <arm-name> <expect: green|repro> [--i-will-pay]
#
# Reads scrape.node*.csv (epoch,raft_term,is_leader,storm_jumps,headroom_cores,
# readyz) and logs.node*.txt. Skips the first 2 baseline rows per node.
#
#   green  (post-fix): every node stayed /readyz ready, storm-jumps == 0, raft
#          term drift <= GATE_TERM_DRIFT_MAX, headroom >= GATE_HEADROOM_MIN, and
#          NO step-down log lines.
#   repro  (pre-fix):  a step-down WAS observed — any node went notready during
#          the storm, or a step-down log substring appeared. (Pre-fix images lack
#          the ferrosa_raft_* metrics, so detection is readiness + log based.)
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ARM="${1:?arm name required}"; EXPECT="${2:?expect required (green|repro)}"; shift 2 || true
# shellcheck source=lib.sh
source "${HERE}/lib.sh" "$@"

ARM_OUT="${OUT_DIR}/${ARM}"
REPORT="${ARM_OUT}/VERDICT.txt"
mkdir -p "${ARM_OUT}"

# Summarize one node CSV: "rows notready tmin tmax tcount smax hmin hcount".
summarize_csv() {
  awk -F, '
    NR==1 { next }                                  # header
    { dr++ }                                         # data-row index
    dr<=2 { next }                                   # skip 2 baseline rows
    {
      rows++
      if ($6=="notready") nr++
      if ($2!="") { if (tc==0||$2<tmin) tmin=$2; if (tc==0||$2>tmax) tmax=$2; tc++ }
      if ($4!="") { if ($4>smax) smax=$4 }
      if ($5!="") { if (hc==0||$5<hmin) hmin=$5; hc++ }
    }
    END { printf "%d %d %s %s %d %d %s %d\n",
            rows+0, nr+0, (tc?tmin:"-"), (tc?tmax:"-"), tc+0, smax+0, (hc?hmin:"-"), hc+0 }
  ' "$1"
}

{
  echo "=== scheduler-B0 regression verdict: arm=${ARM} expect=${EXPECT} ==="
  echo "app=${FLY_APP} scan_concurrency=${SCAN_CONCURRENCY} storm_secs=${STORM_SECS}"
  echo
} > "${REPORT}"

total_rows=0
any_notready=0
max_storm=0
worst_term_drift=0
min_headroom=""
metrics_present=0
log_hits=0

for i in $(seq 0 $((NODE_COUNT - 1))); do
  csv="${ARM_OUT}/scrape.node${i}.csv"
  if [ ! -s "${csv}" ]; then
    echo "node${i}: NO CSV DATA (${csv} missing/empty)" | tee -a "${REPORT}" >&2
    continue
  fi
  read -r rows nr tmin tmax tc smax hmin hc <<<"$(summarize_csv "${csv}")"
  total_rows=$(( total_rows + rows ))
  [ "${nr}" -gt 0 ] && any_notready=1
  [ "${smax}" -gt "${max_storm}" ] && max_storm="${smax}"
  if [ "${tc}" -gt 0 ]; then
    metrics_present=1
    drift=$(( tmax - tmin ))
    [ "${drift}" -gt "${worst_term_drift}" ] && worst_term_drift="${drift}"
  fi
  if [ "${hc}" -gt 0 ]; then
    metrics_present=1
    if [ -z "${min_headroom}" ] || [ "${hmin}" -lt "${min_headroom}" ]; then min_headroom="${hmin}"; fi
  fi
  printf 'node%d: rows=%d notready=%d term=[%s..%s](n=%s) storm_jumps_max=%s headroom_min=%s(n=%s)\n' \
    "${i}" "${rows}" "${nr}" "${tmin}" "${tmax}" "${tc}" "${smax}" "${hmin}" "${hc}" | tee -a "${REPORT}"

  logf="${ARM_OUT}/logs.node${i}.txt"
  if [ -s "${logf}" ]; then
    hits="$(grep -Ec "${STEPDOWN_LOG_PATTERNS}" "${logf}" 2>/dev/null || true)"
    hits="${hits:-0}"
    [ "${hits}" -gt 0 ] && { log_hits=$(( log_hits + hits )); \
      printf '  log step-down matches: %d\n' "${hits}" | tee -a "${REPORT}"; }
  fi
done

{
  echo
  echo "aggregate: total_storm_rows=${total_rows} any_notready=${any_notready} \
storm_jumps_max=${max_storm} worst_term_drift=${worst_term_drift} \
min_headroom=${min_headroom:--} metrics_present=${metrics_present} log_stepdown_hits=${log_hits}"
} | tee -a "${REPORT}"

# ── Decide ────────────────────────────────────────────────────────────────────
verdict="FAIL"; reason=""
if [ "${total_rows}" -eq 0 ]; then
  reason="no storm-window samples collected (harness failure — not a vacuous pass)"
elif [ "${EXPECT}" = "repro" ]; then
  # Non-vacuity arm: a step-down must have been observed.
  if [ "${any_notready}" -eq 1 ] || [ "${log_hits}" -gt 0 ]; then
    verdict="PASS"; reason="step-down reproduced (notready=${any_notready}, log_hits=${log_hits})"
  else
    reason="expected a pre-fix step-down but saw none — workload too weak or box too fast"
  fi
elif [ "${EXPECT}" = "green" ]; then
  ok=1; why=""
  [ "${metrics_present}" -eq 1 ] || { ok=0; why="${why} ferrosa_raft/sched metrics absent (post-fix image not deployed?);"; }
  [ "${any_notready}" -eq 0 ]     || { ok=0; why="${why} leader dropped (readyz notready);"; }
  [ "${max_storm}" -eq 0 ]        || { ok=0; why="${why} storm_jumps=${max_storm} (>0);"; }
  [ "${worst_term_drift}" -le "${GATE_TERM_DRIFT_MAX}" ] || { ok=0; why="${why} term drift ${worst_term_drift}>${GATE_TERM_DRIFT_MAX};"; }
  if [ -n "${min_headroom}" ]; then
    [ "${min_headroom}" -ge "${GATE_HEADROOM_MIN}" ] || { ok=0; why="${why} headroom ${min_headroom}<${GATE_HEADROOM_MIN};"; }
  else
    ok=0; why="${why} no headroom samples;"
  fi
  [ "${log_hits}" -eq 0 ]         || { ok=0; why="${why} step-down log lines=${log_hits};"; }
  if [ "${ok}" -eq 1 ]; then
    verdict="PASS"; reason="no step-down: leader stable, storm_jumps=0, term stable, headroom>=${GATE_HEADROOM_MIN}"
  else
    reason="green expectation violated:${why}"
  fi
else
  reason="unknown expectation '${EXPECT}'"
fi

echo "VERDICT[${ARM}]: ${verdict} — ${reason}" | tee -a "${REPORT}" >&2
[ "${verdict}" = "PASS" ]
