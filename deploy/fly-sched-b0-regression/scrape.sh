#!/bin/sh
# Per-node metric + readiness sampler for the scheduler-B0 regression.
#
# Emits one CSV row per interval capturing the T0.6 exit-gate signals:
#   - ferrosa_raft_election_storm_term_jumps_total  (post-fix only; MUST stay 0)
#   - ferrosa_sched_consensus_headroom_cores        (post-fix only; MUST stay >0)
#   - readyz  (leader present/absent — works on BOTH builds; a drop to
#             `notready` is the step-down signal, and the only one the pre-fix
#             image emits since it predates the ferrosa_raft_* / ferrosa_sched_*
#             metrics)
#
# Missing metrics render as empty fields (a pre-fix node has neither the storm
# counter nor headroom), never as a fake 0 — the assessor tells absent from zero.
#
# Usage: scrape.sh <out_csv> <interval_secs> <duration_secs>
set -eu

OUT="${1:?out csv path required}"
INTERVAL="${2:-2}"
DURATION="${3:-300}"
METRICS_URL="http://localhost:9090/metrics"
READY_URL="http://localhost:9090/readyz"

metric() { awk -v k="$1" '$1==k {print $2; found=1} END{if(!found)print ""}'; }

echo "epoch,storm_jumps,headroom_cores,readyz" > "${OUT}"

end=$(( $(date +%s) + DURATION ))
while [ "$(date +%s)" -lt "${end}" ]; do
  now="$(date +%s)"
  body="$(curl -s --max-time 3 "${METRICS_URL}" 2>/dev/null || true)"
  storms="$(printf '%s\n' "${body}" | metric ferrosa_raft_election_storm_term_jumps_total)"
  headroom="$(printf '%s\n' "${body}" | metric ferrosa_sched_consensus_headroom_cores)"
  if curl -sf --max-time 3 "${READY_URL}" 2>/dev/null | grep -q '"ready":true'; then
    ready=ready
  else
    ready=notready
  fi
  printf '%s,%s,%s,%s\n' "${now}" "${storms}" "${headroom}" "${ready}" >> "${OUT}"
  sleep "${INTERVAL}"
done
