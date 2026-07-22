#!/bin/sh
# Per-node metric + readiness sampler for the scheduler-B0 regression.
#
# Emits one CSV row per interval capturing exactly the T0.6 exit-gate signals:
#   - ferrosa_raft_current_term                     (post-fix only; blank pre-fix)
#   - ferrosa_raft_is_leader                        (post-fix only)
#   - ferrosa_raft_election_storm_term_jumps_total  (post-fix only; MUST stay 0)
#   - ferrosa_sched_consensus_headroom_cores        (post-fix only; MUST stay >0)
#   - readyz  (leader present/absent — works on BOTH builds; the pre-fix
#             step-down signal, since pre-fix lacks the ferrosa_raft_* metrics)
#
# Missing metrics render as empty fields (a pre-fix node has no ferrosa_raft_*),
# never as a fake 0 — the assessor distinguishes "absent" from "zero".
#
# Usage: scrape.sh <out_csv> <interval_secs> <duration_secs>
set -eu

OUT="${1:?out csv path required}"
INTERVAL="${2:-2}"
DURATION="${3:-300}"
METRICS_URL="http://localhost:9090/metrics"
READY_URL="http://localhost:9090/readyz"

# Extract the value of a bare Prometheus metric line (`name value`). Empty if
# the series is absent (older build) so the assessor can tell absent from zero.
metric() { awk -v k="$1" '$1==k {print $2; found=1} END{if(!found)print ""}'; }

echo "epoch,raft_term,is_leader,storm_jumps,headroom_cores,readyz" > "${OUT}"

end=$(( $(date +%s) + DURATION ))
while [ "$(date +%s)" -lt "${end}" ]; do
  now="$(date +%s)"
  body="$(curl -s --max-time 3 "${METRICS_URL}" 2>/dev/null || true)"
  term="$(printf '%s\n' "${body}" | metric ferrosa_raft_current_term)"
  leader="$(printf '%s\n' "${body}" | metric ferrosa_raft_is_leader)"
  storms="$(printf '%s\n' "${body}" | metric ferrosa_raft_election_storm_term_jumps_total)"
  headroom="$(printf '%s\n' "${body}" | metric ferrosa_sched_consensus_headroom_cores)"
  # readyz: "ready" if the endpoint reports ready:true (leader known), else the
  # HTTP failure/served body ("notready"). curl -f makes 503 a non-zero exit.
  if curl -sf --max-time 3 "${READY_URL}" 2>/dev/null | grep -q '"ready":true'; then
    ready=ready
  else
    ready=notready
  fi
  printf '%s,%s,%s,%s,%s,%s\n' "${now}" "${term}" "${leader}" "${storms}" "${headroom}" "${ready}" >> "${OUT}"
  sleep "${INTERVAL}"
done
