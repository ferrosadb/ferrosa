#!/usr/bin/env bash
# End-to-end scheduler-B0 no-step-down regression (task t_88223ad0 / T0.6).
#
# One fly app, two image labels, arms run SEQUENTIALLY (teardown between) so
# node names/IPs are reused cleanly:
#   1. build post-fix image (B0 HEAD) + pre-fix image (origin/main pre-B0)
#   2. POST-FIX arm: nodes=post-fix, client=post-fix -> assert GREEN (no step-down)
#   3. PRE-FIX  arm: nodes=pre-fix,  client=post-fix -> assert REPRO (step-down)
# The client always runs the post-fix loadgen (only it has --scan-storm).
#
# Dry-run by default: prints the full plan without billing. Pass --i-will-pay to
# execute. Set RUN_PREFIX_ARM=0 to run only the green arm.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Single app for both arms.
export FLY_APP="${FLY_APP:-${FLY_APP_PREFIX:-ferrosa-schedb0}-t06}"
# shellcheck source=lib.sh
source "${HERE}/lib.sh" "$@"
require_flyctl
command -v jq >/dev/null 2>&1 || die "jq required (brew install jq)"
mkdir -p "${OUT_DIR}"

log "=== T0.6 regression: app=${FLY_APP} region=${FLY_REGION} nodes=${NODE_COUNT}x${VM_CPUS} ==="

# ── 1. Build images ───────────────────────────────────────────────────────────
log "building POST-FIX image (ref ${POSTFIX_REF})"
POSTFIX_IMAGE="$("${HERE}/build-image.sh" "${POSTFIX_REF}" postfix "$@" | tail -n1)"
log "post-fix image: ${POSTFIX_IMAGE}"

PREFIX_IMAGE=""
if [ "${RUN_PREFIX_ARM}" = "1" ]; then
  log "building PRE-FIX image (ref ${PREFIX_REF})"
  PREFIX_IMAGE="$("${HERE}/build-image.sh" "${PREFIX_REF}" prefix "$@" | tail -n1)"
  log "pre-fix image: ${PREFIX_IMAGE}"
fi

run_arm() {  # $1=arm $2=node_image $3=expect
  local arm="$1" node_image="$2" expect="$3"
  log "──────── ARM ${arm} (expect ${expect}) ────────"
  NODE_IMAGE="${node_image}" CLIENT_IMAGE="${POSTFIX_IMAGE}" "${HERE}/provision.sh" "$@"
  "${HERE}/run.sh" "${arm}" "$@"
  local rc=0
  "${HERE}/assess.sh" "${arm}" "${expect}" "$@" || rc=$?
  # Tear down this arm's machines before the next (keep the app + images).
  "${HERE}/teardown.sh" "$@"
  return "${rc}"
}

green_rc=0; repro_rc=0

# ── 2. POST-FIX arm (the shipping fix must stay green) ─────────────────────────
run_arm postfix "${POSTFIX_IMAGE}" green || green_rc=$?

# ── 3. PRE-FIX arm (non-vacuity: the workload must reproduce a step-down) ──────
if [ "${RUN_PREFIX_ARM}" = "1" ]; then
  run_arm prefix "${PREFIX_IMAGE}" repro || repro_rc=$?
fi

# ── Final report ──────────────────────────────────────────────────────────────
{
  echo "======== T0.6 SCHEDULER-B0 REGRESSION SUMMARY ========"
  echo "post-fix (green expectation): $([ "${green_rc}" -eq 0 ] && echo PASS || echo FAIL)"
  if [ "${RUN_PREFIX_ARM}" = "1" ]; then
    echo "pre-fix  (repro expectation): $([ "${repro_rc}" -eq 0 ] && echo PASS || echo FAIL)"
  else
    echo "pre-fix  arm: SKIPPED (RUN_PREFIX_ARM=0)"
  fi
  echo "artifacts: ${OUT_DIR}"
} | tee "${OUT_DIR}/SUMMARY.txt"

# B0 exit gate: post-fix green is mandatory; when the repro arm runs it must
# reproduce (else the gate is vacuous).
if [ "${green_rc}" -ne 0 ]; then die "POST-FIX arm was not green — B0 exit gate FAILED"; fi
if [ "${RUN_PREFIX_ARM}" = "1" ] && [ "${repro_rc}" -ne 0 ]; then
  die "PRE-FIX arm did not reproduce a step-down — gate is vacuous, tighten the workload"
fi
log "T0.6 PASSED (post-fix green$([ "${RUN_PREFIX_ARM}" = "1" ] && echo ', pre-fix reproduced'))."
