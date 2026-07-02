#!/usr/bin/env bash
# Orchestrate provision -> seed -> probe -> teardown for the fly streaming-scan
# memory harness. Dry-run by default; pass --i-will-pay to actually bill.
# Teardown ALWAYS runs (even if a probe fails) so machines never leak.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${HERE}/lib.sh" "$@"

if [ "${I_WILL_PAY}" -eq 1 ]; then
  PASS_ARG="--i-will-pay"
else
  PASS_ARG=""
fi

log "=== streaming-scan fly harness (i_will_pay=${I_WILL_PAY}) ==="

cleanup() {
  log "--- teardown (always runs) ---"
  "${HERE}/teardown.sh" ${PASS_ARG} || log "[WARN] teardown reported failure — check for leaked machines"
}
trap cleanup EXIT

"${HERE}/provision.sh" ${PASS_ARG}
"${HERE}/seed.sh"      ${PASS_ARG}
"${HERE}/probe.sh"     ${PASS_ARG}

log "=== harness complete ==="
