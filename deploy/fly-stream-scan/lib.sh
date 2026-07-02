#!/usr/bin/env bash
# Shared helpers for the fly streaming-scan harness. Sourced by the other scripts.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=config.env
source "${HERE}/config.env"

# Dry-run unless the caller explicitly opts into billing with --i-will-pay.
# Every fly-mutating command goes through `fly_do` so nothing bills by accident.
I_WILL_PAY=0
for arg in "$@"; do
  if [ "${arg}" = "--i-will-pay" ]; then
    I_WILL_PAY=1
  fi
done

log()  { printf '[fly-stream-scan] %s\n' "$*" >&2; }
die()  { printf '[fly-stream-scan][FATAL] %s\n' "$*" >&2; exit 1; }

require_flyctl() {
  command -v flyctl >/dev/null 2>&1 || die "flyctl not found on PATH. Install: https://fly.io/docs/flyctl/install/"
}

# Run (or, in dry-run, only PRINT) a billing/mutating flyctl command.
fly_do() {
  if [ "${I_WILL_PAY}" -eq 1 ]; then
    log "RUN: $*"
    "$@"
  else
    log "DRY-RUN (pass --i-will-pay to execute): $*"
  fi
}

# Machine name for node index i (0-based).
node_name() { printf '%s-node-%d' "${FLY_APP}" "$1"; }

# List provisioned machine ids for this app (empty in dry-run / not-provisioned).
node_machine_ids() {
  if command -v flyctl >/dev/null 2>&1; then
    flyctl machine list --app "${FLY_APP}" --json 2>/dev/null \
      | { command -v jq >/dev/null 2>&1 && jq -r '.[].id' || cat; } || true
  fi
}
