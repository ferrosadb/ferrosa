#!/usr/bin/env bash
# Destroy all machines in FLY_APP (nodes + client). Keeps the app + registry so
# a subsequent arm can reuse pushed images. Pass --destroy-app to also remove the
# app. Dry-run by default.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${HERE}/lib.sh" "$@"
require_flyctl

DESTROY_APP=0
for arg in "$@"; do [ "${arg}" = "--destroy-app" ] && DESTROY_APP=1; done

if [ "${I_WILL_PAY}" -eq 1 ]; then
  ids="$(flyctl machine list --app "${FLY_APP}" --json 2>/dev/null | jq -r '.[].id' || true)"
  for id in ${ids}; do
    log "destroying machine ${id}"
    flyctl machine destroy "${id}" --app "${FLY_APP}" --force || log "WARN destroy ${id} failed"
  done
  if [ "${DESTROY_APP}" -eq 1 ]; then
    log "destroying app ${FLY_APP}"
    flyctl apps destroy "${FLY_APP}" --yes || log "WARN app destroy failed"
  fi
else
  log "DRY-RUN: would destroy all machines in ${FLY_APP}$([ "${DESTROY_APP}" -eq 1 ] && echo ' + the app')"
fi
