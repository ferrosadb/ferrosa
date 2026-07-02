#!/usr/bin/env bash
# Destroy every ferrosa machine + the app so no billing leaks. Fail-loud: if any
# destroy fails, exit non-zero and name the survivor so it can be cleaned up.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${HERE}/lib.sh" "$@"

require_flyctl

log "Tearing down app=${FLY_APP}"
failed=0

ids="$(node_machine_ids)"
if [ -z "${ids}" ]; then
  log "No machines found for ${FLY_APP} (nothing to destroy, or dry-run)."
else
  for id in ${ids}; do
    if ! fly_do flyctl machine destroy "${id}" --app "${FLY_APP}" --force; then
      log "[FAIL] machine ${id} did not destroy — investigate to avoid billing leak"
      failed=1
    fi
  done
fi

if ! fly_do flyctl apps destroy "${FLY_APP}" --yes; then
  log "[FAIL] app ${FLY_APP} did not destroy — investigate to avoid billing leak"
  failed=1
fi

if [ "${failed}" -ne 0 ]; then
  die "teardown left resources behind — clean up manually to stop billing."
fi
log "Teardown complete."
