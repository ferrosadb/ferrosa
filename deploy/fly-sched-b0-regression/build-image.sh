#!/usr/bin/env bash
# Build + push ONE regression image (ferrosa + loadgen) from a git ref into the
# FLY_APP registry under a caller-supplied label. Echoes the pushed image tag as
# its LAST stdout line so run-all.sh can capture it.
#
#   build-image.sh <git-ref> <label-prefix> [--i-will-pay]
#
# For a ref that is not the current HEAD (the pre-fix arm), a temporary git
# worktree is checked out at that ref and the harness dir is copied in (the
# Dockerfile + entrypoint + scraper are new files that do not exist at the
# pre-fix ref), so the plain `COPY . .` build sees them.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

REF="${1:?git ref required}"; LABEL_PREFIX="${2:?label prefix required}"; shift 2 || true
# shellcheck source=lib.sh
source "${HERE}/lib.sh" "$@"
require_flyctl

SHA="$(git -C "${REPO_ROOT}" rev-parse --short "${REF}")"
LABEL="${LABEL_PREFIX}-${SHA}-$(date +%s)"          # time-unique (avoid stale-digest trap)
IMAGE="registry.fly.io/${FLY_APP}:${LABEL}"

# Resolve the build context: HEAD builds from the live worktree; any other ref
# builds from a throwaway worktree with the harness files overlaid.
CONTEXT="${REPO_ROOT}"
CLEANUP_WORKTREE=""
if [ "$(git -C "${REPO_ROOT}" rev-parse "${REF}")" != "$(git -C "${REPO_ROOT}" rev-parse HEAD)" ]; then
  CONTEXT="${OUT_DIR}/worktree-${SHA}"
  if [ "${I_WILL_PAY}" -eq 1 ]; then
    log "checking out ref ${REF} (${SHA}) into ${CONTEXT} for the pre-fix build"
    rm -rf "${CONTEXT}"
    mkdir -p "${OUT_DIR}"
    git -C "${REPO_ROOT}" worktree add --detach "${CONTEXT}" "${REF}" >&2
    CLEANUP_WORKTREE="${CONTEXT}"
    # Overlay the harness dir (new files absent at the pre-fix ref).
    mkdir -p "${CONTEXT}/deploy/fly-sched-b0-regression"
    cp "${HERE}"/*.sh "${HERE}"/*.Dockerfile "${CONTEXT}/deploy/fly-sched-b0-regression/"
  else
    log "DRY-RUN: would checkout ${REF} (${SHA}) into ${CONTEXT} + overlay the harness dir"
  fi
fi

cleanup() {
  if [ -n "${CLEANUP_WORKTREE}" ]; then
    git -C "${REPO_ROOT}" worktree remove --force "${CLEANUP_WORKTREE}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

log "building image ${IMAGE} from ref ${REF} (context ${CONTEXT})"
if [ "${I_WILL_PAY}" -eq 1 ]; then
  mkdir -p "${OUT_DIR}"
  FLY_TOML="${OUT_DIR}/fly.${FLY_APP}.toml"
  printf 'app = "%s"\nprimary_region = "%s"\n' "${FLY_APP}" "${FLY_REGION}" > "${FLY_TOML}"
  ( cd "${CONTEXT}" && flyctl deploy --app "${FLY_APP}" --config "${FLY_TOML}" \
      --image-label "${LABEL}" --build-only --push \
      --dockerfile "${CONTEXT}/deploy/fly-sched-b0-regression/ferrosa-regression.Dockerfile" >&2 )
  log "waiting 20s for image manifest to propagate"
  sleep 20
else
  log "DRY-RUN (pass --i-will-pay to execute): (cd ${CONTEXT} && flyctl deploy --app ${FLY_APP} --image-label ${LABEL} --build-only --push --dockerfile .../ferrosa-regression.Dockerfile)"
fi

# The captured tag (LAST line on stdout).
printf '%s\n' "${IMAGE}"
