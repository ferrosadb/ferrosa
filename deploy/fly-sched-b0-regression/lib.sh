#!/usr/bin/env bash
# Shared helpers for the scheduler-B0 no-step-down regression harness. Sourced by
# the other scripts. Dry-run by default; every fly-mutating command goes through
# `fly_do` / `fly_retry` so nothing bills unless the caller passes --i-will-pay.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Repo root is two levels up (deploy/fly-sched-b0-regression -> repo).
REPO_ROOT="$(cd "${HERE}/../.." && pwd)"
# shellcheck source=config.env
source "${HERE}/config.env"

# FLY_APP must be set by the caller (per-arm). Fail loud if missing.
: "${FLY_APP:?FLY_APP must be set (per-arm app name) before sourcing lib.sh}"

case "${OUT_DIR}" in
  /*) : ;;
  *)  OUT_DIR="${REPO_ROOT}/${OUT_DIR}" ;;
esac

# Dry-run unless the caller explicitly opts into billing with --i-will-pay.
I_WILL_PAY=0
for arg in "$@"; do
  [ "${arg}" = "--i-will-pay" ] && I_WILL_PAY=1
done

log() { printf '[schedb0-regression][%s] %s\n' "${FLY_APP}" "$*" >&2; }
die() { printf '[schedb0-regression][%s][FATAL] %s\n' "${FLY_APP}" "$*" >&2; exit 1; }

require_flyctl() {
  command -v flyctl >/dev/null 2>&1 \
    || die "flyctl not found on PATH. Install: https://fly.io/docs/flyctl/install/"
}

# Run (or, in dry-run, only PRINT) a billing/mutating flyctl command.
fly_do() {
  if [ "${I_WILL_PAY}" -eq 1 ]; then
    log "RUN: $*"; "$@"
  else
    log "DRY-RUN (pass --i-will-pay to execute): $*"
  fi
}

node_name()   { printf '%s-node-%d' "${FLY_APP}" "$1"; }
client_name() { printf '%s-client' "${FLY_APP}"; }

node_private_ip() {
  command -v flyctl >/dev/null 2>&1 || { printf ''; return; }
  flyctl machine list --app "${FLY_APP}" --json 2>/dev/null \
    | jq -r --arg n "$(node_name "$1")" '.[] | select(.name==$n) | .private_ip' 2>/dev/null | head -n1
}

# Comma-separated bracketed-IPv6 CQL node list (fly private net is IPv6; loadgen
# parses numeric SocketAddr, not DNS). Empty in dry-run (no machines).
all_node_cql_addrs() {
  local out="" i ip
  for i in $(seq 0 $((NODE_COUNT - 1))); do
    ip="$(node_private_ip "${i}")"
    [ -n "${ip}" ] || continue
    [ -n "${out}" ] && out="${out},"
    out="${out}[${ip}]:9042"
  done
  printf '%s' "${out}"
}

machine_id_for_name() {
  local name="$1"
  command -v flyctl >/dev/null 2>&1 || { printf ''; return; }
  flyctl machine list --app "${FLY_APP}" --json 2>/dev/null \
    | { command -v jq >/dev/null 2>&1 \
        && jq -r --arg n "${name}" '.[] | select(.name==$n) | .id' \
        || cat; } | head -n1
}

# Run a command on a named machine via `flyctl ssh console`. Dry-run prints only.
fly_ssh() {
  local name="$1"; shift
  if [ "${I_WILL_PAY}" -eq 1 ]; then
    local id; id="$(machine_id_for_name "${name}")"
    [ -n "${id}" ] || die "no machine id for name ${name} in app ${FLY_APP}"
    flyctl ssh console --app "${FLY_APP}" --machine "${id}" -C "$*"
  else
    log "DRY-RUN ssh ${name}: $*"
  fi
}

fly_get() {
  local name="$1" remote="$2" local_path="$3"
  if [ "${I_WILL_PAY}" -eq 1 ]; then
    local id; id="$(machine_id_for_name "${name}")"
    [ -n "${id}" ] || die "no machine id for ${name}"
    flyctl ssh sftp get "${remote}" "${local_path}" --app "${FLY_APP}" --machine "${id}" \
      || log "WARN: could not fetch ${remote} from ${name}"
  else
    log "DRY-RUN sftp get ${name}:${remote} -> ${local_path}"
  fi
}

# Retry a fly-mutating command (absorbs MANIFEST_UNKNOWN registry-propagation
# 404 on the first `machine run` after a fresh push). Dry-run prints.
fly_retry() {
  local attempts="${FLY_RETRY_ATTEMPTS:-4}" delay="${FLY_RETRY_DELAY:-15}" n=1
  if [ "${I_WILL_PAY}" -ne 1 ]; then
    log "DRY-RUN (pass --i-will-pay to execute): $*"; return 0
  fi
  while :; do
    log "RUN (attempt ${n}/${attempts}): $*"
    "$@" && return 0
    [ "${n}" -ge "${attempts}" ] && die "command failed after ${attempts} attempts: $*"
    log "attempt ${n} failed; retrying in ${delay}s (registry propagation?)"
    sleep "${delay}"; n=$((n + 1))
  done
}
