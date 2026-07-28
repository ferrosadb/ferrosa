#!/usr/bin/env bash
# Local 3-node Elle strict-serializability run with fault injection.
#
# A cheap stand-in for deploy/fly-accord-elle/certify-nemesis.sh: same generator,
# same checker, same list-append workload — on podman instead of Fly. Use it to
# iterate on the harness and to reproduce faults without spending a Fly run.
#
# NOT a substitute for the Fly certification. One host, container networking, and
# 1 GiB per node; it cannot speak to real multi-host behaviour. It exists so that
# a broken harness is caught here rather than after a 26-minute cloud run.
#
# Usage:  deploy/local-elle/certify-local.sh [ops_per_worker] [workers]
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE="${ROOT_DIR}/ferrosa-jepsen/tests/docker/elle-cluster-rf3.yml"
IMAGE="${FERROSA_NIGHTLY_IMAGE:-localhost/ferrosa-elle-current:latest}"
OUT_EDN="${OUT_EDN:-${ROOT_DIR}/deploy/local-elle/local-history.edn}"
OPS="${1:-3000}"
WORKERS="${2:-6}"
KEYS="${KEYS:-8}"
RF="${RF:-3}"

log() { printf '\n[local-cert] %s\n' "$*" >&2; }
die() { printf '\n[local-cert][FATAL] %s\n' "$*" >&2; exit 1; }

command -v podman >/dev/null || die "podman not on PATH"
command -v lein   >/dev/null || die "lein not on PATH (needed for the checker)"

# --- Node helpers -----------------------------------------------------------
# Every check inspects LIVE state. An earlier version of this harness trusted
# `podman ps` output captured before a fault and concluded a dead node was
# healthy, which turned every subsequent run into a silent 2-of-3 cluster whose
# SERIAL-read failures looked like a product bug. Never infer liveness.
running() { [ "$(podman inspect "$1" --format '{{.State.Running}}' 2>/dev/null)" = "true" ]; }
ready() { curl -sf --max-time 3 "http://localhost:$1/readyz" >/dev/null 2>&1; }
port_for() { case "$1" in 1) echo 9090;; 2) echo 9091;; 3) echo 9092;; esac; }

wait_all_ready() {
  local i n up rdy
  for i in $(seq 1 60); do
    up=0; rdy=0
    for n in 1 2 3; do
      running "ferrosa-elle-node${n}" && up=$((up + 1))
      ready "$(port_for "$n")" && rdy=$((rdy + 1))
    done
    [ "$up" = 3 ] && [ "$rdy" = 3 ] && { log "3/3 running and ready (${i}x2s)"; return 0; }
    sleep 2
  done
  die "cluster did not reach 3 running + 3 ready"
}

# Kill a node, verify it actually died, restart it, verify it actually returned.
#
# The restart is NOT deferred behind a sleep in a background subshell. A previous
# version did that, and when the parent timed out the subshell was killed before
# the restart ran — leaving the node dead for every later run.
restart_node() {
  # NOTE: split deliberately. `local n="$1" c="...${n}"` does NOT work — bash
  # expands every word of the `local` builtin BEFORE performing any assignment,
  # so ${n} resolves against the OUTER scope (unset) and `set -u` aborts.
  local n="$1"
  local c="ferrosa-elle-node${n}"
  local t0 t1 i
  log "FAULT: SIGKILL node${n}"
  podman kill --signal SIGKILL "$c" >/dev/null 2>&1 || log "WARN: kill node${n} returned non-zero"

  running "$c" && die "node${n} still running after SIGKILL — the fault did not bite, so any verdict would be meaningless"
  log "node${n} confirmed down (exit=$(podman inspect "$c" --format '{{.State.ExitCode}}' 2>/dev/null))"

  sleep "${DOWN_SECS:-20}"

  t0=$(date +%s)
  podman start "$c" >/dev/null 2>&1 || die "could not restart node${n}"
  for i in $(seq 1 60); do
    if running "$c" && ready "$(port_for "$n")"; then
      t1=$(date +%s)
      log "node${n} REJOIN_SECONDS=$((t1 - t0))"
      return 0
    fi
    sleep 2
  done
  die "node${n} did not return within 120s — refusing to continue with a degraded cluster"
}

# --- Bring up ---------------------------------------------------------------
log "cluster up (image=$IMAGE)"
FERROSA_NIGHTLY_IMAGE="$IMAGE" podman compose -f "$COMPOSE" down >/dev/null 2>&1
FERROSA_NIGHTLY_IMAGE="$IMAGE" podman compose -f "$COMPOSE" up -d >/dev/null 2>&1 \
  || die "compose up failed"
wait_all_ready

# --- Run generator + faults -------------------------------------------------
# Generator in the BACKGROUND, faults in the FOREGROUND: the fault schedule then
# controls timing and cannot be orphaned, and we can tell whether the generator
# was still alive when each fault fired.
log "generator: $KEYS keys, $OPS ops/worker, $WORKERS workers, rf=$RF"
GEN_LOG="$(mktemp)"
( cd "$ROOT_DIR" && cargo run -q -p ferrosa-jepsen --example elle_list_append -- \
    127.0.0.1:9042 "$OUT_EDN" "$KEYS" "$OPS" "$WORKERS" "$RF" ) >"$GEN_LOG" 2>&1 &
GEN_PID=$!

sleep "${WARMUP_SECS:-15}"
kill -0 "$GEN_PID" 2>/dev/null || die "generator exited during warmup — see $GEN_LOG"

restart_node 3   # replica
kill -0 "$GEN_PID" 2>/dev/null \
  && log "generator still running after the replica restart" \
  || log "WARN: generator finished before the replica restart — that fault hit an idle cluster"

restart_node 1   # the node the generator's first worker is pinned to
if kill -0 "$GEN_PID" 2>/dev/null; then
  log "generator still running after the coordinator restart — faults fired against live traffic"
else
  log "WARN: generator finished before the coordinator restart — raise ops before trusting a green verdict"
fi

log "waiting for generator to finish"
wait "$GEN_PID"
GEN_RC=$?
grep -E "pinning|=== failure|^ +[0-9]+ +:|wrote" "$GEN_LOG" | head -20 >&2
[ "$GEN_RC" = 0 ] || log "WARN: generator exited $GEN_RC"

wait_all_ready   # the cluster must be whole again before the verdict means anything

# --- Verdict ----------------------------------------------------------------
[ -s "$OUT_EDN" ] || die "history is empty"
log "outcome breakdown"
for t in ok info fail; do
  printf '  :%-5s %s\n' "$t" "$(grep -c ":type :$t" "$OUT_EDN")" >&2
done

log "Elle check"
( cd "${ROOT_DIR}/ferrosa-jepsen/elle-checker" && lein run "$OUT_EDN" )
RC=$?
log "checker exit=$RC  (0 only on a definitive valid? true)"
log "REMINDER: a green verdict over a mostly-:fail history is VACUOUS — read the breakdown above before citing it."
exit "$RC"
