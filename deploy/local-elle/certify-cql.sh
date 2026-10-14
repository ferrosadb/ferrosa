#!/usr/bin/env bash
# Fault-free Elle list-append STRICT-SERIALIZABILITY certification for the CQL
# (Accord) transaction path, on a LOCAL 3-node RF=3 cluster.
#
# This is the fault-free leg of the plan's S4 gate
# (specs/plans/read-in-transaction.md: "Elle list-append, :valid? true, live
# RF=3 + nemesis, no assumptions"). It is the automated fault-free step of
# .github/workflows/elle-strict-serializable-nightly.yml, run against a
# locally-built image for the commit in this worktree instead of against a
# nightly .deb. The fault-injected/restart leg is `certify-local.sh`
# (forge t_68cf9a98) — keep the two separate so a red result stays
# attributable.
#
# What it does, in order:
#   1. cluster up from the REPO'S OWN compose
#      (ferrosa-jepsen/tests/docker/elle-cluster-rf3.yml) and wait for
#      3/3 running AND ready. A green probe means Raft has a leader.
#   2. run the CQL list-append generator (ferrosa-jepsen/examples/
#      elle_list_append.rs) against 127.0.0.1:9042, emitting an .edn history.
#   3. re-verify the cluster is WHOLE. A verdict from a degraded cluster is a
#      statement about a broken cluster, not about the engine.
#   4. run the standalone lein Elle checker
#      (ferrosa-jepsen/elle-checker) on the history, UNCHANGED, and record its
#      verdict verbatim plus the raw anomaly dump.
#   5. tear the cluster down on EVERY exit path.
#
# The verdict is whatever the checker says. This script never rewrites it,
# never re-runs for a better one, and never widens a timeout or a bound to
# turn a run green: the generator's own 30 s per-request timeout and the
# compose's per-node memory limit are the ones that apply.
#
# Usage:
#   deploy/local-elle/certify-cql.sh [out-dir]
# Env (all optional):
#   FERROSA_NIGHTLY_IMAGE  node image (default localhost/ferrosa-elle-current:latest)
#   KEYS OPS WORKERS RF    generator args (defaults 8 300 8 3 — the nightly's
#                          fault-free args)
#   JAVA_HOME              JDK for lein (default /opt/homebrew/opt/openjdk@17)
#   CARGO_TARGET_DIR       host cargo target dir for the generator (default
#                          <suite>/.target-elle; kept OUT of the repo)
#   READY_TIMEOUT          seconds to wait for 3/3 nodes (default 240)
#
# Idempotent: `down` first, `down` last, and the evidence dir is created fresh.
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_REL="ferrosa-jepsen/tests/docker/elle-cluster-rf3.yml"
COMPOSE="$ROOT_DIR/$COMPOSE_REL"
IMAGE="${FERROSA_NIGHTLY_IMAGE:-localhost/ferrosa-elle-current:latest}"
KEYS="${KEYS:-8}"
OPS="${OPS:-300}"
WORKERS="${WORKERS:-8}"
RF="${RF:-3}"
JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@17}"
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$(dirname "$ROOT_DIR")/.target-elle}"
READY_TIMEOUT="${READY_TIMEOUT:-240}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_DIR="${1:-$ROOT_DIR/deploy/local-elle/evidence/$STAMP}"
OUT_EDN="$OUT_DIR/elle-cql-$STAMP.edn"
GEN_LOG="$OUT_DIR/generator.log"
CHECK_LOG="$OUT_DIR/checker.log"
CLUSTER_LOG="$OUT_DIR/cluster.log"
VERDICT_FILE="$OUT_DIR/VERDICT.txt"

mkdir -p "$OUT_DIR"
log() { printf '\n[certify-cql] %s\n' "$*" >&2; }
die() { printf '\n[certify-cql][FATAL] %s\n' "$*" >&2; echo "FATAL: $*" >> "$VERDICT_FILE"; exit 1; }

command -v podman >/dev/null || die "podman not on PATH"
[ -x "$JAVA_HOME/bin/java" ] || die "no JDK at JAVA_HOME=$JAVA_HOME (the checker needs one)"
[ -f "$COMPOSE" ] || die "missing repo compose $COMPOSE_REL"
[ -f "$ROOT_DIR/ferrosa-jepsen/elle-checker/project.clj" ] || die "missing the Elle checker project"

teardown() {
  podman compose -f "$COMPOSE" logs --no-color > "$CLUSTER_LOG" 2>&1 || true
  podman compose -f "$COMPOSE" down -v >/dev/null 2>&1 || true
}
trap teardown EXIT

export FERROSA_NIGHTLY_IMAGE="$IMAGE"

log "cluster up (image=$IMAGE, compose=$COMPOSE_REL)"
podman compose -f "$COMPOSE" down -v >/dev/null 2>&1 || true
podman compose -f "$COMPOSE" up -d --no-build > "$OUT_DIR/compose-up.log" 2>&1 \
  || die "compose up failed — see $OUT_DIR/compose-up.log"

# Every probe inspects LIVE state; liveness is never inferred from an earlier
# reading (a previous harness concluded a dead node was healthy that way and
# silently ran a 2-of-3 cluster).
port_for() { case "$1" in 1) echo 9090;; 2) echo 9091;; 3) echo 9092;; esac; }
ready() { curl -sf --max-time 3 "http://localhost:$1/readyz" >/dev/null 2>&1; }

# The CI job's own cluster-formation recipe (`.github/workflows/ci.yml`,
# jobs.build-node-image -> postgres-jepsen-fault, "Wait for PostgreSQL cluster
# formation"): `/readyz` alone is NOT enough. A node answers /readyz as soon as
# a Raft leader exists, while its DDL path and its view of the ring can still be
# catching up — and a driver that connects in that window sees fewer than three
# nodes. That is exactly how a previous run started its generator against a
# 2-node view and died with `initial cluster metadata fetch: Connection refused`
# before writing any history. So wait for the FULL condition on EVERY node:
#   /api/cluster/status  -> this node's own host_id, mode "cluster", DDL on the
#                           cluster path (not local Direct);
#   /api/cluster/ring    -> all three host ids present and every one "Normal".
HOST_IDS=(e11e0001-0001-0001-0001-000000000001
          e11e0002-0002-0002-0002-000000000002
          e11e0003-0003-0003-0003-000000000003)

formed() { # formed <node-number> -> 0 iff this node reports cluster mode + a 3-node Normal ring
  local n="$1" web status ring
  web="$(port_for "$n")"
  status="$(curl -sf --max-time 5 "http://localhost:$web/api/cluster/status")" || return 1
  printf '%s' "$status" | jq -e --arg id "${HOST_IDS[$((n-1))]}" \
    '.host_id == $id and .mode == "cluster" and .ddl_path == "cluster"' >/dev/null || return 1
  ring="$(curl -sf --max-time 5 "http://localhost:$web/api/cluster/ring")" || return 1
  printf '%s' "$ring" | jq -e \
    '([.nodes[] | select(.state == "Normal") | .host_id] | sort) == (["e11e0001-0001-0001-0001-000000000001","e11e0002-0002-0002-0002-000000000002","e11e0003-0003-0003-0003-000000000003"] | sort)' \
    >/dev/null || return 1
  return 0
}

log "waiting for 3/3 nodes running, ready, in cluster mode with a 3-node Normal ring (budget ${READY_TIMEOUT}s)"
deadline=$((SECONDS + READY_TIMEOUT))
up=0; rdy=0; ok=0
while (( SECONDS < deadline )); do
  up=0; rdy=0; ok=0
  for n in 1 2 3; do
    [ "$(podman inspect "ferrosa-elle-node$n" --format '{{.State.Running}}' 2>/dev/null)" = "true" ] && up=$((up+1))
    ready "$(port_for "$n")" && rdy=$((rdy+1))
    formed "$n" && ok=$((ok+1))
  done
  [ "$up" = 3 ] && [ "$rdy" = 3 ] && [ "$ok" = 3 ] && break
  sleep 2
done
[ "$up" = 3 ] && [ "$rdy" = 3 ] && [ "$ok" = 3 ] || {
  podman ps -a --filter name=ferrosa-elle --format '{{.Names}} {{.Status}}' >&2
  for n in 1 2 3; do
    echo "--- node$n ---" >&2
    curl -s --max-time 5 "http://localhost:$(port_for "$n")/api/cluster/status" >&2; echo >&2
    podman logs "ferrosa-elle-node$n" 2>&1 | tail -20 >&2
  done
  die "cluster did not reach 3 running + 3 ready + 3 cluster-formed within ${READY_TIMEOUT}s (last: up=$up ready=$rdy formed=$ok)"
}
log "cluster formed: 3/3 running, ready, cluster mode, 3-node Normal ring"

# --- Generator -------------------------------------------------------------
log "building the CQL list-append generator"
ulimit -n 8192 2>/dev/null || log "WARN: could not raise the file-descriptor limit"
CARGO_TARGET_DIR="$CARGO_TARGET_DIR" cargo build --release -q \
  --manifest-path "$ROOT_DIR/Cargo.toml" \
  -p ferrosa-jepsen --example elle_list_append >> "$GEN_LOG" 2>&1 \
  || die "failed to build the generator — see $GEN_LOG"
GEN_BIN="$CARGO_TARGET_DIR/release/examples/elle_list_append"
[ -x "$GEN_BIN" ] || die "generator binary not at $GEN_BIN"

log "generator: keys=$KEYS ops/worker=$OPS workers=$WORKERS rf=$RF -> $OUT_EDN"
"$GEN_BIN" 127.0.0.1:9042 "$OUT_EDN" "$KEYS" "$OPS" "$WORKERS" "$RF" >> "$GEN_LOG" 2>&1
GEN_RC=$?
log "generator exit=$GEN_RC (non-zero is expected only under faults; this leg injects none)"
grep -E "pinning|=== failure|^ +[0-9]+ +:|wrote" "$GEN_LOG" | head -25 >&2

[ -s "$OUT_EDN" ] || die "history is empty ($OUT_EDN)"

# The cluster must be WHOLE again before the verdict means anything.
log "re-checking the cluster is whole"
up=0; rdy=0
for n in 1 2 3; do
  [ "$(podman inspect "ferrosa-elle-node$n" --format '{{.State.Running}}' 2>/dev/null)" = "true" ] && up=$((up+1))
  ready "$(port_for "$n")" && rdy=$((rdy+1))
done
[ "$up" = 3 ] && [ "$rdy" = 3 ] || die "cluster degraded after the run ($up running / $rdy ready) — refusing to certify"

# --- Op breakdown ----------------------------------------------------------
log "outcome breakdown"
for t in ok info fail; do
  printf '  :%-5s %s\n' "$t" "$(grep -c ":type :$t" "$OUT_EDN")" >&2
done
OK_COUNT="$(grep -c ':type :ok' "$OUT_EDN")"
OK_APPENDS="$(grep -c ':type :ok.*:append' "$OUT_EDN")"
OK_READS="$(grep -c ':type :ok.*:r ' "$OUT_EDN")"

# --- Checker (UNCHANGED, verbatim verdict) --------------------------------
log "Elle check (lein, checker reused unchanged)"
{
  echo "=== Elle list-append / strict-serializable ==="
  echo "history: $OUT_EDN"
  echo "op breakdown: ok=$OK_COUNT info=$(grep -c ':type :info' "$OUT_EDN") fail=$(grep -c ':type :fail' "$OUT_EDN")"
  echo "  of the :ok ops: appends=$OK_APPENDS reads=$OK_READS"
} > "$VERDICT_FILE"
( cd "$ROOT_DIR/ferrosa-jepsen/elle-checker" \
    && JAVA_HOME="$JAVA_HOME" JAVA_TOOL_OPTIONS=-Djava.awt.headless=true lein run "$OUT_EDN" ) \
  2>&1 | tee -a "$CHECK_LOG" >> "$VERDICT_FILE"
CHECK_RC="${PIPESTATUS[0]}"

log "checker exit=$CHECK_RC  (0 only on a definitive valid? true)"

# A degenerate history certifies nothing. Fail loud rather than let a
# zero-work run be quoted as a pass. This is not a threshold on the RESULT —
# it refuses to report when no operation was observed to commit at all.
if [ "$OK_COUNT" = 0 ]; then
  log "REFUSING: the history contains no :ok op — nothing was certified"
  exit 2
fi
if [ "$OK_APPENDS" = 0 ]; then
  log "WARN: no append was observed to commit; a verdict over reads alone is weak evidence"
fi

log "artifacts: $OUT_DIR"
log "REMINDER: read the op breakdown above before quoting the verdict — a green"
log "verdict over an overwhelmingly :fail/:info history is VACUOUS, not a pass."
exit "$CHECK_RC"
