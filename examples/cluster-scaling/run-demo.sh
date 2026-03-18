#!/usr/bin/env bash
# run-demo.sh — Orchestrate the cluster scaling demo: standalone → pair → Raft
# Runs CQL scripts at each phase, asserts row counts, and verifies background
# writes survive topology transitions.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

HOST="${FERROSA_HOST:-127.0.0.1}"
PORT="${FERROSA_PORT:-9042}"

# ── Compose file shortcuts ──────────────────────────────────────────────────
BASE="-f docker-compose.yml"
PAIR="$BASE -f docker-compose.pair.yml"
CLUSTER="$PAIR -f docker-compose.cluster.yml"

# ── Counters ────────────────────────────────────────────────────────────────
pass=0
fail=0
bg_writer_pid=""

# ── Cleanup ─────────────────────────────────────────────────────────────────
cleanup() {
  echo ""
  echo "Cleaning up..."
  if [ -n "$bg_writer_pid" ] && kill -0 "$bg_writer_pid" 2>/dev/null; then
    kill "$bg_writer_pid" 2>/dev/null || true
    wait "$bg_writer_pid" 2>/dev/null || true
  fi
  docker compose $CLUSTER down -v 2>/dev/null || docker compose down -v 2>/dev/null || true
}
trap cleanup EXIT

# ── Helpers ─────────────────────────────────────────────────────────────────

wait_cql() {
  local port=$1
  local elapsed=0
  local timeout=120
  echo "  Waiting for CQL on $HOST:$port (up to ${timeout}s)..."
  while ! cqlsh "$HOST" "$port" -e "SELECT cluster_name FROM system.local;" >/dev/null 2>&1; do
    sleep 2
    elapsed=$((elapsed + 2))
    if [ "$elapsed" -ge "$timeout" ]; then
      echo "  TIMEOUT: CQL not available on port $port after ${timeout}s"
      docker compose $CLUSTER logs --tail=20 2>/dev/null || true
      exit 1
    fi
  done
  echo "  CQL ready on port $port (${elapsed}s)"
}

wait_writes_ready() {
  local port=${1:-$PORT}
  local elapsed=0
  local timeout=60
  echo "  Waiting for write path to stabilize on port $port..."
  while ! cqlsh "$HOST" "$port" -e "INSERT INTO app.events (tenant_id, event_date, event_id, event_type, payload) VALUES ('_probe', '1970-01-01', 0, 'probe', 'test');" >/dev/null 2>&1; do
    sleep 2
    elapsed=$((elapsed + 2))
    if [ "$elapsed" -ge "$timeout" ]; then
      echo "  WARNING: write path not ready after ${timeout}s, continuing anyway"
      return
    fi
  done
  # Clean up probe row
  cqlsh "$HOST" "$port" -e "DELETE FROM app.events WHERE tenant_id = '_probe' AND event_date = '1970-01-01';" >/dev/null 2>&1 || true
  echo "  Write path ready (${elapsed}s)"
}

run_cql() {
  local file=$1
  local port=${2:-$PORT}
  echo "  Running $file on port $port..."
  if cqlsh "$HOST" "$port" -f "$file" 2>&1; then
    echo "  PASS: $file"
    pass=$((pass + 1))
  else
    echo "  FAIL: $file"
    fail=$((fail + 1))
  fi
}

assert_count() {
  local label=$1 query=$2 expected=$3 port=${4:-$PORT}
  local result
  result=$(cqlsh "$HOST" "$port" -e "$query" 2>/dev/null | awk '/^[[:space:]]+[0-9]+/{print $1; exit}')
  if [ "${result:-}" = "$expected" ]; then
    echo "  ASSERT PASS: $label = $expected"
    pass=$((pass + 1))
  else
    echo "  ASSERT FAIL: $label expected=$expected actual=${result:-null}"
    fail=$((fail + 1))
  fi
}

start_bg_writes() {
  (
    local i=0
    while true; do
      cqlsh "$HOST" "$PORT" -e \
        "INSERT INTO app.events (tenant_id, event_date, event_id, event_type, payload) VALUES ('bg_writer', '2026-03-18', $((10000 + i)), 'heartbeat', '{\"seq\":$i}');" \
        >/dev/null 2>&1 || true
      i=$((i + 1))
      sleep 1
    done
  ) &
  bg_writer_pid=$!
  echo "  Background writer started (pid=$bg_writer_pid)"
}

stop_bg_writes() {
  if [ -n "$bg_writer_pid" ] && kill -0 "$bg_writer_pid" 2>/dev/null; then
    kill "$bg_writer_pid" 2>/dev/null || true
    wait "$bg_writer_pid" 2>/dev/null || true
    echo "  Background writer stopped (pid=$bg_writer_pid)"
    bg_writer_pid=""
  fi
}

# ════════════════════════════════════════════════════════════════════════════
echo "╔══════════════════════════════════════════╗"
echo "║  Ferrosa Cluster Scaling Demo            ║"
echo "╚══════════════════════════════════════════╝"
echo ""

# ── Act 1: Standalone / Development ─────────────────────────────────────────
echo "╔══════════════════════════════════════════╗"
echo "║  Act 1: Standalone / Development         ║"
echo "╚══════════════════════════════════════════╝"
echo ""

echo "Building node1..."
docker compose build node1

echo "Starting standalone node1..."
docker compose up -d

wait_cql 9042

run_cql 01-standalone-schema.cql
run_cql 02-standalone-writes.cql

assert_count "acme (standalone)" \
  "SELECT COUNT(*) FROM app.events WHERE tenant_id = 'acme' AND event_date = '2026-03-18';" \
  5
assert_count "globex (standalone)" \
  "SELECT COUNT(*) FROM app.events WHERE tenant_id = 'globex' AND event_date = '2026-03-18';" \
  5

echo ""
echo "Starting background writes before adding node2..."
start_bg_writes
echo ""

# ── Act 2: Pair Mode / Low-Volume Production ────────────────────────────────
echo "╔══════════════════════════════════════════╗"
echo "║  Act 2: Pair Mode / Low-Volume Production║"
echo "╚══════════════════════════════════════════╝"
echo ""

echo "Adding node2 in pair mode..."
docker compose $PAIR up -d

wait_cql 9043
wait_writes_ready

run_cql 03-pair-writes.cql
run_cql 04-pair-ddl.cql

assert_count "acme (pair)" \
  "SELECT COUNT(*) FROM app.events WHERE tenant_id = 'acme' AND event_date = '2026-03-18';" \
  8
assert_count "globex (pair)" \
  "SELECT COUNT(*) FROM app.events WHERE tenant_id = 'globex' AND event_date = '2026-03-18';" \
  7

echo ""

# ── Act 3: Raft Cluster / Full Production ────────────────────────────────────
echo "╔══════════════════════════════════════════╗"
echo "║  Act 3: Raft Cluster / Full Production   ║"
echo "╚══════════════════════════════════════════╝"
echo ""

echo "Adding node3 in Raft cluster mode..."
docker compose $CLUSTER up -d

wait_cql 9044
wait_writes_ready

run_cql 05-cluster-writes.cql
run_cql 06-cluster-ddl.cql

assert_count "acme (cluster)" \
  "SELECT COUNT(*) FROM app.events WHERE tenant_id = 'acme' AND event_date = '2026-03-18';" \
  10
assert_count "initech (cluster)" \
  "SELECT COUNT(*) FROM app.events WHERE tenant_id = 'initech' AND event_date = '2026-03-18';" \
  3

echo ""

# ── Stop background writes ──────────────────────────────────────────────────
stop_bg_writes
echo ""

# ── Final Verification ──────────────────────────────────────────────────────
echo "╔══════════════════════════════════════════╗"
echo "║  Final Verification                      ║"
echo "╚══════════════════════════════════════════╝"
echo ""

run_cql 07-verify.cql

# Confirm background writes survived topology transitions
bg_count=$(cqlsh "$HOST" "$PORT" -e \
  "SELECT COUNT(*) FROM app.events WHERE tenant_id = 'bg_writer' AND event_date = '2026-03-18';" \
  2>/dev/null | grep -oP '\d+' | tail -1)

if [ "${bg_count:-0}" -gt 0 ]; then
  echo "  ASSERT PASS: bg_writer count = ${bg_count} (>0, writes survived transitions)"
  pass=$((pass + 1))
else
  echo "  ASSERT FAIL: bg_writer count = ${bg_count:-0} (expected >0)"
  fail=$((fail + 1))
fi

echo ""

# ── Summary ─────────────────────────────────────────────────────────────────
echo "╔══════════════════════════════════════════╗"
echo "║  Summary                                 ║"
echo "╚══════════════════════════════════════════╝"
echo ""
echo "  Passed: $pass"
echo "  Failed: $fail"
echo ""

if [ "$fail" -gt 0 ]; then
  echo "  RESULT: FAIL"
  exit 1
else
  echo "  RESULT: PASS"
  exit 0
fi
