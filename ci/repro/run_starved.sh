#!/usr/bin/env bash
# CPU-starvation race fuzzer for the post-DDL count(*) / schema-propagation
# investigation. Starves the ferrosa runtime (nice + N busy hogs) to widen
# scheduling-race windows, then runs a DDL+count hammer against it.
#
# See README.md in this directory for the full investigation writeup.
#
# Usage:
#   FERROSA_BIN=target/debug/ferrosa \
#   FERROSA_CFG=/path/to/ferrosa.toml \
#   ci/repro/run_starved.sh [workers] [iters] [harness.py]
#
# Defaults assume you run from the ferrosa repo root with a debug build and a
# config whose CQL port is 19042. The harness defaults to the robust
# convergence-aware count_ddl_hammer.py in this directory.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
BIN="${FERROSA_BIN:-target/debug/ferrosa}"
CFG="${FERROSA_CFG:-/tmp/ferrosa-starved.toml}"
DATA="${FERROSA_DATA:-/tmp/ferrosa-starved-data}"
PORT="${FERROSA_CQL_PORT:-19042}"
WORKERS="${1:-4}"
ITERS="${2:-25}"
HARNESS="${3:-$HERE/count_ddl_hammer.py}"

[ -x "$BIN" ] || { echo "ferrosa binary not found/executable: $BIN"; exit 1; }
if [ ! -f "$CFG" ]; then
  echo "No config at $CFG — set FERROSA_CFG to a ferrosa.toml whose CQL bind is 127.0.0.1:$PORT"; exit 1
fi

NCPU=$(getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu)
rm -rf "$DATA"; mkdir -p "$DATA"
rm -f /tmp/ferrosa-count-counters.txt

# Start ferrosa at low priority so the busy hogs win scheduling — starving its
# tokio runtime. FERROSA_COUNT_PROBE=1 enables the env-gated durable counters
# (apply count-probe-instrumentation.patch first; counters dump to
# /tmp/ferrosa-count-counters.txt via an independent OS thread).
FERROSA_COUNT_PROBE=1 FERROSA_CONFIG="$CFG" FERROSA_DATA_DIR="$DATA" \
  nice -n 19 "$BIN" >/tmp/ferrosa-starved-server.log 2>&1 &
SRV=$!
trap 'kill -9 $SRV ${HOGS[*]:-} 2>/dev/null' EXIT
for _ in $(seq 1 120); do (echo >"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null && break; sleep 1; done
echo "server up (pid $SRV), ncpu=$NCPU; spawning $((NCPU*2)) CPU hogs"

HOGS=()
for _ in $(seq 1 $((NCPU*2))); do ( while :; do : ; done ) & HOGS+=($!); done

python3 "$HARNESS" "$WORKERS" "$ITERS" 2>&1

# Relieve starvation so the durable-counter dumper thread writes a final
# snapshot, then stop the server.
kill -9 "${HOGS[@]}" 2>/dev/null
sleep 1
echo "=== DURABLE COUNTERS (/tmp/ferrosa-count-counters.txt) ==="
cat /tmp/ferrosa-count-counters.txt 2>/dev/null || echo "(no counter file — was the instrumentation patch applied?)"
kill -9 "$SRV" 2>/dev/null; trap - EXIT
