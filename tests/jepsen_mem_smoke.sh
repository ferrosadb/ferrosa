#!/usr/bin/env bash
# Memory-constrained jepsen-driver smoke — a fast, local regression guard for the
# class of bug that kept the tier-multi-dc nightly red for 2+ weeks: the
# ferrosa-jepsen driver materializing the entire operation history in RAM and
# OOM-ing the runner.
#
# It builds an arm64/x86_64 Linux `ferrosa-jepsen` in a container, then runs
# `run --tier multi-dc` under a hard memory cap. That tier currently exercises
# MockCqlSession (the orchestrator's node_count<=3 gate excludes T3 — see
# t_8527bddf), so it SPINS at CPU speed, generating history as fast as possible —
# the worst case for the recorder. With the streaming HistoryRecorder, RSS stays
# flat (history goes to a host-mounted disk volume, NOT counted in the cgroup),
# so the container must NOT be OOM-killed. Before the fix it died in seconds.
#
# Usage: tests/jepsen_mem_smoke.sh [--mem 2g] [--seconds 90] [--rebuild]
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
MEM=2g
SECONDS_RUN=90
REBUILD=no
while [ $# -gt 0 ]; do
  case "$1" in
    --mem)      MEM="$2"; shift 2 ;;
    --seconds)  SECONDS_RUN="$2"; shift 2 ;;
    --rebuild)  REBUILD=yes; shift ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

# container_runtime() equivalent: prefer docker, fall back to podman.
RT="$(command -v docker || command -v podman || true)"
[ -n "$RT" ] || { echo "need docker or podman" >&2; exit 1; }

BIN="$REPO/target-linux/release/ferrosa-jepsen"
if [ "$REBUILD" = yes ] || [ ! -x "$BIN" ]; then
  echo "=== building Linux ferrosa-jepsen (container) ==="
  "$RT" run --rm -v "$REPO":/work:Z -w /work \
    -e CARGO_TARGET_DIR=/work/target-linux \
    -v ferrosa-jepsen-cargo:/usr/local/cargo/registry \
    rust:slim-bookworm bash -c \
    "apt-get update -qq && apt-get install -y -qq capnproto >/dev/null 2>&1 && cargo build --release -p ferrosa-jepsen"
fi
[ -x "$BIN" ] || { echo "driver not built at $BIN" >&2; exit 1; }

HOSTTMP="$(mktemp -d)"
trap '"$RT" rm -f jmem-smoke >/dev/null 2>&1 || true; rm -rf "$HOSTTMP"' EXIT
"$RT" rm -f jmem-smoke >/dev/null 2>&1 || true

echo "=== running 'run --tier multi-dc' under --memory=$MEM for ${SECONDS_RUN}s ==="
# TMPDIR -> host volume so the streamed history lands on real disk (not the
# container's cgroup-accounted memory): RSS then reflects the driver's true
# resident set, which is what we assert stays bounded.
"$RT" run -d --name jmem-smoke --memory="$MEM" --memory-swap="$MEM" -e TMPDIR=/histdata \
  -v "$BIN":/ferrosa-jepsen:ro -v "$HOSTTMP":/histdata \
  rust:slim-bookworm /ferrosa-jepsen run --tier multi-dc --output-dir /tmp/out >/dev/null

peak_rss=0
iters=$(( SECONDS_RUN / 5 ))
for _ in $(seq 1 "$iters"); do
  st="$("$RT" inspect -f '{{.State.Status}}/{{.State.OOMKilled}}/{{.State.ExitCode}}' jmem-smoke 2>/dev/null || echo "gone")"
  mem="$("$RT" stats --no-stream --format '{{.MemUsage}}' jmem-smoke 2>/dev/null || true)"
  hist="$(du -sh "$HOSTTMP" 2>/dev/null | cut -f1)"
  echo "  state=$st rss=$mem hist_on_disk=$hist"
  case "$st" in
    */true/*) echo "FAIL: container was OOM-killed — history is materializing in RAM" >&2; exit 1 ;;
    exited/*) echo "FAIL: driver exited early ($st)" >&2; "$RT" logs jmem-smoke 2>&1 | tail -20 >&2; exit 1 ;;
  esac
  sleep 5
done

oom="$("$RT" inspect -f '{{.State.OOMKilled}}' jmem-smoke 2>/dev/null)"
[ "$oom" = "false" ] || { echo "FAIL: OOMKilled=$oom" >&2; exit 1; }
echo "PASS: RSS stayed bounded under $MEM for ${SECONDS_RUN}s while the history streamed to disk (no OOM)."
