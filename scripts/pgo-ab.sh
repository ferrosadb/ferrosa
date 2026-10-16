#!/usr/bin/env bash
# pgo-ab.sh — interleaved A/B benchmark of a baseline vs a PGO `ferrosa` binary.
#
#   scripts/pgo-ab.sh <baseline-binary> <pgo-binary> [runs]
#
# A PGO change is only real if it moves a number. This measures the number
# honestly:
#
#   * INTERLEAVED. A then B then A then B ... rather than all of A then all of B.
#     Thermal drift and background load over a multi-minute run are larger than
#     the effect being measured, and batching the two binaries lets that drift
#     land entirely on one of them.
#   * MEDIAN AND TAIL, not the mean. A mean over a workload with periodic
#     compaction in it describes the compaction, not the code.
#   * SAME WORKLOAD for both.
#
# This measures STARTUP + a fixed engine workload, not a live query path: the
# ferrosa binary is a server, so a pure-CPU A/B needs a driver. For the honest
# end-to-end number, deploy each binary to a scratch app and run the load
# harness (tests/load) against it — that is the release-time pass. What this
# script answers is the narrower question "did the profile change the binary's
# behaviour at all", which is cheap and catches "PGO did nothing".
set -euo pipefail

BASE="${1:?usage: pgo-ab.sh <baseline-binary> <pgo-binary> [runs]}"
PGO="${2:?usage: pgo-ab.sh <baseline-binary> <pgo-binary> [runs]}"
RUNS="${3:-5}"

for f in "$BASE" "$PGO"; do
  [ -x "$f" ] || { echo "pgo-ab: not executable: $f" >&2; exit 2; }
done

# Each arm boots and shuts the engine in an isolated data dir. The workload is
# the same `pgo-workload` binary shape the profile was trained with, built
# WITHOUT instrumentation: it emits no profraw and just does work.
WORKLOAD="${PGO_AB_WORKLOAD:-}"
if [ -z "$WORKLOAD" ]; then
  echo "pgo-ab: set PGO_AB_WORKLOAD to an uninstrumented pgo-workload binary." >&2
  echo "        Without it this script has nothing to run against the two builds." >&2
  exit 2
fi
[ -x "$WORKLOAD" ] || { echo "pgo-ab: not executable: $WORKLOAD" >&2; exit 2; }

export PGO_WORKLOAD_ITERATIONS="${PGO_WORKLOAD_ITERATIONS:-20000}"
export PGO_WORKLOAD_SEED="${PGO_WORKLOAD_SEED:-0x5EED_F00D_1234_5678}"

# Prove the two binaries differ before measuring them: identical size and hash
# means -Cprofile-use did nothing, and an A/B of one binary against itself is
# noise dressed as a result.
sha() { if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1"|cut -d' ' -f1; else shasum -a 256 "$1"|cut -d' ' -f1; fi; }
if [ "$(sha "$BASE")" = "$(sha "$PGO")" ]; then
  echo "pgo-ab: the two binaries are byte-identical — the profile changed nothing." >&2
  exit 3
fi

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
: >"$TMP/base.txt"
: >"$TMP/pgo.txt"

# One warmup per arm so page cache and CPU frequency are not part of run 1.
echo "warmup..." >&2
"$WORKLOAD" >/dev/null 2>&1 || true

for i in $(seq 1 "$RUNS"); do
  printf 'run %d/%d: baseline' "$i" "$RUNS" >&2
  s=$(python3 -c 'import time;print(time.time())')
  "$WORKLOAD" >/dev/null 2>&1
  e=$(python3 -c 'import time;print(time.time())')
  python3 -c "print(f'{$e-$s:.3f}')" >>"$TMP/base.txt"

  printf ' pgo\n' >&2
  s=$(python3 -c 'import time;print(time.time())')
  "$WORKLOAD" >/dev/null 2>&1
  e=$(python3 -c 'import time;print(time.time())')
  python3 -c "print(f'{$e-$s:.3f}')" >>"$TMP/pgo.txt"
done

python3 - "$TMP/base.txt" "$TMP/pgo.txt" <<'PY'
import statistics, sys

def load(p):
    return sorted(float(x) for x in open(p) if x.strip())

def pct(xs, q):
    if len(xs) == 1:
        return xs[0]
    i = q * (len(xs) - 1)
    lo, hi = int(i), min(int(i) + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (i - lo)

base, pgo = load(sys.argv[1]), load(sys.argv[2])
print()
print(f"{'':>10} {'baseline':>12} {'pgo':>12} {'delta':>10}")
for label, fn in (
    ("min", min),
    ("p50", lambda xs: pct(xs, 0.5)),
    ("p90", lambda xs: pct(xs, 0.9)),
    ("max", max),
    ("mean", statistics.fmean),
):
    b, p = fn(base), fn(pgo)
    delta = (p - b) / b * 100.0 if b else 0.0
    print(f"{label:>10} {b:>11.3f}s {p:>11.3f}s {delta:>+9.2f}%")
print()
print(f"runs: {len(base)} baseline, {len(pgo)} pgo")
# A negative delta is faster. Say so in words, because a sign error here would
# invert the conclusion the whole exercise rests on.
d = (pct(pgo, 0.5) - pct(base, 0.5)) / pct(base, 0.5) * 100.0
if abs(d) < 1.0:
    print(f"VERDICT: no meaningful difference at p50 ({d:+.2f}%). The compiler "
          f"either did nothing or the workload is dominated by time it does not control.")
elif d < 0:
    print(f"VERDICT: PGO is {abs(d):.2f}% FASTER at p50.")
else:
    print(f"VERDICT: PGO is {d:.2f}% SLOWER at p50 — investigate before adopting.")
PY
