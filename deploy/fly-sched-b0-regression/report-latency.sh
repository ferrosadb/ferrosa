#!/usr/bin/env bash
# Compare interactive point-read latency UNDER scan storm: pre-B0 vs full stack.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="${OUT_DIR:-target/fly-sched-b0-regression}"
cd "${HERE}/../.." 2>/dev/null || true
line_for() { grep -E "p50=.*p99=" "$1" 2>/dev/null | grep -iE "^Read:|read" | tail -1 || grep -E "p50=.*p99=" "$1" 2>/dev/null | tail -1; }
val() { echo "$1" | grep -oE "$2=[0-9.]+ms" | head -1 | sed "s/$2=//"; }
printf "\n%-12s %-10s %-10s %-10s %-10s\n" "arm" "p50" "p95" "p99" "p100(max)"
printf -- "------------------------------------------------------------\n"
for arm in prefix postfix; do
  label=$([ "$arm" = prefix ] && echo "pre-B0" || echo "B3-stack")
  f="${OUT_DIR}/${arm}/read-under-storm.stats.txt"
  ln="$(line_for "$f")"
  if [ -z "$ln" ]; then printf "%-12s (no read-latency data at %s)\n" "$label" "$f"; continue; fi
  printf "%-12s %-10s %-10s %-10s %-10s\n" "$label" "$(val "$ln" p50)" "$(val "$ln" p95)" "$(val "$ln" p99)" "$(val "$ln" p100)"
done
echo
