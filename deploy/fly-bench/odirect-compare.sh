#!/usr/bin/env bash
# Extract the O_DIRECT A/B comparison from the two arms' collected node /metrics
# and nosqlbench result bundles. Reads the fly-bench results dirs written by
# run_target/collect_node_metrics for RUN_ID=<base>-buffered and <base>-direct.
#
#   odirect-compare.sh <base-run-id>
set -euo pipefail
BASE="${1:?base run id required (e.g. odirect-ab-<sha>)}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FLYBENCH_RESULTS="${HERE}/../../target/fly-bench"

# Sum a Prometheus counter/gauge across every node scrape in an arm's *-after dir.
# $1 = arm results dir, $2 = metric name. Prints the summed value (0 if absent).
sum_metric() {
  local dir="$1" metric="$2"
  local total=0 v
  # collect_node_metrics writes '<metric> <value>' lines inside the /metrics dump.
  while IFS= read -r v; do
    # integer add (metrics here are integer counters/gauges)
    total=$(( total + ${v%%.*} ))
  done < <(grep -rhE "^${metric} [0-9]" "${dir}"/*-after/nodes/ 2>/dev/null | awk '{print $2}')
  echo "${total}"
}
max_metric() {
  local dir="$1" metric="$2"
  grep -rhE "^${metric} [0-9]" "${dir}"/*-after/nodes/ 2>/dev/null \
    | awk '{print $2}' | sort -n | tail -1 | sed 's/\..*//' || echo 0
}

arm_row() {  # $1 = arm label, $2 = results dir
  local arm="$1" dir="$2"
  if [ ! -d "${dir}" ]; then
    printf '%-10s (no results dir: %s)\n' "${arm}" "${dir}"
    return
  fi
  local fb ff fbytes se smax stot
  fb="$(sum_metric "${dir}" ferrosa_sstable_direct_write_fallbacks_total)"
  ff="$(sum_metric "${dir}" ferrosa_sstable_direct_write_files_total)"
  fbytes="$(sum_metric "${dir}" ferrosa_sstable_direct_write_bytes_total)"
  se="$(sum_metric "${dir}" ferrosa_sched_runtime_stall_events_total)"
  smax="$(max_metric "${dir}" ferrosa_sched_runtime_stall_max_micros)"
  stot="$(sum_metric "${dir}" ferrosa_sched_runtime_stall_micros_total)"
  printf '%-10s | fallbacks=%-6s files=%-6s bytes=%-12s | stall_events=%-6s stall_max_us=%-9s stall_total_us=%-10s\n' \
    "${arm}" "${fb}" "${ff}" "${fbytes}" "${se}" "${smax}" "${stot}"
}

echo "======== O_DIRECT A/B COMPARISON (${BASE}) ========"
echo
echo "── Node metrics (summed across nodes, post-workload) ──"
BUF_DIR="${FLYBENCH_RESULTS}/${BASE}-buffered"
DIR_DIR="${FLYBENCH_RESULTS}/${BASE}-direct"
arm_row buffered "${BUF_DIR}"
arm_row direct   "${DIR_DIR}"
echo
echo "Validity gate: the DIRECT arm must show fallbacks=0 AND files>0."
echo "  fallbacks>0 ⇒ the volume fs rejected O_DIRECT (arm INVALID, fell back to buffered)."
echo "  files==0    ⇒ no SSTable was written through the direct writer (no flush happened — workload too light)."
echo "Expected win: direct arm stall_events / stall_max_us materially LOWER than buffered."
echo

echo "── nosqlbench latency (from result bundles) ──"
for arm in buffered direct; do
  tgz=$(ls "${FLYBENCH_RESULTS}/${BASE}-${arm}"/*.tgz 2>/dev/null | head -1 || true)
  if [ -z "${tgz}" ]; then echo "${arm}: no nb bundle"; continue; fi
  tmp="$(mktemp -d)"; tar xzf "${tgz}" -C "${tmp}" 2>/dev/null || true
  echo "── ${arm} (${tgz##*/}) ──"
  # nosqlbench summary lines: service_time / result latency percentiles. Grep the
  # common HDR/summary keys; fall back to listing files if the schema differs.
  grep -rhiE 'p99|p100|percentile|service_time|result.*(mean|max)' "${tmp}" 2>/dev/null \
    | grep -iE 'p99|p100|max|mean' | head -20 || true
  echo "   (full bundle extracted at ${tmp})"
done
