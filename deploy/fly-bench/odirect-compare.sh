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

echo "── nosqlbench op latency (result-success timer, from --report-csv-to) ──"
echo "   percentiles in the CSV's duration_unit (usually ms); p100 = max = the outlier tail."
# Parse the last cumulative row of result-success.csv (dropwizard CsvReporter):
# header cols include count,max,mean,p95,p99,p999,duration_unit — locate by name.
parse_nb_csv() {  # $1 = arm results dir
  local dir="$1" tgz tmp csv
  tgz=$(ls -t "${dir}"/*.tgz 2>/dev/null | head -1 || true)  # newest = this run's
  [ -z "${tgz}" ] && { echo "no nb bundle"; return; }
  tmp="$(mktemp -d)"; tar xzf "${tgz}" -C "${tmp}" 2>/dev/null || true
  # nb5 (this version) names the op-latency timer '*cycles_servicetime*.csv'
  # (columns: t,count,max,mean,...,p95,p98,p99,p999,...,duration_unit=NANOSECONDS).
  # Fall back to the older result-success/result names.
  csv=$(find "${tmp}" -name "*cycles_servicetime*.csv" 2>/dev/null | head -1)
  [ -z "${csv}" ] && csv=$(find "${tmp}" -name "result-success.csv" 2>/dev/null | head -1)
  [ -z "${csv}" ] && csv=$(find "${tmp}" -name "result.csv" 2>/dev/null | head -1)
  [ -z "${csv}" ] && { echo "no latency CSV in bundle ($(find "${tmp}" -name '*.csv' | wc -l | tr -d ' ') csv files)"; return; }
  # Report in ms (÷1e6 when the unit is NANOSECONDS, else raw).
  awk -F, 'NR==1{for(i=1;i<=NF;i++)h[$i]=i} END{
      u=$h["duration_unit"]; d=(u=="NANOSECONDS")?1e6:((u=="MICROSECONDS")?1e3:1);
      printf "count=%s  mean=%.1fms  p95=%.1fms  p99=%.1fms  p999=%.1fms  p100(max)=%.1fms",
      $h["count"], $h["mean"]/d, $h["p95"]/d, $h["p99"]/d, $h["p999"]/d, $h["max"]/d
    }' "${csv}"
  echo "   [$(basename "${csv%.csv}")]"
}
printf 'buffered | '; parse_nb_csv "${BUF_DIR}"
printf 'direct   | '; parse_nb_csv "${DIR_DIR}"
echo
echo "Read the tail: compare p99 / p999 / p100(max). If O_DIRECT helps under disk"
echo "saturation, the buffered arm should show larger outlier p100/p999 than direct."
