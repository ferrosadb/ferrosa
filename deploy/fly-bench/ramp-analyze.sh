#!/usr/bin/env bash
# Analyze the O_DIRECT load-ramp + scan A/B: per arm, print the offered-vs-achieved
# throughput curve with write p50/p99/p100 per stage, locate the knee (where the
# cluster stops keeping up), and summarize reads-under-load (scan-storm) + stalls.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="${HERE}/../../target/fly-odirect-ramp"
STAGES=(${RAMP_STAGES:-2000 4000 8000 16000 32000 64000})
STAGE_SECS="${STAGE_SECS:-75}"

echo "======== O_DIRECT LOAD-RAMP + SCAN A/B ========"
echo "(write p50/p99/p100 in ms; achieved = ops actually completed / ${STAGE_SECS}s;"
echo " KNEE = first stage where achieved falls below ~80% of offered — the cluster"
echo " stopped keeping up. Compare where each arm's knee sits.)"

for arm in buffered direct; do
  adir="${OUT}/${arm}"
  echo ""
  echo "──── ${arm} ────"
  printf "  %-9s %-10s %-7s | %-8s %-8s %-8s\n" offered achieved "keep%" p50ms p99ms p100ms
  knee=""
  for X in "${STAGES[@]}"; do
    row="${adir}/stage-${X}.servicetime.csvrow"
    [ -s "$row" ] || { printf "  %-9s (no data)\n" "$X"; continue; }
    # dropwizard timer CSV columns (fixed order), values in NANOSECONDS:
    # 1=t 2=count 3=max 4=mean 5=min 6=stddev 7=p50 8=p75 9=p95 10=p98 11=p99 12=p999 13=mean_rate
    read -r offered achieved keep p50 p99 p100 < <(awk -F, -v X="$X" -v secs="$STAGE_SECS" '{
        ach=$2/secs; keep=(X>0)?ach/X*100:0;
        printf "%d %.0f %.0f %.1f %.1f %.1f", X, ach, keep, $7/1e6, $11/1e6, $3/1e6
      }' "$row")
    printf "  %-9s %-10s %-6s%% | %-8s %-8s %-8s\n" "$offered" "$achieved" "$keep" "$p50" "$p99" "$p100"
    if [ -z "$knee" ] && [ "${keep%.*}" -lt 80 ] 2>/dev/null; then knee="$X"; fi
  done
  echo "  → knee (first stage under 80% keep-up): ${knee:-not reached in tested range}"
  # reads-under-load: concurrent full-table scan latency (nb servicetime CSV row)
  scanrow="${adir}/scan.servicetime.csvrow"
  if [ -s "$scanrow" ]; then
    echo "  full-table scans under the ramp (baselines.iot):"
    tail -1 "$scanrow" | awk -F, '{if($2!="")printf "    count=%s  mean=%.0fms  p50=%.0fms  p99=%.0fms  p100=%.0fms\n",$2,$4/1e6,$7/1e6,$11/1e6,$3/1e6; else print "    (no scan rows completed)"}'
  else
    echo "  scans: no output captured"
  fi
  # runtime stalls across the ramp (max over the last stage snapshot)
  laststage="${STAGES[-1]}"
  smax=$(grep -rhE "^ferrosa_sched_runtime_stall_max_micros " "${adir}"/*.stage-${laststage}.metrics 2>/dev/null | awk '{print $2}' | sort -n | tail -1)
  sev=$(grep -rhE "^ferrosa_sched_runtime_stall_events_total " "${adir}"/*.stage-${laststage}.metrics 2>/dev/null | awk '{s+=$2} END{print s+0}')
  echo "  runtime stalls (end of ramp): events=${sev:-?} max_micros=${smax:-?}  (NB: unreliable proxy, see t_10d8df9d)"
done

echo ""
echo "READ THE CURVES:"
echo " - If DIRECT's knee is at a higher offered rate than BUFFERED, O_DIRECT lets the"
echo "   cluster sustain more write throughput before collapse (a real win)."
echo " - If DIRECT's scan latency under load is lower, O_DIRECT's cache-preservation"
echo "   helps reads-under-write (the other hypothesis)."
echo " - If the two curves + scan latencies overlap, O_DIRECT has no throughput/read"
echo "   benefit on this hardware — park it (correctness-validated, off by default)."
