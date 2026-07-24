#!/bin/sh
# Per-node diagnostic sampler for the scheduler latency-freeze investigation.
#
# The 2026-07-22 B3 run showed a ~2.5 s window where the cluster stopped
# accepting reads AND writes (client throughput froze), with no raft step-down.
# The node logs were not captured, so the cause is unknown. This sampler gathers
# enough to root-cause a repeat:
#
#   $METRICS_CSV  — ~1 s samples: readyz-probe latency, thread R/D counts, and
#                   compaction/flush/s3/sstable/memtable/sched/raft gauges.
#   $THREADS_LOG  — ~250 ms line: probe_ms + per-state thread counts + readyz.
#   $STACK_DIR/*  — on ANY detected pause (readyz probe > 800 ms, or >=3 threads
#                   in uninterruptible-I/O 'D' state): a full all-thread gdb
#                   backtrace + kernel stacks + per-thread wchan + top + a
#                   /metrics snapshot. Plus one baseline dump for comparison.
#
# POSIX sh; needs: curl, procps (pidof/ps/top), gdb, root (ptrace + /proc stacks).
set -u
METRICS_CSV="${1:-/tmp/metrics.csv}"
THREADS_LOG="${2:-/tmp/threads.log}"
STACK_DIR="${3:-/tmp/stackdumps}"
DURATION="${4:-320}"
PROBE_URL="http://localhost:9090/readyz"
METRICS_URL="http://localhost:9090/metrics"
mkdir -p "$STACK_DIR"

now_ms() { date +%s%3N; }
pid_of() { pidof ferrosa 2>/dev/null | tr ' ' '\n' | head -1; }
gauge()  { printf '%s\n' "$1" | grep "^$2 " | awk '{print $2}' | head -1; }
gsum()   { printf '%s\n' "$1" | grep -E "^$2" | awk '{s+=$2} END{printf "%.0f", s+0}'; }

dump() {  # reason label -> capture everything about the current process state
  pid="$(pid_of)"; [ -n "$pid" ] || return 0
  ts="$(now_ms)"; base="$STACK_DIR/${2}-${ts}"
  {
    echo "== $(date -u +%FT%TZ) reason='$1' pid=$pid =="
    for t in /proc/"$pid"/task/*; do
      tid="$(basename "$t")"
      echo "tid=$tid state=$(awk '{print $3}' "$t/stat" 2>/dev/null) wchan=$(cat "$t/wchan" 2>/dev/null) comm=$(cat "$t/comm" 2>/dev/null)"
    done
  } > "${base}.procfs.txt" 2>&1
  for t in /proc/"$pid"/task/*; do echo "== tid=$(basename "$t") comm=$(cat "$t/comm" 2>/dev/null) =="; cat "$t/stack" 2>/dev/null; done > "${base}.kstacks.txt" 2>&1
  gdb -batch -p "$pid" -ex 'set pagination off' -ex 'set confirm off' -ex 'thread apply all bt' > "${base}.gdb.txt" 2>&1
  top -bH -n1 2>/dev/null | head -60 > "${base}.top.txt" 2>&1
  curl -s --max-time 5 "$METRICS_URL" 2>/dev/null > "${base}.metrics.txt"
}

echo "epoch_ms,probe_ms,thr_R,thr_D,compact,flush,s3up,memtable_b,sstables,sched_active,headroom,raft_term,is_leader,readyz" > "$METRICS_CSV"
: > "$THREADS_LOG"

start=$(date +%s); end=$(( start + DURATION ))
last_metrics=0; last_dump=0; dumps=0; baseline_done=0
while [ "$(date +%s)" -lt "$end" ]; do
  pid="$(pid_of)"
  t0="$(now_ms)"
  ready="$(curl -s --max-time 5 "$PROBE_URL" 2>/dev/null | grep -o '"ready":true' || echo notready)"
  probe_ms=$(( $(now_ms) - t0 ))

  R=0; D=0
  if [ -n "$pid" ]; then
    for st in /proc/"$pid"/task/*/stat; do
      case "$(awk '{print $3}' "$st" 2>/dev/null)" in R) R=$((R+1));; D) D=$((D+1));; esac
    done
  fi
  echo "$(now_ms) probe_ms=$probe_ms R=$R D=$D $ready" >> "$THREADS_LOG"

  nowsec=$(date +%s)
  # One baseline (healthy) dump ~20 s in, for comparison.
  if [ "$baseline_done" -eq 0 ] && [ $(( nowsec - start )) -ge 20 ] && [ -n "$pid" ]; then
    dump "baseline-healthy" baseline; baseline_done=1
  fi
  # PAUSE: readyz probe slow, or several threads stuck in uninterruptible I/O.
  if { [ "$probe_ms" -gt 800 ] || [ "$D" -ge 3 ]; } && [ -n "$pid" ] \
     && [ $(( nowsec - last_dump )) -ge 2 ] && [ "$dumps" -lt 25 ]; then
    dump "pause probe_ms=${probe_ms} D=${D}" "pause-probe${probe_ms}-D${D}"
    last_dump=$nowsec; dumps=$((dumps+1))
  fi

  if [ $(( nowsec - last_metrics )) -ge 1 ]; then
    b="$(curl -s --max-time 3 "$METRICS_URL" 2>/dev/null || true)"
    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
      "$(now_ms)" "$probe_ms" "$R" "$D" \
      "$(gsum "$b" 'ferrosa_.*compact.*_total')" \
      "$(gsum "$b" 'ferrosa_.*flush.*_total')" \
      "$(gsum "$b" 'ferrosa_.*s3.*(upload|object).*_total')" \
      "$(gsum "$b" 'ferrosa_.*memtable.*bytes')" \
      "$(gsum "$b" 'ferrosa_storage_stats_sstable_count')" \
      "$(gauge "$b" ferrosa_sched_pool_active)" \
      "$(gauge "$b" ferrosa_sched_consensus_headroom_cores)" \
      "$(gauge "$b" ferrosa_raft_current_term)" \
      "$(gauge "$b" ferrosa_raft_is_leader)" \
      "$ready" >> "$METRICS_CSV"
    last_metrics=$nowsec
  fi
  sleep 0.25
done
echo "diag: done ($dumps pause dumps) at $(date -u +%FT%TZ)" >> "$THREADS_LOG"
