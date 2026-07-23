#!/usr/bin/env bash
set -euo pipefail

ORG="${ORG:-ferrosa}"
REGION="${REGION:-lax}"
FERROSA_APP="${FERROSA_APP:-ferrosa-lax}"
CASSANDRA_APP="${CASSANDRA_APP:-ferrosa-cassandra-lax}"
BENCH_APP="${BENCH_APP:-ferrosa-bench-lax}"
TIGRIS_BUCKET="${TIGRIS_BUCKET:-ferrosa-lax}"

FERROSA_MEMORY_MB="${FERROSA_MEMORY_MB:-4096}"
FERROSA_CPUS="${FERROSA_CPUS:-2}"
FERROSA_CPU_KIND="${FERROSA_CPU_KIND:-performance}"
FERROSA_VOLUME_GB="${FERROSA_VOLUME_GB:-2}"
FERROSA_USE_VOLUMES="${FERROSA_USE_VOLUMES:-true}"
FERROSA_RAFT_ELECTION_MIN_MS="${FERROSA_RAFT_ELECTION_MIN_MS:-10000}"
FERROSA_RAFT_ELECTION_MAX_MS="${FERROSA_RAFT_ELECTION_MAX_MS:-20000}"
FERROSA_RAFT_HEARTBEAT_MS="${FERROSA_RAFT_HEARTBEAT_MS:-$((FERROSA_RAFT_ELECTION_MIN_MS / 3))}"
FERROSA_RAFT_MAX_PAYLOAD_ENTRIES="${FERROSA_RAFT_MAX_PAYLOAD_ENTRIES:-300}"
FERROSA_RAFT_RUNTIME_THREADS="${FERROSA_RAFT_RUNTIME_THREADS:-$((FERROSA_CPUS <= 2 ? 1 : 2))}"
FERROSA_DATA_RUNTIME_THREADS="${FERROSA_DATA_RUNTIME_THREADS:-$((FERROSA_CPUS <= 2 ? 2 : 4))}"
FERROSA_CQL_RUNTIME_THREADS="${FERROSA_CQL_RUNTIME_THREADS:-$((FERROSA_CPUS <= 2 ? 2 : 4))}"
FERROSA_BACKGROUND_RUNTIME_THREADS="${FERROSA_BACKGROUND_RUNTIME_THREADS:-1}"
FERROSA_COMMITLOG_BATCH_TARGET_BYTES="${FERROSA_COMMITLOG_BATCH_TARGET_BYTES:-65536}"
FERROSA_COMMITLOG_BATCH_MAX_DELAY_MICROS="${FERROSA_COMMITLOG_BATCH_MAX_DELAY_MICROS:-1000}"
BENCH_MEMORY_MB="${BENCH_MEMORY_MB:-32768}"
BENCH_CPUS="${BENCH_CPUS:-8}"
CASSANDRA_MEMORY_MB="${CASSANDRA_MEMORY_MB:-8192}"
CASSANDRA_CPUS="${CASSANDRA_CPUS:-4}"
CASSANDRA_VOLUME_GB="${CASSANDRA_VOLUME_GB:-50}"

WORKLOAD="${WORKLOAD:-activities/baselines/cql_iot.yaml}"
SCENARIO="${SCENARIO:-default}"
THREADS="${THREADS:-256}"
WARMUP_CYCLES="${WARMUP_CYCLES:-5000000}"
MEASURE_CYCLES="${MEASURE_CYCLES:-50000000}"
REPEATS="${REPEATS:-5}"
RF="${RF:-3}"
READ_CL="${READ_CL:-LOCAL_QUORUM}"
WRITE_CL="${WRITE_CL:-LOCAL_QUORUM}"
NB_JAVA_MAX_HEAP="${NB_JAVA_MAX_HEAP:-24g}"
REQUEST_TIMEOUT_SECONDS="${REQUEST_TIMEOUT_SECONDS:-30}"
CQL_PROTOCOL_COMPRESSION="${CQL_PROTOCOL_COMPRESSION:-lz4}"
EXTRA_NB_ARGS="${EXTRA_NB_ARGS:-}"
RAMP_WORKLOAD="${RAMP_WORKLOAD:-/usr/local/share/nosqlbench/cql_iot_append.yaml}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
BENCH_GIT_REF="${BENCH_GIT_REF:-origin/main}"
FERROSA_IMAGE_TAG="${FERROSA_IMAGE_TAG:-bench-${RUN_ID}}"
PROFILE_FERROSA="${PROFILE_FERROSA:-false}"
PROFILE_SECONDS="${PROFILE_SECONDS:-240}"
PROFILE_PERF_FREQ="${PROFILE_PERF_FREQ:-99}"
PROFILE_GDB_SAMPLES="${PROFILE_GDB_SAMPLES:-120}"
PROFILE_GDB_INTERVAL_SECONDS="${PROFILE_GDB_INTERVAL_SECONDS:-1}"
FERROSA_MEMORY_SNAPSHOTS="${FERROSA_MEMORY_SNAPSHOTS:-true}"
MEMORY_SNAPSHOT_INTERVAL_SECONDS="${MEMORY_SNAPSHOT_INTERVAL_SECONDS:-2}"
MEMORY_SNAPSHOT_MAX_SECONDS="${MEMORY_SNAPSHOT_MAX_SECONDS:-900}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RESULTS_DIR="${ROOT_DIR}/target/fly-bench/${RUN_ID}"
mkdir -p "$RESULTS_DIR"

require() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

app_exists() {
  flyctl status --app "$1" >/dev/null 2>&1
}

ensure_app() {
  local app="$1"
  if ! app_exists "$app"; then
    flyctl apps create "$app" --org "$ORG"
  fi
}

machine_json() {
  flyctl machines list --app "$1" --json
}

private_ip_for_name() {
  local app="$1"
  local name="$2"
  machine_json "$app" | jq -r --arg name "$name" '.[] | select(.name == $name) | .private_ip'
}

machine_id_for_name() {
  local app="$1"
  local name="$2"
  machine_json "$app" | jq -r --arg name "$name" '.[] | select(.name == $name) | .id'
}

machine_dns_for_id() {
  local app="$1"
  local id="$2"
  echo "${id}.vm.${app}.internal"
}

ferrosa_size_label() {
  if (( FERROSA_MEMORY_MB % 1024 == 0 )); then
    echo "$((FERROSA_MEMORY_MB / 1024))g"
  else
    echo "${FERROSA_MEMORY_MB}m"
  fi
}

ensure_ferrosa_volume() {
  local name="$1"
  local ids=()

  # bash 3.2 (macOS default) has no `mapfile`; read the ids with a while loop.
  while IFS= read -r _id; do
    [ -n "$_id" ] && ids+=("$_id")
  done < <(
    flyctl volumes list --app "$FERROSA_APP" --json \
      | jq -r --arg name "$name" '.[] | select(.name == $name) | .id'
  )

  if (( ${#ids[@]} == 0 )); then
    flyctl volumes create "$name" \
      --app "$FERROSA_APP" --region "$REGION" --size "$FERROSA_VOLUME_GB" \
      --vm-cpu-kind "$FERROSA_CPU_KIND" --vm-cpus "$FERROSA_CPUS" --vm-memory "$FERROSA_MEMORY_MB" \
      --yes
  elif (( ${#ids[@]} == 1 )); then
    echo "using existing volume ${name} (${ids[0]})"
  else
    echo "found multiple volumes named ${name}; run teardown-ferrosa-volumes or remove duplicates before create-ferrosa" >&2
    return 1
  fi
}

write_snapshot() {
  local app="$1"
  local label="$2"
  mkdir -p "${RESULTS_DIR}/${label}"
  flyctl machines list --app "$app" --json > "${RESULTS_DIR}/${label}/machines.json" || true
  flyctl status --app "$app" > "${RESULTS_DIR}/${label}/status.txt" || true
  # `flyctl logs --no-tail` HANGS FOREVER on an app with no machines (it blocks on
  # a log stream that never initializes), and `|| true` cannot rescue a hang — it
  # only handles a non-zero exit. Bound it (macOS has no `timeout`): run detached,
  # hard-kill after 30s so an empty-app teardown snapshot can never wedge the run.
  ( flyctl logs --app "$app" --no-tail > "${RESULTS_DIR}/${label}/logs.txt" 2>&1 ) &
  local _lp=$!
  ( sleep 30; kill "$_lp" 2>/dev/null ) &
  local _kp=$!
  wait "$_lp" 2>/dev/null || true
  kill "$_kp" 2>/dev/null || true
}

collect_node_metrics() {
  local app="$1"
  local label="$2"
  local kind="$3"
  mkdir -p "${RESULTS_DIR}/${label}/nodes"

  machine_json "$app" | jq -r '.[] | [.id, .name] | @tsv' | while IFS=$'\t' read -r id name; do
    local out="${RESULTS_DIR}/${label}/nodes/${name}-${id}.txt"
    {
      echo "### ${app} ${name} ${id}"
      flyctl machine status "$id" --app "$app" || true
      echo
      flyctl ssh console --app "$app" --machine "$id" --command "sh -lc '
        set +e
          echo \"## date\"; date -u
        echo \"## uname\"; uname -a
        echo \"## uptime\"; uptime
        echo \"## memory\"; free -m || cat /proc/meminfo
        echo \"## disk\"; df -h
        echo \"## ferrosa disk usage\"; du -sh /var/lib/ferrosa /var/lib/ferrosa/* /var/lib/ferrosa-raft /var/lib/ferrosa-raft/* 2>/dev/null || true
        echo \"## vmstat\"; vmstat 1 5
        echo \"## iostat\"; iostat -xz 1 3
        echo \"## process\"; ps aux
      '" </dev/null || true
      if [[ "$kind" == "cassandra" ]]; then
        flyctl ssh console --app "$app" --machine "$id" --command "sh -lc '
          set +e
          echo \"## nodetool status\"; /opt/cassandra/bin/nodetool -h ::1 status
          echo \"## nodetool info\"; /opt/cassandra/bin/nodetool -h ::1 info
          echo \"## nodetool tpstats\"; /opt/cassandra/bin/nodetool -h ::1 tpstats
          echo \"## nodetool tablehistograms\"; /opt/cassandra/bin/nodetool -h ::1 tablehistograms || true
          echo \"## nodetool tablestats\"; /opt/cassandra/bin/nodetool -h ::1 tablestats
        '" </dev/null || true
      elif [[ "$kind" == "ferrosa" ]]; then
        flyctl ssh console --app "$app" --machine "$id" --command "sh -lc '
          set +e
          echo \"## ferrosa metrics\"; curl --max-time 10 -g -fsS http://[::1]:9090/metrics
          echo \"## ferrosa cluster status\"; curl --max-time 10 -g -fsS http://[::1]:9090/api/cluster/status
        '" </dev/null || true
      fi
    } > "$out" 2>&1
  done
}

start_ferrosa_profiles() {
  local label="$1"
  local -n profile_pids_ref="$2"
  local profile_dir="${RESULTS_DIR}/${label}/profiles"

  profile_pids_ref=()
  [[ "$PROFILE_FERROSA" == "true" ]] || return 0

  mkdir -p "$profile_dir"
  while IFS=$'\t' read -r id name; do
    local local_log="${profile_dir}/${name}-${id}-profile-ssh.log"
    flyctl ssh console --app "$FERROSA_APP" --machine "$id" --command "sh -lc '
      set +e
      pid=\$(pidof ferrosa 2>/dev/null || pgrep -x ferrosa | head -1)
      out=/tmp/ferrosa-profile-${RUN_ID}-${name}
      rm -f \"\${out}\".*
      {
        echo \"## profile metadata\"
        date -u
        echo \"machine=${name}\"
        echo \"pid=\${pid}\"
        echo \"profile_seconds=${PROFILE_SECONDS}\"
        echo \"profile_perf_freq=${PROFILE_PERF_FREQ}\"
        echo \"## process\"
        ps -o pid,ppid,stat,etime,pcpu,pmem,args -p \"\${pid}\"
      } > \"\${out}.log\" 2>&1

      if [ -z \"\${pid}\" ]; then
        echo \"ferrosa pid not found\" >> \"\${out}.log\"
        exit 0
      fi

      if command -v perf >/dev/null 2>&1; then
        echo \"## perf record\" >> \"\${out}.log\"
        perf record -F ${PROFILE_PERF_FREQ} -g --call-graph fp \
          -p \"\${pid}\" -o \"\${out}.perf.data\" -- sleep ${PROFILE_SECONDS} \
          >> \"\${out}.log\" 2>&1
        perf_status=\$?
        echo \"perf_status=\${perf_status}\" >> \"\${out}.log\"
        if [ -s \"\${out}.perf.data\" ]; then
          perf script -i \"\${out}.perf.data\" > \"\${out}.perf.script\" 2>> \"\${out}.log\"
          gzip -f \"\${out}.perf.script\"
          gzip -c \"\${out}.perf.data\" > \"\${out}.perf.data.gz\"
        fi
      else
        echo \"perf not installed\" >> \"\${out}.log\"
      fi

      if [ ! -s \"\${out}.perf.script.gz\" ] && command -v gdb >/dev/null 2>&1; then
        echo \"## gdb stack sampling fallback\" >> \"\${out}.log\"
        i=0
        : > \"\${out}.gdb-stacks.txt\"
        while [ \"\${i}\" -lt ${PROFILE_GDB_SAMPLES} ]; do
          echo \"### sample \${i} \$(date -u +%FT%TZ)\" >> \"\${out}.gdb-stacks.txt\"
          gdb -batch -p \"\${pid}\" -ex \"thread apply all bt\" >> \"\${out}.gdb-stacks.txt\" 2>&1
          i=\$((i + 1))
          sleep ${PROFILE_GDB_INTERVAL_SECONDS}
        done
        gzip -f \"\${out}.gdb-stacks.txt\"
      fi
    '" > "$local_log" 2>&1 &
    profile_pids_ref+=("$!")
  done < <(machine_json "$FERROSA_APP" | jq -r '.[] | [.id, .name] | @tsv')
}

wait_for_profiles() {
  local -n profile_pids_ref="$1"
  (( ${#profile_pids_ref[@]} > 0 )) || return 0
  for pid in "${profile_pids_ref[@]:-}"; do
    wait "$pid" || true
  done
}

fetch_ferrosa_profiles() {
  local label="$1"
  [[ "$PROFILE_FERROSA" == "true" ]] || return 0

  local profile_dir="${RESULTS_DIR}/${label}/profiles"
  mkdir -p "$profile_dir"
  machine_json "$FERROSA_APP" | jq -r '.[] | [.id, .name] | @tsv' | while IFS=$'\t' read -r id name; do
    local remote_base="/tmp/ferrosa-profile-${RUN_ID}-${name}"
    for suffix in log perf.data.gz perf.script.gz gdb-stacks.txt.gz; do
      flyctl ssh sftp get \
        --app "$FERROSA_APP" --machine "$id" \
        "${remote_base}.${suffix}" \
        "${profile_dir}/${name}-${id}.${suffix}" || true
    done
  done
}

start_ferrosa_memory_snapshots() {
  local label="$1"
  local -n snapshot_pids_ref="$2"
  local snapshot_dir="${RESULTS_DIR}/${label}/memory-snapshots"

  snapshot_pids_ref=()
  [[ "$FERROSA_MEMORY_SNAPSHOTS" == "true" ]] || return 0

  mkdir -p "$snapshot_dir"
  while IFS=$'\t' read -r id name; do
    local local_log="${snapshot_dir}/${name}-${id}-snapshot-ssh.log"
    local local_snapshot="${snapshot_dir}/${name}-${id}.txt"
    flyctl ssh console --app "$FERROSA_APP" --machine "$id" --command "sh -lc '
      set +e
      started=\$(date +%s)
      deadline=\$((started + ${MEMORY_SNAPSHOT_MAX_SECONDS}))
      metric_filter=\"ferrosa_process_resident_memory_bytes|ferrosa_process_virtual_memory_bytes|ferrosa_process_memory_bytes|ferrosa_process_smaps_rollup_bytes|ferrosa_cgroup_memory_|ferrosa_process_cpu_seconds_total|ferrosa_process_io_|ferrosa_host_network_|ferrosa_host_block_device_|ferrosa_storage_stats_memtable_size_bytes|ferrosa_storage_stats_local_sstable_cache_bytes|ferrosa_storage_stats_sstable_size_bytes|ferrosa_storage_stats_s3_bytes|ferrosa_storage_upload_queue_depth|ferrosa_storage_upload_|ferrosa_storage_flush|ferrosa_storage_compaction_|ferrosa_storage_read_limited_rows|ferrosa_storage_sstable_rehydration_|ferrosa_net_rpc_|ferrosa_net_lane_|ferrosa_net_data_lane_|ferrosa_coordinator_|ferrosa_commitlog_|ferrosa_cql_|ferrosa_fd_\"
      while [ \$(date +%s) -lt \"\${deadline}\" ]; do
        ts=\$(date -u +%FT%TZ)
        epoch=\$(date +%s)
        pid=\$(pidof ferrosa 2>/dev/null || pgrep -x ferrosa 2>/dev/null | head -1)
        {
          echo \"### sample ts=\${ts} epoch=\${epoch} pid=\${pid:-missing}\"
          echo \"## cgroup\"
          for f in /sys/fs/cgroup/memory.current /sys/fs/cgroup/memory.max /sys/fs/cgroup/memory.events /sys/fs/cgroup/memory.stat /sys/fs/cgroup/memory.pressure /proc/pressure/memory; do
            if [ -r \"\${f}\" ]; then
              echo \"### \${f}\"
              cat \"\${f}\"
            fi
          done
          if [ -n \"\${pid}\" ] && [ -d \"/proc/\${pid}\" ]; then
            echo \"## proc_status\"
            grep -E \"^(Name|State|Pid|PPid|Threads|FDSize|Vm|Rss|Hugetlb|voluntary_ctxt_switches|nonvoluntary_ctxt_switches)\" \"/proc/\${pid}/status\" 2>/dev/null || true
            echo \"## statm\"
            cat \"/proc/\${pid}/statm\" 2>/dev/null || true
            echo \"## smaps_rollup\"
            grep -E \"^(Rss|Pss|Pss_Dirty|Shared|Private|Referenced|Anonymous|KSM|LazyFree|AnonHugePages|ShmemPmdMapped|FilePmdMapped|Shared_Hugetlb|Private_Hugetlb|Swap|SwapPss|Locked):\" \"/proc/\${pid}/smaps_rollup\" 2>/dev/null || true
            echo \"## proc_io\"
            cat \"/proc/\${pid}/io\" 2>/dev/null || true
            echo \"## fd_count\"
            ls \"/proc/\${pid}/fd\" 2>/dev/null | wc -l || true
            echo \"## thread_count\"
            ls \"/proc/\${pid}/task\" 2>/dev/null | wc -l || true
          fi
          echo \"## disk\"
          df -B1 / /var/lib/ferrosa /var/lib/ferrosa-raft 2>/dev/null || true
          du -sb /var/lib/ferrosa /var/lib/ferrosa/* /var/lib/ferrosa-raft /var/lib/ferrosa-raft/* 2>/dev/null || true
          echo \"## iostat\"
          iostat -xz 1 1 2>/dev/null || true
          echo \"## diskstats\"
          cat /proc/diskstats 2>/dev/null || true
          echo \"## metrics\"
          curl --max-time 5 -g -fsS http://[::1]:9090/metrics 2>/dev/null | grep -E \"\${metric_filter}\" || true
          echo
        }
        if [ -z \"\${pid}\" ]; then
          echo \"### ferrosa process missing at \${ts}; stopping sampler\"
          break
        fi
        sleep ${MEMORY_SNAPSHOT_INTERVAL_SECONDS}
      done
    '" > "$local_snapshot" 2> "$local_log" &
    snapshot_pids_ref+=("$!")
  done < <(machine_json "$FERROSA_APP" | jq -r '.[] | [.id, .name] | @tsv')
}

stop_ferrosa_memory_snapshots() {
  local -n snapshot_pids_ref="$1"
  (( ${#snapshot_pids_ref[@]} > 0 )) || return 0

  for pid in "${snapshot_pids_ref[@]:-}"; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
    fi
  done
  for pid in "${snapshot_pids_ref[@]:-}"; do
    wait "$pid" >/dev/null 2>&1 || true
  done
}

fetch_ferrosa_memory_snapshots() {
  local label="$1"
  [[ "$FERROSA_MEMORY_SNAPSHOTS" == "true" ]] || return 0

  # Snapshots are streamed to local files as they are sampled, so a VM OOM or
  # restart still leaves data up to the SSH disconnect. Kept as a hook for the
  # run pipeline symmetry with profiles.
  return 0
}

wait_for_cassandra_node() {
  local machine_id="$1"
  local label="$2"

  echo "waiting for Cassandra ${label} (${machine_id})"
  flyctl ssh console --app "$CASSANDRA_APP" --machine "$machine_id" --command "sh -lc '
    for i in \$(seq 1 60); do
      if /opt/cassandra/bin/nodetool -h ::1 status >/tmp/nodetool-status.txt 2>/tmp/nodetool-status.err; then
        cat /tmp/nodetool-status.txt
        exit 0
      fi
      sleep 10
    done
    cat /tmp/nodetool-status.err >&2 || true
    exit 1
  '"
}

wait_for_cassandra_ring() {
  local expected="${1:-3}"
  local seed_id
  seed_id="$(machine_id_for_name "$CASSANDRA_APP" cassandra-1)"

  echo "waiting for Cassandra ring to report ${expected} UN nodes"
  flyctl ssh console --app "$CASSANDRA_APP" --machine "$seed_id" --command "sh -lc '
    stable=0
    for i in \$(seq 1 90); do
      if /opt/cassandra/bin/nodetool -h ::1 status >/tmp/nodetool-ring.txt 2>/tmp/nodetool-ring.err; then
        cat /tmp/nodetool-ring.txt
        up=\$(awk '\''/^UN[[:space:]]/ { c++ } END { print c+0 }'\'' /tmp/nodetool-ring.txt)
        if [ \"\$up\" -eq \"$expected\" ]; then
          stable=\$((stable + 1))
          if [ \"\$stable\" -ge 3 ]; then
            exit 0
          fi
        else
          stable=0
        fi
      else
        stable=0
      fi
      sleep 10
    done
    cat /tmp/nodetool-ring.err >&2 || true
    exit 1
  '"
}

wait_for_ferrosa_http() {
  local machine_id="$1"
  local label="$2"

  echo "waiting for Ferrosa ${label} (${machine_id})"
  # Outer retry so a transient `flyctl ssh` TRANSPORT drop during node startup
  # (curl "Could not connect" then "ssh shell: Process exited with status 1")
  # retries the whole ssh instead of killing the run under set -e. Each inner
  # attempt polls for up to 60s; up to 10 attempts (~11 min) of patience.
  local attempt
  for attempt in $(seq 1 10); do
    if flyctl ssh console --app "$FERROSA_APP" --machine "$machine_id" --command "sh -lc '
        for i in \$(seq 1 20); do
          if curl --max-time 5 -g -fsS http://[::1]:9090/admin/membership-snapshot >/tmp/ferrosa-status.json 2>/tmp/ferrosa-status.err; then
            cat /tmp/ferrosa-status.json
            exit 0
          fi
          sleep 3
        done
        exit 1
      '"; then
      return 0
    fi
    echo "  ${label} readiness attempt ${attempt}/10 failed (transient ssh/curl or still starting); retrying in 6s"
    sleep 6
  done
  echo "ferrosa ${label} not ready after retries" >&2
  return 1
}

validate_ferrosa_membership() {
  local label="${1:-ferrosa}"
  mkdir -p "${RESULTS_DIR}/${label}/membership"

  while IFS=$'\t' read -r id name; do
    flyctl ssh console --app "$FERROSA_APP" --machine "$id" --command "sh -lc '
      curl --max-time 10 -g -fsS http://[::1]:9090/admin/membership-snapshot
    '" | tail -n 1 > "${RESULTS_DIR}/${label}/membership/${name}-${id}-membership.json"
    flyctl ssh console --app "$FERROSA_APP" --machine "$id" --command "sh -lc '
      curl --max-time 10 -g -fsS http://[::1]:9090/api/cluster/status || true
    '" | tail -n 1 > "${RESULTS_DIR}/${label}/membership/${name}-${id}-status.json"
    flyctl ssh console --app "$FERROSA_APP" --machine "$id" --command "sh -lc '
      curl --max-time 10 -g -fsS http://[::1]:9090/api/cluster/ring || true
    '" | tail -n 1 > "${RESULTS_DIR}/${label}/membership/${name}-${id}-ring.json"
  done < <(machine_json "$FERROSA_APP" | jq -r '.[] | [.id, .name] | @tsv')

  for file in "${RESULTS_DIR}/${label}/membership/"*-membership.json; do
    if ! jq -e '
      select(.openraft_voters | length == 3)
      | select(.live_peer_count >= 2)
      | select(.state_members | length == 3)
    ' "$file" >/dev/null; then
      echo "Ferrosa membership validation failed for ${file}; see ${RESULTS_DIR}/${label}/membership" >&2
      return 1
    fi
  done
}

preflight() {
  require flyctl
  require jq
  require docker
  flyctl orgs show "$ORG"
  ensure_app "$FERROSA_APP"
  flyctl storage list --org "$ORG"
  if ! flyctl storage status "$TIGRIS_BUCKET" --app "$FERROSA_APP" >/dev/null 2>&1; then
    echo "Tigris bucket ${TIGRIS_BUCKET} is not available yet. If this fails, enable billing for org ${ORG}." >&2
    flyctl storage create --org "$ORG" --app "$FERROSA_APP" --name "$TIGRIS_BUCKET" --yes
  fi
}

build_images() {
  ensure_app "$FERROSA_APP"
  ensure_app "$BENCH_APP"

  local source_sha
  if [[ "$BENCH_GIT_REF" == "WORKTREE" ]]; then
    source_sha="$(git -C "$ROOT_DIR" rev-parse HEAD)"
  else
    git -C "$ROOT_DIR" fetch origin main
    source_sha="$(git -C "$ROOT_DIR" rev-parse "$BENCH_GIT_REF")"
  fi
  echo "$BENCH_GIT_REF" > "${RESULTS_DIR}/ferrosa-source-ref.txt"
  echo "$source_sha" > "${RESULTS_DIR}/ferrosa-source-sha.txt"

  local build_ctx
  build_ctx="$(mktemp -d)"
  trap 'rm -rf "$build_ctx"' RETURN
  if [[ "$BENCH_GIT_REF" == "WORKTREE" ]]; then
    tar -C "$ROOT_DIR" \
      --exclude .git \
      --exclude target \
      --exclude .cargo/registry \
      --exclude .cargo/git \
      -cf - . | tar -x -C "$build_ctx"
  else
    git -C "$ROOT_DIR" archive "$BENCH_GIT_REF" | tar -x -C "$build_ctx"
  fi
  cp "${ROOT_DIR}/deploy/fly-bench/ferrosa-entrypoint.sh" "$build_ctx/ferrosa-entrypoint.sh"
  cp "${ROOT_DIR}/deploy/fly-bench/ferrosa-main.Dockerfile" "$build_ctx/Dockerfile.fly-bench"
  cat > "$build_ctx/fly.toml" <<EOF
app = "${FERROSA_APP}"
primary_region = "${REGION}"

[build]
  dockerfile = "Dockerfile.fly-bench"
EOF

  flyctl deploy "$build_ctx" \
    --app "$FERROSA_APP" \
    --config "$build_ctx/fly.toml" \
    --dockerfile "$build_ctx/Dockerfile.fly-bench" \
    --remote-only \
    --build-only \
    --push \
    --image-label "bench-${RUN_ID}"

  # fly machines are linux/amd64; force that platform so a local build on an
  # arm64 host (Apple Silicon + podman) does not produce an arm64 image that
  # exits -1 (exec format error) on fly.
  docker build \
    --platform linux/amd64 \
    -t "registry.fly.io/${BENCH_APP}:bench-${RUN_ID}" \
    "${ROOT_DIR}/deploy/fly-bench/nosqlbench"
  flyctl auth docker
  docker push "registry.fly.io/${BENCH_APP}:bench-${RUN_ID}"
}

set_ferrosa_secrets() {
  # --stage: set secrets WITHOUT a deploy. Plain `secrets set` tries to grab the
  # app config from a running machine to redeploy it; right after recreate-ferrosa
  # the app has zero machines, so it dies with "could not create a fly.toml from
  # any machines". Staged secrets are applied to the machines created next. The
  # `|| true` is a backstop: these S3 secrets are idempotent and already persist
  # on the app from the first deploy, so a transient failure must not abort.
  flyctl secrets set --stage --app "$FERROSA_APP" \
    FERROSA_S3_ENDPOINT="https://fly.storage.tigris.dev" \
    FERROSA_S3_BUCKET="$TIGRIS_BUCKET" \
    FERROSA_S3_REGION="auto" || true
}

create_ferrosa_cluster() {
  ensure_app "$FERROSA_APP"
  set_ferrosa_secrets

  local image="registry.fly.io/${FERROSA_APP}:${FERROSA_IMAGE_TAG}"
  local common_env=(
    --env "FERROSA_DATA_DIR=/var/lib/ferrosa"
    --env "FERROSA_RAFT_DATA_DIR=/var/lib/ferrosa-raft"
    --env "FERROSA_CQL_BIND=[::]:9042"
    --env "FERROSA_WEB_BIND=[::]:9090"
    --env "FERROSA_INTERNODE_BIND=[::]:17000"
    --env "FERROSA_CLUSTER_NAME=ferrosa-lax-bench"
    --env "FERROSA_GRAPH_ENABLED=false"
    --env "FERROSA_AUTH_ENABLED=false"
    --env "FERROSA_CACHE_MAX_BYTES=${FERROSA_CACHE_MAX_BYTES:-402653184}"
    --env "FERROSA_CACHE_MIN_BYTES=${FERROSA_CACHE_MIN_BYTES:-0}"
    --env "FERROSA_LOCAL_DISK_FREE_RESERVE_BYTES=${FERROSA_LOCAL_DISK_FREE_RESERVE_BYTES:-268435456}"
    --env "FERROSA_LOCAL_DISK_EVICTION_LOW_WATER_BYTES=${FERROSA_LOCAL_DISK_EVICTION_LOW_WATER_BYTES:-805306368}"
    --env "FERROSA_LOCAL_DISK_EVICTION_TARGET_FREE_BYTES=${FERROSA_LOCAL_DISK_EVICTION_TARGET_FREE_BYTES:-1207959552}"
    --env "FERROSA_FLUSH_THRESHOLD_BYTES=${FERROSA_FLUSH_THRESHOLD_BYTES:-67108864}"
    --env "FERROSA_MEMTABLE_BACKPRESSURE_BYTES=${FERROSA_MEMTABLE_BACKPRESSURE_BYTES:-536870912}"
    --env "FERROSA_FLUSH_INTERVAL_SECS=${FERROSA_FLUSH_INTERVAL_SECS:-5}"
    --env "FERROSA_URGENT_FLUSH_INTERVAL_MILLIS=${FERROSA_URGENT_FLUSH_INTERVAL_MILLIS:-100}"
    --env "FERROSA_URGENT_S3_SYNC_INTERVAL_SECS=${FERROSA_URGENT_S3_SYNC_INTERVAL_SECS:-1}"
    --env "FERROSA_COMPACTION_WORKERS=${FERROSA_COMPACTION_WORKERS:-$((FERROSA_CPUS <= 2 ? 1 : 2))}"
    --env "FERROSA_COMPACTION_VERIFY_OUTPUT=${FERROSA_COMPACTION_VERIFY_OUTPUT:-false}"
    --env "FERROSA_WRITE_VERIFY=${FERROSA_WRITE_VERIFY:-false}"
    --env "FERROSA_SSTABLE_COMPRESSION_THREADS=${FERROSA_SSTABLE_COMPRESSION_THREADS:-$((FERROSA_CPUS <= 2 ? 1 : 4))}"
    --env "FERROSA_SSTABLE_DIRECT_IO=${FERROSA_SSTABLE_DIRECT_IO:-0}"
    --env "FERROSA_RUNTIME_STALL_THRESHOLD_MS=${FERROSA_RUNTIME_STALL_THRESHOLD_MS:-300}"
    --env "FERROSA_S3_UPLOAD_WORKERS=${FERROSA_S3_UPLOAD_WORKERS:-8}"
    --env "FERROSA_S3_COMPACTION_UPLOAD_WORKERS=${FERROSA_S3_COMPACTION_UPLOAD_WORKERS:-4}"
    --env "FERROSA_S3_COMPACTION_UPLOAD_QUEUE_DEPTH=${FERROSA_S3_COMPACTION_UPLOAD_QUEUE_DEPTH:-16}"
    --env "FERROSA_S3_DELETE_WORKERS=${FERROSA_S3_DELETE_WORKERS:-2}"
    --env "FERROSA_HINTED_HANDOFF_MAX_MB=${FERROSA_HINTED_HANDOFF_MAX_MB:-64}"
    --env "FERROSA_COMMITLOG_BATCH_TARGET_BYTES=${FERROSA_COMMITLOG_BATCH_TARGET_BYTES}"
    --env "FERROSA_COMMITLOG_BATCH_MAX_DELAY_MICROS=${FERROSA_COMMITLOG_BATCH_MAX_DELAY_MICROS}"
    --env "FERROSA_FORMATION_TIMEOUT_SECS=90"
    --env "FERROSA_RAFT_HEARTBEAT_MS=${FERROSA_RAFT_HEARTBEAT_MS}"
    --env "FERROSA_RAFT_ELECTION_MIN_MS=${FERROSA_RAFT_ELECTION_MIN_MS}"
    --env "FERROSA_RAFT_ELECTION_MAX_MS=${FERROSA_RAFT_ELECTION_MAX_MS}"
    --env "FERROSA_RAFT_MAX_PAYLOAD_ENTRIES=${FERROSA_RAFT_MAX_PAYLOAD_ENTRIES}"
    --env "FERROSA_RAFT_RUNTIME_THREADS=${FERROSA_RAFT_RUNTIME_THREADS}"
    --env "FERROSA_DATA_RUNTIME_THREADS=${FERROSA_DATA_RUNTIME_THREADS}"
    --env "FERROSA_CQL_RUNTIME_THREADS=${FERROSA_CQL_RUNTIME_THREADS}"
    --env "FERROSA_BACKGROUND_RUNTIME_THREADS=${FERROSA_BACKGROUND_RUNTIME_THREADS}"
    --env "FERROSA_HEARTBEAT_INTERVAL_MS=${FERROSA_HEARTBEAT_INTERVAL_MS:-1000}"
    --env "FERROSA_HEARTBEAT_TIMEOUT_MS=${FERROSA_HEARTBEAT_TIMEOUT_MS:-10000}"
    --env "FERROSA_LANE_PENDING_STREAM_CAPACITY=${FERROSA_LANE_PENDING_STREAM_CAPACITY:-8192}"
    --env "FERROSA_MAX_STREAMS_PER_LANE=${FERROSA_MAX_STREAMS_PER_LANE:-2048}"
    --env "FERROSA_DATA_LANE_MAX_IN_FLIGHT=${FERROSA_DATA_LANE_MAX_IN_FLIGHT:-4096}"
  )
  local volume_args=()
  if [[ "$FERROSA_USE_VOLUMES" == "true" ]]; then
    for n in 1 2 3; do
      ensure_ferrosa_volume "ferrosa_data_${n}"
    done
  fi

  volume_args=()
  if [[ "$FERROSA_USE_VOLUMES" == "true" ]]; then
    volume_args=(--volume "ferrosa_data_1:/var/lib/ferrosa")
  fi
  flyctl machine run "$image" \
    --app "$FERROSA_APP" --org "$ORG" --region "$REGION" \
    --name ferrosa-1 --restart always --rootfs-size 20 \
    --vm-cpu-kind "$FERROSA_CPU_KIND" --vm-cpus "$FERROSA_CPUS" --vm-memory "$FERROSA_MEMORY_MB" \
    "${volume_args[@]}" \
    "${common_env[@]}" \
    --env "FERROSA_HOST_ID=aa111111-1111-1111-1111-111111111111" \
    --env "FERROSA_S3_PREFIX=ferrosa-lax-bench/${RUN_ID}/node1"

  local seed_id
  seed_id="$(machine_id_for_name "$FERROSA_APP" ferrosa-1)"
  wait_for_ferrosa_http "$seed_id" ferrosa-1

  local seed
  seed="$(machine_dns_for_id "$FERROSA_APP" "$seed_id"):17000"

  for n in 2 3; do
    local host_id
    if [[ "$n" == "2" ]]; then
      host_id="bb222222-2222-2222-2222-222222222222"
    else
      host_id="cc333333-3333-3333-3333-333333333333"
    fi
    volume_args=()
    if [[ "$FERROSA_USE_VOLUMES" == "true" ]]; then
      volume_args=(--volume "ferrosa_data_${n}:/var/lib/ferrosa")
    fi
    flyctl machine run "$image" \
      --app "$FERROSA_APP" --org "$ORG" --region "$REGION" \
      --name "ferrosa-${n}" --restart always --rootfs-size 20 \
      --vm-cpu-kind "$FERROSA_CPU_KIND" --vm-cpus "$FERROSA_CPUS" --vm-memory "$FERROSA_MEMORY_MB" \
      "${volume_args[@]}" \
      "${common_env[@]}" \
      --env "FERROSA_HOST_ID=${host_id}" \
      --env "FERROSA_S3_PREFIX=ferrosa-lax-bench/${RUN_ID}/node${n}" \
      --env "FERROSA_SEED=${seed}"

    local node_id
    node_id="$(machine_id_for_name "$FERROSA_APP" "ferrosa-${n}")"
    wait_for_ferrosa_http "$node_id" "ferrosa-${n}"
  done

  flyctl secrets deploy --app "$FERROSA_APP" --detach || true
  validate_ferrosa_membership "ferrosa-create"
}

teardown_ferrosa_machines() {
  if app_exists "$FERROSA_APP"; then
    write_snapshot "$FERROSA_APP" ferrosa-before-recreate
    machine_json "$FERROSA_APP" | jq -r '.[].id' | while read -r id; do
      flyctl machine destroy "$id" --app "$FERROSA_APP" --force
    done
  fi
}

teardown_ferrosa_volumes() {
  if app_exists "$FERROSA_APP"; then
    flyctl volumes list --app "$FERROSA_APP" --json \
      | jq -r '.[] | select(.name | startswith("ferrosa_data_")) | .id' \
      | while read -r id; do
        flyctl volumes destroy "$id" --app "$FERROSA_APP" --yes
      done
  fi
}

create_bench_node() {
  ensure_app "$BENCH_APP"
  flyctl machine run "registry.fly.io/${BENCH_APP}:bench-${RUN_ID}" \
    --app "$BENCH_APP" --org "$ORG" --region "$REGION" \
    --name nosqlbench-1 --restart always --rootfs-persist always --rootfs-size 20 \
    --vm-cpu-kind performance --vm-cpus "$BENCH_CPUS" --vm-memory "$BENCH_MEMORY_MB" \
    --env "NB_JAVA_MAX_HEAP=${NB_JAVA_MAX_HEAP}"
}

create_cassandra_cluster() {
  ensure_app "$CASSANDRA_APP"
  for n in 1 2 3; do
    flyctl volumes create "cassandra_data_${n}" \
      --app "$CASSANDRA_APP" --region "$REGION" --size "$CASSANDRA_VOLUME_GB" --yes
  done

  local image="cassandra:5.0"
  flyctl machine run "$image" \
    --app "$CASSANDRA_APP" --org "$ORG" --region "$REGION" \
    --name cassandra-1 --restart always \
    --vm-cpu-kind performance --vm-cpus "$CASSANDRA_CPUS" --vm-memory "$CASSANDRA_MEMORY_MB" \
    --volume "cassandra_data_1:/var/lib/cassandra" \
    --file-local "/usr/local/bin/fly-cassandra-entrypoint=${ROOT_DIR}/deploy/fly-bench/cassandra-entrypoint.sh" \
    --entrypoint /usr/local/bin/fly-cassandra-entrypoint \
    --env "CASSANDRA_CLUSTER_NAME=ferrosa-baseline-lax" \
    --env "CASSANDRA_DC=datacenter1" \
    --env "CASSANDRA_RACK=rack1" \
    --env "CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch"

  local seed_id
  seed_id="$(machine_id_for_name "$CASSANDRA_APP" cassandra-1)"
  wait_for_cassandra_node "$seed_id" cassandra-1

  local seed_ip
  seed_ip="$(private_ip_for_name "$CASSANDRA_APP" cassandra-1)"
  for n in 2 3; do
    flyctl machine run "$image" \
      --app "$CASSANDRA_APP" --org "$ORG" --region "$REGION" \
      --name "cassandra-${n}" --restart always \
      --vm-cpu-kind performance --vm-cpus "$CASSANDRA_CPUS" --vm-memory "$CASSANDRA_MEMORY_MB" \
      --volume "cassandra_data_${n}:/var/lib/cassandra" \
      --file-local "/usr/local/bin/fly-cassandra-entrypoint=${ROOT_DIR}/deploy/fly-bench/cassandra-entrypoint.sh" \
      --entrypoint /usr/local/bin/fly-cassandra-entrypoint \
      --env "CASSANDRA_CLUSTER_NAME=ferrosa-baseline-lax" \
      --env "CASSANDRA_DC=datacenter1" \
      --env "CASSANDRA_RACK=rack1" \
      --env "CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch" \
      --env "CASSANDRA_SEEDS=${seed_ip}"
    local node_id
    node_id="$(machine_id_for_name "$CASSANDRA_APP" "cassandra-${n}")"
    wait_for_cassandra_node "$node_id" "cassandra-${n}"
  done
  wait_for_cassandra_ring 3
}

run_target() {
  local target="$1"
  local app="$2"
  local machine_prefix="$3"

  local contact_points
  contact_points="$(
    machine_json "$app" \
      | jq -r --arg p "$machine_prefix" --arg app "$app" '[.[] | select(.name | startswith($p)) | .id + ".vm." + $app + ".internal"] | join(",")'
  )"

  local bench_machine
  bench_machine="$(machine_id_for_name "$BENCH_APP" nosqlbench-1)"

  write_snapshot "$app" "${target}-before"
  if [[ "$target" == ferrosa-* ]]; then
    collect_node_metrics "$app" "${target}-before" ferrosa
  elif [[ "$target" == cassandra-* ]]; then
    collect_node_metrics "$app" "${target}-before" cassandra
  fi
  # NB: start_ferrosa_{profiles,memory_snapshots} use bash-4.3 `local -n`
  # namerefs, which the macOS default bash 3.2 rejects. Guard the CALL SITES on
  # the enable flags (default false here) so the nameref code never executes when
  # the feature is off. (If you enable either on a bash-3.2 host, convert those
  # four functions off namerefs first.)
  local profile_pids=()
  if [[ "$target" == ferrosa-* && "$PROFILE_FERROSA" == "true" ]]; then
    start_ferrosa_profiles "$target" profile_pids
  fi
  local memory_snapshot_pids=()
  if [[ "$target" == ferrosa-* && "$FERROSA_MEMORY_SNAPSHOTS" == "true" ]]; then
    start_ferrosa_memory_snapshots "$target" memory_snapshot_pids
  fi
  local bench_status=0
  flyctl ssh console --app "$BENCH_APP" --machine "$bench_machine" --command \
    "sh -lc \"TARGET_NAME='${target}' CONTACT_POINTS='${contact_points}' RUN_ID='${RUN_ID}' WORKLOAD='${WORKLOAD}' SCENARIO='${SCENARIO}' THREADS='${THREADS}' WARMUP_CYCLES='${WARMUP_CYCLES}' MEASURE_CYCLES='${MEASURE_CYCLES}' REPEATS='${REPEATS}' RF='${RF}' READ_CL='${READ_CL}' WRITE_CL='${WRITE_CL}' NB_JAVA_MAX_HEAP='${NB_JAVA_MAX_HEAP}' REQUEST_TIMEOUT_SECONDS='${REQUEST_TIMEOUT_SECONDS}' CQL_PROTOCOL_COMPRESSION='${CQL_PROTOCOL_COMPRESSION}' EXTRA_NB_ARGS='${EXTRA_NB_ARGS}' run-nb\"" \
    || bench_status=$?
  if [[ "$target" == ferrosa-* && "$FERROSA_MEMORY_SNAPSHOTS" == "true" ]]; then
    stop_ferrosa_memory_snapshots memory_snapshot_pids
    fetch_ferrosa_memory_snapshots "$target"
  fi
  if [[ "$target" == ferrosa-* && "$PROFILE_FERROSA" == "true" ]]; then
    wait_for_profiles profile_pids
    fetch_ferrosa_profiles "$target"
  fi
  write_snapshot "$app" "${target}-after"
  if [[ "$target" == ferrosa-* ]]; then
    collect_node_metrics "$app" "${target}-after" ferrosa
  elif [[ "$target" == cassandra-* ]]; then
    collect_node_metrics "$app" "${target}-after" cassandra
  fi

  flyctl ssh sftp get \
    --app "$BENCH_APP" --machine "$bench_machine" \
    "/results/${RUN_ID}-${target}.tgz" \
    "${RESULTS_DIR}/${RUN_ID}-${target}.tgz" || true
  return "$bench_status"
}

run_ferrosa_ramp() {
  local size_label
  size_label="$(ferrosa_size_label)"
  local stages=(
    "16:1000:1000:1"
    "32:10000:10000:1"
    "64:100000:100000:1"
    "128:1000000:1000000:1"
    "256:5000000:5000000:1"
  )

  for stage in "${stages[@]}"; do
    IFS=: read -r stage_threads stage_warmup stage_main stage_repeats <<<"$stage"
    (
      WORKLOAD="$RAMP_WORKLOAD"
      SCENARIO=default
      THREADS="$stage_threads"
      WARMUP_CYCLES="$stage_warmup"
      MEASURE_CYCLES="$stage_main"
      REPEATS="$stage_repeats"
      run_target "ferrosa-${size_label}-t${stage_threads}-c${stage_main}" "$FERROSA_APP" "ferrosa-"
    )
  done
}

run_ferrosa_t128() {
  local size_label
  size_label="$(ferrosa_size_label)"
  # Env-overridable so a harsher A/B can drive more threads / cycles without a
  # code change (the O_DIRECT freeze-repro needs sustained heavy write load).
  local threads="${THREADS:-128}"
  local warmup="${WARMUP_CYCLES:-1000000}"
  local measure="${MEASURE_CYCLES:-1000000}"
  (
    WORKLOAD="$RAMP_WORKLOAD"
    SCENARIO=default
    THREADS="$threads"
    WARMUP_CYCLES="$warmup"
    MEASURE_CYCLES="$measure"
    REPEATS=1
    run_target "ferrosa-${size_label}-t${threads}-c${measure}" "$FERROSA_APP" "ferrosa-"
  )
}

run_cassandra_ramp() {
  wait_for_cassandra_ring 3

  local stages=(
    "16:1000:1000:1"
    "32:10000:10000:1"
    "64:100000:100000:1"
    "128:1000000:1000000:1"
    "256:5000000:5000000:1"
  )

  for stage in "${stages[@]}"; do
    IFS=: read -r stage_threads stage_warmup stage_main stage_repeats <<<"$stage"
    (
      WORKLOAD="$RAMP_WORKLOAD"
      SCENARIO=default
      THREADS="$stage_threads"
      WARMUP_CYCLES="$stage_warmup"
      MEASURE_CYCLES="$stage_main"
      REPEATS="$stage_repeats"
      run_target "cassandra-8g-t${stage_threads}-c${stage_main}" "$CASSANDRA_APP" "cassandra-"
    )
  done
}

teardown_cassandra() {
  if app_exists "$CASSANDRA_APP"; then
    write_snapshot "$CASSANDRA_APP" cassandra-final
    flyctl apps destroy "$CASSANDRA_APP" --yes
  fi
}

case "${1:-}" in
  preflight) preflight ;;
  build-images) build_images ;;
  create-ferrosa) create_ferrosa_cluster ;;
  teardown-ferrosa) teardown_ferrosa_machines ;;
  teardown-ferrosa-volumes) teardown_ferrosa_volumes ;;
  recreate-ferrosa)
    teardown_ferrosa_machines
    teardown_ferrosa_volumes
    create_ferrosa_cluster
    ;;
  create-bench) create_bench_node ;;
  create-cassandra) create_cassandra_cluster ;;
  run-ferrosa) run_target "ferrosa-$(ferrosa_size_label)" "$FERROSA_APP" "ferrosa-" ;;
  run-ferrosa-t128) run_ferrosa_t128 ;;
  run-ferrosa-ramp) run_ferrosa_ramp ;;
  run-cassandra) run_target "cassandra-8g" "$CASSANDRA_APP" "cassandra-" ;;
  run-cassandra-ramp) run_cassandra_ramp ;;
  teardown-cassandra) teardown_cassandra ;;
  full)
    preflight
    build_images
    create_ferrosa_cluster
    create_bench_node
    run_target "ferrosa-$(ferrosa_size_label)" "$FERROSA_APP" "ferrosa-"
    create_cassandra_cluster
    run_target "cassandra-8g" "$CASSANDRA_APP" "cassandra-"
    teardown_cassandra
    ;;
  *)
    cat >&2 <<EOF
usage: $0 preflight|build-images|create-ferrosa|teardown-ferrosa|teardown-ferrosa-volumes|recreate-ferrosa|create-bench|create-cassandra|run-ferrosa|run-ferrosa-t128|run-ferrosa-ramp|run-cassandra|run-cassandra-ramp|teardown-cassandra|full

Results are written under: ${RESULTS_DIR}
EOF
    exit 2
    ;;
esac
