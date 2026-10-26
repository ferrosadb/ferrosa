#!/bin/sh
# Software user CPU sampling only; PMU counters are excluded after measured overhead.
set -eu
out=$1
seconds=$2
mode=${3:-stats}
mkdir -p "$out"
pid=$(pgrep -x ferrosa)
test "$(printf '%s\n' "$pid" | wc -l)" -eq 1
export LC_ALL=C
date -u +%FT%TZ > "$out/started-at.txt"
printf '%s\n' "$pid" > "$out/engine-pid.txt"
uname -a > "$out/uname.txt"
getconf CLK_TCK > "$out/clock-ticks.txt"
perf --version > "$out/perf-version.txt"
sha256sum /usr/local/bin/ferrosa > "$out/binary-sha256.txt"
ps -L -p "$pid" -o pid,tid,comm > "$out/threads-before.txt"
cat /proc/cpuinfo > "$out/cpuinfo.txt"
cat /proc/mounts > "$out/mounts.txt"
cat /proc/diskstats > "$out/diskstats-before.txt"
cat /proc/net/dev > "$out/netdev-before.txt"
cat /proc/net/snmp > "$out/snmp-before.txt"
cat /proc/net/netstat > "$out/netstat-before.txt"
curl -fsS http://127.0.0.1:9090/metrics > "$out/metrics-before.prom" 2> "$out/metrics-before.err" || true

workers=""
profile_workers=""
start() {
    # Detach each group and restore signals ignored by a noninteractive shell.
    python3 -c 'import os,signal,sys; os.setsid(); signal.signal(signal.SIGINT, signal.SIG_DFL); os.execvp(sys.argv[1], sys.argv[1:])' "$@" &
    last_worker=$!
    workers="$workers $last_worker"
}
start sh -c 'exec pidstat -h -t -u -r -d -w -p "$1" 1 "$2" > "$3" 2>&1' sh "$pid" "$seconds" "$out/pidstat.txt"
start sh -c 'exec iostat -x -y 1 "$1" > "$2" 2>&1' sh "$seconds" "$out/iostat.txt"
start sh -c 'exec mpstat -P ALL 1 "$1" > "$2" 2>&1' sh "$seconds" "$out/mpstat.txt"
start sh -c 'exec vmstat -w 1 "$1" > "$2" 2>&1' sh "$seconds" "$out/vmstat.txt"
start sh -c 'exec sar -n DEV,TCP,ETCP 1 "$1" > "$2" 2>&1' sh "$seconds" "$out/network.txt"
stats_workers=$workers
if [ "$mode" = profile ]; then
    start sh -c 'exec perf record -e cpu-clock:u -F 49 -g --call-graph fp -p "$1" -o "$3/cpu.data" -- sleep "$2" > "$3/cpu-record.log" 2>&1' sh "$pid" "$seconds" "$out"
    profile_workers="$profile_workers $last_worker"
fi
touch "$out/ready"
python3 - "$pid" "$seconds" "$out" <<'SAMPLE'
import json, pathlib, subprocess, sys, time, urllib.request
pid, seconds, out = sys.argv[1], int(sys.argv[2]), pathlib.Path(sys.argv[3])
began = time.monotonic()
with (out / "proc-sockets.jsonl").open("w") as stream:
    for index in range(seconds):
        if (out / "stop").exists():
            break
        sample = {"wall_time_ns": time.time_ns(), "elapsed_s": time.monotonic() - began, "process": {}, "tasks": {}, "errors": {}}
        def read(path):
            try: return pathlib.Path(path).read_text()
            except OSError as error:
                sample["errors"][str(path)] = type(error).__name__
                return None
        for name in ["io", "status", "schedstat", "stat"]:
            sample["process"][name] = read(f"/proc/{pid}/{name}")
        for task in pathlib.Path(f"/proc/{pid}/task").iterdir():
            sample["tasks"][task.name] = {name: read(task / name) for name in ["comm", "wchan", "schedstat", "stat"]}
        for path in ["/proc/stat", "/proc/loadavg", "/proc/pressure/cpu", "/proc/pressure/io", "/proc/pressure/memory", "/sys/fs/cgroup/cpu.stat", "/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/io.stat"]:
            sample[path] = read(path)
        try:
            result = subprocess.run(["ss", "-tinp"], capture_output=True, text=True, timeout=5)
            sample["sockets"] = result.stdout
            sample["socket_exit"] = result.returncode
        except subprocess.TimeoutExpired:
            sample["errors"]["ss"] = "timeout"
        try:
            with urllib.request.urlopen("http://127.0.0.1:9090/metrics", timeout=2) as response:
                (out / f"metrics-{index}.prom").write_bytes(response.read())
        except Exception as error:
            sample["errors"]["metrics"] = type(error).__name__
        stream.write(json.dumps(sample) + "\n")
        stream.flush()
        time.sleep(max(0, began + index + 1 - time.monotonic()))
SAMPLE

stopped=0
if [ -e "$out/stop" ]; then
    stopped=1
    python3 - "$stats_workers" "$profile_workers" <<'STOP'
import os, signal, sys
for group, sig in [(sys.argv[1], signal.SIGTERM), (sys.argv[2], signal.SIGINT)]:
    for pid in group.split():
        try: os.killpg(int(pid), sig)
        except ProcessLookupError: pass
STOP
fi
failed=0
for worker in $workers; do
    if wait "$worker"; then
        printf '%s ok\n' "$worker"
    else
        status=$?
        if [ "$stopped" -eq 1 ] && { [ "$status" -eq 130 ] || [ "$status" -eq 143 ]; }; then
            printf '%s stopped after requested capture (exit %s)\n' "$worker" "$status"
        else
            printf '%s failed %s\n' "$worker" "$status"
            failed=1
        fi
    fi
done > "$out/collector-exits.txt"
ps -L -p "$pid" -o pid,tid,comm > "$out/threads-after.txt"
cat /proc/diskstats > "$out/diskstats-after.txt"
cat /proc/net/dev > "$out/netdev-after.txt"
cat /proc/net/snmp > "$out/snmp-after.txt"
cat /proc/net/netstat > "$out/netstat-after.txt"
curl -fsS http://127.0.0.1:9090/metrics > "$out/metrics-after.prom" 2> "$out/metrics-after.err" || true
if [ "$mode" = profile ]; then
    perf script -i "$out/cpu.data" > "$out/cpu.perf-script" 2> "$out/cpu-script.err" || true
    perf buildid-list -i "$out/cpu.data" > "$out/buildids.txt" 2>&1 || true
fi
date -u +%FT%TZ > "$out/finished-at.txt"
touch "$out/done"
exit "$failed"
