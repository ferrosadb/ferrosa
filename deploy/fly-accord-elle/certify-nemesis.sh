#!/usr/bin/env bash
# Single-DC 3-node Elle strict-serializability cert WITH FAULT INJECTION.
#
# The classic Jepsen strict-serializability-under-partition test on the proven
# 3-node RF=3 fly harness: run the Elle list-append generator on the seed (node1)
# and, WHILE it runs, fire a nemesis schedule:
#   1. partition-one : isolate node3 (drop its traffic to/from node1+node2). The
#      MAJORITY (node1+node2) retains quorum and keeps committing; node3 falls
#      behind and catches up on heal. Validates NO split-brain / lost / reordered
#      writes across a partition — the safety property Elle checks.
#   2. slow          : add 200ms netem latency on node2+node3 so the coordinator's
#      commits must wait on a slow replica — validates ordering under WAN latency.
# Replicates chaos::network primitives (ip6tables/tc) over `flyctl ssh`. RF=3 so a
# 2/1 partition keeps a quorum available (unlike a 3/3 dual-DC split). Teardown
# ALWAYS runs (trap). Elle CHECK runs locally afterwards.
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ORG="${ORG:-ferrosa}"
REGION="${REGION:-lax}"
APP="${APP:-ferrosa-accord-elle-nemesis-cert}"
BUCKET="${BUCKET:-ferrosa-accord-elle-nem-${RANDOM}${RANDOM}}"
RUN_ID="$(git -C "$ROOT_DIR" rev-parse --short HEAD)"
if ! git -C "$ROOT_DIR" diff --quiet HEAD 2>/dev/null \
   || [ -n "$(git -C "$ROOT_DIR" ls-files -o --exclude-standard -- '*.rs' '*.toml')" ]; then
  WT_HASH="$( { git -C "$ROOT_DIR" diff HEAD; \
                git -C "$ROOT_DIR" ls-files -o --exclude-standard -- '*.rs' '*.toml' \
                  | LC_ALL=C sort | xargs -r cat; } 2>/dev/null \
              | shasum -a 256 | cut -c1-12 )"
  [ -z "$WT_HASH" ] && WT_HASH="$(date +%s)"
  RUN_ID="${RUN_ID}-dirty-${WT_HASH}"
fi
IMAGE="registry.fly.io/${APP}:cert-${RUN_ID}"
CPU_KIND="${CPU_KIND:-performance}"
CPUS="${CPUS:-2}"
MEM="${MEM:-4096}"
OUT_EDN="${OUT_EDN:-${ROOT_DIR}/deploy/fly-accord-elle/elle-fly-history-nemesis.edn}"
# Generous ops so the run outlasts the ~155s nemesis schedule (the coordinator-
# isolation window blocks each in-flight op up to the 30s client timeout, slowing
# throughput sharply during that phase — so over-provision).
GEN_ARGS="${GEN_ARGS:-8 700 8 3}"

log() { printf '\n[nem-cert] %s\n' "$*" >&2; }
die() { printf '\n[nem-cert][FATAL] %s\n' "$*" >&2; exit 1; }
mid() { flyctl machine list --app "$APP" --json 2>/dev/null | jq -r ".[]|select(.name==\"$1\")|.id" | head -1; }
mip() { flyctl machine list --app "$APP" --json 2>/dev/null | jq -r ".[]|select(.name==\"$1\")|.private_ip" | head -1; }
dns() { printf '%s.vm.%s.internal' "$1" "$APP"; }

KEEP="${KEEP:-0}"
teardown() {
  if [ "$KEEP" = "1" ]; then log "KEEP=1 — NOT tearing down (app=$APP)."; return; fi
  log "=== teardown (always) app=$APP ==="
  local ids; ids="$(flyctl machine list --app "$APP" --json 2>/dev/null | jq -r '.[].id' || true)"
  for id in $ids; do flyctl machine destroy "$id" --app "$APP" --force 2>&1 | tail -1 || log "WARN: machine $id leak"; done
  flyctl apps destroy "$APP" --yes 2>&1 | tail -1 || log "WARN: app $APP leak"
  flyctl storage destroy "$BUCKET" --yes 2>&1 | tail -1 || log "WARN: bucket $BUCKET leak"
}
trap teardown EXIT

command -v flyctl >/dev/null || die "flyctl not on PATH"
command -v jq >/dev/null || die "jq not on PATH"

# 1. App + bucket + secrets.
log "app + tigris bucket"
flyctl apps create "$APP" --org "$ORG" 2>&1 | tail -2 || true
if ! flyctl storage status "$BUCKET" --app "$APP" >/dev/null 2>&1; then
  flyctl storage create --org "$ORG" --app "$APP" --name "$BUCKET" --yes 2>&1 | tail -5 \
    || die "tigris create failed"
fi
flyctl secrets set --app "$APP" \
  FERROSA_S3_ENDPOINT="https://fly.storage.tigris.dev" \
  FERROSA_S3_BUCKET="$BUCKET" \
  FERROSA_S3_REGION="auto" 2>&1 | tail -2

# 2. Build + push (image includes iptables/iproute2 for the nemeses).
log "build+push image $IMAGE"
BUILD_CTX="$(mktemp -d)"
trap 'rm -rf "$BUILD_CTX"; teardown' EXIT
tar -C "$ROOT_DIR" --exclude .git --exclude target --exclude '.wt-*' \
    --exclude .cargo/registry --exclude .cargo/git -cf - . | tar -x -C "$BUILD_CTX"
cp "${ROOT_DIR}/deploy/fly-accord-elle/ferrosa-elle.Dockerfile" "$BUILD_CTX/Dockerfile.elle"
cat > "$BUILD_CTX/fly.toml" <<EOF
app = "${APP}"
primary_region = "${REGION}"
[build]
  dockerfile = "Dockerfile.elle"
EOF
BUILD_LOG="$(mktemp)"
flyctl deploy "$BUILD_CTX" --app "$APP" --config "$BUILD_CTX/fly.toml" \
  --dockerfile "$BUILD_CTX/Dockerfile.elle" --remote-only --build-only --push \
  --image-label "cert-${RUN_ID}" 2>&1 | tee "$BUILD_LOG" | grep -iE "compiling ferrosa|finished|error|image:|image size|CACHED" || true
grep -q "image:" "$BUILD_LOG" || die "image build/push failed — see $BUILD_LOG"

log "image pushed; settling 30s"
sleep 30

mrun() {
  local i out
  for i in 1 2 3 4 5; do
    if out="$(flyctl machine run "$@" 2>&1)"; then printf '%s\n' "$out" | tail -3; return 0; fi
    printf '%s\n' "$out" | tail -2
    if printf '%s' "$out" | grep -qiE "MANIFEST_UNKNOWN|manifest unknown|not found|429|timeout|deadline"; then
      log "transient machine-run error (attempt $i/5) — retry in 15s"; sleep 15; continue
    fi
    return 1
  done
  return 1
}

COMMON=(
  --env "FERROSA_DATA_DIR=/var/lib/ferrosa"
  --env "FERROSA_RAFT_DATA_DIR=/var/lib/ferrosa-raft"
  --env "FERROSA_CQL_BIND=[::]:9042"
  --env "FERROSA_WEB_BIND=[::]:9090"
  --env "FERROSA_INTERNODE_BIND=[::]:17000"
  --env "FERROSA_CLUSTER_NAME=ferrosa-accord-elle-nem"
  --env "FERROSA_GRAPH_ENABLED=false"
  --env "FERROSA_AUTH_ENABLED=false"
  --env "FERROSA_FORMATION_TIMEOUT_SECS=120"
)
HOST_IDS=(aa111111-1111-1111-1111-111111111111 bb222222-2222-2222-2222-222222222222 cc333333-3333-3333-3333-333333333333)

wait_ready() { # <machine-id> <name>
  local id="$1" name="$2" i
  for i in $(seq 1 50); do
    if flyctl ssh console --app "$APP" --machine "$id" --command \
        "sh -lc 'curl -sf http://localhost:9090/readyz'" 2>/dev/null | grep -q 'ready'; then
      log "  $name ready"; return 0
    fi
    sleep 6
  done
  die "$name did not become ready"
}

launch_node() { # <n> [seed]
  local n="$1" seed="${2:-}"
  log "run node$n${seed:+ join $seed}"
  local run_args=(
    "$IMAGE" --app "$APP" --org "$ORG" --region "$REGION"
    --name "ferrosa-${n}" --restart always --rootfs-size 20
    --vm-cpu-kind "$CPU_KIND" --vm-cpus "$CPUS" --vm-memory "$MEM"
    "${COMMON[@]}"
    --env "FERROSA_HOST_ID=${HOST_IDS[$((n-1))]}"
    --env "FERROSA_S3_PREFIX=accord-elle-nem/${RUN_ID}/node${n}"
  )
  [ -n "$seed" ] && run_args+=(--env "FERROSA_SEED=${seed}")
  mrun "${run_args[@]}" || die "node$n run failed"
  wait_ready "$(mid ferrosa-${n})" "ferrosa-${n}"
}

# 3. Seed node1, join node2/node3.
launch_node 1
SEED="$(dns "$(mid ferrosa-1)"):17000"
launch_node 2 "$SEED"
launch_node 3 "$SEED"

declare -a MIDS IPS
for n in 1 2 3; do
  MIDS[$n]="$(mid ferrosa-${n})"; IPS[$n]="$(mip ferrosa-${n})"
  [ -n "${MIDS[$n]}" ] && [ -n "${IPS[$n]}" ] || die "missing id/ip for ferrosa-${n}"
done
SEED_ID="${MIDS[1]}"
log "cluster up (n1=${IPS[1]} n2=${IPS[2]} n3=${IPS[3]}). settling 20s."
sleep 20

# --- Nemesis primitives (ip6tables/tc over flyctl ssh) ---
nem() { flyctl ssh console --app "$APP" --machine "$1" --command "sh -lc '$2'" 2>&1 | tail -1; }
log "nemesis preflight (ip6tables + tc)"
PF="$(nem "${MIDS[3]}" "ip6tables -L INPUT >/dev/null 2>&1 && tc qdisc show dev eth0 >/dev/null 2>&1 && echo NEM_OK || echo NEM_FAIL")"
log "preflight: $PF"
[ "$PF" = "NEM_OK" ] || log "WARN: nemesis tools unavailable — faults may be no-ops; investigate before trusting the verdict."

# Isolate node3 from node1+node2 (majority partition; node1+node2 keep quorum).
inject_partition_one() {
  log "NEMESIS inject partition-one (isolate node3)"
  nem "${MIDS[3]}" "ip6tables -A INPUT -s ${IPS[1]} -j DROP; ip6tables -A OUTPUT -d ${IPS[1]} -j DROP; ip6tables -A INPUT -s ${IPS[2]} -j DROP; ip6tables -A OUTPUT -d ${IPS[2]} -j DROP"
  nem "${MIDS[1]}" "ip6tables -A INPUT -s ${IPS[3]} -j DROP; ip6tables -A OUTPUT -d ${IPS[3]} -j DROP"
  nem "${MIDS[2]}" "ip6tables -A INPUT -s ${IPS[3]} -j DROP; ip6tables -A OUTPUT -d ${IPS[3]} -j DROP"
}
heal_netfilter() { log "NEMESIS heal partition"; for n in 1 2 3; do nem "${MIDS[$n]}" "ip6tables -F INPUT; ip6tables -F OUTPUT"; done; }
# Isolate node1 (the generator's coordinator) INTO the minority: node2+node3 keep
# quorum, node1 is alone. The generator, pinned to node1, then CANNOT commit — its
# COMMITs must go indeterminate (:info) once the 30s client timeout elapses. This
# proves (a) the fault genuinely bit, and (b) the minority correctly REFUSES to
# commit (no split-brain) rather than fabricating a write.
inject_isolate_coordinator() {
  log "NEMESIS inject isolate-coordinator (node1 -> minority; commits must go :info)"
  nem "${MIDS[1]}" "ip6tables -A INPUT -s ${IPS[2]} -j DROP; ip6tables -A OUTPUT -d ${IPS[2]} -j DROP; ip6tables -A INPUT -s ${IPS[3]} -j DROP; ip6tables -A OUTPUT -d ${IPS[3]} -j DROP"
  nem "${MIDS[2]}" "ip6tables -A INPUT -s ${IPS[1]} -j DROP; ip6tables -A OUTPUT -d ${IPS[1]} -j DROP"
  nem "${MIDS[3]}" "ip6tables -A INPUT -s ${IPS[1]} -j DROP; ip6tables -A OUTPUT -d ${IPS[1]} -j DROP"
}
# 200ms latency on node2+node3 so the coordinator (node1) must wait on a slow replica.
inject_slow() { log "NEMESIS inject slow (200ms on node2+node3)"; for n in 2 3; do nem "${MIDS[$n]}" "tc qdisc add dev eth0 root netem delay 200ms 50ms"; done; }
heal_slow() { log "NEMESIS heal slow"; for n in 2 3; do nem "${MIDS[$n]}" "tc qdisc del dev eth0 root 2>/dev/null || true"; done; }

nemesis_schedule() {
  sleep 25
  inject_partition_one;        sleep 20; heal_netfilter   # minority isolated; majority available (safety)
  sleep 12
  inject_isolate_coordinator;  sleep 40; heal_netfilter   # coordinator in minority >30s => :info (correct refusal)
  sleep 12
  inject_slow;                 sleep 22; heal_slow        # ordering under latency
}
log "starting nemesis schedule (parallel) + generator (foreground)"
nemesis_schedule &
NEM_PID=$!

# 4. Generator on the seed (node1, majority side), foreground.
log "run generator on seed: elle_list_append localhost:9042 /tmp/hist.edn $GEN_ARGS"
flyctl ssh console --app "$APP" --machine "$SEED_ID" --command \
  "sh -lc 'elle_list_append localhost:9042 /tmp/hist.edn ${GEN_ARGS}'" 2>&1 | tail -40 \
  || log "WARN: generator returned non-zero (faults may abort some ops — history still analyzed)"

wait "$NEM_PID" 2>/dev/null || true
heal_netfilter; heal_slow

# 5. Retrieve history.
log "fetch history -> $OUT_EDN"
flyctl ssh console --app "$APP" --machine "$SEED_ID" --command \
  "sh -lc 'cat /tmp/hist.edn'" > "$OUT_EDN" 2>/dev/null
lines="$(wc -l < "$OUT_EDN" | tr -d ' ')"
[ "$lines" -gt 2 ] || die "history looks empty ($lines lines)"
log "history retrieved: $lines lines -> $OUT_EDN"
log "=== fault-injection run complete; teardown next (trap) ==="
log "NEXT: cd ferrosa-jepsen/elle-checker && lein run $OUT_EDN"
