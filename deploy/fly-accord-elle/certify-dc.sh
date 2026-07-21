#!/usr/bin/env bash
# Dual-DC (3+3) Elle strict-serializability certification with FAULT INJECTION.
#
# Stands up a SIX-node ferrosa cluster on fly.io spanning two logical DCs
# (dc1 = nodes 1-3, dc2 = nodes 4-6, one Raft/Accord cluster), runs the Elle
# list-append generator IN-network on the dc1 seed, and — WHILE the workload runs —
# fires a nemesis schedule against the WAN between the DCs:
#   1. dc-partition : drop ALL cross-DC traffic (ip6tables) for a window, then heal.
#   2. dc-slow      : add 200ms netem latency on the dc2 side for a window, then heal.
# These replicate ferrosa-jepsen/src/chaos/wan_bridge.rs {DcPartition,DcSlow} exactly,
# driven over `flyctl ssh` (the chaos crate's Fly executor does the same).
#
# Replication is SimpleStrategy RF=6 so every key replicates across BOTH DCs — a 3/3
# partition therefore removes quorum (Accord commits STALL, never split-brain), the
# safety property strict-serializability must preserve. The Elle CHECK runs locally
# afterwards. Teardown ALWAYS runs (trap) so machines/app/bucket never leak.
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ORG="${ORG:-ferrosa}"
REGION="${REGION:-lax}"
APP="${APP:-ferrosa-accord-elle-dc-cert}"
BUCKET="${BUCKET:-ferrosa-accord-elle-dc-${RANDOM}${RANDOM}}"
# Deterministic dirty-aware image label (see certify.sh rationale): uncommitted code
# must NOT collide with a prior committed-code image and get served stale.
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
OUT_EDN="${OUT_EDN:-${ROOT_DIR}/deploy/fly-accord-elle/elle-fly-history-dc.edn}"
# Workload: keys, ops/worker, workers, rf. rf=6 => replication spans both DCs.
# ops/worker is generous so the run outlasts the ~110s nemesis schedule below.
GEN_ARGS="${GEN_ARGS:-8 500 8 6}"

log() { printf '\n[dc-cert] %s\n' "$*" >&2; }
die() { printf '\n[dc-cert][FATAL] %s\n' "$*" >&2; exit 1; }
mid() { flyctl machine list --app "$APP" --json 2>/dev/null | jq -r ".[]|select(.name==\"$1\")|.id" | head -1; }
mip() { flyctl machine list --app "$APP" --json 2>/dev/null | jq -r ".[]|select(.name==\"$1\")|.private_ip" | head -1; }
dns() { printf '%s.vm.%s.internal' "$1" "$APP"; }

KEEP="${KEEP:-0}"
teardown() {
  if [ "$KEEP" = "1" ]; then log "KEEP=1 — NOT tearing down (app=$APP). Destroy manually!"; return; fi
  log "=== teardown (always) app=$APP ==="
  local ids; ids="$(flyctl machine list --app "$APP" --json 2>/dev/null | jq -r '.[].id' || true)"
  for id in $ids; do flyctl machine destroy "$id" --app "$APP" --force 2>&1 | tail -1 || log "WARN: machine $id leak"; done
  flyctl apps destroy "$APP" --yes 2>&1 | tail -1 || log "WARN: app $APP leak — check billing"
  flyctl storage destroy "$BUCKET" --yes 2>&1 | tail -1 || log "WARN: bucket $BUCKET leak — flyctl storage destroy $BUCKET"
}
trap teardown EXIT

command -v flyctl >/dev/null || die "flyctl not on PATH"
command -v jq >/dev/null || die "jq not on PATH"

# 1. App + Tigris bucket + S3 secrets.
log "app + tigris bucket"
flyctl apps create "$APP" --org "$ORG" 2>&1 | tail -2 || true
if ! flyctl storage status "$BUCKET" --app "$APP" >/dev/null 2>&1; then
  flyctl storage create --org "$ORG" --app "$APP" --name "$BUCKET" --yes 2>&1 | tail -5 \
    || die "tigris create failed (enable billing for org $ORG?)"
fi
flyctl secrets set --app "$APP" \
  FERROSA_S3_ENDPOINT="https://fly.storage.tigris.dev" \
  FERROSA_S3_BUCKET="$BUCKET" \
  FERROSA_S3_REGION="auto" 2>&1 | tail -2

# 2. Build + push the image (adds iptables/iproute2 for the nemeses).
log "build+push image $IMAGE (full workspace compile — ~10-15 min)"
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
if ! grep -qiE "Compiling ferrosa-cql|Compiling ferrosa " "$BUILD_LOG"; then
  log "WARN: no 'Compiling ferrosa' lines — image may be CACHED/stale. Full log: $BUILD_LOG"
fi
grep -q "image:" "$BUILD_LOG" || die "image build/push failed (no image pushed) — see $BUILD_LOG"

log "image pushed; settling 30s for fly registry propagation"
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
  --env "FERROSA_CLUSTER_NAME=ferrosa-accord-elle-dc"
  --env "FERROSA_GRAPH_ENABLED=false"
  --env "FERROSA_AUTH_ENABLED=false"
  --env "FERROSA_FORMATION_TIMEOUT_SECS=180"
)
# Six fixed host-ids (ordering: dc1 n1-3, dc2 n4-6).
HOST_IDS=(
  aa111111-1111-1111-1111-111111111111 bb222222-2222-2222-2222-222222222222
  cc333333-3333-3333-3333-333333333333 dd444444-4444-4444-4444-444444444444
  ee555555-5555-5555-5555-555555555555 ff666666-6666-6666-6666-666666666666
)
DC_OF() { [ "$1" -le 3 ] && echo dc1 || echo dc2; }

wait_ready() { # <machine-id> <name>
  local id="$1" name="$2" i
  for i in $(seq 1 60); do
    if flyctl ssh console --app "$APP" --machine "$id" --command \
        "sh -lc 'curl -sf http://localhost:9090/readyz'" 2>/dev/null | grep -q 'ready'; then
      log "  $name ready"; return 0
    fi
    sleep 6
  done
  die "$name did not become ready"
}

launch_node() { # <n> [seed_host:port]
  local n="$1" seed="${2:-}" dc; dc="$(DC_OF "$n")"
  log "run node$n ($dc)${seed:+ join $seed}"
  # Build ONE always-non-empty args array (macOS bash 3.2 + `set -u` errors on
  # expanding an EMPTY array via [@], so never do that — append conditionally).
  local run_args=(
    "$IMAGE" --app "$APP" --org "$ORG" --region "$REGION"
    --name "ferrosa-${n}" --restart always --rootfs-size 20
    --vm-cpu-kind "$CPU_KIND" --vm-cpus "$CPUS" --vm-memory "$MEM"
    "${COMMON[@]}"
    --env "FERROSA_DATA_CENTER=${dc}"
    --env "FERROSA_HOST_ID=${HOST_IDS[$((n-1))]}"
    --env "FERROSA_S3_PREFIX=accord-elle-dc/${RUN_ID}/node${n}"
  )
  [ -n "$seed" ] && run_args+=(--env "FERROSA_SEED=${seed}")
  mrun "${run_args[@]}" || die "node$n run failed"
  wait_ready "$(mid ferrosa-${n})" "ferrosa-${n}"
}

# 3. dc1 seed, then dc1 joiners, then dc2 (node4 joins the dc1 seed cross-DC to form
#    ONE cluster, node5/6 seed within dc2).
launch_node 1
SEED1="$(dns "$(mid ferrosa-1)"):17000"
launch_node 2 "$SEED1"
launch_node 3 "$SEED1"
launch_node 4 "$SEED1"                       # cross-DC join → single cluster
SEED2="$(dns "$(mid ferrosa-4)"):17000"
launch_node 5 "$SEED2"
launch_node 6 "$SEED2"

# 4. Collect ordered machine-id + private-IP arrays for the nemeses.
declare -a MIDS IPS
for n in 1 2 3 4 5 6; do
  MIDS[$n]="$(mid ferrosa-${n})"; IPS[$n]="$(mip ferrosa-${n})"
  [ -n "${MIDS[$n]}" ] && [ -n "${IPS[$n]}" ] || die "missing id/ip for ferrosa-${n}"
done
SEED_ID="${MIDS[1]}"
log "cluster up (dc1: ${IPS[1]},${IPS[2]},${IPS[3]} | dc2: ${IPS[4]},${IPS[5]},${IPS[6]}). settling 25s."
sleep 25

# --- Nemesis primitives (mirror chaos::wan_bridge; dc_a=1-3, dc_b=4-6; IPv6 => ip6tables) ---
nem() { flyctl ssh console --app "$APP" --machine "$1" --command "sh -lc '$2'" 2>&1 | tail -1; }

# Preflight: fault injection needs NET_ADMIN for ip6tables/tc inside the fly VM. If
# these fail, the "faults" would be silent no-ops and the run would be a FAKE
# healthy run — surface it loudly so we don't mis-certify.
log "nemesis preflight (ip6tables + tc must be usable, else faults are no-ops)"
PF_IPT="$(nem "${MIDS[1]}" "ip6tables -L INPUT >/dev/null 2>&1 && echo IP6TABLES_OK || echo IP6TABLES_FAIL")"
PF_TC="$(nem "${MIDS[4]}" "tc qdisc show dev eth0 >/dev/null 2>&1 && echo TC_OK || echo TC_FAIL")"
log "preflight: $PF_IPT / $PF_TC"
case "$PF_IPT $PF_TC" in
  *FAIL*) log "WARN: nemesis tools unavailable (no NET_ADMIN?) — faults will NOT inject; this would be a no-fault run. Investigate before trusting the verdict." ;;
esac
inject_dc_partition() {
  log "NEMESIS inject dc-partition (drop all cross-DC traffic)"
  for a in 1 2 3; do for b in 4 5 6; do
    nem "${MIDS[$a]}" "ip6tables -A INPUT -s ${IPS[$b]} -j DROP; ip6tables -A OUTPUT -d ${IPS[$b]} -j DROP"
  done; done
  for b in 4 5 6; do for a in 1 2 3; do
    nem "${MIDS[$b]}" "ip6tables -A INPUT -s ${IPS[$a]} -j DROP; ip6tables -A OUTPUT -d ${IPS[$a]} -j DROP"
  done; done
}
heal_all_netfilter() {
  log "NEMESIS heal dc-partition (flush ip6tables)"
  for n in 1 2 3 4 5 6; do nem "${MIDS[$n]}" "ip6tables -F INPUT; ip6tables -F OUTPUT"; done
}
inject_dc_slow() {
  log "NEMESIS inject dc-slow (200ms netem on dc2)"
  for b in 4 5 6; do nem "${MIDS[$b]}" "tc qdisc add dev eth0 root netem delay 200ms 50ms"; done
}
heal_dc_slow() {
  log "NEMESIS heal dc-slow (remove netem)"
  for b in 4 5 6; do nem "${MIDS[$b]}" "tc qdisc del dev eth0 root 2>/dev/null || true"; done
}

# 5. Nemesis SCHEDULE runs in parallel with the generator (background subshell).
nemesis_schedule() {
  sleep 25                       # steady-state baseline
  inject_dc_partition; sleep 15; heal_all_netfilter
  sleep 15
  inject_dc_slow;      sleep 30; heal_dc_slow
}
log "starting nemesis schedule (parallel) + generator (foreground)"
nemesis_schedule &
NEM_PID=$!

# 6. Run the Elle generator ON the dc1 seed, in-network, FOREGROUND.
log "run generator on seed: elle_list_append localhost:9042 /tmp/hist.edn $GEN_ARGS"
flyctl ssh console --app "$APP" --machine "$SEED_ID" --command \
  "sh -lc 'elle_list_append localhost:9042 /tmp/hist.edn ${GEN_ARGS}'" 2>&1 | tail -40 \
  || log "WARN: generator returned non-zero (faults may abort some ops — history still analyzed)"

wait "$NEM_PID" 2>/dev/null || true
heal_all_netfilter; heal_dc_slow   # belt-and-suspenders: ensure no lingering faults

# 7. Retrieve the history EDN.
log "fetch history -> $OUT_EDN"
flyctl ssh console --app "$APP" --machine "$SEED_ID" --command \
  "sh -lc 'cat /tmp/hist.edn'" > "$OUT_EDN" 2>/dev/null
lines="$(wc -l < "$OUT_EDN" | tr -d ' ')"
[ "$lines" -gt 2 ] || die "history looks empty ($lines lines) — see cluster logs"
log "history retrieved: $lines lines -> $OUT_EDN"
log "=== dual-DC fault-injection run complete; teardown next (trap) ==="
log "NEXT: cd ferrosa-jepsen/elle-checker && lein run $OUT_EDN"
