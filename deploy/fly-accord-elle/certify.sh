#!/usr/bin/env bash
# Stand up a 3-node RF=3 ferrosa cluster on fly.io (current build), run the Elle
# list-append generator IN-network on the seed, retrieve the history, and tear
# the cluster down. Teardown ALWAYS runs (trap) so machines never leak.
#
# The Elle CHECK runs locally afterwards (see certify.sh output for the EDN path).
# Reuses the proven fly-lax-benchmark deploy pattern (Tigris + fixed host-ids +
# FERROSA_SEED wiring). Small + short-lived: 3 x performance-2x / 2 GiB, one region.
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ORG="${ORG:-ferrosa}"
REGION="${REGION:-lax}"
APP="${APP:-ferrosa-accord-elle-cert}"
# Unique per run: Tigris imposes a cooldown on reusing a just-deleted bucket name.
BUCKET="${BUCKET:-ferrosa-accord-elle-${RANDOM}${RANDOM}}"
RUN_ID="$(git -C "$ROOT_DIR" rev-parse --short HEAD)"
IMAGE="registry.fly.io/${APP}:cert-${RUN_ID}"
CPU_KIND="${CPU_KIND:-performance}"
CPUS="${CPUS:-2}"
MEM="${MEM:-4096}"  # fly 'performance' CPU requires >= 2048 MiB per vCPU (2x -> 4096 min)
OUT_EDN="${OUT_EDN:-${ROOT_DIR}/deploy/fly-accord-elle/elle-fly-history.edn}"
# Workload: keys, ops/worker, workers, rf
GEN_ARGS="${GEN_ARGS:-8 200 8 3}"

log() { printf '\n[certify] %s\n' "$*" >&2; }
die() { printf '\n[certify][FATAL] %s\n' "$*" >&2; exit 1; }
mid() { flyctl machine list --app "$APP" --json 2>/dev/null | jq -r ".[]|select(.name==\"$1\")|.id" | head -1; }
dns() { printf '%s.vm.%s.internal' "$1" "$APP"; }

KEEP="${KEEP:-0}"
teardown() {
  if [ "$KEEP" = "1" ]; then log "KEEP=1 — NOT tearing down (app=$APP). Destroy manually!"; return; fi
  log "=== teardown (always) app=$APP ==="
  local ids; ids="$(flyctl machine list --app "$APP" --json 2>/dev/null | jq -r '.[].id' || true)"
  for id in $ids; do flyctl machine destroy "$id" --app "$APP" --force 2>&1 | tail -1 || log "WARN: machine $id leak"; done
  flyctl apps destroy "$APP" --yes 2>&1 | tail -1 || log "WARN: app $APP leak — check billing"
  # apps destroy does NOT remove the Tigris bucket — destroy it explicitly or it leaks.
  flyctl storage destroy "$BUCKET" --yes 2>&1 | tail -1 || log "WARN: bucket $BUCKET leak — flyctl storage destroy $BUCKET"
}
trap teardown EXIT

command -v flyctl >/dev/null || die "flyctl not on PATH"
command -v jq >/dev/null || die "jq not on PATH"

# 1. App + Tigris bucket.
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

# 2. Build + push the image (ferrosa + baked-in elle generator) from a clean ctx.
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
flyctl deploy "$BUILD_CTX" --app "$APP" --config "$BUILD_CTX/fly.toml" \
  --dockerfile "$BUILD_CTX/Dockerfile.elle" --remote-only --build-only --push \
  --image-label "cert-${RUN_ID}" 2>&1 | tail -8 || die "image build/push failed"

# Let the pushed manifest propagate before launching machines by digest —
# otherwise the first `machine run` can 404 MANIFEST_UNKNOWN (fly registry race).
log "image pushed; settling 30s for fly registry propagation"
sleep 30

# Launch a fly machine, retrying on the transient registry/manifest race.
mrun() {
  local i out
  for i in 1 2 3 4 5; do
    if out="$(flyctl machine run "$@" 2>&1)"; then printf '%s\n' "$out" | tail -3; return 0; fi
    printf '%s\n' "$out" | tail -2
    if printf '%s' "$out" | grep -qiE "MANIFEST_UNKNOWN|manifest unknown|not found|429|timeout|deadline"; then
      log "transient machine-run error (attempt $i/5) — retry in 15s"
      sleep 15
      continue
    fi
    return 1
  done
  return 1
}

# 3. Common env (trimmed fly-lax pattern for a small healthy cluster).
COMMON=(
  --env "FERROSA_DATA_DIR=/var/lib/ferrosa"
  --env "FERROSA_RAFT_DATA_DIR=/var/lib/ferrosa-raft"
  --env "FERROSA_CQL_BIND=[::]:9042"
  --env "FERROSA_WEB_BIND=[::]:9090"
  --env "FERROSA_INTERNODE_BIND=[::]:17000"
  --env "FERROSA_CLUSTER_NAME=ferrosa-accord-elle"
  --env "FERROSA_GRAPH_ENABLED=false"
  --env "FERROSA_AUTH_ENABLED=false"
  --env "FERROSA_FORMATION_TIMEOUT_SECS=120"
)
HOST_IDS=(aa111111-1111-1111-1111-111111111111 bb222222-2222-2222-2222-222222222222 cc333333-3333-3333-3333-333333333333)

wait_ready() { # <machine-id> <name>
  local id="$1" name="$2" i
  for i in $(seq 1 40); do
    if flyctl ssh console --app "$APP" --machine "$id" --command \
        "sh -lc 'curl -sf http://localhost:9090/readyz'" 2>/dev/null | grep -q 'ready'; then
      log "  $name ready"; return 0
    fi
    sleep 6
  done
  die "$name did not become ready"
}

# 4. Seed node (node1).
log "run node1 (seed)"
mrun "$IMAGE" --app "$APP" --org "$ORG" --region "$REGION" \
  --name ferrosa-1 --restart always --rootfs-size 20 \
  --vm-cpu-kind "$CPU_KIND" --vm-cpus "$CPUS" --vm-memory "$MEM" \
  "${COMMON[@]}" \
  --env "FERROSA_HOST_ID=${HOST_IDS[0]}" \
  --env "FERROSA_S3_PREFIX=accord-elle/${RUN_ID}/node1" || die "node1 run failed"
SEED_ID="$(mid ferrosa-1)"; [ -n "$SEED_ID" ] || die "no seed id"
wait_ready "$SEED_ID" ferrosa-1
SEED="$(dns "$SEED_ID"):17000"

# 5. Joiners (node2, node3).
for n in 2 3; do
  log "run node$n (join $SEED)"
  mrun "$IMAGE" --app "$APP" --org "$ORG" --region "$REGION" \
    --name "ferrosa-${n}" --restart always --rootfs-size 20 \
    --vm-cpu-kind "$CPU_KIND" --vm-cpus "$CPUS" --vm-memory "$MEM" \
    "${COMMON[@]}" \
    --env "FERROSA_HOST_ID=${HOST_IDS[$((n-1))]}" \
    --env "FERROSA_S3_PREFIX=accord-elle/${RUN_ID}/node${n}" \
    --env "FERROSA_SEED=${SEED}" || die "node$n run failed"
  wait_ready "$(mid ferrosa-${n})" "ferrosa-${n}"
done

log "cluster up. settling 20s before workload."
sleep 20

# 6. Run the Elle generator ON the seed (in-network → all peers reachable).
log "run generator on seed: elle_list_append localhost:9042 /tmp/hist.edn $GEN_ARGS"
flyctl ssh console --app "$APP" --machine "$SEED_ID" --command \
  "sh -lc 'elle_list_append localhost:9042 /tmp/hist.edn ${GEN_ARGS}'" 2>&1 | tail -6 \
  || die "generator run failed"

# 7. Retrieve the history EDN.
log "fetch history -> $OUT_EDN"
flyctl ssh console --app "$APP" --machine "$SEED_ID" --command \
  "sh -lc 'cat /tmp/hist.edn'" > "$OUT_EDN" 2>/dev/null
lines="$(wc -l < "$OUT_EDN" | tr -d ' ')"
[ "$lines" -gt 2 ] || die "history looks empty ($lines lines) — see cluster logs"
log "history retrieved: $lines lines -> $OUT_EDN"
log "=== provisioning + run complete; teardown next (trap) ==="
