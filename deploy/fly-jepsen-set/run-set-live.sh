#!/usr/bin/env bash
# Live `set` durability run (JE-6): ferrosa-jepsen's orchestrator, on this host, drives a
# THROWAWAY 3-node Fly cluster under partition-halves and kill-minority, and writes
# results.json, the history and the `.book.json` per combination into $OUT_DIR.
#
# Shape:
#   - one Fly app + Tigris bucket, created here and ALWAYS destroyed by the EXIT trap
#     (KEEP=1 skips teardown for debugging; say so if you use it);
#   - 3 machines, RF=3, node1 the seed; each node advertises 127.0.0.1:3904N as its CQL
#     broadcast so the driver on this host dials every node through its own `fly proxy`;
#   - nemeses run through `fly ssh console` / the Machines API (FERROSA_JEPSEN_FLY_*), aimed at
#     the machines' private IPv6 addresses (FERROSA_JEPSEN_FLY_NODE_IPS), never at loopback.
#
# No credential crosses argv: flyctl uses its own login, and the Tigris keys are app secrets
# that `fly storage create` installs and the entrypoint maps into FERROSA_S3_*.
#
# Required: JEPSEN_BIN (a built `ferrosa-jepsen` binary), OUT_DIR.
# Never point this at ports 19042-19044 or 9042 on this host: those are live clusters.
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ORG="${ORG:-ferrosa}"
REGION="${REGION:-lax}"
SUFFIX="${SUFFIX:-$(date -u +%m%d%H%M)}"
APP="${APP:-ferrosa-je6-set-${SUFFIX}}"
BUCKET="${BUCKET:-ferrosa-je6-set-${SUFFIX}}"
CPU_KIND="${CPU_KIND:-performance}"
CPUS="${CPUS:-2}"
MEM="${MEM:-4096}"
NEMESES="${NEMESES:-partition-halves kill-minority}"
RUN_SECS="${RUN_SECS:-150}"
PORTS=(39042 39043 39044)
: "${JEPSEN_BIN:?set JEPSEN_BIN to a built ferrosa-jepsen binary}"
: "${OUT_DIR:?set OUT_DIR to the evidence directory}"
mkdir -p "$OUT_DIR"

log() { printf '\n[je6] %s\n' "$*" >&2; }
die() { printf '\n[je6][FATAL] %s\n' "$*" >&2; exit 1; }
mid() { flyctl machine list --app "$APP" --json | jq -r ".[]|select(.name==\"$1\")|.id" | head -1; }
mip() { flyctl machine list --app "$APP" --json | jq -r ".[]|select(.name==\"$1\")|.private_ip" | head -1; }

for p in "${PORTS[@]}"; do
  case "$p" in 19042|19043|19044|9042) die "port $p belongs to a live cluster";; esac
  if lsof -nP -iTCP:"$p" -sTCP:LISTEN >/dev/null 2>&1; then die "local port $p is already in use"; fi
done

PIDS=()
KEEP="${KEEP:-0}"
teardown() {
  for pid in "${PIDS[@]:-}"; do [ -n "$pid" ] && kill "$pid" 2>/dev/null; done
  if [ "$KEEP" = "1" ]; then log "KEEP=1: NOT tearing down app=$APP bucket=$BUCKET"; return; fi
  log "teardown app=$APP bucket=$BUCKET"
  local ids id
  ids="$(flyctl machine list --app "$APP" --json 2>/dev/null | jq -r '.[].id')"
  for id in $ids; do
    flyctl machine destroy "$id" --app "$APP" --force >&2 || log "LEAK: machine $id not destroyed"
  done
  flyctl apps destroy "$APP" --yes >&2 || log "LEAK: app $APP not destroyed"
  flyctl storage destroy "$BUCKET" --yes >&2 || log "LEAK: bucket $BUCKET not destroyed"
}
trap teardown EXIT

command -v flyctl >/dev/null || die "flyctl not on PATH"
command -v jq >/dev/null || die "jq not on PATH"
[ -x "$JEPSEN_BIN" ] || die "JEPSEN_BIN $JEPSEN_BIN is not executable"

log "app $APP + bucket $BUCKET in $ORG/$REGION"
flyctl apps create "$APP" --org "$ORG" >&2 || die "app create failed"
flyctl storage create --org "$ORG" --app "$APP" --name "$BUCKET" --yes >&2 || die "tigris create failed"
flyctl secrets set --app "$APP" --stage \
  FERROSA_S3_ENDPOINT="https://fly.storage.tigris.dev" \
  FERROSA_S3_BUCKET="$BUCKET" FERROSA_S3_REGION="auto" >&2 || die "secrets set failed"

COMMIT="$(git -C "$ROOT_DIR" rev-parse HEAD)"
git -C "$ROOT_DIR" diff --quiet HEAD || die "worktree is dirty; the evidence must name a commit"
IMAGE="registry.fly.io/${APP}:je6-${COMMIT:0:12}"
BUILD_CTX="$(mktemp -d)"
trap 'rm -rf "$BUILD_CTX"; teardown' EXIT
log "build $IMAGE from $COMMIT"
tar -C "$ROOT_DIR" --exclude .git --exclude target --exclude '.wt-*' -cf - . | tar -x -C "$BUILD_CTX"
cp "$ROOT_DIR/deploy/fly-accord-elle/ferrosa-elle.Dockerfile" "$BUILD_CTX/Dockerfile.je6"
printf 'app = "%s"\nprimary_region = "%s"\n[build]\n  dockerfile = "Dockerfile.je6"\n' \
  "$APP" "$REGION" > "$BUILD_CTX/fly.toml"
flyctl deploy "$BUILD_CTX" --app "$APP" --config "$BUILD_CTX/fly.toml" \
  --dockerfile "$BUILD_CTX/Dockerfile.je6" --remote-only --build-only --push \
  --image-label "je6-${COMMIT:0:12}" > "$OUT_DIR/build.log" 2>&1 || die "image build failed; see $OUT_DIR/build.log"
printf '%s\n' "$COMMIT" > "$OUT_DIR/ferrosa-commit.txt"

HOST_IDS=(aa111111-1111-1111-1111-111111111111 bb222222-2222-2222-2222-222222222222 cc333333-3333-3333-3333-333333333333)
wait_ready() {
  local id="$1" name="$2" i
  for i in $(seq 1 50); do
    if flyctl ssh console --app "$APP" --machine "$id" --command \
        "sh -lc 'curl -sf http://localhost:9090/readyz'" 2>/dev/null | grep -q ready; then
      log "$name ready"; return 0
    fi
    sleep 6
  done
  die "$name did not become ready"
}
launch_node() {
  local n="$1" seed="${2:-}"
  local args=(
    "$IMAGE" --app "$APP" --org "$ORG" --region "$REGION"
    --name "ferrosa-${n}" --restart always --rootfs-size 20
    --vm-cpu-kind "$CPU_KIND" --vm-cpus "$CPUS" --vm-memory "$MEM"
    --env "FERROSA_DATA_DIR=/var/lib/ferrosa"
    --env "FERROSA_RAFT_DATA_DIR=/var/lib/ferrosa-raft"
    --env "FERROSA_CQL_BIND=[::]:9042"
    --env "FERROSA_WEB_BIND=[::]:9090"
    --env "FERROSA_INTERNODE_BIND=[::]:17000"
    --env "FERROSA_CQL_BROADCAST=127.0.0.1:${PORTS[$((n-1))]}"
    --env "FERROSA_CLUSTER_NAME=ferrosa-je6-set"
    --env "FERROSA_GRAPH_ENABLED=false"
    --env "FERROSA_AUTH_ENABLED=false"
    --env "FERROSA_FORMATION_TIMEOUT_SECS=120"
    --env "FERROSA_HOST_ID=${HOST_IDS[$((n-1))]}"
    --env "FERROSA_S3_PREFIX=je6-set/${SUFFIX}/node${n}"
  )
  [ -n "$seed" ] && args+=(--env "FERROSA_SEED=${seed}")
  log "run node$n${seed:+ (join $seed)}"
  flyctl machine run "${args[@]}" >&2 || die "node$n run failed"
  wait_ready "$(mid "ferrosa-${n}")" "ferrosa-${n}"
}
launch_node 1
SEED="$(mid ferrosa-1).vm.${APP}.internal:17000"
launch_node 2 "$SEED"
launch_node 3 "$SEED"

MIDS=(); IPS=()
for n in 1 2 3; do
  MIDS+=("$(mid "ferrosa-${n}")"); IPS+=("$(mip "ferrosa-${n}")")
  [ -n "${MIDS[$((n-1))]}" ] && [ -n "${IPS[$((n-1))]}" ] || die "missing id/ip for ferrosa-${n}"
done
printf 'app=%s\nregion=%s\nmachines=%s\nprivate_ips=%s\nimage=%s\nvm=%s/%sx%sMB\n' \
  "$APP" "$REGION" "${MIDS[*]}" "${IPS[*]}" "$IMAGE" "$CPU_KIND" "$CPUS" "$MEM" > "$OUT_DIR/cluster.txt"

log "nemesis preflight (ip6tables) on node3"
flyctl ssh console --app "$APP" --machine "${MIDS[2]}" --command \
  "sh -lc 'ip6tables -L INPUT >/dev/null && echo NEM_OK'" 2>/dev/null | grep -q NEM_OK \
  || die "ip6tables unavailable on node3; the partition would be a no-op"

log "fly logs -> $OUT_DIR/cluster.log (whole run)"
flyctl logs --app "$APP" > "$OUT_DIR/cluster.log" 2>&1 &
PIDS+=("$!")

for i in 0 1 2; do
  flyctl proxy "${PORTS[$i]}:9042" "${MIDS[$i]}.vm.${APP}.internal" --app "$APP" > "$OUT_DIR/proxy-node$((i+1)).log" 2>&1 &
  PIDS+=("$!")
done
for p in "${PORTS[@]}"; do
  for i in $(seq 1 30); do nc -z 127.0.0.1 "$p" 2>/dev/null && break; sleep 1; done
  nc -z 127.0.0.1 "$p" 2>/dev/null || die "fly proxy on $p never listened"
done
log "proxies up on ${PORTS[*]}"

STATUS=0
for nem in $NEMESES; do
  log "RUN set under $nem for ${RUN_SECS}s"
  FERROSA_TEST_CLUSTER_NODES="127.0.0.1:${PORTS[0]},127.0.0.1:${PORTS[1]},127.0.0.1:${PORTS[2]}" \
  FERROSA_JEPSEN_FLY_APP="$APP" \
  FERROSA_JEPSEN_FLY_MACHINE_IDS="$(IFS=,; echo "${MIDS[*]}")" \
  FERROSA_JEPSEN_FLY_NODE_IPS="$(IFS=,; echo "${IPS[*]}")" \
  JEPSEN_RUN_DURATION_SECS="$RUN_SECS" \
  RUST_LOG="${RUST_LOG:-info}" \
    "$JEPSEN_BIN" run --tier smoke --topology t1 --pattern set --nemesis "$nem" \
      --output-dir "$OUT_DIR/$nem" > "$OUT_DIR/$nem.run.log" 2>&1
  rc=$?
  log "$nem exited $rc"
  [ "$rc" -eq 0 ] || STATUS=$rc
done
log "done (status $STATUS); teardown follows"
exit "$STATUS"
