#!/usr/bin/env bash
# Multi-DC INVARIANT certification on fly.io.
#
# Six nodes across TWO logical DCs (dc1 = nodes 1-3, dc2 = nodes 4-6), one app,
# one region: the DC link is the internode path between the two groups, and the
# WAN nemesis reproduces its faults. Each invariant is asserted AND its evidence
# printed, because "one copy per remote DC" is only checkable against a record.
#
#   A CONFIG    every node's OWN view reports the DC it was started with — i.e.
#               FERROSA_DATA_CENTER is wired through to system.local, not ignored
#   B REGISTRY  a dc1 node records the DC its cross-DC peer ANNOUNCED (the
#               announcement is the only source the DC registry has)
#   C ONE COPY  a keyspace created on a dc1 node appears on EVERY dc2 node: one
#               copy left dc1 and dc2's own Raft group took it to its quorum
#   D NOT PER NODE  the WAN carried exactly ONE copy for dc2 — counted from the
#               sender's own log, before vs after the statement
#   E OWED, NOT DROPPED  with dc1<->dc2 partitioned, a DDL on dc1 still SUCCEEDS
#               (a LOCAL/QUORUM client is never held behind a WAN) and the copy is
#               recorded as OWED rather than silently lost
#
# What this does NOT claim: that an owed copy is eventually delivered by itself.
# Nothing drains that debt yet — the repair path is the next wiring step — so the
# script asserts the debt is RECORDED and says so, rather than asserting a repair
# that does not exist.
#
# It does not run a workload either: DML replica fan-out is still per remote NODE
# (slice 4), so an Elle-style write across DCs is not a fair test yet. DDL is the
# path the relay covers today.
#
# Teardown ALWAYS runs (trap), so machines/app/bucket never leak. KEEP=1 leaves
# everything up for inspection.
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ORG="${ORG:-ferrosa}"
REGION="${REGION:-lax}"
APP="${APP:-ferrosa-multi-dc-inv}"
BUCKET="${BUCKET:-ferrosa-multi-dc-${RANDOM}${RANDOM}}"
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
OUT_LOG="${OUT_LOG:-${ROOT_DIR}/deploy/fly-accord-elle/multi-dc-invariants.log}"

log() { printf '\n[multi-dc] %s\n' "$*" >&2; }
die() { printf '\n[multi-dc][FATAL] %s\n' "$*" >&2; exit 1; }
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
  flyctl storage destroy "$BUCKET" --yes 2>&1 | tail -1 || log "WARN: bucket $BUCKET leak"
}
trap teardown EXIT

command -v flyctl >/dev/null || die "flyctl not on PATH"
command -v jq >/dev/null || die "jq not on PATH"

# 1. App + Tigris bucket + S3 secrets (the nodes persist to object storage).
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

# 2. Build + push the image (adds iptables/iproute2 for the nemesis).
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
  --image-label "cert-${RUN_ID}" 2>&1 | tee "$BUILD_LOG" | grep -iE "compiling ferrosa|finished|error|image:|image size" || true
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
  --env "FERROSA_CLUSTER_NAME=ferrosa-multi-dc-inv"
  --env "FERROSA_GRAPH_ENABLED=false"
  --env "FERROSA_AUTH_ENABLED=false"
  --env "FERROSA_FORMATION_TIMEOUT_SECS=180"
)
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
  local run_args=(
    "$IMAGE" --app "$APP" --org "$ORG" --region "$REGION"
    --name "ferrosa-${n}" --restart always --rootfs-size 20
    --vm-cpu-kind "$CPU_KIND" --vm-cpus "$CPUS" --vm-memory "$MEM"
    "${COMMON[@]}"
    --env "FERROSA_DATA_CENTER=${dc}"
    --env "FERROSA_HOST_ID=${HOST_IDS[$((n-1))]}"
    --env "FERROSA_S3_PREFIX=multi-dc/${RUN_ID}/node${n}"
  )
  [ -n "$seed" ] && run_args+=(--env "FERROSA_SEED=${seed}")
  mrun "${run_args[@]}" || die "node$n run failed"
  wait_ready "$(mid ferrosa-${n})" "ferrosa-${n}"
}

# 3. dc1 seed, dc1 joiners, then dc2 (node4 joins the dc1 seed cross-DC, node5/6
#    seed within dc2) — the same shape certificate as certify-dc.sh.
launch_node 1
SEED1="$(dns "$(mid ferrosa-1)"):17000"
launch_node 2 "$SEED1"
launch_node 3 "$SEED1"
launch_node 4 "$SEED1"
SEED2="$(dns "$(mid ferrosa-4)"):17000"
launch_node 5 "$SEED2"
launch_node 6 "$SEED2"

declare -a MIDS IPS
for n in 1 2 3 4 5 6; do
  MIDS[$n]="$(mid ferrosa-${n})"; IPS[$n]="$(mip ferrosa-${n})"
  [ -n "${MIDS[$n]}" ] && [ -n "${IPS[$n]}" ] || die "missing id/ip for ferrosa-${n}"
done
log "cluster up (dc1: ${IPS[1]},${IPS[2]},${IPS[3]} | dc2: ${IPS[4]},${IPS[5]},${IPS[6]}). settling 30s."
sleep 30

# ── assertion primitives ──────────────────────────────────────────────────────
FAILED=0
pass() { log "  PASS  $*"; }
fail() { log "  FAIL  $*"; FAILED=$((FAILED + 1)); }

# One CQL statement on one node, through the in-image `cql_exec`. The statement is
# passed to the VM shell in double quotes, so a CQL string literal's single quotes
# survive unchanged.
cql() { # <machine-name> <statement>
  flyctl ssh console --app "$APP" --machine "$(mid "$1")" --command "cql_exec localhost:9042 \"$2\"" 2>&1
}

# Everything this machine has logged (the fly log buffer, not a live tail).
node_log() { flyctl logs --app "$APP" --machine "$(mid "$1")" --no-tail 2>/dev/null; }

# How many cross-DC deliveries that machine has logged so far.
deliveries() { node_log "$1" | grep -c 'cross-DC copy delivered' || true; }

nem() { flyctl ssh console --app "$APP" --machine "$1" --command "sh -lc '$2'" 2>&1 | tail -1; }
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

# ── A: the configured DC is the DC each node reports ──────────────────────────
log "INV A — each node's own view reports the DC it was started with"
for n in 1 2 3 4 5 6; do
  dc="$(DC_OF "$n")"
  out="$(cql "ferrosa-$n" "SELECT data_center FROM system.local WHERE data_center = '$dc'")"
  if printf '%s' "$out" | grep -q 'ROWS 1'; then
    pass "node$n reports $dc"
  else
    fail "node$n does not report $dc — system.local says otherwise: $(printf '%s' "$out" | tail -2 | tr '\n' ' ')"
  fi
done

# ── B: the DC registry's only source is what peers announce ──────────────────
log "INV B — a dc1 node recorded the DC its cross-DC peer announced"
if node_log ferrosa-1 | grep -q 'data_center=Some("dc2")'; then
  pass "dc1 seed recorded an announced dc2 peer"
else
  fail "dc1 seed never recorded a peer announcing dc2 — the DC registry has no source"
fi

# ── C + D: one copy per remote DC, and it becomes that DC's own quorum ───────
KS="dc_relay_probe_${RANDOM}${RANDOM}"
log "INV C/D — create $KS on the dc1 seed; it must reach every dc2 node in ONE copy"
before="$(deliveries ferrosa-1)"
log "  cross-DC deliveries logged by the dc1 seed before the statement: $before"
created="$(cql ferrosa-1 "CREATE KEYSPACE $KS WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}")"
if printf '%s' "$created" | grep -q '^OK '; then
  pass "the DDL committed on the dc1 seed"
else
  fail "the DDL did not commit on the dc1 seed: $(printf '%s' "$created" | tail -2 | tr '\n' ' ')"
fi
sleep 10
after="$(deliveries ferrosa-1)"
if [ "$((after - before))" = "1" ]; then
  pass "exactly ONE cross-DC copy left dc1 (deliveries $before -> $after)"
else
  fail "expected exactly one cross-DC copy for dc2, saw $((after - before)) (deliveries $before -> $after)"
fi
for n in 1 2 3 4 5 6; do
  out="$(cql "ferrosa-$n" "USE $KS")"
  if printf '%s' "$out" | grep -q '^OK '; then
    pass "node$n ($(DC_OF "$n")) has $KS"
  else
    fail "node$n ($(DC_OF "$n")) does NOT have $KS: $(printf '%s' "$out" | tail -2 | tr '\n' ' ')"
  fi
done

# ── E: a partitioned DC does not hold the client, and the copy is recorded owed ─
KS2="dc_relay_owed_${RANDOM}${RANDOM}"
log "INV E — with dc1<->dc2 partitioned, a DDL on dc1 must still succeed (owed, not dropped): $KS2"
inject_dc_partition
owed_before="$(node_log ferrosa-1 | grep -c 'copies owed' || true)"
created2="$(cql ferrosa-1 "CREATE KEYSPACE $KS2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}")"
if printf '%s' "$created2" | grep -q '^OK '; then
  pass "the DDL succeeded while dc2 was unreachable — the client is not held by the WAN"
else
  fail "the DDL failed while dc2 was unreachable: $(printf '%s' "$created2" | tail -2 | tr '\n' ' ')"
fi
owed_after="$(node_log ferrosa-1 | grep -c 'copies owed' || true)"
if [ "$((owed_after - owed_before))" -ge 1 ]; then
  pass "the copy owed to dc2 was RECORDED ($owed_before -> $owed_after 'copies owed' lines)"
else
  fail "the copy owed to dc2 was not recorded — it would be lost silently"
fi
heal_all_netfilter
log "NOTE: nothing drains that debt yet — the repair path is the next wiring step."
log "      This script asserts the debt is recorded; it does NOT assert a repair."
if [ "${WAIT_FOR_REPAIR:-0}" = "1" ]; then
  log "WAIT_FOR_REPAIR=1 — watching for dc2 to receive $KS2 anyway (expected to FAIL today)"
  sleep 30
  for n in 4 5 6; do
    out="$(cql "ferrosa-$n" "USE $KS2")"
    if printf '%s' "$out" | grep -q '^OK '; then pass "node$n received the repaired copy"; else fail "node$n never received $KS2 (no repair path yet)"; fi
  done
fi

# ── verdict ───────────────────────────────────────────────────────────────────
if [ "$FAILED" = "0" ]; then
  log "=== MULTI-DC INVARIANTS: ALL PASS ==="
else
  log "=== MULTI-DC INVARIANTS: $FAILED FAILED ==="
fi
log "dc1: ${IPS[1]},${IPS[2]},${IPS[3]} | dc2: ${IPS[4]},${IPS[5]},${IPS[6]}"
[ "$FAILED" = "0" ] || exit 1
