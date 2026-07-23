#!/usr/bin/env bash
# Provision one arm's cluster: NODE_COUNT ferrosa nodes on shared-cpu (the
# throttle race fuzzer) + one load-driver. Idempotent app create; deterministic
# host IDs + seed wiring (the proven fly-accord-elle / O_DIRECT formation recipe).
#
# Inputs (env):
#   NODE_IMAGE   — image for the ferrosa nodes (arm-specific: pre-fix or post-fix)
#   CLIENT_IMAGE — image for the driver (ALWAYS post-fix; it has --scan-storm)
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${HERE}/lib.sh" "$@"
require_flyctl

: "${NODE_IMAGE:?NODE_IMAGE must be set}"
: "${CLIENT_IMAGE:?CLIENT_IMAGE must be set}"
[ "${NODE_COUNT}" -ge 3 ] || die "NODE_COUNT=${NODE_COUNT} < 3; need >=3 for RF=3 / distinct IPs."

log "provisioning ${NODE_COUNT}x${VM_CPUS}/${VM_MEMORY_MB}MiB nodes + 1 client (${CLIENT_VM_CPUS})"
log "  node image:   ${NODE_IMAGE}"
log "  client image: ${CLIENT_IMAGE}"

fly_do flyctl apps create "${FLY_APP}" --org "${FLY_ORG}" || true

# Common node env. Production raft election/quorum defaults are intentionally
# NOT overridden — the regression measures the shipping regime.
COMMON_ENV=(
  --env "FERROSA_DATA_DIR=/var/lib/ferrosa"
  --env "FERROSA_RAFT_DATA_DIR=/var/lib/ferrosa-raft"
  --env "FERROSA_CQL_BIND=[::]:9042"
  --env "FERROSA_WEB_BIND=[::]:9090"
  --env "FERROSA_INTERNODE_BIND=[::]:17000"
  --env "FERROSA_CLUSTER_NAME=${FLY_APP}"
  --env "FERROSA_AUTH_ENABLED=false"
  --env "FERROSA_GRAPH_ENABLED=false"
  --env "FERROSA_FORMATION_TIMEOUT_SECS=120"
  --env "FERROSA_REPLICATION_FACTOR=${REPLICATION_FACTOR}"
)
HOST_IDS=(
  aa111111-1111-1111-1111-111111111111
  bb222222-2222-2222-2222-222222222222
  cc333333-3333-3333-3333-333333333333
)

SEED_DNS="$(node_name 0).vm.${FLY_APP}.internal:17000"
for i in $(seq 0 $((NODE_COUNT - 1))); do
  name="$(node_name "${i}")"
  seed_args=()
  [ "${i}" -ne 0 ] && seed_args=(--env "FERROSA_SEED=${SEED_DNS}")
  log "ferrosa node ${i}: ${name}${seed_args:+ (join ${SEED_DNS})}"
  fly_retry flyctl machine run "${NODE_IMAGE}" \
    --app "${FLY_APP}" --region "${FLY_REGION}" --name "${name}" \
    --vm-size "${VM_CPUS}" --vm-memory "${VM_MEMORY_MB}" --restart always \
    "${COMMON_ENV[@]}" \
    --env "FERROSA_HOST_ID=${HOST_IDS[${i}]}" \
    ${seed_args[@]+"${seed_args[@]}"}
done

log "load-driver: $(client_name) (image ${CLIENT_IMAGE})"
fly_retry flyctl machine run "${CLIENT_IMAGE}" \
  --app "${FLY_APP}" --region "${FLY_REGION}" --name "$(client_name)" \
  --vm-size "${CLIENT_VM_CPUS}" --vm-memory "${CLIENT_VM_MEMORY_MB}" \
  sleep infinity

# Gate on readiness so the cluster is actually formed before load.
if [ "${I_WILL_PAY}" -eq 1 ]; then
  for i in $(seq 0 $((NODE_COUNT - 1))); do
    name="$(node_name "${i}")"; id="$(machine_id_for_name "${name}")"
    [ -n "${id}" ] || die "no machine id for ${name} during readiness wait"
    log "waiting for ${name} /readyz ..."
    ready=0
    for _ in $(seq 1 50); do
      if flyctl ssh console --app "${FLY_APP}" --machine "${id}" \
           --command "sh -lc 'curl -sf http://localhost:9090/readyz'" 2>/dev/null | grep -q ready; then
        log "  ${name} ready"; ready=1; break
      fi
      sleep 6
    done
    [ "${ready}" -eq 1 ] || die "${name} not ready (formation failed?) — flyctl logs --app ${FLY_APP} --machine ${id}"
  done
  log "all ${NODE_COUNT} nodes ready; settling 15s for ring/schema convergence"
  sleep 15
fi
log "provision complete (dry-run unless --i-will-pay)."
