#!/usr/bin/env bash
# Provision N ferrosa nodes as separate fly machines (distinct private IPs),
# 2 GiB each, forming an RF=3 cluster over the fly private network.
#
# SCAFFOLD: dry-run by default. Pass --i-will-pay to actually create machines.
# NEVER raise VM_MEMORY_MB above 2048 — the 2 GiB cap is the assertion.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${HERE}/lib.sh" "$@"

require_flyctl

[ "${VM_MEMORY_MB}" -le 2048 ] || die "VM_MEMORY_MB=${VM_MEMORY_MB} > 2048; the 2 GiB cap is intentional and must never be raised."
[ "${NODE_COUNT}" -ge 3 ] || die "NODE_COUNT=${NODE_COUNT} < 3; need >=3 distinct machines for distinct private IPs / RF=3."

log "Provisioning app=${FLY_APP} region=${FLY_REGION} nodes=${NODE_COUNT} mem=${VM_MEMORY_MB}MiB RF=${REPLICATION_FACTOR}"

# 1. App (idempotent).
fly_do flyctl apps create "${FLY_APP}" --org "${FLY_ORG}" || true

# 2. Build image from the pinned git ref (same overlay approach as fly-bench:
#    a clean `git archive` + the entrypoint that maps FLY_PRIVATE_IP → broadcast).
IMAGE_TAG="registry.fly.io/${FLY_APP}:$(git rev-parse --short "${BENCH_GIT_REF}" 2>/dev/null || echo scaffold)"
log "Would build+push ferrosa image ${IMAGE_TAG} from ref ${BENCH_GIT_REF} (Dockerfile: deploy/fly-bench/ferrosa-main.Dockerfile)"
fly_do flyctl deploy --app "${FLY_APP}" --image-label "$(git rev-parse --short "${BENCH_GIT_REF}" 2>/dev/null || echo scaffold)" --build-only --dockerfile ../fly-bench/ferrosa-main.Dockerfile

# 3. One machine per node. First machine is the seed; the rest join via its
#    private IP. Each gets its own machine => its own FLY_PRIVATE_IP.
for i in $(seq 0 $((NODE_COUNT - 1))); do
  name="$(node_name "${i}")"
  seed_env=""
  if [ "${i}" -ne 0 ]; then
    # Non-seed nodes join the cluster via the seed's fly-internal DNS name.
    seed_env="--env FERROSA_SEED_NODES=$(node_name 0).vm.${FLY_APP}.internal:17000"
  fi
  log "node ${i}: ${name}"
  fly_do flyctl machine run "${IMAGE_TAG}" \
    --app "${FLY_APP}" \
    --region "${FLY_REGION}" \
    --name "${name}" \
    --vm-size "${VM_CPUS}" \
    --vm-memory "${VM_MEMORY_MB}" \
    --env "FERROSA_REPLICATION_FACTOR=${REPLICATION_FACTOR}" \
    --env "FERROSA_BULK_STREAMING_RANGE_READ=1" \
    ${seed_env}
done

log "Provision step complete (dry-run unless --i-will-pay was passed)."
