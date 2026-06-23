#!/usr/bin/env bash
# test-cluster-up.sh — Bring up the 3-node Ferrosa test cluster locally via Podman.
#
# Ports: 30042-30044 (CQL), 30000/30001 (RustFS S3).
# Project name: ferrosa-test-w1 (isolated from the live fmem cluster).
#
# Usage:
#   source <(scripts/test-cluster-up.sh)            # sets env vars in current shell
#   scripts/test-cluster-up.sh --keep               # leave cluster running after script exits
#   scripts/test-cluster-up.sh --help
#
# After sourcing, use:
#   cargo test -- --ignored

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
PROJECT_NAME="ferrosa-test-w1"
PROFILE="trio"
KEEP=0
COMPOSE_CMD=""

# ── Parse arguments ───────────────────────────────────────────────────────────
for arg in "$@"; do
    case "$arg" in
        --keep) KEEP=1 ;;
        --quint) PROFILE="quint" ;;
        --help|-h)
            echo "Usage: $0 [--keep] [--quint]"
            echo ""
            echo "  --keep    Do not tear down the cluster on exit."
            echo "  --quint   5-node cluster (ports 30042-30046) instead of the"
            echo "            default 3-node trio. Needed to exercise RF<node-count"
            echo "            replica placement (e.g. multi-shard Accord transactions)."
            echo ""
            echo "Bring up a Ferrosa test cluster via Podman."
            echo "Source this script to inherit FERROSA_TEST_CLUSTER_NODES."
            exit 0
            ;;
        *) echo "Unknown argument: $arg"; exit 1 ;;
    esac
done

# Node count + host CQL ports are derived from the profile.
if [[ "${PROFILE}" == "quint" ]]; then
    NODE_COUNT=5
else
    NODE_COUNT=3
fi

# ── Detect compose binary ─────────────────────────────────────────────────────
if podman compose version >/dev/null 2>&1; then
    COMPOSE_CMD="podman compose"
elif command -v podman-compose >/dev/null 2>&1; then
    COMPOSE_CMD="podman-compose"
else
    echo "ERROR: Neither 'podman compose' nor 'podman-compose' is available." >&2
    echo "Install podman-compose: pip install podman-compose" >&2
    exit 1
fi

# ── Compose files ─────────────────────────────────────────────────────────────
COMPOSE_BASE="${REPO_ROOT}/tests/docker-compose.cluster.yml"
COMPOSE_OVERRIDE="${REPO_ROOT}/tests/docker-compose.cluster.podman.yml"

# ── Teardown function ─────────────────────────────────────────────────────────
teardown() {
    echo ""
    echo "Tearing down ferrosa test cluster (project: ${PROJECT_NAME})..."
    ${COMPOSE_CMD} \
        -f "${COMPOSE_BASE}" \
        -f "${COMPOSE_OVERRIDE}" \
        --project-name "${PROJECT_NAME}" \
        down -v --remove-orphans 2>/dev/null || true
    echo "Cluster torn down."
}

if [[ $KEEP -eq 0 ]]; then
    trap teardown EXIT INT TERM
fi

# ── Bring up cluster ──────────────────────────────────────────────────────────
echo "Starting Ferrosa test cluster (profile: ${PROFILE}, project: ${PROJECT_NAME})..."
echo "Profile: ${PROFILE} (${NODE_COUNT} nodes), CQL 30042.., RustFS 30000/30001"
echo ""

${COMPOSE_CMD} \
    -f "${COMPOSE_BASE}" \
    -f "${COMPOSE_OVERRIDE}" \
    --project-name "${PROJECT_NAME}" \
    --profile "${PROFILE}" \
    up -d --build

# ── Wait for health ───────────────────────────────────────────────────────────
echo ""
echo "Waiting for all ${NODE_COUNT} nodes to become healthy (timeout: 120s)..."

TIMEOUT=120
ELAPSED=0
INTERVAL=5
NODES=()
for i in $(seq 1 "${NODE_COUNT}"); do
    NODES+=("${PROJECT_NAME}-node${i}-1")
done

while true; do
    ALL_HEALTHY=1
    for node in "${NODES[@]}"; do
        status=$(podman inspect --format='{{.State.Health.Status}}' "${node}" 2>/dev/null || echo "missing")
        if [[ "${status}" != "healthy" ]]; then
            ALL_HEALTHY=0
            echo "  ${node}: ${status}"
        fi
    done

    if [[ $ALL_HEALTHY -eq 1 ]]; then
        echo ""
        echo "All ${NODE_COUNT} nodes are healthy."
        break
    fi

    if [[ $ELAPSED -ge $TIMEOUT ]]; then
        echo ""
        echo "ERROR: Nodes did not become healthy within ${TIMEOUT}s." >&2
        for node in "${NODES[@]}"; do
            echo "  ${node}: $(podman inspect --format='{{.State.Health.Status}}' "${node}" 2>/dev/null || echo 'missing')" >&2
        done
        exit 1
    fi

    sleep $INTERVAL
    ELAPSED=$((ELAPSED + INTERVAL))
done

# ── Emit env vars ─────────────────────────────────────────────────────────────
CLUSTER_NODES=""
for i in $(seq 1 "${NODE_COUNT}"); do
    port=$((30041 + i))
    CLUSTER_NODES="${CLUSTER_NODES:+${CLUSTER_NODES},}127.0.0.1:${port}"
done
echo ""
echo "# Source these environment variables to run ignored cluster tests:"
echo "export FERROSA_TEST_CLUSTER_NODES=\"${CLUSTER_NODES}\""
echo ""
echo "FERROSA_TEST_CLUSTER_NODES=${CLUSTER_NODES}"

if [[ $KEEP -eq 0 ]]; then
    echo ""
    echo "Cluster is running. Press Ctrl-C to tear down."
    # Wait indefinitely so the trap fires on Ctrl-C
    while true; do sleep 10; done
fi
