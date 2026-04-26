#!/usr/bin/env bash
# test-cluster-up-ci.sh — Bring up the 3-node Ferrosa test cluster for CI via Docker.
#
# Uses the default port range (9042-9044) since CI runners are isolated.
# Designed to be run in GitHub Actions or other CI environments.
#
# Usage:
#   scripts/test-cluster-up-ci.sh
#   FERROSA_TEST_CLUSTER_NODES=$(scripts/test-cluster-up-ci.sh | grep '^FERROSA_TEST_CLUSTER_NODES=' | cut -d= -f2-)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
PROJECT_NAME="ferrosa-test-ci"
PROFILE="trio"

# ── Verify Docker Compose is available ───────────────────────────────────────
if ! docker compose version >/dev/null 2>&1; then
    echo "ERROR: 'docker compose' is not available." >&2
    echo "Install Docker Engine with the Compose plugin: https://docs.docker.com/compose/install/" >&2
    exit 1
fi

# ── Compose file ──────────────────────────────────────────────────────────────
COMPOSE_BASE="${REPO_ROOT}/tests/docker-compose.cluster.yml"

# ── Pre-build the node image ONCE (avoids BuildKit per-target context race) ──
# Building inside `compose up --build` parallelizes per-service builds. When
# all 5 nodeN services share the same `build: { context: .., dockerfile:
# Dockerfile }`, BuildKit's per-target context-snapshot reads race and one
# target (typically node2) reports `failed to compute cache key:
# "/Cargo.lock": not found`. Pre-building the image with a single
# `docker build` invocation is the canonical way around this — see p1-32 spec.
echo "Building ferrosa-test-node:latest (single docker build, avoids BuildKit race)..." >&2
docker build \
    -t ferrosa-test-node:latest \
    -f "${REPO_ROOT}/Dockerfile" \
    "${REPO_ROOT}"

# ── Bring up cluster (no --build; uses the pre-built image via image: tag) ──
echo "Starting Ferrosa CI test cluster (profile: ${PROFILE}, project: ${PROJECT_NAME})..." >&2
echo "Ports: CQL 9042/9043/9044, RustFS 9000/9001" >&2

docker compose \
    -f "${COMPOSE_BASE}" \
    --project-name "${PROJECT_NAME}" \
    --profile "${PROFILE}" \
    up -d

# ── Wait for health ───────────────────────────────────────────────────────────
echo "Waiting for all 3 nodes to become healthy (timeout: 180s)..." >&2

TIMEOUT=180
ELAPSED=0
INTERVAL=5
NODES=("${PROJECT_NAME}-node1-1" "${PROJECT_NAME}-node2-1" "${PROJECT_NAME}-node3-1")

while true; do
    ALL_HEALTHY=1
    for node in "${NODES[@]}"; do
        status=$(docker inspect --format='{{.State.Health.Status}}' "${node}" 2>/dev/null || echo "missing")
        if [[ "${status}" != "healthy" ]]; then
            ALL_HEALTHY=0
        fi
    done

    if [[ $ALL_HEALTHY -eq 1 ]]; then
        echo "All 3 nodes are healthy." >&2
        break
    fi

    if [[ $ELAPSED -ge $TIMEOUT ]]; then
        echo "ERROR: Nodes did not become healthy within ${TIMEOUT}s." >&2
        for node in "${NODES[@]}"; do
            echo "  ${node}: $(docker inspect --format='{{.State.Health.Status}}' "${node}" 2>/dev/null || echo 'missing')" >&2
        done
        exit 1
    fi

    sleep $INTERVAL
    ELAPSED=$((ELAPSED + INTERVAL))
done

# ── Emit env vars (stdout only — caller can capture) ─────────────────────────
CLUSTER_NODES="127.0.0.1:9042,127.0.0.1:9043,127.0.0.1:9044"
echo "FERROSA_TEST_CLUSTER_NODES=${CLUSTER_NODES}"
