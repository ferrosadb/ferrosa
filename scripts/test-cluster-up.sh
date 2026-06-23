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
# Prefer the NATIVE python `podman-compose` over `podman compose`. The latter
# shells out to the external `/usr/local/bin/docker-compose` provider, which on
# macOS+podman has a create-time image-lookup bug: it namespaces a locally-built
# image as `docker.io/library/ferrosa-test-node` and then fails with "no such
# image ... image not known" even though `podman images` lists it (t_88f278).
# The python provider talks to podman directly and does not hit that bug.
if command -v podman-compose >/dev/null 2>&1; then
    COMPOSE_CMD="podman-compose"
elif podman compose version >/dev/null 2>&1; then
    COMPOSE_CMD="podman compose"
    echo "WARNING: using 'podman compose' (external docker-compose provider)." >&2
    echo "         If bring-up fails with 'image not known', install the native" >&2
    echo "         provider instead: pip install podman-compose" >&2
else
    echo "ERROR: Neither 'podman-compose' nor 'podman compose' is available." >&2
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

# ── Build the node image ONCE ─────────────────────────────────────────────────
# All N nodeN services share `image: ferrosa-test-node:latest` with identical
# `build:` blocks. `compose up --build` would launch N CONCURRENT cargo-release
# compiles of the SAME image — filling the podman VM disk ("no space left on
# device"). Build it once here, then bring the cluster up with `--no-build`.
# Mirrors scripts/test-cluster-up-ci.sh (p1-32). Set REBUILD=1 to force a rebuild
# from the current tree (needed when validating local code changes).
if [[ "${REBUILD:-0}" != "1" ]] && podman image inspect ferrosa-test-node:latest >/dev/null 2>&1; then
    echo "Using existing ferrosa-test-node:latest image (set REBUILD=1 to rebuild)."
else
    echo "Building ferrosa-test-node:latest (single podman build, avoids per-service race)..."
    podman build -t ferrosa-test-node:latest -f "${REPO_ROOT}/Dockerfile" "${REPO_ROOT}"
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
    up -d --no-build || true

# ── Resolve node container names ──────────────────────────────────────────────
# The compose providers disagree on the separator: the python `podman-compose`
# names containers `<project>_node<i>_1` (underscores) while `podman compose`
# (docker-compose) uses `<project>-node<i>-1` (hyphens). Resolve the real name
# per node so the retry + health loops work under either provider.
resolve_node_name() {
    local i="$1"
    local underscore="${PROJECT_NAME}_node${i}_1"
    local hyphen="${PROJECT_NAME}-node${i}-1"
    if podman container exists "${underscore}" 2>/dev/null; then
        echo "${underscore}"
    else
        echo "${hyphen}"
    fi
}

# ── Retry flaky node starts ───────────────────────────────────────────────────
# podman-compose starts the N node containers in parallel; some intermittently
# fail with "starting some containers: internal libpod error" and are left in
# `created` (never ran). The failure is transient — an idempotent `podman start`
# with backoff brings them up. Without this, a >3-node bring-up routinely lands
# 1-2 nodes short (t_88f278).
for i in $(seq 1 "${NODE_COUNT}"); do
    c="$(resolve_node_name "${i}")"
    for attempt in 1 2 3 4 5; do
        st=$(podman inspect --format '{{.State.Status}}' "${c}" 2>/dev/null || echo missing)
        [[ "${st}" == "running" ]] && break
        podman start "${c}" >/dev/null 2>&1 && break
        echo "  ${c}: start attempt ${attempt} failed (transient libpod race), retrying..."
        sleep 3
    done
done

# ── Wait for health ───────────────────────────────────────────────────────────
echo ""
echo "Waiting for all ${NODE_COUNT} nodes to become healthy (timeout: 120s)..."

TIMEOUT=120
ELAPSED=0
INTERVAL=5
NODES=()
for i in $(seq 1 "${NODE_COUNT}"); do
    NODES+=("$(resolve_node_name "${i}")")
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
