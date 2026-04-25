#!/usr/bin/env bash
# test-cluster-down.sh — Tear down the Ferrosa test cluster.
#
# Detects whether podman or docker is in use and tears down the appropriate project.
#
# Usage:
#   scripts/test-cluster-down.sh             # tears down local Podman cluster
#   scripts/test-cluster-down.sh --ci        # tears down CI Docker cluster

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

CI_MODE=0
for arg in "$@"; do
    case "$arg" in
        --ci) CI_MODE=1 ;;
        --help|-h)
            echo "Usage: $0 [--ci]"
            echo ""
            echo "  --ci   Use Docker (CI) project name and ports."
            echo "         Default: Podman local cluster."
            exit 0
            ;;
        *) echo "Unknown argument: $arg"; exit 1 ;;
    esac
done

COMPOSE_BASE="${REPO_ROOT}/tests/docker-compose.cluster.yml"
COMPOSE_OVERRIDE="${REPO_ROOT}/tests/docker-compose.cluster.podman.yml"

if [[ $CI_MODE -eq 1 ]]; then
    # CI Docker teardown
    if ! docker compose version >/dev/null 2>&1; then
        echo "ERROR: 'docker compose' is not available." >&2
        exit 1
    fi
    PROJECT_NAME="ferrosa-test-ci"
    echo "Tearing down CI test cluster (project: ${PROJECT_NAME})..."
    docker compose \
        -f "${COMPOSE_BASE}" \
        --project-name "${PROJECT_NAME}" \
        down -v --remove-orphans
else
    # Local Podman teardown
    if podman compose version >/dev/null 2>&1; then
        COMPOSE_CMD="podman compose"
    elif command -v podman-compose >/dev/null 2>&1; then
        COMPOSE_CMD="podman-compose"
    else
        echo "ERROR: Neither 'podman compose' nor 'podman-compose' is available." >&2
        exit 1
    fi
    PROJECT_NAME="ferrosa-test-w1"
    echo "Tearing down local test cluster (project: ${PROJECT_NAME})..."
    ${COMPOSE_CMD} \
        -f "${COMPOSE_BASE}" \
        -f "${COMPOSE_OVERRIDE}" \
        --project-name "${PROJECT_NAME}" \
        down -v --remove-orphans
fi

echo "Done."
