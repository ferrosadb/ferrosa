#!/usr/bin/env bash
# test-with-cluster.sh — Bring up the test cluster, run ignored cluster tests, tear down.
#
# Locally uses Podman (ports 30042-30044).
# Pass --ci to use Docker (CI port range 9042-9044).
#
# Usage:
#   scripts/test-with-cluster.sh                        # local Podman
#   scripts/test-with-cluster.sh --ci                   # Docker (CI)
#   scripts/test-with-cluster.sh -- --test-threads=1    # pass extra cargo flags

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

CI_MODE=0
EXTRA_CARGO_ARGS=()

# ── Parse arguments ───────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --ci) CI_MODE=1; shift ;;
        --help|-h)
            echo "Usage: $0 [--ci] [-- <cargo-test-args>]"
            echo ""
            echo "  --ci   Use Docker (CI mode). Default: Podman (local)."
            echo "  --     Pass remaining arguments to 'cargo test'."
            exit 0
            ;;
        --) shift; EXTRA_CARGO_ARGS=("$@"); break ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

# ── Pick the up-script ────────────────────────────────────────────────────────
if [[ $CI_MODE -eq 1 ]]; then
    UP_SCRIPT="${SCRIPT_DIR}/test-cluster-up-ci.sh"
    DOWN_ARGS="--ci"
    CLUSTER_NODES="127.0.0.1:9042,127.0.0.1:9043,127.0.0.1:9044"
else
    UP_SCRIPT="${SCRIPT_DIR}/test-cluster-up.sh"
    DOWN_ARGS=""
    CLUSTER_NODES="127.0.0.1:30042,127.0.0.1:30043,127.0.0.1:30044"
fi

DOWN_SCRIPT="${SCRIPT_DIR}/test-cluster-down.sh"

# ── Ensure teardown on exit ───────────────────────────────────────────────────
teardown() {
    echo ""
    echo "Tearing down test cluster..."
    # shellcheck disable=SC2086
    "${DOWN_SCRIPT}" ${DOWN_ARGS} || true
}
trap teardown EXIT INT TERM

# ── Bring up cluster ──────────────────────────────────────────────────────────
echo "=== Bringing up test cluster ==="
if [[ $CI_MODE -eq 1 ]]; then
    "${UP_SCRIPT}"
else
    # Run with --keep so it returns instead of waiting indefinitely
    "${UP_SCRIPT}" --keep
fi

# ── Export cluster env var and run tests ──────────────────────────────────────
echo ""
echo "=== Running cluster tests ==="
export FERROSA_TEST_CLUSTER_NODES="${CLUSTER_NODES}"
echo "FERROSA_TEST_CLUSTER_NODES=${CLUSTER_NODES}"

cd "${REPO_ROOT}"
cargo test --workspace -- --ignored "${EXTRA_CARGO_ARGS[@]+"${EXTRA_CARGO_ARGS[@]}"}"

echo ""
echo "=== All cluster tests passed ==="
