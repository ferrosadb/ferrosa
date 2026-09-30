#!/usr/bin/env bash
# rf-evidence-ci.sh — CI entry point for the replication-factor evidence harness.
#
# Starts N real ferrosa node processes, proves they form ONE cluster, then
# asserts invariant D1 (achieved replica count == min(configured RF, node count))
# with physical per-node storage evidence, and exercises process death.
#
# Exits non-zero (propagating the harness's code) if the cluster cannot be
# established or the invariant is violated. It NEVER reports a pass against a
# one-node cluster: rf_evidence.py exits 2 in that case.
#
# Requirements: a release `ferrosa` binary and python3 with cassandra-driver.
# Usage:
#   scripts/rf-evidence-ci.sh [--nodes 4] [--base-port 49200] [--binary path]
#
# NOTE: set CARGO_BUILD_JOBS low and stagger node starts when CI runs this
# alongside other heavy jobs — the harness retries formation, but the machine
# has a real ceiling.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

NODES="${RF_NODES:-4}"
BASE_PORT="${RF_BASE_PORT:-49200}"
BINARY="${RF_BINARY:-${REPO_ROOT}/target/release/ferrosa}"
PY="${RF_PYTHON:-python3}"
OUT="${RF_OUT:-${REPO_ROOT}/target/rf-evidence/rf-evidence.json}"

# Allow extra args through (e.g. --rows, --rf, --formation-attempts).
EXTRA=("$@")

if [[ ! -x "${BINARY}" ]]; then
    echo "ERROR: ferrosa binary not found/executable at ${BINARY}" >&2
    echo "       build it first: cargo build --release -p ferrosa" >&2
    exit 5
fi

if ! "${PY}" -c "import cassandra.cluster" >/dev/null 2>&1; then
    echo "ERROR: python cassandra-driver not importable by '${PY}'." >&2
    echo "       pip install cassandra-driver, or set RF_PYTHON to an interpreter that has it." >&2
    exit 5
fi

mkdir -p "$(dirname "${OUT}")"

exec "${PY}" "${SCRIPT_DIR}/rf_evidence.py" \
    --nodes "${NODES}" \
    --base-port "${BASE_PORT}" \
    --binary "${BINARY}" \
    --out "${OUT}" \
    "${EXTRA[@]}"
