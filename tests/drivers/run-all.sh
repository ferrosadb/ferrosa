#!/usr/bin/env bash
# run-all.sh — Build Ferrosa, start infrastructure, run every driver test.
#
# Usage:
#   ./tests/drivers/run-all.sh
#
# Environment:
#   CONTAINER_RUNTIME  Container runtime to use (default: docker; set to podman for Podman)
#
# Exit codes:
#   0 = all drivers passed
#   1 = one or more drivers failed

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.drivers.yml"

# Allow overriding to podman for environments without Docker
RUNTIME="${CONTAINER_RUNTIME:-docker}"

DRIVERS=("python-tests" "go-tests" "node-tests" "java-tests" "rust-tests" "csharp-tests" "python-auth-tests")

pass=0
fail=0
failed_drivers=()

# ---------------------------------------------------------------------------
# Cleanup on exit
# ---------------------------------------------------------------------------
cleanup() {
    echo ""
    echo "=== Tearing down ==="
    "$RUNTIME" compose -f "$COMPOSE_FILE" down -v --remove-orphans 2>/dev/null || true
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# 1. Build Ferrosa Docker image (skip if one is already present)
# ---------------------------------------------------------------------------
# Build/release/run: reuse an existing ferrosa-test-node:latest image (loaded
# from the build-once CI artifact, or built by a prior local run) instead of
# recompiling the release binary inside Docker every time.
if "$RUNTIME" image inspect ferrosa-test-node:latest >/dev/null 2>&1; then
    echo "=== Reusing existing ferrosa-test-node:latest image (skipping build) ==="
else
    echo "=== Building Ferrosa image (runtime: $RUNTIME) ==="
    "$RUNTIME" compose -f "$COMPOSE_FILE" build ferrosa
fi

# ---------------------------------------------------------------------------
# 2. Start Ferrosa + RustFS
# ---------------------------------------------------------------------------
echo ""
echo "=== Starting Ferrosa + RustFS ==="
# ferrosa: auth-disabled functional node (CQL/graph/bolt/sparql smoke).
# ferrosa-auth: auth-enabled node for the auth-enforcement tests.
"$RUNTIME" compose -f "$COMPOSE_FILE" up -d ferrosa ferrosa-auth

# ---------------------------------------------------------------------------
# 3. Wait for Ferrosa CQL port
# ---------------------------------------------------------------------------
echo ""
echo "=== Waiting for Ferrosa CQL port (9042) ==="
MAX_WAIT=120
for i in $(seq 1 "$MAX_WAIT"); do
    # Check from inside the container using bash /dev/tcp (no nc needed).
    # Falls back to checking the compose healthcheck status.
    if "$RUNTIME" compose -f "$COMPOSE_FILE" exec -T ferrosa bash -c 'echo > /dev/tcp/127.0.0.1/9042' 2>/dev/null; then
        echo "Ferrosa ready after ${i}s"
        break
    fi
    if [ "$i" -eq "$MAX_WAIT" ]; then
        echo "FAIL: Ferrosa did not become ready within ${MAX_WAIT}s"
        echo ""
        echo "--- Ferrosa logs ---"
        "$RUNTIME" compose -f "$COMPOSE_FILE" logs ferrosa | tail -50
        exit 1
    fi
    sleep 1
done

# ---------------------------------------------------------------------------
# 4. Run each driver test
# ---------------------------------------------------------------------------
echo ""
echo "=== Running driver tests ==="

for driver in "${DRIVERS[@]}"; do
    echo ""
    echo "--- $driver ---"
    if "$RUNTIME" compose -f "$COMPOSE_FILE" run --rm "$driver"; then
        echo "  => $driver: PASS"
        pass=$((pass + 1))
    else
        echo "  => $driver: FAIL"
        fail=$((fail + 1))
        failed_drivers+=("$driver")
    fi
done

# ---------------------------------------------------------------------------
# 5. Report
# ---------------------------------------------------------------------------
echo ""
echo "=============================="
echo "Driver test results: $pass passed, $fail failed"
if [ ${#failed_drivers[@]} -gt 0 ]; then
    echo "Failed: ${failed_drivers[*]}"
fi
echo "=============================="

if [ "$fail" -gt 0 ]; then
    exit 1
fi
