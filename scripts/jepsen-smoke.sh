#!/usr/bin/env bash
# Jepsen Smoke Test — Quick validation before full correctness run
#
# Prerequisites:
#   - Docker or Podman installed (auto-detected)
#   - ferrosa binary built: cargo build --release
#
# Usage:
#   ./scripts/jepsen-smoke.sh          # Run quick smoke test
#   ./scripts/jepsen-smoke.sh --full   # Run all phases

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Auto-detect container runtime
if command -v podman &>/dev/null; then
    RUNTIME=podman
    COMPOSE="podman compose"
elif command -v docker &>/dev/null; then
    RUNTIME=docker
    COMPOSE="docker compose"
else
    echo "ERROR: Neither docker nor podman found. Install one first."
    exit 1
fi

echo "=== Jepsen Smoke Test ==="
echo "Container runtime: $RUNTIME"
echo "Root: $ROOT_DIR"
echo ""

# Step 1: Build if needed
if [ ! -f "$ROOT_DIR/target/release/ferrosa" ]; then
    echo ">>> Building ferrosa (release)..."
    cargo build --release --manifest-path "$ROOT_DIR/Cargo.toml"
fi

# Step 2: Start 3-node cluster
echo ">>> Starting 3-node cluster..."
cd "$ROOT_DIR/tests"
$COMPOSE -f docker-compose.cluster.yml up -d 2>/dev/null || {
    echo "ERROR: Failed to start cluster. Check $COMPOSE logs."
    exit 1
}

# Wait for nodes to be ready
echo ">>> Waiting for CQL port..."
for i in 1 2 3 4 5 6 7 8 9 10; do
    if $RUNTIME exec ferrosa-node1 sh -c "echo > /dev/tcp/localhost/9042" 2>/dev/null; then
        echo "    Node ready after ${i}s"
        break
    fi
    sleep 1
done

# Step 3: Run unit tests (no infra needed)
echo ""
echo ">>> Running Jepsen unit tests..."
cargo test -p ferrosa-jepsen --lib 2>&1 | tail -3

# Step 4: Run smoke tier (needs containers)
echo ""
echo ">>> Running Jepsen smoke tier..."
FERROSA_TEST_CONTAINERS=1 cargo test -p ferrosa-jepsen --test smoke_tier -- --nocapture 2>&1 | tail -10

# Step 5: Cleanup
echo ""
echo ">>> Cleaning up cluster..."
cd "$ROOT_DIR/tests"
$COMPOSE -f docker-compose.cluster.yml down 2>/dev/null || true

echo ""
echo "=== Jepsen Smoke Complete ==="
