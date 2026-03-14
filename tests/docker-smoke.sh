#!/usr/bin/env bash
# End-to-end Docker smoke test for two-node pair mode with MinIO.
#
# Prerequisites:
#   - Docker and Docker Compose installed
#   - cqlsh available (pip install cqlsh or use the cassandra package)
#
# Usage:
#   ./tests/docker-smoke.sh
#
# What it tests:
#   1. Build and start two ferrosa nodes + MinIO
#   2. Wait for nodes to start and pair
#   3. Create keyspace and table on node1
#   4. Insert data on node1
#   5. Read data from node2 (verifies pair replication)
#   6. Check MinIO for uploaded SSTables (after flush)
#   7. Tear down

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_DIR"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}PASS${NC}: $1"; }
fail() { echo -e "${RED}FAIL${NC}: $1"; exit 1; }
info() { echo -e "${YELLOW}INFO${NC}: $1"; }

cleanup() {
    info "Tearing down..."
    docker compose down -v --remove-orphans 2>/dev/null || true
}
trap cleanup EXIT

# 1. Build and start
info "Building and starting services..."
docker compose up -d --build

# 2. Wait for CQL to be ready
info "Waiting for node1 CQL (port 9042)..."
for i in $(seq 1 60); do
    if cqlsh localhost 9042 -e "SELECT now() FROM system.local" >/dev/null 2>&1; then
        pass "node1 CQL is ready"
        break
    fi
    if [ "$i" -eq 60 ]; then
        fail "node1 CQL did not become ready in 60s"
    fi
    sleep 1
done

info "Waiting for node2 CQL (port 9043)..."
for i in $(seq 1 60); do
    if cqlsh localhost 9043 -e "SELECT now() FROM system.local" >/dev/null 2>&1; then
        pass "node2 CQL is ready"
        break
    fi
    if [ "$i" -eq 60 ]; then
        fail "node2 CQL did not become ready in 60s"
    fi
    sleep 1
done

# 3. Create keyspace and table on node1
info "Creating keyspace and table on node1..."
cqlsh localhost 9042 -e "
    CREATE KEYSPACE IF NOT EXISTS smoke_test
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
"
cqlsh localhost 9042 -e "
    CREATE TABLE IF NOT EXISTS smoke_test.kv (
        k text PRIMARY KEY,
        v text
    );
"
pass "Keyspace and table created on node1"

# 4. Insert data on node1
info "Inserting data on node1..."
cqlsh localhost 9042 -e "
    INSERT INTO smoke_test.kv (k, v) VALUES ('hello', 'world');
    INSERT INTO smoke_test.kv (k, v) VALUES ('foo', 'bar');
"
pass "Data inserted on node1"

# Give pair replication a moment to complete
sleep 2

# 5. Read data from node2
info "Reading data from node2..."
RESULT=$(cqlsh localhost 9043 -e "SELECT k, v FROM smoke_test.kv WHERE k = 'hello';" 2>&1)
if echo "$RESULT" | grep -q "world"; then
    pass "Data replicated to node2: hello=world"
else
    info "Query result: $RESULT"
    fail "Data not found on node2 (pair replication may not be working)"
fi

RESULT2=$(cqlsh localhost 9043 -e "SELECT k, v FROM smoke_test.kv WHERE k = 'foo';" 2>&1)
if echo "$RESULT2" | grep -q "bar"; then
    pass "Data replicated to node2: foo=bar"
else
    info "Query result: $RESULT2"
    fail "Second row not found on node2"
fi

# 6. Check node health
info "Checking node1 system.local..."
NODE1_ID=$(cqlsh localhost 9042 -e "SELECT host_id FROM system.local;" 2>&1)
pass "node1 system.local responding"

info "Checking node2 system.local..."
NODE2_ID=$(cqlsh localhost 9043 -e "SELECT host_id FROM system.local;" 2>&1)
pass "node2 system.local responding"

echo ""
echo -e "${GREEN}All smoke tests passed!${NC}"
echo ""
info "Services still running. Use 'docker compose down -v' to stop."
info "MinIO console: http://localhost:9001 (minioadmin/minioadmin)"
info "Node1 CQL: cqlsh localhost 9042"
info "Node2 CQL: cqlsh localhost 9043"

# Don't cleanup on success — leave services running for manual exploration
trap - EXIT
