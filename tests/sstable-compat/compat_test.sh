#!/bin/bash
# Ferrosa -> Cassandra 5.1 SSTable compatibility test.
#
# Writes an SSTable with ferrosa, imports it into a Cassandra 5.1 container
# using sstableloader, queries the data with cqlsh, and verifies the values
# match what ferrosa wrote.
#
# Prerequisites: Docker, Docker Compose, cargo
#
# Usage (from the repository root):
#   bash tests/sstable-compat/compat_test.sh
#
# The test passes when it exits 0 and prints "PASS" for each check.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
IMPORT_DIR="$SCRIPT_DIR/import"

echo "=== Ferrosa -> Cassandra 5.1 SSTable Compatibility Test ==="

# 1. Build the write-sstable binary.
echo "[1/6] Building write-sstable..."
cargo build --manifest-path "$SCRIPT_DIR/Cargo.toml" 2>&1

# 2. Write test SSTables into the import/ directory.
echo "[2/6] Writing SSTable..."
mkdir -p "$IMPORT_DIR"
"$REPO_ROOT/target/debug/write-sstable" "$IMPORT_DIR"

# 3. Start Cassandra via Docker Compose.
echo "[3/6] Starting Cassandra 5.1..."
docker compose -f "$SCRIPT_DIR/docker-compose.yml" up -d

# 4. Wait for Cassandra to become healthy.
echo "[4/6] Waiting for Cassandra to be ready (up to 120 s)..."
CASSANDRA_CTR=$(docker compose -f "$SCRIPT_DIR/docker-compose.yml" ps -q cassandra)
for i in $(seq 1 24); do
    if docker exec "$CASSANDRA_CTR" cqlsh -e "describe keyspaces" >/dev/null 2>&1; then
        echo "    Cassandra ready after $((i * 5)) s"
        break
    fi
    if [ "$i" -eq 24 ]; then
        echo "ERROR: Cassandra did not become ready within 120 s"
        docker compose -f "$SCRIPT_DIR/docker-compose.yml" logs cassandra | tail -30
        docker compose -f "$SCRIPT_DIR/docker-compose.yml" down
        exit 1
    fi
    sleep 5
done

# 5. Create the keyspace and table.
echo "[5/6] Creating schema..."
docker exec "$CASSANDRA_CTR" cqlsh -e "
    CREATE KEYSPACE IF NOT EXISTS compat
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    CREATE TABLE IF NOT EXISTS compat.simple_types (
        id       int     PRIMARY KEY,
        v_text   text,
        v_bool   boolean,
        v_bigint bigint,
        v_float  float,
        v_double double,
        v_blob   blob
    );
"

# Copy the SSTable files into the container.
docker cp "$IMPORT_DIR/compat/simple_types/." "$CASSANDRA_CTR:/import/compat/simple_types/"

# 6. Import using sstableloader.
echo "[6/6] Loading SSTable with sstableloader..."
docker exec "$CASSANDRA_CTR" sstableloader -d localhost /import/compat/simple_types

# 7. Query and verify.
echo ""
echo "=== Verifying round-trip values ==="
PASS=0
FAIL=0

check() {
    local desc="$1"
    local query="$2"
    local expected="$3"
    local result
    result=$(docker exec "$CASSANDRA_CTR" cqlsh -e "$query" 2>&1)
    if echo "$result" | grep -q "$expected"; then
        echo "  PASS: $desc"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $desc"
        echo "        Expected to find: $expected"
        echo "        Got: $result"
        FAIL=$((FAIL + 1))
    fi
}

check "v_text = 'hello'" \
    "SELECT v_text FROM compat.simple_types WHERE id = 1;" \
    "hello"

check "v_bool = true" \
    "SELECT v_bool FROM compat.simple_types WHERE id = 1;" \
    "True"

check "v_bigint = 42" \
    "SELECT v_bigint FROM compat.simple_types WHERE id = 1;" \
    "42"

check "v_blob = 0xdeadbeef" \
    "SELECT v_blob FROM compat.simple_types WHERE id = 1;" \
    "deadbeef"

echo ""
if [ "$FAIL" -eq 0 ]; then
    echo "All $PASS checks passed."
    docker compose -f "$SCRIPT_DIR/docker-compose.yml" down
    exit 0
else
    echo "$FAIL/$((PASS + FAIL)) checks FAILED."
    docker compose -f "$SCRIPT_DIR/docker-compose.yml" down
    exit 1
fi
