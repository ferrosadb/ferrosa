#!/usr/bin/env bash
# cqlsh smoke test — starts ferrosa and runs a full CQL workflow via cqlsh.
#
# Usage:
#   ./tests/cqlsh_smoke_test.sh
#
# Prerequisites:
#   - cargo build must succeed
#   - cqlsh must be on PATH
#
# Exit codes:
#   0 = all checks passed
#   1 = test failure

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

PORT=19042
DATA_DIR=$(mktemp -d)
FERROSA_PID=""

cleanup() {
    if [[ -n "$FERROSA_PID" ]]; then
        kill "$FERROSA_PID" 2>/dev/null || true
        wait "$FERROSA_PID" 2>/dev/null || true
    fi
    rm -rf "$DATA_DIR"
}
trap cleanup EXIT

echo "=== Building ferrosa ==="
cargo build --manifest-path "$PROJECT_DIR/Cargo.toml" 2>&1 | tail -1

echo "=== Starting ferrosa on port $PORT ==="
FERROSA_DATA_DIR="$DATA_DIR" \
FERROSA_CQL_BIND="127.0.0.1:$PORT" \
FERROSA_AUTH_DISABLED=1 \
    cargo run --manifest-path "$PROJECT_DIR/Cargo.toml" 2>"$DATA_DIR/ferrosa.log" &
FERROSA_PID=$!

# Wait for server to be ready
echo "Waiting for server..."
for i in $(seq 1 30); do
    if nc -z 127.0.0.1 "$PORT" 2>/dev/null; then
        echo "Server ready after ${i}s"
        break
    fi
    if ! kill -0 "$FERROSA_PID" 2>/dev/null; then
        echo "FAIL: ferrosa exited unexpectedly"
        cat "$DATA_DIR/ferrosa.log"
        exit 1
    fi
    sleep 1
done

if ! nc -z 127.0.0.1 "$PORT" 2>/dev/null; then
    echo "FAIL: server did not start within 30s"
    cat "$DATA_DIR/ferrosa.log"
    exit 1
fi

PASS=0
FAIL=0

run_cql() {
    local desc="$1"
    local cql="$2"
    printf "  %-50s " "$desc"
    if output=$(echo "$cql" | cqlsh 127.0.0.1 "$PORT" 2>&1); then
        echo "PASS"
        PASS=$((PASS + 1))
    else
        echo "FAIL"
        echo "    Output: $output"
        FAIL=$((FAIL + 1))
    fi
}

echo ""
echo "=== Phase 1: Introspection (cqlsh startup queries) ==="
run_cql "SELECT system.local" "SELECT cluster_name, tokens FROM system.local;"
run_cql "SELECT system.peers" "SELECT * FROM system.peers;"
run_cql "SELECT system_schema.keyspaces" "SELECT * FROM system_schema.keyspaces;"
run_cql "SELECT system_schema.tables" "SELECT * FROM system_schema.tables;"
run_cql "SELECT system_schema.columns" "SELECT * FROM system_schema.columns;"
run_cql "SELECT system_schema.types" "SELECT * FROM system_schema.types;"
run_cql "SELECT system_schema.functions" "SELECT * FROM system_schema.functions;"
run_cql "SELECT system_schema.aggregates" "SELECT * FROM system_schema.aggregates;"
run_cql "SELECT system_schema.views" "SELECT * FROM system_schema.views;"
run_cql "SELECT system_schema.indexes" "SELECT * FROM system_schema.indexes;"

echo ""
echo "=== Phase 2: DDL ==="
run_cql "CREATE KEYSPACE" \
    "CREATE KEYSPACE test_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
run_cql "CREATE TABLE users" \
    "CREATE TABLE test_ks.users (id int PRIMARY KEY, name text, email text);"
run_cql "CREATE TABLE events" \
    "CREATE TABLE test_ks.events (user_id int, ts timestamp, data text, PRIMARY KEY (user_id, ts));"

echo ""
echo "=== Phase 3: DML writes ==="
run_cql "INSERT user 1" \
    "INSERT INTO test_ks.users (id, name, email) VALUES (1, 'Alice', 'alice@test.com');"
run_cql "INSERT user 2" \
    "INSERT INTO test_ks.users (id, name, email) VALUES (2, 'Bob', 'bob@test.com');"
run_cql "INSERT event 1" \
    "INSERT INTO test_ks.events (user_id, ts, data) VALUES (1, '2024-01-01 00:00:00+0000', 'login');"
run_cql "INSERT event 2" \
    "INSERT INTO test_ks.events (user_id, ts, data) VALUES (1, '2024-01-01 01:00:00+0000', 'logout');"

echo ""
echo "=== Phase 4: DML reads ==="
run_cql "SELECT user by PK" \
    "SELECT * FROM test_ks.users WHERE id = 1;"
run_cql "SELECT all events for user" \
    "SELECT * FROM test_ks.events WHERE user_id = 1;"

echo ""
echo "=== Phase 5: UPDATE and DELETE ==="
run_cql "UPDATE user name" \
    "UPDATE test_ks.users SET name = 'Alice Updated' WHERE id = 1;"
run_cql "DELETE event" \
    "DELETE FROM test_ks.events WHERE user_id = 1 AND ts = '2024-01-01 00:00:00+0000';"

echo ""
echo "=== Phase 6: Verify mutations ==="
run_cql "Verify UPDATE" \
    "SELECT name FROM test_ks.users WHERE id = 1;"

echo ""
echo "=== Phase 7: DDL cleanup ==="
run_cql "DROP TABLE users" "DROP TABLE test_ks.users;"
run_cql "DROP TABLE events" "DROP TABLE test_ks.events;"
run_cql "DROP KEYSPACE" "DROP KEYSPACE test_ks;"

echo ""
echo "=============================="
echo "Results: $PASS passed, $FAIL failed"
echo "=============================="

if [[ $FAIL -gt 0 ]]; then
    echo ""
    echo "Server log:"
    tail -20 "$DATA_DIR/ferrosa.log"
    exit 1
fi
