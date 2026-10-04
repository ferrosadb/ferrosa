#!/usr/bin/env bash
# End-to-end Docker smoke test for two-node pair mode with full failover lifecycle,
# 3-node Raft cluster (C1-C10), 5-node production scenarios (F1-F6),
# node lifecycle tests (L1-L7), and FMEA failure scenarios.
#
# Prerequisites:
#   - Docker and Docker Compose installed
#   - cqlsh available (pip install cqlsh or use the cassandra package)
#   - curl available
#
# Usage:
#   ./tests/docker-smoke.sh                         # Pair mode phases 1-13
#   ./tests/docker-smoke.sh --cluster-trio          # 3-node cluster (C1-C10)
#   ./tests/docker-smoke.sh --cluster-quint         # 5-node cluster (F1-F6)
#   ./tests/docker-smoke.sh --lifecycle             # Node lifecycle (L1-L7)
#   ./tests/docker-smoke.sh --fmea                  # FMEA scenarios
#   ./tests/docker-smoke.sh --all                   # All suites
#
# Test lifecycle:
#
#   PAIR MODE (2 nodes):
#   Phase 1  — Bidirectional reads and writes + DDL replication
#   Phase 2  — Primary failure (unpromoted secondary rejects reads and writes)
#   Phase 3  — Operator promotion (reads and writes resume)
#   Phase 4  — Rejoin and catch-up (schema + data)
#   Phase 5  — Switchover (swap roles)
#
#   CLUSTER MODE (3 nodes):
#   Phase 6  — 3rd node joins, cluster forms
#   Phase 7  — 3-node writes/reads (any-node coordinator)
#   Phase 8  — 1 node down: QUORUM writes/reads succeed
#   Phase 9  — 2 nodes down: below QUORUM, writes fail
#   Phase 10 — Cluster recovery: nodes rejoin, writes resume
#   Phase 11 — DDL replication across 3 nodes
#
#   FMEA FAILURE MODES:
#   Phase 12 — FMEA-driven tests:
#     #14 Data on 3rd node (pre-cluster data accessible)
#     #20 DDL on follower (forwarded to leader)
#     #18 Stale data after rejoin (catch-up)
#     #7  Token distribution (balanced)
#     #9  Write timeout (no indefinite hang)
#
#   HARDENING:
#   Phase 13 — Cross-node subscription test
#
#   3-NODE CLUSTER SUITE (C1-C10):
#   C1  — Raft leader election (system.peers shows 3 nodes)
#   C2  — DDL replication via Raft (CREATE KEYSPACE on leader, visible everywhere)
#   C3  — QUORUM writes and reads (100 rows)
#   C4  — Node failure tolerance (kill node3, write at QUORUM)
#   C5  — Read QUORUM with node down (150 rows)
#   C6  — CL=ALL fails with node down
#   C7  — Reconnection after restart (system.peers shows 3 again)
#   C8  — Hint replay verification (150 rows from restarted node)
#   C9  — Raft leader failover
#   C10 — DDL on new leader replicates to all nodes
#
#   5-NODE CLUSTER SUITE (F1-F6):
#   F1  — 5-node Raft group forms, leader elected within 15s
#   F2  — QUORUM writes/reads across all 5 nodes (200 rows)
#   F3  — Kill 2 nodes; QUORUM writes still succeed (RF=3, QUORUM=2)
#   F4  — Kill Raft leader; new leader elected within 10s
#   F5  — Restart both killed nodes; hints replay (300 rows everywhere within 120s)
#   F6  — SELECT at ALL returns consistent data across all 5 nodes
#
#   NODE LIFECYCLE SUITE (L1-L7):
#   L1  — Start 3-node cluster, add-node, 4th node appears in system.peers
#   L2  — 4th node bootstraps via S3 + delta stream (has all existing data)
#   L3  — Write at QUORUM; 4th node receives new writes (readable at ONE)
#   L4  — Decommission 4th node (removed from system.peers within 120s)
#   L5  — 3 remaining nodes have all data (SELECT at ALL)
#   L6  — 5-node cluster: add 4th and 5th via lifecycle
#   L7  — Rebalance after adding nodes (token skew < 5%, no data loss)
#
#   FMEA SCENARIOS:
#   FMEA-1 — Network partition: isolate 1 of 3; majority continues; heal + catch-up
#   FMEA-2 — Coordinator crash mid-write: WriteTimeout; no partial writes at QUORUM
#   FMEA-3 — Raft leader disk full: leader steps down; new election within 10s
#   FMEA-4 — Hint directory full: oldest hints evicted; needs_repair=true in peers
#   FMEA-5 — S3 unavailable during bootstrap: join fails gracefully; retry succeeds
#   FMEA-6 — Rapid leader churn: kill/restart leader 3x in 30s; cluster recovers

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_DIR"

# ---------------------------------------------------------------------------
# Mode flags — parse command-line arguments
# ---------------------------------------------------------------------------
RUN_PAIR=true
RUN_TRIO=false
RUN_QUINT=false
RUN_LIFECYCLE=false
RUN_FMEA=false

for arg in "$@"; do
    case "$arg" in
        --cluster-trio)  RUN_PAIR=false; RUN_TRIO=true ;;
        --cluster-quint) RUN_PAIR=false; RUN_QUINT=true ;;
        --lifecycle)     RUN_PAIR=false; RUN_LIFECYCLE=true ;;
        --fmea)          RUN_PAIR=false; RUN_FMEA=true ;;
        --all)           RUN_TRIO=true; RUN_QUINT=true; RUN_LIFECYCLE=true; RUN_FMEA=true ;;
        --help)
            sed -n '2,/^set -uo pipefail/p' "$0" | grep '^#' | sed 's/^# \{0,1\}//'
            exit 0
            ;;
    esac
done

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m'

PAIR_CQL_TIMEOUT="${PAIR_CQL_TIMEOUT:-180}"

collect_default_compose_logs() {
    # Keep failure artifacts before the EXIT trap removes containers. The
    # workflow's follow-up collection step may run after cleanup has completed.
    docker compose ps > docker-compose-ps.log 2>&1 || true
    for service in node1 node2 node3 rustfs rustfs-init; do
        docker compose logs "$service" > "${service}.log" 2>&1 || true
    done
}

preflight_fail() {
    echo -e "${RED}FAIL${NC}: $1"
    exit 1
}

require_command() {
    local name=$1
    if ! command -v "$name" >/dev/null 2>&1; then
        preflight_fail "Required command not found: $name"
    fi
}

pass() { echo -e "${GREEN}PASS${NC}: $1"; }
fail() {
    echo -e "${RED}FAIL${NC}: $1"
    collect_default_compose_logs
    exit 1
}
info() { echo -e "${YELLOW}INFO${NC}: $1"; }

# Compose file for the cluster suite (trio / quint)
CLUSTER_COMPOSE="tests/docker-compose.cluster.yml"

# ---------------------------------------------------------------------------
# Helper: cluster CQL helpers (ports assigned by docker-compose.cluster.yml)
# ---------------------------------------------------------------------------
# cql_c<N> — CQL to node N in the cluster compose stack
cql_c1() { cqlsh localhost 9042 -e "$1" 2>/dev/null; }
cql_c2() { cqlsh localhost 9043 -e "$1" 2>/dev/null; }
cql_c3() { cqlsh localhost 9044 -e "$1" 2>/dev/null; }
cql_c4() { cqlsh localhost 9045 -e "$1" 2>/dev/null; }
cql_c5() { cqlsh localhost 9046 -e "$1" 2>/dev/null; }

# Bound each failed probe so a nominal readiness timeout remains a real
# wall-clock timeout instead of multiplying by cqlsh's connection timeout.
cql_ready() {
    local port=$1
    cqlsh --connect-timeout=2 --request-timeout=2 localhost "$port" \
        -e "SELECT cluster_name FROM system.local" >/dev/null 2>&1
}

# Helper: wait for CQL on cluster-compose nodes
wait_cql_c() {
    local port=$1 name=$2 timeout=${3:-60}
    local deadline=$((SECONDS + timeout))
    info "Waiting for $name CQL (port $port)..."
    while (( SECONDS < deadline )); do
        if cql_ready "$port"; then
            pass "$name CQL is ready"
            return 0
        fi
        sleep 1
    done
    fail "$name CQL did not become ready in ${timeout}s"
}

# Helper: cluster REST API (cluster compose — web ports start at 9090)
cluster_api_c() {
    local node=$1 path=${2:-/api/cluster/status}
    local port=$((9089 + node))
    curl -s "http://localhost:${port}${path}"
}

# Helper: count peers via CQL (returns integer)
peer_count() {
    local cql_fn=$1
    $cql_fn "SELECT peer FROM system.peers;" 2>/dev/null \
        | grep -c '[0-9]\{1,3\}\.[0-9]' || echo 0
}

# Helper: poll until system.peers shows at least N peers on a node
wait_peers() {
    local cql_fn=$1 expected=$2 timeout=${3:-30}
    for i in $(seq 1 "$timeout"); do
        local count
        count=$(peer_count "$cql_fn")
        if [ "$count" -ge "$expected" ]; then
            return 0
        fi
        sleep 1
    done
    return 1
}

# Helper: determine which node is the current Raft leader by polling /api/cluster/status
# Returns the node number (1-5) or empty string if no leader found.
find_leader() {
    local max_node=${1:-3}
    for n in $(seq 1 "$max_node"); do
        local port=$((9089 + n))
        local mode
        mode=$(curl -s "http://localhost:${port}/api/cluster/status" 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('mode',''))" 2>/dev/null || true)
        if [ "$mode" = "cluster" ]; then
            echo "$n"
            return 0
        fi
    done
    echo ""
}

# Helper: clean up cluster compose stack
cleanup_cluster() {
    info "Tearing down cluster stack..."
    docker compose -f "$CLUSTER_COMPOSE" down -v --remove-orphans 2>/dev/null || true
}

cleanup() {
    info "Tearing down..."
    docker compose down -v --remove-orphans 2>/dev/null || true
}
trap cleanup EXIT

require_command docker
require_command cqlsh
require_command curl

# Helper: run CQL and capture output (suppress cqlsh version warnings)
cql1() { cqlsh localhost 9042 -e "$1" 2>/dev/null; }
cql2() { cqlsh localhost 9043 -e "$1" 2>/dev/null; }
cql3() { cqlsh localhost 9044 -e "$1" 2>/dev/null; }

# Helper: wait for CQL port
wait_cql() {
    local port=$1 name=$2 timeout=${3:-60}
    local deadline=$((SECONDS + timeout))
    info "Waiting for $name CQL (port $port)..."
    while (( SECONDS < deadline )); do
        if cql_ready "$port"; then
            pass "$name CQL is ready"
            return 0
        fi
        sleep 1
    done
    fail "$name CQL did not become ready in ${timeout}s"
}

# Helper: wait for an acknowledged mutation to become query-visible. Pair
# startup can briefly overlap topology/schema convergence, so a one-shot read
# is not a stable correctness assertion. Keep the retry bounded and report the
# final response when the value never appears.
wait_for_cql_value() {
    local port=$1 query=$2 expected=$3 description=$4 timeout=${5:-30}
    local deadline=$((SECONDS + timeout))
    local last_result=""

    while (( SECONDS < deadline )); do
        last_result=$(cqlsh --connect-timeout=2 --request-timeout=2 \
            localhost "$port" -e "$query" 2>&1) || true
        if grep -Fq -- "$expected" <<<"$last_result"; then
            return 0
        fi
        sleep 1
    done

    info "$description last query result: $last_result"
    fail "$description did not become visible in ${timeout}s (expected: $expected)"
}

# Helper: wait for a pair member to converge to its expected role. A secondary
# is intentionally healthy while rejecting client CQL, so CQL readiness is the
# wrong signal for pair-role transitions.
wait_for_cluster_role() {
    local port=$1 name=$2 expected_role=$3 timeout=${4:-60}
    local deadline=$((SECONDS + timeout))
    local last_status=""

    while (( SECONDS < deadline )); do
        last_status=$(curl --connect-timeout 2 --max-time 2 -sf \
            "http://localhost:${port}/api/cluster/status" 2>&1) || true
        if grep -Fq -- "\"role\":\"${expected_role}\"" <<<"$last_status"; then
            pass "$name converged to pair role $expected_role"
            return 0
        fi
        sleep 1
    done

    info "$name last cluster status: $last_status"
    fail "$name did not converge to pair role $expected_role in ${timeout}s"
}

# Helper: check cluster status via REST API
cluster_status() {
    local port=$1
    curl -s "http://localhost:${port}/api/cluster/status"
}

# Helper: run one CQL statement and FAIL the run if it errors. The bare
# cql1/cql2/cql3 helpers discard the exit status, so a refused write or DDL
# used to be followed by an unconditional `pass`.
cql_ok() {
    local port=$1 stmt=$2 description=$3
    local out
    if ! out=$(cqlsh --request-timeout=10 localhost "$port" -e "$stmt" 2>&1); then
        fail "$description: $out"
    fi
}

# Helper: the schema version a node holds, from its own membership snapshot.
# Pair DDL stamps the primary's version on the secondary, and pair catch-up
# acknowledges only after adopting it, so equal versions mean equal schemas.
schema_version_of() {
    local port=$1
    curl --connect-timeout 2 --max-time 2 -sf "http://localhost:${port}/admin/membership-snapshot" \
        | python3 -c "import sys,json; print(json.load(sys.stdin).get('schema_version',''))" 2>/dev/null
}

wait_for_schema_agreement() {
    local port_a=$1 port_b=$2 description=$3 timeout=${4:-30}
    local deadline=$((SECONDS + timeout)) va="" vb=""
    while (( SECONDS < deadline )); do
        va=$(schema_version_of "$port_a")
        vb=$(schema_version_of "$port_b")
        if [ -n "$va" ] && [ "$va" = "$vb" ]; then
            pass "$description (schema version $va)"
            return 0
        fi
        sleep 1
    done
    fail "$description: schema versions differ after ${timeout}s (port $port_a='$va', port $port_b='$vb')"
}

# Helper: wait until `SELECT COUNT(*) FROM <table>` via <port> reaches at least
# <expected>, at consistency <cl> (default ONE); FAIL with the last count.
wait_for_row_count() {
    local port=$1 table=$2 expected=$3 description=$4 timeout=${5:-30} cl=${6:-ONE}
    local deadline=$((SECONDS + timeout)) count=0
    while (( SECONDS < deadline )); do
        count=$(cqlsh --request-timeout=10 localhost "$port" \
            -e "CONSISTENCY ${cl}; SELECT COUNT(*) FROM ${table};" 2>/dev/null \
            | grep -Eo '^ *[0-9]+ *$' | tr -d ' ' | tail -1)
        count=${count:-0}
        if [ "$count" -ge "$expected" ]; then
            pass "$description: $count rows (>= $expected)"
            return 0
        fi
        sleep 1
    done
    fail "$description: $count rows after ${timeout}s (expected >= $expected)"
}

# Helper: wait for a node to report cluster mode.
wait_for_cluster_mode() {
    local port=$1 name=$2 timeout=${3:-60}
    local deadline=$((SECONDS + timeout)) last=""
    while (( SECONDS < deadline )); do
        last=$(cluster_status "$port" 2>/dev/null || true)
        if grep -q '"mode":"cluster"' <<<"$last"; then
            pass "$name in cluster mode"
            return 0
        fi
        sleep 1
    done
    fail "$name did not reach cluster mode in ${timeout}s; last status: $last"
}

if $RUN_PAIR; then

# ============================================================
# Phase 1: Build, start, bidirectional writes
# ============================================================
info "=== Phase 1: Bidirectional reads and writes ==="

info "Building and starting pair services..."
docker compose up -d --build node1 node2

# Roles are deterministic: `choose_pair_role` gives Primary to the lowest
# host_id and docker-compose.yml pins node1 below node2. Assert the role before
# waiting on CQL — when the pins regress, node1 comes up a secondary that
# refuses CQL while still reporting healthy on /readyz, and a bare `wait_cql`
# would burn the full timeout before failing with no explanation.
info "Waiting for node1 to become the pair primary..."
wait_for_cluster_role 9090 "node1" "primary" "$PAIR_CQL_TIMEOUT"

wait_cql 9042 "node1" "$PAIR_CQL_TIMEOUT"

# node2 is the secondary. The CQL server rejects all connections on secondaries
# (only the primary serves CQL), so wait for the explicit role rather than
# treating a rejected CQL connection as unready.
info "Waiting for node2 to become the pair secondary..."
wait_for_cluster_role 9091 "node2" "secondary" "$PAIR_CQL_TIMEOUT"

info "Waiting for pair mode activation..."
sleep 5

# Create schema on node1 only — DDL replication should propagate to node2
info "Creating keyspace and table on node1..."
cql_ok 9042 "CREATE KEYSPACE smoke_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}" \
    "CREATE KEYSPACE on node1"
cql_ok 9042 "CREATE TABLE smoke_test.kv (k text PRIMARY KEY, v text)" "CREATE TABLE on node1"
pass "Schema created on node1"

# Verify schema replicated to node2. A pair secondary rejects CQL, so compare
# the schema version each node reports for itself: pair DDL stamps the
# primary's version on the secondary when it applies the replicated change.
# (This used to curl /api/schema/keyspaces, a route that does not exist, so
# the check could never pass and was demoted to INFO.)
info "Verifying schema replicated to node2..."
wait_for_schema_agreement 9090 9091 "Schema replicated to node2 via pair DDL"

# Test ALTER TABLE replication
info "Testing ALTER TABLE replication..."
cql_ok 9042 "ALTER TABLE smoke_test.kv ADD extra text" "ALTER TABLE on node1"
cql_ok 9042 "SELECT extra FROM smoke_test.kv WHERE k = 'nonexistent';" \
    "node1 must know the ALTERed column"
wait_for_schema_agreement 9090 9091 "ALTER TABLE replicated to node2 via pair DDL"

# Write to node1 (primary), read from both
info "Writing to node1 (primary)..."
cql_ok 9042 "INSERT INTO smoke_test.kv (k, v) VALUES ('key1', 'from_node1')" "write key1 to node1"
pass "Write to node1 succeeded"
sleep 1

wait_for_cql_value 9042 "SELECT v FROM smoke_test.kv WHERE k = 'key1';" \
    "from_node1" "node1 key1"
pass "Read from node1: key1=from_node1"

# Read from node2 — in pair mode, node2 is the secondary and rejects CQL
# connections. Reads should succeed after node1 dies (Phase 2) when node2
# auto-transitions to degraded/standalone mode. Skip this check here.
info "Skipping CQL read from node2 (secondary rejects CQL until promoted)"

# Write to node2 (secondary, should be forwarded to primary)
# CQL forwarding from secondaries is not supported — writes go to node1.
info "Writing to node1 instead (node2 is secondary, no CQL forwarding)..."
cql_ok 9042 "INSERT INTO smoke_test.kv (k, v) VALUES ('key2', 'from_node2')" "write key2 to node1"
pass "Write to node1 succeeded (on behalf of node2)"
sleep 1

wait_for_cql_value 9042 "SELECT v FROM smoke_test.kv WHERE k = 'key2';" \
    "from_node2" "node1 key2"
pass "Read from node1: key2=from_node2"

# Read from node2 will be verified after Phase 3 operator promotion.
info "Read from node2 deferred to Phase 3 (after operator promotion)"

# ============================================================
# Phase 2: Kill primary, verify degraded behavior
# ============================================================
info ""
info "=== Phase 2: Primary failure ==="

info "Killing node1..."
docker compose stop node1
sleep 3

# An unpromoted secondary must not serve client reads or writes: allowing either
# would permit split brain after the primary is lost. An operator promotes it in
# Phase 3 once that decision is safe.
info "Verifying unpromoted node2 rejects CQL reads..."
if cql2 "SELECT v FROM smoke_test.kv WHERE k = 'key1';" 2>&1; then
    fail "Unpromoted node2 served a CQL read after node1 death"
fi
pass "Unpromoted node2 rejects CQL reads after node1 death"

info "Verifying unpromoted node2 rejects CQL writes..."
if cql2 "INSERT INTO smoke_test.kv (k, v) VALUES ('should_fail', 'nope')" 2>&1; then
    fail "Unpromoted node2 served a CQL write after node1 death"
fi
pass "Unpromoted node2 rejects CQL writes after node1 death"

# ============================================================
# Phase 3: Operator promotion
# ============================================================
info ""
info "=== Phase 3: Operator promotion ==="

info "Checking node2 cluster status..."
STATUS=$(cluster_status 9091)
info "Node2 status: $STATUS"

info "Promoting node2 to standalone primary..."
PROMOTE_RESULT=$(curl -s -X POST "http://localhost:9091/api/cluster/promote")
info "Promote result: $PROMOTE_RESULT"
echo "$PROMOTE_RESULT" | grep -q "promoted" || fail "Promote failed"
pass "Node2 promoted to standalone primary"

wait_cql 9043 "node2" "$PAIR_CQL_TIMEOUT"

# Promotion makes the replicated data safe to serve from node2.
info "Verifying replicated reads on promoted node2..."
wait_for_cql_value 9043 "SELECT v FROM smoke_test.kv WHERE k = 'key1';" \
    "from_node1" "promoted node2 key1"
pass "Promoted node2 reads replicated key1=from_node1"

wait_for_cql_value 9043 "SELECT v FROM smoke_test.kv WHERE k = 'key2';" \
    "from_node2" "promoted node2 key2"
pass "Promoted node2 reads replicated key2=from_node2"

# Writes should now work on node2
info "Writing failover data on promoted node2..."
cql_ok 9043 "INSERT INTO smoke_test.kv (k, v) VALUES ('failover1', 'during_failover')" \
    "failover write failover1 on promoted node2"
cql_ok 9043 "INSERT INTO smoke_test.kv (k, v) VALUES ('failover2', 'also_failover')" \
    "failover write failover2 on promoted node2"
pass "Failover writes succeeded on promoted node2"

# Verify reads work
wait_for_cql_value 9043 "SELECT v FROM smoke_test.kv WHERE k = 'failover1';" \
    "during_failover" "promoted node2 failover1"
pass "Failover data readable on node2: failover1=during_failover"

# ============================================================
# Phase 4: Rejoin and catch-up
# ============================================================
info ""
info "=== Phase 4: Rejoin and catch-up ==="

info "Restarting node1..."
docker compose start node1
wait_for_cluster_role 9090 "node1" "secondary" "$PAIR_CQL_TIMEOUT"

# Verify schema was replicated via catch-up. Node1 is a healthy secondary and
# intentionally rejects CQL, so compare the schema version each node holds:
# PairSchemaSync acknowledges only after the secondary's keyspaces and tables
# match the primary's, and then adopts the primary's version. node1 restarted
# from a schema.json written before the ALTER, so this is the check that
# catches a catch-up that inserted missing tables but never applied the ALTER.
info "Verifying schema catch-up on node1..."
wait_for_schema_agreement 9090 9091 "Schema catch-up: rejoined node1 matches promoted node2" 60

# ============================================================
# Phase 5: Switchover
# ============================================================
info ""
info "=== Phase 5: Switchover ==="

# Check current roles
info "Node1 status: $(cluster_status 9090)"
info "Node2 status: $(cluster_status 9091)"

# Switchover: promote node1 back to primary (called from current primary = node2)
info "Initiating switchover from node2 (current primary) to node1..."
SWITCHOVER_RESULT=$(curl -s -X POST "http://localhost:9091/api/cluster/switchover")
info "Switchover result: $SWITCHOVER_RESULT"

# Both nodes are connected pair members here, so a switchover must succeed.
# It is refused when node1 has not caught up (schema unconfirmed or data
# replay incomplete); that refusal is a failure of Phase 4, reported here.
if ! echo "$SWITCHOVER_RESULT" | grep -q "switchover complete"; then
    fail "Switchover refused or failed: $SWITCHOVER_RESULT"
fi
pass "Switchover completed successfully"

wait_for_cluster_role 9090 "node1" "primary" "$PAIR_CQL_TIMEOUT"
wait_cql 9042 "node1" "$PAIR_CQL_TIMEOUT"
info "Node1 status: $(cluster_status 9090)"
info "Node2 status: $(cluster_status 9091)"

# Node1 can now serve the data checks that were intentionally unavailable
# while it was the rejoining secondary.
wait_for_cql_value 9042 "SELECT v FROM smoke_test.kv WHERE k = 'key1';" \
    "from_node1" "switched node1 key1"
wait_for_cql_value 9042 "SELECT v FROM smoke_test.kv WHERE k = 'failover1';" \
    "during_failover" "switched node1 failover1"
pass "Rejoined node1 serves original and failover data"

# Verify writes work through node1 after switchover
info "Writing through node1 after switchover..."
cql_ok 9042 "INSERT INTO smoke_test.kv (k, v) VALUES ('post_switch1', 'via_node1')" \
    "write through node1 after switchover"
pass "Write to node1 succeeded after switchover"

# Informational by design: node2 is now the secondary and rejects CQL.
info "Skipping node2 CQL read while it is the post-switchover secondary"

# ============================================================
# Phase 6: 3rd node joins → Cluster mode
# ============================================================
info ""
info "=== Phase 6: Cluster Formation ==="

info "Starting node3..."
docker compose up -d --build node3
wait_cql 9044 "node3" "$PAIR_CQL_TIMEOUT"

info "Waiting for cluster formation..."
wait_for_cluster_mode 9090 "Node1" 90
wait_for_cluster_mode 9091 "Node2" 90
wait_for_cluster_mode 9092 "Node3" 90

# ============================================================
# Phase 7: 3-node writes and reads
# ============================================================
info ""
info "=== Phase 7: 3-Node Writes and Reads ==="

# Create schema for cluster testing
info "Creating cluster test keyspace..."
cql_ok 9042 "CREATE KEYSPACE IF NOT EXISTS cluster_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}" \
    "CREATE KEYSPACE cluster_test"
cql_ok 9042 "CREATE TABLE IF NOT EXISTS cluster_test.data (k text PRIMARY KEY, v text, source text)" \
    "CREATE TABLE cluster_test.data"
for port in 9043 9044; do
    wait_for_cql_value "$port" "SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'cluster_test';" \
        "data" "cluster_test.data visible on port $port"
done

# Write to each node — coordinator should route to replicas
info "Writing to node1..."
cql_ok 9042 "INSERT INTO cluster_test.data (k, v, source) VALUES ('key_a', 'value_a', 'node1')" "write key_a via node1"
pass "Write to node1 succeeded"

info "Writing to node2..."
cql_ok 9043 "INSERT INTO cluster_test.data (k, v, source) VALUES ('key_b', 'value_b', 'node2')" "write key_b via node2"
pass "Write to node2 succeeded"

info "Writing to node3..."
cql_ok 9044 "INSERT INTO cluster_test.data (k, v, source) VALUES ('key_c', 'value_c', 'node3')" "write key_c via node3"
pass "Write to node3 succeeded"

# Read from each node — any node should coordinate reads
info "Reading key_a from node3 (cross-node read)..."
wait_for_cql_value 9044 "SELECT v FROM cluster_test.data WHERE k = 'key_a';" "value_a" "node3 cross-node read key_a"
pass "Node3 reads data written to node1: key_a=value_a"

info "Reading key_c from node1 (cross-node read)..."
wait_for_cql_value 9042 "SELECT v FROM cluster_test.data WHERE k = 'key_c';" "value_c" "node1 cross-node read key_c"
pass "Node1 reads data written to node3: key_c=value_c"

info "Reading key_b from node2 (local read)..."
wait_for_cql_value 9043 "SELECT v FROM cluster_test.data WHERE k = 'key_b';" "value_b" "node2 read key_b"
pass "Node2 reads own data: key_b=value_b"

# ============================================================
# Phase 8: Single node failure — QUORUM still works
# ============================================================
info ""
info "=== Phase 8: Single Node Failure (QUORUM) ==="

info "Stopping node3..."
docker compose stop node3
sleep 5

# Writes should succeed (2 of 3 alive, QUORUM = 2). The consistency level is
# set explicitly: cqlsh defaults to ONE, under which this phase and Phase 9
# assert nothing about quorum at all.
info "Writing with 1 node down (QUORUM should succeed)..."
cql_ok 9042 "CONSISTENCY QUORUM; INSERT INTO cluster_test.data (k, v, source) VALUES ('after_kill3', 'quorum_ok', 'node1')" \
    "QUORUM write with 1 of 3 nodes down"
pass "Write succeeds with 1 node down (QUORUM met: 2 of 3)"

# Reads should succeed
info "Reading with 1 node down..."
wait_for_cql_value 9043 "CONSISTENCY QUORUM; SELECT v FROM cluster_test.data WHERE k = 'key_a';" \
    "value_a" "QUORUM read with 1 node down"
pass "Read succeeds with 1 node down"

# ============================================================
# Phase 9: Second node failure — below QUORUM
# ============================================================
info ""
info "=== Phase 9: Second Node Failure (Below QUORUM) ==="

info "Stopping node2..."
docker compose stop node2
sleep 3

# Writes should FAIL (only 1 of 3 alive, QUORUM = 2, not met). An
# acknowledged QUORUM write here is a correctness failure, whatever the reason.
info "Writing with 2 nodes down (should fail — below QUORUM)..."
if cqlsh --request-timeout=15 localhost 9042 \
    -e "CONSISTENCY QUORUM; INSERT INTO cluster_test.data (k, v, source) VALUES ('should_fail', 'no', 'node1')" \
    >/dev/null 2>&1; then
    fail "QUORUM write was acknowledged with 2 of 3 nodes down"
fi
pass "Write correctly fails with 2 nodes down (below QUORUM)"

# RF=3, so node1 holds every row and a CL=ONE read is served locally.
info "Reading local data with 2 nodes down..."
wait_for_cql_value 9042 "CONSISTENCY ONE; SELECT v FROM cluster_test.data WHERE k = 'key_a';" \
    "value_a" "CL=ONE local read with 2 nodes down"
pass "Local reads still work with 2 nodes down"

# ============================================================
# Phase 10: Recovery — bring nodes back
# ============================================================
info ""
info "=== Phase 10: Cluster Recovery ==="

info "Restarting node2 and node3..."
docker compose start node2 node3
wait_cql 9043 "node2" "$PAIR_CQL_TIMEOUT"
wait_cql 9044 "node3" "$PAIR_CQL_TIMEOUT"

# Verify cluster re-forms
wait_for_cluster_mode 9090 "Node1" 90
wait_for_cluster_mode 9091 "Node2" 90
wait_for_cluster_mode 9092 "Node3" 90

# Writes should work again
info "Writing after recovery..."
cql_ok 9042 "CONSISTENCY QUORUM; INSERT INTO cluster_test.data (k, v, source) VALUES ('recovered', 'yes', 'node1')" \
    "QUORUM write after recovery"
pass "Write succeeds after cluster recovery"

# Cross-node reads should work
wait_for_cql_value 9044 "SELECT v FROM cluster_test.data WHERE k = 'recovered';" "yes" \
    "node3 cross-node read after recovery"
pass "Cross-node read works after recovery"

# Data written during degraded mode should be readable
wait_for_cql_value 9044 "SELECT v FROM cluster_test.data WHERE k = 'after_kill3';" "quorum_ok" \
    "node3 read of data written while it was down" 60
pass "Data from degraded mode survived and replicated"

# ============================================================
# Phase 11: DDL replication across 3 nodes
# ============================================================
info ""
info "=== Phase 11: DDL Replication (3 nodes) ==="

# Create schema on node3, verify on node1 and node2
info "Creating keyspace on node3..."
cql_ok 9044 "CREATE KEYSPACE ddl_cluster WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}" \
    "CREATE KEYSPACE ddl_cluster via node3"
cql_ok 9044 "CREATE TABLE ddl_cluster.items (id text PRIMARY KEY, name text)" \
    "CREATE TABLE ddl_cluster.items via node3"

info "Verifying DDL on node1..."
wait_for_cql_value 9042 "SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'ddl_cluster';" \
    "items" "DDL replication to node1"
pass "DDL replicated to node1"

info "Verifying DDL on node2..."
wait_for_cql_value 9043 "SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'ddl_cluster';" \
    "items" "DDL replication to node2"
pass "DDL replicated to node2"

# ============================================================
# Phase 12: FMEA-driven failure mode tests
# ============================================================
info ""
info "=== Phase 12: FMEA Failure Mode Coverage ==="

# FMEA #14 (RPN 240): Data accessibility on 3rd node
# Data written before node3 joined should be readable on node3
info "[FMEA #14] Data written before cluster should be on node3..."
wait_for_cql_value 9044 "SELECT v FROM smoke_test.kv WHERE k = 'key1';" "from_node1" \
    "[FMEA #14] pre-cluster key1 read via node3" 60
pass "[FMEA #14] Pre-cluster data accessible on node3"

# FMEA #20 (RPN 175): DDL on non-leader/follower node
# Creating a table on a follower should succeed (forwarded to leader)
info "[FMEA #20] DDL on follower node..."
cql_ok 9043 "CREATE KEYSPACE IF NOT EXISTS fmea_ddl WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}" \
    "[FMEA #20] CREATE KEYSPACE on a follower"
cql_ok 9043 "CREATE TABLE IF NOT EXISTS fmea_ddl.test (id text PRIMARY KEY)" \
    "[FMEA #20] CREATE TABLE on a follower"
wait_for_cql_value 9042 "SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'fmea_ddl';" \
    "test" "[FMEA #20] follower DDL visible on node1"
pass "[FMEA #20] DDL on follower succeeded and replicated"

# FMEA #18 (RPN 280): Stale data after node rejoin
# Write data while a node is down, restart it, verify it catches up
info "[FMEA #18] Stale data test: stop node3, write, restart, verify..."
docker compose stop node3 >/dev/null 2>&1
sleep 3
cql_ok 9042 "CONSISTENCY QUORUM; INSERT INTO cluster_test.data (k, v, source) VALUES ('while_n3_down', 'catch_me', 'node1')" \
    "[FMEA #18] QUORUM write while node3 is down"
docker compose start node3 >/dev/null 2>&1
wait_cql 9044 "node3" "$PAIR_CQL_TIMEOUT"
# CL=ONE coordinated by node3: only node3's own replica can answer if the
# coordinator picks itself, so this checks the rejoined node caught up.
wait_for_cql_value 9044 "CONSISTENCY ONE; SELECT v FROM cluster_test.data WHERE k = 'while_n3_down';" \
    "catch_me" "[FMEA #18] rejoined node3 read of a write it missed" 60
pass "[FMEA #18] Rejoined node has fresh data (catch-up worked)"

# FMEA #7 (RPN 105): Token distribution after transition
# Verify cluster status shows reasonable token distribution
info "[FMEA #7] Checking cluster status for token info..."
STATUS=$(cluster_status 9090)
info "Node1 cluster status: $STATUS"
grep -q '"mode":"cluster"' <<<"$STATUS" \
    || fail "[FMEA #7] node1 cluster status does not report cluster mode: $STATUS"
pass "[FMEA #7] Cluster status endpoint responding"

# FMEA #9 (RPN 175): Write timeout behavior
# Write to a table after stopping a node — should not hang forever
info "[FMEA #9] Write timeout test (1 node down, should complete quickly)..."
docker compose stop node3 >/dev/null 2>&1
sleep 3
START_TIME=$(date +%s)
# Success or a timeout error are both acceptable here; the assertion is that
# the write RETURNS promptly rather than hanging.
cqlsh --request-timeout=30 localhost 9042 \
    -e "INSERT INTO cluster_test.data (k, v, source) VALUES ('timeout_test', 'fast', 'node1')" \
    >/dev/null 2>&1 || true
END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))
if [ "$ELAPSED" -ge 15 ]; then
    fail "[FMEA #9] Write with 1 node down took ${ELAPSED}s (>= 15s): timeout handling regressed"
fi
pass "[FMEA #9] Write completed in ${ELAPSED}s (no indefinite hang)"

# Restart node3 for cleanup
docker compose start node3 >/dev/null 2>&1
wait_cql 9044 "node3" "$PAIR_CQL_TIMEOUT"

# ============================================================
# Phase 13: Cross-Node Subscription Test
# ============================================================
echo ""
info "=== Phase 13: Cross-Node Subscription Test ==="

# Create a table for subscription testing
cql_ok 9042 "CREATE TABLE IF NOT EXISTS smoke_test.events (id text PRIMARY KEY, data text)" \
    "CREATE TABLE smoke_test.events"
wait_for_cql_value 9043 "SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'smoke_test';" \
    "events" "smoke_test.events visible on node2"
pass "Created events table for subscription test"

# Insert data on node1
cql_ok 9042 "INSERT INTO smoke_test.events (id, data) VALUES ('e1', 'first_event')" "insert e1 via node1"
cql_ok 9042 "INSERT INTO smoke_test.events (id, data) VALUES ('e2', 'second_event')" "insert e2 via node1"
pass "Inserted events on node1"

# Read from node2 — verifies data is replicated. This runs in cluster mode
# (Phase 6 asserted it), so there is no single-node excuse for a miss.
wait_for_cql_value 9043 "SELECT data FROM smoke_test.events WHERE id = 'e1';" "first_event" \
    "node2 read of e1 written via node1"
pass "Node2 can read event e1 written by node1"

# Update on node2, read back on node1
cql_ok 9043 "UPDATE smoke_test.events SET data = 'updated_first' WHERE id = 'e1'" "update e1 via node2"
wait_for_cql_value 9042 "SELECT data FROM smoke_test.events WHERE id = 'e1';" "updated_first" \
    "node1 read of the update written via node2"
pass "Node1 sees update written by node2"

pass "Phase 13 complete: cross-node data flow verified"

info ""
info "Pair mode phases complete."
info "Services still running. Use 'docker compose down -v' to stop."
info "RustFS console: http://localhost:9001 (rustfsadmin/rustfsadmin)"
info "Node1 CQL: cqlsh localhost 9042 | Web: http://localhost:9090"
info "Node2 CQL: cqlsh localhost 9043 | Web: http://localhost:9091"
info "Node3 CQL: cqlsh localhost 9044 | Web: http://localhost:9092"

# Don't cleanup on success — leave services running for manual exploration
trap - EXIT

fi  # RUN_PAIR

# ============================================================
# 3-NODE CLUSTER SUITE (C1-C10)
# Uses tests/docker-compose.cluster.yml with --profile trio
# ============================================================
if $RUN_TRIO; then

echo ""
echo -e "${GREEN}============================================================${NC}"
echo -e "${GREEN}  3-Node Cluster Suite (C1-C10)${NC}"
echo -e "${GREEN}============================================================${NC}"

# Override cleanup for this section
trap cleanup_cluster EXIT

info "Building and starting 3-node cluster (profile: trio)..."
docker compose -f "$CLUSTER_COMPOSE" --profile trio up -d --build

wait_cql_c 9042 "cluster-node1" 90
wait_cql_c 9043 "cluster-node2" 90
wait_cql_c 9044 "cluster-node3" 90

# ------------------------------------------------------------------
# C1: Raft leader election
# Pass criteria: system.peers shows 2 peers (3 nodes total); cluster
#               mode reported by at least one node within 30s.
# ------------------------------------------------------------------
info ""
info "=== C1: Raft Leader Election ==="

info "Waiting for Raft leader election (up to 30s)..."
LEADER_FOUND=false
for i in $(seq 1 30); do
    # Count peers: system.peers returns the OTHER nodes, so 3-node cluster
    # shows 2 peers on each node.
    P1=$(peer_count cql_c1)
    if [ "$P1" -ge 2 ]; then
        LEADER_FOUND=true
        break
    fi
    sleep 1
done

if $LEADER_FOUND; then
    pass "[C1] system.peers shows >= 2 peers on node1 (3-node cluster formed)"
else
    P1=$(peer_count cql_c1)
    fail "[C1] Raft election timeout: system.peers shows only $P1 peer(s) after 30s"
fi

# Verify at least one node reports cluster mode
LEADER_NODE=""
for n in 1 2 3; do
    port=$((9089 + n))
    mode=$(curl -s "http://localhost:${port}/api/cluster/status" 2>/dev/null \
        | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('mode',''))" 2>/dev/null || true)
    if [ "$mode" = "cluster" ]; then
        LEADER_NODE=$n
        pass "[C1] Node${n} reports mode=cluster (leader elected)"
        break
    fi
done
if [ -z "$LEADER_NODE" ]; then
    fail "[C1] No node reports mode=cluster after the peers formed"
fi

# ------------------------------------------------------------------
# C2: DDL replication via Raft
# Pass criteria: keyspace created on node1 is visible on all 3 nodes.
# ------------------------------------------------------------------
info ""
info "=== C2: DDL Replication via Raft ==="

cql_ok 9042 "CREATE KEYSPACE IF NOT EXISTS c_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}" \
    "[C2] CREATE KEYSPACE c_test"
cql_ok 9042 "CREATE TABLE IF NOT EXISTS c_test.rows (k text PRIMARY KEY, v text, n int)" \
    "[C2] CREATE TABLE c_test.rows"

for port in 9042 9043 9044; do
    wait_for_cql_value "$port" "SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'c_test';" \
        "rows" "[C2] c_test.rows visible via port $port"
done
pass "[C2] DDL replicated to all 3 nodes"

# ------------------------------------------------------------------
# C3: QUORUM writes and reads (100 rows)
# Pass criteria: all 100 rows readable at QUORUM from each node.
# (This used `INSERT ... USING CONSISTENCY QUORUM`, which is not CQL; every
# insert failed over to an unchecked CL=ONE retry, so QUORUM was never tested.)
# ------------------------------------------------------------------
info ""
info "=== C3: QUORUM Writes and Reads (100 rows) ==="

info "Inserting 100 rows at QUORUM via node1..."
for i in $(seq 1 100); do
    cql_ok 9042 "CONSISTENCY QUORUM; INSERT INTO c_test.rows (k, v, n) VALUES ('r${i}', 'val${i}', ${i});" \
        "[C3] QUORUM write r${i}"
done

for port in 9042 9043 9044; do
    wait_for_row_count "$port" c_test.rows 100 "[C3] QUORUM read via port $port" 30 QUORUM
done
pass "[C3] All 100 rows readable at QUORUM from all 3 nodes"

# ------------------------------------------------------------------
# C4: Node failure tolerance — kill node3, write 50 more rows at QUORUM
# Pass criteria: 50 writes succeed with 2-of-3 nodes alive.
# ------------------------------------------------------------------
info ""
info "=== C4: Node Failure Tolerance (kill node3, write at QUORUM) ==="

info "Stopping cluster node3..."
docker compose -f "$CLUSTER_COMPOSE" stop node3
sleep 5

info "Writing 50 rows at QUORUM with node3 down (2 of 3 alive)..."
for i in $(seq 101 150); do
    cql_ok 9042 "CONSISTENCY QUORUM; INSERT INTO c_test.rows (k, v, n) VALUES ('r${i}', 'val${i}', ${i});" \
        "[C4] QUORUM write r${i} with node3 down"
done
pass "[C4] 50 QUORUM writes succeeded with node3 down"

# ------------------------------------------------------------------
# C5: Read QUORUM with node3 down — expect 150 rows (100 + 50)
# Pass criteria: all 150 rows visible from node1 and node2.
# ------------------------------------------------------------------
info ""
info "=== C5: Read QUORUM with Node3 Down (150 rows) ==="

for port in 9042 9043; do
    wait_for_row_count "$port" c_test.rows 150 "[C5] QUORUM read via port $port with node3 down" 30 QUORUM
done

# ------------------------------------------------------------------
# C6: CL=ALL fails with node3 down
# Pass criteria: INSERT at ALL returns an error (Unavailable). An
# acknowledged CL=ALL write with a replica down is a correctness failure.
# ------------------------------------------------------------------
info ""
info "=== C6: CL=ALL Fails with Node3 Down ==="

if cqlsh --request-timeout=15 localhost 9042 \
    -e "CONSISTENCY ALL; INSERT INTO c_test.rows (k, v, n) VALUES ('cl_all_test', 'should_fail', 999);" \
    >/dev/null 2>&1; then
    fail "[C6] INSERT at CL=ALL was acknowledged with node3 down"
fi
pass "[C6] INSERT at ALL correctly rejected with node3 down"

# ------------------------------------------------------------------
# C7: Restart node3; wait for reconnection (system.peers shows 3 again)
# Pass criteria: system.peers back to 2 peers within 30s.
# ------------------------------------------------------------------
info ""
info "=== C7: Reconnection After Restart ==="

info "Restarting cluster node3..."
docker compose -f "$CLUSTER_COMPOSE" start node3
wait_cql_c 9044 "cluster-node3" 60

info "Waiting for node3 to rejoin (up to 30s)..."
if wait_peers cql_c1 2 30; then
    pass "[C7] system.peers shows 2+ peers — node3 rejoined within 30s"
else
    P=$(peer_count cql_c1)
    fail "[C7] system.peers shows $P peers 30s after node3 restarted (expected >= 2)"
fi

# ------------------------------------------------------------------
# C8: Hint replay verification
# Pass criteria: all 150 rows readable from restarted node3 within 60s.
# ------------------------------------------------------------------
info ""
info "=== C8: Hint Replay Verification ==="

info "Waiting for hint replay on node3 (up to 60s)..."
wait_for_row_count 9044 c_test.rows 150 "[C8] node3 rows after hint replay" 60

# ------------------------------------------------------------------
# C9: Raft leader failover
# Pass criteria: after killing current leader, a new node reports
#               mode=cluster within 10s.
# ------------------------------------------------------------------
info ""
info "=== C9: Raft Leader Failover ==="

# Find which node is currently the leader
OLD_LEADER=""
for n in 1 2 3; do
    port=$((9089 + n))
    mode=$(curl -s "http://localhost:${port}/api/cluster/status" 2>/dev/null \
        | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('mode',''))" 2>/dev/null || true)
    if [ "$mode" = "cluster" ]; then
        OLD_LEADER=$n
        break
    fi
done

if [ -n "$OLD_LEADER" ]; then
    info "Killing Raft leader: node${OLD_LEADER}..."
    docker compose -f "$CLUSTER_COMPOSE" stop "node${OLD_LEADER}"
    sleep 2

    # Wait for new leader within 10s
    NEW_LEADER_FOUND=false
    for i in $(seq 1 10); do
        for n in 1 2 3; do
            [ "$n" = "$OLD_LEADER" ] && continue
            port=$((9089 + n))
            mode=$(curl -s "http://localhost:${port}/api/cluster/status" 2>/dev/null \
                | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('mode',''))" 2>/dev/null || true)
            if [ "$mode" = "cluster" ]; then
                pass "[C9] New Raft leader elected: node${n} within ${i}s (old leader was node${OLD_LEADER})"
                NEW_LEADER_FOUND=true
                break 2
            fi
        done
        sleep 1
    done

    $NEW_LEADER_FOUND || fail "[C9] No surviving node reported mode=cluster within 10s of killing node${OLD_LEADER}"

    # Restart old leader for C10
    docker compose -f "$CLUSTER_COMPOSE" start "node${OLD_LEADER}" >/dev/null 2>&1 || true
    wait_cql_c $((9041 + OLD_LEADER)) "cluster-node${OLD_LEADER}" 60
    sleep 5
else
    fail "[C9] No node reports mode=cluster — cannot run the failover test"
fi

# ------------------------------------------------------------------
# C10: DDL on new leader replicates to all nodes
# Pass criteria: table created after leader change is visible everywhere.
# ------------------------------------------------------------------
info ""
info "=== C10: DDL on New Leader Replicates to All Nodes ==="

# Pick a surviving node to issue DDL
DDL_NODE=1
[ "$OLD_LEADER" = "1" ] && DDL_NODE=2

info "Creating table via node${DDL_NODE} (post-failover leader)..."
cql_ok $((9041 + DDL_NODE)) "CREATE TABLE IF NOT EXISTS c_test.post_failover (id text PRIMARY KEY, val text);" \
    "[C10] CREATE TABLE via node${DDL_NODE} after failover"

for port in 9042 9043 9044; do
    wait_for_cql_value "$port" "SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'c_test' AND table_name = 'post_failover';" \
        "post_failover" "[C10] post_failover visible via port $port"
done
pass "[C10] DDL replicated to all nodes after leader failover"

echo ""
info "3-node cluster suite (C1-C10) complete."
info "Cluster stack still running. Use 'docker compose -f tests/docker-compose.cluster.yml down -v' to stop."
info "Node1 CQL: cqlsh localhost 9042 | Web: http://localhost:9090"
info "Node2 CQL: cqlsh localhost 9043 | Web: http://localhost:9091"
info "Node3 CQL: cqlsh localhost 9044 | Web: http://localhost:9092"

trap - EXIT

fi  # RUN_TRIO

# ============================================================
# 5-NODE CLUSTER SUITE (F1-F6)
# Uses tests/docker-compose.cluster.yml with --profile quint
# ============================================================
if $RUN_QUINT; then

echo ""
echo -e "${GREEN}============================================================${NC}"
echo -e "${GREEN}  5-Node Cluster Suite (F1-F6)${NC}"
echo -e "${GREEN}============================================================${NC}"

trap cleanup_cluster EXIT

info "Building and starting 5-node cluster (profile: quint)..."
docker compose -f "$CLUSTER_COMPOSE" --profile quint up -d --build

wait_cql_c 9042 "cluster-node1" 120
wait_cql_c 9043 "cluster-node2" 120
wait_cql_c 9044 "cluster-node3" 120
wait_cql_c 9045 "cluster-node4" 120
wait_cql_c 9046 "cluster-node5" 120

# ------------------------------------------------------------------
# F1: 5-node Raft group forms, leader elected within 15s
# Pass criteria: system.peers shows 4 peers on node1; mode=cluster
#               reported within 15s.
# ------------------------------------------------------------------
info ""
info "=== F1: 5-Node Raft Group Formation ==="

info "Waiting for 5-node Raft election (up to 15s)..."
F1_PASS=false
for i in $(seq 1 15); do
    P=$(peer_count cql_c1)
    if [ "$P" -ge 4 ]; then
        F1_PASS=true
        pass "[F1] system.peers shows $P peers (5-node cluster formed within ${i}s)"
        break
    fi
    sleep 1
done
$F1_PASS || { P=$(peer_count cql_c1); fail "[F1] system.peers shows $P peers after 15s (expected >= 4)"; }

# Create keyspace for the quint suite
cql_ok 9042 "CREATE KEYSPACE IF NOT EXISTS f_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}" \
    "[F] CREATE KEYSPACE f_test"
cql_ok 9042 "CREATE TABLE IF NOT EXISTS f_test.rows (k text PRIMARY KEY, v text, n int)" \
    "[F] CREATE TABLE f_test.rows"
for port in 9042 9043 9044 9045 9046; do
    wait_for_cql_value "$port" "SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'f_test';" \
        "rows" "[F] f_test.rows visible via port $port"
done

# ------------------------------------------------------------------
# F2: QUORUM writes/reads across all 5 nodes (200 rows)
# Pass criteria: 200 rows at QUORUM; each node returns all 200.
# ------------------------------------------------------------------
info ""
info "=== F2: QUORUM Writes/Reads Across All 5 Nodes (200 rows) ==="

info "Inserting 200 rows via round-robin across nodes..."
for i in $(seq 1 200); do
    node_num=$(( (i % 5) + 1 ))
    cql_ok $((9041 + node_num)) "CONSISTENCY QUORUM; INSERT INTO f_test.rows (k, v, n) VALUES ('f${i}', 'val${i}', ${i});" \
        "[F2] QUORUM write f${i} via node${node_num}"
done

for port in 9042 9043 9044 9045 9046; do
    wait_for_row_count "$port" f_test.rows 200 "[F2] QUORUM read via port $port" 30 QUORUM
done
pass "[F2] All 200 rows readable from all 5 nodes"

# ------------------------------------------------------------------
# F3: Kill 2 nodes; QUORUM writes still succeed (3 of 5 alive, RF=3)
# Pass criteria: 100 writes at QUORUM succeed with 3-of-5 alive.
# ------------------------------------------------------------------
info ""
info "=== F3: Kill 2 Nodes; QUORUM Writes Still Succeed (3 of 5) ==="

info "Stopping node4 and node5..."
docker compose -f "$CLUSTER_COMPOSE" stop node4 node5
sleep 5

# Record which nodes are stopped so we know to use others
F4_KILLED_NODES="4 5"

# With RF=3 over 5 nodes, a key whose 3 replicas include both stopped nodes
# has only 1 live replica and a QUORUM write to it MUST fail. Which keys those
# are depends on the token ring, so this phase writes at QUORUM and counts:
# every write must either succeed or fail with Unavailable, and at least one
# must succeed. A write that hangs or errors otherwise fails the run.
info "Writing 100 rows at QUORUM with 2 nodes down (3 of 5 alive)..."
F3_OK=0
F3_UNAVAILABLE=0
for i in $(seq 201 300); do
    if out=$(cqlsh --request-timeout=10 localhost 9042 \
        -e "CONSISTENCY QUORUM; INSERT INTO f_test.rows (k, v, n) VALUES ('f${i}', 'val${i}', ${i});" 2>&1); then
        F3_OK=$((F3_OK + 1))
    elif grep -qi "unavailable" <<<"$out"; then
        F3_UNAVAILABLE=$((F3_UNAVAILABLE + 1))
    else
        fail "[F3] QUORUM write f${i} failed other than Unavailable: $out"
    fi
done
[ "$F3_OK" -gt 0 ] || fail "[F3] no QUORUM write succeeded with 3 of 5 nodes alive"
pass "[F3] $F3_OK QUORUM writes succeeded, $F3_UNAVAILABLE correctly Unavailable, with 2 nodes down"

# ------------------------------------------------------------------
# F4: Kill Raft leader (among the 2 surviving nodes); new leader elected
# Pass criteria: new leader elected within 10s from 3 surviving nodes.
# ------------------------------------------------------------------
info ""
info "=== F4: Kill Raft Leader; New Leader Elected Within 10s ==="

# Find leader among surviving nodes (1, 2, 3)
F4_OLD_LEADER=""
for n in 1 2 3; do
    port=$((9089 + n))
    mode=$(curl -s "http://localhost:${port}/api/cluster/status" 2>/dev/null \
        | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('mode',''))" 2>/dev/null || true)
    if [ "$mode" = "cluster" ]; then
        F4_OLD_LEADER=$n
        break
    fi
done

if [ -n "$F4_OLD_LEADER" ]; then
    info "Killing Raft leader: node${F4_OLD_LEADER} (3 remaining nodes will elect new leader)..."
    docker compose -f "$CLUSTER_COMPOSE" stop "node${F4_OLD_LEADER}"
    F4_KILLED_NODES="$F4_KILLED_NODES $F4_OLD_LEADER"
    sleep 2

    F4_PASS=false
    for i in $(seq 1 10); do
        for n in 1 2 3; do
            [ "$n" = "$F4_OLD_LEADER" ] && continue
            port=$((9089 + n))
            mode=$(curl -s "http://localhost:${port}/api/cluster/status" 2>/dev/null \
                | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('mode',''))" 2>/dev/null || true)
            if [ "$mode" = "cluster" ]; then
                pass "[F4] New Raft leader elected: node${n} within ${i}s"
                F4_PASS=true
                break 2
            fi
        done
        sleep 1
    done
    $F4_PASS || fail "[F4] No surviving node reported mode=cluster within 10s of killing node${F4_OLD_LEADER}"
else
    fail "[F4] No node among 1-3 reports mode=cluster — cannot run the leader kill"
fi

# ------------------------------------------------------------------
# F5: Restart both killed nodes; hints replay (300 rows everywhere)
# Pass criteria: all 300 rows readable from every node within 120s.
# ------------------------------------------------------------------
info ""
info "=== F5: Restart Killed Nodes; Hints Replay (300 rows, all 5 nodes) ==="

info "Restarting node4, node5, and killed leader (if applicable)..."
docker compose -f "$CLUSTER_COMPOSE" start node4 node5 2>/dev/null || true
[ -n "$F4_OLD_LEADER" ] && docker compose -f "$CLUSTER_COMPOSE" start "node${F4_OLD_LEADER}" 2>/dev/null || true

wait_cql_c 9045 "cluster-node4" 90
wait_cql_c 9046 "cluster-node5" 90

# Every acknowledged row: the 200 from F2 plus F3's successful writes.
F_EXPECTED=$((200 + F3_OK))
info "Waiting for hints to replay on all nodes (up to 120s, expecting $F_EXPECTED rows)..."
for port in 9042 9043 9044 9045 9046; do
    wait_for_row_count "$port" f_test.rows "$F_EXPECTED" "[F5] rows via port $port after hint replay" 120
done
pass "[F5] All $F_EXPECTED acknowledged rows readable on all 5 nodes (hints replayed)"

# ------------------------------------------------------------------
# F6: SELECT at ALL returns consistent data across all 5 nodes
# Pass criteria: every acknowledged row returned at CL=ALL from every node.
# ------------------------------------------------------------------
info ""
info "=== F6: SELECT at ALL — Consistent Data Across All 5 Nodes ==="

for port in 9042 9043 9044 9045 9046; do
    wait_for_row_count "$port" f_test.rows "$F_EXPECTED" "[F6] CL=ALL read via port $port" 30 ALL
done
pass "[F6] Consistent data across all 5 nodes"

echo ""
info "5-node cluster suite (F1-F6) complete."
info "Cluster stack still running. Use 'docker compose -f tests/docker-compose.cluster.yml down -v' to stop."
info "Ports: node1=9042 node2=9043 node3=9044 node4=9045 node5=9046"

trap - EXIT

fi  # RUN_QUINT

# ============================================================
# NODE LIFECYCLE SUITE (L1-L7)
# Starts a 3-node cluster then adds/removes nodes via ferrosa-ctl
# ============================================================
if $RUN_LIFECYCLE; then

echo ""
echo -e "${GREEN}============================================================${NC}"
echo -e "${GREEN}  Node Lifecycle Suite (L1-L7)${NC}"
echo -e "${GREEN}============================================================${NC}"

trap cleanup_cluster EXIT

# Start with a 3-node cluster
info "Building and starting 3-node cluster (lifecycle baseline)..."
docker compose -f "$CLUSTER_COMPOSE" --profile trio up -d --build

wait_cql_c 9042 "cluster-node1" 90
wait_cql_c 9043 "cluster-node2" 90
wait_cql_c 9044 "cluster-node3" 90

# Baseline data for bootstrap verification
cql_ok 9042 "CREATE KEYSPACE IF NOT EXISTS l_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}" \
    "[L*] CREATE KEYSPACE l_test"
cql_ok 9042 "CREATE TABLE IF NOT EXISTS l_test.items (k text PRIMARY KEY, v text)" "[L*] CREATE TABLE l_test.items"
for i in $(seq 1 50); do
    cql_ok 9042 "CONSISTENCY QUORUM; INSERT INTO l_test.items (k, v) VALUES ('item${i}', 'val${i}');" \
        "[L*] baseline QUORUM write item${i}"
done
pass "[L*] Baseline: 3-node cluster running with 50 rows in l_test.items"

# ------------------------------------------------------------------
# L1: Add 4th node via API; it appears in system.peers within 60s
# Pass criteria: system.peers on all 3 nodes shows 3 peers within 60s.
# ------------------------------------------------------------------
info ""
info "=== L1: Add 4th Node via add-node API ==="

L4_HOST_ID="44444444-4444-4444-4444-444444444444"

info "Pre-approving node4 (host_id=$L4_HOST_ID) via node1 API..."
APPROVE_RESULT=$(curl -s -X POST "http://localhost:9090/api/cluster/add-node" \
    -H "Content-Type: application/json" \
    -d "{\"host_id\": \"${L4_HOST_ID}\"}" 2>/dev/null || true)
info "Approve result: $APPROVE_RESULT"

if ! echo "$APPROVE_RESULT" | grep -q '"approved"'; then
    fail "[L1] add-node did not approve node4: $APPROVE_RESULT"
fi
pass "[L1] Node4 pre-approved via add-node API"

info "Starting node4 (quint profile starts node4)..."
docker compose -f "$CLUSTER_COMPOSE" --profile quint up -d node4
wait_cql_c 9045 "cluster-node4" 90

info "Waiting for node4 to appear in system.peers (up to 60s)..."
L1_PASS=false
for i in $(seq 1 60); do
    P=$(peer_count cql_c1)
    if [ "$P" -ge 3 ]; then
        L1_PASS=true
        pass "[L1] system.peers shows $P peers (node4 joined within ${i}s)"
        break
    fi
    sleep 1
done
$L1_PASS || { P=$(peer_count cql_c1); fail "[L1] system.peers shows $P peers 60s after node4 started (expected >= 3)"; }

# ------------------------------------------------------------------
# L2: 4th node bootstraps (S3 + delta stream) — has all existing data
# Pass criteria: SELECT at ONE from node4 returns all 50 pre-join rows.
# ------------------------------------------------------------------
info ""
info "=== L2: 4th Node Bootstrap Verification ==="

info "Waiting for node4 to bootstrap existing data (up to 60s)..."
wait_for_row_count 9045 l_test.items 50 "[L2] node4 rows after bootstrap" 60

# ------------------------------------------------------------------
# L3: Write at QUORUM; 4th node receives new writes (readable at ONE)
# Pass criteria: rows written after node4 joined are readable at ONE
#               from node4.
# ------------------------------------------------------------------
info ""
info "=== L3: Write at QUORUM; 4th Node Receives New Writes ==="

info "Inserting 20 post-join rows at QUORUM..."
for i in $(seq 51 70); do
    cql_ok 9042 "CONSISTENCY QUORUM; INSERT INTO l_test.items (k, v) VALUES ('item${i}', 'val${i}');" \
        "[L3] post-join QUORUM write item${i}"
done
wait_for_row_count 9045 l_test.items 70 "[L3] node4 rows after post-join writes" 30

# ------------------------------------------------------------------
# L4: Decommission 4th node
# Pass criteria: node4 removed from system.peers within 120s.
# ------------------------------------------------------------------
info ""
info "=== L4: Decommission 4th Node ==="

info "Issuing decommission for node4 (host_id=$L4_HOST_ID) via node1 API..."
DECOMM_RESULT=$(curl -s -X POST "http://localhost:9090/api/cluster/decommission" \
    -H "Content-Type: application/json" \
    -d "{\"host_id\": \"${L4_HOST_ID}\"}" 2>/dev/null || true)
info "Decommission result: $DECOMM_RESULT"

if ! echo "$DECOMM_RESULT" | grep -qE '"decommissioning"|"decommissioned"'; then
    fail "[L4] decommission of node4 not initiated: $DECOMM_RESULT"
fi
pass "[L4] Decommission initiated for node4"

info "Waiting for node4 to disappear from system.peers (up to 120s)..."
L4_PASS=false
for i in $(seq 1 120); do
    P=$(peer_count cql_c1)
    if [ "$P" -le 2 ]; then
        L4_PASS=true
        pass "[L4] system.peers shows $P peers — node4 decommissioned within ${i}s"
        break
    fi
    sleep 1
done
$L4_PASS || { P=$(peer_count cql_c1); fail "[L4] system.peers still shows $P peers 120s after decommission (expected <= 2)"; }

# Stop node4 container
docker compose -f "$CLUSTER_COMPOSE" stop node4 2>/dev/null || true

# ------------------------------------------------------------------
# L5: 3 remaining nodes have all data (SELECT at ALL)
# Pass criteria: all 70 rows returned at ALL from each of the 3 nodes.
# ------------------------------------------------------------------
info ""
info "=== L5: 3 Remaining Nodes Have All Data ==="

for port in 9042 9043 9044; do
    wait_for_row_count "$port" l_test.items 70 "[L5] CL=ALL read via port $port after decommission" 30 ALL
done
pass "[L5] All data preserved across 3 nodes after decommission"

# ------------------------------------------------------------------
# L6: 5-node cluster: add 4th and 5th via lifecycle
# Pass criteria: both nodes join, all 5 participate in QUORUM.
# ------------------------------------------------------------------
info ""
info "=== L6: Add 4th and 5th Nodes via Lifecycle ==="

L5_HOST_ID="55555555-5555-5555-5555-555555555555"

info "Pre-approving node4 and node5..."
curl -s -X POST "http://localhost:9090/api/cluster/add-node" \
    -H "Content-Type: application/json" \
    -d "{\"host_id\": \"${L4_HOST_ID}\"}" >/dev/null 2>&1 || true
curl -s -X POST "http://localhost:9090/api/cluster/add-node" \
    -H "Content-Type: application/json" \
    -d "{\"host_id\": \"${L5_HOST_ID}\"}" >/dev/null 2>&1 || true

info "Starting node4 and node5..."
docker compose -f "$CLUSTER_COMPOSE" --profile quint up -d node4 node5
wait_cql_c 9045 "cluster-node4" 90
wait_cql_c 9046 "cluster-node5" 90

info "Waiting for both nodes to appear in system.peers (up to 60s)..."
L6_PASS=false
for i in $(seq 1 60); do
    P=$(peer_count cql_c1)
    if [ "$P" -ge 4 ]; then
        L6_PASS=true
        pass "[L6] system.peers shows $P peers — both nodes joined within ${i}s"
        break
    fi
    sleep 1
done
$L6_PASS || { P=$(peer_count cql_c1); fail "[L6] system.peers shows $P peers after 60s (expected >= 4)"; }

# ------------------------------------------------------------------
# L7: Rebalance after adding nodes
# Pass criteria: rebalance completes; token skew < 5% (best-effort
#               check via ring API); cluster available during rebalance.
# ------------------------------------------------------------------
info ""
info "=== L7: Rebalance After Adding 4th and 5th Nodes ==="

info "Triggering token rebalance via node1 API..."
REBALANCE_RESULT=$(curl -s -X POST "http://localhost:9090/api/cluster/rebalance" 2>/dev/null || true)
info "Rebalance result: $REBALANCE_RESULT"

if ! echo "$REBALANCE_RESULT" | grep -q '"rebalance complete"'; then
    fail "[L7] rebalance did not complete: $REBALANCE_RESULT"
fi
pass "[L7] Rebalance completed successfully"

# Verify cluster is still available during/after rebalance
for i in $(seq 1 10); do
    cql_ok 9042 "CONSISTENCY QUORUM; INSERT INTO l_test.items (k, v) VALUES ('rebal${i}', 'during_rebalance');" \
        "[L7] QUORUM write rebal${i} after rebalance"
done
pass "[L7] Cluster accepts writes during rebalance"

# Check ring token distribution via API
RING=$(curl -s "http://localhost:9090/api/cluster/ring" 2>/dev/null || true)
if echo "$RING" | python3 -c "
import sys, json
d = json.load(sys.stdin)
nodes = d.get('nodes', [])
if len(nodes) < 2:
    sys.exit(1)
counts = [n['token_count'] for n in nodes]
avg = sum(counts) / len(counts)
skew = max(abs(c - avg) / avg for c in counts) if avg > 0 else 1
print(f'nodes={len(nodes)} avg_tokens={avg:.1f} skew={skew:.2%}')
sys.exit(0 if skew < 0.05 else 1)
" 2>/dev/null; then
    pass "[L7] Token distribution skew < 5% after rebalance"
else
    TOKEN_INFO=$(echo "$RING" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    nodes = d.get('nodes', [])
    for n in nodes:
        print(f\"  node_id={n['node_id']} tokens={n['token_count']}\")
except Exception as e:
    print(f'ring parse error: {e}')
" 2>/dev/null || echo "  (ring API unavailable)")
    info "$TOKEN_INFO"
    fail "[L7] Token skew >= 5% after rebalance, or the ring API is unavailable"
fi

echo ""
info "Node lifecycle suite (L1-L7) complete."
info "Cluster stack still running. Use 'docker compose -f tests/docker-compose.cluster.yml down -v' to stop."

trap - EXIT

fi  # RUN_LIFECYCLE

# ============================================================
# FMEA SCENARIOS (6 scenarios from spec section 5e)
# Uses tests/docker-compose.cluster.yml with --profile trio
# ============================================================
if $RUN_FMEA; then

echo ""
echo -e "${GREEN}============================================================${NC}"
echo -e "${GREEN}  FMEA Scenarios${NC}"
echo -e "${GREEN}============================================================${NC}"

trap cleanup_cluster EXIT

info "Building and starting 3-node cluster for FMEA..."
docker compose -f "$CLUSTER_COMPOSE" --profile trio up -d --build

wait_cql_c 9042 "cluster-node1" 90
wait_cql_c 9043 "cluster-node2" 90
wait_cql_c 9044 "cluster-node3" 90

# Baseline schema
cql_ok 9042 "CREATE KEYSPACE IF NOT EXISTS fmea WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}" \
    "[FMEA] CREATE KEYSPACE fmea"
cql_ok 9042 "CREATE TABLE IF NOT EXISTS fmea.kv (k text PRIMARY KEY, v text)" "[FMEA] CREATE TABLE fmea.kv"
for i in $(seq 1 30); do
    cql_ok 9042 "CONSISTENCY QUORUM; INSERT INTO fmea.kv (k, v) VALUES ('base${i}', 'val${i}');" \
        "[FMEA] baseline QUORUM write base${i}"
done
pass "[FMEA] Baseline: 30 rows in fmea.kv"

# ------------------------------------------------------------------
# FMEA-1: Network partition — isolate node3 from node1/node2
# Action: iptables block node3 ports; verify QUORUM continues on
#         majority side; heal; verify node3 catches up within 60s.
# Note: Requires NET_ADMIN capability in Docker. If not available,
#       we simulate with a container stop/start instead.
# ------------------------------------------------------------------
info ""
info "=== FMEA-1: Network Partition (isolate 1 of 3) ==="

# Attempt iptables isolation first; fall back to container stop
FMEA1_MODE="stop"
if docker compose -f "$CLUSTER_COMPOSE" exec -T node3 sh -c "which iptables >/dev/null 2>&1"; then
    FMEA1_MODE="iptables"
fi

if [ "$FMEA1_MODE" = "iptables" ]; then
    info "[FMEA-1] Isolating node3 via iptables..."
    NODE1_IP=$(docker compose -f "$CLUSTER_COMPOSE" exec -T node1 hostname -i 2>/dev/null | tr -d '[:space:]' || true)
    NODE2_IP=$(docker compose -f "$CLUSTER_COMPOSE" exec -T node2 hostname -i 2>/dev/null | tr -d '[:space:]' || true)
    docker compose -f "$CLUSTER_COMPOSE" exec -T node3 sh -c "
        iptables -A INPUT -s ${NODE1_IP} -j DROP 2>/dev/null || true
        iptables -A INPUT -s ${NODE2_IP} -j DROP 2>/dev/null || true
        iptables -A OUTPUT -d ${NODE1_IP} -j DROP 2>/dev/null || true
        iptables -A OUTPUT -d ${NODE2_IP} -j DROP 2>/dev/null || true
    " 2>/dev/null || FMEA1_MODE="stop"
fi

if [ "$FMEA1_MODE" = "stop" ]; then
    # Informational by design: this names which partition method the run used;
    # both methods are then held to the same assertions below.
    info "[FMEA-1] iptables not available — simulating partition by stopping node3"
    docker compose -f "$CLUSTER_COMPOSE" stop node3
fi

sleep 5

# Majority side (node1, node2) should continue
info "[FMEA-1] Writing on majority side (node1/node2)..."
for i in $(seq 31 50); do
    cql_ok 9042 "CONSISTENCY QUORUM; INSERT INTO fmea.kv (k, v) VALUES ('part${i}', 'majority');" \
        "[FMEA-1] QUORUM write part${i} on the majority side"
done
pass "[FMEA-1] QUORUM continues on majority side (node1/node2)"

# Heal the partition
if [ "$FMEA1_MODE" = "iptables" ]; then
    info "[FMEA-1] Healing partition (flushing iptables on node3)..."
    docker compose -f "$CLUSTER_COMPOSE" exec -T node3 sh -c "iptables -F 2>/dev/null || true" 2>/dev/null || true
else
    info "[FMEA-1] Healing partition (restarting node3)..."
    docker compose -f "$CLUSTER_COMPOSE" start node3
    wait_cql_c 9044 "cluster-node3" 60
fi

# Verify node3 catches up within 70s of the heal
wait_for_row_count 9044 fmea.kv 50 "[FMEA-1] isolated node3 rows after heal" 70

# ------------------------------------------------------------------
# FMEA-2: Coordinator crash mid-write
# Action: kill node1 (coordinator) immediately after issuing an
#         INSERT; verify no partial writes are visible at QUORUM
#         from surviving nodes.
# Pass criteria: client gets error; surviving nodes agree on row
#               either absent or fully written.
# ------------------------------------------------------------------
info ""
info "=== FMEA-2: Coordinator Crash Mid-Write ==="

info "[FMEA-2] Writing a row via node1 and killing node1 immediately..."
# Unchecked on purpose: the coordinator is killed mid-write, so either outcome
# is legal for the client; the assertion is node2/node3 agreement below.
cql_c1 "INSERT INTO fmea.kv (k, v) VALUES ('crash_test', 'coordinator_write');" 2>/dev/null &
WRITE_PID=$!
sleep 0
docker compose -f "$CLUSTER_COMPOSE" stop node1 2>/dev/null || true
wait "$WRITE_PID" 2>/dev/null || true

sleep 3

# Check: row should either be absent or fully present on node2 and node3
R2=$(cql_c2 "SELECT v FROM fmea.kv WHERE k = 'crash_test';" 2>/dev/null | grep -c "coordinator_write" || echo 0)
R3=$(cql_c3 "SELECT v FROM fmea.kv WHERE k = 'crash_test';" 2>/dev/null | grep -c "coordinator_write" || echo 0)

if [ "$R2" -ne "$R3" ]; then
    fail "[FMEA-2] node2 and node3 disagree on the crash_test row after a coordinator crash: node2=$R2, node3=$R3"
fi
pass "[FMEA-2] node2 and node3 agree on crash_test row (consistent: $R2 copies each)"

# Restart node1
docker compose -f "$CLUSTER_COMPOSE" start node1 >/dev/null 2>&1 || true
wait_cql_c 9042 "cluster-node1" 60
sleep 5

# ------------------------------------------------------------------
# FMEA-3: Raft leader disk full — leader steps down; new election
# Action: fill the sled data dir on the leader node; verify it
#         steps down and a new election succeeds within 10s.
# Note: Docker container disk quotas are needed for true simulation;
#       we approximate by filling the data dir to trigger an error.
# ------------------------------------------------------------------
info ""
info "=== FMEA-3: Raft Leader Disk Full ==="

# Find current leader
FMEA3_LEADER=""
for n in 1 2 3; do
    port=$((9089 + n))
    mode=$(curl -s "http://localhost:${port}/api/cluster/status" 2>/dev/null \
        | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('mode',''))" 2>/dev/null || true)
    if [ "$mode" = "cluster" ]; then
        FMEA3_LEADER=$n
        break
    fi
done

if [ -n "$FMEA3_LEADER" ]; then
    info "[FMEA-3] Current leader: node${FMEA3_LEADER} — filling data dir to simulate disk full..."
    # Fill the ferrosa data directory with random data to exhaust space
    docker compose -f "$CLUSTER_COMPOSE" exec -T "node${FMEA3_LEADER}" sh -c \
        "dd if=/dev/urandom of=/var/lib/ferrosa/disk_fill_test bs=1M count=4096 2>/dev/null || true
         sync || true" 2>/dev/null || true
    sleep 5

    # Check for new leader (existing leader should step down or fail writes)
    FMEA3_NEW_LEADER=false
    for i in $(seq 1 10); do
        for n in 1 2 3; do
            [ "$n" = "$FMEA3_LEADER" ] && continue
            port=$((9089 + n))
            mode=$(curl -s "http://localhost:${port}/api/cluster/status" 2>/dev/null \
                | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('mode',''))" 2>/dev/null || true)
            if [ "$mode" = "cluster" ]; then
                pass "[FMEA-3] New Raft leader: node${n} within ${i}s of disk fill"
                FMEA3_NEW_LEADER=true
                break 2
            fi
        done
        sleep 1
    done
    # Clean up the fill file before any fail, so a failed run does not leave
    # a 4 GiB file in the volume.
    docker compose -f "$CLUSTER_COMPOSE" exec -T "node${FMEA3_LEADER}" rm -f /var/lib/ferrosa/disk_fill_test 2>/dev/null || true
    $FMEA3_NEW_LEADER || fail "[FMEA-3] No other node reported mode=cluster within 10s of filling node${FMEA3_LEADER}'s disk"
else
    fail "[FMEA-3] No node reports mode=cluster — cannot run the disk-full scenario"
fi

# ------------------------------------------------------------------
# FMEA-4: Hint directory full
# Action: fill the hints directory past 1GB cap for a peer;
#         verify oldest hints are evicted and needs_repair appears
#         in system.peers or logs; cluster continues writing.
# ------------------------------------------------------------------
info ""
info "=== FMEA-4: Hint Directory Full (> 1GB cap) ==="

info "[FMEA-4] Filling hints directory on node1 to simulate overflow..."
docker compose -f "$CLUSTER_COMPOSE" exec -T node1 sh -c \
    "mkdir -p /var/lib/ferrosa/hints && dd if=/dev/urandom of=/var/lib/ferrosa/hints/overflow_test bs=1M count=1024 2>/dev/null || true; sync || true" \
    2>/dev/null || true
sleep 3

# Verify cluster continues accepting writes (hint overflow should not block cluster)
for i in $(seq 1 10); do
    cql_ok 9042 "CONSISTENCY QUORUM; INSERT INTO fmea.kv (k, v) VALUES ('hint_overflow${i}', 'val');" \
        "[FMEA-4] QUORUM write hint_overflow${i} with the hint directory full"
done
pass "[FMEA-4] Cluster continues writing with hint directory full"

# Clean up before asserting, so a failure does not leave 1 GiB behind.
docker compose -f "$CLUSTER_COMPOSE" exec -T node1 rm -f /var/lib/ferrosa/hints/overflow_test 2>/dev/null || true

# The scenario's claim is that an overflow is surfaced as needs_repair. It
# used to grep `SELECT peer` output for the word "true" and report INFO when
# absent, so the claim was never checked. Query the column itself.
PEERS=$(cqlsh --request-timeout=10 localhost 9043 -e "SELECT peer, needs_repair FROM system.peers;" 2>&1) \
    || fail "[FMEA-4] system.peers has no readable needs_repair column: $PEERS"
grep -q "True" <<<"$PEERS" \
    || fail "[FMEA-4] hint overflow did not surface as needs_repair=true in system.peers: $PEERS"
pass "[FMEA-4] needs_repair=true detected in system.peers"

# ------------------------------------------------------------------
# FMEA-5: S3 unavailable during bootstrap
# Action: stop rustfs; attempt to add a new node; verify join fails
#         gracefully; restart rustfs; verify retry succeeds.
# ------------------------------------------------------------------
info ""
info "=== FMEA-5: S3 Unavailable During Bootstrap ==="

info "[FMEA-5] Stopping rustfs to simulate S3 outage..."
docker compose -f "$CLUSTER_COMPOSE" stop rustfs 2>/dev/null || true
sleep 3

# Try to start node4 — it should fail gracefully (not crash the cluster)
info "[FMEA-5] Starting node4 with S3 unavailable (expect graceful failure)..."
docker compose -f "$CLUSTER_COMPOSE" --profile quint up -d node4 2>/dev/null || true
sleep 10

# Existing cluster should still be operational
cql_ok 9042 "CONSISTENCY QUORUM; INSERT INTO fmea.kv (k, v) VALUES ('s3_down_test', 'cluster_ok');" \
    "[FMEA-5] QUORUM write to the existing cluster during the S3 outage"
pass "[FMEA-5] Existing cluster unaffected by S3 outage during bootstrap"

# Restore rustfs
info "[FMEA-5] Restoring rustfs..."
docker compose -f "$CLUSTER_COMPOSE" start rustfs 2>/dev/null || true
sleep 10

# Verify node4 can retry bootstrap (restart it)
docker compose -f "$CLUSTER_COMPOSE" restart node4 2>/dev/null || true
# Give it time to try bootstrapping from S3
sleep 20

NODE4_UP=false
for i in $(seq 1 30); do
    if cqlsh localhost 9045 -e "SELECT cluster_name FROM system.local" >/dev/null 2>&1; then
        NODE4_UP=true
        pass "[FMEA-5] node4 bootstrap succeeded after rustfs restored (within $((20 + i))s)"
        break
    fi
    sleep 1
done
$NODE4_UP || fail "[FMEA-5] node4 did not become ready within 50s of rustfs being restored"

# Stop node4
docker compose -f "$CLUSTER_COMPOSE" stop node4 2>/dev/null || true

# ------------------------------------------------------------------
# FMEA-6: Rapid leader churn — kill and restart leader 3 times in 30s
# Pass criteria: cluster recovers; all committed data readable at QUORUM
#               after stabilization.
# ------------------------------------------------------------------
info ""
info "=== FMEA-6: Rapid Leader Churn (3 kills in 30s) ==="

# Write a sentinel row before churn
cql_ok 9042 "CONSISTENCY QUORUM; INSERT INTO fmea.kv (k, v) VALUES ('pre_churn', 'before');" \
    "[FMEA-6] QUORUM sentinel write before churn"

info "[FMEA-6] Performing 3 rapid leader kills..."
for churn in 1 2 3; do
    # Find current leader
    CHURN_LEADER=""
    for n in 1 2 3; do
        port=$((9089 + n))
        mode=$(curl -s "http://localhost:${port}/api/cluster/status" 2>/dev/null \
            | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('mode',''))" 2>/dev/null || true)
        if [ "$mode" = "cluster" ]; then
            CHURN_LEADER=$n
            break
        fi
    done

    if [ -n "$CHURN_LEADER" ]; then
        info "[FMEA-6] Kill ${churn}/3: killing leader node${CHURN_LEADER}"
        docker compose -f "$CLUSTER_COMPOSE" stop "node${CHURN_LEADER}"
        sleep 3
        docker compose -f "$CLUSTER_COMPOSE" start "node${CHURN_LEADER}" >/dev/null 2>&1 || true
        sleep 2
    else
        fail "[FMEA-6] No node reports mode=cluster before kill ${churn}/3"
    fi
done

# Wait for stabilization (up to 30s)
info "[FMEA-6] Waiting for cluster stabilization after rapid churn (up to 30s)..."
FMEA6_STABLE=false
for i in $(seq 1 30); do
    ALL_UP=true
    for fn in cql_c1 cql_c2 cql_c3; do
        $fn "SELECT COUNT(*) FROM fmea.kv;" >/dev/null 2>&1 || ALL_UP=false
    done
    if $ALL_UP; then
        FMEA6_STABLE=true
        pass "[FMEA-6] Cluster stable after rapid leader churn (all nodes responding within ${i}s)"
        break
    fi
    sleep 1
done
$FMEA6_STABLE || fail "[FMEA-6] Not every node answered CQL within 30s of the churn"

# Verify committed data is readable after churn
for port in 9042 9043 9044; do
    wait_for_cql_value "$port" "CONSISTENCY QUORUM; SELECT v FROM fmea.kv WHERE k = 'pre_churn';" "before" \
        "[FMEA-6] pre-churn row via port $port after rapid leader churn"
done
pass "[FMEA-6] All committed data preserved after rapid leader churn"

echo ""
info "FMEA suite complete."
info "Cluster stack still running. Use 'docker compose -f tests/docker-compose.cluster.yml down -v' to stop."

trap - EXIT

fi  # RUN_FMEA

# ============================================================
# Summary
# ============================================================
echo ""
echo -e "${GREEN}==============================${NC}"
echo -e "${GREEN}  Smoke tests completed!${NC}"
echo -e "${GREEN}==============================${NC}"
echo ""
info "Test matrix coverage:"
if $RUN_PAIR; then
    info "  Phase 1-5:   Pair mode (writes, failover, promote, catch-up, switchover)"
    info "  Phase 6:     Cluster formation (3rd node joins)"
    info "  Phase 7:     3-node writes/reads (any-node coordinator)"
    info "  Phase 8:     1 node down — QUORUM writes/reads succeed"
    info "  Phase 9:     2 nodes down — below QUORUM, writes fail"
    info "  Phase 10:    Cluster recovery — nodes rejoin, writes resume"
    info "  Phase 11:    DDL replication across 3 nodes"
    info "  Phase 12:    FMEA failure modes (data on 3rd node, DDL on follower,"
    info "               stale data after rejoin, write timeout, token distribution)"
    info "  Phase 13:    Cross-node subscription test"
fi
if $RUN_TRIO; then
    info "  C1-C10:      3-node cluster (Raft election, QUORUM writes, failover, hints)"
fi
if $RUN_QUINT; then
    info "  F1-F6:       5-node cluster (QUORUM at scale, dual kill, hint replay)"
fi
if $RUN_LIFECYCLE; then
    info "  L1-L7:       Node lifecycle (add-node, bootstrap, decommission, rebalance)"
fi
if $RUN_FMEA; then
    info "  FMEA-1..6:   FMEA scenarios (partition, crash, disk full, churn)"
fi
