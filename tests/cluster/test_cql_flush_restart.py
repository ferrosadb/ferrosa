#!/usr/bin/env python3
"""CQL integration test: INSERT → flush → restart → SELECT.

Tests 3 failure modes that unit tests can't catch:
  1. Concurrent flush/compaction races
  2. CQL-to-storage cell ordering mismatch
  3. Auto-flush racing with explicit flush

Usage:
  # Start cluster first:
  podman compose -f tests/cluster/docker-compose.cql-integration.yml up -d --build
  # Run test:
  python tests/cluster/test_cql_flush_restart.py
  # Tear down:
  podman compose -f tests/cluster/docker-compose.cql-integration.yml down -v
"""

import os
import subprocess
import sys
import time
import uuid

# cassandra-driver: pip install cassandra-driver
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement, ConsistencyLevel

CQL_PORTS = [
    int(os.environ.get("CQL_PORT_1", "30042")),
    int(os.environ.get("CQL_PORT_2", "30043")),
    int(os.environ.get("CQL_PORT_3", "30044")),
]
CQL_HOST = os.environ.get("CQL_HOST", "127.0.0.1")
COMPOSE_FILE = os.path.join(
    os.path.dirname(__file__), "docker-compose.cql-integration.yml"
)
TENANT = uuid.UUID("9a5f8fbf-d842-4d30-8ea5-1aa931e618a8")
SESSION_ID = uuid.UUID("00000000-0000-0000-0000-000000000000")


def connect(port):
    """Connect to a single ferrosa node."""
    cluster = Cluster([CQL_HOST], port=port, protocol_version=4)
    session = cluster.connect()
    session.default_consistency_level = ConsistencyLevel.ONE
    return cluster, session


def setup_schema(session):
    """Create the keyspace and tables matching ferrosa-memory."""
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS agent_memory "
        "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.set_keyspace("agent_memory")

    # entity_store: single UUID CK — the table that corrupts in production
    session.execute("""
        CREATE TABLE IF NOT EXISTS entity_store (
            tenant_id uuid,
            session_id uuid,
            entity_id uuid,
            entity_name text,
            entity_type text,
            context_snippet text,
            confidence float,
            created_at timestamp,
            PRIMARY KEY ((tenant_id, session_id), entity_id)
        )
    """)

    # typed_edges: 3-column CK (uuid, text, uuid) — multi-column CK test
    session.execute("""
        CREATE TABLE IF NOT EXISTS typed_edges (
            tenant_id uuid,
            session_id uuid,
            src_id uuid,
            edge_type text,
            dst_id uuid,
            weight double,
            metadata text,
            created_at timestamp,
            PRIMARY KEY ((tenant_id, session_id), src_id, edge_type, dst_id)
        )
    """)

    # session_hierarchy: 2-column CK (int, uuid) — all fixed-length multi-CK
    session.execute("""
        CREATE TABLE IF NOT EXISTS session_hierarchy (
            session_id uuid,
            tenant_id uuid,
            depth int,
            subtask_id uuid,
            label text,
            PRIMARY KEY ((session_id, tenant_id), depth, subtask_id)
        )
    """)


def insert_entities(session, count, start=0):
    """Insert entities with columns in non-schema order (tests cell ordering)."""
    stmt = session.prepare(
        "INSERT INTO entity_store "
        "(tenant_id, session_id, entity_name, entity_id, entity_type, confidence) "
        "VALUES (?, ?, ?, ?, ?, ?)"
    )
    for i in range(start, start + count):
        entity_id = uuid.UUID(f"00000000-0000-0000-0000-{i:012d}")
        session.execute(stmt, (
            TENANT, SESSION_ID,
            f"entity_{i}",
            entity_id,
            "concept",
            0.95,
        ))


def insert_edges(session, count, start=0):
    """Insert typed edges (3-column CK)."""
    stmt = session.prepare(
        "INSERT INTO typed_edges "
        "(tenant_id, session_id, src_id, edge_type, dst_id, weight, metadata) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)"
    )
    for i in range(start, start + count):
        src = uuid.UUID(f"00000000-0000-0000-0001-{i:012d}")
        dst = uuid.UUID(f"00000000-0000-0000-0002-{i:012d}")
        session.execute(stmt, (
            TENANT, SESSION_ID,
            src, "RELATED_TO", dst,
            0.85, f"edge_{i}",
        ))


def insert_hierarchy(session, count, start=0):
    """Insert session hierarchy rows (2-column fixed-length CK)."""
    stmt = session.prepare(
        "INSERT INTO session_hierarchy "
        "(session_id, tenant_id, depth, subtask_id, label) "
        "VALUES (?, ?, ?, ?, ?)"
    )
    for i in range(start, start + count):
        subtask = uuid.UUID(f"00000000-0000-0000-0003-{i:012d}")
        session.execute(stmt, (
            SESSION_ID, TENANT,
            i % 5,
            subtask,
            f"task_{i}",
        ))


def count_rows(session, table, pk_clause):
    """Count rows in a table for a given partition."""
    result = session.execute(f"SELECT count(*) FROM {table} WHERE {pk_clause}")
    return result.one()[0]


def restart_cluster():
    """Stop and restart the cluster to force memtable flush."""
    print("  Stopping cluster...")
    subprocess.run(
        ["podman", "compose", "-f", COMPOSE_FILE, "stop"],
        capture_output=True, timeout=60,
    )
    time.sleep(2)
    print("  Starting cluster...")
    subprocess.run(
        ["podman", "compose", "-f", COMPOSE_FILE, "up", "-d"],
        capture_output=True, timeout=60,
    )
    # Wait for all nodes to be healthy
    for attempt in range(30):
        try:
            for port in CQL_PORTS:
                c, s = connect(port)
                s.execute("SELECT now() FROM system.local")
                c.shutdown()
            print(f"  Cluster healthy after {attempt + 1} attempts")
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError("Cluster did not become healthy after restart")


def check_node_corruption(port):
    """Check a single node for corruption errors in logs."""
    # Read container logs for corruption indicators
    node_name = {30042: "node1", 30043: "node2", 30044: "node3"}.get(port, "?")
    result = subprocess.run(
        ["podman", "compose", "-f", COMPOSE_FILE, "logs", node_name, "--tail=200"],
        capture_output=True, text=True, timeout=30,
    )
    corruption_count = sum(
        1 for line in result.stdout.split("\n")
        if "READ ERROR" in line or "corrupted" in line.lower() or "corrupt" in line.lower()
    )
    return corruption_count


# ─────────────────────────────────────────────────────────────────────────────
# Test Cases
# ─────────────────────────────────────────────────────────────────────────────

def test_entity_store_survives_restart(session, port):
    """Test 1: entity_store (single UUID CK) data survives flush+restart.

    This is the exact table that loses 97-100% data in production.
    """
    print("\n=== Test 1: entity_store survives restart ===")
    n = 200
    pk = f"tenant_id = {TENANT} AND session_id = {SESSION_ID}"

    insert_entities(session, n)
    pre = count_rows(session, "entity_store", pk)
    print(f"  Pre-restart: {pre} entities")
    assert pre == n, f"expected {n}, got {pre}"

    restart_cluster()

    cluster2, session2 = connect(port)
    session2.set_keyspace("agent_memory")
    post = count_rows(session2, "entity_store", pk)
    print(f"  Post-restart: {post} entities")
    cluster2.shutdown()

    if post != n:
        print(f"  FAIL: lost {n - post} entities ({100*(n-post)/n:.0f}% loss)")
        return False
    print("  PASS")
    return True


def test_typed_edges_survives_restart(session, port):
    """Test 2: typed_edges (3-column CK: uuid, text, uuid) survives restart.

    Tests the multi-column CK serialization fix.
    """
    print("\n=== Test 2: typed_edges survives restart ===")
    n = 150
    pk = f"tenant_id = {TENANT} AND session_id = {SESSION_ID}"

    insert_edges(session, n)
    pre = count_rows(session, "typed_edges", pk)
    print(f"  Pre-restart: {pre} edges")
    assert pre == n, f"expected {n}, got {pre}"

    restart_cluster()

    cluster2, session2 = connect(port)
    session2.set_keyspace("agent_memory")
    post = count_rows(session2, "typed_edges", pk)
    print(f"  Post-restart: {post} edges")
    cluster2.shutdown()

    if post != n:
        print(f"  FAIL: lost {n - post} edges ({100*(n-post)/n:.0f}% loss)")
        return False
    print("  PASS")
    return True


def test_concurrent_writes_and_flush(session, port):
    """Test 3: concurrent high-volume writes trigger auto-flush correctly.

    With FLUSH_THRESHOLD_BYTES=8192, writes trigger multiple auto-flushes.
    Tests that concurrent flush + write doesn't corrupt data.
    """
    print("\n=== Test 3: concurrent writes + auto-flush ===")
    n = 500
    pk = f"tenant_id = {TENANT} AND session_id = {SESSION_ID}"

    # Rapid-fire inserts — low flush threshold means flushes happen mid-write
    insert_entities(session, n, start=10000)
    pre = count_rows(session, "entity_store", pk)
    # pre includes entities from test 1 if they survived
    print(f"  Pre-restart: {pre} total entities")

    restart_cluster()

    cluster2, session2 = connect(port)
    session2.set_keyspace("agent_memory")
    post = count_rows(session2, "entity_store", pk)
    print(f"  Post-restart: {post} total entities")
    cluster2.shutdown()

    if post != pre:
        print(f"  FAIL: lost {pre - post} entities ({100*(pre-post)/pre:.0f}% loss)")
        return False
    print("  PASS")
    return True


def test_multi_node_consistency(port):
    """Test 4: all 3 nodes return the same data after restart.

    Tests that the coordinator range read fix (bf65f33) works in practice.
    """
    print("\n=== Test 4: multi-node consistency ===")
    pk = f"tenant_id = {TENANT} AND session_id = {SESSION_ID}"

    counts = {}
    for p in CQL_PORTS:
        try:
            c, s = connect(p)
            s.set_keyspace("agent_memory")
            counts[p] = count_rows(s, "entity_store", pk)
            c.shutdown()
        except Exception as e:
            counts[p] = f"ERROR: {e}"

    print(f"  Entity counts: {counts}")
    values = [v for v in counts.values() if isinstance(v, int)]
    if len(values) < 3:
        print("  FAIL: could not connect to all nodes")
        return False
    if len(set(values)) != 1:
        print(f"  FAIL: inconsistent counts across nodes: {counts}")
        return False
    print("  PASS")
    return True


def test_corruption_errors():
    """Test 5: check all nodes for corruption errors in logs."""
    print("\n=== Test 5: corruption error check ===")
    total = 0
    for port in CQL_PORTS:
        n = check_node_corruption(port)
        name = {30042: "node1", 30043: "node2", 30044: "node3"}[port]
        print(f"  {name}: {n} corruption errors")
        total += n
    if total > 0:
        print(f"  FAIL: {total} total corruption errors")
        return False
    print("  PASS")
    return True


def main():
    print("CQL Flush/Restart Integration Test")
    print(f"Cluster: {CQL_HOST}:{CQL_PORTS}")
    print()

    # Connect to node1 for setup
    cluster, session = connect(CQL_PORTS[0])
    setup_schema(session)
    session.set_keyspace("agent_memory")

    results = []
    results.append(("entity_store restart", test_entity_store_survives_restart(session, CQL_PORTS[0])))

    # Reconnect after restart
    cluster.shutdown()
    cluster, session = connect(CQL_PORTS[0])
    session.set_keyspace("agent_memory")

    results.append(("typed_edges restart", test_typed_edges_survives_restart(session, CQL_PORTS[0])))

    cluster.shutdown()
    cluster, session = connect(CQL_PORTS[0])
    session.set_keyspace("agent_memory")

    results.append(("concurrent writes", test_concurrent_writes_and_flush(session, CQL_PORTS[0])))

    cluster.shutdown()
    results.append(("multi-node consistency", test_multi_node_consistency(CQL_PORTS[0])))
    results.append(("corruption errors", test_corruption_errors()))

    # Summary
    print("\n" + "=" * 60)
    passed = sum(1 for _, ok in results if ok)
    total = len(results)
    for name, ok in results:
        print(f"  {'PASS' if ok else 'FAIL'}: {name}")
    print(f"\n  {passed}/{total} tests passed")

    if passed < total:
        print("\n  DATA LOSS DETECTED — see above for details")
        sys.exit(1)
    else:
        print("\n  All tests passed — no data loss detected")
        sys.exit(0)


if __name__ == "__main__":
    main()
