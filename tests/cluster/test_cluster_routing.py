"""Integration test: cluster-mode read/write routing.

Verifies that reads and writes are correctly routed to the token owner
in a multi-node cluster. This catches:
- Reads returning empty because they go to the wrong node (P0)
- Stack overflow from infinite recursion in read routing
- Writes going to local storage instead of the token owner

Run against the 3-node cluster:
    podman compose -f tests/docker-compose.cluster.yml --profile trio up -d
    pytest tests/cluster/test_cluster_routing.py -v -s
"""

import os
import time
import uuid

import pytest
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy

FERROSA_HOST = os.environ.get("FERROSA_HOST", "127.0.0.1")
FERROSA_CQL_PORT = int(os.environ.get("FERROSA_CQL_PORT", "9042"))

KEYSPACE = "routing_test"
TABLE = "items"


@pytest.fixture(scope="module")
def session():
    cluster = Cluster(
        contact_points=[FERROSA_HOST],
        port=FERROSA_CQL_PORT,
        load_balancing_policy=RoundRobinPolicy(),
        protocol_version=4,
        schema_metadata_enabled=False,
        token_metadata_enabled=False,
    )
    sess = cluster.connect()
    yield sess
    sess.shutdown()
    cluster.shutdown()


@pytest.fixture(scope="module")
def schema(session):
    session.execute(
        f"CREATE KEYSPACE IF NOT EXISTS {KEYSPACE} "
        "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.execute(
        f"CREATE TABLE IF NOT EXISTS {KEYSPACE}.{TABLE} ("
        "  id text PRIMARY KEY,"
        "  data text"
        ")"
    )
    yield


class TestClusterRouting:
    """Verify reads and writes are routed to the correct replica."""

    def test_write_then_immediate_read(self, session, schema):
        """Write a row and immediately read it back.

        With RF=1 and RoundRobinPolicy, the write and read may go to
        different coordinator nodes. The read must still find the data
        because the coordinator routes to the correct replica.
        """
        row_id = str(uuid.uuid4())
        session.execute(
            f"INSERT INTO {KEYSPACE}.{TABLE} (id, data) "
            f"VALUES ('{row_id}', 'test_data')"
        )

        # Immediate read — must find the data even if different coordinator
        rows = list(session.execute(
            f"SELECT data FROM {KEYSPACE}.{TABLE} WHERE id = '{row_id}'"
        ))
        assert len(rows) == 1, f"expected 1 row, got {len(rows)} — read routing failed"
        assert rows[0].data == "test_data"

    def test_many_keys_all_readable(self, session, schema):
        """Write 100 keys and verify all are readable.

        With 3 nodes and RF=1, keys distribute across all nodes.
        If read routing is broken, ~2/3 of reads return empty.
        """
        count = 100
        ids = []
        for i in range(count):
            row_id = f"routing_test_{i}_{uuid.uuid4().hex[:8]}"
            ids.append(row_id)
            session.execute(
                f"INSERT INTO {KEYSPACE}.{TABLE} (id, data) "
                f"VALUES ('{row_id}', 'value_{i}')"
            )

        # Read all back
        found = 0
        missing = []
        for row_id in ids:
            rows = list(session.execute(
                f"SELECT data FROM {KEYSPACE}.{TABLE} WHERE id = '{row_id}'"
            ))
            if rows:
                found += 1
            else:
                missing.append(row_id)

        print(f"\nFound {found}/{count} rows")
        if missing:
            print(f"Missing (first 10): {missing[:10]}")

        assert found == count, (
            f"READ ROUTING BUG: only {found}/{count} rows readable. "
            f"{count - found} rows lost — reads going to wrong node."
        )

    def test_read_from_each_coordinator(self, session, schema):
        """Write one row, then read it 30 times.

        With RoundRobinPolicy on 3 nodes, each node gets ~10 reads.
        ALL must succeed — proving every coordinator can route reads
        to the correct replica.
        """
        row_id = f"coordinator_test_{uuid.uuid4().hex[:8]}"
        session.execute(
            f"INSERT INTO {KEYSPACE}.{TABLE} (id, data) "
            f"VALUES ('{row_id}', 'coordinator_test')"
        )

        successes = 0
        for _ in range(30):
            rows = list(session.execute(
                f"SELECT data FROM {KEYSPACE}.{TABLE} WHERE id = '{row_id}'"
            ))
            if rows and rows[0].data == "coordinator_test":
                successes += 1

        print(f"\n{successes}/30 reads succeeded")
        assert successes == 30, (
            f"READ ROUTING: only {successes}/30 reads succeeded. "
            f"Some coordinators can't route reads to the correct replica."
        )

    def test_no_stack_overflow_during_reads(self, session, schema):
        """Rapid reads should not cause stack overflow.

        The stack overflow bug (393d6f8) happened when ReadRequestHandler
        called storage.read() which routed through the coordinator which
        sent another ReadRequest → infinite recursion.
        """
        row_id = f"overflow_test_{uuid.uuid4().hex[:8]}"
        session.execute(
            f"INSERT INTO {KEYSPACE}.{TABLE} (id, data) "
            f"VALUES ('{row_id}', 'overflow_check')"
        )

        # 100 rapid reads — if there's a stack overflow, the node crashes
        for i in range(100):
            rows = list(session.execute(
                f"SELECT data FROM {KEYSPACE}.{TABLE} WHERE id = '{row_id}'"
            ))
            assert len(rows) == 1, f"read {i}: expected 1 row, got {len(rows)}"

        print("\n100 rapid reads completed without stack overflow")
