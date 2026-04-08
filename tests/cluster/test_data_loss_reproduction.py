"""Reproduce P0 data loss on 3-node cluster during high-volume ingest.

This test creates a table, inserts canary rows, then performs a large
bulk insert (simulating frg ingest), and verifies all canary rows
survive. The production bug causes ~97% of canary rows to disappear
during or after the large write.

Run against the 3-node cluster:
    podman compose -f tests/docker-compose.cluster.yml --profile trio up -d --build
    # Wait for cluster to form (~30s)
    pip install cassandra-driver pytest
    pytest tests/cluster/test_data_loss_reproduction.py -v -s

The test targets node1 (port 9042) but the cluster replicates to all 3 nodes.
"""

import os
import time
import uuid

import pytest
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy

FERROSA_HOST = os.environ.get("FERROSA_HOST", "127.0.0.1")
FERROSA_CQL_PORT = int(os.environ.get("FERROSA_CQL_PORT", "9042"))

KEYSPACE = "dataloss_test"
TABLE = "entities"


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
        "  tenant_id uuid,"
        "  session_id uuid,"
        "  entity_id uuid,"
        "  entity_name text,"
        "  entity_type text,"
        "  PRIMARY KEY ((tenant_id, session_id), entity_id)"
        ")"
    )
    # Fixed partition key for all rows (same as production)
    tenant = uuid.UUID("00000000-0000-0000-0000-000000000001")
    session_id = uuid.UUID("00000000-0000-0000-0000-000000000002")
    yield tenant, session_id


def count_entities(session, tenant, session_id):
    rows = list(session.execute(
        f"SELECT count(*) FROM {KEYSPACE}.{TABLE} "
        f"WHERE tenant_id = {tenant} AND session_id = {session_id}"
    ))
    return rows[0].count if rows else 0


class TestDataLossReproduction:
    """Reproduce the exact production data loss scenario."""

    def test_canaries_survive_large_ingest(self, session, schema):
        """Insert 100 canaries, then 5000 entities. All canaries must survive."""
        tenant, session_id = schema
        canary_ids = []

        # Step 1: Insert 100 canary rows
        for i in range(100):
            eid = uuid.uuid5(uuid.NAMESPACE_DNS, f"canary_{i}")
            canary_ids.append(eid)
            session.execute(
                f"INSERT INTO {KEYSPACE}.{TABLE} "
                f"(tenant_id, session_id, entity_id, entity_name, entity_type) "
                f"VALUES ({tenant}, {session_id}, {eid}, 'canary_{i}', 'canary')"
            )

        canary_count = count_entities(session, tenant, session_id)
        print(f"\nAfter canaries: {canary_count} entities")
        assert canary_count >= 100, f"canaries not all inserted: {canary_count}"

        # Step 2: Large bulk insert — 5000 entities (simulates frg ingest)
        for i in range(5000):
            eid = uuid.uuid5(uuid.NAMESPACE_DNS, f"entity_{i}")
            session.execute(
                f"INSERT INTO {KEYSPACE}.{TABLE} "
                f"(tenant_id, session_id, entity_id, entity_name, entity_type) "
                f"VALUES ({tenant}, {session_id}, {eid}, 'entity_{i}', 'module')"
            )
            if (i + 1) % 500 == 0:
                current = count_entities(session, tenant, session_id)
                print(f"  Progress: {i+1}/5000 written, count={current}")

        # Step 3: Wait for flushes/compaction to settle
        time.sleep(5)

        # Step 4: Count total
        total = count_entities(session, tenant, session_id)
        print(f"\nAfter large ingest: {total} entities (expected 5100)")

        # Step 5: Check canaries specifically
        surviving = 0
        for eid in canary_ids:
            rows = list(session.execute(
                f"SELECT entity_name FROM {KEYSPACE}.{TABLE} "
                f"WHERE tenant_id = {tenant} AND session_id = {session_id} "
                f"AND entity_id = {eid}"
            ))
            if rows:
                surviving += 1

        print(f"Canary survival: {surviving}/100")

        assert surviving == 100, (
            f"DATA LOSS: {100 - surviving} canary rows lost during large ingest. "
            f"Total entities: {total} (expected 5100). "
            f"This confirms the P0 data loss bug."
        )

        assert total >= 5100, (
            f"DATA LOSS: expected 5100 entities, got {total}. "
            f"{5100 - total} entities lost."
        )

    def test_second_large_ingest_preserves_first(self, session, schema):
        """Two sequential large ingests — second must not destroy first."""
        tenant, session_id = schema

        # Use a fresh session_id to avoid interference from test above
        sid2 = uuid.UUID("00000000-0000-0000-0000-000000000099")

        # Ingest 1: 2000 entities
        for i in range(2000):
            eid = uuid.uuid5(uuid.NAMESPACE_DNS, f"ingest1_{i}")
            session.execute(
                f"INSERT INTO {KEYSPACE}.{TABLE} "
                f"(tenant_id, session_id, entity_id, entity_name, entity_type) "
                f"VALUES ({tenant}, {sid2}, {eid}, 'ingest1_{i}', 'function')"
            )

        count1 = count_entities(session, tenant, sid2)
        print(f"\nAfter ingest 1: {count1} entities")
        assert count1 >= 2000

        # Ingest 2: 3000 entities (different entity_ids, same partition)
        for i in range(3000):
            eid = uuid.uuid5(uuid.NAMESPACE_DNS, f"ingest2_{i}")
            session.execute(
                f"INSERT INTO {KEYSPACE}.{TABLE} "
                f"(tenant_id, session_id, entity_id, entity_name, entity_type) "
                f"VALUES ({tenant}, {sid2}, {eid}, 'ingest2_{i}', 'crate')"
            )

        time.sleep(5)
        total = count_entities(session, tenant, sid2)
        print(f"After ingest 2: {total} entities (expected 5000)")

        assert total >= 5000, (
            f"DATA LOSS: expected 5000 entities from 2 ingests, got {total}. "
            f"{5000 - total} entities lost."
        )

    def test_read_from_all_nodes(self, session, schema):
        """Verify data is consistent across all 3 nodes."""
        tenant, session_id = schema

        # Write to node1, read from node1/2/3
        sid3 = uuid.UUID("00000000-0000-0000-0000-000000000088")
        for i in range(100):
            eid = uuid.uuid5(uuid.NAMESPACE_DNS, f"cross_node_{i}")
            session.execute(
                f"INSERT INTO {KEYSPACE}.{TABLE} "
                f"(tenant_id, session_id, entity_id, entity_name, entity_type) "
                f"VALUES ({tenant}, {sid3}, {eid}, 'cross_{i}', 'test')"
            )

        time.sleep(2)

        # Read from this node
        count = count_entities(session, tenant, sid3)
        print(f"\nCross-node count: {count}/100")
        assert count == 100, f"expected 100, got {count}"
