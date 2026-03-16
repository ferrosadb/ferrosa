"""CQL driver smoke tests using the DataStax Python cassandra-driver.

Each test is idempotent (IF NOT EXISTS / IF EXISTS).  The entire suite
uses the ``python_test`` keyspace to avoid collisions with other drivers.
"""

import os
import uuid
from datetime import datetime, timezone

import pytest
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy
from cassandra.query import SimpleStatement

FERROSA_HOST = os.environ.get("FERROSA_HOST", "127.0.0.1")
FERROSA_CQL_PORT = int(os.environ.get("FERROSA_CQL_PORT", "9042"))

KEYSPACE = "python_test"


@pytest.fixture(scope="module")
def session():
    """Create a shared session for the whole module."""
    cluster = Cluster(
        contact_points=[FERROSA_HOST],
        port=FERROSA_CQL_PORT,
        load_balancing_policy=RoundRobinPolicy(),
        protocol_version=5,
    )
    sess = cluster.connect()
    yield sess
    sess.shutdown()
    cluster.shutdown()


# ---- Connection & introspection ------------------------------------------


class TestConnection:
    def test_connect(self, session):
        """Driver can connect and the session is open."""
        assert session is not None

    def test_system_local(self, session):
        """Query system.local returns at least one row."""
        rows = session.execute("SELECT cluster_name, data_center FROM system.local")
        row_list = list(rows)
        assert len(row_list) >= 1
        assert row_list[0].cluster_name is not None

    def test_system_peers(self, session):
        """system.peers is queryable (may return 0 rows for single node)."""
        rows = session.execute("SELECT * FROM system.peers")
        # Just confirm it doesn't error -- single-node may have no peers.
        assert rows is not None


# ---- DDL -----------------------------------------------------------------


class TestDDL:
    def test_create_keyspace(self, session):
        session.execute(
            f"CREATE KEYSPACE IF NOT EXISTS {KEYSPACE} "
            "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
        )
        session.set_keyspace(KEYSPACE)

    def test_create_table(self, session):
        session.set_keyspace(KEYSPACE)
        session.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id int PRIMARY KEY,
                name text,
                email text,
                active boolean,
                score float,
                rating double,
                age bigint,
                profile blob,
                user_uuid uuid,
                created_at timestamp
            )
            """
        )

    def test_create_clustering_table(self, session):
        session.set_keyspace(KEYSPACE)
        session.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                user_id int,
                ts timestamp,
                data text,
                PRIMARY KEY (user_id, ts)
            )
            """
        )


# ---- DML (write + read) -------------------------------------------------


class TestDML:
    @pytest.fixture(autouse=True)
    def _set_keyspace(self, session):
        session.set_keyspace(KEYSPACE)

    def test_insert_text_int(self, session):
        session.execute(
            "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@test.com')"
        )

    def test_insert_boolean(self, session):
        session.execute("INSERT INTO users (id, active) VALUES (2, true)")

    def test_insert_float_double(self, session):
        session.execute(
            "INSERT INTO users (id, score, rating) VALUES (3, 95.5, 99.12345678)"
        )

    def test_insert_bigint(self, session):
        session.execute("INSERT INTO users (id, age) VALUES (4, 9223372036854775807)")

    def test_insert_blob(self, session):
        session.execute("INSERT INTO users (id, profile) VALUES (5, 0xdeadbeef)")

    def test_insert_uuid(self, session):
        test_uuid = uuid.uuid4()
        session.execute(
            SimpleStatement("INSERT INTO users (id, user_uuid) VALUES (%s, %s)"),
            (6, test_uuid),
        )

    def test_insert_timestamp(self, session):
        now = datetime.now(timezone.utc)
        session.execute(
            SimpleStatement("INSERT INTO users (id, created_at) VALUES (%s, %s)"),
            (7, now),
        )

    def test_select_by_pk(self, session):
        rows = list(session.execute("SELECT * FROM users WHERE id = 1"))
        assert len(rows) == 1
        assert rows[0].name == "Alice"
        assert rows[0].email == "alice@test.com"

    def test_select_boolean(self, session):
        rows = list(session.execute("SELECT active FROM users WHERE id = 2"))
        assert len(rows) == 1
        assert rows[0].active is True

    def test_select_float_double(self, session):
        rows = list(session.execute("SELECT score, rating FROM users WHERE id = 3"))
        assert len(rows) == 1
        assert abs(rows[0].score - 95.5) < 0.01
        assert abs(rows[0].rating - 99.12345678) < 0.0001

    def test_select_bigint(self, session):
        rows = list(session.execute("SELECT age FROM users WHERE id = 4"))
        assert len(rows) == 1
        assert rows[0].age == 9223372036854775807

    def test_insert_clustering(self, session):
        session.execute(
            "INSERT INTO events (user_id, ts, data) VALUES "
            "(1, '2024-01-01T00:00:00Z', 'login')"
        )
        session.execute(
            "INSERT INTO events (user_id, ts, data) VALUES "
            "(1, '2024-01-01T01:00:00Z', 'logout')"
        )

    def test_select_clustering_range(self, session):
        rows = list(
            session.execute("SELECT * FROM events WHERE user_id = 1 ORDER BY ts ASC")
        )
        assert len(rows) == 2
        assert rows[0].data == "login"
        assert rows[1].data == "logout"


# ---- Prepared statements -------------------------------------------------


class TestPrepared:
    @pytest.fixture(autouse=True)
    def _set_keyspace(self, session):
        session.set_keyspace(KEYSPACE)

    def test_prepare_and_execute(self, session):
        prepared = session.prepare(
            "INSERT INTO users (id, name, email) VALUES (?, ?, ?)"
        )
        session.execute(prepared, (100, "Prepared", "prepared@test.com"))

        rows = list(session.execute("SELECT name FROM users WHERE id = 100"))
        assert len(rows) == 1
        assert rows[0].name == "Prepared"

    def test_prepare_select(self, session):
        prepared = session.prepare("SELECT * FROM users WHERE id = ?")
        rows = list(session.execute(prepared, (1,)))
        assert len(rows) == 1
        assert rows[0].name == "Alice"


# ---- UDT (User-Defined Type) --------------------------------------------


class TestUDT:
    @pytest.fixture(autouse=True)
    def _set_keyspace(self, session):
        session.set_keyspace(KEYSPACE)

    def test_create_type(self, session):
        session.execute(
            """
            CREATE TYPE IF NOT EXISTS address (
                street text,
                city text,
                zip int
            )
            """
        )

    def test_create_table_with_udt(self, session):
        session.execute(
            """
            CREATE TABLE IF NOT EXISTS contacts (
                id int PRIMARY KEY,
                name text,
                home_address frozen<address>
            )
            """
        )

    def test_insert_udt(self, session):
        session.execute(
            "INSERT INTO contacts (id, name, home_address) "
            "VALUES (1, 'Alice', {street: '123 Main St', city: 'Springfield', zip: 62704})"
        )

    def test_select_udt(self, session):
        rows = list(session.execute("SELECT * FROM contacts WHERE id = 1"))
        assert len(rows) == 1
        assert rows[0].name == "Alice"
        # The driver returns UDTs as named tuples or ordered dicts.
        addr = rows[0].home_address
        assert addr is not None


# ---- Cleanup (runs last by naming convention) ----------------------------


class TestZZCleanup:
    """Tear down test artifacts.  Prefixed 'ZZ' so pytest runs it last."""

    def test_drop_keyspace(self, session):
        session.execute(f"DROP KEYSPACE IF EXISTS {KEYSPACE}")
