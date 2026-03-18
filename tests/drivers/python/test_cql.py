"""CQL driver smoke tests using the DataStax Python cassandra-driver.

Each test is idempotent (IF NOT EXISTS / IF EXISTS).  The entire suite
uses the ``python_test`` keyspace to avoid collisions with other drivers.
"""

import os
import time
import uuid
from datetime import datetime, timezone

import pytest
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy
from cassandra.query import BatchStatement, BatchType, SimpleStatement

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
        # Ferrosa's core CQL smoke coverage does not depend on driver-side
        # schema/token metadata refresh, and disabling it avoids treating
        # optional server event registration as a connection blocker.
        schema_metadata_enabled=False,
        token_metadata_enabled=False,
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


# ---- Collections ---------------------------------------------------------


class TestCollections:
    @pytest.fixture(autouse=True)
    def _set_keyspace(self, session):
        session.set_keyspace(KEYSPACE)

    def test_create_table_collections(self, session):
        session.execute(
            """
            CREATE TABLE IF NOT EXISTS collections (
                id int PRIMARY KEY,
                tags list<text>,
                scores set<int>,
                props map<text, text>
            )
            """
        )

    def test_insert_list(self, session):
        session.execute(
            SimpleStatement("INSERT INTO collections (id, tags) VALUES (%s, %s)"),
            (1, ["tag1", "tag2", "tag3"]),
        )

    def test_insert_set(self, session):
        session.execute(
            SimpleStatement("INSERT INTO collections (id, scores) VALUES (%s, %s)"),
            (2, {10, 20, 30}),
        )

    def test_insert_map(self, session):
        session.execute(
            SimpleStatement("INSERT INTO collections (id, props) VALUES (%s, %s)"),
            (3, {"key1": "val1", "key2": "val2"}),
        )

    def test_select_list(self, session):
        rows = list(session.execute("SELECT tags FROM collections WHERE id = 1"))
        assert len(rows) == 1
        assert rows[0].tags == ["tag1", "tag2", "tag3"]

    def test_select_set(self, session):
        rows = list(session.execute("SELECT scores FROM collections WHERE id = 2"))
        assert len(rows) == 1
        # Sets are unordered but the driver returns a sorted set.
        assert set(rows[0].scores) == {10, 20, 30}

    def test_select_map(self, session):
        rows = list(session.execute("SELECT props FROM collections WHERE id = 3"))
        assert len(rows) == 1
        assert rows[0].props == {"key1": "val1", "key2": "val2"}


# ---- ALTER TABLE ---------------------------------------------------------


class TestAlterTable:
    @pytest.fixture(autouse=True)
    def _set_keyspace(self, session):
        session.set_keyspace(KEYSPACE)

    def test_alter_add_column(self, session):
        session.execute("ALTER TABLE users ADD phone text")
        session.execute(
            "INSERT INTO users (id, name, phone) VALUES (800, 'PhoneUser', '555-1234')"
        )
        rows = list(session.execute("SELECT name, phone FROM users WHERE id = 800"))
        assert len(rows) == 1
        assert rows[0].name == "PhoneUser"
        assert rows[0].phone == "555-1234"


# ---- DELETE / UPDATE / LWT ----------------------------------------------


class TestDeleteUpdate:
    @pytest.fixture(autouse=True)
    def _set_keyspace(self, session):
        session.set_keyspace(KEYSPACE)

    def test_delete_row(self, session):
        session.execute("INSERT INTO users (id, name) VALUES (900, 'ToDelete')")
        rows = list(session.execute("SELECT * FROM users WHERE id = 900"))
        assert len(rows) == 1

        session.execute("DELETE FROM users WHERE id = 900")
        rows = list(session.execute("SELECT * FROM users WHERE id = 900"))
        assert len(rows) == 0

    def test_update_row(self, session):
        session.execute(
            "INSERT INTO users (id, name, email) VALUES (901, 'BeforeUpdate', 'old@test.com')"
        )
        session.execute("UPDATE users SET email = 'new@test.com' WHERE id = 901")
        rows = list(session.execute("SELECT email FROM users WHERE id = 901"))
        assert len(rows) == 1
        assert rows[0].email == "new@test.com"

    def test_insert_if_not_exists(self, session):
        # Ensure row does not exist first.
        session.execute("DELETE FROM users WHERE id = 902")

        # First INSERT IF NOT EXISTS should be applied.
        rows = list(
            session.execute(
                "INSERT INTO users (id, name) VALUES (902, 'LWT') IF NOT EXISTS"
            )
        )
        assert len(rows) == 1
        assert rows[0].applied is True

        # Second INSERT IF NOT EXISTS should NOT be applied.
        rows = list(
            session.execute(
                "INSERT INTO users (id, name) VALUES (902, 'LWT2') IF NOT EXISTS"
            )
        )
        assert len(rows) == 1
        assert rows[0].applied is False


# ---- Batch ---------------------------------------------------------------


class TestBatch:
    @pytest.fixture(autouse=True)
    def _set_keyspace(self, session):
        session.set_keyspace(KEYSPACE)

    def test_batch_insert(self, session):
        batch = BatchStatement(batch_type=BatchType.LOGGED)
        batch.add(
            SimpleStatement("INSERT INTO users (id, name) VALUES (%s, %s)"),
            (701, "Batch1"),
        )
        batch.add(
            SimpleStatement("INSERT INTO users (id, name) VALUES (%s, %s)"),
            (702, "Batch2"),
        )
        batch.add(
            SimpleStatement("INSERT INTO users (id, name) VALUES (%s, %s)"),
            (703, "Batch3"),
        )
        session.execute(batch)

        for uid, expected_name in [(701, "Batch1"), (702, "Batch2"), (703, "Batch3")]:
            rows = list(
                session.execute(
                    SimpleStatement("SELECT name FROM users WHERE id = %s"), (uid,)
                )
            )
            assert len(rows) == 1, f"expected 1 row for id={uid}"
            assert rows[0].name == expected_name


# ---- TTL -----------------------------------------------------------------


class TestTTL:
    @pytest.fixture(autouse=True)
    def _set_keyspace(self, session):
        session.set_keyspace(KEYSPACE)

    def test_insert_with_ttl(self, session):
        session.execute(
            "INSERT INTO users (id, name) VALUES (950, 'Ephemeral') USING TTL 1"
        )
        # Verify it exists right away.
        rows = list(session.execute("SELECT name FROM users WHERE id = 950"))
        assert len(rows) == 1
        assert rows[0].name == "Ephemeral"

        # Wait for TTL to expire.
        time.sleep(2)

        rows = list(session.execute("SELECT name FROM users WHERE id = 950"))
        assert len(rows) == 0


# ---- LIMIT / COUNT ------------------------------------------------------


class TestLimitCount:
    @pytest.fixture(autouse=True)
    def _set_keyspace(self, session):
        session.set_keyspace(KEYSPACE)

    def test_select_count(self, session):
        rows = list(session.execute("SELECT COUNT(*) FROM users"))
        assert len(rows) == 1
        assert rows[0].count > 0

    def test_select_limit(self, session):
        # Ensure at least 3 rows exist so LIMIT 2 is meaningful.
        session.execute("INSERT INTO users (id, name) VALUES (601, 'Limit1')")
        session.execute("INSERT INTO users (id, name) VALUES (602, 'Limit2')")
        session.execute("INSERT INTO users (id, name) VALUES (603, 'Limit3')")

        rows = list(session.execute("SELECT * FROM users LIMIT 2"))
        assert len(rows) == 2


# ---- Error handling ------------------------------------------------------


class TestErrorHandling:
    @pytest.fixture(autouse=True)
    def _set_keyspace(self, session):
        session.set_keyspace(KEYSPACE)

    def test_query_nonexistent_table(self, session):
        from cassandra import InvalidRequest

        with pytest.raises(InvalidRequest):
            session.execute("SELECT * FROM nonexistent_table_xyz")

    def test_invalid_syntax(self, session):
        from cassandra import InvalidRequest, SyntaxException

        with pytest.raises((SyntaxException, InvalidRequest)):
            session.execute("SELEC BROKEN QUERY")


# ---- system_schema introspection ----------------------------------------


class TestSystemSchema:
    @pytest.fixture(autouse=True)
    def _set_keyspace(self, session):
        session.set_keyspace(KEYSPACE)

    def test_system_schema_keyspaces(self, session):
        rows = list(
            session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
        )
        keyspace_names = [r.keyspace_name for r in rows]
        assert KEYSPACE in keyspace_names

    def test_system_schema_tables(self, session):
        rows = list(
            session.execute(
                "SELECT table_name FROM system_schema.tables "
                "WHERE keyspace_name = %s",
                (KEYSPACE,),
            )
        )
        table_names = [r.table_name for r in rows]
        assert "users" in table_names


# ---- NULL handling -------------------------------------------------------


class TestNullHandling:
    @pytest.fixture(autouse=True)
    def _set_keyspace(self, session):
        session.set_keyspace(KEYSPACE)

    def test_insert_null(self, session):
        session.execute(
            SimpleStatement("INSERT INTO users (id, name) VALUES (%s, %s)"),
            (960, None),
        )
        rows = list(session.execute("SELECT name FROM users WHERE id = 960"))
        assert len(rows) == 1
        assert rows[0].name is None

    def test_insert_empty_string(self, session):
        session.execute(
            SimpleStatement("INSERT INTO users (id, name) VALUES (%s, %s)"),
            (961, ""),
        )
        rows = list(session.execute("SELECT name FROM users WHERE id = 961"))
        assert len(rows) == 1
        assert rows[0].name == ""


# ---- Secondary index -----------------------------------------------------


class TestCreateIndex:
    @pytest.fixture(autouse=True)
    def _set_keyspace(self, session):
        session.set_keyspace(KEYSPACE)

    def test_create_index(self, session):
        session.execute("CREATE INDEX IF NOT EXISTS idx_users_name ON users (name)")


# ---- Cleanup (runs last by naming convention) ----------------------------


class TestZZCleanup:
    """Tear down test artifacts.  Prefixed 'ZZ' so pytest runs it last."""

    def test_drop_keyspace(self, session):
        session.execute(f"DROP KEYSPACE IF EXISTS {KEYSPACE}")
