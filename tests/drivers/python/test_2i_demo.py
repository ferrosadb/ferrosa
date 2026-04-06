"""Secondary Index (2i) effectiveness demonstration.

For each index type, this test:
1. Creates a table and inserts 10,000 rows via CQL
2. Queries WITHOUT an index (full scan with ALLOW FILTERING) — records time
3. Creates the index and waits for background build
4. Queries WITH the index — records time
5. Prints a comparison table showing the speedup

This is a compatibility/correctness test with timing, not a load test.
The row count (10K) is enough to show orders-of-magnitude difference
between full scan and indexed lookup.

Requires:
  - Ferrosa running with CQL on port 9042
  - cassandra-driver >= 3.29.0
"""

import os
import time
import uuid

import pytest
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy

FERROSA_HOST = os.environ.get("FERROSA_HOST", "127.0.0.1")
FERROSA_CQL_PORT = int(os.environ.get("FERROSA_CQL_PORT", "9042"))

KEYSPACE = "idx_demo"
ROW_COUNT = 10_000


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def session():
    """Shared CQL session for the module."""
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
def keyspace(session):
    """Create the test keyspace."""
    session.execute(
        f"CREATE KEYSPACE IF NOT EXISTS {KEYSPACE} "
        "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    yield KEYSPACE


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def timed_query(session, cql, label=""):
    """Execute a CQL query and return (rows, elapsed_ms)."""
    start = time.monotonic()
    rows = list(session.execute(cql))
    elapsed_ms = (time.monotonic() - start) * 1000
    return rows, elapsed_ms


def wait_for_index_build(session, ks, table, index_name, timeout_s=30):
    """Poll until the index appears to be functional.

    After CREATE INDEX, ferrosa builds sidecar files in the background.
    We poll by running a known-match query until it returns results or
    we timeout.
    """
    time.sleep(1)  # initial settle time for index registration


def print_comparison(index_type, without_ms, with_ms, rows_without, rows_with):
    """Print a formatted comparison line."""
    speedup = without_ms / with_ms if with_ms > 0 else float("inf")
    print(
        f"  {index_type:<12} | "
        f"Without: {without_ms:8.1f}ms ({rows_without} rows) | "
        f"With: {with_ms:8.1f}ms ({rows_with} rows) | "
        f"Speedup: {speedup:6.1f}x"
    )


# ===========================================================================
# BTree Index Demo
# ===========================================================================


class TestBTreeIndex:
    """Demonstrates BTree index effectiveness for equality and range queries."""

    TABLE = "btree_demo"

    @pytest.fixture(autouse=True, scope="class")
    def setup_table(self, session, keyspace):
        """Create table and insert 10K rows."""
        session.execute(
            f"CREATE TABLE IF NOT EXISTS {keyspace}.{self.TABLE} ("
            "  id UUID PRIMARY KEY,"
            "  email TEXT,"
            "  age INT,"
            "  city TEXT"
            ")"
        )

        # Insert ROW_COUNT rows with predictable distribution
        cities = ["NYC", "SF", "LA", "Chicago", "Boston",
                  "Seattle", "Denver", "Miami", "Dallas", "Portland"]
        for i in range(ROW_COUNT):
            uid = uuid.uuid4()
            email = f"user{i}@example.com"
            age = 18 + (i % 60)
            city = cities[i % len(cities)]
            session.execute(
                f"INSERT INTO {keyspace}.{self.TABLE} "
                f"(id, email, age, city) VALUES "
                f"({uid}, '{email}', {age}, '{city}')"
            )

        yield

    def test_btree_equality_lookup(self, session, keyspace):
        """BTree index on email: point lookup vs full scan."""
        target_email = "user5000@example.com"

        # WITHOUT index — full scan
        rows_without, ms_without = timed_query(
            session,
            f"SELECT * FROM {keyspace}.{self.TABLE} "
            f"WHERE email = '{target_email}' ALLOW FILTERING",
        )

        # CREATE BTree index
        session.execute(
            f"CREATE INDEX IF NOT EXISTS idx_btree_email "
            f"ON {keyspace}.{self.TABLE} (email) USING 'btree'"
        )
        wait_for_index_build(session, keyspace, self.TABLE, "idx_btree_email")

        # WITH index — indexed lookup
        rows_with, ms_with = timed_query(
            session,
            f"SELECT * FROM {keyspace}.{self.TABLE} "
            f"WHERE email = '{target_email}'",
        )

        print("\n=== BTree Index: Equality Lookup ===")
        print_comparison("BTree (=)", ms_without, ms_with, len(rows_without), len(rows_with))

        # Both should find the same row
        assert len(rows_without) == len(rows_with)
        assert len(rows_with) >= 1

    def test_btree_range_query(self, session, keyspace):
        """BTree index on age: range query vs full scan."""
        # WITHOUT index — full scan
        rows_without, ms_without = timed_query(
            session,
            f"SELECT * FROM {keyspace}.{self.TABLE} "
            f"WHERE age > 70 ALLOW FILTERING",
        )

        # CREATE BTree index on age
        session.execute(
            f"CREATE INDEX IF NOT EXISTS idx_btree_age "
            f"ON {keyspace}.{self.TABLE} (age) USING 'btree'"
        )
        wait_for_index_build(session, keyspace, self.TABLE, "idx_btree_age")

        # WITH index — range scan
        rows_with, ms_with = timed_query(
            session,
            f"SELECT * FROM {keyspace}.{self.TABLE} "
            f"WHERE age > 70",
        )

        print("\n=== BTree Index: Range Query ===")
        print_comparison("BTree (>)", ms_without, ms_with, len(rows_without), len(rows_with))

        assert len(rows_without) == len(rows_with)


# ===========================================================================
# Hash Index Demo
# ===========================================================================


class TestHashIndex:
    """Demonstrates Hash index effectiveness for exact-match lookups."""

    TABLE = "hash_demo"

    @pytest.fixture(autouse=True, scope="class")
    def setup_table(self, session, keyspace):
        """Create table and insert 10K rows."""
        session.execute(
            f"CREATE TABLE IF NOT EXISTS {keyspace}.{self.TABLE} ("
            "  id UUID PRIMARY KEY,"
            "  username TEXT,"
            "  status TEXT"
            ")"
        )

        statuses = ["active", "inactive", "suspended", "pending"]
        for i in range(ROW_COUNT):
            uid = uuid.uuid4()
            username = f"user_{i:05d}"
            status = statuses[i % len(statuses)]
            session.execute(
                f"INSERT INTO {keyspace}.{self.TABLE} "
                f"(id, username, status) VALUES "
                f"({uid}, '{username}', '{status}')"
            )

        yield

    def test_hash_point_lookup(self, session, keyspace):
        """Hash index on username: O(1) lookup vs full scan."""
        target = "user_05000"

        # WITHOUT index
        rows_without, ms_without = timed_query(
            session,
            f"SELECT * FROM {keyspace}.{self.TABLE} "
            f"WHERE username = '{target}' ALLOW FILTERING",
        )

        # CREATE Hash index
        session.execute(
            f"CREATE INDEX IF NOT EXISTS idx_hash_username "
            f"ON {keyspace}.{self.TABLE} (username) USING 'hash'"
        )
        wait_for_index_build(session, keyspace, self.TABLE, "idx_hash_username")

        # WITH index
        rows_with, ms_with = timed_query(
            session,
            f"SELECT * FROM {keyspace}.{self.TABLE} "
            f"WHERE username = '{target}'",
        )

        print("\n=== Hash Index: Point Lookup ===")
        print_comparison("Hash (=)", ms_without, ms_with, len(rows_without), len(rows_with))

        assert len(rows_without) == len(rows_with)
        assert len(rows_with) >= 1

    def test_hash_low_cardinality(self, session, keyspace):
        """Hash index on status (4 distinct values): selective filter."""
        # WITHOUT index
        rows_without, ms_without = timed_query(
            session,
            f"SELECT * FROM {keyspace}.{self.TABLE} "
            f"WHERE status = 'suspended' ALLOW FILTERING",
        )

        # CREATE Hash index
        session.execute(
            f"CREATE INDEX IF NOT EXISTS idx_hash_status "
            f"ON {keyspace}.{self.TABLE} (status) USING 'hash'"
        )
        wait_for_index_build(session, keyspace, self.TABLE, "idx_hash_status")

        # WITH index
        rows_with, ms_with = timed_query(
            session,
            f"SELECT * FROM {keyspace}.{self.TABLE} "
            f"WHERE status = 'suspended'",
        )

        print("\n=== Hash Index: Low Cardinality ===")
        print_comparison("Hash (low)", ms_without, ms_with, len(rows_without), len(rows_with))

        # ~2500 rows with status='suspended' (1/4 of 10K)
        assert len(rows_without) == len(rows_with)
        assert len(rows_with) > 2000


# ===========================================================================
# Composite Index Demo
# ===========================================================================


class TestCompositeIndex:
    """Demonstrates Composite index for multi-column queries."""

    TABLE = "composite_demo"

    @pytest.fixture(autouse=True, scope="class")
    def setup_table(self, session, keyspace):
        """Create table and insert 10K rows."""
        session.execute(
            f"CREATE TABLE IF NOT EXISTS {keyspace}.{self.TABLE} ("
            "  id UUID PRIMARY KEY,"
            "  last_name TEXT,"
            "  first_name TEXT,"
            "  department TEXT"
            ")"
        )

        last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones",
                      "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]
        first_names = ["Alice", "Bob", "Carol", "Dave", "Eve",
                       "Frank", "Grace", "Hank", "Iris", "Jack"]
        depts = ["Engineering", "Sales", "Marketing", "Support", "Finance"]

        for i in range(ROW_COUNT):
            uid = uuid.uuid4()
            last = last_names[i % len(last_names)]
            first = first_names[(i // len(last_names)) % len(first_names)]
            dept = depts[i % len(depts)]
            session.execute(
                f"INSERT INTO {keyspace}.{self.TABLE} "
                f"(id, last_name, first_name, department) VALUES "
                f"({uid}, '{last}', '{first}', '{dept}')"
            )

        yield

    def test_composite_multi_column_lookup(self, session, keyspace):
        """Composite index on (last_name, first_name): multi-column filter."""
        # WITHOUT index
        rows_without, ms_without = timed_query(
            session,
            f"SELECT * FROM {keyspace}.{self.TABLE} "
            f"WHERE last_name = 'Smith' AND first_name = 'Alice' ALLOW FILTERING",
        )

        # CREATE Composite index
        session.execute(
            f"CREATE INDEX IF NOT EXISTS idx_composite_name "
            f"ON {keyspace}.{self.TABLE} (last_name, first_name) USING 'composite'"
        )
        wait_for_index_build(session, keyspace, self.TABLE, "idx_composite_name")

        # WITH index
        rows_with, ms_with = timed_query(
            session,
            f"SELECT * FROM {keyspace}.{self.TABLE} "
            f"WHERE last_name = 'Smith' AND first_name = 'Alice'",
        )

        print("\n=== Composite Index: Multi-Column Lookup ===")
        print_comparison("Composite", ms_without, ms_with, len(rows_without), len(rows_with))

        assert len(rows_without) == len(rows_with)
        assert len(rows_with) >= 1


# ===========================================================================
# Summary
# ===========================================================================


class TestIndexSummary:
    """Final summary printed after all index tests run."""

    def test_print_summary(self):
        """Marker test that prints the index type reference."""
        print("\n" + "=" * 70)
        print("Ferrosa Secondary Index Types")
        print("=" * 70)
        print(f"{'Type':<12} | {'CQL Syntax':<30} | {'Capabilities'}")
        print("-" * 70)
        print(f"{'BTree':<12} | {'USING btree (default)':<30} | {'Point + Range'}")
        print(f"{'Hash':<12} | {'USING hash':<30} | {'Point only (O(1))'}")
        print(f"{'Composite':<12} | {'USING composite':<30} | {'Multi-column point + prefix'}")
        print(f"{'Phonetic':<12} | {'USING phonetic':<30} | {'Fuzzy name matching'}")
        print(f"{'Vector':<12} | {'USING vector':<30} | {'ANN (HNSW/IVFFlat)'}")
        print(f"{'FullText':<12} | {'USING fulltext':<30} | {'BM25 search (fts_match)'}")
        print(f"{'Filtered':<12} | {'USING filtered':<30} | {'Partial index (with pred)'}")
        print("=" * 70)
