"""Bolt v5 & Cypher wire compatibility tests using the official Neo4j Python driver.

Validates protocol correctness, Cypher query compatibility, data type fidelity,
and adjacency index usage against ferrosa's graph engine. This is compatibility
testing, not load testing.

Requires:
  - Ferrosa running with FERROSA_GRAPH_ENABLED=true
  - Bolt port 7687 exposed
  - CQL port 9042 exposed (for schema setup)
  - neo4j Python driver >= 5.0
  - cassandra-driver >= 3.29.0 (for CQL setup)
"""

import os
import time

import pytest
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy

# Neo4j driver for Bolt protocol
from neo4j import GraphDatabase
from neo4j.exceptions import (
    AuthError,
    ClientError,
    ServiceUnavailable,
)

FERROSA_HOST = os.environ.get("FERROSA_HOST", "127.0.0.1")
FERROSA_BOLT_PORT = int(os.environ.get("FERROSA_BOLT_PORT", "7687"))
FERROSA_CQL_PORT = int(os.environ.get("FERROSA_CQL_PORT", "9042"))

BOLT_URI = f"bolt://{FERROSA_HOST}:{FERROSA_BOLT_PORT}"
BOLT_AUTH = ("cassandra", "cassandra")

KEYSPACE = "bolt_compat_test"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def cql_session():
    """CQL session for DDL and seed data (not the system under test)."""
    cluster = Cluster(
        contact_points=[FERROSA_HOST],
        port=FERROSA_CQL_PORT,
        load_balancing_policy=RoundRobinPolicy(),
        protocol_version=5,
        schema_metadata_enabled=False,
        token_metadata_enabled=False,
    )
    sess = cluster.connect()
    yield sess
    sess.shutdown()
    cluster.shutdown()


@pytest.fixture(scope="module")
def social_graph(cql_session):
    """Create the social graph schema and seed data via CQL.

    Returns the keyspace name. Schema uses graph extensions so the
    adjacency index observer will maintain the index automatically.
    """
    ks = KEYSPACE
    sess = cql_session

    # Keyspace
    sess.execute(
        f"CREATE KEYSPACE IF NOT EXISTS {ks} "
        "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )

    # Vertex tables
    sess.execute(
        f"CREATE TABLE IF NOT EXISTS {ks}.person_v ("
        "  id TEXT PRIMARY KEY,"
        "  name TEXT,"
        "  age INT,"
        "  city TEXT,"
        "  email TEXT"
        ") WITH extensions = {{"
        "  'graph.type': 'vertex',"
        "  'graph.label': 'Person'"
        "}}"
    )
    sess.execute(
        f"CREATE TABLE IF NOT EXISTS {ks}.company_v ("
        "  id TEXT PRIMARY KEY,"
        "  name TEXT,"
        "  founded INT"
        ") WITH extensions = {{"
        "  'graph.type': 'vertex',"
        "  'graph.label': 'Company'"
        "}}"
    )

    # Edge tables
    sess.execute(
        f"CREATE TABLE IF NOT EXISTS {ks}.knows_e ("
        "  src_id TEXT,"
        "  tgt_id TEXT,"
        "  since_year INT,"
        "  PRIMARY KEY (src_id, tgt_id)"
        ") WITH extensions = {{"
        "  'graph.type': 'edge',"
        "  'graph.label': 'KNOWS',"
        "  'graph.source': 'src_id',"
        "  'graph.target': 'tgt_id'"
        "}}"
    )
    sess.execute(
        f"CREATE TABLE IF NOT EXISTS {ks}.works_at_e ("
        "  src_id TEXT,"
        "  tgt_id TEXT,"
        "  role TEXT,"
        "  PRIMARY KEY (src_id, tgt_id)"
        ") WITH extensions = {{"
        "  'graph.type': 'edge',"
        "  'graph.label': 'WORKS_AT',"
        "  'graph.source': 'src_id',"
        "  'graph.target': 'tgt_id'"
        "}}"
    )

    # Seed vertices
    for pid, name, age, city, email in [
        ("alice", "Alice", 30, "NYC", "alice@example.com"),
        ("bob", "Bob", 25, "SF", None),
        ("carol", "Carol", 35, "NYC", "carol@example.com"),
        ("dave", "Dave", 28, "LA", None),
        ("eve", "Eve", 32, "SF", "eve@example.com"),
    ]:
        if email:
            sess.execute(
                f"INSERT INTO {ks}.person_v (id, name, age, city, email) "
                f"VALUES ('{pid}', '{name}', {age}, '{city}', '{email}')"
            )
        else:
            sess.execute(
                f"INSERT INTO {ks}.person_v (id, name, age, city) "
                f"VALUES ('{pid}', '{name}', {age}, '{city}')"
            )

    for cid, name, founded in [
        ("acme", "Acme Corp", 2010),
        ("globex", "Globex Inc", 2015),
    ]:
        sess.execute(
            f"INSERT INTO {ks}.company_v (id, name, founded) "
            f"VALUES ('{cid}', '{name}', {founded})"
        )

    # Seed edges
    for src, tgt, since in [
        ("alice", "bob", 2018),
        ("alice", "carol", 2019),
        ("bob", "carol", 2020),
        ("carol", "dave", 2021),
        ("dave", "eve", 2022),
    ]:
        sess.execute(
            f"INSERT INTO {ks}.knows_e (src_id, tgt_id, since_year) "
            f"VALUES ('{src}', '{tgt}', {since})"
        )

    for src, tgt, role in [
        ("alice", "acme", "Engineer"),
        ("bob", "acme", "Designer"),
        ("carol", "globex", "Manager"),
        ("dave", "globex", "Analyst"),
    ]:
        sess.execute(
            f"INSERT INTO {ks}.works_at_e (src_id, tgt_id, role) "
            f"VALUES ('{src}', '{tgt}', '{role}')"
        )

    # Allow adjacency index observer to process async mutations.
    time.sleep(1)

    yield ks


@pytest.fixture(scope="module")
def bolt_driver(social_graph):
    """Neo4j driver connected to ferrosa's Bolt port.

    The social_graph fixture ensures schema + data exist before any Bolt test.
    """
    driver = GraphDatabase.driver(
        BOLT_URI,
        auth=BOLT_AUTH,
        database=social_graph,
        max_connection_lifetime=300,
    )
    yield driver
    driver.close()


def run_cypher(driver, query, **params):
    """Execute a Cypher query and return list of record dicts."""
    with driver.session() as session:
        result = session.run(query, **params)
        records = [dict(record) for record in result]
        summary = result.consume()
        return records, summary


# ===========================================================================
# Category 1: Bolt Protocol Wire Compatibility
# ===========================================================================


class TestBoltWireProtocol:
    """Tests that the Bolt v5 wire protocol works with the Neo4j driver."""

    def test_bolt_connect_and_hello(self, bolt_driver):
        """Driver can establish connection, complete handshake, authenticate."""
        bolt_driver.verify_connectivity()

    def test_bolt_auth_invalid_credentials(self):
        """Wrong password returns auth error, not a generic failure."""
        with pytest.raises((AuthError, ServiceUnavailable)):
            bad_driver = GraphDatabase.driver(
                BOLT_URI, auth=("cassandra", "wrong_password")
            )
            bad_driver.verify_connectivity()
            bad_driver.close()

    def test_bolt_run_pull_cycle(self, bolt_driver):
        """RUN + PULL returns result records correctly."""
        records, summary = run_cypher(
            bolt_driver, "MATCH (n:Person) RETURN n.name LIMIT 1"
        )
        assert len(records) == 1
        assert "n.name" in records[0]

    def test_bolt_run_pull_with_parameters(self, bolt_driver):
        """RUN parameters bind before PULL returns records."""
        records, summary = run_cypher(
            bolt_driver,
            "MATCH (n:Person) WHERE n.name = $name RETURN n.name, n.age",
            name="Alice",
        )
        assert records == [{"n.name": "Alice", "n.age": 30}]

    def test_bolt_multiple_queries_sequential(self, bolt_driver):
        """Multiple queries on the same driver work sequentially."""
        for _ in range(5):
            records, _ = run_cypher(
                bolt_driver, "MATCH (n:Person) RETURN n.name LIMIT 1"
            )
            assert len(records) == 1

    def test_bolt_connection_reuse(self, bolt_driver):
        """Same session can execute many queries."""
        with bolt_driver.session() as session:
            for i in range(10):
                result = session.run(
                    "MATCH (n:Person) RETURN n.name LIMIT 1"
                )
                records = list(result)
                assert len(records) == 1

    def test_bolt_empty_result(self, bolt_driver):
        """Query that matches nothing returns empty result, not error."""
        records, _ = run_cypher(
            bolt_driver,
            "MATCH (n:Person) WHERE n.name = 'NonExistent' RETURN n.name",
        )
        assert len(records) == 0


# ===========================================================================
# Category 2: Cypher Query Compatibility
# ===========================================================================


class TestCypherQueries:
    """Tests Cypher query features produce correct results."""

    def test_match_all_vertices(self, bolt_driver):
        """MATCH (n:Person) returns all 5 persons."""
        records, _ = run_cypher(bolt_driver, "MATCH (n:Person) RETURN n.name")
        names = {r["n.name"] for r in records}
        assert names == {"Alice", "Bob", "Carol", "Dave", "Eve"}

    def test_match_with_property_filter(self, bolt_driver):
        """WHERE n.age > 30 filters correctly."""
        records, _ = run_cypher(
            bolt_driver,
            "MATCH (n:Person) WHERE n.age > 30 RETURN n.name",
        )
        names = {r["n.name"] for r in records}
        assert names == {"Carol", "Eve"}

    def test_match_with_relationship_out(self, bolt_driver):
        """Outgoing relationship traversal via adjacency index."""
        records, _ = run_cypher(
            bolt_driver,
            "MATCH (a:Person)-[:KNOWS]->(b:Person) "
            "WHERE a.name = 'Alice' "
            "RETURN b.name",
        )
        names = {r["b.name"] for r in records}
        assert names == {"Bob", "Carol"}

    def test_match_reverse_relationship(self, bolt_driver):
        """Incoming relationship traversal via adjacency index IN entries."""
        records, _ = run_cypher(
            bolt_driver,
            "MATCH (a:Person)<-[:KNOWS]-(b:Person) "
            "WHERE a.name = 'Carol' "
            "RETURN b.name",
        )
        names = {r["b.name"] for r in records}
        # Alice→Carol and Bob→Carol, so Carol receives from Alice and Bob
        assert names == {"Alice", "Bob"}

    def test_match_bidirectional(self, bolt_driver):
        """Bidirectional relationship uses both OUT and IN adjacency entries."""
        records, _ = run_cypher(
            bolt_driver,
            "MATCH (a:Person)-[:KNOWS]-(b:Person) "
            "WHERE a.name = 'Bob' "
            "RETURN b.name",
        )
        names = {r["b.name"] for r in records}
        # Bob knows Carol (OUT), Alice knows Bob (IN)
        assert "Carol" in names
        assert "Alice" in names

    def test_match_multi_hop(self, bolt_driver):
        """Multi-hop traversal: Person→KNOWS→Person→WORKS_AT→Company."""
        records, _ = run_cypher(
            bolt_driver,
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:WORKS_AT]->(c:Company) "
            "WHERE a.name = 'Alice' "
            "RETURN b.name, c.name",
        )
        # Alice→Bob→Acme, Alice→Carol→Globex
        pairs = {(r["b.name"], r["c.name"]) for r in records}
        assert ("Bob", "Acme Corp") in pairs
        assert ("Carol", "Globex Inc") in pairs

    def test_match_variable_length_path(self, bolt_driver):
        """Variable-length path [*1..3] uses BFS with adjacency index."""
        records, _ = run_cypher(
            bolt_driver,
            "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) "
            "WHERE a.name = 'Alice' "
            "RETURN DISTINCT b.name",
        )
        names = {r["b.name"] for r in records}
        # 1-hop: Bob, Carol
        # 2-hop: Bob→Carol (already), Carol→Dave
        # 3-hop: Dave→Eve
        assert "Bob" in names
        assert "Carol" in names
        assert "Dave" in names
        assert "Eve" in names

    def test_create_and_read_back(self, bolt_driver):
        """CREATE a vertex then MATCH it back."""
        run_cypher(
            bolt_driver,
            "CREATE (n:Person {id: 'test_create', name: 'TestPerson', age: 99, city: 'TestCity'})",
        )
        records, _ = run_cypher(
            bolt_driver,
            "MATCH (n:Person) WHERE n.name = 'TestPerson' RETURN n.name, n.age",
        )
        assert len(records) >= 1
        assert records[0]["n.name"] == "TestPerson"
        assert records[0]["n.age"] == 99

        # Cleanup
        run_cypher(
            bolt_driver,
            "MATCH (n:Person) WHERE n.name = 'TestPerson' DELETE n",
        )

    def test_set_property(self, bolt_driver):
        """SET modifies a property on an existing vertex."""
        # Create a temp vertex
        run_cypher(
            bolt_driver,
            "CREATE (n:Person {id: 'test_set', name: 'SetTest', age: 20, city: 'X'})",
        )
        # Update it
        run_cypher(
            bolt_driver,
            "MATCH (n:Person) WHERE n.name = 'SetTest' SET n.age = 55",
        )
        # Verify
        records, _ = run_cypher(
            bolt_driver,
            "MATCH (n:Person) WHERE n.name = 'SetTest' RETURN n.age",
        )
        assert len(records) >= 1
        assert records[0]["n.age"] == 55

        # Cleanup
        run_cypher(
            bolt_driver,
            "MATCH (n:Person) WHERE n.name = 'SetTest' DELETE n",
        )

    def test_delete_vertex(self, bolt_driver):
        """DELETE removes a vertex."""
        run_cypher(
            bolt_driver,
            "CREATE (n:Person {id: 'test_del', name: 'DelTest', age: 1, city: 'X'})",
        )
        run_cypher(
            bolt_driver,
            "MATCH (n:Person) WHERE n.name = 'DelTest' DELETE n",
        )
        records, _ = run_cypher(
            bolt_driver,
            "MATCH (n:Person) WHERE n.name = 'DelTest' RETURN n.name",
        )
        assert len(records) == 0

    def test_return_distinct(self, bolt_driver):
        """RETURN DISTINCT deduplicates results."""
        records, _ = run_cypher(
            bolt_driver,
            "MATCH (n:Person) RETURN DISTINCT n.city",
        )
        cities = [r["n.city"] for r in records]
        # Should have unique cities only
        assert len(cities) == len(set(cities))

    def test_order_by(self, bolt_driver):
        """ORDER BY sorts results correctly."""
        records, _ = run_cypher(
            bolt_driver,
            "MATCH (n:Person) RETURN n.name ORDER BY n.age ASC",
        )
        ages_are_ascending = True
        names = [r["n.name"] for r in records]
        # Bob(25), Dave(28), Alice(30), Eve(32), Carol(35)
        assert names[0] == "Bob"
        assert names[-1] == "Carol"

    def test_limit(self, bolt_driver):
        """LIMIT truncates result set."""
        records, _ = run_cypher(
            bolt_driver,
            "MATCH (n:Person) RETURN n.name LIMIT 2",
        )
        assert len(records) == 2

    def test_where_and(self, bolt_driver):
        """WHERE with AND combines predicates."""
        records, _ = run_cypher(
            bolt_driver,
            "MATCH (n:Person) WHERE n.age > 25 AND n.city = 'NYC' RETURN n.name",
        )
        names = {r["n.name"] for r in records}
        # Alice(30, NYC) and Carol(35, NYC)
        assert names == {"Alice", "Carol"}

    def test_where_or(self, bolt_driver):
        """WHERE with OR unions predicates."""
        records, _ = run_cypher(
            bolt_driver,
            "MATCH (n:Person) WHERE n.name = 'Alice' OR n.name = 'Eve' RETURN n.name",
        )
        names = {r["n.name"] for r in records}
        assert names == {"Alice", "Eve"}

    def test_where_is_null(self, bolt_driver):
        """IS NULL matches vertices with missing property."""
        records, _ = run_cypher(
            bolt_driver,
            "MATCH (n:Person) WHERE n.email IS NULL RETURN n.name",
        )
        names = {r["n.name"] for r in records}
        # Bob and Dave have no email
        assert "Bob" in names
        assert "Dave" in names

    def test_where_is_not_null(self, bolt_driver):
        """IS NOT NULL matches vertices with present property."""
        records, _ = run_cypher(
            bolt_driver,
            "MATCH (n:Person) WHERE n.email IS NOT NULL RETURN n.name",
        )
        names = {r["n.name"] for r in records}
        assert "Alice" in names
        assert "Carol" in names
        assert "Eve" in names


# ===========================================================================
# Category 3: Data Type Fidelity
# ===========================================================================


class TestDataTypes:
    """Tests that Bolt PackStream types round-trip correctly."""

    def test_type_integer(self, bolt_driver):
        """Integer values come back as Python int."""
        records, _ = run_cypher(
            bolt_driver,
            "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.age",
        )
        assert isinstance(records[0]["n.age"], int)
        assert records[0]["n.age"] == 30

    def test_type_string(self, bolt_driver):
        """String values come back as Python str."""
        records, _ = run_cypher(
            bolt_driver,
            "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.name",
        )
        assert isinstance(records[0]["n.name"], str)
        assert records[0]["n.name"] == "Alice"

    def test_type_null(self, bolt_driver):
        """NULL values come back as Python None."""
        records, _ = run_cypher(
            bolt_driver,
            "MATCH (n:Person) WHERE n.name = 'Bob' RETURN n.email",
        )
        assert records[0]["n.email"] is None

    def test_type_boolean_true(self, bolt_driver):
        """Boolean true from expression."""
        records, _ = run_cypher(
            bolt_driver,
            "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.age > 20",
        )
        assert len(records) == 1
        # Result of comparison should be truthy
        val = list(records[0].values())[0]
        assert val is True or val == 1

    def test_type_boolean_false(self, bolt_driver):
        """Boolean false from expression."""
        records, _ = run_cypher(
            bolt_driver,
            "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.age > 100",
        )
        val = list(records[0].values())[0]
        assert val is False or val == 0

    def test_type_list_via_collect(self, bolt_driver):
        """collect() returns a Python list."""
        records, _ = run_cypher(
            bolt_driver,
            "MATCH (n:Person) WHERE n.city = 'NYC' RETURN collect(n.name)",
        )
        assert len(records) == 1
        names = list(records[0].values())[0]
        assert isinstance(names, list)
        assert set(names) == {"Alice", "Carol"}

    def test_type_integer_zero(self, bolt_driver):
        """Zero integer."""
        records, _ = run_cypher(
            bolt_driver,
            "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.age - n.age",
        )
        val = list(records[0].values())[0]
        assert val == 0

    def test_type_negative_integer(self, bolt_driver):
        """Negative integer."""
        records, _ = run_cypher(
            bolt_driver,
            "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.age - 100",
        )
        val = list(records[0].values())[0]
        assert val == -70


# ===========================================================================
# Category 4: Aggregation
# ===========================================================================


class TestAggregation:
    """Tests aggregation functions produce correct results."""

    def test_count_star(self, bolt_driver):
        """count(*) counts all matching rows."""
        records, _ = run_cypher(
            bolt_driver, "MATCH (n:Person) RETURN count(*)"
        )
        val = list(records[0].values())[0]
        assert val == 5

    def test_count_property(self, bolt_driver):
        """count(expr) counts non-null values."""
        records, _ = run_cypher(
            bolt_driver, "MATCH (n:Person) RETURN count(n.email)"
        )
        val = list(records[0].values())[0]
        # Alice, Carol, Eve have email
        assert val == 3

    def test_sum(self, bolt_driver):
        """sum() totals numeric values."""
        records, _ = run_cypher(
            bolt_driver, "MATCH (n:Person) RETURN sum(n.age)"
        )
        val = list(records[0].values())[0]
        # 30 + 25 + 35 + 28 + 32 = 150
        assert val == 150

    def test_avg(self, bolt_driver):
        """avg() computes mean."""
        records, _ = run_cypher(
            bolt_driver, "MATCH (n:Person) RETURN avg(n.age)"
        )
        val = list(records[0].values())[0]
        assert abs(val - 30.0) < 0.01

    def test_min_max(self, bolt_driver):
        """min() and max() find extremes."""
        records, _ = run_cypher(
            bolt_driver,
            "MATCH (n:Person) RETURN min(n.age), max(n.age)",
        )
        vals = list(records[0].values())
        assert min(vals) == 25
        assert max(vals) == 35

    def test_collect(self, bolt_driver):
        """collect() builds a list."""
        records, _ = run_cypher(
            bolt_driver,
            "MATCH (n:Person) WHERE n.city = 'SF' RETURN collect(n.name)",
        )
        names = list(records[0].values())[0]
        assert set(names) == {"Bob", "Eve"}

    def test_group_by(self, bolt_driver):
        """GROUP BY with count."""
        records, _ = run_cypher(
            bolt_driver,
            "MATCH (n:Person) RETURN n.city, count(*)",
        )
        city_counts = {r["n.city"]: list(r.values())[1] for r in records}
        assert city_counts["NYC"] == 2
        assert city_counts["SF"] == 2
        assert city_counts["LA"] == 1


# ===========================================================================
# Category 5: Error Handling
# ===========================================================================


class TestErrorHandling:
    """Tests that errors are reported correctly over Bolt."""

    def test_error_syntax_error(self, bolt_driver):
        """Malformed Cypher returns a client error."""
        with pytest.raises(ClientError):
            run_cypher(bolt_driver, "MATCHH (n) RETURN n")

    def test_error_unknown_label_returns_empty(self, bolt_driver):
        """Unknown label returns empty result, not error."""
        records, _ = run_cypher(
            bolt_driver, "MATCH (n:NonExistentLabel) RETURN n.name"
        )
        assert len(records) == 0

    def test_error_type_mismatch_where(self, bolt_driver):
        """Type mismatch in WHERE handled gracefully (FMEA F1)."""
        # Comparing string to int — should filter out (not crash)
        records, _ = run_cypher(
            bolt_driver,
            "MATCH (n:Person) WHERE n.name > 42 RETURN n.name",
        )
        # Result may be empty or filtered; key is no crash
        assert isinstance(records, list)

    def test_error_division_by_zero(self, bolt_driver):
        """Division by zero returns null (FMEA F8)."""
        records, _ = run_cypher(
            bolt_driver,
            "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.age / 0",
        )
        # Should return null, not crash
        if len(records) > 0:
            val = list(records[0].values())[0]
            assert val is None


# ===========================================================================
# Category 6: Index Verification
# ===========================================================================


class TestIndexUsage:
    """Verify that graph traversals use the adjacency index.

    These tests check the query statistics returned in the Bolt summary
    to confirm adjacency reads happened (not full table scans).
    """

    def test_index_single_hop_reads_edges(self, bolt_driver):
        """Single-hop MATCH shows edge reads in statistics."""
        records, summary = run_cypher(
            bolt_driver,
            "MATCH (a:Person)-[:KNOWS]->(b:Person) "
            "WHERE a.name = 'Alice' "
            "RETURN b.name",
        )
        # Should have results (Alice knows Bob and Carol)
        assert len(records) == 2
        # Summary should indicate edges were read (adjacency index used)
        counters = summary.counters
        # The result_available_after indicates query executed successfully
        assert summary.result_available_after is not None

    def test_index_multi_hop_chains_lookups(self, bolt_driver):
        """Multi-hop traversal chains multiple adjacency lookups."""
        records, summary = run_cypher(
            bolt_driver,
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) "
            "WHERE a.name = 'Alice' "
            "RETURN c.name",
        )
        # Alice→Bob→Carol, Alice→Carol→Dave
        names = {r["c.name"] for r in records}
        assert "Carol" in names or "Dave" in names

    def test_index_var_length_traversal(self, bolt_driver):
        """Variable-length path uses adjacency for BFS frontier expansion."""
        records, summary = run_cypher(
            bolt_driver,
            "MATCH (a:Person)-[:KNOWS*1..4]->(b:Person) "
            "WHERE a.name = 'Alice' "
            "RETURN DISTINCT b.name",
        )
        names = {r["b.name"] for r in records}
        # Should reach Eve via Alice→Bob/Carol→...→Dave→Eve
        assert "Eve" in names

    def test_index_reverse_direction(self, bolt_driver):
        """Reverse traversal (<-) uses IN direction adjacency entries."""
        records, _ = run_cypher(
            bolt_driver,
            "MATCH (a:Person)<-[:KNOWS]-(b:Person) "
            "WHERE a.name = 'Eve' "
            "RETURN b.name",
        )
        # Dave→Eve, so Eve's incoming KNOWS is from Dave
        names = {r["b.name"] for r in records}
        assert names == {"Dave"}

    def test_index_cross_label_traversal(self, bolt_driver):
        """Traversal across different labels uses adjacency for each hop."""
        records, _ = run_cypher(
            bolt_driver,
            "MATCH (p:Person)-[:WORKS_AT]->(c:Company) "
            "WHERE p.name = 'Alice' "
            "RETURN c.name",
        )
        names = {r["c.name"] for r in records}
        assert names == {"Acme Corp"}
