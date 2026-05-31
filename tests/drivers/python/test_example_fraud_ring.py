"""Example: Fraud Ring Detection with Cypher Graph Traversals.

Demonstrates why graph queries are essential for financial crime detection.
Traditional SQL/CQL cannot express "find all accounts within 3 hops of a
suspicious transaction" without N+1 queries. Cypher solves this in one query.

Business value:
- Detect coordinated fraud rings (A sends to B sends to C sends back to A)
- Find money laundering chains (layered transactions through shell accounts)
- Identify unusual patterns (same amount, rapid succession, circular flow)

This example creates a financial network, seeds suspicious transactions,
then uses Cypher to detect patterns that would be invisible to row-level queries.

Requires: Ferrosa with FERROSA_GRAPH_ENABLED=true, Bolt port 7687.
"""

import os
import time

import pytest
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy

FERROSA_HOST = os.environ.get("FERROSA_HOST", "127.0.0.1")
FERROSA_CQL_PORT = int(os.environ.get("FERROSA_CQL_PORT", "9042"))
FERROSA_BOLT_PORT = int(os.environ.get("FERROSA_BOLT_PORT", "7687"))

KEYSPACE = "fraud_detection"


@pytest.fixture(scope="module")
def cql_session():
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
def fraud_graph(cql_session):
    """Build the financial network schema and seed data."""
    sess = cql_session

    sess.execute(
        f"CREATE KEYSPACE IF NOT EXISTS {KEYSPACE} "
        "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )

    # Accounts (vertices)
    sess.execute(
        f"CREATE TABLE IF NOT EXISTS {KEYSPACE}.account_v ("
        "  id TEXT PRIMARY KEY,"
        "  name TEXT,"
        "  account_type TEXT,"
        "  risk_score INT,"
        "  country TEXT"
        ") WITH extensions = {"
        "  'graph.type': 'vertex',"
        "  'graph.label': 'Account'"
        "}"
    )

    # Transactions (edges)
    sess.execute(
        f"CREATE TABLE IF NOT EXISTS {KEYSPACE}.transfer_e ("
        "  src_id TEXT,"
        "  tgt_id TEXT,"
        "  amount DECIMAL,"
        "  currency TEXT,"
        "  timestamp TIMESTAMP,"
        "  PRIMARY KEY (src_id, tgt_id)"
        ") WITH extensions = {"
        "  'graph.type': 'edge',"
        "  'graph.label': 'TRANSFER',"
        "  'graph.source': 'src_id',"
        "  'graph.target': 'tgt_id',"
        "  'graph.source_label': 'Account',"
        "  'graph.target_label': 'Account'"
        "}"
    )

    # Seed accounts: 3 legitimate, 4 in a fraud ring, 2 shell companies
    accounts = [
        ("acct_legit_1", "Alice Corp", "business", 10, "US"),
        ("acct_legit_2", "Bob Industries", "business", 5, "US"),
        ("acct_legit_3", "Carol Services", "business", 8, "UK"),
        ("acct_ring_1", "Shell Alpha LLC", "business", 85, "CY"),
        ("acct_ring_2", "Omega Holdings", "business", 72, "VG"),
        ("acct_ring_3", "Sigma Investments", "business", 90, "PA"),
        ("acct_ring_4", "Delta Consulting", "business", 78, "CY"),
        ("acct_shell_1", "Phantom Corp", "shell", 95, "VG"),
        ("acct_shell_2", "Ghost LLC", "shell", 92, "KY"),
    ]

    for aid, name, atype, risk, country in accounts:
        sess.execute(
            f"INSERT INTO {KEYSPACE}.account_v (id, name, account_type, risk_score, country) "
            f"VALUES ('{aid}', '{name}', '{atype}', {risk}, '{country}')"
        )

    # Seed transactions: legitimate + fraud ring (circular flow)
    transfers = [
        # Legitimate transactions
        ("acct_legit_1", "acct_legit_2", 50000, "USD"),
        ("acct_legit_2", "acct_legit_3", 25000, "GBP"),
        # Fraud ring: circular flow A→B→C→D→A
        ("acct_ring_1", "acct_ring_2", 100000, "USD"),
        ("acct_ring_2", "acct_ring_3", 99500, "USD"),
        ("acct_ring_3", "acct_ring_4", 99000, "USD"),
        ("acct_ring_4", "acct_ring_1", 98500, "USD"),  # back to start!
        # Layering through shell companies
        ("acct_ring_1", "acct_shell_1", 200000, "USD"),
        ("acct_shell_1", "acct_shell_2", 195000, "USD"),
        ("acct_shell_2", "acct_ring_3", 190000, "USD"),
    ]

    for src, tgt, amount, currency in transfers:
        sess.execute(
            f"INSERT INTO {KEYSPACE}.transfer_e (src_id, tgt_id, amount, currency) "
            f"VALUES ('{src}', '{tgt}', {amount}, '{currency}')"
        )

    time.sleep(1)  # adjacency index settle
    yield KEYSPACE


@pytest.fixture(scope="module")
def bolt_driver(fraud_graph):
    from neo4j import GraphDatabase
    driver = GraphDatabase.driver(
        f"bolt://{FERROSA_HOST}:{FERROSA_BOLT_PORT}",
        auth=("cassandra", "cassandra"),
        database=fraud_graph,
    )
    yield driver
    driver.close()


def run_cypher(driver, query):
    with driver.session() as session:
        result = session.run(query)
        return [dict(r) for r in result]


class TestFraudRingDetection:
    """Detect fraud patterns using Cypher graph traversals."""

    def test_find_high_risk_accounts(self, bolt_driver):
        """Find all accounts with risk score above 70 — simple vertex filter."""
        records = run_cypher(
            bolt_driver,
            "MATCH (a:Account) WHERE a.risk_score > 70 RETURN a.name, a.risk_score "
            "ORDER BY a.risk_score DESC",
        )
        names = [r["a.name"] for r in records]
        assert "Phantom Corp" in names
        assert "Sigma Investments" in names
        assert len(records) >= 5  # all ring + shell accounts

    def test_find_circular_transfers(self, bolt_driver):
        """Find accounts reachable via transfer chains (fraud ring indicator)."""
        # From ring_1, follow TRANSFER edges up to 4 hops — should reach
        # ring_1 again (circular flow).
        records = run_cypher(
            bolt_driver,
            "MATCH (start:Account)-[:TRANSFER*1..4]->(dest:Account) "
            "WHERE start.name = 'Shell Alpha LLC' "
            "RETURN DISTINCT dest.name",
        )
        names = {r["dest.name"] for r in records}
        # Should find the full ring: Omega, Sigma, Delta, and back to Alpha
        assert "Omega Holdings" in names
        assert "Sigma Investments" in names

    def test_shell_company_layering(self, bolt_driver):
        """Detect money flowing through shell companies (layering pattern)."""
        records = run_cypher(
            bolt_driver,
            "MATCH (a:Account)-[:TRANSFER]->(shell:Account)-[:TRANSFER]->(b:Account) "
            "WHERE shell.account_type = 'shell' "
            "RETURN a.name, shell.name, b.name",
        )
        assert len(records) >= 1
        # Shell Alpha → Phantom Corp → Ghost LLC is a layering chain

    def test_cross_border_transfers(self, bolt_driver):
        """Find transfers between accounts in different countries."""
        records = run_cypher(
            bolt_driver,
            "MATCH (src:Account)-[:TRANSFER]->(dst:Account) "
            "WHERE src.country = 'CY' "
            "RETURN src.name, dst.name, dst.country",
        )
        assert len(records) >= 1
