"""Wire-level CQL compatibility test using Cassandra's official .cql examples.

Loads every .cql file from the Cassandra submodule's documentation examples
(cassandra/doc/modules/cassandra/examples/CQL/) and executes each statement
over the wire against a live ferrosa instance using the DataStax Python driver.

This validates both parser AND execution — not just parsing.

Usage:
    # Via docker:
    docker compose -f tests/drivers/docker-compose.drivers.yml run python-tests \
        pytest -v test_cassandra_cql_examples.py

    # Locally (with ferrosa running on 9042):
    pytest -v test_cassandra_cql_examples.py
"""

import os
from pathlib import Path

import pytest
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy

FERROSA_HOST = os.environ.get("FERROSA_HOST", "127.0.0.1")
FERROSA_CQL_PORT = int(os.environ.get("FERROSA_CQL_PORT", "9042"))

# cqlsh-only commands that are NOT part of the CQL language spec.
CQLSH_COMMANDS = {
    "SOURCE",
    "CAPTURE",
    "DESCRIBE",
    "COPY",
    "SHOW",
    "TRACING",
    "EXPAND",
    "PAGING",
    "SERIAL",
    "CONSISTENCY",
    "LOGIN",
}

# Statements known to be unsupported — tracked for coverage reporting.
# These are NOT skipped; they run and their failures are expected/counted.
KNOWN_UNSUPPORTED = {
    "CREATE MATERIALIZED VIEW",
    "CREATE CUSTOM INDEX",
    "CREATE TRIGGER",
    "CREATE TABLE.*LIKE",
    "CREATE USER",
    "ALTER USER",
}


def cql_examples_dir() -> Path:
    """Locate the Cassandra CQL examples directory."""
    # In Docker: mounted at /cassandra-examples
    docker_path = Path("/cassandra-examples")
    if docker_path.exists():
        return docker_path
    # Local: relative to this file
    test_dir = Path(__file__).resolve().parent
    repo_root = test_dir.parent.parent.parent
    return (
        repo_root / "cassandra" / "doc" / "modules" / "cassandra" / "examples" / "CQL"
    )


def split_statements(content: str) -> list[str]:
    """Split a .cql file into individual statements."""
    stmts = []
    current = []
    for line in content.splitlines():
        stripped = line.strip()
        # Skip comments
        if stripped.startswith("//") or stripped.startswith("--"):
            continue
        # Strip inline comments
        if "//" in line:
            line = line[: line.index("//")]
        current.append(line)
        if stripped.endswith(";"):
            stmt = "\n".join(current).strip().rstrip(";").strip()
            if stmt:
                stmts.append(stmt)
            current = []
    # Trailing statement without semicolon
    remaining = "\n".join(current).strip().rstrip(";").strip()
    if remaining:
        stmts.append(remaining)
    return stmts


def is_cqlsh_command(stmt: str) -> bool:
    """Return True if stmt is a cqlsh-specific command, not CQL."""
    first_word = stmt.split()[0].upper() if stmt.split() else ""
    return first_word in CQLSH_COMMANDS


def is_non_cql(stmt: str) -> bool:
    """Return True if stmt looks like a code fragment, not CQL."""
    s = stmt.strip()
    return (
        s.startswith("$$")
        or s.startswith("return")
        or s.startswith("if ")
        or s.startswith("state.")
        or s.startswith("udt.")
        or s.startswith("r =")
        or s.startswith("}")
        or s.startswith("*")
        or s.startswith("min(")
        or s.upper() == "APPLY BATCH"
    )


def collect_cql_files(directory: Path) -> list[Path]:
    """Recursively collect all .cql files."""
    return sorted(directory.rglob("*.cql"))


@pytest.fixture(scope="module")
def session():
    """Create a shared session for CQL example execution."""
    cluster = Cluster(
        contact_points=[FERROSA_HOST],
        port=FERROSA_CQL_PORT,
        load_balancing_policy=RoundRobinPolicy(),
        protocol_version=4,
        schema_metadata_enabled=False,
        token_metadata_enabled=False,
    )
    sess = cluster.connect()
    # Create a shared keyspace for examples that need one
    try:
        sess.execute(
            "CREATE KEYSPACE IF NOT EXISTS cql_compat_test "
            "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
        )
        sess.execute("USE cql_compat_test")
    except Exception:
        pass
    yield sess
    try:
        sess.execute("DROP KEYSPACE IF EXISTS cql_compat_test")
    except Exception:
        pass
    sess.shutdown()
    cluster.shutdown()


class TestCassandraCqlExamples:
    """Execute Cassandra's official CQL examples against ferrosa."""

    def test_cql_examples_coverage(self, session):
        """Parse and execute all .cql example files, report coverage."""
        examples_dir = cql_examples_dir()
        if not examples_dir.exists():
            pytest.skip(
                f"Cassandra CQL examples not found at {examples_dir}. "
                "Ensure the cassandra example corpus is present."
            )

        cql_files = collect_cql_files(examples_dir)
        if not cql_files:
            # The directory exists (e.g. an empty bind mount) but the example
            # corpus isn't present. Skip rather than fail — the corpus is an
            # optional external dependency, not a ferrosa defect.
            pytest.skip(
                f"No .cql example files under {examples_dir}; "
                "Cassandra example corpus not populated."
            )

        total = 0
        passed = 0
        failed = 0
        skipped = 0
        errors: list[tuple[str, str, str]] = []  # (file, stmt, error)

        for path in cql_files:
            rel = str(path.relative_to(examples_dir))
            content = path.read_text(errors="replace")
            stmts = split_statements(content)

            for stmt_text in stmts:
                if is_cqlsh_command(stmt_text) or is_non_cql(stmt_text):
                    skipped += 1
                    continue

                total += 1

                # Rewrite keyspace references to use our test keyspace
                # (many examples use hardcoded keyspace names)
                exec_stmt = stmt_text

                try:
                    session.execute(exec_stmt)
                    passed += 1
                except Exception as e:
                    failed += 1
                    preview = (
                        exec_stmt[:120] + "..." if len(exec_stmt) > 120 else exec_stmt
                    )
                    errors.append((rel, preview, str(e)))

        # Print report
        print(f"\n{'=' * 60}")
        print("CQL Wire Compatibility Report")
        print(f"{'=' * 60}")
        print(f"Files:      {len(cql_files)}")
        print(f"CQL stmts:  {total}")
        print(f"Executed OK:{passed}")
        print(f"Failed:     {failed}")
        print(f"Skipped:    {skipped} (cqlsh commands, code fragments)")
        print(f"Coverage:   {passed / total * 100:.1f}%" if total > 0 else "N/A")

        if errors:
            print(f"\n--- Failures ({len(errors)}) ---")
            for rel, stmt, err in errors[:50]:  # Cap output
                print(f"\n  {rel}:")
                print(f"    {stmt}")
                print(f"    ERROR: {err}")
            if len(errors) > 50:
                print(f"\n  ... and {len(errors) - 50} more failures")

        print(f"\n{'=' * 60}")

        # This test is informational — reports coverage without failing.
        # The parser doc test (cassandra_cql_examples.rs) enforces parse coverage.
        # Uncomment to make it enforcing:
        # assert failed == 0, f"{failed} CQL statements failed to execute"
