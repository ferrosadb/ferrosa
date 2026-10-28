"""Require ready Bolt benchmark fixtures before timing.

Correctness: missing, failed and mismatched indexes reject a benchmark run.
Last revised: 2026-10-26
Last changed: Exercise Ferrosa's ordinary Bolt schema/index bootstrap path.
"""

import importlib.util
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch


SOURCE = (
    Path(__file__).resolve().parents[2]
    / "deploy/fly-bench/pipeline/validated_multimetric_bench.py"
)
SPEC = importlib.util.spec_from_file_location("bolt_benchmark", SOURCE)
BENCH = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BENCH)


class Result:
    def __init__(self, rows):
        self.rows = rows
        self.consumed = False

    def single(self):
        return self.rows[0] if self.rows else None

    def consume(self):
        self.consumed = True
        return None

    def __iter__(self):
        return iter(self.rows)


class Driver:
    def __init__(self):
        self.calls = []
        self.results = []
        self.rows = [
            {
                "name": f"bench_{prop}",
                "state": "ONLINE",
                "type": "RANGE",
                "labelsOrTypes": ["BenchRow"],
                "properties": [prop],
            }
            for prop in ("pk", "bucket", "payload")
        ]
        self.fail_ddl = False

    def session(self, **_kwargs):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def run(self, query, **_kwargs):
        self.calls.append(query)
        if self.fail_ddl and query.startswith("CREATE INDEX"):
            raise RuntimeError("DDL rejected")
        result = Result(self.rows if query.startswith("SHOW INDEXES") else [])
        self.results.append(result)
        return result


class BoltBenchmarkSchema(unittest.TestCase):
    def setUp(self):
        self.driver = Driver()
        self.adapter = object.__new__(BENCH.BoltAdapter)
        self.adapter.driver = self.driver
        self.adapter.database = None
        self.adapter.args = SimpleNamespace(bolt_backend="neo4j")

    def test_all_fixture_properties_are_indexed(self):
        self.adapter.ensure_schema()
        ddl = [query for query in self.driver.calls if query.startswith("CREATE INDEX")]
        self.assertEqual(len(ddl), 3)
        for prop in ("pk", "bucket", "payload"):
            self.assertTrue(any(f"ON (n.{prop})" in query for query in ddl))

    def test_ddl_errors_abort_the_run(self):
        self.driver.fail_ddl = True
        with self.assertRaisesRegex(RuntimeError, "DDL rejected"):
            self.adapter.ensure_schema()

    def test_missing_index_aborts_the_run(self):
        self.driver.rows.pop()
        with self.assertRaises(RuntimeError):
            self.adapter.ensure_schema()

    def test_populating_index_aborts_the_run(self):
        self.driver.rows[-1]["state"] = "POPULATING"
        with self.assertRaises(RuntimeError):
            self.adapter.ensure_schema()

    def test_wrong_property_aborts_the_run(self):
        self.driver.rows[-1]["properties"] = ["wrong"]
        with self.assertRaises(RuntimeError):
            self.adapter.ensure_schema()

    def ferrosa_setup(self):
        self.adapter.args = SimpleNamespace(
            bolt_backend="ferrosa",
            keyspace="bench",
            bolt_database=None,
            cql_port=9042,
            cql_user=None,
            cql_password=None,
        )
        with patch.object(BENCH, "CqlAdapter") as cql:
            self.adapter.ensure_schema()
            cql.assert_called_once()
            statements = [
                call.args[0] for call in cql.return_value.session.execute.call_args_list
            ]
            cql.return_value.close.assert_called_once()
        return statements

    def test_ferrosa_default_setup_does_not_predeclare_tables_or_indexes(self):
        statements = self.ferrosa_setup()
        self.assertTrue(any(query.startswith("CREATE KEYSPACE") for query in statements))
        ddl = [
            query.upper().strip()
            for query in statements
            if query.upper().strip().startswith(("CREATE ", "ALTER ", "DROP "))
        ]
        self.assertTrue(
            all(query.startswith("CREATE KEYSPACE") for query in ddl),
            f"Ferrosa's default Bolt benchmark must derive its table/indexes through Bolt: {ddl}",
        )

    def test_ferrosa_bootstraps_with_ordinary_bolt_create_then_removes_its_sentinel(self):
        self.ferrosa_setup()
        creates = [
            (i, query)
            for i, query in enumerate(self.driver.calls)
            if query.strip().startswith("CREATE (") and ":BenchRow" in query
        ]
        self.assertEqual(len(creates), 1, "bootstrap must exercise an ordinary Bolt CREATE")
        create_at, create = creates[0]
        for prop in ("pk", "bucket", "payload"):
            self.assertRegex(create, rf"\b{prop}\s*:")
        self.assertTrue(self.driver.results[create_at].consumed)
        deletes = [
            (i, query)
            for i, query in enumerate(self.driver.calls)
            if ":BenchRow" in query and "DELETE" in query.upper()
        ]
        self.assertEqual(len(deletes), 1, "remove the owned bootstrap row before timing")
        delete_at, delete = deletes[0]
        self.assertGreater(delete_at, create_at)
        self.assertRegex(delete, r"(?:\{\s*pk\s*:|WHERE\s+\w+\.pk\s*=)")
        self.assertTrue(self.driver.results[delete_at].consumed)
        self.assertFalse(any(query.startswith("CREATE INDEX") for query in self.driver.calls))


class BenchmarkScanValues(unittest.TestCase):
    def test_correct_keys_do_not_hide_incorrect_scan_payloads(self):
        payloads = BENCH.make_payloads(512)
        adapter = SimpleNamespace(
            count=lambda: 2,
            point_read=lambda pk: (pk, pk % 64, payloads[pk % len(payloads)]),
            scan=lambda _limit: [(0, 0, "wrong"), (1, 1, payloads[1])],
        )
        args = SimpleNamespace(repeats=1, read_samples=2, seed=123)
        with self.assertRaisesRegex(ValueError, "incorrect"):
            BENCH.measure(adapter, args, 2, 2, None)


if __name__ == "__main__":
    unittest.main()
