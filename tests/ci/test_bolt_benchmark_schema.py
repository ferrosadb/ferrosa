"""Require explicit, indexed Bolt benchmark fixtures before timing.

Correctness: missing, failed and mismatched indexes reject a benchmark run.
Last revised: 2026-10-26
Last changed: Add schema-readiness contracts for the Bolt comparison harness.
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

    def single(self):
        return self.rows[0] if self.rows else None

    def consume(self):
        return None

    def __iter__(self):
        return iter(self.rows)


class Driver:
    def __init__(self):
        self.calls = []
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
        return Result(self.rows if query.startswith("SHOW INDEXES") else [])


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

    def test_ferrosa_declares_shared_storage_indexes_through_cql(self):
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
            self.assertTrue(
                any("'graph.label': 'BenchRow'" in query for query in statements)
            )
            indexes = [
                query for query in statements if query.startswith("CREATE INDEX")
            ]
            self.assertEqual(len(indexes), 3)
            cql.return_value.close.assert_called_once()


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
