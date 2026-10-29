"""Driver-free contracts for the measured scan delivery probe."""
import ast
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).parent
TREE = ast.parse((ROOT / 'profile_operation_probe.py').read_text())
MAIN = next(n for n in TREE.body if isinstance(n, ast.FunctionDef) and n.name == 'main')
SCAN = next(n for n in ast.walk(MAIN) if isinstance(n, ast.FunctionDef) and n.name == 'scan_delivery')


def scan_delivery(arguments, adapter, timestamps):
    ticks = iter(timestamps)
    scope = {'a': arguments, 'adapter': adapter,
             'scan_limit': min(arguments.rows, arguments.scan_limit or arguments.rows),
             'time': SimpleNamespace(perf_counter=lambda: next(ticks))}
    exec(compile(ast.Module(body=[SCAN], type_ignores=[]), 'scan_delivery', 'exec'), scope)
    return scope['scan_delivery']()


class ScanDeliveryContract(unittest.TestCase):
    def test_cql_consumes_two_pages_and_separates_first_page_from_drain(self):
        class Statement:
            def __init__(self, query, **options):
                self.query, self.options = query, options

        class Pages:
            index = 0
            data = [[(i, i % 64, 'x') for i in range(5000)],
                    [(i, i % 64, 'x') for i in range(5000, 10000)]]

            @property
            def current_rows(self):
                return self.data[self.index]

            @property
            def has_more_pages(self):
                return self.index + 1 < len(self.data)

            def fetch_next_page(self):
                self.index += 1

        calls = []

        def execute(statement, **_options):
            calls.append(statement)
            return Pages()

        cassandra = ModuleType('cassandra')
        cassandra.ConsistencyLevel = SimpleNamespace(ONE=1)
        query = ModuleType('cassandra.query')
        query.SimpleStatement = Statement
        arguments = SimpleNamespace(rows=20000, scan_limit=10000, explicit_scan_timing=True,
                                    protocol='cql', scope='scan_contract')
        with patch.dict(sys.modules, {'cassandra': cassandra, 'cassandra.query': query}):
            result = scan_delivery(arguments, SimpleNamespace(session=SimpleNamespace(execute=execute)), [0, 2, 5])
        rows, total, first, drain, pages = result
        self.assertEqual([row[0] for row in rows], list(range(10000)))
        self.assertEqual((total, first, drain, pages), (5, 2, 3, 2))
        self.assertEqual(calls[0].query,
                         'SELECT pk, bucket, payload FROM scan_contract.scan_contract LIMIT 10000')
        self.assertEqual(calls[0].options, {'fetch_size': 5000, 'consistency_level': 1})

    def test_postgres_stream_separates_first_row_from_drain(self):
        calls = []

        class Cursor:
            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

            def stream(self, query, **options):
                calls.append((query, options))
                yield (0, 0, 'x')
                yield (1, 1, 'x')

        arguments = SimpleNamespace(rows=2, scan_limit=None, explicit_scan_timing=True,
                                    protocol='pg', scope='scan_contract')
        result = scan_delivery(arguments, SimpleNamespace(_conn=lambda: SimpleNamespace(cursor=Cursor)), [0, 1, 4])
        self.assertEqual(result, ([(0, 0, 'x'), (1, 1, 'x')], 4, 1, 3, None))
        self.assertEqual(calls, [('SELECT pk, bucket, payload FROM scan_contract LIMIT 3', {'size': 1})])

    def test_barrier_matches_one_and_eight_reader_controls(self):
        barrier = next(n for n in ast.walk(MAIN) if isinstance(n, ast.Call)
                       and isinstance(n.func, ast.Attribute) and n.func.attr == 'Barrier')
        expression = compile(ast.Expression(body=barrier.args[0]), 'barrier parties', 'eval')
        for workers in [1, 8]:
            self.assertEqual(eval(expression, {'a': SimpleNamespace(concurrency=workers)}), workers + 1)


if __name__ == '__main__':
    unittest.main()
