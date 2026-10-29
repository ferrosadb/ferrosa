"""Keep invalid results out of the benchmark's timing report.

Correctness: missing, wrong, duplicate and incomplete reads are rejected.
Last revised: 2026-10-25
Last changed: Pin output payload bytes independently from payload variant count.
"""

import types
import json
import sys
import tempfile
from pathlib import Path
from unittest import mock

import workload
import unittest

from validated_multimetric_bench import make_payloads, measure


class Adapter:
    def __init__(self):
        self.payloads = make_payloads(512)
        self.keys = []

    def count(self):
        return 8

    def count_seconds(self):
        # Retain the original adapter contract for the negative control, so
        # failures reflect missing validation rather than a missing fixture API.
        return 0.001

    def point_read(self, pk):
        self.keys.append(pk)
        return pk, pk % 64, self.payloads[pk % len(self.payloads)]

    def scan(self, limit):
        return [(pk, pk % 64, self.payloads[pk % len(self.payloads)]) for pk in range(limit)]


class ContractTests(unittest.TestCase):
    def setUp(self):
        self.args = types.SimpleNamespace(repeats=2, read_samples=12, seed=1234)

    def test_valid_values_and_repeatable_key_schedule(self):
        first, second = Adapter(), Adapter()
        result = measure(first, self.args, 8, 8, None)
        measure(second, self.args, 8, 8, None)
        self.assertEqual(result["scan_rows"], 8)
        self.assertEqual(result["read_samples"], 12)
        self.assertEqual(first.keys, second.keys)

    def test_missing_and_incorrect_point_values_are_rejected(self):
        for received in [None, (9, 0, "wrong"), (0, 0, "wrong")]:
            with self.subTest(received=received):
                adapter = Adapter()
                adapter.point_read = lambda pk: received
                with self.assertRaisesRegex(ValueError, "point read"):
                    measure(adapter, self.args, 8, 8, None)

    def test_empty_short_duplicate_and_out_of_range_scans_are_rejected(self):
        for scanned in [[], [(0,)], [(0,)] * 8, [(i,) for i in range(1, 9)]]:
            with self.subTest(scanned=scanned):
                adapter = Adapter()
                adapter.scan = lambda limit: scanned
                with self.assertRaisesRegex(ValueError, "scan"):
                    measure(adapter, self.args, 8, 8, None)

    def test_wrong_initial_census_is_rejected(self):
        for actual in [0, 7, 9]:
            with self.assertRaisesRegex(ValueError, "census"):
                measure(Adapter(), self.args, 8, actual, None)

    def test_count_drift_is_rejected(self):
        adapter = Adapter()
        adapter.count = lambda: 7
        with self.assertRaisesRegex(ValueError, "count changed"):
            measure(adapter, self.args, 8, 8, None)


    def test_main_reports_bytes_from_payloads_instead_of_variant_count(self):
        adapter = Adapter()
        adapter.payloads = make_payloads(7)
        adapter.close = lambda: None
        state = {"rows": 0}
        fixture = types.SimpleNamespace(
            payloads=adapter.payloads, scope="MetadataFixture", adapter=adapter,
            setup=lambda: None, count=lambda: state["rows"],
            scan_and_validate=lambda n: {"rows": n}, plan=lambda: [],
            read=adapter.point_read,
        )
        adapter.args = types.SimpleNamespace(table="metadata_fixture")
        def validate(row, pk):
            self.assertEqual(row, (pk, pk % 64, adapter.payloads[pk % 7]))
        fixture.validate_row = validate
        def seed(*args):
            state["rows"] = 8
            return 1.0
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "result.json"
            argv = ["workload.py", "--runtime-config", "unused.json", "--arm", "E",
                    "--round", "1", "--protocol", "pg", "--revision", "fixture",
                    "--image", "fixture", "--rows", "8", "--reads", "8",
                    "--concurrency", "1", "--out", str(output)]
            with mock.patch.object(sys, "argv", argv), \
                 mock.patch.object(workload, "Workload", return_value=fixture), \
                 mock.patch.object(workload.instrument, "load", side_effect=seed), \
                 mock.patch.object(workload.faulthandler, "dump_traceback_later"):
                workload.main()
            result = json.loads(output.read_text())
        self.assertTrue(result["validated"])
        self.assertEqual(result["payload_variants"], 7)
        self.assertEqual(result["payload_bytes"], len(adapter.payloads[0].encode("utf-8")))
        self.assertNotEqual(result["payload_bytes"], result["payload_variants"])


if __name__ == "__main__":
    unittest.main()
