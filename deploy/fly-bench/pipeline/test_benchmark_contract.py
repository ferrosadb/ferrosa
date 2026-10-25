"""Keep invalid results out of the benchmark's timing report.

Correctness: missing, wrong, duplicate and incomplete reads are rejected.
Last revised: 2026-10-24
Last changed: Added negative controls for the recovered Fly instrument.
"""

import types
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
        return [(pk,) for pk in range(limit)]


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


if __name__ == "__main__":
    unittest.main()
