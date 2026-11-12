"""RED contract for the future stdlib compaction experiment analyzer.

summarize_probe returns {shape: {samples: int, metrics: {field: stats}}}.
summarize_phase returns populated op names plus ``combined``, each containing
count, throughput_ops_per_second, and latency_ms (p50/p95/p99/p999).
All evidence validation failures raise ValueError; no bad row may be dropped.
"""

import importlib.util
import io
import json
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
import sys
import unittest
from unittest.mock import mock_open, patch


# Test list:
# - Probe shape isolation, odd/even medians, extrema, and singleton evidence.
# - Reject empty/incomplete probes, invalid times, and non-True correctness.
# - Known nearest-rank tails, ties, small samples, and pooled raw latencies.
# - Phase throughput uses wall time; reject count gaps, errors, and mismatches.
# - Reject incomplete phase rows, unknown ops, invalid latencies/durations.

ROOT = Path(__file__).resolve().parents[2]
ANALYZER_PATH = ROOT / "deploy/fly-compaction-explore/analyze.py"
MODULE_NAME = "ferrosa_compaction_experiment_analysis"
SPEC = importlib.util.spec_from_file_location(MODULE_NAME, ANALYZER_PATH)
analyze = importlib.util.module_from_spec(SPEC)
sys.modules[MODULE_NAME] = analyze
SPEC.loader.exec_module(analyze)


def probe_sample(**overrides):
    sample = {
        "shape": "wide",
        "elapsed_ms": 10,
        "peak_live_delta_bytes": 100,
        "alloc_events": 5,
        "input_bytes": 1000,
        "output_bytes": 500,
        "correctness_ok": True,
    }
    sample.update(overrides)
    return sample


def phase_sample(op="read", latency_ms=1, **overrides):
    sample = {"op": op, "latency_ms": latency_ms, "ok": True, "mismatch": False}
    sample.update(overrides)
    return sample


class ProbeSummaryTests(unittest.TestCase):
    def test_groups_shapes_and_reports_known_medians_and_extrema(self):
        samples = [
            probe_sample(elapsed_ms=90, peak_live_delta_bytes=400,
                         alloc_events=12, input_bytes=9000, output_bytes=900),
            probe_sample(shape="narrow", elapsed_ms=3,
                         peak_live_delta_bytes=30, alloc_events=1,
                         input_bytes=100, output_bytes=50),
            probe_sample(elapsed_ms=2, peak_live_delta_bytes=100,
                         alloc_events=2, input_bytes=1000, output_bytes=500),
            probe_sample(shape="narrow", elapsed_ms=8,
                         peak_live_delta_bytes=70, alloc_events=4,
                         input_bytes=300, output_bytes=80),
            probe_sample(elapsed_ms=10, peak_live_delta_bytes=900,
                         alloc_events=8, input_bytes=3000, output_bytes=100),
        ]

        result = analyze.summarize_probe(samples)

        self.assertEqual(set(result), {"wide", "narrow"})
        self.assertEqual(result["wide"], {
            "samples": 3,
            "metrics": {
                "elapsed_ms": {"median": 10, "min": 2, "max": 90},
                "peak_live_delta_bytes": {"median": 400, "max": 900},
                "alloc_events": {"median": 8},
                "input_bytes": {"median": 3000},
                "output_bytes": {"median": 500},
            },
        })
        self.assertEqual(result["narrow"], {
            "samples": 2,
            "metrics": {
                "elapsed_ms": {"median": 5.5, "min": 3, "max": 8},
                "peak_live_delta_bytes": {"median": 50, "max": 70},
                "alloc_events": {"median": 2.5},
                "input_bytes": {"median": 200},
                "output_bytes": {"median": 65},
            },
        })

    def test_singleton_keeps_zero_allocation_and_byte_measurements(self):
        result = analyze.summarize_probe([
            probe_sample(elapsed_ms=0.25, peak_live_delta_bytes=0,
                         alloc_events=0, input_bytes=0, output_bytes=0)
        ])
        self.assertEqual(result["wide"], {
            "samples": 1,
            "metrics": {
                "elapsed_ms": {"median": 0.25, "min": 0.25, "max": 0.25},
                "peak_live_delta_bytes": {"median": 0, "max": 0},
                "alloc_events": {"median": 0},
                "input_bytes": {"median": 0},
                "output_bytes": {"median": 0},
            },
        })

    def test_rejects_empty_evidence(self):
        with self.assertRaises(ValueError):
            analyze.summarize_probe([])

    def test_rejects_each_missing_required_field_even_after_a_valid_row(self):
        for field in probe_sample():
            with self.subTest(field=field):
                incomplete = probe_sample()
                del incomplete[field]
                with self.assertRaises(ValueError):
                    analyze.summarize_probe([probe_sample(), incomplete])

    def test_rejects_nonpositive_nonfinite_and_nonnumeric_elapsed_time(self):
        for value in (0, -0.0, -1, float("nan"), float("inf"),
                      float("-inf"), None, "10", True, False):
            with self.subTest(elapsed_ms=value):
                with self.assertRaises(ValueError):
                    analyze.summarize_probe([
                        probe_sample(), probe_sample(elapsed_ms=value)
                    ])

    def test_correctness_requires_exactly_true_for_every_shape(self):
        for value in (False, None, 0, 1, "true", "false", [], {}):
            with self.subTest(correctness_ok=value):
                with self.assertRaises(ValueError):
                    analyze.summarize_probe([
                        probe_sample(),
                        probe_sample(shape="narrow", correctness_ok=value),
                    ])

    def test_rejects_invalid_metrics_after_valid_evidence(self):
        for field in ("peak_live_delta_bytes", "alloc_events", "input_bytes",
                      "output_bytes"):
            for value in (-1, float("nan"), float("inf"), float("-inf"),
                          None, "1", True, False, [], {}):
                with self.subTest(field=field, value=value):
                    with self.assertRaises(ValueError):
                        analyze.summarize_probe([
                            probe_sample(), probe_sample(**{field: value})
                        ])

    def test_rejects_invalid_shapes_and_nonobject_rows(self):
        for shape in ("", "  ", None, 1, True, [], {}):
            with self.subTest(shape=shape):
                with self.assertRaises(ValueError):
                    analyze.summarize_probe([probe_sample(shape=shape)])
        for row in (None, [], "wide", 1, True):
            with self.subTest(row=row):
                with self.assertRaises(ValueError):
                    analyze.summarize_probe([probe_sample(), row])


class PhaseSummaryTests(unittest.TestCase):
    def assert_summary(self, summary, count, throughput, percentiles):
        self.assertEqual(summary["count"], count)
        self.assertAlmostEqual(summary["throughput_ops_per_second"], throughput)
        self.assertEqual(summary["latency_ms"], dict(zip(
            ("p50", "p95", "p99", "p999"), percentiles
        )))

    def test_known_thousand_sample_distribution_has_distinct_tail_ranks(self):
        # Deliberately descending: nearest ranks of 1..1000 are known exactly.
        samples = [phase_sample(latency_ms=n) for n in range(1000, 0, -1)]
        result = analyze.summarize_phase(samples, expected_operations=1000,
                                        elapsed_seconds=4)
        self.assert_summary(result["read"], 1000, 250, (500, 950, 990, 999))
        self.assert_summary(result["combined"], 1000, 250, (500, 950, 990, 999))

    def test_combined_percentiles_pool_unequal_raw_read_and_write_samples(self):
        # Pool = 1..99, 10000. Averaging per-op percentiles cannot yield this.
        samples = [phase_sample("write", 10000)]
        samples.extend(phase_sample("read", n) for n in range(99, 0, -1))
        result = analyze.summarize_phase(samples, expected_operations=100,
                                        elapsed_seconds=2.5)
        self.assertEqual(set(result), {"read", "write", "combined"})
        self.assert_summary(result["read"], 99, 39.6, (50, 95, 99, 99))
        self.assert_summary(result["write"], 1, 0.4,
                            (10000, 10000, 10000, 10000))
        self.assert_summary(result["combined"], 100, 40, (50, 95, 99, 10000))

    def test_two_samples_use_lower_nearest_rank_median_without_interpolation(self):
        result = analyze.summarize_phase(
            [phase_sample("write", 9), phase_sample("write", 1)],
            expected_operations=2, elapsed_seconds=0.5,
        )
        self.assert_summary(result["write"], 2, 4, (1, 9, 9, 9))
        self.assert_summary(result["combined"], 2, 4, (1, 9, 9, 9))

    def test_repeated_latencies_retain_their_sample_weight(self):
        samples = [phase_sample(latency_ms=100)]
        samples.extend(phase_sample(latency_ms=2) for _ in range(19))
        result = analyze.summarize_phase(samples, expected_operations=20,
                                        elapsed_seconds=2)
        self.assert_summary(result["combined"], 20, 10, (2, 2, 100, 100))

    def test_singleton_fractional_latency_has_identical_percentiles(self):
        result = analyze.summarize_phase([phase_sample(latency_ms=0.125)],
                                        expected_operations=1, elapsed_seconds=2)
        self.assert_summary(result["read"], 1, 0.5, (0.125,) * 4)
        self.assert_summary(result["combined"], 1, 0.5, (0.125,) * 4)

    def test_rejects_missing_and_extra_samples(self):
        for expected in (1, 3):
            with self.subTest(expected_operations=expected):
                with self.assertRaises(ValueError):
                    analyze.summarize_phase(
                        [phase_sample(), phase_sample("write")],
                        expected_operations=expected, elapsed_seconds=1,
                    )

    def test_rejects_empty_evidence_even_if_zero_operations_were_expected(self):
        for expected in (0, 1):
            with self.subTest(expected_operations=expected):
                with self.assertRaises(ValueError):
                    analyze.summarize_phase([], expected_operations=expected,
                                            elapsed_seconds=1)

    def test_rejects_nonpositive_nonfinite_and_nonnumeric_duration(self):
        for duration in (0, -0.0, -1, float("nan"), float("inf"),
                         float("-inf"), None, "1", True, False):
            with self.subTest(elapsed_seconds=duration):
                with self.assertRaises(ValueError):
                    analyze.summarize_phase([phase_sample()],
                                            expected_operations=1,
                                            elapsed_seconds=duration)

    def test_rejects_failed_or_mismatched_reads_and_writes_at_exact_count(self):
        for op in ("read", "write"):
            for flags in ({"ok": False}, {"mismatch": True},
                          {"ok": False, "mismatch": True}):
                with self.subTest(op=op, flags=flags):
                    with self.assertRaises(ValueError):
                        analyze.summarize_phase(
                            [phase_sample(), phase_sample(op, **flags)],
                            expected_operations=2, elapsed_seconds=1,
                        )

    def test_requires_boolean_status_fields_without_truthiness_coercion(self):
        for field in ("ok", "mismatch"):
            for value in (None, 0, 1, "true", "false"):
                with self.subTest(field=field, value=value):
                    with self.assertRaises(ValueError):
                        analyze.summarize_phase(
                            [phase_sample(**{field: value})],
                            expected_operations=1, elapsed_seconds=1,
                        )

    def test_rejects_each_missing_required_field(self):
        for field in phase_sample():
            with self.subTest(field=field):
                incomplete = phase_sample()
                del incomplete[field]
                with self.assertRaises(ValueError):
                    analyze.summarize_phase([phase_sample(), incomplete],
                                            expected_operations=2,
                                            elapsed_seconds=1)

    def test_rejects_invalid_latency_for_either_operation(self):
        for op in ("read", "write"):
            for latency in (-1, float("nan"), float("inf"), float("-inf"),
                            None, "1", True, False):
                with self.subTest(op=op, latency_ms=latency):
                    with self.assertRaises(ValueError):
                        analyze.summarize_phase(
                            [phase_sample(), phase_sample(op, latency)],
                            expected_operations=2, elapsed_seconds=1,
                        )

    def test_rejects_unknown_or_missing_operation_values(self):
        for op in ("delete", "READ", "", None, 1):
            with self.subTest(op=op):
                with self.assertRaises(ValueError):
                    analyze.summarize_phase([phase_sample(), phase_sample(op)],
                                            expected_operations=2,
                                            elapsed_seconds=1)

    def test_rejects_invalid_expected_operation_counts(self):
        for expected in (0, -1, 1.0, 1.5, True, False, None, "1",
                         float("nan"), float("inf"), [], {}):
            with self.subTest(expected_operations=expected):
                with self.assertRaises(ValueError):
                    analyze.summarize_phase([phase_sample()],
                                            expected_operations=expected,
                                            elapsed_seconds=1)

    def test_rejects_nonobject_rows_and_unhashable_operations(self):
        for row in (None, [], "read", 1, True,
                    phase_sample(op=[]), phase_sample(op={})):
            with self.subTest(row=row):
                with self.assertRaises(ValueError):
                    analyze.summarize_phase([phase_sample(), row],
                                            expected_operations=2,
                                            elapsed_seconds=1)

    def test_zero_latency_is_valid(self):
        result = analyze.summarize_phase([phase_sample(latency_ms=0)],
                                        expected_operations=1, elapsed_seconds=2)
        self.assert_summary(result["combined"], 1, 0.5, (0, 0, 0, 0))


class AnalyzerCliTests(unittest.TestCase):
    def run_cli(self, arguments, evidence):
        output = io.StringIO()
        with patch("builtins.open", mock_open(read_data=evidence)):
            with redirect_stdout(output):
                analyze.main(arguments)
        return json.loads(output.getvalue())

    def test_probe_jsonl(self):
        rows = [probe_sample(), probe_sample(elapsed_ms=20)]
        result = self.run_cli(["probe", "evidence.jsonl"],
                              "\n".join(map(json.dumps, rows)) + "\n")
        self.assertEqual(result, analyze.summarize_probe(rows))

    def test_phase_jsonl(self):
        rows = [phase_sample(), phase_sample("write", 3)]
        result = self.run_cli(
            ["phase", "evidence.jsonl", "--expected-operations", "2",
             "--elapsed-seconds", "0.5"], "\n".join(map(json.dumps, rows)))
        self.assertEqual(result, analyze.summarize_phase(rows, 2, 0.5))

    def test_invalid_jsonl_never_emits_a_partial_summary(self):
        for bad_row in ("", "not json", "null", "[]", "{}",
                        '{"shape": "wide", "shape": "narrow"}',
                        json.dumps(probe_sample(alloc_events=float("nan"))),
                        json.dumps(probe_sample(output_bytes=-1))):
            with self.subTest(bad_row=bad_row):
                output, errors = io.StringIO(), io.StringIO()
                evidence = json.dumps(probe_sample()) + "\n" + bad_row + "\n"
                with patch("builtins.open", mock_open(read_data=evidence)):
                    with redirect_stdout(output), redirect_stderr(errors):
                        with self.assertRaises(SystemExit) as failure:
                            analyze.main(["probe", "evidence.jsonl"])
                self.assertEqual(failure.exception.code, 2)
                self.assertEqual(output.getvalue(), "")
                self.assertIn("error:", errors.getvalue())

    def test_phase_requires_both_flags(self):
        for flags in ([], ["--expected-operations", "1"],
                      ["--elapsed-seconds", "1"]):
            with self.subTest(flags=flags), redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as failure:
                    analyze.main(["phase", "evidence.jsonl", *flags])
                self.assertEqual(failure.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
