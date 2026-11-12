"""Validate and summarize compaction experiment JSONL using only the stdlib."""

import argparse
import json
import math
from collections.abc import Mapping
from statistics import median


PROBE_METRICS = (
    "elapsed_ms", "peak_live_delta_bytes", "alloc_events",
    "input_bytes", "output_bytes",
)


def _number(value, label, *, positive=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{label} must be a finite number")
    try:
        finite = math.isfinite(value)
    except OverflowError:
        finite = False
    if not finite or value < 0 or (positive and value == 0):
        bound = "positive" if positive else "nonnegative"
        raise ValueError(f"{label} must be finite and {bound}")
    return value


def _row(row, required, index):
    if not isinstance(row, Mapping):
        raise ValueError(f"row {index} must be an object")
    missing = set(required).difference(row)
    if missing:
        raise ValueError(f"row {index} missing fields: {', '.join(sorted(missing))}")


def summarize_probe(samples):
    """Return per-shape probe statistics; reject any invalid evidence row."""
    groups = {}
    for index, row in enumerate(samples, 1):
        _row(row, (*PROBE_METRICS, "shape", "correctness_ok"), index)
        shape = row["shape"]
        if not isinstance(shape, str) or not shape.strip():
            raise ValueError(f"row {index}: shape must be a nonempty string")
        if row["correctness_ok"] is not True:
            raise ValueError(f"row {index}: correctness_ok must be true")
        metrics = groups.setdefault(shape, {field: [] for field in PROBE_METRICS})
        for field in PROBE_METRICS:
            value = _number(row[field], f"row {index}: {field}",
                            positive=field == "elapsed_ms")
            metrics[field].append(value)
    if not groups:
        raise ValueError("probe evidence must not be empty")

    result = {}
    for shape, metrics in groups.items():
        stats = {field: {"median": median(values)}
                 for field, values in metrics.items()}
        stats["elapsed_ms"].update(min=min(metrics["elapsed_ms"]),
                                   max=max(metrics["elapsed_ms"]))
        stats["peak_live_delta_bytes"]["max"] = max(metrics["peak_live_delta_bytes"])
        result[shape] = {"samples": len(metrics["elapsed_ms"]), "metrics": stats}
    return result


def summarize_phase(samples, expected_operations, elapsed_seconds):
    """Return per-operation and pooled nearest-rank latency/throughput stats."""
    if (isinstance(expected_operations, bool)
            or not isinstance(expected_operations, int)
            or expected_operations <= 0):
        raise ValueError("expected_operations must be a positive integer")
    _number(elapsed_seconds, "elapsed_seconds", positive=True)
    groups = {}
    combined = []
    for index, row in enumerate(samples, 1):
        _row(row, ("op", "latency_ms", "ok", "mismatch"), index)
        op = row["op"]
        if not isinstance(op, str) or op not in ("read", "write"):
            raise ValueError(f"row {index}: op must be read or write")
        if row["ok"] is not True or row["mismatch"] is not False:
            raise ValueError(f"row {index}: ok must be true and mismatch must be false")
        latency = _number(row["latency_ms"], f"row {index}: latency_ms")
        groups.setdefault(op, []).append(latency)
        combined.append(latency)
    if not combined or len(combined) != expected_operations:
        raise ValueError(f"expected {expected_operations} operations, got {len(combined)}")

    groups["combined"] = combined
    result = {}
    for op, values in groups.items():
        values = sorted(values)
        count = len(values)
        # Integer arithmetic implements ceil(p * count) without rounding error.
        percentiles = {name: values[(rank * count + 999) // 1000 - 1]
                       for name, rank in (("p50", 500), ("p95", 950),
                                          ("p99", 990), ("p999", 999))}
        throughput = _number(count / elapsed_seconds, "throughput_ops_per_second")
        result[op] = {"count": count, "throughput_ops_per_second": throughput,
                      "latency_ms": percentiles}
    return result


def _json_object(pairs):
    row = {}
    for key, value in pairs:
        if key in row:
            raise ValueError(f"duplicate JSON field: {key}")
        row[key] = value
    return row


def _reject_constant(value):
    raise ValueError(f"invalid JSON constant: {value}")


def _read_jsonl(path):
    with open(path, encoding="utf-8") as source:
        for line_number, line in enumerate(source, 1):
            try:
                yield json.loads(line, object_pairs_hook=_json_object,
                                 parse_constant=_reject_constant)
            except ValueError as exc:
                raise ValueError(f"line {line_number}: {exc}") from exc


def main(argv=None):
    """Print a JSON summary, or fail with a nonzero exit and a diagnostic."""
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_subparsers(dest="mode", required=True)
    probe = modes.add_parser("probe", help="summarize probe samples by shape")
    probe.add_argument("input", help="input JSONL path")
    phase = modes.add_parser("phase", help="summarize operation latency and throughput")
    phase.add_argument("input", help="input JSONL path")
    phase.add_argument("--expected-operations", type=int, required=True)
    phase.add_argument("--elapsed-seconds", type=float, required=True)
    args = parser.parse_args(argv)
    try:
        rows = _read_jsonl(args.input)
        if args.mode == "probe":
            result = summarize_probe(rows)
        else:
            result = summarize_phase(rows, args.expected_operations, args.elapsed_seconds)
        output = json.dumps(result, allow_nan=False, sort_keys=True)
    except (ValueError, OSError) as exc:
        parser.error(str(exc))
    print(output)


if __name__ == "__main__":
    main()
