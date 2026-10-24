#!/usr/bin/env python3
"""Summarize exact raw samples; compare only identical workload dimensions."""
import collections
import json
import statistics
import sys
from pathlib import Path

FIELDS = ("rows", "partitions", "fanin", "value_bytes", "complex", "purge", "compressed")
METRICS = ("alloc_events", "allocated_bytes", "peak_live_delta_bytes", "elapsed_ns")


def summarize(path):
    groups = collections.defaultdict(list)
    for line in Path(path).read_text().splitlines():
        item = json.loads(line)
        assert item["correct"] is True, "failed correctness sample"
        groups[tuple(item[k] for k in FIELDS)].append(item)
    results = {}
    for key, samples in groups.items():
        row = dict(zip(FIELDS, key))
        row["samples"] = len(samples)
        row["metrics"] = {
            field: {
                "median": statistics.median(s[field] for s in samples),
                "min": min(s[field] for s in samples),
                "max": max(s[field] for s in samples),
            }
            for field in METRICS
        }
        results[key] = row
    return results


def main():
    baseline = summarize(sys.argv[1])
    if len(sys.argv) == 2:
        print(json.dumps(list(baseline.values()), indent=2))
        return
    candidate = summarize(sys.argv[2])
    assert baseline.keys() == candidate.keys(), "workload mismatch"
    comparison = []
    for key, before in baseline.items():
        after = candidate[key]
        comparison.append({
            "workload": dict(zip(FIELDS, key)),
            "baseline": before,
            "candidate": after,
            "candidate_over_baseline": {
                field: after["metrics"][field]["median"] / before["metrics"][field]["median"]
                for field in METRICS
            },
        })
    print(json.dumps(comparison, indent=2))


if __name__ == "__main__":
    main()
