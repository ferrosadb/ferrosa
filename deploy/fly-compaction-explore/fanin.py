#!/usr/bin/env python3
"""Bounded fresh-process fan-in sweep of an existing allocation probe.

Rows are per input partition. Reported peak live-byte deltas are allocator
measurements, not production RSS or physical SSTable byte counts. Repetition
indices in samples.jsonl are zero-based; each original probe repetition is 0.
"""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import random
import statistics
import subprocess
import sys


DIMENSIONS = ("rows", "partitions", "fanin", "value_bytes")
METRICS = ("elapsed_ns", "peak_live_delta_bytes", "alloc_events", "allocated_bytes")
VERIFY = {"FERROSA_COMPACTION_VERIFY_OUTPUT": "true"}


def cases():
    """Return independent case dictionaries for both four-point families."""
    return [
        {"name": f"{family}_fanin{fanin}",
         "rows": 262144 // fanin if family == "fixed_total" else 4096,
         "partitions": 1, "fanin": fanin, "value_bytes": 64}
        for family in ("fixed_total", "fixed_per_input")
        for fanin in (4, 16, 32, 64)
    ]


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON field: {key}")
        result[key] = value
    return result


def parse_sample(stdout, case):
    """Validate exactly one probe JSON line, retaining its original metrics."""
    lines = [line.strip() for line in stdout.splitlines() if line.strip()]
    if len(lines) != 1:
        raise ValueError("expected exactly one nonempty probe JSON line")
    sample = json.loads(lines[0], object_pairs_hook=unique_object)
    required = set(DIMENSIONS + METRICS) | {
        "correct", "complex", "purge", "compressed", "repetition"}
    if not isinstance(sample, dict) or set(sample) != required:
        raise ValueError("probe object does not match the expected schema")
    if sample["correct"] is not True:
        raise ValueError("probe did not report correct=true")
    for field in ("complex", "purge", "compressed"):
        if sample[field] is not False:
            raise ValueError(f"expected {field}=false")
    for field in DIMENSIONS:
        if (type(sample[field]) is not int or sample[field] <= 0
                or sample[field] != case[field]):
            raise ValueError(f"probe {field} does not match the matrix case")
    if type(sample["repetition"]) is not int or sample["repetition"] != 0:
        raise ValueError("fresh probe must report repetition=0")
    for field in METRICS:
        value = sample[field]
        if (type(value) not in (int, float)
                or (type(value) is float and not math.isfinite(value))
                or value < 0 or (field == "elapsed_ns" and value == 0)):
            raise ValueError(f"invalid probe measurement: {field}")
    return sample


def summarize(records):
    """Summarize only validated samples, including any partial run."""
    summaries = {}
    for case in cases():
        probes = [record["probe"] for record in records
                  if record["case"]["name"] == case["name"]]
        if not probes:
            continue
        summary = {"samples": len(probes)}
        for metric in METRICS:
            values = [probe[metric] for probe in probes]
            summary[metric] = {"median": statistics.median(values)}
            if metric in ("elapsed_ns", "peak_live_delta_bytes"):
                summary[metric]["max"] = max(values)
            if metric == "elapsed_ns":
                summary[metric]["min"] = min(values)
        summaries[case["name"]] = summary
    return summaries


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path, value):
    # Replace atomically so an interrupted update leaves the prior manifest.
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n",
                         encoding="utf-8")
    temporary.replace(path)


def run(args):
    binary = args.binary.resolve(strict=True)
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise ValueError("--binary must be an existing executable file")
    manifest = {
        "status": "running", "binary_path": str(binary),
        "binary_sha256": sha256(binary),
        "script_sha256": sha256(Path(__file__).resolve()),
        "environment": VERIFY.copy(), "repetitions": args.repetitions,
        "seed": args.seed, "timeout": args.timeout,
        "invocations": [],
    }
    rng = random.Random(args.seed)
    for repetition in range(args.repetitions):
        order = cases()
        rng.shuffle(order)
        for case in order:
            index = len(manifest["invocations"])
            prefix = f"{index:04d}"
            manifest["invocations"].append({
                "order": index, "case": case, "repetition": repetition,
                "command": [str(binary)] + [str(case[key]) for key in DIMENSIONS] + ["1"],
                "environment": VERIFY.copy(), "timeout": args.timeout,
                "stdout": prefix + ".stdout", "stderr": prefix + ".stderr",
                "invocation_file": prefix + ".invocation.json", "status": "pending",
            })
    output = args.output
    output.mkdir()  # Require a new directory; never overwrite prior evidence.
    manifest_path = output / "manifest.json"
    write_json(manifest_path, manifest)
    records = []
    env = dict(os.environ, **VERIFY)
    try:
        with (output / "samples.jsonl").open("x", encoding="utf-8") as samples:
            for invocation in manifest["invocations"]:
                invocation_path = output / invocation["invocation_file"]
                invocation["status"] = "running"
                write_json(invocation_path, invocation)
                write_json(manifest_path, manifest)
                try:
                    # Files preserve even partial timeout output without buffering
                    # unbounded child output in the runner's memory.
                    with (output / invocation["stdout"]).open("xb") as stdout, \
                            (output / invocation["stderr"]).open("xb") as stderr:
                        completed = subprocess.run(
                            invocation["command"], env=env, stdin=subprocess.DEVNULL,
                            stdout=stdout, stderr=stderr, timeout=args.timeout,
                            check=False)
                    invocation["returncode"] = completed.returncode
                    if completed.returncode != 0:
                        raise ValueError(f"probe exited with status {completed.returncode}")
                    probe = parse_sample(
                        (output / invocation["stdout"]).read_text(encoding="utf-8"),
                        invocation["case"])
                    record = {"case": invocation["case"],
                              "repetition": invocation["repetition"], "probe": probe}
                    samples.write(json.dumps(record, allow_nan=False) + "\n")
                    samples.flush()
                    records.append(record)
                    invocation["status"] = "complete"
                except (OSError, ValueError, subprocess.SubprocessError, KeyboardInterrupt) as error:
                    invocation["status"] = "failed"
                    invocation["error"] = str(error) or type(error).__name__
                    raise
                finally:
                    write_json(invocation_path, invocation)
                    write_json(manifest_path, manifest)
        manifest["status"] = "complete"
    except (OSError, ValueError, subprocess.SubprocessError, KeyboardInterrupt) as error:
        manifest["status"] = "failed"
        manifest["error"] = str(error) or type(error).__name__
        raise
    finally:
        write_json(manifest_path, manifest)
        write_json(output / "summary.json", summarize(records))


def positive_int(text):
    value = int(text)
    if value <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return value


def positive_seconds(text):
    value = float(text)
    if not math.isfinite(value) or value <= 0:
        raise argparse.ArgumentTypeError("must be finite positive seconds")
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True,
                        help="existing release compaction_allocation_probe executable")
    parser.add_argument("--output", type=Path, required=True, help="new output directory")
    parser.add_argument("--repetitions", type=positive_int, default=5)
    parser.add_argument("--seed", type=int, default=601)
    parser.add_argument("--timeout", type=positive_seconds, default=120,
                        help="timeout in seconds per fresh process (default: 120)")
    args = parser.parse_args()
    try:
        run(args)
    except (OSError, ValueError, subprocess.SubprocessError, KeyboardInterrupt) as error:
        print(f"fanin: {str(error) or type(error).__name__}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
