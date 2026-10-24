#!/usr/bin/env python3
"""Alternate source arms in fresh processes; never overlap build/test traffic."""
import json
import os
from pathlib import Path
import subprocess

shapes = [
    (256, 1, 4, 128, ""),
    (2048, 1, 4, 128, ""),
    (16384, 1, 4, 128, ""),
    (64, 128, 4, 64, "purge"),
    (2048, 1, 4, 128, "complex-purge-lz4"),
    (256, 1, 4, 16384, "lz4"),
    (2048, 1, 16, 64, ""),
    (1, 1, 4, 8388608, ""),
]
env = dict(os.environ, FERROSA_COMPACTION_DIRECT_READ="1")
outputs = {arm: Path(f"/work/results/{arm}-paired.jsonl").open("w")
           for arm in ("baseline", "candidate")}
for shape in shapes:
    for round_number in range(7):
        order = ("baseline", "candidate") if round_number % 2 == 0 else ("candidate", "baseline")
        for arm in order:
            subprocess.run(["sync"], check=True)
            args = [f"/work/results/{arm}-final-probe", *map(str, shape[:4]), "1", shape[4]]
            result = subprocess.run(args, env=env, text=True, capture_output=True, check=True)
            records = [json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")]
            assert len(records) == 1 and records[0]["correct"] is True
            record = records[0]
            record.update(ab_round=round_number, pair_order=list(order), io_mode="direct",
                          run_type="paired_fresh_process")
            outputs[arm].write(json.dumps(record) + "\n")
            outputs[arm].flush()
for output in outputs.values():
    output.close()
