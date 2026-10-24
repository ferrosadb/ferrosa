#!/usr/bin/env python3
"""Bounded raw/direct attribution with unchanged saved probe executables."""
import json
import os
from pathlib import Path
import subprocess

output = Path('/work/results/v4-io-controls.jsonl').open('w')
for rows, value_bytes in [(16384, 128), (1, 8388608)]:
    for direct in ['0', '1']:
        for round_number in range(3):
            arms = ['baseline', 'candidate'] if round_number % 2 == 0 else ['candidate', 'baseline']
            for arm in arms:
                subprocess.run(['sync'], check=True)
                env = dict(os.environ, FERROSA_COMPACTION_DIRECT_READ=direct)
                command = ['/usr/bin/time', '-f', 'IO_DIAG %I %O %M',
                           f'/work/results/{arm}-final-probe', str(rows), '1', '4',
                           str(value_bytes), '1']
                result = subprocess.run(command, env=env, text=True, capture_output=True, check=True)
                record = json.loads(next(line for line in result.stdout.splitlines() if line.startswith('{')))
                diag = next(line for line in result.stderr.splitlines() if line.startswith('IO_DIAG')).split()
                record.update(arm=arm, direct=direct, control_round=round_number,
                              process_input_blocks=int(diag[1]), process_output_blocks=int(diag[2]),
                              process_max_rss_kib=int(diag[3]))
                output.write(json.dumps(record) + '\n')
                output.flush()
output.close()
