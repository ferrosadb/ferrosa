#!/usr/bin/env python3
"""Trace synthetic probes; attribute input preads to executor/readahead threads."""
import collections
import json
import os
from pathlib import Path
import re
import subprocess
import time

for _ in range(120):
    path = Path('/work/results/v4-io-controls.jsonl')
    if path.exists() and len(path.read_text().splitlines()) == 24:
        break
    time.sleep(1)
else:
    raise RuntimeError('I/O controls did not complete; refuse overlapping traces')

summary = []
for rows, value_bytes in [(16384, 128), (1, 8388608)]:
    for direct in ['0', '1']:
        for arm in ['baseline', 'candidate']:
            prefix = f'/work/logs/io-trace-{arm}-{rows}-{direct}'
            command = ['strace', '-ff', '-qq', '-yy', '-ttt', '-T', '-s', '32',
                       '-e', 'trace=pread64,read,openat,close,prctl', '-o', prefix,
                       f'/work/results/{arm}-final-probe', str(rows), '1', '4',
                       str(value_bytes), '1']
            if not os.environ.get('TRACE_PARSE_ONLY'):
                result = subprocess.run(command, env=dict(os.environ, FERROSA_COMPACTION_DIRECT_READ=direct),
                                        text=True, capture_output=True, check=True)
                record = json.loads(next(s for s in result.stdout.splitlines() if s.startswith('{')))
                assert record['correct'] is True
            calls = []
            threads = []
            unmatched = 0
            for trace in Path('/work/logs').glob(Path(prefix).name + '.*'):
                text = trace.read_text()
                # Linux truncates PR_SET_NAME to15 bytes; match the stable prefix.
                if not any(name in text for name in ['compaction-exec', 'sstable-read']):
                    continue
                threads.append(trace.name)
                for line in text.splitlines():
                    if 'pread64(' not in line or '/input-' not in line or '-Data.db>' not in line:
                        continue
                    match = re.search(r'^([0-9.]+).*pread64\([^<]*<([^>]+)>.*?,\s*(\d+),\s*(\d+)\)\s*=\s*(\d+)', line)
                    if match:
                        stamp, file, requested, offset, returned = match.groups()
                        calls.append((float(stamp), file, int(offset), int(returned), int(requested)))
                    else:
                        unmatched += 1
            previous = {}
            backward = 0
            for _, file, offset, _, _ in sorted(calls):
                if file in previous and offset < previous[file]:
                    backward += 1
                previous[file] = offset
            summary.append(dict(arm=arm, rows=rows, value_bytes=value_bytes, direct=direct,
                                input_pread_calls=len(calls), input_pread_bytes=sum(c[3] for c in calls),
                                input_requested_bytes=sum(c[4] for c in calls), backward_input_requests=backward,
                                unmatched_input_lines=unmatched, trace_threads=threads, correct=True))
            Path('/work/results/v4-input-io-traces.json').write_text(json.dumps(summary, indent=2) + '\n')
