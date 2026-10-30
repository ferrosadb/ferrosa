"""Collect bounded samples; decode recordings only after the workload stops."""
import json
import resource
import signal
import struct
import subprocess
import sys
import time
from pathlib import Path

SWITCH_PERIOD = 100
SWITCH_FILE_CAP = 64 * 1024 * 1024

def bounded_switch_file():
    resource.setrlimit(resource.RLIMIT_FSIZE, (SWITCH_FILE_CAP, SWITCH_FILE_CAP))

target, label = sys.argv[1:]
out = Path('/tmp/pipeline_bolt_protocol_144_profile_' + label)
out.mkdir()
if target.startswith('ferrosa'):
    pids = [Path('/tmp/pipeline_bolt_protocol_144_ferrosa.pid').read_text().strip()]
else:
    name = {'postgres': 'postgres', 'cassandra': 'java', 'oxigraph': 'oxigraph'}[target]
    pids = []
    for process in Path('/proc').iterdir():
        try:
            if process.name.isdigit() and (process / 'comm').read_text().strip() == name:
                pids.append(process.name)
        except FileNotFoundError:
            pass
assert pids
with (out / 'record.log').open('wb') as log:
    cpu = subprocess.Popen(['perf', 'record', '-e', 'cpu-clock:u', '-F', '49', '-g',
                            '-p', ','.join(pids), '-o', str(out / 'cpu.data')],
                           stdout=log, stderr=subprocess.STDOUT)
    time.sleep(1)
    assert cpu.poll() is None
    (out / 'ready').touch()
    go_deadline = time.monotonic() + 120
    while not (out / 'go').exists() and time.monotonic() < go_deadline:
        time.sleep(.1)
    assert (out / 'go').exists(), 'profiling workload GO was never received'
    time.sleep(2)
    with (out / 'switch-record.log').open('wb') as switch_log:
        switches = subprocess.Popen(
            ['perf', 'record', '-e', 'context-switches', '-c', str(SWITCH_PERIOD), '-g',
             '-p', ','.join(pids), '-o', str(out / 'switch.data'), '--', 'sleep', '5'],
            stdout=switch_log, stderr=subprocess.STDOUT, preexec_fn=bounded_switch_file)
        try:
            switch_code = switches.wait(timeout=15)
        except subprocess.TimeoutExpired:
            switches.kill()
            switch_code = switches.wait()
    # The controller requests stop after the timed client operation completes.
    # Keep all perf-script decoding outside both the workload and CPU recording.
    deadline = time.monotonic() + 60
    while not (out / 'stop').exists() and time.monotonic() < deadline:
        time.sleep(.1)
    cpu.send_signal(signal.SIGINT)
    try:
        cpu_code = cpu.wait(timeout=20)
    except subprocess.TimeoutExpired:
        cpu.kill()
        cpu_code = cpu.wait()
        (out / 'incomplete').touch()
with (out / 'cpu.data').open('rb') as stream:
    header = stream.read(104)
assert header[:8] == b'PERFILE2'
(out / 'capture-status.json').write_text(json.dumps({
    'perf_exit': cpu_code, 'data_size': struct.unpack_from('<Q', header, 48)[0],
    'pids': pids, 'stop_requested': (out / 'stop').exists(),
    'postprocessing_after_workload': True}))
with (out / 'cpu.perf-script').open('w') as text, (out / 'script.err').open('w') as err:
    subprocess.run(['perf', 'script', '-i', str(out / 'cpu.data')],
                   stdout=text, stderr=err, timeout=30, check=True)
switch_status = {'exit': switch_code, 'sample_period': SWITCH_PERIOD,
                 'file_cap_bytes': SWITCH_FILE_CAP,
                 'scope': 'sampled context-switch callchains; event weights are estimates, not wait durations'}
if switch_code == 0:
    with (out / 'switch.perf-script').open('w') as text, (out / 'switch-script.err').open('w') as err:
        try:
            result = subprocess.run(['perf', 'script', '-i', str(out / 'switch.data')],
                                    stdout=text, stderr=err, timeout=30,
                                    preexec_fn=bounded_switch_file)
            switch_status['exit'] = result.returncode
        except subprocess.TimeoutExpired:
            switch_status['exit'] = 124
(out / 'switch-capture-status.json').write_text(json.dumps(switch_status))
(out / 'done').touch()
