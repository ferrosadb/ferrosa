"""Behavioural contract for the smoke harness's *client-side* contact points.

Nightly run 38533936817, job 115752136272 ("Docker Smoke Test (Pair Mode)") died
in Phase 8 of tests/docker-smoke.sh, five seconds after node3 was stopped:

    INFO: Writing with 1 node down (QUORUM should succeed)...
    FAIL: QUORUM write with 1 of 3 nodes down: Connection error:
      ('Unable to connect to any servers',
       {'127.0.0.1:9042': OperationTimedOut('errors=None, last_host=None'),
        '::1:9042':      OperationTimedOut('errors=None, last_host=None')})

The contact-point set in that message is one node: `localhost` resolves to
127.0.0.1 and ::1, both bound to node1's published port 9042. The artifact's
`docker-compose-ps.log` shows node2 up and healthy throughout
(`0.0.0.0:9043->9042/tcp`), and node2's log shows it received no client
connection at all — the harness never offered it. node1 was up but not
*serving*: its CQL request runtime was frozen across exactly that window
(node1.log: `runtime scheduling stall STARTED ... stall_ms=4966`,
`RECOVERED ... stalls=2 worst_ms=4966`).

cqlsh speaks to a single host:port and has no client-side failover, so the
harness turned one node's transient freeze into "the cluster is down". These
tests run the Phase 8 client block *as it is written in the script* (extracted
at test time, not retyped) against a modelled topology that mirrors the
artifact: 9042 accepts and never answers, 9043 answers, 9044 is stopped.
"""

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DOCKER_SMOKE = ROOT / "tests" / "docker-smoke.sh"

PHASE8_START = 'info "Writing with 1 node down (QUORUM should succeed)..."'
PHASE8_END = 'pass "Write succeeds with 1 node down (QUORUM met: 2 of 3)"'

# Test double for cqlsh. It models a ferrosa node's CQL endpoint, not the CQL
# protocol: a frozen node accepts the socket and never answers (the driver
# gives up at --connect-timeout and prints the driver's own error), a healthy
# node acknowledges, a stopped node refuses. Every contact point it is handed
# is recorded so the test can see which nodes the harness actually tried.
CQLSH_SHIM = r"""#!/usr/bin/env bash
set -uo pipefail
host=""
port=""
connect_timeout=5
while [ $# -gt 0 ]; do
    case "$1" in
        -e) shift 2 ;;
        --connect-timeout=*) connect_timeout="${1#*=}"; shift ;;
        --request-timeout=*) shift ;;
        -*) shift ;;
        *)
            # `cqlsh [opts] <host> <port> -e <stmt>`: the port is the numeric arg.
            case "$1" in
                ''|*[!0-9]*) host="$1" ;;
                *) port="$1" ;;
            esac
            shift
            ;;
    esac
done

printf '%s:%s\n' "$host" "$port" >> "${SMOKE_SHIM_CALLS:?}"

if [ -n "${SMOKE_SHIM_STOPPED_PORT:-}" ] && [ "$port" = "$SMOKE_SHIM_STOPPED_PORT" ]; then
    echo "Connection refused: no node is listening on $port" >&2
    exit 1
fi

case ",${SMOKE_SHIM_FROZEN_PORTS:-}," in
    *",$port,"*)
        # Real stall, scaled down so the contract test stays fast: the driver
        # waits out --connect-timeout and then reports OperationTimedOut for
        # every host it was given.
        python3 -c 'import sys, time; time.sleep(float(sys.argv[1]) * float(sys.argv[2]))' \
            "$connect_timeout" "${SMOKE_SHIM_TIMEOUT_SCALE:-0.1}"
        printf "Connection error: ('Unable to connect to any servers', {'127.0.0.1:%s': OperationTimedOut('errors=None, last_host=None'), '::1:%s': OperationTimedOut('errors=None, last_host=None')})\n" \
            "$port" "$port"
        exit 1
        ;;
esac

echo "acknowledged"
exit 0
"""


def _bash_function(text: str, name: str) -> str:
    """Extract a top-level bash function definition by brace matching."""
    start = text.index(f"\n{name}() {{") + 1
    depth = 0
    for index in range(text.index("{", start), len(text)):
        if text[index] == "{":
            depth += 1
        elif text[index] == "}":
            depth -= 1
            if depth == 0:
                return text[start : index + 1]
    raise AssertionError(f"unterminated bash function {name}()")


def _phase8_block(text: str) -> str:
    """The Phase 8 QUORUM write, verbatim from the script."""
    block = text.split(PHASE8_START, 1)[1].split(PHASE8_END, 1)[0]
    return f"{PHASE8_START}\n{block}{PHASE8_END}"


class QuorumWriteContactPointTest(unittest.TestCase):
    def _run_phase8(
        self,
        tmp: Path,
        frozen_ports: str = "",
        stopped_port: str = "",
    ):
        """Run the script's own Phase 8 block against a modelled cluster."""
        smoke = DOCKER_SMOKE.read_text(encoding="utf-8")

        shim_dir = tmp / "bin"
        shim_dir.mkdir()
        shim = shim_dir / "cqlsh"
        shim.write_text(CQLSH_SHIM, encoding="utf-8")
        shim.chmod(shim.stat().st_mode | 0o111)

        calls = tmp / "cqlsh-calls"
        calls.write_text("", encoding="utf-8")

        script = "\n".join(
            [
                "set -uo pipefail",
                'fail() { echo "FAIL: $1"; exit 1; }',
                'pass() { echo "PASS: $1"; }',
                'info() { echo "INFO: $1"; }',
                _bash_function(smoke, "cql_ok"),
                # Present only once the Phase 8 write has contact points to walk.
                *(
                    [_bash_function(smoke, "cql_ok_any")]
                    if "\ncql_ok_any() {" in smoke
                    else []
                ),
                _phase8_block(smoke),
            ]
        )
        runner = tmp / "run-phase8.sh"
        runner.write_text(script, encoding="utf-8")

        env = {
            **os.environ,
            "PATH": f"{shim_dir}{os.pathsep}{os.environ['PATH']}",
            "SMOKE_SHIM_CALLS": str(calls),
            "SMOKE_SHIM_FROZEN_PORTS": frozen_ports,
            "SMOKE_SHIM_STOPPED_PORT": stopped_port,
        }
        proc = subprocess.run(
            ["bash", str(runner)],
            capture_output=True,
            text=True,
            env=env,
            timeout=120,
        )
        contact_points = sorted(calls.read_text(encoding="utf-8").split())
        return proc, contact_points

    def test_quorum_write_walks_the_surviving_contact_points(self):
        """One frozen node must not read as a dead cluster.

        node1 is the harness's only contact point today. When it stops
        answering, the write has to reach node2 — up, healthy and idle in the
        same artifact — before the phase is declared failed.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            proc, contact_points = self._run_phase8(Path(tmp_dir), frozen_ports="9042")
            self.assertEqual(
                0,
                proc.returncode,
                "the QUORUM write must be acknowledged by the surviving node while "
                "one contact point is frozen; the harness gave up instead\n"
                f"contact points tried: {contact_points}\n"
                f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}",
            )
            self.assertIn(
                "localhost:9043",
                contact_points,
                "the harness must offer node2 (9043) as a contact point for the "
                "QUORUM write — it is one of the two surviving replicas",
            )

    def test_quorum_write_still_fails_loud_when_no_survivor_answers(self):
        """The failover must not swallow a genuinely unreachable cluster."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            proc, contact_points = self._run_phase8(
                Path(tmp_dir), frozen_ports="9042,9043"
            )
            self.assertNotEqual(
                0,
                proc.returncode,
                "a QUORUM write with no reachable survivor must fail the run\n"
                f"stdout:\n{proc.stdout}",
            )
            self.assertIn(
                "FAIL: QUORUM write with 1 of 3 nodes down",
                proc.stdout,
                "the phase must report the failure, not pass quietly",
            )
            self.assertIn("9042", proc.stdout, "the failure must name what it tried")

    def test_quorum_write_never_contacts_the_stopped_node(self):
        """Phase 8 stopped node3; its port must not become a contact point."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            proc, contact_points = self._run_phase8(
                Path(tmp_dir), frozen_ports="9042", stopped_port="9044"
            )
            self.assertNotIn(
                "localhost:9044",
                contact_points,
                "node3 was stopped by the phase; the write must not target it",
            )


if __name__ == "__main__":
    unittest.main()
