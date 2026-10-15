"""Registering a LaunchAgent must survive the bootout/bootstrap race, and must
never report a success it did not get.

`launchctl bootout` returns before launchd finishes tearing the job down, so a
`bootstrap` issued straight after can fail with:

    Bootstrap failed: 5: Input/output error

Observed on 2026-10-14: the plist was fine (`plutil -lint` passed) and the
identical command retried five seconds later returned 0. The installers used to do
`bootout ... || true` then a bare, unchecked `bootstrap`, and then print
"...loaded; will start now and on every login" regardless -- so a lost race left
the machine with NO service while the installer claimed there was one.

These tests drive the real registration function against a fake `launchctl` that
mirrors the interface the function actually reads: `print` succeeding is the only
signal that a job is registered, and a non-zero exit is the only signal that it is
not. A fake that answered the right CONTENT in the wrong framing would measure
itself instead of the behaviour (see the tdd skill, "a fixture that fakes a tool
must mirror its contract").
"""

import os
import shutil
import stat
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
REGISTER_LIB = REPO / "launchd" / "register-agent.sh"
LABEL = "com.ferrosadb.ferrosa"

# A fake `launchctl`. It keeps its state in a file so that a job registered by one
# invocation is visible to the next, exactly as launchd's own database is.
FAKE_LAUNCHCTL = """#!/bin/sh
# usage: launchctl <verb> <domain[/label] | domain plist>
state="$FAKE_STATE"
verb=$1
case "$verb" in
  print)
    if [ -f "$state" ]; then exit 0; else
      echo "Could not find service \\"$2\\" in domain for user" >&2
      exit 113
    fi
    ;;
  bootout)
    rm -f "$state"
    exit 0
    ;;
  bootstrap)
    # Fail the first $FAKE_FAIL_BOOTSTRAP attempts, then succeed -- the race.
    if [ -f "$FAKE_BOOTSTRAP_COUNT" ]; then
      n=$(cat "$FAKE_BOOTSTRAP_COUNT")
    else
      n=0
    fi
    n=$((n + 1))
    echo "$n" > "$FAKE_BOOTSTRAP_COUNT"
    if [ "$n" -le "${FAKE_FAIL_BOOTSTRAP:-0}" ]; then
      echo "Bootstrap failed: 5: Input/output error" >&2
      exit 5
    fi
    : > "$state"
    exit 0
    ;;
  *)
    exit 0
    ;;
esac
"""


class LaunchdRegistrationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="launchd-test-"))
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

        self.bindir = self.tmp / "bin"
        self.bindir.mkdir()
        fake = self.bindir / "launchctl"
        fake.write_text(FAKE_LAUNCHCTL)
        fake.chmod(fake.stat().st_mode | stat.S_IEXEC)

        self.plist = self.tmp / f"{LABEL}.plist"
        self.plist.write_text("<plist><dict></dict></plist>")

        self.env = dict(os.environ)
        self.env["PATH"] = f"{self.bindir}:{self.env['PATH']}"
        self.env["FAKE_STATE"] = str(self.tmp / "registered")
        self.env["FAKE_BOOTSTRAP_COUNT"] = str(self.tmp / "count")

    def register(self, fail_bootstrap: int, timeout: int = 60):
        """Run the real function with a fake launchctl, return (rc, stdout/stderr)."""
        self.env["FAKE_FAIL_BOOTSTRAP"] = str(fail_bootstrap)
        script = textwrap.dedent(
            f"""
            set -eu
            . "{REGISTER_LIB}"
            register_launchd_agent "{LABEL}" "{self.plist}"
            """
        )
        proc = subprocess.run(
            ["sh", "-c", script],
            env=self.env,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return proc.returncode, proc.stdout + proc.stderr

    def registered(self) -> bool:
        return (self.tmp / "registered").exists()

    # --- the race ---------------------------------------------------------

    def test_a_bootstrap_that_loses_the_race_is_retried_until_the_job_is_registered(self):
        """The defect, exactly as it was hit: the first bootstrap loses the race."""
        rc, out = self.register(fail_bootstrap=2)
        self.assertTrue(
            self.registered(),
            f"the job must end up registered even when the first bootstrap raced "
            f"the teardown; rc={rc} output={out!r}",
        )
        self.assertEqual(rc, 0, f"a recovered registration is a success; output={out!r}")

    def test_a_first_attempt_success_is_not_disturbed(self):
        """The common case: nothing races, one bootstrap, registered."""
        rc, out = self.register(fail_bootstrap=0)
        self.assertTrue(self.registered(), f"output={out!r}")
        self.assertEqual(rc, 0, f"output={out!r}")

    # --- the negative control: never claim an unverified success ---------

    def test_a_bootstrap_that_never_succeeds_is_reported_as_a_failure(self):
        """If the job cannot be loaded the caller MUST be told.

        This is the load-bearing half: without it, a registration that silently
        gave up would still read as a successful install -- which is how a
        machine ends up with no service and an installer that says it worked.
        """
        rc, out = self.register(fail_bootstrap=999)
        self.assertFalse(
            self.registered(), f"the fake never registers the job; output={out!r}"
        )
        self.assertNotEqual(
            rc, 0, f"a registration that did not happen must not report success; output={out!r}"
        )
        self.assertIn("FAILED to load", out, f"the failure must name itself; output={out!r}")


if __name__ == "__main__":
    unittest.main()
