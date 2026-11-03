"""Tests for the machine-wide extraction lock in scripts/lean_gap_report.py.

Charon and Aeneas' Lean backend are what exhaust this host: charon reached 21.5 GB and 24.4 GB
resident on ferrosa-common, and three concurrent `aeneas main.exe -backend lean` runs took a
30 GB machine to a global OOM that killed an unrelated CI rustc. These cases pin the property
that keeps that from recurring -- one crate extracted at a time across processes -- plus the two
behaviours that make the lock safe to leave in the path of a sweep: it is released when a holder
is killed, and a caller that cannot wait says so instead of losing its measurement silently.

Each case runs real child processes against a real lock file, because the point is cross-process
serialization and an in-process mock would prove nothing.
"""

import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "lean_gap_report.py"
SENTINEL = "gave up waiting for the extraction lock"

# A child that takes the extraction lock, holds it for `hold` seconds, and records the interval
# it owned the lock in a shared log. time.time() is comparable across processes.
CHILD = r"""
import importlib.util, os, sys, time
script, log, tag, hold = sys.argv[1], sys.argv[2], sys.argv[3], float(sys.argv[4])
spec = importlib.util.spec_from_file_location("lean_gap_report", script)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
with mod.single_extraction():
    start = time.time()
    time.sleep(hold)
    end = time.time()
with open(log, "a", encoding="utf-8") as fh:
    fh.write("%s %.6f %.6f\n" % (tag, start, end))
"""


class ExtractionLockTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.log = self.root / "intervals.log"
        self.env = {**os.environ, "LEAN_GAP_LOCK": str(self.root / "lock"),
                    "LEAN_GAP_LOCK_TIMEOUT": ""}

    def tearDown(self):
        self._tmp.cleanup()

    def spawn(self, tag: str, hold: float, env: dict | None = None) -> subprocess.Popen:
        return subprocess.Popen(
            [sys.executable, "-c", CHILD, str(SCRIPT), str(self.log), tag, str(hold)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            env={**self.env, **(env or {})},
        )

    def intervals(self) -> dict[str, tuple[float, float]]:
        out = {}
        for line in self.log.read_text().splitlines():
            tag, start, end = line.split()
            out[tag] = (float(start), float(end))
        return out

    def test_concurrent_callers_never_extract_at_the_same_time(self):
        runs = [self.spawn(f"run{i}", 0.6) for i in range(3)]
        for run in runs:
            out, err = run.communicate(timeout=60)
            self.assertEqual(run.returncode, 0, err)

        spans = sorted(self.intervals().values())
        self.assertEqual(len(spans), 3, "every run must record an interval")
        for (_, prev_end), (next_start, _) in zip(spans, spans[1:]):
            self.assertGreaterEqual(
                next_start, prev_end - 0.01,
                f"two crates were extracted at once: {spans}",
            )
        total = spans[-1][1] - spans[0][0]
        self.assertGreaterEqual(total, 3 * 0.6 * 0.9, f"three 0.6s holds took only {total:.2f}s")

    def test_a_killed_holder_releases_the_lock(self):
        holder = self.spawn("killed", 30)
        lock = Path(self.env["LEAN_GAP_LOCK"])
        deadline = time.time() + 20
        while time.time() < deadline and not (lock.exists() and lock.read_text().strip()):
            time.sleep(0.05)
        self.assertTrue(lock.exists() and lock.read_text().strip(), "holder never took the lock")
        holder.kill()
        holder.communicate(timeout=30)

        started = time.time()
        waiter = self.spawn("after-kill", 0.1)
        _, err = waiter.communicate(timeout=30)
        self.assertEqual(waiter.returncode, 0, err)
        self.assertLess(time.time() - started, 10, "the lock outlived the process that held it")

    def test_a_waiter_that_cannot_wait_fails_loudly_and_names_the_holder(self):
        holder = self.spawn("holder", 10)
        lock = Path(self.env["LEAN_GAP_LOCK"])
        deadline = time.time() + 20
        while time.time() < deadline and not (lock.exists() and lock.read_text().strip()):
            time.sleep(0.05)
        pid = lock.read_text().strip()
        self.assertTrue(pid, "holder never wrote its pid")

        waiter = self.spawn("impatient", 0, env={"LEAN_GAP_LOCK_TIMEOUT": "0.5"})
        _, err = waiter.communicate(timeout=30)
        holder.kill()
        holder.communicate(timeout=30)

        self.assertNotEqual(waiter.returncode, 0, "a timed-out waiter must not look successful")
        self.assertIn("LEAN_GAP_LOCK_TIMEOUT", err)
        self.assertIn(pid, err, f"the failure must name the holder pid {pid}: {err}")
        self.assertIn(SENTINEL, err)
        self.assertNotIn("impatient", self.intervals() if self.log.exists() else {},
                         "a run that never got the lock must not record a measurement")

    def test_the_script_reports_waiting_on_stderr(self):
        holder = self.spawn("holder", 4)
        lock = Path(self.env["LEAN_GAP_LOCK"])
        deadline = time.time() + 20
        while time.time() < deadline and not (lock.exists() and lock.read_text().strip()):
            time.sleep(0.05)
        waiter = self.spawn("patient", 0.1)
        _, err = waiter.communicate(timeout=60)
        holder.communicate(timeout=60)
        self.assertEqual(waiter.returncode, 0, err)
        self.assertIn("waiting for the extraction lock", err)


class DriverSentinelTest(unittest.TestCase):
    """The batch drivers decide `locked` from one line of stderr.

    If the script's give-up message and the drivers' pattern drift apart, a sweep stops behind
    another session's extraction and silently records `nojson`/stale rows instead -- the exact
    "measured nothing, said nothing" failure the lock exists to prevent. Pin the contract.
    """

    def test_every_driver_matches_the_scripts_give_up_message(self):
        script = SCRIPT.read_text()
        self.assertIn(SENTINEL, script, "the script no longer prints the sentinel")
        for name in ("lean_gap_sweep.sh", "lean_gap_sample.sh", "lean_gap_inventory.py"):
            body = (REPO / "scripts" / name).read_text()
            self.assertIn(SENTINEL, body, f"{name} does not recognise a queued run")


if __name__ == "__main__":
    unittest.main()
