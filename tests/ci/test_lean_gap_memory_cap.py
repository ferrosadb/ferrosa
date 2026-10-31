"""Tests for the per-extraction memory ceiling in scripts/lean_gap_report.py.

The lock stops concurrent extractions; this stops one extraction from taking the host down alone.
A whole-crate charon run has reached 24.4 GB resident, and a launcher with no limits of its own --
a systemd user unit is the one that did it -- is not a constraint the scan can rely on. So every
child runs inside a cgroup scope with `MemoryMax`, the kill lands inside that scope, and the parent
survives to record `oom` instead of dying with it.

The two real cases run actual scopes and read the kernel's own numbers, because the failure that
matters is a ceiling that looks configured and is not.
"""

import contextlib
import importlib.util
import io
import os
import sys
import unittest
from pathlib import Path
from unittest import mock

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "lean_gap_report.py"


def load_report():
    spec = importlib.util.spec_from_file_location("lean_gap_report", SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {SCRIPT} as a module")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


rep = load_report()

# Prints the memory ceiling this process is actually running under, from its own cgroup.
CEILING_CHILD = (
    "import pathlib\n"
    "path = pathlib.Path('/proc/self/cgroup').read_text().strip().split('::', 1)[1]\n"
    "print(pathlib.Path('/sys/fs/cgroup' + path + '/memory.max').read_text().strip())\n"
)

# Asks for more than the ceiling in one allocation; the kernel kills it well before that.
OVER_CAP_CHILD = "x = bytearray(2 * 1024 * 1024 * 1024); print(len(x))\n"


class CapDefaultTest(unittest.TestCase):
    def test_the_default_leaves_the_rest_of_the_box_a_working_set(self):
        total_kb = next(int(line.split()[1]) for line in
                        Path("/proc/meminfo").read_text().splitlines()
                        if line.startswith("MemTotal:"))
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("LEAN_GAP_MEM_MAX", None)
            self.assertEqual(rep.mem_cap(), f"{total_kb * 6 // 10 // (1024 * 1024)}G")

    def test_an_explicit_ceiling_is_used_verbatim(self):
        with mock.patch.dict(os.environ, {"LEAN_GAP_MEM_MAX": "12G"}):
            self.assertEqual(rep.mem_cap(), "12G")

    def test_the_ceiling_can_be_turned_off_and_the_command_is_left_alone(self):
        for off in ("0", "none", "off", ""):
            with mock.patch.dict(os.environ, {"LEAN_GAP_MEM_MAX": off}):
                self.assertIsNone(rep.mem_cap())
                self.assertEqual(rep.bounded(["sleep", "0"]), ["sleep", "0"])


class KillAttributionTest(unittest.TestCase):
    def test_a_scope_kill_only_counts_when_a_ceiling_was_in_force(self):
        self.assertTrue(rep.killed_by_cap(137, "18G"))
        self.assertTrue(rep.killed_by_cap(-9, "18G"))
        self.assertFalse(rep.killed_by_cap(137, None), "unbounded SIGKILL is somebody else's kill")
        self.assertFalse(rep.killed_by_cap(1, "18G"))


class MissingScopeTest(unittest.TestCase):
    def test_an_absent_user_scope_is_announced_once_and_the_run_continues(self):
        err = io.StringIO()
        with mock.patch.object(rep, "scope_available", return_value=False), \
                mock.patch.object(rep, "_WARNED_UNBOUNDED", False), \
                mock.patch.dict(os.environ, {"LEAN_GAP_MEM_MAX": "1G"}), \
                contextlib.redirect_stderr(err):
            self.assertEqual(rep.bounded(["sleep", "0"]), ["sleep", "0"])
            rep.bounded(["sleep", "0"])
        self.assertEqual(err.getvalue().count("running children unbounded"), 1,
                         f"the warning must be said once, not repeated: {err.getvalue()}")


@unittest.skipUnless(rep.scope_available(), "no user cgroup scope on this host")
class RealScopeTest(unittest.TestCase):
    def test_a_child_really_gets_the_ceiling(self):
        with mock.patch.dict(os.environ, {"LEAN_GAP_MEM_MAX": "256M"}):
            res = rep.run([sys.executable, "-c", CEILING_CHILD])
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertEqual(res.stdout.strip(), str(256 * 1024 * 1024),
                         f"the child's cgroup ceiling is not the configured one: {res.stdout!r}")

    def test_exceeding_the_ceiling_kills_the_child_and_not_this_process(self):
        # Swap is set to zero here on purpose: the ceiling bounds RAM, and `LEAN_GAP_MEM_SWAP_MAX`
        # lets a child spill past it by that much before the kill. The property under test is the
        # kill, not the spill allowance, so remove the allowance rather than sizing around it.
        with mock.patch.dict(os.environ, {"LEAN_GAP_MEM_MAX": "256M",
                                          "LEAN_GAP_MEM_SWAP_MAX": "0"}):
            res = rep.run([sys.executable, "-c", OVER_CAP_CHILD])
            self.assertTrue(rep.killed_by_cap(res.returncode, rep.mem_cap()),
                            f"expected a capped kill, got rc={res.returncode} err={res.stderr!r}")


if __name__ == "__main__":
    unittest.main()
