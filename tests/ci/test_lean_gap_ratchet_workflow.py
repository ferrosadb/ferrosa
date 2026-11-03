"""The Lean gap ratchet job is a control, so its shape is pinned.

What has to stay true: the pin in the workflow equals the pin in the baseline (a job that
builds one build and compares it against another's numbers is the drift the gate exists to
catch), the measurement stays off the every-push path, and the gate runs `check` -- not
`update`, which is the one command that can raise a ceiling.
"""
from __future__ import annotations

import json
import os
import subprocess
import tempfile
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github/workflows/lean-specs.yml"
BASELINE = ROOT / "specs/lean/gaps/baseline.json"


def load_yaml(path: Path) -> dict:
    try:
        import yaml
    except ImportError:  # pragma: no cover - PyYAML is present in CI here
        raise unittest.SkipTest("PyYAML is not installed")
    return yaml.safe_load(path.read_text())


class RatchetWorkflowTest(unittest.TestCase):
    def setUp(self):
        self.text = WORKFLOW.read_text()
        self.doc = load_yaml(WORKFLOW)
        self.triggers = self.doc[True] if True in self.doc else self.doc["on"]
        self.job = self.doc["jobs"]["ratchet"]

    def test_it_lives_in_the_lean_workflow_and_is_scheduled(self):
        self.assertIn("ratchet", self.doc["jobs"])
        self.assertIn("schedule", self.triggers,
                      "a weekly run is what notices growth on a quiet repo")
        self.assertIn("workflow_dispatch", self.triggers)

    def test_the_ratchet_never_runs_on_push(self):
        # `push` here is the corpus build; the measurement is too expensive for every push.
        push_paths = self.triggers["push"]["paths"]
        self.assertFalse(any("gaps" in p or "lean_gap" in p for p in push_paths))
        self.assertNotIn("push", self.job.get("if", ""))
        pr_paths = self.triggers["pull_request"]["paths"]
        self.assertIn("specs/lean/**", pr_paths)
        self.assertIn("scripts/lean_gap_*.py", pr_paths)

    def test_the_corpus_and_book_jobs_skip_the_weekly_run(self):
        for job in ("specs", "book"):
            self.assertEqual(self.doc["jobs"][job]["if"], "github.event_name != 'schedule'")

    def test_the_pin_matches_the_baseline(self):
        baseline = json.loads(BASELINE.read_text())
        env = self.job["env"]
        self.assertEqual(env["AENEAS_COMMIT"], baseline["toolchain"]["aeneas"]["commit"])
        self.assertEqual(env["CHARON_COMMIT"], baseline["toolchain"]["charon"]["commit"])

    def test_the_gate_runs_check_against_the_pinned_build(self):
        runs = "\n".join(s.get("run", "") for s in self.job["steps"])
        self.assertIn("python3 scripts/lean_gap_ratchet.py check", runs)
        # The measurement has to use the pin in env, not $HOME/src/aeneas on the runner.
        self.assertIn('git -C aeneas checkout "$AENEAS_COMMIT"', runs)
        self.assertIn("./aeneas/bootstrap.sh", runs)
        self.assertIn("lake build", runs)
        self.assertIn("python3 scripts/lean_gap_inventory.py --only", runs)

    def test_bootstrap_installs_the_pinned_charon_formatter_first(self):
        step = next(s for s in self.job["steps"] if s.get("name") == "Build the pinned toolchain")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            charon = root / "aeneas/charon/charon"
            charon.mkdir(parents=True)
            (charon / "rust-toolchain").write_text('[toolchain]\nchannel = "nightly-2026-09-17"\n')
            tools = root / "tools"
            tools.mkdir()
            rustup = tools / "rustup"
            rustup.write_text('#!/bin/sh\nprintf "%s\\n" "$@" > "$FORMATTER_RECEIPT"\n')
            rustup.chmod(0o755)
            bootstrap = root / "aeneas/bootstrap.sh"
            bootstrap.write_text('#!/bin/sh\nset -eu\ntest -s "$FORMATTER_RECEIPT"\ntouch "$AENEAS" "$CHARON"\nchmod +x "$AENEAS" "$CHARON"\n')
            bootstrap.chmod(0o755)
            receipt = root / "formatter-receipt"
            env = {**os.environ, "PATH": str(tools) + os.pathsep + os.environ["PATH"],
                   "AENEAS": str(root / "built-aeneas"), "CHARON": str(root / "built-charon"),
                   "FORMATTER_RECEIPT": str(receipt)}
            result = subprocess.run(["bash", "-euo", "pipefail", "-c", step["run"]],
                                    cwd=root, env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            args = receipt.read_text().splitlines()
            self.assertEqual(args[:3], ["toolchain", "install", "nightly-2026-09-17"])
            self.assertIn("--component", args)
            self.assertIn("rustfmt", args)

    def test_update_only_runs_on_a_dispatch_that_asked_for_it(self):
        for step in self.job["steps"]:
            if "lean_gap_ratchet.py update" in step.get("run", ""):
                self.assertEqual(step.get("if"), "${{ inputs.record_shrink }}")
        self.assertNotIn("--accept-growth", self.text,
                         "CI must never be able to raise a ceiling by itself")

    def test_the_measurement_runs_in_the_checked_out_tree(self):
        # lean_gap_inventory.py defaults REPO to ~/src/ferrosa-suite/ferrosa, which does
        # not exist on a runner; without this the job measures nothing and says so as an
        # empty inventory rather than an error.
        self.assertEqual(self.job["env"]["REPO"], "${{ github.workspace }}")

    def test_the_measurement_is_time_bounded(self):
        # Unbounded in the inventory by default; a job that can hang is not a gate.
        self.assertIn("WHOLE_BUDGET", self.job["env"])
        self.assertIn("timeout-minutes", self.job)

    def test_actions_are_pinned_to_commits(self):
        for uses in re.findall(r"uses:\s*(\S+)", self.text):
            if uses.startswith("./"):
                continue
            self.assertRegex(uses, r"@[0-9a-f]{40}$", f"{uses} is not SHA-pinned")


if __name__ == "__main__":
    unittest.main()
