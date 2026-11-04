"""The Lean gap ratchet job is a control, so its shape is pinned.

What has to stay true: the pin in the workflow equals the pin in the baseline (a job that
builds one build and compares it against another's numbers is the drift the gate exists to
catch), the measurement stays off the every-push path, and the gate runs `check` -- not
`update`, which is the one command that can raise a ceiling.

Correctness: execute checkout/bootstrap contracts against disposable tools and Git sources.
Last revised: 2026-11-03
Last changed: Reproduce opam's local fetch from the workflow's pinned Charon checkout.
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
        self.assertIn("python3 scripts/lean_gap_bootstrap.py aeneas", runs)
        self.assertIn("lake build", runs)
        self.assertIn("python3 scripts/lean_gap_inventory.py --only", runs)

    def test_charon_checkout_serves_the_local_git_fetch_used_by_opam(self):
        step = next(s for s in self.job["steps"] if s.get("name") == "Fetch the pinned aeneas")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            # Fixture repositories must not inherit operator Git hooks, signing,
            # credentials, or overrides of upload-pack's lazy-fetch policy.
            env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
            env.update({"GIT_CONFIG_GLOBAL": os.devnull,
                        "GIT_CONFIG_NOSYSTEM": "1", "GIT_TERMINAL_PROMPT": "0"})

            def git(repository, *arguments):
                return subprocess.run(["git", "-C", str(repository), *arguments],
                                      env=env, capture_output=True, text=True, check=True)

            def origin(name, files):
                repository = root / (name + "-origin")
                subprocess.run(["git", "init", "-q", "-b", "main", str(repository)],
                               env=env, capture_output=True, text=True, check=True)
                for key, value in [("user.name", "CI Fixture"),
                                   ("user.email", "fixture@example.invalid"),
                                   ("commit.gpgsign", "false"),
                                   ("uploadpack.allowFilter", "true")]:
                    git(repository, "config", key, value)
                # Checkout needs only the second commit's files. A downstream
                # unfiltered fetch also needs this retired historical blob.
                historical = repository / "historical-only.txt"
                historical.write_text("historical source not present at the pin\n")
                git(repository, "add", ".")
                git(repository, "commit", "-qm", "historical source")
                historical_blob = git(repository, "rev-parse", "HEAD:historical-only.txt").stdout.strip()
                historical.unlink()
                for path, content in files.items():
                    output = repository / path
                    output.parent.mkdir(parents=True, exist_ok=True)
                    output.write_text(content)
                git(repository, "add", "-A")
                git(repository, "commit", "-qm", "pinned source")
                return repository, git(repository, "rev-parse", "HEAD").stdout.strip(), historical_blob

            charon, charon_pin, historical_blob = origin("charon", {
                "name_matcher_parser.opam": 'opam-version: "2.0"\nversion: "0.1"\n',
            })
            aeneas, aeneas_pin, _ = origin("aeneas", {"charon-pin": charon_pin + "\n"})
            workspace = root / "workspace"
            workspace.mkdir()
            fetched = subprocess.run(["bash", "-euo", "pipefail", "-c", step["run"]],
                                     cwd=workspace, capture_output=True, text=True,
                                     env={**env, "AENEAS_REPO": aeneas.as_uri(),
                                          "AENEAS_COMMIT": aeneas_pin,
                                          "CHARON_REPO": charon.as_uri(),
                                          "CHARON_COMMIT": charon_pin})
            self.assertEqual(fetched.returncode, 0, fetched.stdout + fetched.stderr)
            checkout = workspace / "aeneas/charon"
            self.assertEqual(git(checkout, "rev-parse", "HEAD").stdout.strip(), charon_pin)
            self.assertEqual(git(workspace / "aeneas", "rev-parse", "HEAD").stdout.strip(), aeneas_pin)
            # bootstrap.sh installs the sibling .opam file from the Git checkout.
            # Opam then runs this ordinary fetch against git+file://...#HEAD.
            source = root / "opam-switch/sources/name_matcher_parser"
            source.mkdir(parents=True)
            git(source, "init", "-q")
            git(source, "remote", "add", "origin", checkout.as_uri())
            transfer = subprocess.run(["git", "-C", str(source), "fetch", "-q"],
                                      env=env, capture_output=True, text=True)
            self.assertEqual(transfer.returncode, 0,
                             "opam's local Git source must be self-contained: " + transfer.stderr)
            self.assertEqual(git(source, "rev-parse", "FETCH_HEAD").stdout.strip(), charon_pin)
            git(source, "cat-file", "-e", historical_blob)

    def test_bootstrap_installs_the_pinned_charon_formatter_first(self):
        step = next(s for s in self.job["steps"] if s.get("name") == "Build the pinned toolchain")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            scripts = root / "scripts"
            scripts.mkdir()
            (scripts / "lean_gap_bootstrap.py").write_bytes(
                (ROOT / "scripts/lean_gap_bootstrap.py").read_bytes())
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
            # A different build failure must be reported, not retried after setup.
            calls = root / "failed-bootstrap-calls"
            bootstrap.write_text('#!/bin/sh\nprintf x >> "$FAILED_BOOTSTRAP_CALLS"\nexit 23\n')
            failure = subprocess.run(["bash", "-euo", "pipefail", "-c", step["run"]],
                                     cwd=root, env={**env, "FAILED_BOOTSTRAP_CALLS": str(calls)},
                                     capture_output=True, text=True)
            self.assertEqual(failure.returncode, 23, failure.stdout + failure.stderr)
            self.assertEqual(calls.read_text(), "x", "failed bootstrap must execute exactly once")

    def test_cache_upload_failure_does_not_replace_the_semantic_gate(self):
        save = next(s for s in self.job["steps"] if s.get("name") == "Save the verified toolchain")
        self.assertIs(save.get("continue-on-error"), True)
        self.assertNotIn("always()", save["if"])
        check = next(s for s in self.job["steps"] if "lean_gap_ratchet.py check" in s.get("run", ""))
        self.assertFalse(check.get("continue-on-error", False))

    def test_completed_toolchain_is_saved_before_measurement(self):
        steps = self.job["steps"]
        restore = next(s for s in steps if s.get("id") == "toolchain-cache")
        save = next(s for s in steps if s.get("name") == "Save the verified toolchain")
        self.assertIn("actions/cache/restore@", restore["uses"])
        self.assertIn("actions/cache/save@", save["uses"])
        self.assertEqual(save["with"]["path"], restore["with"]["path"])
        self.assertEqual(save["with"]["key"], "${{ steps.toolchain-cache.outputs.cache-primary-key }}")
        self.assertEqual(save["if"], "${{ success() && steps.toolchain-cache.outputs.cache-hit != 'true' }}")
        order = {s.get("name"): i for i, s in enumerate(steps)}
        self.assertLess(order["Build the pinned toolchain"], order["Save the verified toolchain"])
        self.assertLess(order["Build the Aeneas Lean backend"], order["Save the verified toolchain"])
        self.assertLess(order["Save the verified toolchain"], order["Re-measure the gated crates"])

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
