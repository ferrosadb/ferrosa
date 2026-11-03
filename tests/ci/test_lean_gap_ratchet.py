"""The Lean gap ratchet: growth fails, shrinkage passes, mismatched toolchains fail.

Each case builds a two-directory inventory in a temporary directory (a `state.json` plus a
per-crate record) and runs the script as a subprocess, because the behaviour that matters
is its exit status and the text an operator reads in a failed CI job.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "scripts/lean_gap_ratchet.py"


def record(crate: str = "ferrosa-cdc", *, lean_errors: int = 2, stage: str = "aeneas",
           instances: list[str] | None = None, sorry: int = 0) -> dict:
    errors = [{"location": loc, "kind": "lean.unknownIdentifier",
               "message": "Unknown identifier `x`"}
              for loc in (instances if instances is not None else
                          [f"{crate}.lean:{i}:2" for i in range(lean_errors)])]
    return {"stage": stage, "scope": "crate", "translation_errors": 0,
            "lean_errors": len(errors), "errors": errors,
            "sorry_declarations": sorry, "compiled": not errors}


class Fixture:
    """An inventory directory plus the baseline that check runs against."""

    def __init__(self, root: Path):
        self.root = root
        self.inventory = root / "gaps"
        self.inventory.mkdir()
        self.baseline = root / "baseline.json"
        self.state = self.inventory / "state.json"
        self.scope = root / "ratchet-scope.txt"

    def write(self, records: dict[str, dict], toolchain: dict | None = None) -> None:
        crates = {}
        for crate, rec in records.items():
            self.inventory.joinpath(f"{crate}.json").write_text(json.dumps(rec))
            crates[crate] = {
                "fingerprint": "deadbeef",
                "measured_at": "2026-10-31T23:23:24+00:00",
                "measurement": {k: rec.get(k) for k in
                                ("stage", "scope", "translation_errors", "lean_errors",
                                 "sorry_declarations")},
            }
            crates[crate]["measurement"]["sorry"] = rec.get("sorry_declarations")
        self.state.write_text(json.dumps({
            "toolchain": toolchain or {"charon": "7b462d9159db2ed3015162352183906a30bfafc6",
                                       "aeneas": "80b690a9ad1ef1fb97fee23ef1e1c9486ff8f8b9",
                                       "lean": "Lean (version 4.35.0, x86_64-unknown-linux-gnu,"
                                               " commit eb6cafb7290e8bd5f2937522d8d804f58f02bafc,"
                                               " Release)"},
            "crates": crates,
        }))

    def run(self, *args: str, env: dict | None = None) -> subprocess.CompletedProcess:
        cmd = [sys.executable, str(SCRIPT), "--inventory", str(self.inventory),
               "--baseline", str(self.baseline), "--state", str(self.state),
               "--scope", str(self.scope), *args]
        return subprocess.run(cmd, capture_output=True, text=True,
                              env={**os.environ, **(env or {})})


class RatchetTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.fx = Fixture(Path(self._tmp.name))

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def baseline_of(self, records: dict[str, dict], toolchain: dict | None = None) -> None:
        self.fx.write(records, toolchain)
        done = self.fx.run("update", "--no-verify-build")
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)

    # ------------------------------------------------------------ baseline

    def test_defaults_follow_the_inventory_directory(self):
        """--inventory alone must not read the repository's state.json and baseline."""
        self.baseline_of({"ferrosa-cdc": record()})
        (self.fx.inventory / "baseline.json").write_text(self.fx.baseline.read_text())
        done = subprocess.run([sys.executable, str(SCRIPT), "--inventory", str(self.fx.inventory),
                               "check", "--no-verify-build"], capture_output=True, text=True)
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)

    def test_unchanged_inventory_passes(self):
        self.baseline_of({"ferrosa-cdc": record()})
        done = self.fx.run("check", "--no-verify-build")
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertIn("no class grew", done.stdout)

    def test_growth_fails_naming_class_counts_and_new_location(self):
        self.baseline_of({"ferrosa-common": record(
            "ferrosa-common", lean_errors=2,
            instances=["FerrosaCommon.lean:4609:2", "FerrosaCommon.lean:4610:2"])})
        self.fx.write({"ferrosa-common": record(
            "ferrosa-common", lean_errors=3,
            instances=["FerrosaCommon.lean:4609:2", "FerrosaCommon.lean:4610:2",
                       "FerrosaCommon.lean:4612:2"])})
        done = self.fx.run("check", "--no-verify-build")
        self.assertEqual(done.returncode, 1, done.stdout + done.stderr)
        self.assertIn("FAIL growth: ferrosa-common lean.unknownIdentifier 2 -> 3 (+1)",
                      done.stdout)
        self.assertIn("new at FerrosaCommon.lean:4612:2", done.stdout)
        self.assertIn("::error title=Lean gap ratchet::", done.stdout)
        # The instance that was already there must not be reported as new.
        self.assertNotIn("new at FerrosaCommon.lean:4609:2", done.stdout)

    def test_new_class_counts_as_growth_from_zero(self):
        self.baseline_of({"ferrosa-cdc": record(lean_errors=0)})
        rec = record(lean_errors=1, instances=["FerrosaCdc.lean:7:1"])
        rec["errors"][0]["kind"] = "lean.synthInstanceFailed"
        self.fx.write({"ferrosa-cdc": rec})
        done = self.fx.run("check", "--no-verify-build")
        self.assertEqual(done.returncode, 1)
        self.assertIn("lean.synthInstanceFailed 0 -> 1", done.stdout)

    # ------------------------------------------------------------ shrinkage

    def test_shrink_passes_and_update_records_it(self):
        self.baseline_of({"ferrosa-cdc": record(lean_errors=5)})
        self.fx.write({"ferrosa-cdc": record(lean_errors=3)})
        done = self.fx.run("check", "--no-verify-build")
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertIn("closed: ferrosa-cdc lean.unknownIdentifier 5 -> 3", done.stdout)

        updated = self.fx.run("update", "--no-verify-build")
        self.assertEqual(updated.returncode, 0, updated.stdout + updated.stderr)
        self.assertIn("recorded: closed: ferrosa-cdc lean.unknownIdentifier 5 -> 3",
                      updated.stdout)
        after = json.loads(self.fx.baseline.read_text())
        self.assertEqual(after["crates"]["ferrosa-cdc"]["classes"]["lean.unknownIdentifier"], 3)
        self.assertEqual(self.fx.run("check", "--no-verify-build").returncode, 0)

    def test_update_refuses_to_ratchet_growth_up(self):
        self.baseline_of({"ferrosa-cdc": record(lean_errors=2)})
        self.fx.write({"ferrosa-cdc": record(lean_errors=4)})
        done = self.fx.run("update", "--no-verify-build")
        self.assertEqual(done.returncode, 1)
        self.assertIn("refusing to ratchet the baseline up", done.stdout)
        self.assertEqual(
            json.loads(self.fx.baseline.read_text())["crates"]["ferrosa-cdc"]["classes"]
            ["lean.unknownIdentifier"], 2)

    def test_update_may_record_growth_when_asked(self):
        self.baseline_of({"ferrosa-cdc": record(lean_errors=2)})
        self.fx.write({"ferrosa-cdc": record(lean_errors=4)})
        done = self.fx.run("update", "--no-verify-build", "--accept-growth")
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertIn("accepted: FAIL growth: ferrosa-cdc", done.stdout)

    # ------------------------------------------------------------ coverage

    def test_unmeasured_crate_fails_closed(self):
        self.baseline_of({"ferrosa-cdc": record(), "ferrosa-sched": record("ferrosa-sched")})
        self.fx.write({"ferrosa-cdc": record()})
        done = self.fx.run("check", "--no-verify-build")
        self.assertEqual(done.returncode, 1)
        self.assertIn("FAIL unmeasured: ferrosa-sched", done.stdout)

    def test_coverage_regression_fails_even_with_accept_growth(self):
        self.baseline_of({"ferrosa-cdc": record()})
        crash = {"stage": "aeneas-crash", "scope": "crate", "error": "Fatal error: exception Not_found",
                 "lean_errors": None, "translation_errors": None}
        self.fx.write({"ferrosa-cdc": crash})
        done = self.fx.run("check", "--no-verify-build")
        self.assertEqual(done.returncode, 1)
        self.assertIn("FAIL coverage: ferrosa-cdc elaborated in the baseline", done.stdout)

        updated = self.fx.run("update", "--no-verify-build", "--accept-growth")
        self.assertEqual(updated.returncode, 1)
        self.assertIn("refusing to record a baseline that drops a crate", updated.stdout)

    def test_crash_changes_class_when_the_exception_changes(self):
        self.baseline_of({"ferrosa-cdc": {"stage": "aeneas-crash", "scope": "crate",
                                          "error": "Fatal error: exception Not_found"}})
        self.fx.write({"ferrosa-cdc": {"stage": "aeneas-crash", "scope": "crate",
                                       "error": "Fatal error: exception Failure"}})
        done = self.fx.run("check", "--no-verify-build")
        self.assertEqual(done.returncode, 1)
        self.assertIn("crash.Not_found 1 -> 0", done.stdout)
        self.assertIn("crash.Failure 0 -> 1", done.stdout)

    # ------------------------------------------------------------ drift

    def test_state_json_from_another_build_fails(self):
        self.baseline_of({"ferrosa-cdc": record()})
        self.fx.write({"ferrosa-cdc": record()},
                      toolchain={"charon": "7b462d9159db2ed3015162352183906a30bfafc6",
                                 "aeneas": "d8480a0ecb47986a86fa556e2d18148dca23e1e6",
                                 "lean": "Lean (version 4.35.0, x86_64-unknown-linux-gnu, "
                                         "commit eb6cafb7290e8bd5f2937522d8d804f58f02bafc, Release)"})
        done = self.fx.run("check", "--no-verify-build")
        self.assertEqual(done.returncode, 1)
        self.assertIn("FAIL toolchain drift: state.json records aeneas d8480a0e", done.stdout)
        self.assertIn("refusing to compare", done.stdout)

    def test_charon_drift_fails(self):
        self.baseline_of({"ferrosa-cdc": record()})
        self.fx.write({"ferrosa-cdc": record()},
                      toolchain={"charon": "0" * 40,
                                 "aeneas": "80b690a9ad1ef1fb97fee23ef1e1c9486ff8f8b9",
                                 "lean": "Lean (version 4.35.0, x86_64-unknown-linux-gnu, "
                                         "commit eb6cafb7290e8bd5f2937522d8d804f58f02bafc, Release)"})
        done = self.fx.run("check", "--no-verify-build")
        self.assertEqual(done.returncode, 1)
        self.assertIn("FAIL toolchain drift: state.json records charon", done.stdout)

    def test_built_checkout_that_is_not_the_pin_fails(self):
        """The binary on this machine is not the one the baseline describes."""
        self.baseline_of({"ferrosa-cdc": record()})
        with tempfile.TemporaryDirectory() as other:
            repo = Path(other)
            subprocess.run(["git", "init", "-q", str(repo)], check=True)
            subprocess.run(["git", "-C", str(repo), "-c", "user.email=t@t", "-c",
                            "user.name=t", "commit", "-q", "--allow-empty", "-m", "x"],
                           check=True)
            backend = repo / "backends/lean"
            backend.mkdir(parents=True)
            (backend / "lean-toolchain").write_text("leanprover/lean4:v4.35.0\n")
            lean = repo / "lean"
            lean.write_text("#!/bin/sh\necho 'Lean (version 4.35.0)'\n")
            lean.chmod(0o755)
            done = self.fx.run("check", env={"AENEAS_SRC": str(repo), "CHARON_SRC": str(repo),
                                             "LEAN_PKG": str(backend), "LEAN": str(lean)})
        self.assertEqual(done.returncode, 1, done.stdout + done.stderr)
        self.assertIn("FAIL toolchain drift: the aeneas that was built is", done.stdout)

    def test_merge_commit_with_the_pinned_tree_is_not_drift(self):
        """A merge whose tree equals the pin is the pin: content is the identity."""
        lean_line = ("Lean (version 4.35.0, x86_64-unknown-linux-gnu, commit "
                     "eb6cafb7290e8bd5f2937522d8d804f58f02bafc, Release)")
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp) / "aeneas"
            fake_lean = Path(tmp) / "lean"
            fake_lean.write_text(f"#!/bin/sh\necho '{lean_line}'\n")
            fake_lean.chmod(0o755)
            backend = repo / "backends/lean"
            backend.mkdir(parents=True)
            (backend / "lean-toolchain").write_text("leanprover/lean4:v4.35.0\n")
            subprocess.run(["git", "init", "-q", str(repo)], check=True)
            (repo / "f").write_text("1")
            for cmd in (["add", "f"], ["commit", "-q", "-m", "one"]):
                subprocess.run(["git", "-C", str(repo), "-c", "user.email=t@t", "-c",
                                "user.name=t", *cmd], check=True)
            parent = subprocess.run(["git", "-C", str(repo), "rev-parse", "HEAD"],
                                    capture_output=True, text=True, check=True).stdout.strip()
            env = {"AENEAS_SRC": str(repo), "CHARON_SRC": str(repo), "LEAN": str(fake_lean)}
            self.fx.write({"ferrosa-cdc": record()},
                          toolchain={"charon": parent, "aeneas": parent, "lean": lean_line})
            done = self.fx.run("update", env=env)
            self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
            baseline = json.loads(self.fx.baseline.read_text())
            self.assertEqual(baseline["toolchain"]["aeneas"]["commit"], parent)

            # A merge commit that changed nothing: different id, same tree. The measurement
            # is still valid, so this is not drift, and refusing it would fail the gate on
            # the merge that landed the pinned work.
            subprocess.run(["git", "-C", str(repo), "-c", "user.email=t@t", "-c",
                            "user.name=t", "commit", "-q", "--allow-empty", "-m", "merge"],
                           check=True)
            head = subprocess.run(["git", "-C", str(repo), "rev-parse", "HEAD"],
                                  capture_output=True, text=True, check=True).stdout.strip()
            self.assertNotEqual(head, parent)
            self.fx.write({"ferrosa-cdc": record()},
                          toolchain={"charon": head, "aeneas": head, "lean": lean_line})
            done = self.fx.run("check", env=env)
            self.assertEqual(done.returncode, 0, done.stdout + done.stderr)

    def test_a_killed_run_with_no_counts_is_not_a_measurement(self):
        """`aeneas` with no number is a run that died, not a crate with zero reds."""
        self.baseline_of({"ferrosa-view": record("ferrosa-view", lean_errors=4)})
        killed = {"stage": "aeneas", "scope": "crate", "translation_errors": None,
                  "lean_errors": None, "sorry_declarations": None,
                  "error": "no .lean produced"}
        self.fx.write({"ferrosa-view": killed})
        done = self.fx.run("check", "--no-verify-build")
        self.assertEqual(done.returncode, 1)
        self.assertIn("FAIL coverage: ferrosa-view", done.stdout)

    # ------------------------------------------------------------ scope

    def test_a_crate_declared_out_of_scope_is_not_gated(self):
        self.fx.scope.write_text("# crates the runner cannot measure\n"
                                 "ferrosa-cdc 21.5 GB resident in charon\n")
        self.fx.write({"ferrosa-cdc": record(), "ferrosa-sched": record("ferrosa-sched")})
        done = self.fx.run("update", "--no-verify-build")
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertIn("not gated (ratchet-scope.txt): ferrosa-cdc", done.stdout)
        self.assertEqual(list(json.loads(self.fx.baseline.read_text())["crates"]),
                         ["ferrosa-sched"])

    def test_a_gated_crate_declared_out_of_scope_fails(self):
        self.baseline_of({"ferrosa-cdc": record()})
        self.fx.scope.write_text("ferrosa-cdc too big for the runner\n")
        done = self.fx.run("check", "--no-verify-build")
        self.assertEqual(done.returncode, 1)
        self.assertIn("FAIL scope: ferrosa-cdc is in the baseline", done.stdout)

    # ------------------------------------------------------------ detail

    def test_missing_detail_still_ratchets_the_total(self):
        """A count without an error list must not become an unwatched number."""
        self.baseline_of({"ferrosa-cdc": record(lean_errors=2)})
        self.fx.write({"ferrosa-cdc": record(lean_errors=3)})
        (self.fx.inventory / "ferrosa-cdc.json").unlink()
        done = self.fx.run("check", "--no-verify-build")
        self.assertEqual(done.returncode, 1)
        self.assertIn("lean.unclassified 0 -> 3", done.stdout)

    def test_modules_are_compared_module_by_module(self):
        state = {"toolchain": {"charon": "7b462d9159db2ed3015162152183906a30bfafc6",
                               "aeneas": "80b690a9", "lean": "Lean (version 4.35.0, x86_64, commit eb6cafb, Release)"},
                 "crates": {"ferrosa-schema": {"fingerprint": "x", "measured_at": "t",
                    "measurement": {"scope": "modules (1/1)", "stage": "modules",
                                    "translation_errors": None, "lean_errors": None,
                                    "sorry": None, "seconds": 1,
                                    "modules": {"audit": {"stage": "aeneas",
                                                          "lean_errors": 2,
                                                          "translation_errors": 0,
                                                          "sorry": 0, "seconds": 1}}}}}}
        self.fx.state.write_text(json.dumps(state))
        detail = {"stage": "aeneas", "lean_errors": 2, "errors": [
            {"location": "Audit.lean:1:1", "kind": "lean.unknownIdentifier", "message": "x"},
            {"location": "Audit.lean:2:1", "kind": "lean.unknownIdentifier", "message": "x"}]}
        (self.fx.inventory / "ferrosa-schema.audit.json").write_text(json.dumps(detail))
        self.assertEqual(self.fx.run("update", "--no-verify-build").returncode, 0)
        detail["errors"].append({"location": "Audit.lean:3:1",
                                 "kind": "lean.unknownIdentifier", "message": "x"})
        detail["lean_errors"] = 3
        (self.fx.inventory / "ferrosa-schema.audit.json").write_text(json.dumps(detail))
        state["crates"]["ferrosa-schema"]["measurement"]["modules"]["audit"]["lean_errors"] = 3
        self.fx.state.write_text(json.dumps(state))
        done = self.fx.run("check", "--no-verify-build")
        self.assertEqual(done.returncode, 1)
        self.assertIn("ferrosa-schema::audit lean.unknownIdentifier 2 -> 3", done.stdout)
        self.assertIn("new at Audit.lean:3:1", done.stdout)


if __name__ == "__main__":
    unittest.main()
