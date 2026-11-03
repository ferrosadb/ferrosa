"""Tests for translate-only measurement and the step-1 coverage gate.

Two milestones were being reported as one. (1) a crate *translates*: charon and aeneas run and write a
`.lean` file. (2) that Lean *elaborates*: `lake env lean` accepts it. A crate that crashes inside
aeneas and a crate whose Lean is merely wrong both read as "red", so the coverage question was
buried under the expensive one, and only the crates that elaborate cheaply were gated at all.

The properties that matter, and that a well-meaning refactor would quietly lose:

* translate-only never elaborates, and says so -- `elaboration: "skipped"`, no `lean_errors`;
* a crash is recorded as a crash with its exception, never as a measurement with no errors;
* a record that names an empty or missing `.lean` fails the gate, because "translated" is a claim
  about a file;
* the elaboration ratchet does not read `elaboration: "skipped"` as a pass.
"""

import contextlib
import importlib.util
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

REPO = Path(__file__).resolve().parents[2]
SCRIPTS = REPO / "scripts"


def load(name: str, script: Path):
    spec = importlib.util.spec_from_file_location(name, script)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {script} as a module")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


rep = load("lean_gap_report", SCRIPTS / "lean_gap_report.py")
tr = load("lean_gap_translate", SCRIPTS / "lean_gap_translate.py")
ratchet = load("lean_gap_ratchet", SCRIPTS / "lean_gap_ratchet.py")

LEAN = "def ferrosa_demo__run : Nat := 1\n"


class TranslateOnlyReport(unittest.TestCase):
    """`lean_gap_report.py --translate-only` stops after aeneas and records what it saw."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.lean = self.dir / "FerrosaDemo.lean"
        self.lean.write_text(LEAN)
        self.out = self.dir / "record.json"

    def tearDown(self):
        self.tmp.cleanup()

    def run_report(self, extracted: dict, *argv: str, elaborate_result: dict | None = None) -> int:
        """Run the report's main() with extraction mocked out; returns its exit status."""
        with mock.patch.object(rep, "extract", return_value=extracted), \
                mock.patch.object(rep, "single_extraction", contextlib.nullcontext), \
                mock.patch.object(rep, "elaborate",
                                  return_value=elaborate_result or {}) as elaborate, \
                mock.patch("sys.argv", ["lean_gap_report.py", "ferrosa-demo", *argv]), \
                contextlib.redirect_stdout(io.StringIO()):
            code = rep.main()
        self.elaborate = elaborate
        return code

    def test_translate_only_never_elaborates_and_says_so(self):
        code = self.run_report({"stage": "aeneas", "lean": str(self.lean),
                                "translation_errors": 0, "aeneas_exit": 1},
                               "--translate-only", "--out", str(self.out))
        self.assertEqual(code, 0)
        self.elaborate.assert_not_called()

        rec = json.loads(self.out.read_text())
        self.assertEqual(rec["stage"], "aeneas-ok")
        self.assertIs(rec["translated"], True)
        self.assertEqual(rec["elaboration"], "skipped")
        self.assertEqual(rec["lean_bytes"], len(LEAN))
        for absent in ("lean_errors", "errors", "compiled", "sorry_declarations"):
            self.assertNotIn(absent, rec,
                             f"{absent} is a claim a translate-only run did not make")

    def test_check_mode_still_elaborates(self):
        # The new mode must not weaken the old one: without the flag, the elaborator runs.
        code = self.run_report({"stage": "aeneas", "lean": str(self.lean),
                                "translation_errors": 0, "aeneas_exit": 1},
                               "--out", str(self.out),
                               elaborate_result={"lean_errors": 0, "errors": [],
                                                 "sorry_declarations": 1, "compiled": True})
        self.assertEqual(code, 0)
        self.elaborate.assert_called_once()

        rec = json.loads(self.out.read_text())
        self.assertEqual(rec["lean_errors"], 0)
        self.assertIs(rec["compiled"], True)
        self.assertNotIn("translated", rec)

    def test_translate_only_defaults_beside_the_elaboration_records_not_on_them(self):
        # The record path defaults to `specs/lean/gaps/translate/`, never to `specs/lean/gaps/`:
        # a translate-only row in the elaboration directory would replace the error list the
        # class ratchet diffs.
        code = self.run_report({"stage": "aeneas", "lean": str(self.lean)},
                               "--translate-only")
        self.assertEqual(code, 0)
        written = rep.GAPS / "translate" / "ferrosa-demo.json"
        self.assertTrue(written.is_file(), f"{written} was not written")
        written.unlink()

    def test_a_crash_is_a_crash_and_not_zero_errors(self):
        code = self.run_report({"stage": "aeneas-crash",
                                "error": 'Fatal error: exception Invalid_argument("List.combine")',
                                "trace": ['Fatal error: exception Invalid_argument("List.combine")']},
                               "--translate-only", "--out", str(self.out))
        self.assertEqual(code, 1)
        self.elaborate.assert_not_called()

        rec = json.loads(self.out.read_text())
        self.assertEqual(rec["stage"], "aeneas-crash")
        self.assertIs(rec["translated"], False)
        self.assertIn("Invalid_argument", rec["error"])
        for absent in ("lean_errors", "compiled", "sorry_declarations"):
            self.assertNotIn(absent, rec, "a crash has no error count; a zero would read as clean")

    def test_an_empty_lean_file_is_not_a_translation(self):
        self.lean.write_text("")
        code = self.run_report({"stage": "aeneas", "lean": str(self.lean)},
                               "--translate-only", "--out", str(self.out))
        self.assertEqual(code, 1)
        rec = json.loads(self.out.read_text())
        self.assertEqual(rec["stage"], "no-lean")
        self.assertIs(rec["translated"], False)


class GateRules(unittest.TestCase):
    """`lean_gap_translate.py check` is a coverage assertion over the whole workspace."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.lean = self.dir / "FerrosaDemo.lean"
        self.lean.write_text(LEAN)

    def tearDown(self):
        self.tmp.cleanup()

    def row(self, **measurement):
        meas = {"scope": "crate", "elaboration": "skipped", "translated": True,
                "lean": str(self.lean), "lean_bytes": len(LEAN), "stage": "aeneas-ok"}
        meas.update(measurement)
        return {"fingerprint": "fp", "measured_at": "2026-11-03T00:00:00+00:00", "measurement": meas}

    def failures(self, records, workspace=("ferrosa-demo",), excluded=None):
        found, _, _ = tr.check(records, list(workspace), excluded or {}, all_crates=list(workspace))
        return [message for _, message in found]

    def test_a_crate_that_translated_passes(self):
        self.assertEqual(self.failures({"ferrosa-demo": self.row()}), [])

    def test_a_crate_with_no_record_fails_and_is_named(self):
        found = self.failures({})
        self.assertEqual(len(found), 1)
        self.assertIn("ferrosa-demo", found[0])
        self.assertIn("unmeasured", found[0])

    def test_a_missing_lean_file_fails_the_gate(self):
        found = self.failures({"ferrosa-demo": self.row(lean=str(self.dir / "gone.lean"))})
        self.assertEqual(len(found), 1)
        self.assertIn("no-file", found[0])
        self.assertIn("ferrosa-demo", found[0])

    def test_an_empty_lean_file_fails_the_gate(self):
        self.lean.write_text("")
        found = self.failures({"ferrosa-demo": self.row(lean_bytes=0)})
        self.assertEqual(len(found), 1)
        self.assertIn("empty", found[0])

    def test_a_truncated_lean_file_fails_the_gate(self):
        found = self.failures({"ferrosa-demo": self.row(lean_bytes=len(LEAN) * 2)})
        self.assertIn("truncated", found[0])

    def test_a_record_without_a_size_fails_the_gate(self):
        row = self.row()
        del row["measurement"]["lean_bytes"]
        self.assertIn("no-file", self.failures({"ferrosa-demo": row})[0])

    def test_a_crash_fails_the_gate_naming_the_exception(self):
        found = self.failures({"ferrosa-demo": self.row(
            stage="aeneas-crash", translated=False, lean=None, lean_bytes=None,
            error='Fatal error: exception Aeneas.Errors.CFailure(_)')})
        self.assertEqual(len(found), 1)
        self.assertIn("crash", found[0])
        self.assertIn("ferrosa-demo", found[0])
        self.assertIn("CFailure", found[0], "the exception is the diagnosis; name it")

    def test_an_unmeasured_stage_fails_the_gate(self):
        for stage in ("oom", "timeout", "locked"):
            found = self.failures({"ferrosa-demo": self.row(stage=stage, translated=False)})
            self.assertEqual(len(found), 1, stage)
            self.assertIn("unmeasured", found[0], stage)
            self.assertIn(stage, found[0], stage)

    def test_an_elaboration_record_is_not_accepted_as_a_translation(self):
        # Point the gate at the wrong directory and it must say so, not read `lean_errors: 0`
        # as "translated".
        row = {"fingerprint": "fp", "measured_at": "now",
               "measurement": {"scope": "crate", "stage": "aeneas", "lean_errors": 0}}
        found = self.failures({"ferrosa-demo": row})
        self.assertEqual(len(found), 1)
        self.assertIn("wrong-mode", found[0])

    def test_a_listed_crate_is_known_unmeasured_rather_than_absent(self):
        found, known, _ = tr.check({"ferrosa-demo": self.row()}, ["ferrosa-demo", "ferrosa-big"],
                                   {"ferrosa-big": "25 GiB in one aeneas run"})
        self.assertEqual(found, [])
        self.assertEqual(len(known), 1)
        self.assertIn("known unmeasured: ferrosa-big -- 25 GiB", known[0])

    def test_a_stale_exclusion_fails_the_gate(self):
        found, _, _ = tr.check({"ferrosa-demo": self.row()}, ["ferrosa-demo"],
                               {"ferrosa-gone": "removed from the workspace"})
        self.assertEqual(len(found), 1)
        self.assertIn("stale", found[0][1])

    def test_only_narrows_the_assertion_without_inventing_stale_entries(self):
        # The workspace still has ferrosa-big; `--only` merely does not assert on it this run.
        found, known, _ = tr.check({"ferrosa-demo": self.row()}, ["ferrosa-demo"],
                                   {"ferrosa-big": "too big"}, all_crates=["ferrosa-demo",
                                                                           "ferrosa-big"])
        self.assertEqual(found, [])
        self.assertEqual(known, [], "an excluded crate outside --only is neither asserted nor stale")

    def test_every_workspace_crate_is_asserted(self):
        # The gate's scope is the workspace, not the 19 crates the class ratchet gates: a crate
        # that is missing from the records must be named even when nothing else is wrong.
        workspace = ["ferrosa-a", "ferrosa-b", "ferrosa-c"]
        found, _, _ = tr.check({"ferrosa-a": self.row()}, workspace, {}, all_crates=workspace)
        self.assertEqual(sorted(c for c, _ in found), ["ferrosa-b", "ferrosa-c"])


class OldGateIsNotFooled(unittest.TestCase):
    """`elaboration: "skipped"` must never be read as a pass by the elaboration ratchet.

    The ratchet compares class counts against a baseline, and a translate-only record has no
    counts at all. The property is that it treats such a row as *not concluded*, so a crate that
    used to elaborate and now only translates is a coverage regression rather than a clean run.
    """

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.inventory = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def write(self, stage: str, **row) -> None:
        state = {"toolchain": {}, "crates": {"ferrosa-demo": {
            "fingerprint": "fp", "measured_at": "2026-11-03T00:00:00+00:00",
            "measurement": {"scope": "crate", "stage": stage, "elaboration": "skipped",
                            "translated": stage == "aeneas-ok", **row}}}}
        (self.inventory / "state.json").write_text(json.dumps(state))
        (self.inventory / "ferrosa-demo.json").write_text(json.dumps(
            {"crate": "ferrosa-demo", "stage": stage, "elaboration": "skipped",
             "translated": stage == "aeneas-ok", **row}))

    def test_a_translate_only_row_is_not_concluded(self):
        self.write("aeneas-ok", lean_bytes=10)
        entry = ratchet.read_records(self.inventory)["ferrosa-demo"]
        self.assertFalse(entry["concluded"],
                         "a crate with no elaboration has no counts; it cannot be a pass")

    def test_a_translate_only_crash_stays_a_crash_and_never_becomes_zero_errors(self):
        # `aeneas-crash` *is* a concluded measurement -- it is deterministic and re-runs the same
        # way -- so the ratchet keeps it as one. What matters is that it stays a crash class out
        # of the exception, not an empty count that would read as a clean crate.
        self.write("aeneas-crash", error="Fatal error: exception Invalid_argument(\"List.combine\")")
        entry = ratchet.read_records(self.inventory)["ferrosa-demo"]
        self.assertEqual(ratchet.outcome(entry), "crash")
        self.assertEqual(entry["classes"], {"crash.Invalid_argument": 1},
                         "a crash is classed by its exception, not counted as no reds")

    def test_a_translate_only_row_is_not_recorded_as_a_ceiling(self):
        # A row with no counts has no ceiling to hold. `update` skips anything that did not
        # conclude, so a translate-only success can never arrive in the baseline as "0 reds".
        self.write("aeneas-ok", lean_bytes=10)
        records = ratchet.read_records(self.inventory)
        gated = [c for c in records if records[c].get("concluded")]
        self.assertEqual(gated, [], "a translate-only success must not be gated as a class baseline")

    def test_a_concluded_baseline_plus_a_translate_only_row_is_coverage_growth(self):
        self.write("aeneas-ok", lean_bytes=10)
        fresh = ratchet.read_records(self.inventory)
        baseline = {"crates": {"ferrosa-demo": {"scope": "crate", "stage": "aeneas",
                                                "concluded": True,
                                                "classes": {"unknownIdentifier": 3}}}}
        growth, _, _ = ratchet.compare(baseline, fresh)
        self.assertEqual(len(growth), 1)
        self.assertIn("FAIL coverage: ferrosa-demo", growth[0])
        self.assertIn("unreported", growth[0])


if __name__ == "__main__":
    unittest.main()
