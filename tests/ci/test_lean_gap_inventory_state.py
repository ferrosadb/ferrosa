"""Tests for the cached-measurement rules in scripts/lean_gap_inventory.py.

An extraction costs 20-240 s and up to 24 GB of resident memory, so a cache dropped for the wrong
reason is not a slowdown -- it is the OOM this suite has already taken the host down for. Two rules
protect it: an `unknown` toolchain field is a resolution failure rather than a different compiler,
and a row keeps its measurement across an identity change only when that crate's own sources
provably did not move.

Correctness: project-selected Lean identity and cached measurements stay reproducible.
Last revised: 2026-11-03
Last changed: Exercise Lean project selection without a global default through subprocesses.
"""

import importlib.util
import datetime
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "lean_gap_inventory.py"


def load_inventory():
    spec = importlib.util.spec_from_file_location("lean_gap_inventory", SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {SCRIPT} as a module")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


inv = load_inventory()

UNKNOWN = {"charon": "7b462d9", "aeneas": "ef8b0be", "lean": "unknown"}
RESOLVED = {"charon": "7b462d9", "aeneas": "ef8b0be",
            "lean": "Lean (version 4.35.0, x86_64-unknown-linux-gnu)"}
NEW_AENEAS = {"charon": "7b462d9", "aeneas": "aaaaaaa",
              "lean": "Lean (version 4.35.0, x86_64-unknown-linux-gnu)"}


class IdentityRules(unittest.TestCase):
    def test_a_field_that_became_resolvable_is_not_a_toolchain_change(self):
        self.assertTrue(inv.toolchain_only_unresolved(UNKNOWN, RESOLVED))
        self.assertTrue(inv.toolchain_only_unresolved(RESOLVED, UNKNOWN))

    def test_two_resolved_values_for_one_field_is_a_toolchain_change(self):
        self.assertFalse(inv.toolchain_only_unresolved(RESOLVED, NEW_AENEAS))

    def test_identical_identities_change_nothing(self):
        self.assertFalse(inv.toolchain_only_unresolved(RESOLVED, RESOLVED))

    def test_a_missing_field_is_not_excused(self):
        partial = {"charon": "7b462d9", "aeneas": "ef8b0be"}
        self.assertFalse(inv.toolchain_only_unresolved(partial, RESOLVED))


class AdoptRules(unittest.TestCase):
    """`should_adopt(prev, fingerprint_at_recorded_tc, fingerprint, recorded_tc, current_tc)`."""

    prev = {"fingerprint": "sources-hashed-with-unknown-lean", "measurement": {"stage": "aeneas"}}

    def adopt(self, prev=None, at_recorded=None, current="sources-hashed-with-known-lean",
              recorded=UNKNOWN, now=RESOLVED):
        return inv.should_adopt(prev or self.prev,
                                at_recorded or self.prev["fingerprint"], current, recorded, now)

    def test_a_row_survives_when_only_the_identity_became_resolvable(self):
        self.assertTrue(self.adopt())

    def test_a_row_without_a_measurement_is_never_adopted(self):
        self.assertFalse(self.adopt(prev={"fingerprint": self.prev["fingerprint"]}))

    def test_a_row_whose_sources_moved_is_re_measured(self):
        moved = "sources-hashed-after-an-edit"
        self.assertFalse(self.adopt(at_recorded=moved))

    def test_a_real_toolchain_bump_is_re_measured(self):
        self.assertFalse(self.adopt(now=NEW_AENEAS))

    def test_a_row_whose_fingerprint_still_matches_needs_no_adoption(self):
        self.assertFalse(self.adopt(current=self.prev["fingerprint"]))


class CoverageRules(unittest.TestCase):
    """`settles(row, fingerprint)` decides whether a crate still needs measuring.

    The sweep is a coverage claim, so the failure that matters is a row being skipped because
    *something* was measured -- a module sample, a timeout, a queued run -- rather than because the
    crate was read end to end.
    """

    def row(self, stage: str | None = "aeneas", scope="crate", fingerprint="fp"):
        return {"fingerprint": fingerprint, "measurement": {"stage": stage, "scope": scope}}

    def test_a_whole_crate_that_finished_settles(self):
        self.assertTrue(inv.settles(self.row(), "fp"))
        self.assertTrue(inv.settles(self.row(stage="compiled"), "fp"))
        self.assertTrue(inv.settles(self.row(stage="aeneas-crash"), "fp"),
                        "a translator crash is deterministic, not a missing measurement")

    def test_a_module_sample_never_settles_a_crate(self):
        # ferrosa-jsonb's `reader` sample elaborated cleanly while the whole crate crashes.
        self.assertFalse(inv.settles(self.row(scope="module reader"), "fp"))

    def test_an_absent_measurement_never_settles(self):
        for stage in ("timeout", "oom", "killed", "locked", "unknown", None):
            self.assertFalse(inv.settles(self.row(stage=stage), "fp"), stage)

    def test_moved_sources_never_settle(self):
        self.assertFalse(inv.settles(self.row(fingerprint="old"), "fp"))


class ModuleCoverageRules(unittest.TestCase):
    """A crate too large to extract whole is covered module by module, not by one sample."""

    def test_every_top_level_module_is_found_and_lib_is_not_one(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp) / "src"
            (src / "nested_ok").mkdir(parents=True)
            (src / "empty_dir").mkdir()
            (src / "lib.rs").write_text("")
            (src / "alpha.rs").write_text("")
            (src / "beta").mkdir()
            (src / "beta" / "mod.rs").write_text("")
            (src / "nested_ok" / "inner.rs").write_text("")
            self.assertEqual(inv.modules(Path(tmp)), ["alpha", "beta", "nested_ok"])

    def test_only_modules_the_lib_build_contains_are_measured(self):
        """A pattern charon cannot resolve is a permanent hole, not a measurement.

        `ferrosa-sstable`'s `src/` holds files the lib never declares (`pump_hooks`,
        `pump_metrics`) and a `bin/` directory of binary targets. Enumerating the directory
        instead of the crate root made five of its "modules" fail inside charon with no
        `.llbc`, so the crate could never finish and every sweep reported it as partial.
        """
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            src = root / "src"
            src.mkdir()
            (src / "lib.rs").write_text(
                "#[cfg(any(test, feature = \"test-support\"))]\n"
                "pub mod test_support_only;\n"
                "#[cfg(test)]\n"
                "mod legacy_tests;\n"
                "pub mod alpha;\n"
                "pub mod beta;\n")
            (root / "Cargo.toml").write_text("[package]\nname = \"demo\"\n\n[features]\n"
                                             "test-support = []\n")
            for name in ("test_support_only", "legacy_tests", "alpha"):
                (src / f"{name}.rs").write_text("")
            (src / "beta").mkdir()
            (src / "beta" / "mod.rs").write_text("")
            (src / "undeclared_leftover.rs").write_text("")
            (src / "bin").mkdir()
            (src / "bin" / "demo-tool.rs").write_text("")
            self.assertEqual(inv.modules(root), ["alpha", "beta"])

    def test_a_feature_gated_module_is_measured_when_that_feature_is_on_by_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            src = root / "src"
            src.mkdir()
            (src / "lib.rs").write_text("#[cfg(feature = \"extra\")]\npub mod extra_mod;\n")
            (root / "Cargo.toml").write_text("[features]\ndefault = [\"extra\"]\nextra = []\n")
            (src / "extra_mod.rs").write_text("")
            self.assertEqual(inv.modules(root), ["extra_mod"])

    def test_a_lib_that_declares_nothing_still_falls_back_to_the_directory(self):
        """Never narrow the module set to nothing: an unparsed root must not drop coverage."""
        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp) / "src"
            src.mkdir()
            (src / "lib.rs").write_text("include!(\"generated.rs\");\n")
            (src / "alpha.rs").write_text("")
            self.assertEqual(inv.modules(Path(tmp)), ["alpha"])

    def test_a_row_claims_no_crate_level_count(self):
        rows = {"alpha": {"stage": "aeneas", "lean_errors": 3, "seconds": 10},
                "beta": {"stage": "aeneas-crash", "lean_errors": None, "seconds": 20}}
        row = inv.coverage_row(rows, "oom")
        self.assertEqual(row["stage"], "modules")
        self.assertEqual(row["scope"], "modules (2/2)")
        self.assertIsNone(row["lean_errors"], "per-module numbers must not be summed into a crate")
        self.assertEqual(row["seconds"], 30)
        self.assertEqual(row["crate_level"], "oom")

    def test_one_unconcluded_module_makes_the_crate_partial(self):
        rows = {"alpha": {"stage": "aeneas"}, "beta": {"stage": "oom"}}
        self.assertEqual(inv.coverage_row(rows, "oom")["stage"], "modules-partial")

    def test_a_partial_row_names_the_module_it_stopped_at_and_why(self):
        """A crate that stopped must not read like a crate that was never measured.

        The CI failure printed `modules (1 measured)` -- a count with no cause, which sent the
        reader looking at the cap instead of at the module that had failed.
        """
        rows = {"alpha": {"stage": "aeneas"},
                "beta": {"stage": "timeout", "error": "module budget of 900s expired"}}
        row = inv.coverage_row(rows, "oom")
        self.assertEqual(row["stage"], "modules-partial")
        self.assertEqual(row["scope"], "modules (1/2); stopped at beta: timeout")
        self.assertEqual(row["stopped"]["module"], "beta")
        self.assertEqual(row["stopped"]["stage"], "timeout")
        self.assertIn("900s", row["stopped"]["error"])

    def test_a_complete_row_claims_no_stop(self):
        rows = {"alpha": {"stage": "aeneas"}, "beta": {"stage": "aeneas-crash"}}
        row = inv.coverage_row(rows, "oom")
        self.assertNotIn("stopped", row)

    def test_a_kill_at_the_host_limit_is_covered_module_by_module_not_concluded(self):
        """A kill is not an answer: the reds it would have reported are still unreported."""
        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp) / "src"
            src.mkdir()
            (src / "lib.rs").write_text("pub mod alpha;\n")
            (src / "alpha.rs").write_text("")
            crates = {"ferrosa-demo": {"dir": Path(tmp), "deps": []}}
            killed = {"stage": "killed", "error": "aeneas killed by signal 9: no cgroup ceiling "
                                                 "was in force"}
            measured = {"stage": "aeneas", "translation_errors": 0, "lean_errors": 3,
                        "sorry": 0, "seconds": 2}

            def fake_run_report(crate, prefix, budget, work_log, out=None):
                return dict(killed if prefix is None else measured)

            with mock.patch.object(inv, "run_report", fake_run_report):
                row = inv.measure("ferrosa-demo", crates)
            self.assertEqual(row["stage"], "modules", "the one module concluded")
            self.assertEqual(row["crate_level"], "killed",
                             "the reason the crate needed modules is kept")

    def test_a_scope_kill_still_falls_back_to_every_module(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp) / "src"
            src.mkdir()
            (src / "lib.rs").write_text("pub mod alpha;\n")
            (src / "alpha.rs").write_text("")
            crates = {"ferrosa-demo": {"dir": Path(tmp), "deps": []}}
            measured = {"stage": "aeneas", "translation_errors": 0, "lean_errors": 1,
                        "sorry": 0, "seconds": 2}

            def fake_run_report(crate, prefix, budget, work_log, out=None):
                return dict({"stage": "oom", "error": "charon killed"} if prefix is None
                            else measured)

            with mock.patch.object(inv, "run_report", fake_run_report):
                row = inv.measure("ferrosa-demo", crates)
            self.assertEqual(row["stage"], "modules")

    def test_only_a_fully_concluded_module_set_settles_the_crate(self):
        full = {"fingerprint": "fp",
                "measurement": {"stage": "modules", "crate_level": "oom",
                                "modules": {"alpha": {"stage": "aeneas"},
                                            "beta": {"stage": "aeneas-crash"}}}}
        self.assertTrue(inv.settles(full, "fp"))
        partial = {"fingerprint": "fp",
                   "measurement": {"stage": "modules-partial", "crate_level": "oom",
                                   "modules": {"alpha": {"stage": "aeneas"},
                                               "beta": {"stage": "oom"}}}}
        self.assertFalse(inv.settles(partial, "fp"))
        self.assertFalse(inv.settles({"fingerprint": "fp",
                                      "measurement": {"stage": "modules", "modules": {}}}, "fp"))


class ModuleSweepProgress(unittest.TestCase):
    """A module sweep keeps what it has finished, and does not redo it after an interruption."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        src = Path(self.tmp.name) / "src"
        src.mkdir(parents=True)
        (src / "alpha.rs").write_text("")
        (src / "beta.rs").write_text("")
        self.crates = {"ferrosa-demo": {"dir": Path(self.tmp.name), "deps": []}}
        self.calls = []

        def fake_run_report(crate, prefix, budget, work_log, out=None):
            self.calls.append(prefix)
            return {"stage": "aeneas", "translation_errors": 0, "lean_errors": 0, "sorry": 0,
                    "seconds": 1}

        self.patch = mock.patch.object(inv, "run_report", fake_run_report)
        self.patch.start()

    def tearDown(self):
        self.patch.stop()
        self.tmp.cleanup()

    def test_every_finished_module_is_reported_as_it_lands(self):
        seen = []
        row = inv.measure_modules("ferrosa-demo", self.crates, {"stage": "oom"},
                                  on_row=lambda mod, r: seen.append((mod, r["stage"])))
        self.assertEqual(seen, [("alpha", "aeneas"), ("beta", "aeneas")])
        self.assertEqual(row["stage"], "modules")
        self.assertEqual(sorted(row["modules"]), ["alpha", "beta"])

    def test_a_concluded_module_is_not_measured_again(self):
        prior = {"alpha": {"stage": "aeneas", "seconds": 9}}
        row = inv.measure_modules("ferrosa-demo", self.crates, {"stage": "oom"}, prior=prior)
        self.assertEqual(self.calls, ["ferrosa_demo::beta::*"],
                         "alpha already concluded, so only beta should be extracted")
        self.assertEqual(row["modules"]["alpha"]["seconds"], 9, "the earlier row is kept as-is")

    def test_an_inconclusive_module_is_retried(self):
        prior = {"alpha": {"stage": "oom"}}
        inv.measure_modules("ferrosa-demo", self.crates, {"stage": "oom"}, prior=prior)
        self.assertEqual(self.calls, ["ferrosa_demo::alpha::*", "ferrosa_demo::beta::*"])


class ModuleSweepRecording(unittest.TestCase):
    """What `main()` leaves in state.json after a module sweep.

    The sweep writes a row after every module so an interrupted run can resume. Those writes
    are this run's own, and the resume guard must not read them as a *competing* run: on
    2026-11-04 it did, so `ferrosa-sstable` kept its first module, discarded the other
    thirteen it had measured, and the ratchet saw a coverage regression.
    """

    CRATE = "ferrosa-demo"

    class Clock(datetime.datetime):
        """A clock that advances two seconds per reading.

        The CI failure was timing-dependent: `measured_at` has second resolution, so a sweep
        whose first progress write landed in the same second as its start escaped the false
        positive, and one that straddled the boundary did not. A clock that always straddles
        it makes the case deterministic instead of leaving it to luck.
        """

        _tick = 0

        @classmethod
        def now(cls, tz=None):
            cls._tick += 2
            naive = datetime.datetime(2026, 11, 4, 12, 0, cls._tick)
            return naive if tz is None else naive.replace(tzinfo=tz)

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.root = root
        self.gaps = root / "gaps"
        self.crate_dir = root / "crate"
        (self.crate_dir / "src").mkdir(parents=True)
        (self.crate_dir / "src" / "lib.rs").write_text("pub mod alpha;\npub mod beta;\n")
        (self.crate_dir / "src" / "alpha.rs").write_text("")
        (self.crate_dir / "src" / "beta.rs").write_text("")
        (self.crate_dir / "Cargo.toml").write_text("[package]\nname = \"ferrosa-demo\"\n")

        meta = {"packages": [{
            "name": self.CRATE,
            "manifest_path": str(self.crate_dir / "Cargo.toml"),
            "targets": [{"kind": ["lib"]}],
            "dependencies": [],
        }]}
        self.measure_calls = []
        self.after_module = None
        test = self

        def fake_measure(crate, crates, sample=False, on_row=None, prior=None,
                         on_crate_level=None):
            test.measure_calls.append(crate)
            if on_crate_level is not None:
                on_crate_level("killed")
            rows = {}
            for module in ("alpha", "beta"):
                row = {"stage": "aeneas", "translation_errors": 0, "lean_errors": 2,
                       "sorry": 0, "seconds": 1, "scope": f"module {module}"}
                rows[module] = row
                if on_row is not None:
                    on_row(module, row)
                if test.after_module is not None:
                    test.after_module(module)
            return inv.coverage_row(rows, "killed")

        patches = [
            mock.patch.object(inv, "REPO", root),
            mock.patch.object(inv, "GAPS", self.gaps),
            mock.patch.object(inv, "STATE", self.gaps / "state.json"),
            mock.patch.object(inv, "DAG_JSON", self.gaps / "dag.json"),
            mock.patch.object(inv, "DAG_MD", self.gaps / "dag.md"),
            mock.patch.object(inv, "metadata", lambda: meta),
            mock.patch.object(inv, "toolchain",
                              lambda: {"charon": "c0ffee", "aeneas": "aenea5",
                                       "lean": "Lean (version 4.35.0)"}),
            mock.patch.object(inv, "measure", fake_measure),
            mock.patch.object(inv, "datetime", self.Clock),
            mock.patch.object(sys, "argv", ["lean_gap_inventory.py", "--only", self.CRATE]),
        ]
        for patch in patches:
            patch.start()
            self.addCleanup(patch.stop)

    def state(self):
        return json.loads((self.gaps / "state.json").read_text())

    def test_a_module_sweep_records_what_it_measured_not_its_first_row(self):
        self.assertEqual(inv.main(), 0)
        measurement = self.state()["crates"][self.CRATE]["measurement"]
        self.assertEqual(measurement["stage"], "modules",
                         "every module concluded, so the crate concluded")
        self.assertEqual(sorted(measurement["modules"]), ["alpha", "beta"])
        self.assertEqual(measurement["scope"], "modules (2/2)")
        self.assertEqual(measurement["crate_level"], "killed",
                         "the crate-level reason is carried, not replaced by a constant")

    def test_a_competing_run_that_finished_first_still_wins(self):
        """The guard exists so a slow run cannot overwrite a newer measurement. Keep it."""
        foreign = {"fingerprint": "deadbeef", "measured_at": "2999-01-01T00:00:00+00:00",
                   "measurement": {"stage": "aeneas", "scope": "crate",
                                   "translation_errors": 0, "lean_errors": 1,
                                   "sorry": 0}}

        def write_foreign(module):
            if module != "alpha":
                return
            state = json.loads((self.gaps / "state.json").read_text())
            state["crates"][self.CRATE] = foreign
            (self.gaps / "state.json").write_text(json.dumps(state))

        self.after_module = write_foreign
        self.assertEqual(inv.main(), 0)
        self.assertEqual(self.state()["crates"][self.CRATE]["measurement"]["stage"], "aeneas",
                         "a measurement newer than this run's start must not be replaced")


class ProjectToolchainIdentity(unittest.TestCase):
    """Portable elan-like entrypoints select the project manifest, never a global default."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="lean-identity-")
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.repo = root / "repo"
        self.backend = root / "backend"
        tools = root / "bin"
        for directory in [self.repo, self.backend, tools]:
            directory.mkdir()
        self.calls = root / "calls.jsonl"
        (self.backend / "lean-toolchain").write_text("4.35.0\n")
        shim = "#!" + sys.executable + "\n" + r"""
import json
import os
from pathlib import Path
import sys

name = Path(sys.argv[0]).name
with Path(os.environ["IDENTITY_CALLS"]).open("a") as output:
    output.write(json.dumps({"tool": name, "cwd": str(Path.cwd())}) + "\n")
if name == "lean" and os.environ.get("IDENTITY_LEAN_FAIL") == "1":
    sys.stderr.write("Lean (version 4.35.0)\n")
    sys.exit(3)
if name == "lake" and os.environ.get("IDENTITY_LAKE_FAIL") == "1":
    sys.exit(3)
manifest = Path.cwd() / "lean-toolchain"
if not manifest.is_file():
    sys.stderr.write("no default toolchain configured\n")
    sys.exit(1)
print("Lean (version " + manifest.read_text().strip() + ")")
"""
        for name in ["lake", "lean"]:
            path = tools / name
            path.write_text(shim)
            path.chmod(0o755)
        self.inv = load_inventory()
        patches = [
            mock.patch.object(self.inv, "REPO", self.repo),
            mock.patch.object(self.inv, "LEAN_PKG", str(self.backend)),
            mock.patch.object(self.inv, "git_identity", side_effect=lambda _, what: what + "-source"),
            mock.patch.dict(os.environ, {
                "PATH": str(tools) + os.pathsep + os.environ.get("PATH", ""),
                "ELAN_HOME": str(root / "empty-elan-home"),
                "IDENTITY_CALLS": str(self.calls),
                "IDENTITY_LAKE_FAIL": "0",
                "IDENTITY_LEAN_FAIL": "0",
                "AENEAS_SRC": str(self.repo / "unbuilt-aeneas"),
            }),
        ]
        for patch in patches:
            patch.start()
            self.addCleanup(patch.stop)
        # Establish that the same fixture executable identifies the backend
        # before testing whether production selects its context correctly.
        control = subprocess.run([str(tools / "lake"), "env", "lean", "--version"],
                                 cwd=self.backend, capture_output=True, text=True, check=True)
        self.assertEqual(control.stdout.strip(), "Lean (version 4.35.0)")
        self.calls.unlink()

    def observed(self):
        return [json.loads(line) for line in self.calls.read_text().splitlines()]

    def ratchet_identity(self):
        # Load the real CLI module while allowing its sibling helper import.
        with mock.patch.object(sys, "path", [str(SCRIPT.parent), *sys.path]):
            spec = importlib.util.spec_from_file_location("ratchet_project_context", SCRIPT.with_name("lean_gap_ratchet.py"))
            ratchet = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(ratchet)
        with mock.patch.dict(os.environ, {"LEAN_PKG": str(self.backend)}):
            return ratchet.pin_lean()

    def test_ratchet_backend_resolves_without_a_global_default(self):
        self.assertFalse((self.repo / "lean-toolchain").exists())
        self.assertEqual(self.ratchet_identity()["version"], "4.35.0")
        self.assertEqual(self.observed(), [{"tool": "lean", "cwd": str(self.backend)}])

    def test_ratchet_repository_toolchain_does_not_override_backend(self):
        (self.repo / "lean-toolchain").write_text("4.36.0\n")
        self.assertEqual(self.ratchet_identity()["version"], "4.35.0")
        self.assertEqual(self.observed(), [{"tool": "lean", "cwd": str(self.backend)}])

    def test_ratchet_nonzero_exit_is_not_an_identity(self):
        os.environ["IDENTITY_LEAN_FAIL"] = "1"
        with self.assertRaises(SystemExit):
            self.ratchet_identity()
        self.assertEqual(self.observed(), [{"tool": "lean", "cwd": str(self.backend)}])

    def test_ratchet_missing_backend_identity_never_uses_repository(self):
        (self.backend / "lean-toolchain").unlink()
        (self.repo / "lean-toolchain").write_text("4.36.0\n")
        with self.assertRaises(SystemExit):
            self.ratchet_identity()
        self.assertEqual(self.observed(), [{"tool": "lean", "cwd": str(self.backend)}])

    def test_backend_project_resolves_without_a_global_default(self):
        self.assertFalse((self.repo / "lean-toolchain").exists())
        self.assertEqual(self.inv.toolchain()["lean"], "Lean (version 4.35.0)")
        self.assertEqual(self.observed(), [{"tool": "lake", "cwd": str(self.backend)}])

    def test_repository_toolchain_does_not_override_the_built_backend(self):
        (self.repo / "lean-toolchain").write_text("4.36.0\n")
        self.assertEqual(self.inv.toolchain()["lean"], "Lean (version 4.35.0)")
        self.assertEqual(self.observed(), [{"tool": "lake", "cwd": str(self.backend)}])

    def test_direct_lean_fallback_keeps_the_backend_context(self):
        os.environ["IDENTITY_LAKE_FAIL"] = "1"
        self.assertEqual(self.inv.toolchain()["lean"], "Lean (version 4.35.0)")
        self.assertEqual(self.observed(), [
            {"tool": "lake", "cwd": str(self.backend)},
            {"tool": "lean", "cwd": str(self.backend)},
        ])

    def test_unidentifiable_backend_fails_once_per_supported_entrypoint(self):
        (self.backend / "lean-toolchain").unlink()
        (self.repo / "lean-toolchain").write_text("4.36.0\n")
        with self.assertRaises(SystemExit):
            self.inv.toolchain()
        self.assertEqual(self.observed(), [
            {"tool": "lake", "cwd": str(self.backend)},
            {"tool": "lean", "cwd": str(self.backend)},
        ])


class FailLoudRules(unittest.TestCase):
    def test_an_unidentifiable_toolchain_raises_instead_of_being_called_unknown(self):
        with self.assertRaises(SystemExit):
            inv.tool_identity(["/nonexistent-toolchain-binary"], "charon")

    def test_a_binary_that_reports_no_identity_raises(self):
        with self.assertRaises(SystemExit):
            inv.tool_identity(["sh", "-c", "exit 3"], "charon")


if __name__ == "__main__":
    unittest.main()
