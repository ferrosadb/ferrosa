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
        for stage in ("timeout", "oom", "locked", "unknown", None):
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
