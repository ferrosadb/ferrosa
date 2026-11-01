"""Tests for `scripts/red_ledger.py`, the join between the Lean red ledger and the Rust tree.

The first version emitted one empty `#[ignore]`d test per red claim, so the join shipped as 59
waivers and `scripts/check-no-new-waivers.py` failed on the branch that added it. An ignored test
that asserts nothing is a waiver whatever it is meant to document. The join is data -- the table of
reds -- plus tests that assert something about that data on every run.
"""

import importlib.util
import shutil
import subprocess
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "red_ledger.py"


def load_generator():
    spec = importlib.util.spec_from_file_location("red_ledger", SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {SCRIPT} as a module")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


gen = load_generator()

SPECS = {
    "areas": [
        {"key": "Ferrosa.Testing.Jepsen", "items": [
            {"kind": "met", "claim": "an empty run establishes nothing"},
            {"kind": "failing", "claim": "the checker keeps read-only transactions",
             "why": "unverified from the code",
             "witness": "Jepsen.a_read_only_transaction_can_carry_the_only_cycle"},
        ]},
        {"key": "Ferrosa.Storage.Recovery", "items": [
            {"kind": "failing", "claim": "replay is idempotent",
             "why": "a zero mutation_id is always replayed",
             "witness": "Recovery.replay_twice_doubles"},
        ]},
    ]
}


class GeneratedJoin(unittest.TestCase):
    def setUp(self):
        self.files = gen.generate(SPECS)

    def test_no_generated_file_carries_a_waiver(self):
        for path, text in self.files.items():
            self.assertNotIn("#[ignore", text, path)
            self.assertNotIn("#[allow", text, path)
            self.assertNotIn("#![allow", text, path)

    def test_every_red_and_only_reds_are_in_the_table(self):
        data = self.files["red-ledger/src/reds.rs"]
        self.assertIn('"testing_jepsen__the_checker_keeps_read_only_transactions"', data)
        self.assertIn('"storage_recovery__replay_is_idempotent"', data)
        self.assertNotIn("an empty run establishes nothing", data)

    def test_the_count_is_pinned_to_the_number_of_reds(self):
        tests = self.files["red-ledger/tests/reds.rs"]
        self.assertIn("REDS.len(),\n        2,\n", tests)

    def test_no_red_becomes_a_test_function(self):
        tests = self.files["red-ledger/tests/reds.rs"]
        self.assertNotIn("fn testing_jepsen__", tests)
        self.assertNotIn("fn storage_recovery__", tests)

    def test_generated_files_are_already_rustfmt_clean(self):
        # CI runs both `cargo fmt --check` and `red_ledger.py --check`. If rustfmt would rewrite
        # the generated text, one of the two must fail: formatting breaks the staleness check, and
        # not formatting breaks the format check.
        rustfmt = shutil.which("rustfmt")
        if rustfmt is None:
            self.fail("rustfmt is not on PATH; install it with `rustup component add rustfmt`")
        # Compare the formatted text rather than trusting `--check`'s exit code: on stdin it prints
        # the diff and still exits 0.
        for path, text in self.files.items():
            out = subprocess.run([rustfmt, "--edition", "2021", "--emit", "stdout"], input=text,
                                 capture_output=True, text=True)
            self.assertEqual(out.returncode, 0, f"rustfmt failed on {path}: {out.stderr}")
            self.assertEqual(out.stdout, text, f"{path} is not rustfmt-clean")


if __name__ == "__main__":
    unittest.main()
