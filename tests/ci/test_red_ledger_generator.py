"""Tests for `scripts/red_ledger.py`, the join between the Lean red ledger and the Rust tree.

The first version emitted one empty `#[ignore]`d test per red claim, so the join shipped as 59
waivers and `scripts/check-no-new-waivers.py` failed on the branch that added it. An ignored test
that asserts nothing is a waiver whatever it is meant to document. The join is data -- the table of
reds -- plus tests that assert something about that data on every run.
"""

import importlib.util
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
        self.assertIn("assert_eq!(REDS.len(), 2,", tests)

    def test_no_red_becomes_a_test_function(self):
        tests = self.files["red-ledger/tests/reds.rs"]
        self.assertNotIn("fn testing_jepsen__", tests)
        self.assertNotIn("fn storage_recovery__", tests)


if __name__ == "__main__":
    unittest.main()
