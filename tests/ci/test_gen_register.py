"""Tests for the theorem register's classification, `specs/lean4/site/gen_register.py`.

The register calls a theorem red when it proves a property the product needs does not hold yet. Four
theorems were marked red only because their docstrings opened with "Red:", and none of them is that:
each is a counterexample to a design the code does not use (an unguarded pair promotion, a
conflated-ballot acceptor) or a history the checker must reject, and each has a Rust test pinning the
code's side. A counterexample is its own kind, so the register stops reporting it as a defect.
"""

import importlib.util
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "specs" / "lean4" / "site" / "gen_register.py"


def load_generator():
    spec = importlib.util.spec_from_file_location("gen_register", SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {SCRIPT} as a module")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


gen = load_generator()


def theorem(short, doc):
    return {"k": "decl", "kind": "theorem", "full": f"Ferrosa.M.{short}", "short": short, "doc": doc}


PART = {"numeral": "II", "title": "Consistency", "intro": "Intro.", "modules": ["Ferrosa.M"]}
MODULES = {"Ferrosa.M": {"events": [
    theorem("guarded", "**No split brain.** At most one writer."),
    theorem("unguarded", "**Counterexample: without the guard, one promotion splits the brain.**"),
    theorem("missing", "**Red: the probe is not constant.**"),
    theorem("cited", "A theorem a failing ledger row cites."),
]}}


class CounterexamplesAreNotReds(unittest.TestCase):
    def setUp(self):
        _, self.text, self.total, self.reds = gen.render_part(PART, MODULES, {"M.cited"})

    def test_a_counterexample_is_labelled_as_one(self):
        line = next(l for l in self.text.splitlines() if "`unguarded`" in l)
        self.assertIn("**Counterexample.**", line)
        self.assertNotIn("**Red.**", line)

    def test_a_counterexample_is_not_counted_red(self):
        self.assertEqual(self.total, 4)
        self.assertEqual(self.reds, 2, "only the Red docstring and the ledger witness are red")

    def test_reds_are_still_red(self):
        for short in ("missing", "cited"):
            line = next(l for l in self.text.splitlines() if f"`{short}`" in l)
            self.assertIn("**Red.**", line)


if __name__ == "__main__":
    unittest.main()
