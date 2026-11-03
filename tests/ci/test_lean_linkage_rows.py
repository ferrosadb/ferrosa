"""Tests for reading `Ferrosa/Broken.lean` rows by field name in the linkage report.

A Lean row is positional: `⟨a, b, c⟩` fills the structure's fields in declaration order. The report
used to unpack `row[:5]` as `area, what, site, ledger, item`, so adding a `plain` field between
`what` and `site` made it read the plain-language sentence as the code site. No sentence written for
a product reader names a file, so every row fell to `no-code-anchor` and the report said the corpus
had lost 17 of its 19 code anchors, with no error anywhere.
"""

import importlib.util
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "lean_linkage_report.py"


def load_report():
    spec = importlib.util.spec_from_file_location("lean_linkage_report", SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {SCRIPT} as a module")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


rep = load_report()

FIXTURE = """\
structure Item where
  /-- The area. -/
  area : String
  what : String
  /-- Words a product reader can follow. -/
  plain : String
  site : String
  ledger : String
  item : String
  deriving Repr

def broken : List Item := [
  ⟨"Recovery", "replay is idempotent",
   "Replaying the log can apply a write twice.",
   "commitlog/mutation.rs: a zero mutation_id is always replayed",
   "Ferrosa.Recovery", "replay is idempotent"⟩
]
"""


class BrokenRowsByName(unittest.TestCase):
    def test_a_field_added_before_site_does_not_move_the_site(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "Broken.lean"
            path.write_text(FIXTURE)
            rows = rep.broken_rows(path)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["site"],
                         "commitlog/mutation.rs: a zero mutation_id is always replayed")
        self.assertEqual(rows[0]["plain"], "Replaying the log can apply a write twice.")
        self.assertEqual(rows[0]["item"], "replay is idempotent")

    def test_a_row_with_the_wrong_number_of_fields_is_refused(self):
        short = FIXTURE.replace('"Ferrosa.Recovery", ', "")
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "Broken.lean"
            path.write_text(short)
            with self.assertRaises(ValueError):
                rep.broken_rows(path)

    def test_every_row_of_the_corpus_reads_its_site_from_the_site_field(self):
        rows = rep.broken_rows(REPO / "specs/lean4/Ferrosa/Broken.lean")
        self.assertGreater(len(rows), 0)
        anchored = [r for r in rows if ".rs" in r["site"]]
        self.assertGreater(len(anchored), 0,
                           "no row's site names a Rust file: the fields are being read by position")
        for r in rows:
            self.assertNotIn(".rs", r.get("plain", ""), r["what"])


if __name__ == "__main__":
    unittest.main()
