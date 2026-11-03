"""Tests for excluding generated code from a Lean gap extraction.

aeneas on `ferrosa_net.llbc` emitted 13,295 "Internal error: please file an issue" records in five
minutes -- 11,147 of them from `envelope_capnp`, 31k lines of capnproto bindings that a build script
generates into OUT_DIR and the crate `include!`s. They are schema output, not product code, and
translating them is what pushes the run past 22 GiB. charon can leave them out (`--exclude`), so the
harness needs a place to say which generated items a crate carries.

The file is per repo, not per run: the list is a fact about the crate's build, and a flag on one
invocation would be forgotten by the next sweep.
"""

import importlib.util
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "lean_gap_report.py"


def load_report():
    spec = importlib.util.spec_from_file_location("lean_gap_report", SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {SCRIPT} as a module")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


rep = load_report()

LINES = """\
# generated protocol bindings: the whole crate cannot be translated with them in
ferrosa-net ferrosa_net::protocol::envelope_capnp
ferrosa-net ferrosa_net::protocol::envelope_capnp::_
ferrosa-sstable ferrosa_sstable::writer::generated
"""


class ExclusionParsing(unittest.TestCase):
    def parse(self, text, crate):
        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as fh:
            fh.write(text)
            path = Path(fh.name)
        try:
            return rep.exclusions(crate, path)
        finally:
            path.unlink()

    def test_a_crate_gets_only_its_own_patterns(self):
        self.assertEqual(self.parse(LINES, "ferrosa-net"),
                         ["ferrosa_net::protocol::envelope_capnp",
                          "ferrosa_net::protocol::envelope_capnp::_"])
        self.assertEqual(self.parse(LINES, "ferrosa-sstable"),
                         ["ferrosa_sstable::writer::generated"])

    def test_a_crate_with_no_patterns_gets_nothing(self):
        self.assertEqual(self.parse(LINES, "ferrosa-cdc"), [])

    def test_a_missing_file_is_not_an_error(self):
        missing = Path(tempfile.mkdtemp()) / "absent.txt"
        self.assertEqual(rep.exclusions("ferrosa-net", missing), [])

    def test_comments_and_blank_lines_are_ignored(self):
        text = "\n# a comment\n\nferrosa-net a::b\n   \n"
        self.assertEqual(self.parse(text, "ferrosa-net"), ["a::b"])

    def test_a_line_without_a_pattern_fails_loudly(self):
        # Fail loud: a typo'd exclusion silently translates the generated code again, which is the
        # 22 GiB run this file exists to prevent.
        with self.assertRaises(SystemExit):
            self.parse("ferrosa-net\n", "ferrosa-net")


class CharonCommand(unittest.TestCase):
    def test_the_command_carries_one_exclude_flag_per_pattern(self):
        cmd = rep.charon_cmd("ferrosa-net", "ferrosa_net::*", ["aa::bb", "cc::dd::_"])
        self.assertIn("--exclude", cmd)
        self.assertEqual([cmd[i + 1] for i, a in enumerate(cmd) if a == "--exclude"],
                         ["aa::bb", "cc::dd::_"])
        self.assertEqual(cmd.count("--exclude"), 2)
        # ordering that matters: the prefix still lands before the `--` that separates cargo args
        self.assertLess(cmd.index("--start-from"), cmd.index("--exclude"))
        self.assertLess(cmd.index("--exclude"), cmd.index("--"))

    def test_no_patterns_means_no_exclude_flag(self):
        cmd = rep.charon_cmd("ferrosa-cdc", "ferrosa_cdc::*", [])
        self.assertNotIn("--exclude", cmd)
        self.assertEqual(cmd[-2:], ["--", "--lib"])


if __name__ == "__main__":
    unittest.main()
