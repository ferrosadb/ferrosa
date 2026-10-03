"""Tests for scripts/check-no-new-waivers.py.

The rule (Ben, 2026-10-03): no waivers. A waiver is either an `#[ignore]` on a
test, or a date-based exemption: `expires = "YYYY-MM-DD"`, "until YYYY-MM-DD",
and the like. Adding one, anywhere in the repo, fails CI unless a commit in the
range carries the trailer `Waiver-Approved-By: Ben Kearns`. Removing one is
always fine. Every case runs against a throwaway git repo.
"""
import datetime
import os
import pathlib
import subprocess
import sys
import tempfile
import unittest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "check-no-new-waivers.py"
TRAILER = "Waiver-Approved-By: Ben Kearns"
ALLOW = "specs/p0-oom-guard/oom-audit-allow.toml"
# Waiver dates must stay in the future however long this test lives.
FUTURE = (datetime.date.today() + datetime.timedelta(days=90)).isoformat()
PAST = "2020-01-01"


class WaiverCheck(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.repo = pathlib.Path(self._tmp.name)
        self.git("init", "-q", "-b", "main")
        self.git("config", "user.email", "test@example.invalid")
        self.git("config", "user.name", "Test")
        self.git("config", "commit.gpgsign", "false")
        hooks = self.repo / ".no-hooks"  # hermetic: no developer commit hooks
        hooks.mkdir()
        self.git("config", "core.hooksPath", str(hooks))
        self.write("src/lib.rs", "#[test]\nfn a() {}\n")
        self.write(ALLOW, f'[[allow]]\npath = "a.rs"\nrule = "r"\nexpires = "{PAST}"\n')
        self.commit("base")
        self.git("checkout", "-q", "-b", "feature")

    def tearDown(self):
        self._tmp.cleanup()

    def git(self, *args):
        return subprocess.run(
            ["git", *args], cwd=self.repo, check=True, capture_output=True, text=True
        ).stdout

    def write(self, rel, text):
        path = self.repo / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)

    def commit(self, message):
        self.git("add", "-A")
        self.git("commit", "-q", "--allow-empty", "-m", message)

    def run_check(self):
        result = subprocess.run(
            [sys.executable, str(SCRIPT), "main"],
            cwd=self.repo,
            capture_output=True,
            text=True,
            env={**os.environ, "GIT_CONFIG_NOSYSTEM": "1"},
        )
        return result.returncode, result.stdout + result.stderr

    # --- allowed --------------------------------------------------------------

    def test_ordinary_changes_pass(self):
        self.write("src/lib.rs", "#[test]\nfn a() {}\n#[test]\nfn b() {}\n")
        self.commit("add a test")
        code, out = self.run_check()
        self.assertEqual(code, 0, out)

    def test_removing_a_waiver_passes(self):
        self.write(ALLOW, "")
        self.commit("drop the waiver")
        code, out = self.run_check()
        self.assertEqual(code, 0, out)

    def test_a_past_until_date_in_prose_passes(self):
        # History ("these were asserts until 2020-01-01") is not an exemption:
        # a waiver's date is in the future.
        self.write("src/lib.rs", "/// These were assert!s until 2020-01-01, when they became errors.\n"
                                 "#[test]\nfn a() {}\n")
        self.commit("doc")
        code, out = self.run_check()
        self.assertEqual(code, 0, out)

    def test_a_date_in_ordinary_prose_passes(self):
        self.write("NOTES.md", "Released on 2026-10-03.\n")
        self.commit("notes")
        code, out = self.run_check()
        self.assertEqual(code, 0, out)

    # --- refused without approval ---------------------------------------------

    def test_adding_ignore_to_a_test_fails(self):
        self.write("src/lib.rs", "#[test]\n#[ignore]\nfn a() {}\n")
        self.commit("skip a")
        code, out = self.run_check()
        self.assertEqual(code, 1, out)
        self.assertIn("src/lib.rs", out)
        self.assertIn("#[ignore]", out)

    def test_ignore_with_a_reason_fails(self):
        self.write("src/lib.rs", '#[test]\n#[ignore = "slow"]\nfn a() {}\n')
        self.commit("skip a")
        code, out = self.run_check()
        self.assertEqual(code, 1, out)

    def test_a_new_dated_allowlist_entry_fails(self):
        self.write(ALLOW, f'[[allow]]\npath = "a.rs"\nrule = "r"\nexpires = "{PAST}"\n'
                          f'[[allow]]\npath = "b.rs"\nrule = "r"\nexpires = "{FUTURE}"\n')
        self.commit("add waiver")
        code, out = self.run_check()
        self.assertEqual(code, 1, out)
        self.assertIn(FUTURE, out)

    def test_extending_an_expiry_fails(self):
        self.write(ALLOW, f'[[allow]]\npath = "a.rs"\nrule = "r"\nexpires = "{FUTURE}"\n')
        self.commit("renew")
        code, out = self.run_check()
        self.assertEqual(code, 1, out)

    def test_an_until_date_exemption_in_code_fails(self):
        self.write("src/lib.rs", f"// allowed until {FUTURE}: tracked elsewhere\n#[test]\nfn a() {{}}\n")
        self.commit("temporary exemption")
        code, out = self.run_check()
        self.assertEqual(code, 1, out)

    def test_an_uncommitted_waiver_fails(self):
        self.write("src/lib.rs", "#[test]\n#[ignore]\nfn a() {}\n")
        code, out = self.run_check()
        self.assertEqual(code, 1, out)

    # --- approval --------------------------------------------------------------

    def test_bens_trailer_approves(self):
        self.write("src/lib.rs", "#[test]\n#[ignore]\nfn a() {}\n")
        self.commit(f"skip a\n\n{TRAILER}")
        code, out = self.run_check()
        self.assertEqual(code, 0, out)

    def test_someone_elses_trailer_does_not_approve(self):
        self.write("src/lib.rs", "#[test]\n#[ignore]\nfn a() {}\n")
        self.commit("skip a\n\nWaiver-Approved-By: An Agent")
        code, out = self.run_check()
        self.assertEqual(code, 1, out)


if __name__ == "__main__":
    unittest.main()
