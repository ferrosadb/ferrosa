"""Tests for scripts/check-no-new-oom-waivers.py.

The rule it enforces (2026-10-03): a p0-oom-audit finding is fixed in code.
Adding an [[allow]] waiver, or extending one's expiry, needs Ben's explicit
approval, recorded as a `Waiver-Approved-By: Ben Kearns` trailer on the commit
that touches the waiver file. Every case runs against a throwaway git repo.
"""
import os
import pathlib
import subprocess
import sys
import tempfile
import unittest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "check-no-new-oom-waivers.py"
WAIVER = "specs/p0-oom-guard/oom-audit-allow.toml"
TRAILER = "Waiver-Approved-By: Ben Kearns"


def entry(path, rule="returns-vec-partition-or-row", expires="2026-12-31", reason="bounded"):
    return (
        "[[allow]]\n"
        f'path = "{path}"\n'
        f'rule = "{rule}"\n'
        f'reason = "{reason}"\n'
        'owner = "storage"\n'
        f'expires = "{expires}"\n\n'
    )


class WaiverCheck(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.repo = pathlib.Path(self._tmp.name)
        self.git("init", "-q", "-b", "main")
        self.git("config", "user.email", "test@example.invalid")
        self.git("config", "user.name", "Test")
        self.git("config", "commit.gpgsign", "false")
        # Hermetic: the developer's global core.hooksPath (commit hooks) must not
        # run inside these throwaway repos.
        hooks = self.repo / ".no-hooks"
        hooks.mkdir()
        self.git("config", "core.hooksPath", str(hooks))

    def tearDown(self):
        self._tmp.cleanup()

    def git(self, *args):
        return subprocess.run(
            ["git", *args], cwd=self.repo, check=True, capture_output=True, text=True
        ).stdout

    def write_waivers(self, text):
        path = self.repo / WAIVER
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)

    def commit(self, message, paths=(WAIVER,)):
        self.git("add", *paths)
        self.git("commit", "-q", "-m", message)

    def base(self, text):
        """Commit `text` as the waiver file on main, then branch off it."""
        self.write_waivers(text)
        self.commit("base")
        self.git("checkout", "-q", "-b", "feature")

    def run_check(self):
        result = subprocess.run(
            [sys.executable, str(SCRIPT), "main"],
            cwd=self.repo,
            capture_output=True,
            text=True,
            env={**os.environ, "GIT_CONFIG_NOSYSTEM": "1"},
        )
        return result.returncode, result.stdout + result.stderr

    # --- allowed changes -------------------------------------------------

    def test_an_unchanged_waiver_file_passes(self):
        self.base(entry("a.rs"))
        self.write_waivers(entry("a.rs"))
        code, out = self.run_check()
        self.assertEqual(code, 0, out)

    def test_removing_a_waiver_passes(self):
        self.base(entry("a.rs") + entry("b.rs"))
        self.write_waivers(entry("a.rs"))
        self.commit("drop b")
        code, out = self.run_check()
        self.assertEqual(code, 0, out)

    def test_shortening_an_expiry_passes(self):
        self.base(entry("a.rs", expires="2026-12-31"))
        self.write_waivers(entry("a.rs", expires="2026-11-01"))
        self.commit("shorten")
        code, out = self.run_check()
        self.assertEqual(code, 0, out)

    def test_editing_only_the_reason_passes(self):
        self.base(entry("a.rs", reason="old"))
        self.write_waivers(entry("a.rs", reason="clearer wording"))
        self.commit("reword")
        code, out = self.run_check()
        self.assertEqual(code, 0, out)

    # --- refused without approval -----------------------------------------

    def test_a_new_waiver_without_approval_fails_and_names_it(self):
        self.base(entry("a.rs"))
        self.write_waivers(entry("a.rs") + entry("new/stacked.rs", rule="clone-on-row-data"))
        self.commit("add a waiver")
        code, out = self.run_check()
        self.assertEqual(code, 1, out)
        self.assertIn("new/stacked.rs", out)
        self.assertIn("clone-on-row-data", out)

    def test_extending_an_expiry_without_approval_fails(self):
        self.base(entry("a.rs", expires="2026-10-02"))
        self.write_waivers(entry("a.rs", expires="2026-12-31"))
        self.commit("renew")
        code, out = self.run_check()
        self.assertEqual(code, 1, out)
        self.assertIn("2026-10-02 -> 2026-12-31", out)

    def test_moving_a_waiver_to_another_path_counts_as_new(self):
        self.base(entry("old/place.rs"))
        self.write_waivers(entry("new/place.rs"))
        self.commit("move")
        code, out = self.run_check()
        self.assertEqual(code, 1, out)

    def test_a_waiver_file_new_on_the_branch_fails(self):
        (self.repo / "README").write_text("x\n")
        self.commit("base without waivers", paths=("README",))
        self.git("checkout", "-q", "-b", "feature")
        self.write_waivers(entry("a.rs"))
        self.commit("introduce waivers")
        code, out = self.run_check()
        self.assertEqual(code, 1, out)

    def test_an_uncommitted_new_waiver_fails(self):
        # The working tree is what CI checks out; a waiver need not be
        # committed separately to be caught.
        self.base(entry("a.rs"))
        self.write_waivers(entry("a.rs") + entry("b.rs"))
        code, out = self.run_check()
        self.assertEqual(code, 1, out)

    # --- approval ------------------------------------------------------------

    def test_a_new_waiver_with_bens_trailer_on_its_commit_passes(self):
        self.base(entry("a.rs"))
        self.write_waivers(entry("a.rs") + entry("b.rs"))
        self.commit(f"add a waiver\n\n{TRAILER}")
        code, out = self.run_check()
        self.assertEqual(code, 0, out)

    def test_a_trailer_on_an_unrelated_commit_does_not_approve(self):
        self.base(entry("a.rs"))
        (self.repo / "other.txt").write_text("x\n")
        self.commit(f"unrelated\n\n{TRAILER}", paths=("other.txt",))
        self.write_waivers(entry("a.rs") + entry("b.rs"))
        self.commit("add a waiver")
        code, out = self.run_check()
        self.assertEqual(code, 1, out)

    def test_a_trailer_naming_someone_else_does_not_approve(self):
        self.base(entry("a.rs"))
        self.write_waivers(entry("a.rs") + entry("b.rs"))
        self.commit("add a waiver\n\nWaiver-Approved-By: An Agent")
        code, out = self.run_check()
        self.assertEqual(code, 1, out)


if __name__ == "__main__":
    unittest.main()
