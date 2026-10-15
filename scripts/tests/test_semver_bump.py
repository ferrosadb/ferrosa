"""Tests for scripts/semver-bump (stdlib unittest). Run: python3 -m unittest discover -s scripts/tests"""
import importlib.machinery
import importlib.util
import os
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
TOOL = HERE.parent / "semver-bump"
loader = importlib.machinery.SourceFileLoader("semver_bump", str(TOOL))
spec = importlib.util.spec_from_loader("semver_bump", loader)
sb = importlib.util.module_from_spec(spec)
loader.exec_module(sb)

# Git hooks export GIT_DIR, GIT_INDEX_FILE and friends. Inherited, they point every git command
# in the throwaway repository below at the REAL repository: its config, its HEAD, its index.
# So the end-to-end tests run git with every GIT_* variable removed.
CLEAN_ENV = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}


class Pure(unittest.TestCase):
    def test_levels(self):
        self.assertEqual(sb.level_of("feat(x): a", ""), "minor")
        self.assertEqual(sb.level_of("fix: a", ""), "patch")
        self.assertEqual(sb.level_of("docs(readme): a", ""), "patch")
        self.assertEqual(sb.level_of("feat(x)!: a", ""), "major")
        self.assertEqual(sb.level_of("fix: a", "x\n\nBREAKING CHANGE: gone"), "major")
        self.assertIsNone(sb.level_of("Update stuff", ""))
        self.assertIsNone(sb.level_of("feat:missing space", ""))

    def test_bump_pre_and_post_1_0(self):
        self.assertEqual(sb.fmt(sb.bump((0, 23, 0, "-nightly"), "major")), "0.24.0-nightly")
        self.assertEqual(sb.fmt(sb.bump((0, 23, 4, ""), "minor")), "0.24.0")
        self.assertEqual(sb.fmt(sb.bump((0, 23, 4, ""), "patch")), "0.23.5")
        self.assertEqual(sb.fmt(sb.bump((1, 2, 3, ""), "major")), "2.0.0")

    def test_cargo_rewrite_tolerates_brackets_and_other_tables(self):
        text = textwrap.dedent('''\
            [workspace]
            members = ["a", "b"]
            [workspace.package]
            authors = ["x <x@y>"]
            version = "0.23.0-nightly"
            edition = "2021"
            [workspace.dependencies]
            serde = { version = "1.0" }
        ''')
        out = sb.write_version("cargo-workspace", text, "0.24.0-nightly", "Cargo.toml")
        self.assertIn('version = "0.24.0-nightly"', out)
        self.assertIn('serde = { version = "1.0" }', out)

    def test_json_rewrite(self):
        out = sb.write_version("json", '{\n  "name": "web",\n  "version": "1.2.3",\n  "x": {"version": "9"}\n}\n', "1.2.4", "package.json")
        self.assertIn('"version": "1.2.4"', out)
        self.assertIn('"x": {"version": "9"}', out)

    def test_message_hook(self):
        with tempfile.NamedTemporaryFile("w", delete=False) as f:
            f.write("fix(cql): close the session\n\nbody\n")
        self.assertEqual(sb.cmd_check_message(f.name), 0)
        Path(f.name).write_text("updated things\n")
        self.assertEqual(sb.cmd_check_message(f.name), 1)
        os.unlink(f.name)


class Repo(unittest.TestCase):
    """End to end in a throwaway git repository."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.git("init", "-q", "-b", "main")
        self.git("config", "user.email", "t@example.com")
        self.git("config", "user.name", "t")
        (self.root / "VERSION").write_text("0.3.1\n")
        (self.root / ".semver-bump.toml").write_text('[[file]]\npath = "VERSION"\nkind = "text"\n')
        self.git("add", "-A")
        self.git("commit", "-q", "-m", "chore: init")
        self.git("checkout", "-q", "-b", "work")

    def tearDown(self):
        self.tmp.cleanup()

    def git(self, *a):
        return subprocess.run(["git", *a], cwd=self.root, env=CLEAN_ENV, check=True, capture_output=True, text=True).stdout

    def commit(self, msg, name="f"):
        (self.root / name).write_text(msg)
        self.git("add", "-A")
        self.git("commit", "-q", "-m", msg)

    def run_tool(self, *a):
        return subprocess.run(["python3", str(TOOL), *a, "--base", "main"], cwd=self.root, env=CLEAN_ENV, capture_output=True, text=True)

    def test_unbumped_fails_then_apply_fixes(self):
        self.commit("feat(x): add y")
        self.assertEqual(self.run_tool("--check").returncode, 1)
        self.assertEqual(self.run_tool("--apply").returncode, 0)
        self.assertEqual((self.root / "VERSION").read_text().strip(), "0.4.0")
        self.assertIn("chore(release): v0.4.0", self.git("log", "-1", "--format=%s"))
        self.assertEqual(self.run_tool("--check").returncode, 0)

    def test_highest_level_wins_and_release_commits_are_ignored(self):
        self.commit("fix: a", "a")
        self.commit("docs: b", "b")
        self.assertEqual(self.run_tool("--apply").returncode, 0)
        self.assertEqual((self.root / "VERSION").read_text().strip(), "0.3.2")
        self.commit("feat!: c", "c")
        self.assertEqual(self.run_tool("--check").returncode, 1)
        self.assertEqual(self.run_tool("--apply").returncode, 0)
        self.assertEqual((self.root / "VERSION").read_text().strip(), "0.4.0")

    def test_non_conventional_commit_refuses(self):
        self.commit("did some stuff")
        r = self.run_tool("--check")
        self.assertEqual(r.returncode, 2)
        self.assertIn("not Conventional Commits", r.stderr)

    def test_hook_environment_cannot_reach_another_repository(self):
        # Run an end-to-end test the way a pre-push hook would: GIT_DIR names another repository.
        decoy = Path(self.tmp.name) / "decoy"
        subprocess.run(["git", "init", "-q", str(decoy)], env=CLEAN_ENV, check=True)
        env = dict(CLEAN_ENV, GIT_DIR=str(decoy / ".git"))
        r = subprocess.run(["python3", "-m", "unittest", "-q", "test_semver_bump.Repo.test_no_changes_passes"],
                           cwd=HERE, env=env, capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr)
        config = (decoy / ".git" / "config").read_text()
        self.assertNotIn("t@example.com", config)
        heads = subprocess.run(["git", "-C", str(decoy), "rev-parse", "--verify", "-q", "HEAD"],
                               env=CLEAN_ENV, capture_output=True, text=True)
        self.assertNotEqual(heads.returncode, 0, "the decoy repository gained a commit")

    def test_no_changes_passes(self):
        self.assertEqual(self.run_tool("--check").returncode, 0)


if __name__ == "__main__":
    unittest.main()
