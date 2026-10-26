"""Release versions come from main's Cargo version ("main leads").

The release bot bumps `[workspace.package] version` on main after every merge
(semver.yml, scripts/semver-bump --release), so the nightly and stable release
workflows read that version instead of computing their own from tags. These
tests pin the version script, both release workflows and the bot workflow.
Run: python3 -m unittest -q tests/ci/test_release_version_from_main.py
"""
import os
import subprocess
import tempfile
import tomllib
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / ".github" / "scripts" / "release-version-from-main.sh"
WORKFLOWS = ROOT / ".github" / "workflows"
NIGHTLY = WORKFLOWS / "nightly-release.yml"
PROMOTE = WORKFLOWS / "promote-release.yml"
CI = WORKFLOWS / "ci.yml"
SEMVER = WORKFLOWS / "semver.yml"


class ScriptTest(unittest.TestCase):
    """The script in a throwaway repository."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.git("init", "-q", "-b", "main")
        self.git("config", "user.email", "t@example.com")
        self.git("config", "user.name", "t")

    def tearDown(self):
        self.tmp.cleanup()

    def git(self, *args):
        return subprocess.run(
            ["git", *args], cwd=self.root, check=True, capture_output=True, text=True
        ).stdout

    def commit_version(self, version, msg="fix: x"):
        (self.root / "Cargo.toml").write_text(
            f'[workspace]\nmembers = []\n\n[workspace.package]\nversion = "{version}"\nedition = "2021"\n'
        )
        self.git("add", "-A")
        self.git("commit", "-q", "--allow-empty", "-m", msg)

    def run_script(self, *args):
        out_file = self.root / "out"
        out_file.write_text("")
        env = dict(os.environ, GITHUB_OUTPUT=str(out_file))
        proc = subprocess.run(
            ["bash", str(SCRIPT), *args], cwd=self.root, env=env, capture_output=True, text=True
        )
        outputs = dict(
            line.split("=", 1) for line in out_file.read_text().splitlines() if "=" in line
        )
        return proc, outputs

    def test_nightly_reports_mains_version(self):
        self.commit_version("0.22.0")
        self.git("tag", "v0.22.0")
        self.commit_version("0.22.1")
        proc, out = self.run_script("nightly")
        self.assertEqual(0, proc.returncode, proc.stderr)
        self.assertEqual("true", out["should_release"])
        self.assertEqual("0.22.1", out["next_version"])
        self.assertEqual("v0.22.1", out["next_tag"])
        self.assertEqual("v0.22.0", out["base_tag"])

    def test_pre_release_suffix_on_main_is_dropped(self):
        self.commit_version("0.24.0-nightly")
        proc, out = self.run_script("nightly")
        self.assertEqual(0, proc.returncode, proc.stderr)
        self.assertEqual("0.24.0", out["next_version"])

    def test_calver_and_semver_bump_tags_are_not_the_stable_base(self):
        self.commit_version("0.22.0")
        self.git("tag", "v0.22.0")
        self.commit_version("0.22.1")
        self.git("tag", "v2026.10.15.0002")
        self.git("tag", "semver-v0.99.0")
        proc, out = self.run_script("nightly")
        self.assertEqual(0, proc.returncode, proc.stderr)
        self.assertEqual("v0.22.0", out["base_tag"])

    def test_nothing_since_stable_skips_unless_forced(self):
        self.commit_version("0.22.0")
        self.git("tag", "v0.22.0")
        proc, out = self.run_script("nightly")
        self.assertEqual(0, proc.returncode, proc.stderr)
        self.assertEqual("false", out["should_release"])

    def test_main_below_latest_stable_refuses_loudly(self):
        self.commit_version("0.21.0")
        self.git("tag", "v0.22.0")
        self.commit_version("0.21.1")
        for mode in ("nightly", "promote"):
            with self.subTest(mode=mode):
                proc, out = self.run_script(mode)
                self.assertNotEqual(0, proc.returncode)
                self.assertIn("BELOW the latest stable tag v0.22.0", proc.stderr)
                self.assertNotIn("next_version", out)

    def test_change_without_a_bump_refuses_loudly(self):
        self.commit_version("0.22.0")
        self.git("tag", "v0.22.0")
        self.commit_version("0.22.0", "fix: landed without a bump")
        proc, out = self.run_script("nightly")
        self.assertNotEqual(0, proc.returncode)
        self.assertIn("not bumped", proc.stderr)

    def test_promote_refuses_an_existing_stable_tag(self):
        self.commit_version("0.22.0")
        self.git("tag", "v0.21.0")
        self.git("tag", "v0.22.0")  # already promoted at this version
        self.commit_version("0.22.0-nightly", "chore(nightly): v2026.10.15.0002")
        proc, out = self.run_script("promote")
        self.assertNotEqual(0, proc.returncode)
        self.assertIn("v0.22.0 already exists", proc.stderr)
        self.assertNotIn("next_version", out)

    def test_promote_returns_mains_version(self):
        self.commit_version("0.22.0")
        self.git("tag", "v0.22.0")
        self.commit_version("0.23.0-nightly")
        proc, out = self.run_script("promote")
        self.assertEqual(0, proc.returncode, proc.stderr)
        self.assertEqual("0.23.0", out["next_version"])
        self.assertEqual("v0.23.0", out["next_tag"])

    def test_unknown_mode_refuses(self):
        self.commit_version("0.22.0")
        proc, _ = self.run_script("sideways")
        self.assertEqual(2, proc.returncode)


class WorkflowContractTest(unittest.TestCase):
    def test_contract_runs_in_ci(self):
        self.assertIn(
            "python3 -m unittest -q tests/ci/test_release_version_from_main.py",
            CI.read_text(encoding="utf-8"),
        )

    def test_nightly_takes_its_version_from_main(self):
        wf = NIGHTLY.read_text(encoding="utf-8")
        self.assertIn("bash .github/scripts/release-version-from-main.sh nightly", wf)
        self.assertNotIn("next-release-version.sh", wf)
        self.assertNotIn("bump:", wf)  # no override input: main's version is the version
        # Artifact/tag naming is unchanged: CalVer nightly tags, built by release.yml.
        self.assertIn('NIGHTLY_TAG="v$(date -u +%Y.%m.%d.%H%M)"', wf)
        self.assertIn('NIGHTLY_VERSION="${VERSION}-nightly"', wf)
        self.assertIn("gh workflow run release.yml --ref", wf)

    def test_nightly_on_push_cuts_only_from_the_bot_release_commit(self):
        # The merge push still carries the previous version; the bot's
        # chore(release) push that follows it is the one to cut from.
        wf = NIGHTLY.read_text(encoding="utf-8")
        self.assertIn(
            "if: \"github.event_name != 'push' || "
            "startsWith(github.event.head_commit.message, 'chore(release): v')\"",
            wf,
        )

    def test_promotion_reads_main_and_never_opens_a_sync_pr(self):
        wf = PROMOTE.read_text(encoding="utf-8")
        self.assertIn("bash .github/scripts/release-version-from-main.sh promote", wf)
        self.assertNotIn("next-release-version.sh", wf)
        self.assertNotIn("gh pr create", wf)
        self.assertNotIn("sync-version", wf)
        self.assertNotIn("pull-requests: write", wf)
        self.assertIn("gh workflow run release.yml --ref", wf)

    def test_tag_computing_script_is_gone(self):
        self.assertFalse((ROOT / ".github" / "scripts" / "next-release-version.sh").exists())


class SemverWorkflowContractTest(unittest.TestCase):
    """The bump happens after merge, by the bot; a PR needs only a conventional title."""

    def setUp(self):
        self.wf = SEMVER.read_text(encoding="utf-8")

    def test_pr_checks_the_title_through_env_and_never_requires_a_bump(self):
        self.assertIn("types: [opened, edited, synchronize, reopened]", self.wf)
        self.assertIn("TITLE: ${{ github.event.pull_request.title }}", self.wf)
        self.assertIn('python3 scripts/semver-bump --check-title "$TITLE"', self.wf)
        self.assertNotIn("--check-title \"${{", self.wf)  # no interpolation into the script
        self.assertNotIn("semver-bump --check ", self.wf)

    def test_release_job_uses_the_app_and_fails_loud_without_it(self):
        self.assertIn("group: semver-release", self.wf)
        self.assertIn("vars.FERROSA_RELEASE_BOT_APP_ID", self.wf)
        self.assertIn("secrets.FERROSA_RELEASE_BOT_PRIVATE_KEY", self.wf)
        self.assertIn("::error::semver release cannot run", self.wf)
        self.assertIn("actions/create-github-app-token@bcd2ba49218906704ab6c1aa796996da409d3eb1", self.wf)
        self.assertIn("python3 scripts/semver-bump --release", self.wf)
        self.assertIn("git push --atomic origin", self.wf)
        self.assertNotIn("contents: write", self.wf)  # the push uses the App token

    def test_release_refuses_a_tag_prefix_that_would_trigger_release_yml(self):
        self.assertIn("v*) echo \"::error::tag_prefix", self.wf)

    def test_no_local_bump_hook(self):
        hooks = (ROOT / ".pre-commit-config.yaml").read_text(encoding="utf-8")
        self.assertNotIn("semver-bump --check --base", hooks)
        self.assertIn("semver-bump --check-message", hooks)

    def test_release_fetches_every_graph_before_offline_lock_refresh(self):
        cfg = tomllib.loads((ROOT / ".semver-bump.toml").read_text())
        for command in cfg["post_apply"]:
            if command[:2] != ["cargo", "update"]:
                continue
            fetch = "cargo fetch --locked"
            if "--manifest-path" in command:
                fetch += " --manifest-path " + command[command.index("--manifest-path") + 1]
            self.assertIn(fetch, self.wf)


class ReleaseLockfilesTest(unittest.TestCase):
    def test_real_release_refreshes_and_commits_every_dependent_lockfile(self):
        """Exercise the real release configuration with small, offline Cargo graphs."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            def run(*command):
                result = subprocess.run(command, cwd=root, capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                return result.stdout

            run("git", "init", "-q", "-b", "main")
            run("git", "config", "user.name", "test")
            run("git", "config", "user.email", "test@example.com")
            (root / ".semver-bump.toml").write_text((ROOT / ".semver-bump.toml").read_text())
            (root / "Cargo.toml").write_text(
                '[workspace]\nmembers = ["shared"]\nresolver = "2"\n'
                'exclude = ["ferrosa-jsonb-conformance", "ferrosa-sstable/fuzz"]\n'
                '[workspace.package]\nversion = "0.1.0"\n'
            )
            shared = root / "shared"
            shared.mkdir()
            (shared / "Cargo.toml").write_text(
                '[package]\nname = "release-shared"\nversion.workspace = true\n'
                'edition = "2021"\n[lib]\npath = "lib.rs"\n'
            )
            (shared / "lib.rs").write_text("")
            manifests = ["Cargo.toml"]
            for name, directory in (("conformance", "ferrosa-jsonb-conformance"),
                                    ("fuzz", "ferrosa-sstable/fuzz")):
                project = root / directory
                project.mkdir(parents=True)
                (project / "Cargo.toml").write_text(
                    f'[package]\nname = "release-{name}"\nversion = "0.0.0"\n'
                    'edition = "2021"\n[workspace]\n[lib]\npath = "lib.rs"\n'
                    f'[dependencies]\nrelease-shared = {{ path = "{os.path.relpath(shared, project)}" }}\n'
                )
                (project / "lib.rs").write_text("")
                manifests.append(f"{directory}/Cargo.toml")
            for manifest in manifests:
                run("cargo", "generate-lockfile", "--offline", "--manifest-path", manifest)
            run("git", "add", ".")
            run("git", "commit", "-qm", "chore: baseline")
            run("git", "tag", "semver-v0.1.0")
            run("git", "commit", "--allow-empty", "-qm", "fix: require a patch release")

            run("python3", str(ROOT / "scripts" / "semver-bump"), "--release")

            for manifest in manifests:
                # --locked is the same invariant enforced by the dependency gate.
                run("cargo", "metadata", "--offline", "--locked", "--format-version", "1",
                    "--manifest-path", manifest)
                lock = Path(manifest).with_name("Cargo.lock")
                packages = tomllib.loads(run("git", "show", f"HEAD:{lock}"))["package"]
                self.assertEqual(next(p["version"] for p in packages
                                      if p["name"] == "release-shared"), "0.1.1")
            self.assertEqual(run("git", "status", "--porcelain"), "")


if __name__ == "__main__":
    unittest.main()
