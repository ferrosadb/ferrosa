"""Contract for the CI build cache and the feature set CI compiles.

GitHub scopes an Actions cache to the ref that saved it. A cache saved by a pull
request, a merge-queue ref or a tag can be restored only by that same ref, never
by main, yet all of them count against the repository's 10 GB cap. When every
run saved, those caches evicted main's, and every job fell back to a cold build.
So a rust-cache step restores everywhere and saves only from main, on a hosted
runner.

sccache's GitHub backend wrote to the same cache store, per run and per ref, on
top of rust-cache. It is gone; rust-cache is the one layer.

CI test jobs compile with --all-features so they all build one dependency set.
Jobs that build a shipped binary or image keep their curated feature set: a
release artifact must not carry test-only or live-infra features.
"""

import re
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = REPO_ROOT / ".github" / "workflows"

RUST_CACHE = "uses: Swatinem/rust-cache@6323deb102c322ba6fcbdcafc7e3dddab59af2b6 # v2.9.2"
MAIN_ONLY = "github.ref == 'refs/heads/main'"
HOSTED_ONLY = "runner.environment == 'github-hosted'"
# Under pull_request_target, github.ref is the BASE branch, so MAIN_ONLY alone
# would let PR code save into main's cache scope.
NOT_PR_TARGET = "github.event_name != 'pull_request_target'"

# Workflows whose only job is to cut or publish a release. Their builds keep the
# curated `ferrosa/full` feature set and are out of scope for --all-features.
RELEASE_WORKFLOWS = {
    "release.yml",
    "nightly-release.yml",
    "promote-release.yml",
    "actions-pin-guard.yml",
}

# Jobs in CI workflows that build a binary or image standing in for a shipped
# one. --all-features would put test-only / live-infra features into it.
RELEASE_SHAPED_JOBS = {
    ("ci.yml", "musl"): "static release binary, reused by the examples job",
    ("ci.yml", "build-node-image"): "release image pushed to GHCR for paired repos",
    ("install-smoke.yml", "build"): "stages the real release tarball layout",
    ("nightly-fuzz.yml", "smoke-test"): "musl fallback for the nightly .deb image",
}

# Jobs whose cargo build runs inside a script rather than a `run:` line, and the
# rust-cache `workspaces:` that points at what the script actually compiles.
SCRIPT_COMPILES = {
    ("install-smoke.yml", "memory-smoke"): "workspaces: ferrosa-memory -> target",
}

CARGO_COMPILE = re.compile(
    r"\bcargo (?:clippy|test|check|doc|build|run|llvm-cov|nextest)\b[^\n|;&]*"
)

# ci.yml `test`, ci.yml `integration` and nightly-fuzz `fuzz` compile the same
# workspace, features, profile and debug level, so they can share one cache.
SHARED_TEST_KEY = "workspace-test-all-features-debug1"
SHARED_TEST_JOBS = {
    ("ci.yml", "test"),
    ("ci.yml", "integration"),
    ("nightly-fuzz.yml", "fuzz"),
}


def workflow_files():
    files = sorted(WORKFLOWS.glob("*.yml"))
    assert files, f"no workflows found under {WORKFLOWS}"
    return files


def jobs(text):
    body = text.split("\njobs:\n", 1)[1]
    parts = re.split(r"^  ([A-Za-z0-9_-]+):\n", body, flags=re.M)
    return dict(zip(parts[1::2], parts[2::2]))


def steps(job):
    return re.split(r"^      - ", job, flags=re.M)[1:]


def job_env(job):
    head = re.split(r"^    steps:\n", job, maxsplit=1, flags=re.M)[0]
    env = head.split("\n    env:\n", 1)
    if len(env) == 1:
        return {}
    pairs = re.findall(r"^      ([A-Z0-9_]+): (.+)$", env[1], flags=re.M)
    return dict(pairs)


def code_lines(text):
    return [line for line in text.split("\n") if not line.lstrip().startswith("#")]


def run_scripts(job):
    """The shell text of every `run:` in a job, comments dropped."""
    scripts = []
    for step in steps(job):
        lines = step.split("\n")
        for i, line in enumerate(lines):
            m = re.match(r"^(?: {8})?run: ?(.*)$", line)
            if not m:
                continue
            if m.group(1) not in ("|", "|-", ">"):
                scripts.append(m.group(1))
                continue
            block = []
            for body in lines[i + 1 :]:
                if body.strip() and not body.startswith(" " * 10):
                    break
                block.append(body)
            scripts.append("\n".join(code_lines("\n".join(block))))
    return scripts


def cargo_commands(job):
    commands = []
    for script in run_scripts(job):
        joined = re.sub(r"\\\n\s*", " ", script)
        commands.extend(m.group(0) for m in CARGO_COMPILE.finditer(joined))
    return commands


def rust_cache_steps(job):
    return [step for step in steps(job) if "Swatinem/rust-cache@" in step]


def save_if(step):
    m = re.search(r"^\s+save-if: (.+)$", step, flags=re.M)
    return m.group(1) if m else ""


def shared_key(step):
    m = re.search(r"^\s+shared-key: (.+)$", step, flags=re.M)
    return m.group(1).strip() if m else None


class CiCacheWorkflowTests(unittest.TestCase):
    def test_every_rust_cache_step_is_pinned_and_saves_only_from_main(self):
        seen = 0
        for path in workflow_files():
            text = path.read_text(encoding="utf-8")
            pr_target = "pull_request_target" in text.split("\njobs:\n", 1)[0]
            for name, job in jobs(text).items():
                for step in rust_cache_steps(job):
                    seen += 1
                    where = f"{path.name}:{name}"
                    self.assertTrue(
                        step.startswith(RUST_CACHE), f"{where} must pin {RUST_CACHE}"
                    )
                    condition = save_if(step)
                    self.assertIn(MAIN_ONLY, condition, f"{where} saves from any ref")
                    self.assertIn(HOSTED_ONLY, condition, f"{where} saves from self-hosted")
                    if pr_target:
                        self.assertIn(NOT_PR_TARGET, condition, f"{where} saves PR code to main")
        self.assertGreater(seen, 10, "expected rust-cache in most compiling jobs")

    def test_no_workflow_uses_sccache(self):
        for path in workflow_files():
            for line in code_lines(path.read_text(encoding="utf-8")):
                self.assertNotRegex(
                    line,
                    r"(?i)sccache|RUSTC_WRAPPER",
                    f"{path.name}: sccache's GHA backend is removed; rust-cache is the one layer",
                )

    def test_rust_cache_only_in_jobs_that_compile(self):
        for path in workflow_files():
            for name, job in jobs(path.read_text(encoding="utf-8")).items():
                script_workspace = SCRIPT_COMPILES.get((path.name, name))
                if script_workspace:
                    for step in rust_cache_steps(job):
                        self.assertIn(script_workspace, step, f"{path.name}:{name}")
                elif rust_cache_steps(job):
                    self.assertTrue(
                        cargo_commands(job),
                        f"{path.name}:{name} restores and saves a Rust cache but never runs cargo",
                    )

    def test_ci_compiles_with_all_features_except_release_shaped_builds(self):
        checked = 0
        for path in workflow_files():
            if path.name in RELEASE_WORKFLOWS:
                continue
            for name, job in jobs(path.read_text(encoding="utf-8")).items():
                if (path.name, name) in RELEASE_SHAPED_JOBS:
                    continue
                for command in cargo_commands(job):
                    checked += 1
                    self.assertIn(
                        "--all-features",
                        command,
                        f"{path.name}:{name} compiles a different feature set: {command}",
                    )
        self.assertGreater(checked, 10, "expected to find the CI cargo commands")

    def test_release_shaped_builds_keep_their_curated_features(self):
        for (workflow, name), why in RELEASE_SHAPED_JOBS.items():
            job = jobs((WORKFLOWS / workflow).read_text(encoding="utf-8"))[name]
            for command in cargo_commands(job):
                self.assertNotIn("--all-features", command, f"{workflow}:{name} ({why})")

    def test_only_jobs_with_one_compile_share_the_test_cache(self):
        for path in workflow_files():
            for name, job in jobs(path.read_text(encoding="utf-8")).items():
                for step in rust_cache_steps(job):
                    key = shared_key(step)
                    if (path.name, name) in SHARED_TEST_JOBS:
                        self.assertEqual(key, SHARED_TEST_KEY, f"{path.name}:{name}")
                        # Job-level, so rust-cache hashes it into the key and a
                        # job at another debug level can never restore this cache.
                        self.assertEqual(
                            job_env(job).get("CARGO_PROFILE_DEV_DEBUG"), '"1"', f"{path.name}:{name}"
                        )
                    else:
                        self.assertNotEqual(key, SHARED_TEST_KEY, f"{path.name}:{name}")

    def test_report_time_bootstrap_never_reaches_rustc(self):
        # anyhow's build script emits rerun-if-env-changed=RUSTC_BOOTSTRAP, so
        # setting it for the compile rebuilds most of the graph and makes the
        # fuzz job's artifacts differ from the shared test cache. libtest reads
        # it at run time, so it goes to the test binaries through the runner.
        job = jobs((WORKFLOWS / "nightly-fuzz.yml").read_text(encoding="utf-8"))["fuzz"]
        self.assertIsNone(
            re.search(r"(?m)^\s+RUSTC_BOOTSTRAP:", job),
            "RUSTC_BOOTSTRAP is exported to the compile in nightly-fuzz `fuzz`",
        )
        self.assertIn(
            'CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER: "env RUSTC_BOOTSTRAP=1"', job
        )


if __name__ == "__main__":
    unittest.main()
