"""The nightly runs EVERY slow test and EVERY #[ignore]d test, with the
cluster the cluster tests need.

Two categories, two reasons, one workflow:

* SLOW tests live behind the `slow-tests` crate feature (never `#[ignore]` —
  ferrosa/CLAUDE.md forbids ignoring a test for being slow). PR CI's `test`
  job compiles them (`--all-features`) but skips running them
  (`--skip ::slow::`); nightly-slow-tests.yml's "Run the slow tests" step is
  the only place they run.
* CLUSTER-GATED tests stay `#[ignore]`d because they need the 3-node cluster,
  not merely because they are slow. ci.yml `integration` (PR CI) brings up
  the cluster and runs them; nightly-slow-tests.yml's
  "Run the ignored (cluster-gated) tests" step re-runs the same selection.

The nightly had no cluster, so the cluster-gated ferrosa-cql FTS test panicked
("start the 3-node test cluster first") every night, and because `cargo test`
stops at the first failing test binary without `--no-fail-fast`, nothing after
it was reported. It failed 12 nights in a row. The tempting fix, skipping the
cluster tests in the nightly, hides them instead: a nightly that quietly stops
running a test is worse than one that fails.

So this pins the opposite: the nightly skips NOTHING, runs the same package
selection as PR CI for each category, brings the cluster up before the tests
and tears it down after, reports every failure, and counts what it selected
with the same flags it runs with — for both categories independently, so
either one going quiet is caught on its own.
"""
from __future__ import annotations

import re
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
CI = REPO_ROOT / ".github" / "workflows" / "ci.yml"
NIGHTLY = REPO_ROOT / ".github" / "workflows" / "nightly-slow-tests.yml"


def step(workflow: str, name: str) -> str:
    """The text of the named step, up to the next step."""
    marker = f"- name: {name}"
    start = workflow.index(marker)
    rest = workflow[start + len(marker):]
    nxt = re.search(r"\n\s+- (?:name|uses):", rest)
    return marker + (rest if nxt is None else rest[: nxt.start()])


def cargo_commands(step_text: str) -> list[str]:
    """Every cargo test invocation in the step, line continuations joined.

    Matches both `cargo test ...` and `cargo nextest run ...`: the PR test job
    runs nextest for its cross-core scheduling, the nightly still runs libtest,
    and both are "the cargo test command" for the purposes of this contract.
    """
    lines = [l for l in step_text.splitlines() if not l.strip().startswith("#")]
    joined = re.sub(r"\\\n\s*", " ", "\n".join(lines))
    return [m.group(0) for m in re.finditer(r"cargo (?:nextest run|test)[^\n]*", joined)]


def cargo_command(step_text: str) -> str:
    """The (single) `cargo test ...` invocation in the step."""
    commands = cargo_commands(step_text)
    assert commands, f"no cargo test command in:\n{step_text}"
    assert len(commands) == 1, f"expected exactly one cargo test command in:\n{step_text}"
    return commands[0]


def cargo_command_containing(step_text: str, needle: str) -> str:
    """The `cargo test ...` invocation in the step that contains `needle`."""
    matches = [c for c in cargo_commands(step_text) if needle in c]
    assert matches, f"no cargo test command containing {needle!r} in:\n{step_text}"
    assert len(matches) == 1, f"expected exactly one match for {needle!r} in:\n{step_text}"
    return matches[0]


def excluded_packages(command: str) -> set[str]:
    return set(re.findall(r"--exclude\s+(\S+)", command))


class NightlyRunsEveryIgnoredTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ci = CI.read_text(encoding="utf-8")
        cls.nightly = NIGHTLY.read_text(encoding="utf-8")
        cls.pr_step = step(cls.ci, "Run ignored (cluster-gated) tests")
        cls.run_step = step(cls.nightly, "Run the ignored (cluster-gated) tests")
        cls.count_step = step(cls.nightly, "Fail if no slow or ignored tests were selected")
        cls.pr_cmd = cargo_command(cls.pr_step)
        cls.run_cmd = cargo_command(cls.run_step)
        cls.count_cmd = cargo_command_containing(cls.count_step, "--ignored")

    def test_the_nightly_skips_no_test(self):
        for name, text in (("run", self.run_cmd), ("count", self.count_cmd)):
            self.assertNotIn(
                "--skip", text,
                f"the nightly {name} command skips tests; skipping hides failures, "
                "give the test what it needs instead",
            )

    def test_the_nightly_covers_the_same_packages_as_pr_ci(self):
        self.assertTrue(excluded_packages(self.pr_cmd), "PR CI excludes nothing; the premise is gone")
        # Most thorough: the nightly may exclude nothing PR CI runs.
        self.assertLessEqual(excluded_packages(self.run_cmd), excluded_packages(self.pr_cmd))

    def test_the_nightly_brings_up_the_cluster_before_the_tests(self):
        self.assertIn("test-cluster-up-ci.sh", self.nightly)
        up = self.nightly.index("test-cluster-up-ci.sh")
        run = self.nightly.index("- name: Run the ignored (cluster-gated) tests")
        self.assertLess(up, run, "the cluster must be up before the tests run")
        self.assertIn('FERROSA_TEST_CONTAINERS: "1"', self.run_step)

    def test_the_cluster_is_torn_down_even_when_the_tests_fail(self):
        down = step(self.nightly, "Tear down cluster")
        self.assertIn("if: always()", down)
        self.assertIn("test-cluster-down.sh", down)

    def test_the_nightly_reports_every_failure_not_only_the_first(self):
        self.assertIn("--no-fail-fast", self.run_cmd)

    def test_the_selection_count_uses_the_same_selection_as_the_run(self):
        self.assertEqual(excluded_packages(self.count_cmd), excluded_packages(self.run_cmd))
        self.assertIn("--ignored", self.count_cmd)
        self.assertIn("--list", self.count_cmd)

    def test_the_cluster_runs_the_image_the_build_job_produced(self):
        # The release build lives in its own job (release-shaped, default
        # features) so the test job can stay --all-features throughout.
        self.assertIn("build-node-image:", self.nightly)
        self.assertIn("needs: build-node-image", self.nightly)
        self.assertIn("docker load", self.nightly)
        self.assertLess(self.nightly.index("docker load"), self.nightly.index("test-cluster-up-ci.sh"))

    def test_the_test_job_is_given_time_for_the_whole_suite(self):
        job = self.nightly[self.nightly.index("  slow-tests:"):]
        match = re.search(r"timeout-minutes:\s*(\d+)", job)
        self.assertIsNotNone(match)
        self.assertGreaterEqual(int(match.group(1)), 120)


class NightlyRunsEverySlowTest(unittest.TestCase):
    """The `slow-tests` feature half of the same bargain: PR CI compiles the
    tests but skips running them, and the nightly is the only place they run.
    """

    @classmethod
    def setUpClass(cls):
        cls.ci = CI.read_text(encoding="utf-8")
        cls.nightly = NIGHTLY.read_text(encoding="utf-8")
        cls.pr_step = step(cls.ci, "Run tests")
        cls.slow_step = step(cls.nightly, "Run the slow tests")
        cls.count_step = step(cls.nightly, "Fail if no slow or ignored tests were selected")
        cls.pr_cmd = cargo_command(cls.pr_step)
        cls.slow_cmd = cargo_command(cls.slow_step)
        cls.slow_count_cmd = cargo_command_containing(cls.count_step, "::slow::")

    def test_pr_ci_compiles_but_skips_the_slow_tests(self):
        self.assertIn("--all-features", self.pr_cmd, "PR CI must still compile the slow-tests feature")
        self.assertIn(
            "--skip ::slow::", self.pr_cmd,
            "PR CI must skip running slow tests; they run in nightly-slow-tests.yml",
        )

    def test_the_nightly_runs_the_slow_tests_under_all_features(self):
        self.assertIn("--all-features", self.slow_cmd)
        self.assertIn("::slow::", self.slow_cmd)
        self.assertNotIn("--skip", self.slow_cmd, "the nightly must not skip slow tests")

    def test_the_nightly_reports_every_slow_failure_not_only_the_first(self):
        self.assertIn("--no-fail-fast", self.slow_cmd)

    def test_the_nightly_slow_run_covers_the_same_packages_as_pr_ci(self):
        self.assertTrue(excluded_packages(self.pr_cmd), "PR CI excludes nothing; the premise is gone")
        self.assertLessEqual(excluded_packages(self.slow_cmd), excluded_packages(self.pr_cmd))

    def test_the_slow_selection_count_uses_the_same_selection_as_the_run(self):
        self.assertEqual(excluded_packages(self.slow_count_cmd), excluded_packages(self.slow_cmd))
        self.assertIn("::slow::", self.slow_count_cmd)
        self.assertIn("--list", self.slow_count_cmd)


if __name__ == "__main__":
    unittest.main()
