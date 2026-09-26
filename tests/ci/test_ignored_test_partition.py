"""The nightly runs EVERY #[ignore]d test, with the cluster the cluster tests need.

Two workflows run `cargo test --workspace -- --ignored`:

* ci.yml `integration` (PR CI) brings up the 3-node cluster, then runs them.
* nightly-slow-tests.yml runs them nightly.

The nightly had no cluster, so the cluster-gated ferrosa-cql FTS test panicked
("start the 3-node test cluster first") every night, and because `cargo test`
stops at the first failing test binary without `--no-fail-fast`, nothing after
it was reported. It failed 12 nights in a row. The tempting fix, skipping the
cluster tests in the nightly, hides them instead: a nightly that quietly stops
running a test is worse than one that fails.

So this pins the opposite: the nightly skips NOTHING, runs the same package
selection as PR CI, brings the cluster up before the tests and tears it down
after, reports every failure, and counts what it selected with the same flags it
runs with.
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


def cargo_command(step_text: str) -> str:
    """The `cargo test ...` invocation with line continuations joined."""
    lines = [l for l in step_text.splitlines() if not l.strip().startswith("#")]
    joined = re.sub(r"\\\n\s*", " ", "\n".join(lines))
    match = re.search(r"cargo test[^\n]*", joined)
    assert match, f"no cargo test command in:\n{step_text}"
    return match.group(0)


def excluded_packages(command: str) -> set[str]:
    return set(re.findall(r"--exclude\s+(\S+)", command))


class NightlyRunsEveryIgnoredTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ci = CI.read_text(encoding="utf-8")
        cls.nightly = NIGHTLY.read_text(encoding="utf-8")
        cls.pr_step = step(cls.ci, "Run ignored (cluster-gated) tests")
        cls.run_step = step(cls.nightly, "Run the ignored (slow) tests")
        cls.count_step = step(cls.nightly, "Fail if no ignored tests were selected")
        cls.pr_cmd = cargo_command(cls.pr_step)
        cls.run_cmd = cargo_command(cls.run_step)
        cls.count_cmd = cargo_command(cls.count_step)

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
        run = self.nightly.index("- name: Run the ignored (slow) tests")
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

    def test_the_job_is_given_time_to_build_the_node_image(self):
        # test-cluster-up-ci.sh builds ferrosa-test-node itself when none is
        # preloaded: a full release build inside docker, then the tests.
        match = re.search(r"timeout-minutes:\s*(\d+)", self.nightly)
        self.assertIsNotNone(match)
        self.assertGreaterEqual(int(match.group(1)), 120)


if __name__ == "__main__":
    unittest.main()
