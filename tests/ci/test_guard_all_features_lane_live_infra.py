"""Tests for scripts/ci/guard-all-features-lane-live-infra.py.

Each case builds a throwaway repository whose lane files are real text, so the
guard's parser is exercised against the same shapes it meets in this tree
(YAML block scalars with `\\` continuations, a shell `SKIPS=( ... )` array).
The last case runs it on this repository.
"""

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
GUARD = REPO / "scripts" / "ci" / "guard-all-features-lane-live-infra.py"

GATED_TARGET = '''#![cfg(feature = "live-infra-tests")]

#[test]
fn needs_a_cluster() {
    panic!("live-infra-tests is enabled but FERROSA_TEST_CLUSTER_NODES is not set");
}
'''

HELPER_GATED = '''#![cfg(feature = "live-infra-tests")]

#[cfg(feature = "live-infra-tests")]
fn helper_that_is_not_a_test() -> Option<String> {
    std::env::var("FERROSA_TEST_CLUSTER_NODES").ok()
}

#[test]
fn needs_containers() {
    panic!("live-infra-tests is enabled but FERROSA_TEST_CONTAINERS is not set");
}
'''

PLAIN_TARGET = '''#[test]
fn an_ordinary_unit_test() {
    assert_eq!(2 + 2, 4);
}
'''

CI_YAML = """name: CI
jobs:
  test:
    steps:
      - name: Run the suite
        run: |
          cargo nextest run --all-features --workspace --lib --tests \\
            --exclude fake-jepsen \\
            -E 'not binary(process_probe)' \\
            -- --skip a_noisy_test @EXTRA_CI@
"""

NIGHTLY_YAML = """name: Nightly Fuzz
jobs:
  fuzz:
    steps:
      - name: Run property tests
        run: |
          cargo test --all-features --workspace --lib --tests \\
            --exclude fake-jepsen \\
            -- --skip a_noisy_test @EXTRA_NIGHTLY@
"""

PREPUSH_SH = """#!/usr/bin/env bash
set -euo pipefail
SKIPS=(
  --skip a_noisy_test
@EXTRA_PREPUSH@
)
exec cargo test \\
  --all-features \\
  --workspace --lib --tests \\
  --exclude fake-jepsen \\
  -- "${SKIPS[@]}"
"""

NO_LANE_YAML = """name: Guard
jobs:
  docs:
    steps:
      - run: echo nothing to run here
"""


def fixture(
    *,
    extra_ci: str = "",
    extra_nightly: str = "",
    extra_prepush: str = "",
    ci_yaml: str = CI_YAML,
    nightly_yaml: str = NIGHTLY_YAML,
    prepush: str = PREPUSH_SH,
    files: dict | None = None,
) -> tempfile.TemporaryDirectory:
    tmp = tempfile.TemporaryDirectory()
    root = Path(tmp.name)
    (root / "fake-crate" / "tests").mkdir(parents=True)
    (root / "fake-crate" / "Cargo.toml").write_text('[package]\nname = "fake-crate"\n')
    (root / "fake-crate" / "tests" / "live_cluster.rs").write_text(GATED_TARGET)
    (root / "fake-jepsen" / "tests").mkdir(parents=True)
    (root / "fake-jepsen" / "Cargo.toml").write_text('[package]\nname = "fake-jepsen"\n')
    (root / "fake-jepsen" / "tests" / "live_jepsen.rs").write_text(GATED_TARGET)
    (root / ".github" / "workflows").mkdir(parents=True)
    (root / ".github" / "workflows" / "ci.yml").write_text(
        ci_yaml.replace("@EXTRA_CI@", extra_ci)
    )
    (root / ".github" / "workflows" / "nightly-fuzz.yml").write_text(
        nightly_yaml.replace("@EXTRA_NIGHTLY@", extra_nightly)
    )
    (root / "scripts").mkdir(parents=True)
    (root / "scripts" / "pre-push-mirror-ci.sh").write_text(
        prepush.replace("@EXTRA_PREPUSH@", extra_prepush)
    )
    for name, body in (files or {}).items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body)
    return tmp


def run_guard(root: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(GUARD), str(root)], capture_output=True, text=True
    )


class GuardTest(unittest.TestCase):
    def expect(self, want: int, msg: str, **kwargs) -> subprocess.CompletedProcess:
        with fixture(**kwargs) as d:
            got = run_guard(Path(d))
            self.assertEqual(got.returncode, want, f"{msg}\n{got.stdout}{got.stderr}")
            return got

    def test_a_lane_missing_a_live_skip_is_reported(self):
        got = self.expect(1, "nightly omits the skip", extra_nightly="")
        self.assertIn("needs_a_cluster", got.stderr, got.stderr)
        self.assertIn("nightly-fuzz.yml", got.stderr, got.stderr)
        # The lane that has the skip must not be named in the report.
        self.assertNotIn("tests/ci", got.stderr)

    def test_every_lane_carrying_the_skip_passes(self):
        self.expect(
            0,
            "all three lanes carry it",
            extra_ci="--skip needs_a_cluster",
            extra_nightly="--skip needs_a_cluster",
            extra_prepush="  --skip needs_a_cluster",
        )

    def test_one_lane_missing_it_still_fails(self):
        got = self.expect(
            1,
            "pre-push mirror omits it",
            extra_ci="--skip needs_a_cluster",
            extra_nightly="--skip needs_a_cluster",
            extra_prepush="",
        )
        self.assertIn("pre-push-mirror-ci.sh", got.stderr, got.stderr)
        self.assertIn("needs_a_cluster", got.stderr, got.stderr)

    def test_a_crate_exclude_covers_all_of_its_live_tests(self):
        # fake-jepsen is excluded by every lane, so its live test must not be
        # demanded by name; fake-crate still needs the explicit skip.
        self.expect(
            0,
            "excluded crate",
            extra_ci="--skip needs_a_cluster",
            extra_nightly="--skip needs_a_cluster",
            extra_prepush="  --skip needs_a_cluster",
        )

    def test_a_prefix_skip_covers_the_test(self):
        self.expect(
            0,
            "substring skip",
            extra_ci="--skip needs_",
            extra_nightly="--skip needs_",
            extra_prepush="  --skip needs_",
        )

    def test_a_nextest_binary_exclusion_covers_the_test(self):
        self.expect(
            0,
            "binary exclusion",
            ci_yaml=CI_YAML.replace("not binary(process_probe)", "not binary(live_cluster)"),
            extra_nightly="--skip needs_a_cluster",
            extra_prepush="  --skip needs_a_cluster",
        )

    def test_a_lane_file_without_an_all_features_command_fails_loud(self):
        got = self.expect(1, "parser staleness", nightly_yaml=NO_LANE_YAML,
                          extra_ci="--skip needs_a_cluster", extra_prepush="  --skip needs_a_cluster")
        self.assertIn("nightly-fuzz.yml", got.stderr, got.stderr)
        self.assertIn("no `cargo test", got.stderr, got.stderr)

    def test_an_ungated_test_is_not_demanded_of_a_lane(self):
        self.expect(
            0,
            "plain test needs no skip",
            extra_ci="--skip needs_a_cluster",
            extra_nightly="--skip needs_a_cluster",
            extra_prepush="  --skip needs_a_cluster",
            files={"fake-crate/tests/plain.rs": PLAIN_TARGET},
        )

    def test_a_helper_fn_behind_the_gate_is_not_demanded(self):
        got = self.expect(
            1,
            "only the gated #[test] fn is demanded",
            files={"fake-crate/tests/live_cluster.rs": HELPER_GATED},
        )
        self.assertIn("needs_containers", got.stderr, got.stderr)
        self.assertNotIn("helper_that_is_not_a_test", got.stderr, got.stderr)

    def test_a_repository_with_no_live_infra_test_is_reported_as_a_broken_scan(self):
        got = self.expect(
            1,
            "empty scan",
            files={
                "fake-crate/tests/live_cluster.rs": PLAIN_TARGET,
                "fake-jepsen/tests/live_jepsen.rs": PLAIN_TARGET,
            },
        )
        self.assertIn("the scan is broken", got.stderr, got.stderr)

    def test_this_repository_covers_every_live_infra_test_in_every_lane(self):
        got = run_guard(REPO)
        self.assertEqual(got.returncode, 0, got.stdout + got.stderr)


if __name__ == "__main__":
    unittest.main()
