import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "nightly-fuzz.yml"


class NightlyFuzzWorkflowTest(unittest.TestCase):
    def test_property_fuzz_keeps_loadgen_coverage_but_skips_binary_smoke(self):
        workflow = WORKFLOW.read_text(encoding="utf-8")
        match = re.search(
            r"name: Run property tests \(45-minute fuzz session\).*?run: \|\n(?P<run>.*?)(?:\n\s{6}\S|\Z)",
            workflow,
            flags=re.S,
        )
        self.assertIsNotNone(match, "nightly fuzz workflow should have a property-test run block")
        assert match is not None

        run_block = match.group("run")
        self.assertIn(
            "cargo test --workspace",
            run_block,
            "nightly fuzz must continue running the workspace property suite",
        )
        self.assertNotIn(
            "--exclude ferrosa-loadgen",
            run_block,
            "do not drop ferrosa-loadgen property coverage to avoid binary_smoke",
        )
        self.assertIn(
            "--skip binary_",
            run_block,
            "nightly fuzz should intentionally skip loadgen binary smoke tests that require FERROSA_TEST_LOADGEN",
        )
        self.assertIn(
            "--skip ucs_load_s3_",
            run_block,
            "nightly fuzz should intentionally skip loadgen S3 smoke tests unless it provisions FERROSA_TEST_CONTAINERS",
        )
        for smoke_test in (
            "ucs_load_read_heavy",
            "ucs_load_balanced",
            "ucs_load_write_heavy",
        ):
            self.assertIn(
                f"--skip {smoke_test}",
                run_block,
                "nightly fuzz should skip long deterministic load smoke tests while keeping proptest coverage",
            )
        self.assertNotIn(
            "--skip ucs_load_random_profile",
            run_block,
            "nightly fuzz must keep ferrosa-loadgen proptest coverage active",
        )


if __name__ == "__main__":
    unittest.main()
