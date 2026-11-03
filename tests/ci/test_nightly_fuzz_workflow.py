import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "nightly-fuzz.yml"
DOCKER_SMOKE = ROOT / "tests" / "docker-smoke.sh"


class NightlyFuzzWorkflowTest(unittest.TestCase):
    def test_property_fuzz_keeps_loadgen_coverage_but_skips_binary_smoke(self):
        workflow = WORKFLOW.read_text(encoding="utf-8")
        match = re.search(
            r"name: Run property tests \([^\n]*fuzz session\).*?run: \|\n(?P<run>(?: {10}[^\n]*\n|\n)*)",
            workflow,
            flags=re.S,
        )
        self.assertIsNotNone(match, "nightly fuzz workflow should have a property-test run block")
        assert match is not None

        run_block = match.group("run")
        self.assertIn(
            "--all-features --workspace --lib --tests",
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

    def test_pair_failover_keeps_replica_reads_and_requires_promotion_for_writes(self):
        smoke = DOCKER_SMOKE.read_text(encoding="utf-8")
        phase_two = smoke.split("# Phase 2: Kill primary, verify degraded behavior", 1)[1]
        phase_two, phase_three = phase_two.split("# Phase 3: Operator promotion", 1)

        self.assertIn(
            'wait_for_cql_value 9043 "SELECT v FROM smoke_test.kv WHERE k = \'key1\';"',
            phase_two,
            "an unpromoted secondary must still serve its replicated data",
        )
        self.assertIn(
            '"from_node1" "unpromoted node2 read key1"',
            phase_two,
            "the replica read must return the replicated value, not merely succeed",
        )
        self.assertIn(
            'if cql2 "INSERT INTO smoke_test.kv (k, v) VALUES (\'should_fail\', \'nope\')"',
            phase_two,
            "writes must remain fenced until operator promotion",
        )
        self.assertIn(
            "Unpromoted node2 served a CQL write after node1 death",
            phase_two,
        )

        promote = 'curl -s -X POST "http://localhost:9091/api/cluster/promote"'
        self.assertIn(promote, phase_three)
        self.assertIn('wait_cql 9043 "node2" "$PAIR_CQL_TIMEOUT"', phase_three)
        for key in ("key1", "key2"):
            self.assertGreater(
                phase_three.index(f"SELECT v FROM smoke_test.kv WHERE k = '{key}';"),
                phase_three.index(promote),
                "the promoted-node phase must also verify both replicated values",
            )


if __name__ == "__main__":
    unittest.main()
