import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
CLUSTER_COMPOSE = ROOT / "tests" / "docker-compose.cluster.yml"
WORKFLOW = ROOT / ".github" / "workflows" / "cluster-data-loss.yml"
CI_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"


def start_cluster_step() -> str:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    step = workflow.split("- name: Start 3-node cluster", 1)[1]
    return step.split("- name: Install Python test deps", 1)[0]


class ClusterDataLossWorkflowTest(unittest.TestCase):
    # Test list:
    # - [x] Every Ferrosa node supports the nightly image override.
    # - [x] Nightly cluster startup forbids source builds.
    # - [x] Readiness has a wall-clock deadline and fails with diagnostics.
    # - [x] Missing nightly artifacts cannot fall back to a source build.
    # - [x] The PR gate runs this regression suite.

    def test_every_node_supports_the_nightly_image_override(self):
        compose = CLUSTER_COMPOSE.read_text(encoding="utf-8")

        self.assertEqual(
            compose.count(
                "image: ${FERROSA_NIGHTLY_IMAGE:-ferrosa-test-node:latest}"
            ),
            5,
            "all trio and quint nodes must consume the pre-built nightly image when set",
        )

    def test_nightly_cluster_startup_forbids_source_builds(self):
        start_step = start_cluster_step()

        self.assertIn(
            "docker compose -f tests/docker-compose.cluster.yml --profile trio up -d --no-build",
            start_step,
            "the packaged nightly regression must never compile the source checkout",
        )

    def test_cluster_readiness_has_a_deadline_and_fails_with_diagnostics(self):
        start_step = start_cluster_step()

        self.assertIn("deadline=$((SECONDS + 120))", start_step)
        self.assertIn("while (( SECONDS < deadline )); do", start_step)
        self.assertIn("for node in node1 node2 node3; do", start_step)
        self.assertIn(".State.Health.Status", start_step)
        self.assertIn('echo "::error::Cluster did not become healthy', start_step)
        self.assertIn("docker compose -f tests/docker-compose.cluster.yml ps", start_step)
        self.assertIn("exit 1", start_step)
        self.assertNotIn("seq 1 60", start_step)

    def test_missing_nightly_artifact_cannot_fall_back_to_a_source_build(self):
        workflow = WORKFLOW.read_text(encoding="utf-8")

        self.assertNotIn("Build Ferrosa image (fallback)", workflow)
        self.assertNotIn("docker build -t ferrosa-nightly .", workflow)

    def test_pr_gate_runs_the_cluster_workflow_regression_suite(self):
        workflow = CI_WORKFLOW.read_text(encoding="utf-8")

        self.assertIn(
            "python3 -m unittest tests.ci.test_cluster_data_loss_workflow",
            workflow,
        )


if __name__ == "__main__":
    unittest.main()
