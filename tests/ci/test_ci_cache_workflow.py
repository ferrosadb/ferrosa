from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yml"


class CiCacheWorkflowTests(unittest.TestCase):
    def test_node_image_build_uses_and_verifies_action_managed_sccache(self):
        workflow = WORKFLOW.read_text(encoding="utf-8")
        node_job = workflow.split("  build-node-image:", 1)[1]
        node_job = node_job.split("\n  driver-smoke:", 1)[0]

        self.assertIn('RUSTC_WRAPPER="$SCCACHE_PATH" cargo build --release', node_job)
        self.assertIn("- name: Verify sccache handled compiler requests", node_job)
        self.assertIn('"compile_requests"', node_job)


if __name__ == "__main__":
    unittest.main()
