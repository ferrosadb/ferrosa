import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
RELEASE_WORKFLOW = ROOT / ".github" / "workflows" / "release.yml"


class ReleaseWorkflowTest(unittest.TestCase):
    def test_release_builds_do_not_run_cache_post_jobs(self):
        workflow = RELEASE_WORKFLOW.read_text(encoding="utf-8")
        self.assertNotIn("Swatinem/rust-cache@", workflow)
        self.assertNotIn("actions/cache/", workflow)

    def test_release_downloads_artifacts_without_node_actions(self):
        workflow = RELEASE_WORKFLOW.read_text(encoding="utf-8")
        self.assertNotIn("actions/download-artifact@", workflow)
        self.assertNotIn("gh run download", workflow)
        self.assertEqual(
            2,
            workflow.count(
                'bash .github/scripts/download-run-artifacts.sh \\\n            "${GITHUB_RUN_ID}"'
            ),
        )
        self.assertEqual(2, workflow.count("actions: read"))
        docker_job = workflow.split("  docker-image:\n", 1)[1].split("\n  release:\n", 1)[0]
        release_job = workflow.split("  release:\n", 1)[1]
        self.assertIn(
            "    permissions:\n      actions: read\n      contents: read\n      packages: write",
            docker_job,
        )
        self.assertIn(
            "    permissions:\n      actions: read\n      contents: write",
            release_job,
        )
        self.assertNotIn("\npermissions:\n  actions: read", workflow)

    def test_release_checkout_is_quiet_and_manifest_inspection_fails_loud(self):
        workflow = RELEASE_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn(
            "env:\n"
            "  CARGO_TERM_COLOR: always\n"
            "  GIT_CONFIG_COUNT: 1\n"
            "  GIT_CONFIG_KEY_0: init.defaultBranch\n"
            "  GIT_CONFIG_VALUE_0: main",
            workflow,
        )
        self.assertNotIn("manifest inspect failed", workflow)
        self.assertNotIn("docker buildx imagetools inspect \"${first_tag}\" ||", workflow)


if __name__ == "__main__":
    unittest.main()
