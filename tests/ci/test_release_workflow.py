import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
RELEASE_WORKFLOW = ROOT / ".github" / "workflows" / "release.yml"


class ReleaseWorkflowTest(unittest.TestCase):
    def test_release_builds_do_not_run_cache_post_jobs(self):
        workflow = RELEASE_WORKFLOW.read_text(encoding="utf-8")
        self.assertNotIn("Swatinem/rust-cache@", workflow)
        self.assertNotIn("actions/cache/", workflow)


if __name__ == "__main__":
    unittest.main()
