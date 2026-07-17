"""Regression guards for the root-context Fly image build."""

from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[2]
DOCKERFILE = ROOT / "deploy" / "fly-bench" / "ferrosa-main.Dockerfile"


class FlyBenchDockerfileTest(unittest.TestCase):
    def test_entrypoint_is_copied_from_its_repository_path(self) -> None:
        dockerfile = DOCKERFILE.read_text(encoding="utf-8")

        self.assertIn(
            "COPY deploy/fly-bench/ferrosa-entrypoint.sh /usr/local/bin/",
            dockerfile,
            "the Fly Dockerfile must build directly from the repository root; "
            "it must not require a caller to stage ferrosa-entrypoint.sh",
        )


if __name__ == "__main__":
    unittest.main()
