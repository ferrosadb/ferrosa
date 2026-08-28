import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


class FerrosaCtlDependencyFeaturesTest(unittest.TestCase):
    def test_tabled_runtime_api_does_not_pull_derive_proc_macros(self) -> None:
        result = subprocess.run(
            ["cargo", "tree", "-p", "ferrosa-ctl", "-e", "features"],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        forbidden = ["tabled_derive", "proc-macro-error2"]
        resolved = [name for name in forbidden if name in result.stdout]
        self.assertEqual([], resolved, "unexpected tabled derive dependencies")


if __name__ == "__main__":
    unittest.main()
