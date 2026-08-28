import hashlib
import os
import subprocess
import tempfile
import unittest
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / ".github" / "scripts" / "download-run-artifacts.sh"


class DownloadRunArtifactsTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.archive = self.root / "artifact.zip"
        with zipfile.ZipFile(self.archive, "w") as archive:
            archive.writestr("payload.txt", "trusted payload\n")
        self.digest = hashlib.sha256(self.archive.read_bytes()).hexdigest()

        fake_bin = self.root / "bin"
        fake_bin.mkdir()
        fake_gh = fake_bin / "gh"
        fake_gh.write_text(
            "#!/usr/bin/env bash\n"
            "set -euo pipefail\n"
            "if [[ \"$*\" == *'/artifacts?'* ]]; then\n"
            "  cat \"${FAKE_ARTIFACT_MANIFEST}\"\n"
            "elif [[ \"$*\" == *'/artifacts/42/zip'* ]]; then\n"
            "  cat \"${FAKE_ARTIFACT_ZIP}\"\n"
            "else\n"
            "  echo \"unexpected gh invocation: $*\" >&2\n"
            "  exit 64\n"
            "fi\n",
            encoding="utf-8",
        )
        fake_gh.chmod(0o755)
        self.env = os.environ.copy()
        self.env.update(
            {
                "FAKE_ARTIFACT_ZIP": str(self.archive),
                "PATH": f"{fake_bin}{os.pathsep}{self.env['PATH']}",
            }
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def run_download(self, digest):
        manifest = self.root / "manifest.tsv"
        manifest.write_text(
            "" if digest is None else f"artifact-one\t42\t{digest}\n",
            encoding="utf-8",
        )
        env = self.env | {"FAKE_ARTIFACT_MANIFEST": str(manifest)}
        destination = self.root / "artifacts"
        result = subprocess.run(
            [str(SCRIPT), "123", "ferrosadb/ferrosa", str(destination)],
            capture_output=True,
            check=False,
            env=env,
            text=True,
        )
        return result, destination

    def test_verifies_digest_before_extracting_artifact(self):
        result, destination = self.run_download(f"sha256:{self.digest}")

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual(
            "trusted payload\n",
            (destination / "artifact-one" / "payload.txt").read_text(encoding="utf-8"),
        )

    def test_rejects_digest_mismatch_before_extraction(self):
        result, destination = self.run_download(f"sha256:{'0' * 64}")

        self.assertNotEqual(0, result.returncode)
        self.assertIn("digest mismatch", result.stderr)
        self.assertFalse((destination / "artifact-one").exists())

    def test_rejects_missing_digest(self):
        result, _ = self.run_download("")

        self.assertNotEqual(0, result.returncode)
        self.assertIn("missing or malformed SHA-256 digest", result.stderr)

    def test_rejects_empty_artifact_manifest(self):
        result, _ = self.run_download(None)

        self.assertNotEqual(0, result.returncode)
        self.assertIn("has no downloadable artifacts", result.stderr)


if __name__ == "__main__":
    unittest.main()
