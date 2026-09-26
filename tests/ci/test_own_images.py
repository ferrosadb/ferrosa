"""Base images come from downloads.ferrosa.ai, pinned by digest, never upstream.

The mirrors live in ferrosa-installer (images/services/sources.json) and are
served by the static registry at downloads.ferrosa.ai. Pulling debian, alpine,
ubuntu, rustfs or aws-cli straight from Docker Hub / quay / ECR made CI red
whenever an upstream tag was removed or rate-limited: on 2026-09-25 PR CI and
four nightlies died on `quay.io/minio/mc` and `rustfs/rustfs:latest`.

Images with no mirror yet are listed in NOT_MIRRORED_YET with the reason, so
the gap is visible and a mirror landing forces the entry to be deleted.
"""
from __future__ import annotations

import re
import subprocess
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
MIRRORED = ("debian", "alpine", "ubuntu", "aws-cli")
# Upstream spellings of the mirrored images, and of images whose upstream broke us.
UPSTREAM = re.compile(
    r"(?<![\w./-])(?:docker\.io/)?(?:library/)?(?:debian|alpine|ubuntu)(?::[\w.-]+)?(?=\s|$|\")"
    r"|quay\.io/minio/mc|public\.ecr\.aws/aws-cli|amazon/aws-cli"
)
IMAGE_REF = re.compile(r"^\s*(?:image:|FROM)\s+(\S+)", re.MULTILINE)
FILES = ("Dockerfile*", ".github/workflows/*.yml", "*docker-compose*.yml", "scripts/*.sh")

# path -> reason it may still name an upstream image.
NOT_MIRRORED_YET = {
    "docker-compose.yml": "rustfs example: our rustfs build is amd64-only",
}


def tracked() -> list[Path]:
    out = subprocess.run(
        ["git", "ls-files", "--", *FILES], cwd=REPO, check=True, capture_output=True, text=True
    ).stdout.split()
    return [REPO / p for p in out]


def upstream_refs(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8", errors="replace")
    refs = [m.group(1) for m in IMAGE_REF.finditer(text)]
    refs += re.findall(r"docker (?:pull|run)[^\n]*?(\S+:\S+)", text)
    return [r for r in refs if UPSTREAM.search(r)]


class OwnImages(unittest.TestCase):
    def test_the_scan_sees_files(self):
        self.assertGreater(len(tracked()), 20)

    def test_base_images_are_ours_and_pinned_by_digest(self):
        offenders = {}
        for path in tracked():
            rel = str(path.relative_to(REPO))
            if rel in NOT_MIRRORED_YET:
                continue
            bad = upstream_refs(path)
            if bad:
                offenders[rel] = bad
        self.assertEqual(offenders, {}, "pull these from downloads.ferrosa.ai@sha256:...")

    def test_our_registry_refs_are_digest_pinned(self):
        for path in tracked():
            text = path.read_text(encoding="utf-8", errors="replace")
            for ref in re.findall(r"downloads\.ferrosa\.ai/(?:debian|alpine|ubuntu|aws-cli|rustfs)(?::[\w.-]+|@sha256:\w+)?", text):
                self.assertIn("@sha256:", ref, f"{path.name}: {ref} must be pinned by digest")

    def test_no_stale_exemptions(self):
        for rel in NOT_MIRRORED_YET:
            self.assertTrue(upstream_refs(REPO / rel) or "rustfs" in (REPO / rel).read_text(),
                            f"{rel} no longer needs its NOT_MIRRORED_YET entry")


if __name__ == "__main__":
    unittest.main()
