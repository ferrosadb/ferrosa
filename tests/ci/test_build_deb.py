"""The .deb carries the glibc build and declares the glibc it needs.

glibc builds run 15-20% faster than musl, and a Debian/Ubuntu host always has
glibc, so the .deb packages the glibc binary. Its control file declares
`Depends: libc6 (>= X)`, with X read from the packaged binary itself, so apt
refuses an install on a host whose glibc is too old instead of the binary
failing at start. The release builds glibc on Ubuntu 22.04 so that X stays at
or below 2.35, and build-deb.sh refuses a binary whose requirement is above
the floor it is given: Ubuntu 22.04 must stay installable.
"""

import re
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / ".github" / "scripts" / "build-deb.sh"
# A real dynamically linked glibc ELF, standing in for the ferrosa binaries.
GLIBC_ELF = shutil.which("true")


def glibc_requirement(path):
    out = subprocess.run(["objdump", "-T", path], capture_output=True, text=True, check=True).stdout
    versions = set(re.findall(r"GLIBC_(\d+\.\d+)", out))
    return max(versions, key=lambda v: tuple(int(p) for p in v.split(".")))


def build(max_glibc):
    work = Path(tempfile.mkdtemp())
    bins = work / "bin"
    bins.mkdir()
    for name in ("ferrosa", "ferrosa-ctl"):
        shutil.copy(GLIBC_ELF, bins / name)
    proc = subprocess.run(
        ["bash", str(SCRIPT), "amd64", str(bins), "9.9.9", max_glibc, str(work / "dist")],
        capture_output=True, text=True, cwd=REPO_ROOT,
    )
    return proc, work / "dist" / "ferrosa_9.9.9_amd64.deb"


def field(deb, name):
    return subprocess.run(["dpkg-deb", "-f", str(deb), name], capture_output=True, text=True, check=True).stdout.strip()


class BuildDeb(unittest.TestCase):
    def test_the_deb_depends_on_the_glibc_its_binary_needs(self):
        proc, deb = build("99.0")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        required = glibc_requirement(GLIBC_ELF)
        self.assertEqual(field(deb, "Depends"), f"ca-certificates, libc6 (>= {required})")
        self.assertEqual(field(deb, "Architecture"), "amd64")
        self.assertEqual(field(deb, "Version"), "9.9.9")

    def test_the_deb_ships_the_given_binaries_and_the_service(self):
        proc, deb = build("99.0")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        listing = subprocess.run(["dpkg-deb", "-c", str(deb)], capture_output=True, text=True, check=True).stdout
        for path in ("./usr/bin/ferrosa", "./usr/bin/ferrosa-ctl", "./lib/systemd/system/ferrosa.service"):
            self.assertIn(path, listing)

    def test_a_binary_needing_more_than_the_floor_is_refused(self):
        proc, deb = build("2.0")
        self.assertNotEqual(proc.returncode, 0)
        self.assertFalse(deb.exists())
        self.assertIn("newer than 2.0", proc.stderr)


if __name__ == "__main__":
    unittest.main()
