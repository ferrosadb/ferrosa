"""check-sync.sh: an extraction is tied to its sources, and to its translator where one exists.

The source-hash comparison needs nothing installed, so it runs everywhere. The translator
comparison needs an aeneas checkout. Where there is none -- every CI runner -- it cannot be made,
and the script must say so rather than report a translator that "moved" to `unknown`.
"""

import hashlib
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "specs" / "lean4-rust" / "check-sync.sh"
CHARON = "7b462d9159db2ed3015162352183906a30bfafc6"


def git(cwd, *args):
    subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True)


def source_hash(crate_dir, sources):
    """The same hash check-sync.sh computes: sha256sum of each file, then of that listing."""
    listing = ""
    for path in sorted((crate_dir).glob(sources)):
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        listing += f"{digest}  {path.relative_to(crate_dir)}\n"
    return hashlib.sha256(listing.encode()).hexdigest()


class CheckSync(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.repo = self.tmp / "repo"
        spec = self.repo / "specs" / "lean4-rust"
        (spec / "FerrosaRust").mkdir(parents=True)
        shutil.copy(SCRIPT, spec / "check-sync.sh")
        (spec / "FerrosaRust" / "Demo.lean").write_text("-- extracted\n")
        crate = self.repo / "demo-crate" / "src"
        crate.mkdir(parents=True)
        (crate / "lib.rs").write_text("pub fn f() {}\n")
        git(self.repo, "init", "-q")
        self.aeneas = self.tmp / "aeneas"
        self.aeneas.mkdir()
        git(self.aeneas, "init", "-q")
        git(self.aeneas, "-c", "user.name=t", "-c", "user.email=t@t", "commit",
            "-q", "--allow-empty", "-m", "a")
        self.aeneas_rev = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], cwd=self.aeneas,
            check=True, capture_output=True, text=True).stdout.strip()

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def write_lock(self, aeneas_rev):
        crate = self.repo / "demo-crate"
        line = (f"Demo demo-crate {source_hash(crate, 'src/lib.rs')} src/lib.rs demo::* "
                f"aeneas {aeneas_rev} charon {CHARON} ok\n")
        (self.repo / "specs" / "lean4-rust" / "sync.lock").write_text(line)

    def run_check(self, aeneas_dir):
        env = dict(os.environ, HOME=str(self.tmp / "home"), AENEAS_DIR=str(aeneas_dir))
        return subprocess.run(["bash", str(self.repo / "specs/lean4-rust/check-sync.sh")],
                              env=env, capture_output=True, text=True)

    def test_without_an_aeneas_checkout_the_translator_is_reported_not_compared(self):
        self.write_lock(self.aeneas_rev)
        out = self.run_check(self.tmp / "no-such-aeneas")
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr)
        self.assertIn("not compared", out.stdout)
        self.assertNotIn("current is unknown", out.stdout)

    def test_matching_sources_and_translator_are_in_sync(self):
        self.write_lock(self.aeneas_rev)
        out = self.run_check(self.aeneas)
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr)
        self.assertIn("in sync", out.stdout)

    def test_a_translator_that_moved_fails(self):
        self.write_lock("0000000")
        out = self.run_check(self.aeneas)
        self.assertEqual(out.returncode, 1, out.stdout)
        self.assertIn("TOOL    Demo", out.stdout)

    def test_sources_that_moved_fail_even_without_a_translator(self):
        self.write_lock(self.aeneas_rev)
        (self.repo / "demo-crate" / "src" / "lib.rs").write_text("pub fn g() {}\n")
        out = self.run_check(self.tmp / "no-such-aeneas")
        self.assertEqual(out.returncode, 1, out.stdout)
        self.assertIn("DRIFT   Demo", out.stdout)


if __name__ == "__main__":
    unittest.main()
