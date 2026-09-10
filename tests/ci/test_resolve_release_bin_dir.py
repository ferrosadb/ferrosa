"""Behavioural tests for .github/scripts/resolve-release-bin-dir.sh.

These RUN the script against fabricated target directories rather than
grepping it, because the bug it fixes was a wrong answer, not a missing
string: `cargo build --release` reported success and the caller then looked
for the binary in a directory cargo had not written to.

The layouts below are the ones cargo actually produces:

  <target-dir>/release/            plain `cargo build --release`
  <target-dir>/<triple>/release/   when --target or `build.target` is set

and <target-dir> itself moves with CARGO_TARGET_DIR or `build.target-dir`.
"""

import os
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / ".github" / "scripts" / "resolve-release-bin-dir.sh"
TRIPLE = "x86_64-unknown-linux-gnu"


def make_binaries(directory: Path, *names: str) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    for name in names:
        f = directory / name
        f.write_text("#!/bin/sh\nexit 0\n")
        f.chmod(f.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def run(cwd: Path, env_extra: dict | None = None):
    env = dict(os.environ)
    # Keep the fabricated tree authoritative: a real `cargo metadata` run from
    # a temp dir would fail anyway, and the script falls back to the env.
    env.pop("CARGO_TARGET_DIR", None)
    if env_extra:
        env.update(env_extra)
    return subprocess.run(
        ["bash", str(SCRIPT), TRIPLE],
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
    )


class ResolveReleaseBinDirTest(unittest.TestCase):
    # Test list:
    # - [x] plain layout: <target-dir>/release
    # - [x] triple layout: <target-dir>/<triple>/release
    # - [x] CARGO_TARGET_DIR relocates the target directory
    # - [x] the triple layout wins nothing when only one binary is present
    # - [x] missing binaries fail loud, naming where it looked

    def test_finds_the_plain_release_layout(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            make_binaries(root / "target" / "release", "ferrosa", "ferrosa-ctl")
            r = run(root)
            self.assertEqual(r.returncode, 0, r.stderr)
            self.assertEqual(r.stdout.strip(), "target/release")

    def test_finds_the_target_triple_layout(self):
        # This is the layout that broke the self-hosted Linux builder: a
        # `build.target` in a cargo config puts the output one level deeper,
        # and the old hardcoded `target/release` missed it.
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            make_binaries(root / "target" / TRIPLE / "release", "ferrosa", "ferrosa-ctl")
            r = run(root)
            self.assertEqual(r.returncode, 0, r.stderr)
            self.assertEqual(r.stdout.strip(), f"target/{TRIPLE}/release")

    def test_honours_cargo_target_dir(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            make_binaries(root / "shared-target" / "release", "ferrosa", "ferrosa-ctl")
            r = run(root, {"CARGO_TARGET_DIR": "shared-target"})
            self.assertEqual(r.returncode, 0, r.stderr)
            self.assertEqual(r.stdout.strip(), "shared-target/release")

    def test_a_directory_holding_only_one_binary_is_not_accepted(self):
        # Half a build is not a build. Accepting a directory with only
        # `ferrosa` would push the failure into the staging script, one step
        # further from the cause.
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            make_binaries(root / "target" / "release", "ferrosa")
            make_binaries(root / "target" / TRIPLE / "release", "ferrosa", "ferrosa-ctl")
            r = run(root)
            self.assertEqual(r.returncode, 0, r.stderr)
            self.assertEqual(
                r.stdout.strip(),
                f"target/{TRIPLE}/release",
                "must skip the directory that is missing ferrosa-ctl",
            )

    def test_missing_binaries_fail_loud_and_say_where_it_looked(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            (root / "target").mkdir()
            r = run(root)
            self.assertNotEqual(r.returncode, 0, "an absent binary must not exit 0")
            self.assertEqual(r.stdout.strip(), "", "nothing may be printed as a result")
            for expected in ("target/release", f"target/{TRIPLE}/release", "build.target"):
                self.assertIn(
                    expected,
                    r.stderr,
                    "the failure must name where it looked and what to check, "
                    "or it reads as a build failure rather than a path one",
                )


if __name__ == "__main__":
    unittest.main()
