"""Execute the CI mirror's setup boundary with observed external commands.

The all-features CQL test needs the same ASC bundle CI builds. These doubles
observe setup order and failure propagation; actual ASC compilation is tested
by the workspace test against a bundle produced by the real pinned builder.
"""

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MIRROR = ROOT / "scripts/pre-push-mirror-ci.sh"


class PrePushPrerequisitesTest(unittest.TestCase):
    def run_mirror(self, supplied=False, build_failure=False):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            calls = tmp / "calls"
            shim = '''#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
kind = Path(sys.argv[0]).name
bundle = os.environ.get("FERROSA_ASC_BUNDLE", "")
with open(os.environ["MIRROR_CALLS"], "a") as out:
    out.write(json.dumps([kind, bundle, sys.argv[1:]]) + "\\n")
if kind == "bash":
    assert sys.argv[1] == "ferrosa-udf/examples/asc-poc/build-bundle.sh", sys.argv
    assert sys.argv[2] == bundle, sys.argv
    if os.environ["MIRROR_BUILD_FAILURE"] == "1":
        sys.exit(23)
    Path(bundle).write_text("observed compiler artifact")
else:
    assert bundle and Path(bundle).stat().st_size > 0, "cargo ran before ASC setup"
'''
            for name in ("bash", "cargo"):
                p = tmp / name
                p.write_text(shim)
                p.chmod(0o755)
            env = {**os.environ, "PATH": f"{tmp}{os.pathsep}{os.environ['PATH']}",
                   "TMPDIR": str(tmp), "MIRROR_CALLS": str(calls),
                   "MIRROR_BUILD_FAILURE": str(int(build_failure))}
            env.pop("FERROSA_ASC_BUNDLE", None)
            env.pop("FERROSA_SKIP_PREPUSH", None)
            if supplied:
                p = tmp / "provided.mjs"
                p.write_text("provided compiler artifact")
                env["FERROSA_ASC_BUNDLE"] = str(p)
            proc = subprocess.run(["/bin/bash", str(MIRROR)], cwd=ROOT,
                                  env=env, capture_output=True, text=True, timeout=10)
            events = [json.loads(s) for s in calls.read_text().splitlines()] if calls.exists() else []
            cleaned = not events or not Path(events[0][1]).exists()
            return proc, events, cleaned

    def test_builds_and_exports_the_bundle_before_running_tests_then_cleans_it(self):
        proc, calls, cleaned = self.run_mirror()
        self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)
        self.assertEqual(["bash", "cargo"], [c[0] for c in calls], calls)
        self.assertEqual(calls[0][1], calls[1][1])
        self.assertTrue(cleaned, calls)

    def test_reuses_an_explicit_bundle(self):
        proc, calls, _ = self.run_mirror(supplied=True)
        self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)
        self.assertEqual(["cargo"], [c[0] for c in calls])

    def test_build_failure_prevents_tests_and_preserves_the_error(self):
        proc, calls, cleaned = self.run_mirror(build_failure=True)
        self.assertEqual(23, proc.returncode, proc.stdout + proc.stderr)
        self.assertEqual(["bash"], [c[0] for c in calls])
        self.assertTrue(cleaned, calls)


if __name__ == "__main__":
    unittest.main()
