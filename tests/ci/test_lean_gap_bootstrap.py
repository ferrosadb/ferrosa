"""Execute the pinned Charon build graph through the bootstrap adapter."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/lean_gap_bootstrap.py"


class BootstrapBuildGraph(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.source = self.root / "aeneas"
        self.charon = self.source / "charon"
        ml = self.charon / "charon-ml"
        ml.mkdir(parents=True)
        # Byte-identical Makefiles from pinned Charon 7b462d91 exercise real
        # recursive $(MAKE), version generation, binary copying and Dune recipes.
        fixture = json.loads((ROOT / "tests/ci/fixtures/charon-7b462d91/makefiles.json").read_text())
        for name, content in fixture["files"].items():
            data = content.encode()
            blob = hashlib.sha1(f"blob {len(data)}\0".encode() + data).hexdigest()
            self.assertEqual(blob, fixture["gitBlobs"][name])
            (self.charon / name).write_bytes(data)
        (ml / "src").mkdir()
        rust = self.charon / "charon"
        rust.mkdir()
        (rust / "Cargo.toml").write_text('version = "0.1.0"\n')
        (rust / "Makefile").write_text(
            "all:\n\t@printf 'rust-release\\n' >> $$BUILD_LOG\n"
            "\t@printf '%s\\n' '$(MAKEFLAGS)' > $$MAKE_FLAGS_RECEIPT\n"
            "\t@mkdir -p target/release\n"
            "\t@printf 'charon' > target/release/charon\n"
            "\t@printf 'driver' > target/release/charon-driver\n"
        )
        self.log = self.root / "calls.log"
        bin_dir = self.root / "bin"
        bin_dir.mkdir()
        dune = bin_dir / "dune"
        dune.write_text(
            "#!/usr/bin/env python3\n"
            "import fcntl, os, pathlib, sys\n"
            "root=pathlib.Path(os.environ['FIXTURE_ROOT'])\n"
            "with (root/'calls.log').open('a') as out: out.write('dune '+ ' '.join(sys.argv[1:])+'\\n')\n"
            "with (root/'dune.lock').open('a') as lock:\n"
            " try: fcntl.flock(lock, fcntl.LOCK_EX|fcntl.LOCK_NB)\n"
            " except BlockingIOError: raise SystemExit('another Dune writer')\n"
            " if sys.argv[1:] == ['fmt']: raise SystemExit('formatter is not a build dependency')\n"
            " if os.environ.get('FAIL_DUNE'): raise SystemExit(23)\n"
        )
        dune.chmod(0o755)
        bootstrap = self.source / "bootstrap.sh"
        bootstrap.write_text('#!/bin/sh\nset -eu\nmake -C "$(dirname "$0")/charon" -j4\n')
        bootstrap.chmod(0o755)
        self.env = dict(os.environ, PATH=str(bin_dir)+os.pathsep+os.environ['PATH'],
                        BUILD_LOG=str(self.log), FIXTURE_ROOT=str(self.root),
                        MAKE_FLAGS_RECEIPT=str(self.root / "make-flags"))

    def run_build(self, command):
        return subprocess.run(command, env=self.env, text=True, capture_output=True, timeout=20)

    def assert_required_outputs(self):
        calls = self.log.read_text().splitlines()
        self.assertEqual(calls.count('rust-release'), 1)
        self.assertEqual(calls.count('dune build'), 1)
        self.assertEqual(calls.count('dune build @doc'), 1)
        self.assertNotIn('dune fmt', calls, 'formatting must not race the required Dune writer')
        self.assertIn('let supported_charon_version = "0.1.0"',
                      (self.charon/'charon-ml/src/CharonVersion.ml').read_text())
        self.assertEqual((self.charon/'bin/charon').read_text(), 'charon')
        self.assertEqual((self.charon/'bin/charon-driver').read_text(), 'driver')
        self.assertIn('-j4', (self.root/'make-flags').read_text())

    def test_explicit_required_targets_are_a_positive_control(self):
        result = self.run_build(['make', '-C', str(self.charon), '-j4',
                                 'build-charon-rust', 'build-dev-charon-ml'])
        self.assertEqual(result.returncode, 0, result.stdout+result.stderr)
        self.assert_required_outputs()

    def test_bootstrap_uses_required_targets_without_formatting(self):
        result = self.run_build([sys.executable, str(SCRIPT), str(self.source)])
        self.assertEqual(result.returncode, 0, result.stdout+result.stderr)
        self.assert_required_outputs()


    def test_explicit_charon_target_is_not_rewritten(self):
        (self.source/'bootstrap.sh').write_text(
            '#!/bin/sh\nset -eu\nmake -C "$(dirname "$0")/charon" -j4 build-dev-charon-ml\n')
        result = self.run_build([sys.executable, str(SCRIPT), str(self.source)])
        self.assertEqual(result.returncode, 0, result.stdout+result.stderr)
        calls = self.log.read_text().splitlines()
        self.assertNotIn('rust-release', calls)
        self.assertEqual(calls, ['dune build', 'dune build @doc'])

    def test_unrelated_default_make_is_not_rewritten(self):
        other = self.source/'other'
        other.mkdir()
        (other/'Makefile').write_text('all:\n\t@printf unrelated > receipt\n')
        (self.source/'bootstrap.sh').write_text(
            '#!/bin/sh\nset -eu\nmake -C "$(dirname "$0")/other" -j4\n')
        result = self.run_build([sys.executable, str(SCRIPT), str(self.source)])
        self.assertEqual(result.returncode, 0, result.stdout+result.stderr)
        self.assertEqual((other/'receipt').read_text(), 'unrelated')
        self.assertFalse(self.log.exists())

    def test_failed_required_build_is_not_retried_or_reported_successful(self):
        self.env['FAIL_DUNE'] = '1'
        result = self.run_build([sys.executable, str(SCRIPT), str(self.source)])
        self.assertNotEqual(result.returncode, 0)
        calls = self.log.read_text().splitlines()
        self.assertEqual(calls.count('dune build'), 1)
        self.assertNotIn('dune build @doc', calls)
        self.assertNotIn('dune fmt', calls)


if __name__ == '__main__':
    unittest.main()
