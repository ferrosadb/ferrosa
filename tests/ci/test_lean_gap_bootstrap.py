"""Exercise the pinned build graph and fresh-runner package prerequisites.

Correctness: required builds and local opam provisioning complete or fail once.
Last revised: 2026-11-05.
Last changed: Execute pinned opam discovery with an isolated dependency PATH.
"""
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


class OpamBootstrapPrerequisite(unittest.TestCase):
    """Run the real package step and pinned discovery fragment, without apt/network builds."""

    def package_and_probe(self, *, existing=False, fail_install=False):
        from tests.ci.test_lean_gap_ratchet_workflow import load_yaml

        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        tools = root / "bin"
        tools.mkdir()
        fixture = json.loads(
            (ROOT / "tests/ci/fixtures/aeneas-80b690a9/bootstrap.json").read_text()
        )
        source = fixture["content"].encode()
        self.assertEqual(
            hashlib.sha1(f"blob {len(source)}\0".encode() + source).hexdigest(),
            fixture["gitBlob"],
        )
        start = fixture["content"].index('say "opam"')
        end = fixture["content"].index(
            "# ---------------------------------------------------------------- opam root", start
        )
        discovery = fixture["content"][start:end]
        workflow = load_yaml(ROOT / ".github/workflows/lean-specs.yml")
        package = next(
            step["run"] for step in workflow["jobs"]["ratchet"]["steps"]
            if step.get("name") == "System packages the toolchain links against"
        )
        opam_program = f"#!{sys.executable}\nprint('fixture opam')\n"
        programs = {
            "sudo": f'''#!{sys.executable}
import json, os, pathlib, sys
root = pathlib.Path(os.environ["FIXTURE_ROOT"])
args = sys.argv[1:]
with (root / "packages.log").open("a") as out:
    out.write(json.dumps(args) + "\\n")
if args[:2] == ["apt-get", "update"]:
    raise SystemExit(0)
if args[:2] != ["apt-get", "install"]:
    raise SystemExit(97)
if "opam" in args and os.environ["FAIL_OPAM_INSTALL"] == "1":
    raise SystemExit(17)
if "capnproto" in args:
    (root / "schema-installed").touch()
if "opam" in args:
    opam = root / "bin/opam"
    opam.write_text({opam_program!r})
    opam.chmod(0o755)
''',
            "capnp": f'''#!{sys.executable}
import os, pathlib
assert (pathlib.Path(os.environ["FIXTURE_ROOT"]) / "schema-installed").exists()
print("fixture capnp")
''',
            "curl": f'''#!{sys.executable}
import os, pathlib, sys
(pathlib.Path(os.environ["FIXTURE_ROOT"]) / "curl.log").write_text(" ".join(sys.argv[1:]))
print("curl: (22) The requested URL returned error: 403", file=sys.stderr)
raise SystemExit(22)
''',
            "uname": f'''#!{sys.executable}
import sys
print("Linux" if sys.argv[1:] == ["-s"] else "x86_64")
''',
        }
        for name, content in programs.items():
            program = tools / name
            program.write_text(content)
            program.chmod(0o755)
        for name, target in [("python3", sys.executable), ("mkdir", "/bin/mkdir"),
                             ("chmod", "/bin/chmod")]:
            (tools / name).symlink_to(target)
        if existing:
            (tools / "opam").write_text(opam_program)
            (tools / "opam").chmod(0o755)
        env = {
            "PATH": str(tools), "HOME": str(root / "home"),
            "PREFIX": str(root / "prefix"), "FIXTURE_ROOT": str(root),
            "FAIL_OPAM_INSTALL": str(int(fail_install)),
        }
        script = ("set -euo pipefail\nDRY_RUN=0\nsay() { :; }\nnote() { :; }\n"
                  + package + "\n" + discovery + "\nopam --version\n")
        result = subprocess.run(["/bin/bash", "-c", script], cwd=root, env=env,
                                capture_output=True, text=True)
        return root, result

    def test_fresh_runner_provides_opam_without_release_api_discovery(self):
        root, result = self.package_and_probe()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertFalse((root / "curl.log").exists())
        self.assertTrue((root / "bin/opam").is_file())

    def test_package_failure_stops_before_release_discovery(self):
        root, result = self.package_and_probe(fail_install=True)
        self.assertEqual(result.returncode, 17, result.stdout + result.stderr)
        self.assertFalse((root / "curl.log").exists())
        self.assertEqual(len((root / "packages.log").read_text().splitlines()), 2)

    def test_existing_opam_is_a_positive_control_for_the_pinned_fragment(self):
        root, result = self.package_and_probe(existing=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertFalse((root / "curl.log").exists())


if __name__ == '__main__':
    unittest.main()
