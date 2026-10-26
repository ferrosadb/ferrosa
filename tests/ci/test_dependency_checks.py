"""Exercise the dependency gate without network or Rust compilation."""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/check-dependencies.py"


class DependencyChecksTest(unittest.TestCase):
    def run_gate(self, failure="", version="0.20.2", extra_lock=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            subprocess.run(["git", "init", "-q", str(root)], check=True)
            locks = ["Cargo.lock", "fuzz/Cargo.lock"]
            if extra_lock:
                locks.append("new-workspace/Cargo.lock")
            for lock in locks:
                path = root / lock
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("# fixture lock\n")
                path.with_name("Cargo.toml").write_text("# fixture manifest\n")
            (root / "deny.toml").write_text("[advisories]\n")
            subprocess.run(["git", "-C", str(root), "add", "."], check=True)
            calls = root / "calls.jsonl"
            binary = root / "bin/cargo"
            binary.parent.mkdir()
            binary.write_text('''#!/usr/bin/env python3
import json, os, sys
with open(os.environ["GATE_CALLS"], "a") as stream:
    stream.write(json.dumps(sys.argv[1:]) + "\\n")
assert sys.argv[1] == "deny", "dependency checks must not compile or update locks"
if sys.argv[2:] == ["--version"]:
    print("cargo-deny " + os.environ["GATE_VERSION"])
elif os.environ["GATE_FAILURE"] in ("fetch", "check"):
    if os.environ["GATE_FAILURE"] in sys.argv:
        sys.exit(23)
''')
            binary.chmod(0o755)
            env = dict(os.environ, PATH=f"{binary.parent}:{os.environ['PATH']}",
                       GATE_CALLS=str(calls), GATE_VERSION=version,
                       GATE_FAILURE=failure)
            proc = subprocess.run(["python3", str(SCRIPT)], cwd=root, env=env,
                                  capture_output=True, text=True, timeout=10)
            events = [json.loads(line) for line in calls.read_text().splitlines()] if calls.exists() else []
            return proc, events, locks

    def test_checks_every_tracked_lock_with_fresh_data_and_ci_policy(self):
        proc, calls, locks = self.run_gate(extra_lock=True)
        self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)
        self.assertEqual(["deny", "--version"], calls.pop(0))
        self.assertEqual(2 * len(locks), len(calls))
        for lock, refresh, check in zip(sorted(locks), calls[::2], calls[1::2]):
            for command in (refresh, check):
                self.assertIn("--locked", command)
                self.assertIn("--all-features", command)
                self.assertEqual(str(Path(lock).with_name("Cargo.toml")),
                                 command[command.index("--manifest-path") + 1])
                self.assertTrue(command[command.index("--config") + 1].endswith("/deny.toml"))
            self.assertEqual(["fetch", "db", "index"], refresh[-3:])
            self.assertEqual(["check", "advisories", "bans", "licenses", "sources",
                              "--hide-inclusion-graph"], check[-6:])

    def test_refresh_failure_stops_before_any_scan(self):
        proc, calls, _ = self.run_gate(failure="fetch")
        self.assertEqual(23, proc.returncode, proc.stdout + proc.stderr)
        self.assertFalse(any("check" in call for call in calls))

    def test_advisory_failure_stops_before_the_next_workspace(self):
        proc, calls, _ = self.run_gate(failure="check")
        self.assertEqual(23, proc.returncode, proc.stdout + proc.stderr)
        self.assertEqual(1, sum("check" in call for call in calls))
        self.assertEqual(1, sum("fetch" in call for call in calls))

    def test_wrong_tool_version_fails_with_install_instructions(self):
        proc, calls, _ = self.run_gate(version="0.19.0")
        self.assertNotEqual(0, proc.returncode)
        self.assertIn("cargo install", proc.stderr)
        self.assertEqual([["deny", "--version"]], calls)

    def test_ci_and_local_hook_use_the_shared_script_before_builds(self):
        workflow = (ROOT / ".github/workflows/security.yml").read_text()
        hooks = (ROOT / ".pre-commit-config.yaml").read_text()
        self.assertIn("python3 scripts/check-dependencies.py", workflow)
        self.assertIn("--tool-version", workflow)
        self.assertNotIn("run: cargo deny", workflow)
        self.assertIn("fail_fast: true", hooks)
        start = hooks.index("- id: cargo-deny-prepush")
        end = hooks.index("- id: cargo-test-workspace")
        self.assertLess(start, end)
        gate = hooks[start:end]
        self.assertIn("entry: python3 scripts/check-dependencies.py", gate)
        self.assertIn("always_run: true", gate)
        self.assertIn("stages: [pre-push]", gate)


if __name__ == "__main__":
    unittest.main()
