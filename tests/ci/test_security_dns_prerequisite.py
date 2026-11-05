"""Execute the pre-hardening workflow commands against disposable runner state."""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github/workflows/security.yml"
CONTROL = """#!/usr/bin/env python3
import json, os, pathlib, sys
root = pathlib.Path(os.environ['FERROSA_CI_DNS_ROOT'])
state_file = root / 'services.json'
state = json.loads(state_file.read_text())
args = sys.argv[1:]
name = pathlib.Path(sys.argv[0]).name
with (root / 'events.jsonl').open('a') as out:
    out.write(json.dumps([name, *args]) + '\\n')
if name == 'resolvectl':
    assert args == ['flush-caches'], args
elif args[:2] == ['is-active', '--quiet']:
    sys.exit(0 if args[2] in state else 3)
elif args == ['restart', 'systemd-resolved']:
    assert not (root / 'etc/systemd/resolved.conf.d/90-egress-proxy.conf').exists()
    assert not (root / 'run/systemd/resolved.conf.d/90-egress-proxy.conf').exists()
    if os.environ.get('FAIL_DNS_RESTART') == '1':
        sys.exit(23)
elif len(args) == 2 and args[0] == 'stop':
    assert args[1] in ('egress-dns-resolver.service', 'egress-outbound-proxy.service')
    assert (root / 'etc/resolv.conf').resolve() == root / 'run/systemd/resolve/stub-resolv.conf'
    state.remove(args[1])
    state_file.write_text(json.dumps(state))
else:
    raise AssertionError(args)
"""


class SecurityDnsPrerequisite(unittest.TestCase):
    def setUp(self):
        self.workflow = yaml.safe_load(WORKFLOW.read_text())
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        for prefix in ("etc", "run"):
            (self.root / prefix / "systemd/resolved.conf.d").mkdir(parents=True)
        stub = self.root / "run/systemd/resolve/stub-resolv.conf"
        stub.parent.mkdir(parents=True)
        stub.write_text("nameserver 127.0.0.53\n")
        self.resolv = self.root / "etc/resolv.conf"
        self.resolv.write_text("nameserver 127.0.0.100\n")
        self.other = self.root / "etc/systemd/resolved.conf.d/99-unrelated.conf"
        self.other.write_text("unrelated runner configuration\n")
        (self.root / "services.json").write_text("[]")
        (self.bin / "sudo").write_text('#!/bin/sh\nexec "$@"\n')
        (self.bin / "sudo").chmod(0o755)
        for name in ("systemctl", "resolvectl"):
            (self.bin / name).write_text(CONTROL)
            (self.bin / name).chmod(0o755)

    def drop_in(self, prefix):
        path = self.root / prefix / "systemd/resolved.conf.d/90-egress-proxy.conf"
        path.write_text("[Resolve]\nDNS=\nDomains=~.\n")
        return path

    def run_before_hardening(self, *, hosted=True, restart_failure=False):
        steps = self.workflow["jobs"]["go-advisories"]["steps"]
        scripts = []
        for step in steps:
            if step.get("uses", "").startswith("step-security/harden-runner@"):
                break
            if "run" in step:
                scripts.append(step["run"])
        env = dict(os.environ, PATH=str(self.bin) + os.pathsep + os.environ["PATH"])
        env.update(
            RUNNER_ENVIRONMENT="github-hosted" if hosted else "self-hosted",
            RUNNER_OS="Linux",
            RUNNER_TEMP=str(self.root),
            FERROSA_CI_DNS_ROOT=str(self.root),
            FAIL_DNS_RESTART="1" if restart_failure else "0",
        )
        return subprocess.run(
            ["bash", "-euo", "pipefail", "-c", "\n".join(scripts)],
            env=env, text=True, capture_output=True, check=False,
        )

    def events(self):
        path = self.root / "events.jsonl"
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def assert_restored(self, result):
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(self.resolv.is_symlink())
        self.assertEqual(self.resolv.resolve(), self.root / "run/systemd/resolve/stub-resolv.conf")
        self.assertEqual(self.other.read_text(), "unrelated runner configuration\n")

    def test_etc_experiment_is_removed_before_agent_dns_is_configured(self):
        drop_in = self.drop_in("etc")
        services = ["egress-dns-resolver.service", "egress-outbound-proxy.service"]
        (self.root / "services.json").write_text(json.dumps(services))
        self.assert_restored(self.run_before_hardening())
        self.assertFalse(drop_in.exists())
        self.assertEqual(json.loads((self.root / "services.json").read_text()), [])
        mutations = [e for e in self.events() if e[1] != "is-active"]
        self.assertEqual(mutations, [
            ["systemctl", "restart", "systemd-resolved"],
            ["systemctl", "stop", "egress-dns-resolver.service"],
            ["systemctl", "stop", "egress-outbound-proxy.service"],
            ["resolvectl", "flush-caches"],
        ])
        backups = list(self.root.glob("ferrosa-dns.*/etc-90-egress-proxy.conf"))
        self.assertEqual(len(backups), 1)
        self.assertEqual(backups[0].read_text(), "[Resolve]\nDNS=\nDomains=~.\n")

    def test_run_drop_in_is_also_repaired(self):
        drop_in = self.drop_in("run")
        self.assert_restored(self.run_before_hardening())
        self.assertFalse(drop_in.exists())

    def test_resolver_address_alone_is_sufficient_evidence(self):
        self.assert_restored(self.run_before_hardening())

    def test_unrelated_standard_dns_is_untouched(self):
        self.resolv.write_text("nameserver 1.1.1.1\n")
        result = self.run_before_hardening()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.resolv.read_text(), "nameserver 1.1.1.1\n")
        self.assertFalse(self.resolv.is_symlink())
        self.assertFalse(any(e[1] != "is-active" for e in self.events()))

    def test_self_hosted_runner_state_is_untouched(self):
        drop_in = self.drop_in("etc")
        result = self.run_before_hardening(hosted=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(drop_in.exists())
        self.assertEqual(self.resolv.read_text(), "nameserver 127.0.0.100\n")
        self.assertEqual(self.events(), [])

    def test_restart_failure_refuses_to_continue_or_stop_services(self):
        self.drop_in("etc")
        result = self.run_before_hardening(restart_failure=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(any(e[1] == "stop" for e in self.events()))

    def test_missing_stub_refuses_before_service_changes(self):
        self.drop_in("etc")
        (self.root / "run/systemd/resolve/stub-resolv.conf").unlink()
        result = self.run_before_hardening()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.events(), [])

    def test_every_hardened_security_job_uses_same_pre_checkout_prerequisite(self):
        scripts = []
        for name, job in self.workflow["jobs"].items():
            steps = job.get("steps", [])
            hardened = [i for i, s in enumerate(steps) if s.get("uses", "").startswith("step-security/harden-runner@")]
            if not hardened:
                continue
            with self.subTest(job=name):
                index = hardened[0]
                self.assertGreater(index, 0)
                scripts.append(steps[index - 1]["run"])
                self.assertEqual(steps[index]["with"]["egress-policy"], "block")
                self.assertFalse(any("checkout@" in s.get("uses", "") for s in steps[:index]))
        self.assertTrue(scripts)
        self.assertEqual(len(set(scripts)), 1)
        go = self.workflow["jobs"]["go-advisories"]["steps"]
        harden = next(s for s in go if s.get("uses", "").startswith("step-security/harden-runner@"))
        self.assertEqual(set(harden["with"]["allowed-endpoints"].split()), {
            "github.com:443", "api.github.com:443", "objects.githubusercontent.com:443",
            "release-assets.githubusercontent.com:443", "api.osv.dev:443",
        })


if __name__ == "__main__":
    unittest.main()
