"""Failure diagnostics retain current bounded compiler evidence, never secrets."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/lean_gap_diagnostics.py"


class FailureDiagnostics(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.reports = self.root / 'reports'
        self.reports.mkdir()
        self.output = self.root / 'artifact/diagnostics.json'
        self.since = self.root / 'started'
        self.since.write_text('2026-11-04T01:00:00Z\n')

    def record(self, name, **fields):
        data = {'crate': name, 'stage': 'charon', 'generated_at': '2026-11-04T01:01:00Z',
                'error': 'no .llbc produced', 'log': 'error[E0412]: missing type'}
        data.update(fields)
        (self.reports / (name+'.json')).write_text(json.dumps(data))

    def capture(self, **env):
        result = subprocess.run([sys.executable, str(SCRIPT), str(self.reports),
                                 str(self.output), '--since-file', str(self.since)],
                                env={**os.environ, **env}, text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stdout+result.stderr)
        return json.loads(self.output.read_text())

    def test_current_failure_retained_with_timestamp_and_no_stale_baseline(self):
        self.record('ferrosa-ctl')
        self.record('stale', generated_at='2026-11-03T01:00:00Z')
        data = self.capture()
        self.assertTrue(data['measurementStarted'])
        self.assertEqual([r['crate'] for r in data['records']], ['ferrosa-ctl'])
        self.assertEqual(data['records'][0]['stage'], 'charon')
        self.assertEqual(data['records'][0]['generated_at'], '2026-11-04T01:01:00Z')
        self.assertIn('error[E0412]', data['records'][0]['log'])

    def test_secrets_and_unrelated_fields_are_not_published(self):
        secret = 'fixture-token-that-must-never-be-an-artifact'
        self.record('ferrosa-ctl', log=f'{secret} tiny https://user:password@example.invalid/x Bearer abcdefghijkl',
                    toolchain={'private': secret}, environment={'TOKEN': secret})
        data = self.capture(GITHUB_TOKEN=secret, OTHER_PASSWORD='tiny')
        text = self.output.read_text()
        self.assertNotIn(secret, text)
        self.assertNotIn('tiny', text)
        self.assertNotIn('user:password', text)
        self.assertNotIn('abcdefghijkl', text)
        self.assertNotIn('environment', text)
        self.assertNotIn('toolchain', text)
        self.assertIn('[REDACTED]', data['records'][0]['log'])

    def test_record_and_text_limits_report_omissions(self):
        for i in range(40):
            self.record(f'crate-{i:02}', log='x'*20000+'final compiler diagnostic')
        data = self.capture()
        self.assertEqual(len(data['records']), 32)
        self.assertEqual(data['omittedRecords'], 8)
        self.assertLessEqual(len(data['records'][0]['log']), 4096)
        self.assertTrue(data['records'][0]['log'].endswith('final compiler diagnostic'))
        self.assertLess(self.output.stat().st_size, 1048576)

    def test_failure_before_measurement_never_claims_checked_in_reports_are_current(self):
        self.record('ferrosa-ctl')
        self.since.unlink()
        data = self.capture()
        self.assertFalse(data['measurementStarted'])
        self.assertEqual(data['records'], [])

    def test_malformed_and_symlink_reports_are_not_read_as_current_diagnostics(self):
        self.record('invalid', generated_at=[])
        (self.reports/'symlink.json').symlink_to(self.since)
        data = self.capture()
        self.assertEqual(data['records'], [])
        self.assertEqual(data['unreadableReports'], ['invalid.json'])


if __name__ == '__main__':
    unittest.main()
