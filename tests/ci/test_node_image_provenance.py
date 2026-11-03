"""The prebuilt node image names the source of its actual glibc artifact.

Execute the workflow shell with real divergent head/merge Git commits and an
isolated Docker boundary. No compiler, registry credentials or daemon needed.
"""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest

from tests.ci.test_ci_cache_workflow import jobs, steps

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / '.github/workflows/ci.yml'


def script(job, name):
    step = next(s for s in steps(jobs(WORKFLOW.read_text())[job]) if s.startswith('name: ' + name + '\n'))
    body = step.split('        run: |\n', 1)[1]
    return '\n'.join(line[10:] for line in body.splitlines() if line.startswith('          ')) + '\n'


class NodeImageProvenance(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.env = {k: v for k, v in os.environ.items() if not k.startswith('GIT_')}
        self.env.update(GIT_CONFIG_NOSYSTEM='1', GIT_CONFIG_GLOBAL=os.devnull,
                        GIT_AUTHOR_NAME='Fixture', GIT_AUTHOR_EMAIL='fixture@example.invalid',
                        GIT_COMMITTER_NAME='Fixture', GIT_COMMITTER_EMAIL='fixture@example.invalid')
        self.git('init', '-q')
        (self.root / 'source').write_text('base')
        self.git('add', 'source'); self.git('commit', '-qm', 'base')
        base = self.git('rev-parse', 'HEAD')
        self.git('checkout', '-qb', 'feature')
        (self.root / 'feature').write_text('head')
        self.git('add', 'feature'); self.git('commit', '-qm', 'head')
        self.head = self.git('rev-parse', 'HEAD')
        self.git('checkout', '-qb', 'merge', base)
        (self.root / 'other').write_text('main')
        self.git('add', 'other'); self.git('commit', '-qm', 'main')
        self.git('merge', '-qm', 'synthetic merge', 'feature')
        self.merge = self.git('rev-parse', 'HEAD')
        self.assertNotEqual(self.head, self.merge)
        node = self.root / 'node-bin'; node.mkdir()
        for name in ['ferrosa', 'ferrosa-ctl']:
            (node / name).write_text('fixture artifact')
        (node / 'source-revision.txt').write_text(self.head + '\n')
        tools = self.root / 'tools'; tools.mkdir()
        docker = tools / 'docker'
        docker.write_text('#!/bin/bash\nprintf "%s\\n" "$*" >> "$DOCKER_LOG"\nif [ "$1" = build ]; then\n while [ "$#" -gt 0 ]; do\n  if [ "$1" = --iidfile ]; then shift; printf "sha256:fixture\\n" > "$1"; fi\n  shift\n done\nfi\n')
        docker.chmod(0o755)
        self.env.update(PATH=str(tools) + os.pathsep + self.env['PATH'],
                        DOCKER_LOG=str(self.root / 'docker.log'), SOURCE_REVISION=self.head,
                        GITHUB_OUTPUT=str(self.root / 'output'))

    def git(self, *args):
        return subprocess.check_output(['git', *args], cwd=self.root, env=self.env, text=True).strip()

    def run_step(self, job, name):
        body = script(job, name)
        replacements = {'github.sha': self.merge, 'github.event.pull_request.head.sha': self.head,
                        'needs.glibc.outputs.revision': self.head,
                        'github.repository_owner': 'fixture', 'github.actor': 'fixture',
                        'secrets.GITHUB_TOKEN': 'fixture-not-a-token'}
        for key, value in replacements.items():
            body = body.replace('${{ ' + key + ' }}', value)
        return subprocess.run(['bash', '-eu', '-o', 'pipefail', '-c', body], cwd=self.root,
                              env=self.env, capture_output=True, text=True)

    def test_mismatched_checkout_refuses_before_any_docker_operation(self):
        result = self.run_step('build-node-image', 'Package into a thin runtime image')
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertFalse((self.root / 'docker.log').exists(), 'mismatch reached Docker')

    def test_matching_head_artifact_publishes_only_actual_source_revision(self):
        self.git('checkout', '-q', self.head)
        result = self.run_step('build-node-image', 'Package into a thin runtime image')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        result = self.run_step('build-node-image', 'Push image to GHCR (cross-workflow reuse)')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        log = (self.root / 'docker.log').read_text()
        pushes = [line for line in log.splitlines() if line.startswith('push ')]
        self.assertEqual(pushes, ['push ghcr.io/fixture/ferrosa-test-node:' + self.head])
        self.assertIn('org.opencontainers.image.revision=' + self.head, log)

    def test_mismatched_artifact_refuses_before_any_docker_operation(self):
        self.git('checkout', '-q', self.head)
        (self.root / 'node-bin/source-revision.txt').write_text(self.merge + '\n')
        result = self.run_step('build-node-image', 'Package into a thin runtime image')
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((self.root / 'docker.log').exists())

    def test_publication_rechecks_artifact_before_login_or_tag(self):
        self.git('checkout', '-q', self.head)
        (self.root / 'node-bin/source-revision.txt').write_text(self.merge + '\n')
        result = self.run_step('build-node-image', 'Push image to GHCR (cross-workflow reuse)')
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((self.root / 'docker.log').exists())

    def test_recorded_artifact_revision_is_actual_checked_out_head(self):
        self.git('checkout', '-q', self.head)
        artifact = self.root / 'target/x86_64-unknown-linux-gnu/release'
        artifact.mkdir(parents=True)
        self.env['EXPECTED_REVISION'] = self.head
        result = self.run_step('glibc', 'Record the glibc artifact source')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual((artifact / 'source-revision.txt').read_text(), self.head + '\n')
        self.assertEqual((self.root / 'output').read_text(), 'revision=' + self.head + '\n')
        self.git('checkout', '-q', self.merge)
        result = self.run_step('glibc', 'Record the glibc artifact source')
        self.assertNotEqual(result.returncode, 0)

    def test_checkout_output_sidecar_and_format_guard_are_wired(self):
        workflow = WORKFLOW.read_text(); all_jobs = jobs(workflow)
        self.assertIn('ref: ${{ github.event.pull_request.head.sha || github.sha }}', all_jobs['glibc'])
        self.assertIn('revision: ${{ steps.revision.outputs.revision }}', all_jobs['glibc'])
        self.assertIn('source-revision.txt', all_jobs['glibc'])
        self.assertIn('ref: ${{ needs.glibc.outputs.revision }}', all_jobs['build-node-image'])
        self.assertIn('needs: glibc', all_jobs['build-node-image'])
        self.assertIn('tests.ci.test_node_image_provenance', all_jobs['fmt'])


if __name__ == '__main__':
    unittest.main()
