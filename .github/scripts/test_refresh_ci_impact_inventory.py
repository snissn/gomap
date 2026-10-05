#!/usr/bin/env python3
"""Maintenance snapshot contracts; run with uv run --with pyyaml python <file>."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

import ci_impact

HAS_YAML = importlib.util.find_spec('yaml') is not None
if HAS_YAML:
    import refresh_ci_impact_inventory

ROOT = Path(__file__).resolve().parents[2]


@unittest.skipUnless(HAS_YAML, 'maintenance tests require PyYAML (uv run --with pyyaml)')
class RefreshSnapshotContract(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.repo = Path(self.temp.name)
        shutil.copytree(ROOT / '.github', self.repo / '.github',
                        ignore=shutil.ignore_patterns('__pycache__'))
        self.write('source.go', 'package original\n')
        self.write('old.py', '# old source\n')
        self.git('init', '-q')
        self.git('config', 'user.email', 'fixture@example.invalid')
        self.git('config', 'user.name', 'Fixture')
        self.git('add', '.')
        self.git('commit', '-qm', 'initial fixture')
        self.manifest = self.repo / ci_impact.POLICY

    def git(self, *args):
        return ci_impact.git(self.repo, *args).decode().strip()

    def write(self, name, contents):
        path = self.repo / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents)

    def refresh(self):
        with contextlib.redirect_stdout(io.StringIO()):
            refresh_ci_impact_inventory.refresh(self.repo)
        return json.loads(self.manifest.read_text())

    def assertCommittedSnapshot(self, policy):
        # Only the generated manifest is staged here. Untracked/unstaged inputs
        # must survive untouched and must not enter the commit's source binding.
        self.git('add', ci_impact.POLICY)
        self.git('commit', '-qm', 'reviewed manifest')
        inventory, _ = ci_impact.tree_inventory(self.repo, self.git('rev-parse', 'HEAD'))
        self.assertEqual(policy['discovery_source_sha256'], ci_impact.discovery_digest(inventory))
        names = {Path(p).name for p in inventory
                 if Path(p).parent == Path('.github/workflows') and Path(p).suffix in ('.yml', '.yaml')}
        self.assertEqual(set(policy['workflows']), names)
        for name, workflow in policy['workflows'].items():
            blob = inventory['.github/workflows/' + name]
            self.assertEqual(workflow['git_blob'], blob)
            self.assertEqual(workflow['sha256'], ci_impact.digest(ci_impact.git(self.repo, 'cat-file', 'blob', blob)))

    def test_untracked_sources_do_not_enter_snapshot(self):
        self.write('scratch.go', 'package untracked\n')
        policy = self.refresh()
        self.assertCommittedSnapshot(policy)
        self.assertEqual((self.repo / 'scratch.go').read_text(), 'package untracked\n')
        self.assertIn('?? scratch.go', self.git('status', '--short'))

    def test_untracked_workflows_do_not_enter_snapshot(self):
        self.write('.github/workflows/scratch.yaml', 'untracked invalid YAML: [')
        policy = self.refresh()
        self.assertNotIn('scratch.yaml', set(policy['workflows']))
        self.assertCommittedSnapshot(policy)

    def test_partially_staged_source_uses_index_bytes(self):
        self.write('source.go', 'package intended\n')
        self.git('add', 'source.go')
        self.write('source.go', 'package unstaged\n')
        policy = self.refresh()
        self.assertCommittedSnapshot(policy)
        self.assertEqual((self.repo / 'source.go').read_text(), 'package unstaged\n')

    def test_partially_staged_source_and_workflow_use_index_bytes(self):
        workflow = '.github/workflows/treedb-tests.yml'
        staged = (self.repo / workflow).read_text().replace('run: go vet ./...', 'run: go vet -printf ./...')
        self.assertTrue(staged != (self.repo / workflow).read_text())
        self.write(workflow, staged)
        self.write('source.go', 'package intended\n')
        self.git('add', 'source.go', workflow)
        self.write('source.go', 'package unstaged\n')
        self.write(workflow, 'unstaged invalid YAML: [')
        policy = self.refresh()
        self.assertEqual(policy['workflows']['treedb-tests.yml']['sha256'], ci_impact.digest(staged.encode()))
        commands = policy['workflows']['treedb-tests.yml']['jobs']['vet']['commands']
        self.assertTrue(any(c['invocation'] == 'go vet -printf ./...' for c in commands))
        self.assertCommittedSnapshot(policy)
        self.assertEqual((self.repo / 'source.go').read_text(), 'package unstaged\n')
        self.assertEqual((self.repo / workflow).read_text(), 'unstaged invalid YAML: [')

    def test_staged_source_and_workflow_additions_and_deletions(self):
        old = 'hashdb-tests.yml'
        new = 'hashdb-renamed.yaml'
        self.write('.github/workflows/' + new, (self.repo / '.github/workflows' / old).read_text())
        self.write('added.go', 'package added\n')
        self.git('rm', '--cached', 'old.py', '.github/workflows/' + old)
        self.git('add', 'added.go', '.github/workflows/' + new)
        # Reviewed ownership/rules remain an explicit working-manifest input.
        policy = json.loads(self.manifest.read_text())
        for rule in policy['rules']:
            rule['consumers'] = [new + c[len(old):] if c.split('/')[0] == old else c for c in rule['consumers']]
        self.manifest.write_text(json.dumps(policy))
        policy = self.refresh()
        self.assertNotIn(old, set(policy['workflows']))
        self.assertIn(new, set(policy['workflows']))
        with self.assertRaisesRegex(ci_impact.ContractError, 'unreviewed-member-owner'):
            ci_impact.check_policy(policy)
        self.assertCommittedSnapshot(policy)
        self.assertTrue((self.repo / 'old.py').exists())
        self.assertTrue((self.repo / '.github/workflows' / old).exists())

    def test_unstaged_workflow_deletion_does_not_change_snapshot(self):
        (self.repo / '.github/workflows/treedb-tests.yml').unlink()
        policy = self.refresh()
        self.assertIn('treedb-tests.yml', policy['workflows'])
        self.assertCommittedSnapshot(policy)
        self.assertFalse((self.repo / '.github/workflows/treedb-tests.yml').exists())

    def test_unmerged_index_rejected_without_manifest_write(self):
        before = self.manifest.read_bytes()
        oid = self.git('rev-parse', 'HEAD:source.go')
        subprocess.run(['git', '-C', str(self.repo), 'update-index', '--index-info'],
                       input=f'0 {"0" * 40}\tsource.go\n100644 {oid} 1\tsource.go\n100644 {oid} 2\tsource.go\n'.encode(),
                       check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        with self.assertRaises(ci_impact.ContractError):
            self.refresh()
        self.assertEqual(self.manifest.read_bytes(), before)


if __name__ == '__main__':
    unittest.main()
