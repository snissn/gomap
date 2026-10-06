#!/usr/bin/env python3
"""Maintenance snapshot contracts; run with uv run --with pyyaml python <file>."""
import contextlib
import copy
import importlib.util
import io
import json
from pathlib import Path
import shutil
import subprocess
import sys
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
        self.write('go.mod', 'module fixture\n')
        self.write('go.sum', '')
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
            blob = inventory['.github/workflows/' + name]['git_blob']
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

    def test_partially_staged_harness_bindings_use_same_snapshot(self):
        path = '.github/ci/treedb_unix_weighted_shards.tsv'
        self.write(path, '# intended staged weights\n')
        self.git('add', path)
        expected = self.git('rev-parse', ':' + path)
        self.write(path, '# unrelated unstaged weights\n')
        policy = self.refresh()
        self.assertIsInstance(policy['harness_inputs'], dict)
        self.assertEqual(policy['harness_inputs'][path], {'git_blob': expected, 'mode': '100644'})
        self.assertCommittedSnapshot(policy)
        self.assertEqual(self.git('rev-parse', 'HEAD:' + path), expected)
        self.assertEqual((self.repo / path).read_text(), '# unrelated unstaged weights\n')

    def test_missing_staged_harness_rejects_before_manifest_write(self):
        before = self.manifest.read_bytes()
        self.git('rm', '--cached', '.github/ci/treedb_race_weighted_shards.tsv')
        with self.assertRaises(ci_impact.ContractError):
            self.refresh()
        self.assertEqual(self.manifest.read_bytes(), before)

    def test_staged_harness_chmod_ignores_worktree_permissions(self):
        path = '.github/ci/treedb_unix_weighted_shards.tsv'
        blob = self.git('rev-parse', ':' + path)
        self.git('update-index', '--chmod=+x', path)
        (self.repo / path).chmod(0o644)
        policy = self.refresh()
        self.assertEqual(policy['harness_inputs'][path], {'git_blob': blob, 'mode': '100755'})
        self.assertCommittedSnapshot(policy)
        self.git('update-index', '--chmod=-x', path)
        (self.repo / path).chmod(0o755)
        policy = self.refresh()
        self.assertEqual(policy['harness_inputs'][path], {'git_blob': blob, 'mode': '100644'})
        self.assertCommittedSnapshot(policy)

    def test_refresh_upgrades_blob_only_policy_and_restores_later_docs_qualification(self):
        path = '.github/ci/treedb_windows_caching_heavy_tests.txt'
        self.write('docs/retained/RESULTS.json', '{}\n')
        self.git('add', 'docs/retained/RESULTS.json')
        policy = json.loads(self.manifest.read_text())
        policy['schema_version'] = 1
        policy['harness_inputs'] = {p: self.git('rev-parse', ':' + p) for p in policy['harness_inputs']}
        self.manifest.write_text(json.dumps(policy))
        policy = self.refresh()
        self.assertEqual(policy['schema_version'], ci_impact.SCHEMA)
        self.assertIsInstance(policy['harness_inputs'][path], dict)
        self.assertCommittedSnapshot(policy)
        (self.repo / path).chmod(0o755)
        self.git('add', path)
        self.git('commit', '-qm', 'unreviewed executable mode change')

        def forecast():
            base = self.git('rev-parse', 'HEAD')
            self.write('docs/retained/RESULTS.json', base + '\n')
            self.git('add', 'docs/retained/RESULTS.json')
            self.git('commit', '-qm', 'docs candidate')
            head = self.git('rev-parse', 'HEAD')
            candidate = self.git('commit-tree', self.git('write-tree'), '-p', base, '-p', head, '-m', 'merge')
            event = dict(event_name='pull_request', base=base, head=head, candidate=candidate,
                         run_id='123', run_attempt='1', repository='snissn/gomap', workflow_ref='fixture',
                         pr_number=1, event_sha256='fixture')
            environment = dict(runner_image='fixture', python='fixture', platform='linux', gowork='off', runner_os='Linux')
            receipt = ci_impact.plan(self.repo, event, environment)
            self.git('checkout', '-q', '--detach', base)
            return receipt

        stale = forecast()
        self.assertFalse(stale['forecast_qualified'])
        self.assertIn('harness-contract-drift', stale['fallback'])
        policy = self.refresh()
        self.assertCommittedSnapshot(policy)
        fresh = forecast()
        self.assertTrue(fresh['forecast_qualified'], fresh['fallback'])

    def test_coordinated_matrix_metadata_and_members_cannot_pass_source_check(self):
        original = self.refresh()
        target = 'treedb-tests/test/windows-core-8'
        for field in ('matrix', 'runs_on', 'race'):
            with self.subTest(coordinated_field=field):
                policy = copy.deepcopy(original)
                contract = policy['workflows']['treedb-tests.yml']['jobs']['test']
                for member in policy['members']:
                    if member['workflow'] == 'treedb-tests.yml' and member['job'] == 'test':
                        if field == 'matrix' and member['id'] == target:
                            member['variant']['matrix']['package_shard_index'] = 0
                        elif field != 'matrix':
                            member['variant'][field] = 'ubuntu-latest' if field == 'runs_on' else True
                if field == 'matrix':
                    for variant in contract['matrix']['include']:
                        if variant['name'] == 'windows-core-8':
                            variant['package_shard_index'] = 0
                else:
                    contract[field] = 'ubuntu-latest' if field == 'runs_on' else True
                # Internal consistency alone must not corroborate source truth.
                ci_impact.check_policy(policy)
                self.manifest.write_text(json.dumps(policy))
                before = self.manifest.read_bytes()
                result = subprocess.run([sys.executable, str(self.repo / '.github/scripts/refresh_ci_impact_inventory.py'), '--check'],
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(b'workflow-source-contract-mismatch', result.stderr)
                self.assertEqual(self.manifest.read_bytes(), before)

    def test_impact_shadow_is_excluded_only_from_treedb_control_workflow(self):
        path = '.github/workflows/hashdb-tests.yml'
        source = (self.repo / path).read_text()
        source += '\n  impact-shadow:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo original execution\n'
        self.write(path, source)
        self.git('add', path)
        policy = self.refresh()
        self.assertIn('hashdb-tests/impact-shadow/single', {m['id'] for m in policy['members']})
        self.assertNotIn('treedb-tests/impact-shadow/single', {m['id'] for m in policy['members']})
        self.assertCommittedSnapshot(policy)


if __name__ == '__main__':
    unittest.main()
