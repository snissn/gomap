#!/usr/bin/env python3
"""Risk contracts for advisory CI impact planning; real Git identities and diffs."""
import copy
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import sys
import unittest
from unittest.mock import patch

import ci_impact

ROOT = Path(__file__).resolve().parents[2]
ENV = {"runner_image": "fixture", "python": "fixture", "platform": "linux", "gowork": "off", "runner_os": "Linux"}


class ImpactContract(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.repo = Path(self.temp.name)
        shutil.copytree(ROOT / ".github", self.repo / ".github", ignore=shutil.ignore_patterns('__pycache__'))
        for path in ('go.mod', 'go.sum', 'docs/retained/RESULTS.json', 'TreeDB/witness_test.go',
                     'HashDB/example.go', 'scripts/generate.sh', 'fixtures/data.txt'):
            self.write(path, '{}\n')
        # Inventory bindings may include scripts outside .github. Preserve those
        # real bytes in this Git-only fixture instead of omitting declared inputs.
        for path in json.loads((ROOT / ci_impact.POLICY).read_text())['harness_inputs']:
            if not (self.repo / path).exists():
                self.write(path, (ROOT / path).read_text())
        self.git('init', '-q')
        self.git('config', 'user.email', 'fixture@example.invalid')
        self.git('config', 'user.name', 'Fixture')
        self.git('add', '.')
        tree = self.git('write-tree')
        files, _ = ci_impact.tree_inventory(self.repo, tree)
        manifest = self.repo / '.github/ci/ci_impact.json'
        policy = json.loads(manifest.read_text())
        policy['discovery_source_sha256'] = ci_impact.discovery_digest(files)
        if isinstance(policy['harness_inputs'], dict):
            policy['harness_inputs'] = {p: files[p] for p in policy['harness_inputs']}
        manifest.write_text(json.dumps(policy))
        self.base = self.commit()

    def git(self, *args):
        return subprocess.check_output(['git', '-C', str(self.repo), *args], stderr=subprocess.PIPE).decode().strip()

    def write(self, path, data):
        p = self.repo / path
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(data)

    def commit(self):
        self.git('add', '.')
        self.git('commit', '-qm', 'fixture')
        return self.git('rev-parse', 'HEAD')

    def event(self, paths=None):
        for path in paths or ['docs/retained/RESULTS.json']:
            self.write(path, 'changed\n')
        head = self.commit()
        tree = self.git('rev-parse', 'HEAD^{tree}')
        merge = self.git('commit-tree', tree, '-p', self.base, '-p', head, '-m', 'merge')
        self.git('checkout', '-q', '--detach', merge)
        return {"event_name": "pull_request", "base": self.base, "head": head,
                "candidate": merge, "run_id": "123", "run_attempt": "1",
                "repository": "snissn/gomap", "workflow_ref": "fixture", "pr_number": 1,
                "event_sha256": "fixture"}

    def plan(self, event):
        return ci_impact.plan(self.repo, event, ENV)

    def assertFull(self, receipt, code=None):
        self.assertFalse(receipt['omission_authority'])
        self.assertFalse(receipt['forecast_qualified'])
        self.assertTrue(receipt['fallback'])
        self.assertTrue(all(m['decision'] == 'would_run' for m in receipt['members']))
        if code:
            self.assertIn(code, receipt['fallback'])

    def test_bootstrap_and_missing_or_zero_identity_are_full(self):
        event = self.event()
        for key, value in [('base', ''), ('base', '0' * 40), ('base', '1' * 40),
                           ('head', self.base), ('candidate', self.base), ('event_name', 'push'),
                           ('repository', ''), ('pr_number', ''), ('workflow_ref', ''), ('event_sha256', '')]:
            with self.subTest(key=key, value=value):
                modified = dict(event, **{key: value})
                self.assertFull(self.plan(modified))
        # Accepted base, rather than a candidate policy, is the authority.
        self.git('checkout', '-q', self.base)
        self.git('rm', '.github/ci/ci_impact.json')
        bootstrap_base = self.commit()
        bootstrap = dict(event, base=bootstrap_base)
        self.assertFull(self.plan(bootstrap))

    def test_complete_docs_forecast_includes_dynamic_consumers(self):
        receipt = self.plan(self.event())
        self.assertTrue(receipt['forecast_qualified'], receipt['fallback'])
        self.assertFalse(receipt['omission_authority'])
        self.assertEqual(len([m for m in receipt['members'] if m['workflow'] == 'treedb-tests.yml']), 33)
        for m in receipt['members']:
            if (m['workflow'] == 'treedb-tests.yml' and m['job'] in ('test', 'race', 'required')) or m['workflow'] == 'root-tests.yml':
                self.assertEqual(m['decision'], 'would_run', m)
        self.assertTrue(any(m['decision'] == 'would_be_inapplicable' for m in receipt['members']))
        ci_impact.validate(self.repo, self.event_data(receipt), ENV, receipt)

    def event_data(self, receipt):
        return receipt['identity']['event']

    def review_inputs(self):
        self.git('add', '.')
        files, _ = ci_impact.tree_inventory(self.repo, self.git('write-tree'))
        policy = json.loads((self.repo / ci_impact.POLICY).read_text())
        policy['harness_inputs'] = {p: files[p] for p in policy['harness_inputs']}
        policy['discovery_source_sha256'] = ci_impact.discovery_digest(files)
        self.write(ci_impact.POLICY, json.dumps(policy))
        self.base = self.commit()

    def test_qualified_receipt_rejects_equal_value_different_json_types(self):
        receipt = self.plan(self.event())
        self.assertTrue(receipt['forecast_qualified'], receipt['fallback'])
        mutations = [(('schema_version',), float(ci_impact.SCHEMA)),
                     (('omission_authority',), 0), (('forecast_qualified',), 1),
                     (('identity', 'event', 'pr_number'), True),
                     (('inventory', 'base_files'), float(receipt['inventory']['base_files'])),
                     (('members', 0, 'variant', 'race'), int(receipt['members'][0]['variant']['race']))]
        for path, value in mutations:
            with self.subTest(path=path):
                forged = copy.deepcopy(receipt)
                target = forged
                for key in path[:-1]:
                    target = target[key]
                target[path[-1]] = value
                with self.assertRaises(ci_impact.ContractError):
                    ci_impact.validate(self.repo, self.event_data(receipt), ENV, forged)

    def test_source_and_extensionless_executable_mode_drift_survives_later_docs(self):
        initial = self.base
        for path in ('scripts/generate.sh', 'tools/extensionless-reader'):
            with self.subTest(path=path):
                self.git('checkout', '-q', initial)
                self.write(path, '# unchanged dynamic reader\n')
                (self.repo / path).chmod(0o755)
                self.review_inputs()
                before = self.git('rev-parse', 'HEAD:' + path)
                (self.repo / path).chmod(0o644)
                self.base = self.commit()
                self.assertEqual(self.git('rev-parse', 'HEAD:' + path), before)
                self.assertFull(self.plan(self.event()), 'consumer-discovery-drift')
                # A restored candidate cannot repair the stale accepted base.
                self.git('checkout', '-q', self.base)
                (self.repo / path).chmod(0o755)
                self.assertFull(self.plan(self.event()), 'consumer-discovery-drift')

    def test_noncode_harness_mode_binding_and_legacy_blob_policy_fail_closed(self):
        initial = self.base
        for path in ('.github/ci/treedb_unix_weighted_shards.tsv',
                     '.github/ci/treedb_windows_caching_heavy_tests.txt'):
            with self.subTest(path=path):
                self.git('checkout', '-q', initial)
                before = self.git('rev-parse', 'HEAD:' + path)
                (self.repo / path).chmod(0o755)
                self.base = self.commit()
                self.assertEqual(self.git('rev-parse', 'HEAD:' + path), before)
                self.assertFull(self.plan(self.event()), 'harness-contract-drift')
                self.git('checkout', '-q', self.base)
                (self.repo / path).chmod(0o644)
                self.assertFull(self.plan(self.event()), 'harness-contract-drift')
        self.git('checkout', '-q', initial)
        policy = json.loads((self.repo / ci_impact.POLICY).read_text())
        policy['harness_inputs'] = {p: self.git('rev-parse', 'HEAD:' + p) for p in policy['harness_inputs']}
        with self.assertRaises(ci_impact.ContractError):
            ci_impact.check_policy(policy)
        self.write(ci_impact.POLICY, json.dumps(policy))
        self.base = self.commit()
        self.assertFull(self.plan(self.event()), 'bootstrap-no-accepted-policy')

    def test_global_test_scripts_new_globs_platform_and_nested_inputs_are_full(self):
        paths = ['go.mod', 'go.sum', '.github/ci/ci_impact.json', '.github/workflows/race.yml',
                 'TreeDB/witness_test.go', 'TreeDB/thing_windows.go', 'TreeDB/thing_linux.go',
                 'TreeDB/thing.s', 'TreeDB/testdata/new.json', 'scripts/generate.sh',
                 'fixtures/data.txt', 'benchmarks/new/go.mod', 'docs/code.go', 'docs/generate.py']
        for path in paths:
            with self.subTest(path=path):
                self.git('checkout', '-q', self.base)
                event = self.event([path])
                self.assertFull(self.plan(event))

    def test_rename_delete_union_old_and_new_ownership_and_new_names_full(self):
        self.git('mv', 'docs/retained/RESULTS.json', 'docs/retained/new\nname.json')
        event = self.event(['HashDB/example.go'])
        receipt = self.plan(event)
        self.assertFull(receipt)
        changes = receipt['changes']
        self.assertTrue(any(c.get('old_path') == 'docs/retained/RESULTS.json' for c in changes))
        self.assertTrue(any(c.get('path') == 'docs/retained/new\nname.json' for c in changes))
        self.git('checkout', '-q', self.base)
        self.git('rm', 'docs/retained/RESULTS.json')
        receipt = self.plan(self.event(['HashDB/example.go']))
        self.assertTrue(any(c['status'] == 'D' and c['path'] == 'docs/retained/RESULTS.json' for c in receipt['changes']))

    def test_malformed_or_partial_nul_diff_is_rejected(self):
        for raw in (b'M\0file', b'R100\0old\0', b'M\0\0', b'wat\0file\0', b'M\0../bad\0', b'M\0\xff\0'):
            with self.subTest(raw=raw), self.assertRaises(ci_impact.ContractError):
                ci_impact.parse_diff(raw)
        self.assertEqual(ci_impact.parse_diff(b'R100\0old\0new\nname\0'),
                         [{'status': 'R100', 'old_path': 'old', 'path': 'new\nname'}])

    def test_unreviewed_owners_and_unused_consumer_selectors_are_rejected(self):
        original = json.loads((self.repo / '.github/ci/ci_impact.json').read_text())
        for consumer in ('treedb-tests.yml/tezt', 'treedb-tests.yml/test/missing', 'missing.yml'):
            with self.subTest(consumer=consumer):
                policy = copy.deepcopy(original)
                policy['rules'][0]['consumers'] = [consumer]
                with self.assertRaises(ci_impact.ContractError):
                    ci_impact.check_policy(policy)
        policy = copy.deepcopy(original)
        policy['members'][0]['owner'] = 'UNREVIEWED: assign affected owner'
        with self.assertRaises(ci_impact.ContractError):
            ci_impact.check_policy(policy)
        policy = copy.deepcopy(original)
        policy['always_members'].append('treedb-tests/missing/single')
        with self.assertRaises(ci_impact.ContractError):
            ci_impact.check_policy(policy)

    def test_each_inventoried_job_requires_a_member_even_with_same_workflow_retained(self):
        policy = json.loads((self.repo / '.github/ci/ci_impact.json').read_text())
        policy['members'] = [m for m in policy['members']
                             if not (m['workflow'] == 'treedb-tests.yml' and m['job'] == 'vet')]
        self.assertTrue(any(m['workflow'] == 'treedb-tests.yml' for m in policy['members']))
        with self.assertRaises(ci_impact.ContractError):
            ci_impact.check_policy(policy)
        # A malformed accepted policy must not shrink the reported universe.
        self.write('.github/ci/ci_impact.json', json.dumps(policy))
        self.base = self.commit()
        receipt = self.plan(self.event())
        self.assertFull(receipt, 'bootstrap-no-accepted-policy')
        self.assertEqual(len(receipt['members']), 57)
        self.assertTrue(any(m['workflow'] == 'treedb-tests.yml' and m['job'] == 'vet'
                            for m in receipt['members']))
        policy['workflows']['treedb-tests.yml']['jobs'] = {}
        with self.assertRaises(ci_impact.ContractError):
            ci_impact.check_policy(policy)

    def test_every_matrix_member_and_full_typed_descriptor_are_required(self):
        original = json.loads((self.repo / ci_impact.POLICY).read_text())
        target = next(i for i, m in enumerate(original['members']) if m['id'] == 'treedb-tests/test/windows-core-8')
        mutations = [lambda p: p['members'].pop(target),
                     lambda p: p['members'][target].update(id='forged'),
                     lambda p: p['members'][target]['variant']['matrix'].update(package_shard_index=0),
                     lambda p: p['members'][target]['variant']['matrix'].update(package_shard_count=7),
                     lambda p: p['members'][target]['variant']['matrix'].update(package_shard_index=True),
                     lambda p: p['members'][target]['variant']['matrix'].update(os='ubuntu-latest'),
                     lambda p: p['members'][target]['variant'].update(runs_on='ubuntu-latest'),
                     lambda p: p['members'][target]['variant'].update(race=True),
                     lambda p: p['members'][target]['variant'].update(race=0)]
        for index, mutate in enumerate(mutations):
            with self.subTest(mutation=index):
                policy = copy.deepcopy(original)
                mutate(policy)
                with self.assertRaises(ci_impact.ContractError):
                    ci_impact.check_policy(policy)
        # Accepted-base corruption is a full reporting receipt, never 56 members.
        policy = copy.deepcopy(original)
        policy['members'].pop(target)
        self.write(ci_impact.POLICY, json.dumps(policy))
        self.base = self.commit()
        receipt = self.plan(self.event())
        self.assertFull(receipt, 'bootstrap-no-accepted-policy')
        self.assertEqual(len(receipt['members']), 57)

    def test_matrix_invalid_forms_and_label_collisions_fail_closed(self):
        original = json.loads((self.repo / ci_impact.POLICY).read_text())
        invalid = [{}, {'include': []}, {'include': [{}]}, {'include': [{'name': 'x'}, {'name': 'x'}]},
                   {'include': [{'name': 1}, {'name': '1'}]}, {'include': [{'name': 'x'}], 'os': ['linux']},
                   {'os': []}, {'os': ['linux'], 'exclude': []}, '${{ fromJSON(needs.plan.outputs.matrix) }}',
                   {'os': ['${{ inputs.os }}']}, {'os': ['linux', 'linux']}, {'os': 'linux'}]
        for matrix in invalid:
            with self.subTest(matrix=matrix):
                policy = copy.deepcopy(original)
                policy['workflows']['treedb-tests.yml']['jobs']['test']['matrix'] = matrix
                with self.assertRaises(ci_impact.ContractError):
                    ci_impact.check_policy(policy)

    def test_reviewed_harness_blobs_bind_both_trees_and_loaded_runtime(self):
        original_base = self.base
        for path in ('.github/ci/treedb_unix_weighted_shards.tsv',
                     '.github/ci/treedb_windows_caching_heavy_tests.txt'):
            with self.subTest(accepted_base_drift=path):
                self.git('checkout', '-q', original_base)
                self.write(path, 'accepted but unreviewed allocator input\n')
                self.base = self.commit()
                self.assertFull(self.plan(self.event()), 'harness-contract-drift')
        self.base = original_base
        for action in ('edit', 'delete'):
            with self.subTest(candidate=action):
                self.git('checkout', '-q', original_base)
                path = '.github/ci/treedb_race_weighted_shards.tsv'
                if action == 'edit':
                    self.write(path, 'candidate edit\n')
                else:
                    self.git('rm', path)
                self.assertFull(self.plan(self.event()), 'harness-contract-drift')
        self.git('checkout', '-q', original_base)
        runtime = '.github/scripts/ci_impact.py'
        self.write(runtime, (self.repo / runtime).read_text() + '\n# different reviewed runtime\n')
        self.git('add', runtime)
        files, _ = ci_impact.tree_inventory(self.repo, self.git('write-tree'))
        policy = json.loads((self.repo / ci_impact.POLICY).read_text())
        policy['harness_inputs'] = {p: files[p] for p in policy['harness_inputs']}
        policy['discovery_source_sha256'] = ci_impact.discovery_digest(files)
        self.write(ci_impact.POLICY, json.dumps(policy))
        self.base = self.commit()
        self.assertFull(self.plan(self.event()), 'planner-runtime-drift')

    def test_invalid_harness_map_and_missing_planner_binding_are_rejected(self):
        original = json.loads((self.repo / ci_impact.POLICY).read_text())
        for bindings in ([], {}, {'../escape': 'a' * 40}, {'.github/scripts/ci_impact.py': 'invalid'},
                         {'.github/scripts/ci_impact.py': True}, {'go.mod': 'a' * 40},
                         {ci_impact.PLANNER: {'git_blob': 'a' * 40}},
                         {ci_impact.PLANNER: {'git_blob': True, 'mode': '100644'}},
                         {ci_impact.PLANNER: {'git_blob': 'a' * 40, 'mode': 100644}},
                         {ci_impact.PLANNER: {'git_blob': 'a' * 40, 'mode': '120000'}},
                         {ci_impact.PLANNER: {'git_blob': 'a' * 40, 'mode': '100644', 'extra': True}}):
            with self.subTest(bindings=bindings):
                policy = copy.deepcopy(original)
                policy['harness_inputs'] = bindings
                with self.assertRaises(ci_impact.ContractError):
                    ci_impact.check_policy(policy)
        # Nonplanner bindings remain an explicit accepted-policy review choice.
        ci_impact.check_policy(dict(original, harness_inputs={ci_impact.PLANNER: original['harness_inputs'][ci_impact.PLANNER]}))
        self.write(ci_impact.POLICY, json.dumps(dict(original, harness_inputs={})))
        self.base = self.commit()
        self.assertFull(self.plan(self.event()), 'bootstrap-no-accepted-policy')

    def test_workflow_bindings_cover_base_even_when_candidate_restores_blob(self):
        workflow = '.github/workflows/hashdb-tests.yml'
        before = (self.repo / workflow).read_text()
        self.write(workflow, before + '\n# accepted source drift\n')
        self.base = self.commit()
        self.write(workflow, before)
        self.assertFull(self.plan(self.event()), 'workflow-contract-drift')

    def test_workspace_and_make_include_discovery_drift_stays_full_on_later_docs(self):
        base = self.base
        for path in ('build/go.work', 'build/go.work.sum', 'build/inputs.mk'):
            with self.subTest(path=path):
                self.git('checkout', '-q', base)
                self.write(path, 'new dynamic inputs\n')
                self.base = self.commit()
                self.assertFull(self.plan(self.event()), 'consumer-discovery-drift')
    def test_missing_duplicate_unknown_member_or_forged_binding_fails_validation(self):
        event = self.event()
        receipt = self.plan(event)
        mutations = [lambda r: r['members'].pop(),
                     lambda r: r['members'].append(copy.deepcopy(r['members'][0])),
                     lambda r: r['members'][0].update(id='unknown'),
                     lambda r: r['identity']['event'].update(candidate=self.base),
                     lambda r: r['identity'].update(accepted_policy_sha256='forged'),
                     lambda r: r.update(omission_authority=True),
                     lambda r: r['members'][0].update(decision='would_run' if r['members'][0]['decision'] == 'would_be_inapplicable' else 'would_be_inapplicable')]
        for mutate in mutations:
            with self.subTest(mutate=mutate):
                forged = copy.deepcopy(receipt)
                mutate(forged)
                with self.assertRaises(ci_impact.ContractError):
                    ci_impact.validate(self.repo, event, ENV, forged)
        with self.assertRaises(ci_impact.ContractError):
            ci_impact.validate(self.repo, dict(event, run_attempt='2'), ENV, receipt)
        with self.assertRaises(ci_impact.ContractError):
            ci_impact.validate(self.repo, event, dict(ENV, platform='windows'), receipt)

    def test_candidate_cannot_grant_itself_policy_or_discovery_authority(self):
        policy = json.loads((self.repo / '.github/ci/ci_impact.json').read_text())
        policy['rules'] = [{"glob": "**", "consumers": []}]
        self.write('.github/ci/ci_impact.json', json.dumps(policy))
        receipt = self.plan(self.event(['docs/retained/RESULTS.json']))
        self.assertFull(receipt)
        self.assertEqual(receipt['identity']['accepted_policy_commit'], self.base)
        # Missing declared workflow is unsuccessful discovery, never a smaller universe.
        self.git('checkout', '-q', self.base)
        self.git('rm', '.github/workflows/hashdb-tests.yml')
        self.assertFull(self.plan(self.event(['docs/retained/RESULTS.json'])))

    def test_discovery_errors_and_stale_dynamic_consumer_contract_force_full(self):
        event = self.event()
        original = ci_impact.git
        for failure in ('ls-tree', 'diff'):
            def faulty(repo, *args):
                if args[0] == failure:
                    if failure == 'diff':
                        return b'M\0docs/retained/RESULTS.json'
                    raise ci_impact.ContractError('git-discovery-error')
                return original(repo, *args)
            with self.subTest(failure=failure), patch.object(ci_impact, 'git', faulty):
                self.assertFull(self.plan(event))
        reviewed_base = self.base
        for source in ('TreeDB/witness_test.go', '.github/scripts/ci_impact.py', 'TreeDB/native.cpp'):
            with self.subTest(source=source):
                self.git('checkout', '-q', reviewed_base)
                self.write(source, 'new dynamic reader\n')
                self.base = self.commit()
                self.assertFull(self.plan(self.event()), 'consumer-discovery-drift')

    def test_real_cli_replay_uses_independently_retained_original_environment(self):
        event = self.event()
        raw = {'number': 1, 'repository': {'full_name': 'snissn/gomap'},
               'pull_request': {'number': 1, 'base': {'sha': event['base']}, 'head': {'sha': event['head']}}}
        event_path = self.repo / 'event.json'
        event_path.write_text(json.dumps(raw))
        environment_path = self.repo / 'expected-environment.json'
        environment_path.write_text(json.dumps(ENV))
        receipt_path = self.repo / 'reports/receipt.json'
        common = [sys.executable, str(ROOT / '.github/scripts/ci_impact.py'), 'plan',
                  '--repo', str(self.repo), '--event-path', str(event_path),
                  '--event-name', 'pull_request', '--candidate', event['candidate'],
                  '--run-id', '123', '--run-attempt', '1', '--workflow-ref', 'fixture',
                  '--output', str(receipt_path)]
        subprocess.run(common + ['--environment-path', str(environment_path)], check=True, stdout=subprocess.DEVNULL)
        common[2] = 'validate'
        self.assertNotEqual(subprocess.run(common, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode, 0)
        subprocess.run(common + ['--environment-path', str(environment_path)], check=True, stdout=subprocess.DEVNULL)
        self.assertEqual((receipt_path.parent / 'event.json').read_bytes(), event_path.read_bytes())
        self.assertEqual(json.loads((receipt_path.parent / 'environment.json').read_text()), ENV)


class SourceInventoryContract(unittest.TestCase):
    def test_reviewed_workflows_and_original_members_are_complete_and_advisory(self):
        policy = ci_impact.check_policy(json.loads((ROOT / '.github/ci/ci_impact.json').read_text()))
        files = [p for p in (ROOT / '.github/workflows').iterdir() if p.suffix in ('.yml', '.yaml')]
        self.assertEqual({p.name for p in files}, set(policy['workflows']))
        self.assertEqual(len(files), 15)
        self.assertEqual(len(policy['members']), 57)
        for p in files:
            self.assertEqual(ci_impact.digest(p.read_bytes()), policy['workflows'][p.name]['sha256'], p)
        tree_members = [m for m in policy['members'] if m['workflow'] == 'treedb-tests.yml']
        self.assertEqual(len(tree_members), 33)
        tests = [m for m in tree_members if m['job'] == 'test']
        source = (ROOT / '.github/workflows/treedb-tests.yml').read_text()
        import re
        block = source.split('  test:\n', 1)[1].split('  race:\n', 1)[0]
        names = re.findall(r'^          - name: (.+)$', block, re.MULTILINE)
        self.assertEqual({m['variant']['matrix']['name'] for m in tests}, set(names))
        self.assertEqual(len(names), 22)
        required = source.split('  required:\n', 1)[1]
        self.assertNotIn('impact-shadow', required)
        self.assertIn('[[ "$result" == "success" ]]', required)
        shadow = source.split('  impact-shadow:\n', 1)[1].split('  required:\n', 1)[0]
        self.assertNotIn('needs:', shadow)
        self.assertIn('continue-on-error: true', shadow)
        self.assertIn('retention-days: 30', shadow)
        self.assertIn('ci-impact-shadow-${{ github.run_id }}-${{ github.run_attempt }}', shadow)


if __name__ == '__main__':
    unittest.main()
