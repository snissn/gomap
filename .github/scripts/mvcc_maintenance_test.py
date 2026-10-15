#!/usr/bin/env python3
"""Pure syntax, parser and workflow controls; never build or execute Go inputs."""
import ast
import importlib.util
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import subprocess
import tempfile
import time
import unittest

sys.dont_write_bytecode = True
os.environ.setdefault('RUNNER_TEMP', '/nonexistent-maintenance-static-check')
SCRIPTS = Path(__file__).resolve().parent
ROOT = SCRIPTS.parent.parent


def load(name):
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


collector = load('mvcc_maintenance_collect')
bootstrap = load('mvcc_maintenance_bootstrap')


def check_workflow(text):
    event = re.search(r'(?m)^on:\n((?:[ \t].*\n|\n)+)', text)
    assert event and event[1].strip() == 'workflow_dispatch:'
    assert re.search(r'(?m)^permissions:\n  contents: read\n', text)
    assert text.count('contents:') == 1
    assert 'pull_request' not in text and 'push:' not in text and 'self-hosted' not in text
    assert '${{ inputs.' not in text and 'secrets.' not in text
    assert text.count('runs-on: ubuntu-24.04') == 1
    assert "if: github.event_name == 'workflow_dispatch' && github.ref == '" + bootstrap.BRANCH + "'" in text
    actions = re.findall(r'uses: ([^\s]+)', text)
    assert actions == [
        'actions/checkout@11d5960a326750d5838078e36cf38b85af677262',
        'actions/setup-go@40f1582b2485089dde7abd97c1529aa768e1baff',
        'actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02']
    assert 'persist-credentials: false' in text and 'fetch-depth: 0' in text
    assert 'ref: ${{ github.sha }}' in text
    assert 'go-version: "1.26.3"' in text and 'cache: false' in text
    assert 'if: always()' in text and 'if-no-files-found: error' in text
    assert 'include-hidden-files: true' in text
    assert 'runner.temp }}/mvcc-maintenance-a17fe' in text
    runs = re.findall(r'(?m)^        run: (.+)$', text)
    assert runs == ['|', 'python3 -B .github/scripts/mvcc_maintenance_test.py > "$RUNNER_TEMP/mvcc-maintenance-setup/pure-tests.log" 2>&1',
                    'python3 -B .github/scripts/mvcc_maintenance_bootstrap.py > "$RUNNER_TEMP/mvcc-maintenance-setup/bootstrap.log" 2>&1']
    assert text.index('Initialize retained attempt packet') < text.index('Checkout reviewed diagnostic harness')
    assert 'mvcc-maintenance-a17fe/archives' in text
    assert '!${{ runner.temp }}/mvcc-maintenance-a17fe/performance-185/**/tmp' in text


class Contract(unittest.TestCase):
    def test_syntax(self):
        for path in SCRIPTS.glob('mvcc_maintenance_*.py'):
            ast.parse(path.read_text(), filename=str(path))

    def test_original_parser_controls(self):
        collector.selftest()  # Original positive/six negative controls unchanged.

    def test_reviewed_algorithm_equivalence(self):
        tree = ast.parse((SCRIPTS / 'mvcc_maintenance_collect.py').read_text())
        wanted = {'schedule': '5323e2ff46d310326b032152c8b44fa0086feea02ce07403cfc68882da3fd15e',
                  'parse': '08c21b50bace4954db65d4a06ef9860c0723247bbc0bd5dbe48a52c31e21999f',
                  'selftest': '937dd197f07eee5770ebb5ec655d5236e7cc98c56f84e3d242cb3cf13447030c'}
        for name, expected in wanted.items():
            node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)
            self.assertEqual(hashlib.sha256(ast.dump(node, include_attributes=False).encode()).hexdigest(), expected)

    def test_fixed_product_inputs(self):
        self.assertEqual(bootstrap.BASELINE, '5bca87ffe6e1bfba2d693264d2dc82936adc234f')
        self.assertEqual(bootstrap.CANDIDATE, 'a17febafb5353897c59a4e17f992e34c47a2585b')
        self.assertEqual(bootstrap.COMPILER_SHA256, 'd68b7abbc40d0844f673f6cf06ae3cded225c50437c6454fa37ef178d079fe65')
        tree = ast.parse((SCRIPTS / 'mvcc_maintenance_prepare.py').read_text())
        heads = next(n for n in tree.body if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'heads' for t in n.targets))
        self.assertEqual(ast.literal_eval(heads.value), {'baseline': bootstrap.BASELINE, 'candidate': bootstrap.CANDIDATE})
        update = next(n.value for n in tree.body if isinstance(n, ast.Expr) and isinstance(n.value, ast.Call) and isinstance(n.value.func, ast.Attribute) and isinstance(n.value.func.value, ast.Name) and n.value.func.value.id == 'env')
        keys = {k.arg: k.value for k in update.keywords}
        self.assertEqual(ast.literal_eval(keys['GOPROXY']), 'off')
        self.assertEqual(ast.literal_eval(keys['GOSUMDB']), 'off')
        self.assertEqual(ast.literal_eval(keys['GOMAXPROCS']), '2')

    def test_fixed_schedule(self):
        rows = collector.schedule()
        self.assertEqual(len(rows), 14)
        self.assertEqual([(r['kind'], r['revision'], r['count'], r['package']) for r in rows],
                         [(k, rev, count, package) for k, count, package in
                          [('checkpoint', 16, 'public'), ('vacuum', 1, 'db')]
                         for rev in ['baseline', 'candidate', 'candidate', 'baseline', 'baseline', 'candidate']]
                         + [('cohort', 'candidate', 128, 'db')] * 2)
        self.assertEqual([r['size'] for r in rows[-2:]], [8, 16])
        self.assertEqual(rows[0]['name'], 'BenchmarkCOWPublicDirtyCost/command_wal_durable/btree/Pointer4096/N=1024/DirtyCheckpoint')
        self.assertEqual(rows[6]['name'], 'BenchmarkVacuumIndexOnlineCollectionProductionForegroundChurn/bytes_64x')
        self.assertEqual(rows[-2]['name'], 'BenchmarkPrimaryBorrowedValueLogCohort/files=8')
        self.assertEqual(rows[-1]['name'], 'BenchmarkPrimaryBorrowedValueLogCohort/files=16')

    def test_checkpoint_and_cohort_receipts(self):
        def raw(item, values):
            return item['name'] + '-2 ' + str(item['count']) + ' ' + ' '.join(str(v) + ' ' + k for k, v in values.items()) + '\nPASS\n'
        checkpoint = collector.schedule()[0]
        values = {'ns/op': 10, 'B/op': 20, 'allocs/op': 1}
        values.update({k: 1 for k in ['fixture_checkpoint_runs', 'fixture_command_wal_sync_count',
            'fixture_command_wal_file_sync_calls', 'fixture_value_log_sync_calls', 'p50-ns', 'p99-ns']})
        collector.parse(raw(checkpoint, values), checkpoint)
        for key in list(values):
            missing = dict(values)
            del missing[key]
            with self.assertRaises(ValueError, msg=key):
                collector.parse(raw(checkpoint, missing), checkpoint)
        cohort = collector.schedule()[-1]
        values = {'ns/op': 10, 'B/op': 20, 'allocs/op': 1, 'held-cohort-files': 16,
                  'closed-governor-bytes': 0}
        values.update({k: 1 for k in ['held-cohort-bytes', 'held-old-data-bytes', 'held-old-primary-bytes',
            'held-governor-bytes', 'scope-after-reader-governor-bytes', 'scope-complete-governor-bytes', 'last-reader-cleanup-ns']})
        result = collector.parse(raw(cohort, values), cohort)
        self.assertEqual(result['drained-cohort-files'], 0)
        for key, value in [('held-cohort-files', 8), ('closed-governor-bytes', 1), ('held-old-primary-bytes', 0)]:
            with self.assertRaises(ValueError, msg=key):
                collector.parse(raw(cohort, dict(values, **{key: value})), cohort)
        for key in values:
            missing = dict(values)
            del missing[key]
            with self.assertRaises(ValueError, msg=key):
                collector.parse(raw(cohort, missing), cohort)

    def test_manual_context_refusals(self):
        env = {'GITHUB_ACTIONS': 'true', 'GITHUB_EVENT_NAME': 'workflow_dispatch',
               'GITHUB_REF': bootstrap.BRANCH, 'GITHUB_REPOSITORY': 'snissn/gomap',
               'RUNNER_OS': 'Linux', 'RUNNER_ARCH': 'X64',
               'RUNNER_TEMP': '/ephemeral', 'GITHUB_WORKSPACE': '/harness'}
        bootstrap.validate_context(env)
        for key, value in [('GITHUB_ACTIONS', 'false'), ('GITHUB_EVENT_NAME', 'pull_request'),
                           ('GITHUB_EVENT_NAME', 'push'), ('GITHUB_REF', 'refs/heads/other'),
                           ('GITHUB_REPOSITORY', 'other/gomap'), ('RUNNER_ARCH', 'ARM64'),
                           ('RUNNER_OS', 'Windows'), ('RUNNER_TEMP', ''), ('GITHUB_WORKSPACE', '')]:
            with self.assertRaises(ValueError, msg=(key, value)):
                bootstrap.validate_context(dict(env, **{key: value}))

    def test_workflow(self):
        text = (ROOT / '.github/workflows/treedb-mvcc-maintenance-diagnostic.yml').read_text()
        check_workflow(text)
        for original, replacement in [('workflow_dispatch:\n', 'push:\n'),
                                      ('ubuntu-24.04', 'self-hosted'),
                                      ('contents: read', 'contents: write'),
                                      ('go-version: "1.26.3"', 'go-version: "1.26.x"'),
                                      ('cache: false', 'cache: true'),
                                      ('include-hidden-files: true', 'include-hidden-files: false'),
                                      ('if: always()', 'if: success()'),
                                      ('@ea165f8d65b6e75b540449e92b4886f43607fa02', '@v4')]:
            with self.assertRaises(AssertionError, msg=(original, replacement)):
                check_workflow(text.replace(original, replacement))

    def test_real_child_group_join(self):
        self.assertIs(collector.join_group, bootstrap.join_group)
        bootstrap.enable_subreaper()
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / 'forked'
            process = subprocess.Popen([sys.executable, '-c',
                'import os,time,pathlib; pid=os.fork(); '
                'pathlib.Path(' + repr(str(marker)) + ').write_text(str(os.getpid())); time.sleep(30)'],
                start_new_session=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            end = time.monotonic() + 2
            while not marker.exists() and time.monotonic() < end:
                time.sleep(0.01)
            record = bootstrap.join_group(process, interrupted=True)
            self.assertTrue(marker.exists())
            self.assertTrue(record['leader_joined'])
            self.assertTrue(record['process_group_empty'])
            self.assertTrue(record['adopted_children'])
            self.assertTrue(record['kill_requested'])
            print('toy-killed-group-join', json.dumps(record, sort_keys=True))
        process = subprocess.Popen([sys.executable, '-c', 'raise SystemExit(7)'], start_new_session=True)
        record = bootstrap.join_group(process)
        self.assertEqual(process.returncode, 7)
        self.assertTrue(record['leader_joined'])
        self.assertTrue(record['process_group_empty'])
        self.assertFalse(record['kill_requested'])
        print('toy-normal-group-join', json.dumps(record, sort_keys=True))

    def test_unexpected_survivor_is_not_success(self):
        bootstrap.enable_subreaper()
        process = subprocess.Popen([sys.executable, '-c',
            'import os,time; pid=os.fork(); time.sleep(30) if pid==0 else None'], start_new_session=True)
        process.wait(timeout=2)
        record = bootstrap.join_group(process)
        self.assertTrue(record['unexpected_survivors'])
        self.assertTrue(record['kill_requested'])
        self.assertTrue(record['process_group_empty'])
        self.assertTrue(record['adopted_children'])
        print('toy-unexpected-survivor-refusal', json.dumps(record, sort_keys=True))


if __name__ == '__main__':
    unittest.main()
