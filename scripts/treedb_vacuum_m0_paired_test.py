#!/usr/bin/env python3
"""Synthetic contract checks, never physical unchanged-noise/regression evidence."""
import importlib.util
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('paired_summary', HERE/'treedb_vacuum_m0_summarize.py')
M = importlib.util.module_from_spec(spec)
spec.loader.exec_module(M)
spec = importlib.util.spec_from_file_location('original_summary_tests', HERE/'treedb_vacuum_m0_summarize_test.py')
F = importlib.util.module_from_spec(spec)
spec.loader.exec_module(F)


class PairedTests(unittest.TestCase):
    def packet(self, root, slowdown=1.0):
        production, public = F.VacuumM0SummarizeTest().production_observations()
        # Realistic scale and modest sample drift, with unchanged-head residual
        # noise. Values are intentionally synthetic, not host measurements.
        drift = [.98, 1.01, .995, 1.02, 1., .99, 1.015, .985, 1.005, 1.]
        residual = [1.003, .996, 1.005, .998, 1., 1.004, .995, 1.002, .999, 1.001]
        baseline = {name: dict(entry) for name, entry in production.items()}
        for metric, scale in [('vacuum-total-ns/op', 50e9), ('foreground-p99-ns/op', 300e3)]:
            baseline[metric] = {'samples': [scale*x for x in drift]}
            production[metric] = {'samples': [scale*x*y*slowdown for x, y in zip(drift, residual)]}
        (root/'raw').mkdir()
        for arm, metrics in [('production', production), ('baseline', baseline), ('public', public)]:
            for n in range(1, 11):
                benchmark = M.STABILITY_BENCHMARKS['production'] if arm != 'public' else 'BenchmarkPublic'
                row = benchmark+'-2 1 1 ns/op '+ ' '.join(f"{v['samples'][n-1]} {k}" for k,v in metrics.items())
                (root/'raw'/f'{arm}-{n}.txt').write_text(row+'\nPASS\n')
        fixture = {'timing_boundary': 'fixed-work', 'fixture': {
            'logical_digest': 'fixture', 'index_bytes': 2, 'offline_index_bytes': 1,
            'reclaimable_pages': 1, 'reclaimable_page_percent': 50,
            'value_log_bytes': 1, 'offline_value_log_bytes': 1, 'leaf_log_bytes': 0, 'parameters': {}}}
        for name in ['fixture.json', 'baseline-fixture.json']:
            (root/name).write_text(json.dumps(fixture))
        (root/'baseline-db.test').write_bytes(b'inert binary fixture')
        (root/'paired-provenance.json').write_text(json.dumps({
            'candidate_source_sha': 'a'*40, 'baseline_source_sha': 'b'*40,
            'baseline_binary_sha256': hashlib.sha256(b'inert binary fixture').hexdigest(),
            'candidate_db_test_harness_sha256': {}}))

    def run_summary(self, root, contract='v4-proposal', profile='controlled-performance'):
        env = {'git_sha': 'a'*40, 'dirty_state': 'clean', 'go_version': 'test', 'goos': 'linux',
               'goarch': 'amd64', 'device': 'test', 'filesystem': 'test', 'commands': []}
        argv = ['summary', '--repo-root', str(root), '--run-dir', str(root), '--stability-arm', 'production',
                '--acceptance-contract', contract, '--qualification-profile', profile]
        with mock.patch.dict(os.environ, {'M0_BASELINE_SHA': 'b'*40}), mock.patch.object(sys, 'argv', argv), mock.patch.object(M, 'environment', return_value=env), \
             mock.patch.object(M, 'command', return_value='a'*40):
            return M.main()

    def test_uniform_twenty_percent_slowdown_cv_green_paired_red(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); self.packet(root, 1.2)
            self.assertEqual(self.run_summary(root, 'v3'), 0) # Original blind spot preserved visibly.
            old = json.loads((root/'results.json').read_text())
            self.assertTrue(old['gates']['production_cv_at_most_10_percent'])
            self.assertEqual(self.run_summary(root), 1)
            result = json.loads((root/'results.json').read_text())
            self.assertTrue(result['gates']['production_cv_at_most_10_percent'])
            self.assertFalse(result['gates']['paired_material_regression_absent'])
            self.assertIsNone(result['product_performance_approval'])

    def test_synthetic_unchanged_noise_passes_and_retains_numeric_pairs(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); self.packet(root)
            self.assertEqual(self.run_summary(root), 0)
            result = json.loads((root/'results.json').read_text())
            self.assertEqual(result['schema_version'], 4)
            paired = result['paired_regression_proposal']
            self.assertIsNone(paired['statistical_inference'])
            for entry in paired['metrics'].values():
                self.assertEqual(len(entry['ratios']), 10)
                self.assertAlmostEqual(entry['ratios'][1], .996)
                self.assertAlmostEqual(entry['ratios'][9], 1.001)

    def test_missing_pairs_fixture_drift_and_wrong_source_fail_closed(self):
        for defect in ['missing', 'extra', 'fixture', 'source', 'binary', 'harness']:
            with self.subTest(defect=defect), tempfile.TemporaryDirectory() as d:
                root=Path(d);self.packet(root)
                if defect=='missing': (root/'raw/baseline-4.txt').unlink()
                if defect=='extra': (root/'raw/baseline-11.txt').write_bytes((root/'raw/baseline-1.txt').read_bytes())
                if defect=='fixture': (root/'baseline-fixture.json').write_text('{"fixture":{}}')
                if defect=='source': (root/'paired-provenance.json').write_text('{"candidate_source_sha":"wrong"}')
                if defect=='binary': (root/'baseline-db.test').write_bytes(b'changed')
                if defect=='harness':
                    (root/'TreeDB/db').mkdir(parents=True)
                    (root/'TreeDB/db/changed_test.go').write_text('changed')
                with self.assertRaises((ValueError, FileNotFoundError)):
                    self.run_summary(root)

    def test_baseline_semantic_failure_and_cv_failure_still_reject(self):
        for metric,value in [('vacuum-errors/op', 1), ('vacuum-total-ns/op', 90e9)]:
            with self.subTest(metric=metric), tempfile.TemporaryDirectory() as d:
                root=Path(d);self.packet(root)
                path=root/'raw/baseline-4.txt'
                fields=path.read_text().split()
                fields[fields.index(metric)-1]=str(value)
                path.write_text(' '.join(fields[:-1])+'\nPASS\n')
                self.assertEqual(self.run_summary(root), 1)
                self.assertFalse(json.loads((root/'results.json').read_text())['capture_passed'])

    def test_boundary_invalid_values_and_no_hosted_extra_obligation(self):
        for factor, passed in [(1.05, True), (1.05001, False), (.9, True)]:
            b={k:{'samples':[100]*10} for k in M.PRODUCT_V3_CV_METRICS}
            c={k:{'samples':[100*factor]*10} for k in M.PRODUCT_V3_CV_METRICS}
            self.assertEqual(M.paired_comparison(b,c)['passed'],passed)
        for bad in [0, -1, float('nan'), float('inf')]:
            b={k:{'samples':[bad]*10} for k in M.PRODUCT_V3_CV_METRICS}
            with self.assertRaises(ValueError):M.paired_comparison(b,b)
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);self.packet(root)
            for path in (root/'raw').glob('baseline-*'):path.unlink()
            self.assertEqual(self.run_summary(root,'v3','hosted-semantic'),0)


class CaptureScheduleTests(unittest.TestCase):
    def test_actual_controlled_capture_alternates_all_ten_pairs(self):
        # Only external workload/admission tools are inert here; run the actual
        # capture shell to verify routing, ordering and raw sample preservation.
        with tempfile.TemporaryDirectory() as d:
            root=Path(d); scripts=root/'scripts';scripts.mkdir(); shim=root/'bin';shim.mkdir()
            (scripts/'treedb_vacuum_m0_capture.sh').write_bytes((HERE/'treedb_vacuum_m0_capture.sh').read_bytes())
            for name in ['treedb_vacuum_m0_environment.py','treedb_vacuum_m0_summarize_test.py',
                         'treedb_vacuum_m0_paired_test.py','treedb_vacuum_m0_ci_test.py']:
                (scripts/name).write_text('')
            (scripts/'treedb_vacuum_m0_summarize.py').write_text(
                "import os,pathlib,sys;pathlib.Path(os.environ['M0_TEST_ROOT'],'summary-args').write_text(' '.join(sys.argv))")
            (scripts/'treedb_vacuum_m0_baseline.py').write_text(
                "import os,pathlib; p=pathlib.Path(os.environ['RUN_DIR'])/'baseline-db.test'; "
                "p.write_text('#!/bin/sh\\ncase \"$*\" in *-test.bench*) echo baseline >> \"$M0_TEST_ROOT/order\";; esac\\necho PASS\\n'); p.chmod(0o755)")
            tools={
                'git': '#!/bin/sh\ncase "$1" in rev-parse) if [ "$2" = --show-toplevel ]; then echo "$M0_TEST_ROOT"; else echo source; fi;; esac\n',
                'taskset': '#!/bin/sh\nshift 2\nexec "$@"\n',
                'go': '#!/bin/sh\ncase "$*" in *-bench*) case "$*" in *BenchmarkPL06*) echo public >> "$M0_TEST_ROOT/order";; *) echo production >> "$M0_TEST_ROOT/order";; esac;; esac\necho PASS\n',
                'benchstat': '#!/bin/sh\nexit 0\n'}
            for name,body in tools.items():
                (shim/name).write_text(body);(shim/name).chmod(0o755)
            env=dict(os.environ,PATH=str(shim)+os.pathsep+os.environ['PATH'],M0_TEST_ROOT=str(root),
                     RUN_DIR=str(root/'packet'),M0_CONTROLLED='1',M0_QUALIFICATION_PROFILE='controlled-performance',
                     CPU_SET='0,1',GOMAXPROCS='2',M0_LEASE_PATH='/inert',M0_BASELINE_SHA='b'*40)
            result=subprocess.run(['bash',str(scripts/'treedb_vacuum_m0_capture.sh')],env=env,capture_output=True,text=True)
            self.assertEqual(result.returncode,0,result.stderr)
            expected=[]
            for n in range(1,11):
                expected.extend(['baseline','production','public'] if n%2 else ['production','baseline','public'])
            self.assertEqual((root/'order').read_text().splitlines(),expected)
            self.assertEqual(len(list((root/'packet/raw').glob('*.txt'))),30)
            self.assertIn('--acceptance-contract v4-proposal',(root/'summary-args').read_text())


class BaselinePreparationTests(unittest.TestCase):
    def test_build_uses_exact_ancestor_and_candidate_harness(self):
        spec=importlib.util.spec_from_file_location('baseline_builder', HERE/'treedb_vacuum_m0_baseline.py')
        builder=importlib.util.module_from_spec(spec);spec.loader.exec_module(builder)
        with tempfile.TemporaryDirectory() as d:
            root=Path(d); repo=root/'repo'; repo.mkdir(); run=root/'packet'; run.mkdir()
            db=repo/'TreeDB/db';db.mkdir(parents=True)
            def git(*args):
                return subprocess.check_output(['git', *args], cwd=repo, text=True, stderr=subprocess.DEVNULL).strip()
            git('init');git('config','user.email','fixture@example.invalid');git('config','user.name','Fixture')
            (db/'engine.go').write_text('baseline production')
            (db/'obsolete_test.go').write_text('obsolete baseline harness')
            git('add','.');git('commit','-m','baseline');source=git('rev-parse','HEAD')
            (db/'engine.go').write_text('candidate production')
            (db/'obsolete_test.go').unlink()
            (db/'current_test.go').write_text('candidate exact harness')
            git('add','.');git('commit','-m','candidate')
            shim=root/'bin';shim.mkdir()
            fake=shim/'go'
            fake.write_text("""#!/usr/bin/python3
import pathlib,sys
if sys.argv[1]=='version':
 print('inert fixture build info')
else:
 assert pathlib.Path('TreeDB/db/engine.go').read_text()=='baseline production'
 assert pathlib.Path('TreeDB/db/current_test.go').read_text()=='candidate exact harness'
 assert not pathlib.Path('TreeDB/db/obsolete_test.go').exists()
 pathlib.Path(sys.argv[sys.argv.index('-o')+1]).write_bytes(b'inert compiled fixture')
""")
            fake.chmod(0o755)
            with mock.patch.dict(os.environ, {'PATH':str(shim)+os.pathsep+os.environ['PATH']}):
                builder.prepare(repo,run,source)
            provenance=json.loads((run/'paired-provenance.json').read_text())
            self.assertEqual(provenance['baseline_source_sha'],source)
            self.assertEqual(provenance['candidate_source_sha'],git('rev-parse','HEAD'))
            self.assertEqual(provenance['candidate_db_test_harness_sha256'],{
                'TreeDB/db/current_test.go':hashlib.sha256(b'candidate exact harness').hexdigest()})
            self.assertEqual(provenance['baseline_binary_sha256'],hashlib.sha256(b'inert compiled fixture').hexdigest())
            (db/'engine.go').write_text('uncommitted')
            with self.assertRaisesRegex(ValueError,'clean candidate'):
                builder.prepare(repo, root/'unused', source)


if __name__=='__main__': unittest.main()
