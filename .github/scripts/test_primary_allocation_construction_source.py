#!/usr/bin/env python3
"""Fixed source/pure predicate checks; never import the constructor or run Go."""
import ast
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import unittest
from unittest.mock import patch
import yaml

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT/'.github/scripts/primary_allocation_construction.py'
SOURCE = SCRIPT.read_text()
TREE = ast.parse(SOURCE)
FUNCTIONS = {n.name: n for n in TREE.body if isinstance(n, ast.FunctionDef)}


def isolated(names):
    nodes = [FUNCTIONS[name] for name in names]
    namespace = dict(os=os, json=json, base64=base64, re=re,
                     BRANCH='refs/heads/diagnostic/primary-allocation-construction-01')
    exec(compile(ast.Module(body=nodes, type_ignores=[]), '<pure-predicates-only>', 'exec'), namespace)
    return namespace


class PrimaryConstructionSource(unittest.TestCase):
    def test_actual_controls_and_actor_are_required(self):
        ns = isolated(('need', 'authority'))
        head = '1'*40
        a = dict(schema='primary-construction-admission-v1', status='ADMITTED_CONSTRUCTION_ONLY',
                 authority='ROOT', actor_id='1981537', head=head, run_attempt='1', allow_capture=False,
                 independent_source_review_sha256='2'*64, owner_manifest_sha256='3'*64,
                 controls=dict(status='ROOT_ADMITTED_FOCUSED_OBSERVER_CONTROLS', owner_manifest_sha256='3'*64,
                               review_sha256='4'*64, run_id=1, job_id=1,
                               normal=dict(tops=6,outcomes=26,leaves=24,exit=0,skips=0),
                               race=dict(tops=6,outcomes=26,leaves=24,exit=0,skips=0)))
        env = dict(GITHUB_ACTIONS='true', RUNNER_ENVIRONMENT='github-hosted', RUNNER_OS='Linux',
                   GITHUB_EVENT_NAME='workflow_dispatch', GITHUB_REPOSITORY='snissn/gomap',
                   GITHUB_REF=ns['BRANCH'], GITHUB_ACTOR_ID='1981537', GITHUB_RUN_ATTEMPT='1',
                   REVIEWED_HEAD=head, GITHUB_SHA=head, WORKFLOW_COMMIT=head)
        def check(value, changed=None):
            selected = dict(env, ROOT_ADMISSION_B64=base64.b64encode(json.dumps(value).encode()).decode())
            selected.update(changed or {})
            with patch.dict(os.environ, selected, clear=True):
                return ns['authority']()
        self.assertEqual(check(a), a)
        for field,value in [('authority','FOREIGN'),('actor_id','0'),('allow_capture',True),('head','9'*40)]:
            with self.subTest(field=field), self.assertRaises(RuntimeError):
                check(dict(a, **{field:value}))
        for mode in ('normal','race'):
            c = json.loads(json.dumps(a));c['controls'][mode]['exit']=1
            with self.subTest(mode=mode), self.assertRaises(RuntimeError):check(c)
        with self.assertRaises(RuntimeError):check(a, {'GITHUB_RUN_ATTEMPT':'2'})

    def test_complete_fixed_input_pins_and_bootstrap_syntax(self):
        manifest = ROOT/'.github/primary-construction-inputs.json'
        m = json.loads(manifest.read_text())
        for name,row in m['files'].items():
            data=(ROOT/name).read_bytes()
            self.assertEqual((len(data),hashlib.sha256(data).hexdigest()),(row['bytes'],row['sha256']))
        workflow=yaml.safe_load((ROOT/'.github/workflows/treedb-mvcc-maintenance-diagnostic.yml').read_text())
        job=workflow['jobs']['diagnostic']
        self.assertEqual(job['env']['INPUT_MANIFEST_SHA256'],hashlib.sha256(manifest.read_bytes()).hexdigest())
        bootstrap=job['steps'][0]['run'].split("python3 -I -B - <<'PY'\n",1)[1].rsplit('\nPY',1)[0]
        ast.parse(bootstrap)
        self.assertIn('initial_observed_bytes_including_preexisting', bootstrap)
        self.assertNotIn('actions/checkout',str(workflow))
        self.assertNotIn('actions/setup-go',str(workflow))
        self.assertEqual(len(job['steps']),3)

    def test_only_two_normal_builds_and_original_resource_numbers(self):
        worker=ast.get_source_segment(SOURCE,FUNCTIONS['worker'])
        self.assertIn("for arm in ARMS:",worker)
        self.assertIn("'-tags=treedb_test', '-p=1', '-mod=readonly', '-c'",worker)
        self.assertIn("'./TreeDB/mvcc'], 360",worker)
        self.assertNotIn("'-bench",worker)
        self.assertNotIn("'-race'",worker)
        self.assertIn('WHOLE = 840',SOURCE)
        self.assertIn('900-(time.monotonic()-started)',SOURCE)
        self.assertIn('str(24*GIB)',SOURCE)
        self.assertIn('str(10*GIB)',SOURCE)
        self.assertIn('s.st_size > 32*1024**2',SOURCE)
        self.assertIn("common == Path('/home/runner/work')",SOURCE)
        self.assertIn('os.walk(scope, followlinks=False)',SOURCE)

    def test_first_failure_before_throw_and_finite_original_guard(self):
        node=FUNCTIONS['whole_runner']
        text=ast.get_source_segment(SOURCE,node)
        self.assertLess(text.index("write(root/'output/resource-first-failure.json'"),text.index("need(self.first_failure is None"))
        self.assertIn('self.thread.join()',text)
        main=ast.get_source_segment(SOURCE,FUNCTIONS['main'])
        self.assertLess(main.index('authenticate(a)'),main.index("if args.mode == 'worker'"))
        self.assertIn("'--synthetic'",main)
        self.assertIn("result['cleanup_signals'] == []",main)
        self.assertIn("result['errors'] == []",main)
        self.assertIn("physical drain requires independent review",main)
        self.assertNotIn('stage_toolchain(',ast.get_source_segment(SOURCE,FUNCTIONS['worker']))

    def test_precise_owner_sources_and_baseline_separation(self):
        data=ROOT/'.github/primary-construction'
        m=json.loads((data/'owner/overlays.json').read_text())['primary']
        expected={'B':'29b879fbb4c4c6da2d7b18ff7c2a3a341e24e2b8','C':'4934bd6d51d9047ba0187d05c1d211cbe419811a'}
        for arm,head in expected.items():
            spec=json.loads((data/(arm+'-source.json')).read_text())
            self.assertEqual(m[arm]['source'],head);self.assertEqual(spec['commit'],head)
            self.assertEqual(len(m[arm]['Replace']),5)
            for path in m[arm]['Replace'].values():self.assertTrue((data/'owner'/path).is_file())
        self.assertTrue(m['B']['Replace']['TreeDB/caching/db.go'].startswith('baseline-candidate/'))
        self.assertTrue(m['C']['Replace']['TreeDB/caching/db.go'].startswith('candidate/'))
        self.assertIn("GOENV='off', GOWORK='off', GOTELEMETRY='off'",SOURCE)
        self.assertIn("GOCACHE=str(root/'gocache'), GOMODCACHE=str(root/'modcache')",SOURCE)


if __name__ == '__main__':
    unittest.main()
