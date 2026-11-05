#!/usr/bin/env python3
"""Fixed source/pure predicate checks; never import the constructor or run Go."""
import ast
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import threading
import unittest
from unittest.mock import patch
import yaml

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT/'.github/scripts/primary_allocation_construction.py'
SOURCE = SCRIPT.read_text()
TREE = ast.parse(SOURCE)
FUNCTIONS = {n.name: n for n in TREE.body if isinstance(n, ast.FunctionDef)}


def isolated(names):
    context = [n for n in TREE.body if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id.startswith(('FAILURE_', 'FIRST_FAILURE_')) for t in n.targets)]
    names = tuple(dict.fromkeys(('observation', 'failure_context', *names)))
    nodes = context + [FUNCTIONS[name] for name in names]
    namespace = dict(threading=threading, os=os, json=json, base64=base64, re=re, Path=Path, stat=stat, hashlib=hashlib, HARNESS=ROOT,
                     BRANCH='refs/heads/diagnostic/primary-allocation-manifest-transport-01',
                     ROOT_THREAD='01a18e7c-8c23-7fd2-8d55-b0486d938cd3',
                     CONTROLS_PASS_SHA256='5fbc92847359e709fe9ef7812786ff469f9892dedf9e4c81d27a3183e67245c5')
    exec(compile(ast.Module(body=nodes, type_ignores=[]), '<pure-predicates-only>', 'exec'), namespace)
    return namespace


class PrimaryConstructionSource(unittest.TestCase):
    def test_failure_refusal_is_first_literal_and_redacted(self):
        ns = isolated(('need',))
        ns['observation']('runtime_rows', mode='launch', runtime_row=267)
        with self.assertRaises(RuntimeError):
            ns['need'](False, 'fixed tool identity differs')
        first = ns['failure_context']()
        ns['observation']('publication')
        with self.assertRaises(RuntimeError):
            ns['need'](False, 'missing or invalid upload identity')
        self.assertEqual(ns['failure_context'](), first)
        self.assertEqual(first['runtime_row'], 267)
        self.assertEqual(first['first_need_refusal'], 'fixed tool identity differs')
        self.assertEqual(first['stage'], 'runtime_rows')
        self.assertFalse(first['guard_spawned'])
        self.assertFalse(first['guard_returned'])
        ns = isolated(('need',))
        with self.assertRaises(RuntimeError):
            ns['need'](False, 'SECRET URL TOKEN PATH')
        self.assertNotIn('SECRET', json.dumps(ns['failure_context']()))
        self.assertIsNone(ns['failure_context']()['first_need_refusal'])

    def test_unexpected_error_retains_class_not_message(self):
        ns = isolated(('need',))
        ns['observation']('authenticate', mode='worker')
        value = ns['failure_context'](OSError('SECRET URL TOKEN PATH'))
        self.assertEqual(value['error_class'], 'OSError')
        self.assertEqual(value['stage'], 'authenticate')
        self.assertNotIn('SECRET', json.dumps(value))
        self.assertIsNone(value['first_need_refusal'])
        ns = isolated(('need',))
        foreign = type('SECRET/URL', (Exception,), {})('SECRET')
        self.assertEqual(ns['failure_context'](foreign)['error_class'], 'UnexpectedException')

    def test_fixed_stage_mode_and_row_bounds(self):
        ns = isolated(('need',))
        for row in (-1, 0, 267, 4095):
            ns['observation']('runtime_rows', mode='launch', runtime_row=row)
            self.assertEqual(ns['failure_context']()['runtime_row'], row)
        for row in (-2, 4096, True, '0'):
            with self.subTest(row=row), self.assertRaises(ValueError):
                ns['observation']('runtime_rows', runtime_row=row)
        for stage, mode in [('SECRET', None), ('entry', 'SECRET')]:
            with self.assertRaises(ValueError):
                ns['observation'](stage, mode=mode)

    def test_failure_allowlist_and_fixed_source_entry_order(self):
        ns = isolated(('need',))
        calls = [n for n in ast.walk(TREE) if isinstance(n, ast.Call)
                 and isinstance(n.func, ast.Name) and n.func.id == 'need']
        self.assertTrue(all(isinstance(n.args[1], ast.Constant) for n in calls))
        self.assertEqual(ns['FAILURE_REASONS'], frozenset(n.args[1].value for n in calls))
        main = ast.get_source_segment(SOURCE, FUNCTIONS['main'])
        self.assertLess(main.index("observation('authority'"), main.index('a = authority()'))
        self.assertLess(main.index("observation('authenticate'"), main.index('authenticate(a)'))
        self.assertLess(main.index("observation('runtime_rows'"), main.index("tool_identity(row['path'], row)"))
        self.assertLess(main.index('child = subprocess.Popen(cmd)'), main.index("FAILURE_CONTEXT['guard_spawned'] = True"))
        self.assertLess(main.index('code = child.wait'), main.index("FAILURE_CONTEXT['guard_returned'] = True"))
        worker = ast.get_source_segment(SOURCE, FUNCTIONS['worker'])
        self.assertLess(worker.index("write(root/'output/failure-context.json'"), worker.index('publish(root, a, r, run, env, runtime)'))
        self.assertIn('failure_context=failure_context()', worker)

    def test_actual_controls_and_actor_are_required(self):
        ns = isolated(('need', 'authority'))
        head = '1'*40
        a = dict(schema='primary-construction-admission-v1', status='ADMITTED_CONSTRUCTION_ONLY',
                 authority='ROOT', authority_thread=ns['ROOT_THREAD'], actor_id='1981537', head=head, run_attempt='1', allow_capture=False,
                 independent_source_review_sha256='2'*64, owner_manifest_sha256='3'*64,
                 controls=dict(status='ROOT_ADMITTED_FOCUSED_OBSERVER_CONTROLS', owner_manifest_sha256='3'*64,
                               root_receipt_sha256=ns['CONTROLS_PASS_SHA256'],
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
        c=json.loads(json.dumps(a));c['controls']['root_receipt_sha256']='f'*64
        with self.assertRaises(RuntimeError):check(c)
        for field,value in [('authority','FOREIGN'),('authority_thread','FOREIGN'),('authority_thread',None),('actor_id','0'),('allow_capture',True),('head','9'*40)]:
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
        self.assertEqual(len(job['steps']),2)
        self.assertEqual(job['steps'][1]['uses'],'./.github/actions/primary-construction')
        self.assertNotIn('actions/upload-artifact@',str(workflow))

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

    def test_fixed_standard_upload_output_protocol(self):
        ns = isolated(('need', 'upload_outputs'))
        marker='ghadelimiter_12345678-1234-1234-1234-123456789abc'
        values={'artifact-id':'123','artifact-digest':'a'*64,
                'artifact-url':'https://github.com/snissn/gomap/actions/runs/456/artifacts/123'}
        text=''.join(key+'<<'+marker+'\n'+value+'\n'+marker+'\n' for key,value in values.items())
        self.assertEqual(ns['upload_outputs'](text,'snissn/gomap','456'),values)
        for name,changed in [('missing',text.split('artifact-digest')[0]),
                             ('duplicate',text+text),('foreign',text.replace('/456/','/999/')),
                             ('bad-id',text.replace('\n123\n','\n0\n')),
                             ('bad-digest',text.replace('a'*64,'a'*63)),
                             ('bad-marker',text.replace(marker,'foreign')),
                             ('oversized','x'*4097)]:
            with self.subTest(name=name), self.assertRaises((RuntimeError,ValueError)):
                ns['upload_outputs'](changed,'snissn/gomap','456')

    def test_publication_is_guarded_joined_and_not_self_certifying(self):
        worker=ast.get_source_segment(SOURCE,FUNCTIONS['worker'])
        publication=ast.get_source_segment(SOURCE,FUNCTIONS['publish'])
        self.assertLess(worker.index('publish(root, a, r, run, env, runtime)'),worker.index('r.close()'))
        self.assertIn("run('publication', [node, str(bundle)], WHOLE, selected_env=selected)",publication)
        self.assertLess(publication.index('fresh counted standard action output'),publication.index("run('publication'"))
        self.assertIn("*list((root/'bin').iterdir())",publication)
        self.assertNotIn("root/'guard'",publication)
        self.assertLess(publication.index('paths = sorted'),publication.index("run('publication'"))
        self.assertLess(publication.index("run('publication'"),publication.index("write(root/'output/publication-receipt.json'"))
        self.assertIn('selected.clear()',publication)
        self.assertIn('runtime.clear()',worker)
        auth=ast.get_source_segment(SOURCE,FUNCTIONS['authenticate'])
        self.assertIn("sha(proof) == CONTROLS_PASS_SHA256",auth)
        self.assertIn("accepted['authority_thread'] == ROOT_THREAD",auth)
        self.assertIn("accepted['owner_manifest_sha256'] == a['owner_manifest_sha256']",auth)
        self.assertIn('if r.first_failure is None and time.monotonic() < r.deadline:',worker)
        self.assertLess(worker.index('except BaseException as caught:'),worker.index('publish(root, a, r, run, env, runtime)'))
        for field in ('INPUT_IF-NO-FILES-FOUND','INPUT_RETENTION-DAYS','INPUT_COMPRESSION-LEVEL',
                      'INPUT_OVERWRITE','INPUT_INCLUDE-HIDDEN-FILES'):
            self.assertIn(field,publication)
        main=ast.get_source_segment(SOURCE,FUNCTIONS['main'])
        self.assertIn("publication['status'] == 'UPLOAD_JOINED_PENDING_OUTER_GUARD'",main)
        self.assertIn("publication=publication, worker=terminal, guard=result",main)
        self.assertIn('len(encoded) <= 8*1024**2',main)
        self.assertNotIn('set-output',main)

    def test_local_action_uses_standard_environment_and_foreground_close(self):
        directory=ROOT/'.github/actions/primary-construction'
        metadata=yaml.safe_load((directory/'action.yml').read_text())
        self.assertEqual(metadata['runs'],{'using':'node20','main':'index.js'})
        js=(directory/'index.js').read_text()
        self.assertIn('identity(process.execPath, a.tools.node)',js)
        self.assertIn("child.on('close'",js)
        self.assertIn("child.kill('SIGINT')",js)
        self.assertIn('PRIMARY_NODE_PATH: node',js)
        self.assertNotIn('detached:',js)
        self.assertNotIn('console.log',js)
        self.assertNotIn('ACTIONS_RUNTIME_TOKEN:',js)
        self.assertNotIn('setTimeout',js)
        bundle=ROOT/'.github/primary-construction/upload-artifact/index.js'
        self.assertEqual(hashlib.sha256(bundle.read_bytes()).hexdigest(),
                         '0165b8a75330f3228f2c7a234b4ff8a107b9139c2b519147f4c8e9fe99b262d8')

    def test_complete_runtime_manifest_and_refusal_cases(self):
        ns = isolated(('need', 'canonical', 'runtime_inputs'))
        relative = '.github/primary-construction/readonly-runtime-inputs.json'
        raw = (ROOT/relative).read_bytes()
        rows = json.loads(raw)
        bound = dict(path=relative, bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest(), rows=268)
        a = dict(readonly_runtime_manifest=bound)
        self.assertEqual(ns['runtime_inputs'](a), rows)
        self.assertEqual(raw, ns['canonical'](rows)+b'\n')
        for key, value in [('path','/outside.json'),('bytes',len(raw)-1),('bytes',0),
                           ('bytes',1024**2+1),('sha256','0'*64),('rows',267),('rows',4097)]:
            with self.subTest(key=key,value=value), self.assertRaises(RuntimeError):
                ns['runtime_inputs'](dict(readonly_runtime_manifest=dict(bound, **{key:value})))
        with self.assertRaises(RuntimeError):
            ns['runtime_inputs'](dict(a,readonly_runtime_inputs=rows))
        with self.assertRaises(RuntimeError):
            ns['runtime_inputs']({})
        main = ast.get_source_segment(SOURCE, FUNCTIONS['main'])
        self.assertLess(main.index('authenticate(a)'), main.index('readonly = runtime_inputs(a)'))
        self.assertLess(main.index('for row_index, row in enumerate(readonly):'), main.index("if args.mode == 'worker'"))
        self.assertIn("tool_identity(row['path'], row)", main)
        worker = ast.get_source_segment(SOURCE, FUNCTIONS['worker'])
        self.assertIn('readonly = runtime_inputs(a)', worker)
        self.assertIn("tool_identity(row['path'], row)", worker)
        self.assertIn('os.O_NOFOLLOW', ast.get_source_segment(SOURCE, FUNCTIONS['runtime_inputs']))

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
