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
    namespace = dict(os=os, json=json, base64=base64, re=re, Path=Path, stat=stat, hashlib=hashlib, HARNESS=ROOT,
                     BRANCH='refs/heads/diagnostic/primary-8e-b24-controls-binding-01',
                     ROOT_THREAD='01a18e7c-8c23-7fd2-8d55-b0486d938cd3',
                     OWNER_MANIFEST_SHA256='3ca00c97842855ec1c859d08371d17f13d89aa55fc1007be26fbec648684b618',
                     OWNER_REVIEW_SHA256='ffd74ae5d95fd1d4647e2524b778e854f814707d1775ad46a7f585b678d498e6')
    exec(compile(ast.Module(body=nodes, type_ignores=[]), '<pure-predicates-only>', 'exec'), namespace)
    return namespace


class PrimaryConstructionSource(unittest.TestCase):
    def test_new_controls_scope_and_actor_are_required(self):
        ns = isolated(('need', 'authority'))
        head = '1'*40
        a = dict(schema='primary-new-arm-controls-admission-v1', status='ADMITTED_NEW_ARM_CONTROLS_ONLY',
                 authority='ROOT', authority_thread=ns['ROOT_THREAD'], actor_id='1981537', head=head,
                 run_attempt='1', allow_capture=False, prior_controls_credit=False,
                 independent_source_review_sha256='2'*64, owner_manifest_sha256=ns['OWNER_MANIFEST_SHA256'],
                 owner_source_review_sha256=ns['OWNER_REVIEW_SHA256'],
                 arms=dict(B='8e1133085b4052e344a73063f28c2ab142485c73', C='b24dea5e339cf94d419346ac9b38a8c90d6d3f02'),
                 controls=dict(tops=4,outcomes=8,leaves=7,modes=['normal','race']))
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
        for field,value in [('authority','FOREIGN'),('authority_thread','FOREIGN'),('authority_thread',None),
                            ('actor_id','0'),('allow_capture',True),('head','9'*40),('prior_controls_credit',True),
                            ('owner_manifest_sha256','0'*64),('owner_source_review_sha256','0'*64),
                            ('controls',dict(tops=6,outcomes=26,leaves=24,modes=['normal','race'])),('arms',{})]:
            with self.subTest(field=field), self.assertRaises(RuntimeError):
                check(dict(a, **{field:value}))
        c=dict(a);c.pop('authority_thread')
        with self.assertRaises(RuntimeError):check(c)
        with self.assertRaises(RuntimeError):check(a, {'GITHUB_RUN_ATTEMPT':'2'})
        with self.assertRaises(RuntimeError):check(a, {'WORKFLOW_COMMIT':'9'*40})

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

    def test_fixed_normal_preparation_and_both_modes_original_resources(self):
        worker=ast.get_source_segment(SOURCE,FUNCTIONS['worker'])
        self.assertIn("for arm in ARMS:",worker)
        self.assertIn("'-tags=treedb_test', '-p=1', '-mod=readonly', '-c'",worker)
        self.assertIn("for package in ('caching', 'mvcc'):",worker)
        self.assertIn("for mode in ('normal', 'race'):",worker)
        self.assertIn("180 if mode == 'normal' else 600",worker)
        self.assertIn("formatted.strip()",worker)
        self.assertNotIn("'-bench",worker)
        self.assertIn("args.append('-race')",worker)
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
        self.assertNotIn("root-controls-pass.json",auth)
        self.assertIn("OWNER_REVIEW_SHA256",auth)
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
        self.assertLess(main.index('for row in readonly:'), main.index("if args.mode == 'worker'"))
        self.assertIn("tool_identity(row['path'], row)", main)
        worker = ast.get_source_segment(SOURCE, FUNCTIONS['worker'])
        self.assertIn('readonly = runtime_inputs(a)', worker)
        self.assertIn("tool_identity(row['path'], row)", worker)
        self.assertIn('os.O_NOFOLLOW', ast.get_source_segment(SOURCE, FUNCTIONS['runtime_inputs']))

    def test_precise_owner_sources_and_baseline_separation(self):
        data=ROOT/'.github/primary-construction'
        m=json.loads((data/'owner/overlays.json').read_text())['primary']
        expected={'B':'8e1133085b4052e344a73063f28c2ab142485c73','C':'b24dea5e339cf94d419346ac9b38a8c90d6d3f02'}
        for arm,head in expected.items():
            spec=json.loads((data/(arm+'-source.json')).read_text())
            self.assertEqual(m[arm]['source'],head);self.assertEqual(spec['commit'],head)
            self.assertEqual(len(m[arm]['Replace']),5)
            for path in m[arm]['Replace'].values():self.assertTrue((data/'owner'/path).is_file())
        self.assertTrue(m['B']['Replace']['TreeDB/caching/db.go'].startswith('arms/B/'))
        self.assertTrue(m['C']['Replace']['TreeDB/caching/db.go'].startswith('arms/C/'))
        self.assertIn("GOENV='off', GOWORK='off', GOTELEMETRY='off'",SOURCE)
        self.assertIn("GOCACHE=str(root/'gocache'), GOMODCACHE=str(root/'modcache')",SOURCE)

    def test_event_checker_positive_and_adverse_data(self):
        import tempfile
        source=(ROOT/'.github/scripts/primary_new_arm_control_events.py').read_text()
        tree=ast.parse(source)
        namespace=dict(json=json,Path=Path)
        nodes=[n for n in tree.body if isinstance(n,(ast.Assign,ast.AugAssign,ast.FunctionDef))]
        exec(compile(ast.Module(body=nodes,type_ignores=[]),'<extracted-fixed-event-checker>','exec'),namespace)
        rows=[]
        for package,names in sorted(namespace['TOPS'].items()):
            rows.append(dict(Action='start',Package=package))
            for name in sorted(names):
                for test in [name]+[name+'/'+c for c in namespace['CHILDREN'].get(name,())]:
                    rows.extend([dict(Action='run',Package=package,Test=test),dict(Action='pass',Package=package,Test=test)])
            rows.append(dict(Action='pass',Package=package))
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'events.jsonl'
            def check(data):
                path.write_text(''.join(json.dumps(row)+'\n' for row in data))
                return namespace['validate'](path)
            self.assertEqual(check(rows)['outcomes'],8)
            cases=[rows[:-1],rows[1:-1],rows+rows,rows[1:],
                   [dict(Action='skip',Package=rows[0]['Package'])],
                   [dict(Action='fail',Package=rows[0]['Package'])],
                   [dict(Action='output',Package=rows[0]['Package'],Output='WARNING: DATA RACE')],
                   [dict(Action='run',Package=rows[0]['Package'],Test='foreign')]]
            # Missing start is not a required semantic predicate; remove a RUN decisively instead.
            cases[3]=[r for i,r in enumerate(rows) if i!=1]
            for index,data in enumerate(cases):
                with self.subTest(index=index),self.assertRaises(ValueError):check(data)



if __name__ == '__main__':
    unittest.main()
