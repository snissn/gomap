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
    context = [n for n in TREE.body if isinstance(n, ast.Assign) and
               any(isinstance(target, ast.Name) and target.id.startswith('RESOLVER_') for target in n.targets)]
    nodes = context + [FUNCTIONS[name] for name in names]
    namespace = dict(os=os, json=json, base64=base64, re=re, Path=Path, stat=stat, hashlib=hashlib, HARNESS=ROOT, DATA=ROOT/'.github/primary-construction',
                     BRANCH='refs/heads/diagnostic/primary-8e-b24-resolver-role-01',
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
                 controls=dict(tops=4,outcomes=8,leaves=7,modes=['normal','race']),
                 resolver_configuration_policy=ns['RESOLVER_POLICY_BINDING'])
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
                            ('controls',dict(tops=6,outcomes=26,leaves=24,modes=['normal','race'])),('arms',{}),
                            ('resolver_configuration_policy',{}),('resolver_configuration_policy',None)]:
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
        self.assertLess(main.index('readonly = qualified_runtime_inputs('), main.index("if args.mode == 'worker'"))
        self.assertIn("tool_identity(row['path'], row)", ast.get_source_segment(SOURCE,FUNCTIONS['qualified_runtime_inputs']))
        worker = ast.get_source_segment(SOURCE, FUNCTIONS['worker'])
        self.assertNotIn('readonly = runtime_inputs(a)', worker)
        self.assertIn('resolver_record() == readonly[11]', worker)
        self.assertIn("tool_identity(row['path'], row)", worker)
        self.assertIn('os.O_NOFOLLOW', ast.get_source_segment(SOURCE, FUNCTIONS['runtime_inputs']))

    def test_resolver_role_needs_new_exact_root_policy(self):
        ns = isolated(('need', 'sha', 'read', 'resolver_policy'))
        binding = ns['RESOLVER_POLICY_BINDING']
        self.assertIsNone(ns['resolver_policy']({'resolver_configuration_policy': binding}))
        for value in ({}, {'resolver_configuration_policy': {}},
                      {'resolver_configuration_policy': dict(binding, sha256='0'*64)},
                      {'resolver_configuration_policy': dict(binding, decision_sha256='0'*64)}):
            with self.subTest(value=value), self.assertRaises(RuntimeError):
                ns['resolver_policy'](value)
        policy = json.loads((ROOT/binding['path']).read_text())
        self.assertEqual(policy['logical_path'], '/etc/resolv.conf')
        self.assertEqual(policy['resolved_target'], ns['RESOLVER_EXPECTED']['path'])
        self.assertEqual(policy['authority_thread'], ns['ROOT_THREAD'])

    def test_complete_qualified_rows_keep_every_other_record(self):
        ns = isolated(('need', 'validate_qualified_rows'))
        original = json.loads((ROOT/'.github/primary-construction/readonly-runtime-inputs.json').read_text())
        qualified = json.loads(json.dumps(original))
        qualified[11].update(bytes=1024, sha256='a'*64)
        self.assertIsNone(ns['validate_qualified_rows'](original, qualified))
        for index in (0, 10, 12, 267):
            value = json.loads(json.dumps(qualified)); value[index]['sha256'] = '0'*64
            with self.subTest(index=index), self.assertRaises(RuntimeError):
                ns['validate_qualified_rows'](original, value)
        changes = [qualified[:-1], qualified+qualified[:1]]
        for key,value in [('path','/different'), ('bytes',65537), ('bytes',-1),
                          ('bytes',True), ('sha256','bad')]:
            changed = json.loads(json.dumps(qualified)); changed[11][key] = value; changes.append(changed)
        changed = json.loads(json.dumps(qualified)); changed[11]['extra'] = 'no'; changes.append(changed)
        changed = json.loads(json.dumps(qualified)); changed[12] = changed[11]; changes.append(changed)
        for value in changes:
            with self.subTest(value=value[11] if len(value)>11 else None), self.assertRaises(RuntimeError):
                ns['validate_qualified_rows'](original, value)

    def test_stable_resolver_record_and_refusals_without_host_reads(self):
        import io
        from types import SimpleNamespace
        ns = isolated(('need', 'resolver_record'))
        target = ns['RESOLVER_EXPECTED']['path']
        data = b'nameserver 127.0.0.53\n'
        metadata = dict(st_dev=1, st_ino=2, st_mode=stat.S_IFREG|0o644, st_nlink=1,
                        st_size=len(data), st_mtime_ns=3, st_ctime_ns=4)
        closed = []
        fixture = dict(metadata=metadata, after=None, current=None, data=data, target=target)
        class FakePath:
            def __init__(self, value): self.value = value
            def resolve(self, strict=True): return FakePath(fixture['target'])
            def lstat(self): return SimpleNamespace(**(fixture['current'] or fixture['metadata']))
            def __eq__(self, other): return isinstance(other, FakePath) and self.value == other.value
            def __str__(self): return self.value
        def fstat(fd):
            count[0] += 1
            return SimpleNamespace(**((fixture['after'] or fixture['metadata']) if count[0]>1 else fixture['metadata']))
        class Reader(io.BytesIO):
            def __exit__(self, *args): self.close()
        ns.update(Path=FakePath, os=SimpleNamespace(O_RDONLY=0,O_NOFOLLOW=1,O_NONBLOCK=2,
            open=lambda *args:7, fstat=fstat, close=lambda fd:closed.append(fd),
            fdopen=lambda *args,**kwargs:Reader(fixture['data'])))
        def invoke():
            count[0]=0
            return ns['resolver_record']()
        count=[0]
        self.assertEqual(invoke(), dict(path=target,bytes=len(data),sha256=hashlib.sha256(data).hexdigest()))
        for key,value in [('st_mode',stat.S_IFREG|0o755),('st_mode',stat.S_IFDIR|0o644),
                          ('st_nlink',2),('st_size',65537)]:
            fixture['metadata'] = dict(metadata, **{key:value})
            with self.subTest(key=key), self.assertRaises(RuntimeError): invoke()
        fixture['metadata']=metadata
        for change in ('after','current'):
            fixture[change] = dict(metadata,st_ino=9)
            with self.subTest(change=change), self.assertRaises(RuntimeError): invoke()
            fixture[change]=None
        fixture['target']='/different'
        with self.assertRaises(RuntimeError): invoke()
        fixture['target']=target;fixture['data']=data+b'extra'
        with self.assertRaises(RuntimeError): invoke()
        self.assertTrue(closed)
        fixture['data']=data
        def close_failure(fd): raise OSError('synthetic checked-close failure')
        ns['os'].close=close_failure
        with self.assertRaisesRegex(OSError,'synthetic checked-close failure'): invoke()

    def test_qualified_receipt_order_and_no_worker_refresh(self):
        qualified = ast.get_source_segment(SOURCE, FUNCTIONS['qualified_runtime_inputs'])
        main = ast.get_source_segment(SOURCE, FUNCTIONS['main'])
        worker = ast.get_source_segment(SOURCE, FUNCTIONS['worker'])
        self.assertLess(qualified.index('resolver_policy(a)'),qualified.index('resolver_record()'))
        self.assertLess(qualified.index('tool_identity('),qualified.index('resolver_record()'))
        self.assertLess(qualified.index("snapshot['whole_bytes']+len(encoded) <= GIB"), qualified.index('write(path, receipt)'))
        self.assertLess(qualified.index('write(path, receipt)'), qualified.index("need(resolver_record() == qualified[11]"))
        self.assertIn("if mode == 'launch':",qualified)
        self.assertEqual(qualified.count('write(path, receipt)'),1)
        self.assertIn("receipt['job'] == job",qualified)
        self.assertIn("receipt['inventory'] == dict(bytes=len(inventory), rows=268",qualified)
        self.assertIn('os.O_NOFOLLOW | os.O_NONBLOCK',qualified)
        self.assertLess(main.index('authenticate(a)'),main.index('qualified_runtime_inputs('))
        self.assertLess(main.index('qualified_runtime_inputs('),main.index("if args.mode == 'worker':"))
        self.assertLess(worker.index('load_runner()'),worker.index('resolver_record() == readonly[11]'))
        self.assertLess(worker.index('resolver_record() == readonly[11]'),worker.index("run('go-context'"))
        self.assertNotIn('failure_context',worker)
        self.assertNotIn('failure_context',ast.get_source_segment(SOURCE,FUNCTIONS['publish']))

    def test_pure_same_job_capture_worker_revalidation_and_failure_bounds(self):
        import io
        from types import SimpleNamespace
        ns = isolated(('need', 'canonical', 'validate_qualified_rows', 'qualified_runtime_inputs'))
        original = json.loads((ROOT/'.github/primary-construction/readonly-runtime-inputs.json').read_text())
        selected = dict(original[11],bytes=1000,sha256='b'*64)
        state = dict(record=selected,receipt=None,whole_bytes=0,errors=[],writes=0)
        job=dict(GITHUB_SHA='1'*40,GITHUB_RUN_ID='123',GITHUB_RUN_ATTEMPT='1',GITHUB_JOB='diagnostic')
        binding=dict(path='.github/primary-construction/readonly-runtime-inputs.json',bytes=39082,sha256='c'*64,rows=268)
        admission=dict(readonly_runtime_manifest=binding,resolver_configuration_policy=ns['RESOLVER_POLICY_BINDING'])
        authenticated=[]
        class Reader(io.BytesIO):
            def __exit__(self,*args): self.close()
        def encoded(): return ns['canonical'](state['receipt'])+b'\n'
        def info(fd):
            return SimpleNamespace(st_mode=stat.S_IFREG|0o644,st_nlink=1,st_size=len(encoded()),
                                   st_dev=1,st_ino=2,st_mtime_ns=3,st_ctime_ns=4)
        def capture(path,value):
            state['writes']+=1
            state['receipt']=json.loads(json.dumps(value))
        ns.update(resolver_policy=lambda a:None,resolver_record=lambda:dict(state['record']),
                  tool_identity=lambda p,row:authenticated.append((p,dict(row))),
                  census=lambda root:dict(whole_bytes=state['whole_bytes'],errors=state['errors']),
                  write=capture,time=SimpleNamespace(monotonic=lambda:100),GIB=1024**3,WHOLE=840,
                  os=SimpleNamespace(environ=job,O_RDONLY=0,O_NOFOLLOW=1,O_NONBLOCK=2,
                      open=lambda *a:7,fstat=info,fdopen=lambda *a,**kw:Reader(encoded()),close=lambda fd:None))
        root=Path('/fixed-test-task')
        qualified=ns['qualified_runtime_inputs'](root,admission,original,'launch',0)
        self.assertEqual(qualified[11],selected)
        self.assertEqual(len(authenticated),267)
        self.assertEqual([row for p,row in authenticated],[row for i,row in enumerate(original) if i!=11])
        self.assertEqual(state['writes'],1)
        saved=json.loads(json.dumps(state['receipt']))
        self.assertEqual(saved['inventory']['bytes'],len(ns['canonical'](qualified))+1)
        self.assertEqual(saved['inventory']['sha256'],hashlib.sha256(ns['canonical'](qualified)+b'\n').hexdigest())
        self.assertEqual(ns['qualified_runtime_inputs'](root,admission,original,'worker',0),qualified)
        self.assertEqual(state['writes'],1)  # Worker never refreshes its bound snapshot.
        state['record']=dict(selected,sha256='d'*64)
        with self.assertRaisesRegex(RuntimeError,'qualified resolver identity differs'):
            ns['qualified_runtime_inputs'](root,admission,original,'worker',0)
        state['record']=selected
        for key,value in [('job',dict(job,GITHUB_RUN_ID='999')),('policy',{}),
                          ('inventory',dict(saved['inventory'],sha256='0'*64))]:
            state['receipt']=dict(saved,**{key:value})
            with self.subTest(key=key),self.assertRaises(RuntimeError):
                ns['qualified_runtime_inputs'](root,admission,original,'worker',0)
        changed=json.loads(json.dumps(saved));changed['rows'][267]['sha256']='0'*64
        state['receipt']=changed
        with self.assertRaisesRegex(RuntimeError,'all original runtime roles required'):
            ns['qualified_runtime_inputs'](root,admission,original,'worker',0)
        state['receipt']=saved;state['whole_bytes']=1024**3
        before=state['writes']
        with self.assertRaisesRegex(RuntimeError,'qualified runtime receipt reservation failed'):
            ns['qualified_runtime_inputs'](root,admission,original,'launch',0)
        self.assertEqual(state['writes'],before)
        state['whole_bytes']=0;state['errors']=['sticky failure']
        with self.assertRaises(RuntimeError):
            ns['qualified_runtime_inputs'](root,admission,original,'launch',0)
        self.assertEqual(state['writes'],before)
        state['errors']=[]
        snapshots=iter((selected,dict(selected,sha256='e'*64)))
        ns['resolver_record']=lambda:dict(next(snapshots))
        with self.assertRaisesRegex(RuntimeError,'qualified resolver identity differs'):
            ns['qualified_runtime_inputs'](root,admission,original,'launch',0)

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
