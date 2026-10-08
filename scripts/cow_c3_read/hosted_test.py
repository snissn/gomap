"""Pure hosted route refusal tests; no Go, network or operational main."""
import copy
import datetime
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import hosted_contract as c
import hosted_collect as adapter
import hosted_runner as runner
from hosted_functional import functional_events
from protocol import digest, sha, validate_census_file


class HostedContracts(unittest.TestCase):
    def setUp(self):
        self.now=datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
        self.state=dict(policy=copy.deepcopy(c.POLICY),baseline=c.BASELINE,candidate=c.CANDIDATE,
            repository='snissn/gomap',root_actor_id=1981537,dispatch_actor_id=1981537,
            workflow_sha='a'*40,run_id=123,run_attempt=1,attempt='owned-1',
            window_start_utc=(self.now-datetime.timedelta(seconds=300)).isoformat(),
            window_end_utc=(self.now+datetime.timedelta(seconds=19500)).isoformat(),
            admission_deadline_monotonic=19800)
        self.construction=dict(status='CLOSED_PASS',manifest_sha256='b'*64,config_sha256='c'*64,
            closed_utc=(self.now-datetime.timedelta(seconds=10)).isoformat())
        self.value=dict(repository='snissn/gomap',workflow_sha='a'*40,run_id=123,run_attempt=1,
            dispatch_actor_id=1981537,attempt='owned-1',construction_manifest_sha256='b'*64,
            config_sha256='c'*64,policy_sha256=digest(c.POLICY),baseline=c.BASELINE,candidate=c.CANDIDATE,
            verdict='ACCEPT_ACTUAL_HOSTED_C3_CONSTRUCTION',findings=[],reader_release='RELEASED',
            all_source_and_artifact_readers_released=True,accepted_utc=self.now.isoformat(),
            end_utc=(self.now+datetime.timedelta(seconds=19000)).isoformat())
        stamp=self.now.strftime('%Y-%m-%dT%H:%M:%SZ')
        self.comment=dict(user={'id':1981537},created_at=stamp,updated_at=stamp,body=json.dumps(self.value))
    def accept(self,changes=None):
        comment=copy.deepcopy(self.comment);value=copy.deepcopy(self.value)
        if changes:value.update(changes)
        comment['body']=json.dumps(value)
        return c.root_acceptance(comment,self.state,self.construction,self.now)
    def refuse(self,fn):
        with self.assertRaises((ValueError,KeyError,TypeError)):fn()
    def test_actual_capacity_boundary(self):
        value=dict(system='Linux',machine='x86_64',cpu_count=4,affinity=[0,1,2,3],free_bytes=20<<30)
        c.capacity(value)
        for key,bad in [('free_bytes',(20<<30)-1),('cpu_count',3),('cpu_count',True),
                        ('affinity',[0,1,2]),('affinity',[0,1,2,2]),('machine','aarch64')]:
            self.refuse(lambda k=key,v=bad:c.capacity(dict(value,**{k:v})))
    def test_valid_root_receipt(self):
        self.assertEqual(self.accept(),self.value)
    def test_wrong_run_attempt_dispatch_and_source(self):
        for key,value in [('run_id',124),('run_attempt',2),('attempt','spent'),
            ('dispatch_actor_id',1),('workflow_sha','d'*40),('config_sha256','d'*64),
            ('construction_manifest_sha256','d'*64),('candidate',c.BASELINE),('policy_sha256','d'*64)]:
            self.refuse(lambda k=key,v=value:self.accept({k:v}))
    def test_bool_is_not_authority_integer(self):
        for key in ('run_id','run_attempt','dispatch_actor_id'):
            self.refuse(lambda k=key:self.accept({k:True}))
        comment=copy.deepcopy(self.comment);comment['user']['id']=True
        self.refuse(lambda:c.root_acceptance(comment,self.state,self.construction,self.now))
    def test_edited_or_foreign_comment(self):
        for changes in ({'updated_at':'2020-01-01T00:00:00Z'},{'user':{'id':1}}):
            self.refuse(lambda v=changes:c.root_acceptance(dict(self.comment,**v),self.state,self.construction,self.now))
    def test_duplicate_json_and_nonfinite(self):
        self.refuse(lambda:c.strict_json('{"run_id":1,"run_id":2}'))
        self.refuse(lambda:c.strict_json('{"run_id":NaN}'))
        comment=dict(self.comment,body=self.comment['body'][:-1]+',"run_id":123}')
        self.refuse(lambda:c.root_acceptance(comment,self.state,self.construction,self.now))
    def test_root_timestamps_and_finite_remaining(self):
        for key,value in [('accepted_utc',(self.now-datetime.timedelta(seconds=11)).isoformat()),
            ('end_utc',(self.now+datetime.timedelta(seconds=9599)).isoformat()),
            ('end_utc',(self.now+datetime.timedelta(seconds=19501)).isoformat()),
            ('accepted_utc','2026-10-08T00:00:00')]:
            self.refuse(lambda k=key,v=value:self.accept({k:v}))
        stamp=(self.now-datetime.timedelta(seconds=11)).strftime('%Y-%m-%dT%H:%M:%SZ')
        self.refuse(lambda:c.root_acceptance(dict(self.comment,created_at=stamp,updated_at=stamp),self.state,self.construction,self.now))
    def test_reader_release_and_findings(self):
        for changes in ({'findings':['blocked']},{'reader_release':'HELD'},
                        {'all_source_and_artifact_readers_released':False}):
            self.refuse(lambda v=changes:self.accept(v))
    def test_exact_policy_and_products(self):
        c.admission(self.state,9600,clock=lambda:300)
        for key,value in [('candidate',c.BASELINE),('policy',dict(c.POLICY,processes=755))]:
            self.refuse(lambda k=key,v=value:c.admission(dict(self.state,**{k:v}),clock=lambda:300))
    def test_remaining_window_and_deadline(self):
        self.refuse(lambda:c.admission(self.state,9600,clock=lambda:10201))
        self.refuse(lambda:c.admission(self.state,clock=lambda:19801))
    def test_short_root_window_is_used_per_child(self):
        root=dict(self.value,end_utc=(self.now+datetime.timedelta(seconds=9601)).isoformat())
        shortened=c.accepted_window(self.state,root)
        self.assertEqual(shortened['admission_deadline_monotonic'],9901)
        c.admission(shortened,clock=lambda:9900)
        self.refuse(lambda:c.admission(shortened,clock=lambda:9902))
    def test_deadline_refuses_without_spawn_or_signals(self):
        with tempfile.TemporaryDirectory() as d:
            original=mock.Mock();records=[]
            fn=adapter.guarded_child(original,self.state,records,Path(d)/'ledger.json',clock=lambda:19801)
            self.refuse(lambda:fn(['unchanged-binary']))
            original.assert_not_called();self.assertEqual(records[0]['decision'],'REFUSED')
    def test_admitted_child_finishes_after_deadline(self):
        with tempfile.TemporaryDirectory() as d:
            # Clock crosses the cutoff only inside the owned call. Adapter does
            # not run a timer or invoke signals, and returns the original result.
            clock=mock.Mock(return_value=19799)
            def owned(*args,**kwargs):clock.return_value=19801;return 'naturally joined'
            records=[];fn=adapter.guarded_child(owned,self.state,records,Path(d)/'ledger.json',clock=clock)
            self.assertEqual(fn(['unchanged-binary']),'naturally joined')
            self.assertEqual(records[0]['decision'],'ADMITTED_BEFORE_CHILD')
    def test_sealed_originals_rechecked_with_append_allowed(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d).resolve();(root/'raw').write_bytes(b'actual stream')
            c.seal(root,root/'manifest.json');manifest=json.loads((root/'manifest.json').read_text())
            (root/'appended-acceptance.json').write_text('{}');c.verify_seal(root,manifest)
            (root/'raw').write_bytes(b'changed stream');self.refuse(lambda:c.verify_seal(root,manifest))
    def test_sealed_mode_size_missing_and_symlink(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d).resolve();raw=root/'raw';raw.write_text('x');raw.chmod(0o644)
            c.seal(root,root/'manifest.json');manifest=json.loads((root/'manifest.json').read_text())
            raw.chmod(0o600);self.refuse(lambda:c.verify_seal(root,manifest));raw.chmod(0o644)
            raw.unlink();self.refuse(lambda:c.verify_seal(root,manifest))
            raw.symlink_to(root/'manifest.json');self.refuse(lambda:c.verify_seal(root,manifest))
    def test_archive_digest_rechecked(self):
        with tempfile.TemporaryDirectory() as d:
            n=Path(d).resolve();e=n/'evidence';e.mkdir();(e/'raw').write_text('raw')
            c.seal(e,e/'construction-manifest.json');(n/'construction.tar.gz').write_bytes(b'archive')
            result=dict(self.construction,manifest_sha256=sha(e/'construction-manifest.json'),archive_sha256=sha(n/'construction.tar.gz'))
            state=dict(self.state,namespace=str(n));runner.verify_construction(state,result)
            (n/'construction.tar.gz').write_bytes(b'drift');self.refuse(lambda:runner.verify_construction(state,result))
    def test_full_inventory_symlink_dirs_and_specials(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d).resolve();(root/'regular').write_text('x');c.inventory(root)
            (root/'linked-dir').symlink_to(root,target_is_directory=True)
            self.refuse(lambda:c.inventory(root))
    def test_compiler_declared_three_modes_bytes(self):
        with tempfile.TemporaryDirectory() as d:
            paths=[]
            for name in ('gcc','as','ld'):
                path=Path(d).resolve()/name;path.write_text(name);path.chmod(0o755);paths.append(path)
            expected=c.snapshot_files(paths);c.verify_snapshot(c.snapshot_files(paths),expected,'compiler')
            paths[0].write_text('drift');self.refuse(lambda:c.verify_snapshot(c.snapshot_files(paths),expected,'compiler'))
            paths[0].chmod(0o644);self.refuse(lambda:c.snapshot_files(paths))
    def test_missing_duplicate_failed_unjoined_commands(self):
        row=dict(label='one',started_child=True,child_joined=True,custody_released=True,
            wait_status=0,exit_code=0,timed_out=False,signals=[])
        c.released([row],['one'])
        for rows in ([],[row,row],[dict(row,child_joined=False)],[dict(row,exit_code=1)],
                     [dict(row,timed_out=True)],[dict(row,signals=[2])]):
            self.refuse(lambda v=rows:c.released(v,['one']))
    def test_actual_foreign_census_grammar(self):
        with tempfile.TemporaryDirectory() as d:
            file=Path(d)/'ps'
            file.write_text('12 1 1 0.0 0 Z 12 go\n')
            validate_census_file(file,sha(file),benchmark_names=['cowbench-normal'])
            for name in ('go','compile','link','integration.tes','cowbench-normal'):
                file.write_text('12 1 1 0.0 1 S 12 '+name+'\n')
                self.refuse(lambda:validate_census_file(file,sha(file),benchmark_names=['cowbench-normal']))
    def test_functional_actual_grammar_16_and_build_diagnostics(self):
        events=self.events();parsed=functional_events('\n'.join(map(json.dumps,events)),c.TESTS,'pkg')
        self.assertEqual(parsed['top_level_tests'],16);self.assertEqual(parsed['build_output_events'],1)
    def events(self):
        events=[dict(Time='2026-10-08T00:00:00.123456789Z',Action='start',Package='pkg')]
        for name in c.TESTS:
            events += [dict(Time='2026-10-08T00:00:00.12345678-10:00',Action='run',Package='pkg',Test=name),
                       dict(Time='2026-10-08T00:00:00Z',Action='pass',Package='pkg',Test=name)]
        events += [dict(Action='build-output',ImportPath='',Output='retained compiler diagnostic'),
                   dict(Time='2026-10-08T00:00:00Z',Action='pass',Package='pkg')]
        return events
    def test_functional_adverse_schema_and_test_lifecycle(self):
        for change in ('skip','fail','build-fail','wrong-package','missing','duplicate'):
            events=self.events()
            if change in ('skip','fail','build-fail'):events[-1]['Action']=change
            elif change=='wrong-package':events[1]['Package']='other'
            elif change=='missing':del events[1:3]
            else:events.insert(-1,copy.deepcopy(events[2]))
            self.refuse(lambda:functional_events('\n'.join(map(json.dumps,events)),c.TESTS,'pkg'))
    def test_executing_tooling_git_ancestry_and_bytes(self):
        import base64
        from protocol import git_object_id
        with tempfile.TemporaryDirectory() as d:
            root=Path(d).resolve();checkout=root/'checkout';checkout.mkdir();out=root/'proof';out.mkdir()
            names=['scripts/cow_c3_read/'+x for x in c.CANONICAL]
            names+=['scripts/cow_c3_read/hosted_runner.py','.github/workflows/cow-c3-hosted-qualification.yml']
            nodes={};blobs={}
            for name in names:
                path=checkout/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_text('immutable '+name);path.chmod(0o644)
                blobs[name]=git_object_id('blob',path.read_bytes(),'sha1')
            def tree(prefix=''):
                entries={}
                for name in names:
                    if name.startswith(prefix):
                        tail=name[len(prefix):];part=tail.split('/')[0]
                        entries[part]='tree' if '/' in tail else 'blob'
                raw=b''
                for part,kind in sorted(entries.items()):
                    oid=tree(prefix+part+'/') if kind=='tree' else blobs[prefix+part]
                    raw+=(b'40000' if kind=='tree' else b'100644')+b' '+part.encode()+b'\0'+bytes.fromhex(oid)
                oid=git_object_id('tree',raw,'sha1');nodes[oid]=raw;return oid
            root_oid=tree();commit=('tree '+root_oid+'\n\nfixture\n').encode()
            state=dict(self.state,workflow_sha=git_object_id('commit',commit,'sha1'),
                canonical={x:'unused' for x in c.CANONICAL},helpers={'hosted_runner.py':{}})
            listing=b''.join(b'040000 tree '+oid.encode()+b'\ttree'+str(i).encode()+b'\0' for i,oid in enumerate(nodes))
            def mocked_run(state,label,argv,cwd,timeout,directory,sticky,env,**kw):
                if label=='tooling-commit':raw=commit
                elif label=='tooling-trees':raw=listing
                else:
                    requested=kw['input_data'].decode().splitlines()
                    raw=b''.join(oid.encode()+b' tree '+str(len(nodes[oid])).encode()+b'\n'+nodes[oid]+b'\n' for oid in requested)
                (directory/(label+'.stdout')).write_bytes(raw)
            with mock.patch.object(runner,'run',side_effect=mocked_run):
                runner.tooling_authority(state,checkout,out,None,{})
                proof=json.loads((out/'tooling-git-proof.json').read_text())
                self.assertEqual(set(proof['files']),set(names))
                self.assertEqual(state['tooling_proof_sha256'],sha(out/'tooling-git-proof.json'))
                (checkout/names[0]).write_text('runtime drift')
                self.refuse(lambda:runner.tooling_authority(state,checkout,out,None,{}))
    def test_no_ambient_token_in_child_environment(self):
        from protocol import process_environment
        controls=dict(GOROOT='/owned/go',GOCACHE='/owned/cache',GOMODCACHE='/owned/gopath/pkg/mod',
            TMPDIR='/owned/tmp',GOWORK='off',GOMAXPROCS='4',GOGC='100',GOMEMLIMIT='off',GOFLAGS='')
        with mock.patch.dict('os.environ',{'HOSTED_READ_TOKEN':'not captured','GITHUB_TOKEN':'not captured'}):
            environment=process_environment(controls)
        self.assertEqual(len(environment),17)
        self.assertNotIn('HOSTED_READ_TOKEN',environment);self.assertNotIn('GITHUB_TOKEN',environment)

    def test_final_refresh_cancellation_refuses_pass_but_preserves_joins(self):
        # Reproduce the independent R1 AST seam without starting any child/main.
        import ast, types
        node=next(n for n in ast.parse(Path(runner.__file__).read_text()).body
                  if isinstance(n,ast.FunctionDef) and n.name=='matched')
        for signal in (None,2,15,3):
            with self.subTest(signal=signal),tempfile.TemporaryDirectory() as d:
                n=Path(d).resolve();(n/'evidence').mkdir()
                (n/'root-acceptance.json').write_text('{"api_comment":{}}')
                (n/'construction-result.json').write_text('{}')
                state=dict(namespace=str(n),outer_deadline_monotonic=999999999,
                    variants={'baseline':{'source':str(n)}},controls={},run_id=1,run_attempt=1,attempt='test')
                sticky=types.SimpleNamespace(signals=[]);refreshes=[];closures={}
                class Sticky:
                    def __enter__(self):return sticky
                    def __exit__(self,*a):pass
                def refresh(state,observations=None):
                    refreshes.append(observations)
                    if len(refreshes)==2 and signal is not None:sticky.signals.append({'signal':signal,'at':0})
                def output(path,value):
                    closures[Path(path).name]=value;Path(path).write_text(json.dumps(value))
                def supervisor(*a,**k):
                    result={'custody_released':True};output(n/'evidence/matched-driver/collect.json',result);return result
                def analysis(*a,**k):output(n/'evidence/matched-driver/offline-analysis.json',{'custody_released':True})
                def need(value,message):
                    if not value:raise ValueError(message)
                env=dict(state_read=lambda a:state,Path=Path,admission=lambda *a:None,
                    json_read=lambda p:json.loads(Path(p).read_text()),root_acceptance=lambda *a:{},
                    verify_construction=lambda *a:None,accepted_window=lambda s,r:s,full_refresh=refresh,
                    durable=output,sha=lambda p:'hash',now=lambda:'now',host_admit=lambda *a:None,
                    StickySignals=Sticky,time=types.SimpleNamespace(monotonic=lambda:0),
                    sys=types.SimpleNamespace(executable='python'),__file__=runner.__file__,supervise=supervisor,
                    process_environment=lambda c:{},monitored_release=lambda p:True,
                    clean_command=lambda r:None,run=analysis,write=output,need=need)
                exec(compile(ast.Module(body=[node],type_ignores=[]),runner.__file__,'exec'),env)
                invoke=lambda:env['matched'](types.SimpleNamespace(state=n/'state.json',state_sha256='hash'))
                if signal is None:invoke()
                else:self.refuse(invoke)
                closure=closures['closure.json']
                self.assertEqual(closure['status'],'CLOSED_PASS' if signal is None else 'CLOSED_FAILURE')
                self.assertTrue(closure['all_owned_children_joined'])
                self.assertTrue(closure['source_toolchain_compiler_unchanged'])
                self.assertEqual(closure['signals'],sticky.signals)
                self.assertEqual(refreshes,[n/'evidence/matched-source-before',n/'evidence/matched-source-final'])

    def test_refresh_retains_actual_inventory_before_drift_refusal(self):
        with tempfile.TemporaryDirectory() as d:
            n=Path(d).resolve();root=n/'evidence/bootstrap';root.mkdir(parents=True)
            proof=root/'tooling-git-proof.json';proof.write_text('{"files":{}}')
            go=n/'go';go.mkdir();file=go/'go';file.write_text('original Go bytes')
            compilers=[]
            for name in ('gcc','as','ld'):
                p=n/name;p.write_text('original '+name);p.chmod(0o755);compilers.append(p)
            state=dict(namespace=str(n),tooling_proof_sha256=sha(proof),git_repository=str(n),variants={},
                controls={'GOROOT':str(go)},full_toolchain=c.inventory(go),compiler_files=c.snapshot_files(compilers))
            first=n/'before';runner.full_refresh(state,first)
            self.assertEqual(json.loads((first/'full-toolchain-observed.json').read_text()),state['full_toolchain'])
            self.assertEqual(json.loads((first/'declared-compiler-observed.json').read_text()),state['compiler_files'])
            for name in ('Go','compiler'):
                with self.subTest(drift=name):
                    target=file if name=='Go' else compilers[0]
                    original=target.read_text();target.write_text('actual damaged '+name)
                    observed=n/('damaged-'+name)
                    with mock.patch.object(runner,'inventory',wraps=c.inventory) as full, \
                         mock.patch.object(runner,'observe_files',wraps=c.observe_files) as compiler:
                        self.refuse(lambda:runner.full_refresh(state,observed))
                        self.assertEqual(full.call_count,1);self.assertEqual(compiler.call_count,1)
                    self.assertEqual(json.loads((observed/'full-toolchain-observed.json').read_text()),c.inventory(go))
                    self.assertEqual(json.loads((observed/'declared-compiler-observed.json').read_text()),c.snapshot_files(compilers))
                    if name=='Go':self.assertNotEqual(c.inventory(go),state['full_toolchain'])
                    else:self.assertNotEqual(c.snapshot_files(compilers),state['compiler_files'])
                    target.write_text(original)

    def test_compiler_mode_and_nonregular_drift_retained_before_refusal(self):
        import os
        for drift in ('mode','missing','symlink','special'):
            with self.subTest(drift=drift),tempfile.TemporaryDirectory() as d:
                n=Path(d).resolve();root=n/'evidence/bootstrap';root.mkdir(parents=True)
                proof=root/'tooling-git-proof.json';proof.write_text('{"files":{}}')
                go=n/'go';go.mkdir();(go/'go').write_text('Go')
                files=[]
                for name in ('gcc','as','ld'):
                    p=n/name;p.write_text(name);p.chmod(0o755);files.append(p)
                state=dict(namespace=str(n),tooling_proof_sha256=sha(proof),git_repository=str(n),variants={},
                    controls={'GOROOT':str(go)},full_toolchain=c.inventory(go),compiler_files=c.snapshot_files(files))
                target=files[0];expected_sha=sha(target)
                if drift=='mode':target.chmod(0o644)
                else:
                    target.unlink()
                    if drift=='symlink':target.symlink_to(files[1])
                    elif drift=='special':os.mkfifo(target)
                observed=n/'observed'
                with mock.patch.object(c.os,'open',wraps=os.open) as opened:
                    self.refuse(lambda:runner.full_refresh(state,observed))
                    if drift!='mode':self.assertNotIn(target,[call.args[0] for call in opened.call_args_list])
                actual=json.loads((observed/'declared-compiler-observed.json').read_text())
                errors=json.loads((observed/'declared-compiler-observation-errors.json').read_text())
                self.assertEqual(json.loads((observed/'full-toolchain-observed.json').read_text()),state['full_toolchain'])
                if drift=='mode':
                    self.assertEqual(actual[str(target)],dict(sha256=expected_sha,bytes=3,mode=0o644))
                    self.assertEqual(errors,[])
                else:
                    self.assertNotIn(str(target),actual);self.assertEqual(len(errors),1)
                    self.assertEqual(errors[0]['requested_path'],str(target))
                    self.assertEqual(errors[0]['type'],{'missing':'MISSING','symlink':'SYMLINK','special':'NONREGULAR'}[drift])
                    self.assertTrue(errors[0]['error']);self.assertTrue(errors[0]['error_type'])
                self.refuse(lambda:c.snapshot_files(files))

    def test_real_git_archive_modes_use_actual_prepare_export_command(self):
        import ast, os, subprocess, tarfile
        with tempfile.TemporaryDirectory() as d:
            root=Path(d).resolve();repo=root/'repo';repo.mkdir()
            env=dict(PATH=os.defpath,LC_ALL='C',GIT_CONFIG_NOSYSTEM='1',GIT_CONFIG_GLOBAL=os.devnull)
            def git(*argv):
                result=subprocess.run(['git','--no-replace-objects','-C',str(repo),*argv],
                    env=env,capture_output=True,timeout=10,check=True)
                return result.stdout
            git('init');git('config','user.name','hosted fixture');git('config','user.email','hosted-fixture@example.invalid')
            (repo/'ordinary').write_text('ordinary bytes');(repo/'ordinary').chmod(0o644)
            (repo/'executable').write_text('executable bytes');(repo/'executable').chmod(0o755)
            git('add','ordinary','executable');git('commit','-m','mode fixture')
            head=git('rev-parse','HEAD').decode().strip()
            default=root/'default.tar';default.write_bytes(git('archive','--format=tar',head))
            with tarfile.open(default) as tar:
                self.assertEqual({m.name:m.mode for m in tar},{'ordinary':0o664,'executable':0o775})
            self.refuse(lambda:runner.safe_extract(default,root/'default-refused'))
            # Evaluate the actual prepare run argv AST; avoid a duplicated test-only command.
            node=next(n for n in ast.parse(Path(runner.__file__).read_text()).body
                      if isinstance(n,ast.FunctionDef) and n.name=='prepare')
            call=next(n for n in ast.walk(node) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name)
                      and n.func.id=='run' and isinstance(n.args[1],ast.BinOp)
                      and isinstance(n.args[1].right,ast.Constant) and n.args[1].right.value=='-export')
            argv=eval(compile(ast.Expression(call.args[2]),runner.__file__,'eval'),
                dict(args=type('Args',(),{'checkout':repo})(),v={'head':head},str=str))
            normalized=root/'normalized.tar'
            normalized.write_bytes(subprocess.run(argv,env=env,capture_output=True,timeout=10,check=True).stdout)
            with tarfile.open(normalized) as tar:
                self.assertEqual({m.name:m.mode for m in tar},{'ordinary':0o644,'executable':0o755})
            destination=root/'normalized';runner.safe_extract(normalized,destination)
            self.assertEqual((destination/'ordinary').read_text(),'ordinary bytes')
            self.assertEqual((destination/'executable').read_text(),'executable bytes')
            self.assertEqual((destination/'ordinary').stat().st_mode & 0o777,0o644)
            self.assertEqual((destination/'executable').stat().st_mode & 0o777,0o755)

    def test_actual_canonical_generator_freeze_count(self):
        import ast, inspect
        from protocol import schedule
        configuration=runner.draft()
        actual=schedule(configuration);self.assertTrue(inspect.isgenerator(actual))
        items=list(actual);self.assertEqual(len(items),756)
        self.assertEqual(sum(x['phase']=='warmup' for x in items),108)
        self.assertEqual(sum(x['phase']=='measured' for x in items),648)
        node=next(n for n in ast.parse(Path(runner.__file__).read_text()).body
                  if isinstance(n,ast.FunctionDef) and n.name=='freeze')
        check=next(n for n in ast.walk(node) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name)
            and n.func.id=='need' and len(n.args)>1 and isinstance(n.args[1],ast.Constant)
            and n.args[1].value=='original full schedule required')
        def need(value,message):
            if not value:raise ValueError(message)
        expression=compile(ast.Expression(check),runner.__file__,'eval')
        eval(expression,dict(c=configuration,schedule=schedule,need=need))
        changed=copy.deepcopy(configuration);changed['cases'].pop()
        self.refuse(lambda:eval(expression,dict(c=changed,schedule=schedule,need=need)))
        changed=copy.deepcopy(configuration);changed['order'].pop()
        self.refuse(lambda:eval(expression,dict(c=changed,schedule=schedule,need=need)))

    def test_workflow_origin_exports_effective_namespace_before_checkout(self):
        import os, textwrap
        workflow=Path(runner.__file__).parents[2]/'.github/workflows/cow-c3-hosted-qualification.yml'
        text=workflow.read_text();job_env=text.split('    env:',1)[1].split('    steps:',1)[0]
        self.assertNotIn('runner.temp',job_env);self.assertNotIn('HOSTED_NAMESPACE',job_env)
        code=text.split("          python3 -B - <<'PY'\n",1)[1].split('          PY',1)[0]
        code=textwrap.dedent(code)
        with tempfile.TemporaryDirectory() as d:
            root=Path(d).resolve();environment=root/'github-env';output=root/'github-output'
            with mock.patch.dict(os.environ,dict(RUNNER_TEMP=str(root),GITHUB_RUN_ID='123',
                    GITHUB_RUN_ATTEMPT='2',GITHUB_ENV=str(environment),GITHUB_OUTPUT=str(output))):
                exec(compile(code,str(workflow),'exec'),{})
            self.assertEqual(environment.read_text(),'HOSTED_NAMESPACE='+str(root/'cow-c3-123-2')+'\n')
            values=dict(line.split('=',1) for line in output.read_text().splitlines())
            self.assertEqual(set(values),{'utc','monotonic'});c.utc(values['utc']);float(values['monotonic'])
            environment.unlink();output.unlink()
            with mock.patch.dict(os.environ,dict(RUNNER_TEMP=str(root),GITHUB_RUN_ID='123\ninjected',
                    GITHUB_RUN_ATTEMPT='2',GITHUB_ENV=str(environment),GITHUB_OUTPUT=str(output))):
                self.refuse(lambda:exec(compile(code,str(workflow),'exec'),{}))
            self.assertFalse(environment.exists());self.assertFalse(output.exists())

    def test_exact_numeric_and_canonical_adapter_source(self):
        self.assertEqual((c.POLICY['cells'],c.POLICY['processes'],c.POLICY['warmups'],c.POLICY['measured']),(54,756,108,648))
        self.assertEqual((c.POLICY['leaf_seconds'],c.POLICY['readiness_total_seconds']), (300,600))
        self.assertEqual((c.POLICY['noise'],c.POLICY['adverse'],c.POLICY['benefit']),(.30,.05,.10))
        self.assertEqual(c.POLICY['exclusions'],[])
        self.assertEqual(len(runner.LABELS),12)
        self.assertEqual(c.POLICY['construction_remaining_seconds'],10200)
        self.assertEqual(c.POLICY['matched_remaining_seconds'],9600)


if __name__=='__main__':unittest.main()
