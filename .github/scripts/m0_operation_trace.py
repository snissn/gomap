#!/usr/bin/env python3
"""One fixed M0 iteration and checked standard parsed-trace binding."""
import argparse
import base64
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import select
import shutil
import stat
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
ROOT_THREAD = '01a18e7c-8c23-7fd2-8d55-b0486d938cd3'
OWNER_MANIFEST = '86f4ee083aba9e2f2dbdb87abf9ec5be9eb13d416e1bafd25f7b61139e317aa3'
OWNER_REVIEW = '863afc8d80a3f68f767b8be0f8f810f5ab646a5c1e256ea674b7894a97ef883a'
C = '4934bd6d51d9047ba0187d05c1d211cbe419811a'
C_TREE = '7f30f92674110a4544a381468677183ab3133406'
WHOLE_SECONDS = 1020
CONTROLS_PASS_SHA = '5fbc92847359e709fe9ef7812786ff469f9892dedf9e4c81d27a3183e67245c5'
CONTROLS_HEAD = 'ee6c3880e1b0ec6d7e490232f3a3de45643a455e'
CONTROLS_TREE = 'ffc790f080c04708d9363d375f9b380d7430350a'
MIB = 1024**2


def need(ok, message):
    if not ok:
        raise RuntimeError(message)


def sha(path):
    h=hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda:stream.read(1<<20),b''):
            h.update(block)
    return h.hexdigest()


def authority():
    need(os.environ.get('GITHUB_EVENT_NAME')=='workflow_dispatch' and
         os.environ.get('GITHUB_REPOSITORY')=='snissn/gomap' and
         os.environ.get('GITHUB_ACTOR_ID')=='1981537' and
         os.environ.get('GITHUB_RUN_ATTEMPT')=='1' and
         os.environ.get('GITHUB_REF')=='refs/heads/diagnostic/m0-operation-trace-01', 'fixed hosted authority')
    head=os.environ.get('REVIEWED_HEAD','')
    need(re.fullmatch('[0-9a-f]{40}',head) is not None and
         head==os.environ.get('GITHUB_SHA')==os.environ.get('WORKFLOW_COMMIT'), 'exact reviewed harness')
    encoded=os.environ['ROOT_ADMISSION_B64']
    need(len(encoded)<=16384,'bounded admission input')
    admission=json.loads(base64.b64decode(encoded,validate=True))
    need(isinstance(admission,dict),'admission object required')
    need(admission.get('authority_thread')==ROOT_THREAD,'admission immutable ROOT authority thread')
    required={'schema':'m0-operation-trace-admission-v1','status':'ADMITTED_ONE_M0_TRACE_ONLY',
              'authority':'ROOT','authority_thread':ROOT_THREAD,'actor_id':'1981537','expected_head':head,'run_attempt':'1',
              'independent_binding_review_head':head,'independent_binding_review_status':'READY_SOURCE_ONLY',
              'controls_pass_sha256':CONTROLS_PASS_SHA,
              'owner_manifest_sha256':OWNER_MANIFEST,'owner_review_sha256':OWNER_REVIEW,
              'allow_product_capture':True,'only_capture':'M0_bytes_1x_N1_4096'}
    need(all(admission.get(k)==v for k,v in required.items()),'ROOT one-M0 diagnostic admission')
    encoded_review=os.environ['M0_BINDING_REVIEW_B64']
    need(len(encoded_review)<=24000,'bounded encoded actual independent review')
    review_raw=base64.b64decode(encoded_review,validate=True)
    need(0<len(review_raw)<=16384,'bounded actual independent review bytes')
    need(hashlib.sha256(review_raw).hexdigest()==admission.get('independent_binding_review_sha256'),
         'actual final independent review bytes must match ROOT admission')
    # ROOT must authenticate these actual reviewer bytes and source-READY/head
    # assertions before dispatch. Matching bytes is not authorship verification.
    need(len(os.environ['M0_CONTROLS_PASS_B64'])<=24000,'bounded encoded controls receipt')
    controls_raw=base64.b64decode(os.environ['M0_CONTROLS_PASS_B64'],validate=True)
    need(len(controls_raw)<=16384,'bounded ROOT controls receipt')
    controls=json.loads(controls_raw)
    need(type(controls) is dict,'controls receipt object')
    need(controls.get('authority_thread')==ROOT_THREAD,'controls immutable ROOT authority thread')
    need(hashlib.sha256(controls_raw).hexdigest()==CONTROLS_PASS_SHA==admission.get('controls_pass_sha256'),
         'actual canonical controls receipt admission binding')
    required_controls={'schema':'root-primary-m0-controls-pass-v1','status':'ROOT_NORMAL_RACE_CONTROLS_ADMITTED',
        'actor_id':'1981537','authority_thread':ROOT_THREAD,'producer_head':CONTROLS_HEAD,'producer_tree':CONTROLS_TREE,
        'owner_manifest_sha256':OWNER_MANIFEST,'normal':{'tops':6,'outcomes':26,'leaves':24},
        'race':{'tops':6,'outcomes':26,'leaves':24},'literal_normal_exit':0,'literal_race_exit':0,
        'positive_outer_guard_cleanup':True,'product_capture':False}
    need(all(type(controls.get(k)) is type(v) and controls[k]==v for k,v in required_controls.items()),
         'exact corrected controls actual PASS prerequisite')
    for field in ('run_id','artifact_id'):
        need(type(controls.get(field)) is int and controls[field]>0,'actual controls receipt missing '+field)
    for field in ('artifact_sha256','independent_review_sha256'):
        need(re.fullmatch('[0-9a-f]{64}',controls.get(field,'')) is not None,'actual controls hash missing')
    return admission,controls,review_raw


def authenticate():
    manifest=json.loads((REPO/'.github/m0-trace-inputs.json').read_text())
    for name,row in manifest['files'].items():
        path=REPO/name
        s=path.lstat()
        need(stat.S_ISREG(s.st_mode) and s.st_nlink==1 and s.st_size==row['bytes'] and sha(path)==row['sha256'],
             'fixed source/helper identity mismatch: '+name)
    need(sha(REPO/'.github/primary-m0-observers/manifest.json')==OWNER_MANIFEST,'formatted owner manifest')
    need(sha(REPO/'.github/primary-m0-observers/format-source-review.json')==OWNER_REVIEW,'owner format-source review')
    return manifest


def load(name,path):
    spec=importlib.util.spec_from_file_location(name,path)
    module=importlib.util.module_from_spec(spec)
    sys.modules[name]=module
    spec.loader.exec_module(module)
    return module


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('mode',choices=('guard','worker'))
    parser.add_argument('--root',type=Path,required=True)
    a=parser.parse_args()
    admission,controls,review_raw=authority()  # Before any helper import, guard, Go or source acquisition.
    manifest=authenticate()
    need(not a.root.is_symlink(),'root symlink refused')
    root=a.root.resolve(strict=True)
    need(root==Path(os.environ['RUNNER_TEMP']).resolve()/'m0-operation-trace' and not root.is_symlink(),
         'fixed precreated root')
    if a.mode=='guard':
        remaining=WHOLE_SECONDS-(time.monotonic()-float(os.environ['M0_TRACE_STARTED_MONOTONIC']))
        need(remaining>0,'whole M0 deadline exhausted before guard')
        guard=root/'guard'
        cmd=[sys.executable,'-B',str(REPO/'scripts/ycsb_campaign_guard.py'),'--scripts-dir',str(REPO/'scripts'),
             '--out',str(guard),'--cwd',str(REPO),'--filesystem',str(root),'--synthetic',
             '--deadline',str(remaining),'--cleanup-seconds','10','--interval','0.2',
             '--rss-cap-bytes',str(24*1024**3),'--free-floor-bytes',str(10*1024**3),'--',
             sys.executable,'-B',str(Path(__file__).resolve()),'worker','--root',str(root)]
        # Replace only this fixed entrypoint with the unchanged authenticated guard.
        os.execv(sys.executable,cmd)
    consumer=load('m0_trace_consumer',HERE/'m0_trace_consumer.py')
    env={k:v for k,v in os.environ.items() if k not in ('GH_TOKEN','GITHUB_TOKEN','ROOT_ADMISSION_B64','M0_CONTROLS_PASS_B64','M0_BINDING_REVIEW_B64') and not k.startswith('GIT_')}
    token=os.environ.pop('GH_TOKEN',None)
    os.environ.pop('GITHUB_TOKEN',None)
    for name in ('tmp','output'):
        (root/name).mkdir()
    out=root/'output'
    # Ordinary trusted hosted caches are reused and recorded, not relabeled task-local.
    env.update(TMPDIR=str(root/'tmp'),GOTMPDIR=str(root/'tmp'),TMP=str(root/'tmp'),TEMP=str(root/'tmp'),
               GOENV='off',GOTOOLCHAIN='local',GOWORK='off',GOMEMLIMIT='8GiB',GOMAXPROCS='2',
               PYTHONDONTWRITEBYTECODE='1',GIT_CONFIG_NOSYSTEM='1',GIT_CONFIG_GLOBAL=os.devnull,
               GIT_TERMINAL_PROMPT='0',GCM_INTERACTIVE='Never')
    started=float(os.environ['M0_TRACE_STARTED_MONOTONIC'])
    rows=[]
    def write(path,value,adverse=False):
        data=(json.dumps(value,sort_keys=True,indent=2)+'\n').encode()
        need(len(data)<=32*MIB,'finite receipt size refused')
        if not adverse:
            old=path.stat().st_size if path.exists() else 0
            census((str(path.relative_to(root)),len(data)-old))
        with path.open('wb',buffering=0) as stream:
            consumer.checked_write(stream,data)
            stream.flush();os.fsync(stream.fileno())
    def remaining():
        seconds=WHOLE_SECONDS-(time.monotonic()-started)
        need(seconds>0,'whole M0 deadline exhausted')
        return seconds
    first_failure=None
    def census(prospective=None):
        nonlocal first_failure
        selected=[p for directory in (out,root/'guard') if directory.exists()
                  for p in directory.rglob('*') if p.is_file()]
        selected.extend(p for p in root.iterdir() if p.is_file() and p.suffix in ('.json','.log'))
        files={str(p.relative_to(root)):p.stat().st_size for p in selected}
        if prospective:
            name,amount=prospective
            files[name]=files.get(name,0)+amount
        violation=None
        for name,size in files.items():
            limit=128*MIB if name=='output/trace-parsed.stdout.log' else 32*MIB
            if size>limit:violation={'path':name,'observed_or_prospective_bytes':size,'limit':limit};break
        total=sum(files.values())
        if total>256*MIB:violation={'aggregate_bytes':total,'limit':256*MIB}
        if violation:
            if first_failure is None:
                first_failure={'status':'OUTPUT_BOUND_REFUSAL','first_observed_not_peak':True,
                               'monotonic':time.monotonic(),'violation':violation,'file_bytes':files}
                # Counted finite adverse receipt, not an exemption or successful
                # within-cap claim; retain this first row before refusing work.
                write(out/'resource-first-failure.json',first_failure,adverse=True)
            raise RuntimeError('fixed diagnostic output bound refusal')
        need(first_failure is None,'sticky output refusal')
        return {'bytes':total,'files':files}
    def run(label,argv,seconds=30,cwd=None,selected_env=env):
        limit=min(seconds,remaining())
        deadline=time.monotonic()+limit
        row={'label':label,'argv':argv,'limit_seconds':limit,'started_monotonic':time.monotonic(),
             'stdout_eof':False,'stderr_eof':False,'retained_writes_closed':False}
        rows.append(row)
        child=None;opened=[];pipes={};original=None;cleanup=[]
        try:
            outputs={}
            for name in ('stdout','stderr'):
                path=out/(label+'.'+name+'.log')
                outputs[name]=path.open('xb',buffering=0)
                opened.append(outputs[name])
            child=subprocess.Popen(argv,cwd=cwd,env=selected_env,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            row['pid']=child.pid
            poller=select.poll()
            for name,pipe in (('stdout',child.stdout),('stderr',child.stderr)):
                pipes[pipe.fileno()]=(name,pipe)
                poller.register(pipe.fileno(),select.POLLIN|select.POLLHUP|select.POLLERR)
            while pipes:
                balance=min(deadline-time.monotonic(),remaining())
                need(balance>0,'fixed stage timeout: '+label)
                census()
                for fd,mask in poller.poll(max(1,int(min(.2,balance)*1000))):
                    need(not mask & select.POLLNVAL,'invalid owned output pipe')
                    name,pipe=pipes[fd]
                    chunk=os.read(fd,65536)
                    if not chunk:
                        poller.unregister(fd);del pipes[fd]
                        pipe.close()  # Close errors are terminal, not ignored.
                        row[name+'_eof']=True
                    else:
                        census(('output/'+label+'.'+name+'.log',len(chunk)))
                        consumer.checked_write(outputs[name],chunk)
            row['exit']=child.wait(timeout=max(.001,min(deadline-time.monotonic(),remaining())))
            row['joined']=True;row['normal_return']=True
            need(row['exit']==0,'fixed M0 stage failed: '+label)
            census();remaining()
        except BaseException as caught:
            original=caught
            if child is not None:
                if child.poll() is None:
                    try:child.kill()
                    except BaseException as e:cleanup.append(type(e).__name__+': '+str(e))
                try:
                    row['exit']=child.wait(timeout=10);row['joined']=True
                except BaseException as e:cleanup.append(type(e).__name__+': '+str(e))
            row.setdefault('normal_return',False)
        finally:
            if child is not None:
                for pipe in (child.stdout,child.stderr):
                    if pipe is not None and not pipe.closed:
                        try:pipe.close()
                        except BaseException as e:cleanup.append(type(e).__name__+': '+str(e))
            for stream in opened:
                try:stream.flush();os.fsync(stream.fileno())
                except BaseException as e:cleanup.append(type(e).__name__+': '+str(e))
                try:stream.close()
                except BaseException as e:cleanup.append(type(e).__name__+': '+str(e))
            row['retained_writes_closed']=not cleanup and len(opened)==2 and all(f.closed for f in opened)
            row['ended_monotonic']=time.monotonic()
            if original is not None:row['error']=type(original).__name__+': '+str(original)
            if cleanup:row['cleanup_errors']=cleanup
            try:
                write(out/'stages.json',rows,adverse=original is not None or bool(cleanup) or first_failure is not None)
            except BaseException as e:
                cleanup.append('stage receipt: '+type(e).__name__+': '+str(e))
        if original is not None or cleanup:
            raise RuntimeError((type(original).__name__+': '+str(original) if original else '')+
                               ('; retained I/O/child cleanup: '+'; '.join(cleanup) if cleanup else '')) from original
        return '' if label=='trace-parsed' else (out/(label+'.stdout.log')).read_text()
    error=None
    try:
        write(out/'admission.json',admission)
        write(out/'controls-pass.json',controls)
        write(out/'binding-review.json',{'raw_bytes_base64':base64.b64encode(review_raw).decode('ascii'),
            'sha256':hashlib.sha256(review_raw).hexdigest(),'reviewed_head':admission['independent_binding_review_head'],
            'ROOT_admitted_status':admission['independent_binding_review_status'],'script_proves_authorship':False})
        write(out/'input-bindings.json',manifest)
        owner=REPO/'.github/primary-m0-observers'
        tools={}
        for name in ('git','go','gofmt','taskset'):
            path=Path(shutil.which(name) or '').resolve(strict=True)
            tools[name]={'path':str(path),'bytes':path.stat().st_size,'sha256':sha(path)}
        write(out/'hosted-tools.json',tools)
        go,gofmt,git=(tools[n]['path'] for n in ('go','gofmt','git'))
        run('go-version',[go,'version'],10)
        context=json.loads(run('go-context',[go,'env','-json','GOVERSION','GOOS','GOARCH','GOAMD64','CGO_ENABLED',
            'CC','CXX','CGO_CFLAGS','CGO_CPPFLAGS','CGO_CXXFLAGS','CGO_LDFLAGS','GOFLAGS','GOEXPERIMENT',
            'GOROOT','GOCACHE','GOMODCACHE','GOTMPDIR','GOTOOLCHAIN','GOWORK','GOENV'],10))
        need(context['GOVERSION']=='go1.26.10' and context['GOOS']=='linux' and context['GOARCH']=='amd64',
             'fixed source-control SDK/platform')
        need(context['GOFLAGS']==context['GOEXPERIMENT']=='' and env.get('GOFLAGS','')==env.get('GOEXPERIMENT','')=='',
             'unsupported inherited build flags; not overridden')
        write(out/'compile-context.json',context)
        ctools={}
        for field in ('CC','CXX'):
            command=context[field].split()[0]
            path=shutil.which(command)
            if path:
                resolved=Path(path).resolve(strict=True)
                ctools[field]={'path':str(resolved),'bytes':resolved.stat().st_size,'sha256':sha(resolved)}
            else:
                ctools[field]={'status':'NOT_RESOLVED','actual_value':context[field]}
        write(out/'cgo-tool-identities.json',ctools)
        need(context['CGO_ENABLED'] in ('0','1'),'unsupported CGO context')
        if context['CGO_ENABLED']=='1':
            need(all('sha256' in ctools[field] for field in ('CC','CXX')),'enabled CGO compiler identity unresolved')
        source=root/'source-C'; template=root/'git-template';template.mkdir()
        run('source-init',[git,'init','--template='+str(template),str(source)],10)
        run('source-sparse',[git,'-C',str(source),'config','core.sparseCheckout','true'],10)
        run('source-lf',[git,'-C',str(source),'config','core.autocrlf','false'],10)
        (source/'.git/info').mkdir(parents=True,exist_ok=True)
        (source/'.git/info/sparse-checkout').write_text('/*\n!/docs/evidence/\n!/docs/benchmarks/\n!/TreeDB/docs/evidence/\n!/TreeDB/docs/benchmarks/\n')
        run('source-origin',[git,'-C',str(source),'remote','add','origin','https://github.com/snissn/gomap.git'],10)
        authenv=dict(env)
        if token:
            authenv.update(GIT_CONFIG_COUNT='1',GIT_CONFIG_KEY_0='http.https://github.com/.extraheader',
                GIT_CONFIG_VALUE_0='Authorization: Basic '+base64.b64encode(('x-access-token:'+token).encode()).decode())
        try:
            run('source-fetch',[git,'-C',str(source),'-c','protocol.version=2','fetch','--depth=1','--filter=blob:none',
                                '--no-tags','--no-recurse-submodules','origin',C],120,selected_env=authenv)
            text=(out/'source-fetch.stderr.log').read_text().lower()
            need(not any(x in text for x in ('filtering not recognized','does not support filter','filter is not supported')),
                 'partial fetch unsupported; no archive fallback')
            run('source-C-checkout',[git,'-C',str(source),'checkout','--detach',C],60,selected_env=authenv)
        finally:
            authenv.clear();token=None
        observed=run('source-C-identity',[git,'-C',str(source),'show','-s','--format=%H%n%T','HEAD'],10).splitlines()
        need(observed==[C,C_TREE],'candidate source/tree mismatch')
        run('source-C-clean',[git,'-C',str(source),'diff','--exit-code','HEAD','--'],10)
        preimage='TreeDB/db/vacuum_collection_bench_test.go'
        need(sha(source/preimage)==sha(owner/'base'/preimage),'original M0 benchmark preimage')
        descriptions=json.loads((owner/'overlays.json').read_text())
        spec=descriptions['m0']['C']
        need(spec['source']==C and set(spec['Replace'])=={
            'TreeDB/db/vacuum_collection_bench_test.go','TreeDB/db/vacuum_m0_ordinal_observer_test.go'},
            'ONLY fixed M0 overlay admitted')
        need(not (source/'TreeDB/db/vacuum_m0_ordinal_observer_test.go').exists(),'new M0 helper preexists')
        replacements={str(source/n):str(owner/v) for n,v in spec['Replace'].items()}
        overlay=out/'m0-C.overlay.json';write(overlay,{'Replace':replacements})
        formatted=run('gofmt',[gofmt,'-l',*sorted(replacements.values())],30)
        need(not formatted.strip(),'M0 source formatting failed')
        run('consumer-controls',[sys.executable,'-B',str(HERE/'test_m0_trace_consumer.py')],30)
        cpus=sorted(os.sched_getaffinity(0))
        need(len(cpus)>=2,'selected M0 requires first two allowed CPUs')
        selected=cpus[:2]
        write(out/'runtime.json',{'allowed_affinity':cpus,'selected_affinity':selected,
            'GOMAXPROCS':env['GOMAXPROCS'],'GOMEMLIMIT':env['GOMEMLIMIT'],
            'new_hosted_context_not_original_environment_equivalence':True})
        goroot=Path(context['GOROOT'])
        trace_sources={'src/cmd/trace/main.go':'b9aba52f374d35afd58c56c7d7a15244527d54f6534d349ef9ead3e391658558',
            'src/internal/trace/event.go':'a46791f64767c5a8ce98e9d227603ebdc482fd3b3b4b5f01358d1ee7bd56ecd0',
            'src/cmd/go/internal/tool/tool.go':'8cf8cbcfb6c3ae5d726aa3b0b65afd6bbb7a2f9e141650fd7b7b5eff0c95219d'}
        for name,digest in trace_sources.items():need(sha(goroot/name)==digest,'pinned standard trace grammar/source')
        write(out/'trace-tool-sources.json',trace_sources)
        tool_lines=run('trace-tool-path',[go,'tool','-n','trace'],90,cwd=source).splitlines()
        need(len(tool_lines)==1 and Path(tool_lines[0]).is_absolute(),'one standard trace tool path')
        trace_tool=Path(tool_lines[0]).resolve(strict=True)
        write(out/'trace-tool.json',{'path':str(trace_tool),'bytes':trace_tool.stat().st_size,
              'sha256':sha(trace_tool),'grammar':'Go1.26.10 -d=parsed'})
        buildinfo=run('trace-tool-buildinfo',[go,'version','-m',str(trace_tool)],10,cwd=source)
        need('go1.26.10' in buildinfo and '\tpath\tcmd/trace' in buildinfo,'actual standard trace tool build identity')
        binaries=root/'bin';binaries.mkdir();binary=binaries/'db.test'
        run('build-normal',[go,'test','-c','-p','1','-mod=readonly','-overlay='+str(overlay),
                            '-o',str(binary),'./TreeDB/db'],300,cwd=source)
        write(out/'normal-ELF.json',{'path':str(binary),'bytes':binary.stat().st_size,'sha256':sha(binary),
              'source':C,'tree':C_TREE,'overlay_sha256':sha(overlay),'no_race':True})
        run('normal-buildinfo',[go,'version','-m',str(binary)],10,cwd=source)
        arm=out/'arm';arm.mkdir()
        run('one-M0',[tools['taskset']['path'],'-c',','.join(map(str,selected)),str(binary),
            '-test.run=^$',
            '-test.bench=^BenchmarkVacuumIndexOnlineCollectionProductionForegroundChurn$/^bytes_1x$',
            '-test.benchtime=1x','-test.count=1','-test.benchmem','-test.trace='+str(arm/'trace.out'),
            '-test.outputdir='+str(arm),'-m0-ordinal-observer-dir='+str(arm)],240,cwd=source)
        need((arm/'trace.out').is_file() and 0<(arm/'trace.out').stat().st_size<=32*MIB,
             'closed raw trace missing or outside bound')
        run('trace-parsed',[str(trace_tool),'-d=parsed',str(arm/'trace.out')],90,cwd=source)
        owner_receipt=json.loads((arm/'m0-ordinals.json').read_text())
        with (out/'trace-parsed.stdout.log').open('rb') as stream:
            result=consumer.consume(owner_receipt,stream,rows[-1])
        write(out/'trace-bijection.json',result)
        census();remaining()
        write(out/'completion.json',{'status':'M0_DIAGNOSTIC_BIJECTION_PENDING_ROOT_OUTPUT_REVIEW',
            'product':C,'source_tree':C_TREE,'only_M0_overlay':True,'iterations':1,'ordinals':4096,
            'M0_CV_acceptance':False,'outer_guard_result_required':True})
    except BaseException as caught:
        error=caught
        try:
            write(out/'failure.json',{'status':'M0_DIAGNOSTIC_FAILED_OR_INCOMPLETE',
                          'error_class':type(caught).__name__,'reason':str(caught)},adverse=True)
        except BaseException as receipt_error:
            error=RuntimeError(str(caught)+'; failure receipt: '+type(receipt_error).__name__+': '+str(receipt_error))
    finally:
        try:
            files={str(p.relative_to(root)):{'bytes':p.stat().st_size,'sha256':sha(p)}
                   for p in sorted(out.rglob('*')) if p.is_file()}
            write(out/'final-file-manifest.json',files,adverse=error is not None or first_failure is not None)
            # The portable guard itself has no storage/log cap. This worker's
            # pipe checks and sampled census are the diagnostic refusal path.
            output_bytes=sum(row['bytes'] for row in files.values())+(out/'final-file-manifest.json').stat().st_size
            print(json.dumps({'stages':rows,'output_bytes':output_bytes,'product_capture':True,'acceptance':False,
                  'cleanup':'only outer maintained guard can certify descendant drain'},sort_keys=True),flush=True)
            census()
        except BaseException as receipt_error:
            error=receipt_error if error is None else RuntimeError(str(error)+'; final receipt/census: '+
                                    type(receipt_error).__name__+': '+str(receipt_error))
    if error is not None:
        raise error


if __name__=='__main__':
    main()
