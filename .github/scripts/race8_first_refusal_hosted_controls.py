#!/usr/bin/env python3
"""Candidate-only source controls under the unchanged portable Linux guard."""
import argparse
import base64
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
OWNER_MANIFEST = 'fd55f142c1503b1338621d4198a30f617ab96265f5411e3bd8b6e1b69d1b32c1'
OWNER_REVIEW = 'ee439978aa27ec5bcce5a36c44d589d37c7e4039af265342f9f62d8a64f40fef'
C = '15f1bcbdf13b9feacaffa6f635688b464c1208a5'
C_TREE = 'cab3eff3c3ab6a75aa30b60f6bc5cfbe84d82e22'
DIAGNOSTIC_HEAD = '641a721077d71499eed545ef52caa1f0b81a384a'
DIAGNOSTIC_TREE = '254b4ec1b1996824274a9734e70796640bf9fe6d'
WHOLE_SECONDS = 1020


EXPECTED_OVERLAYS = ('TreeDB/caching/cow_basis_refusal_diagnostic.go', 'TreeDB/caching/cow_basis_refusal_diagnostic_test.go', 'TreeDB/caching/cow_flush.go', 'TreeDB/caching/db.go', 'TreeDB/collections/native_basis_refresh.go', 'TreeDB/collections/native_basis_refusal_diagnostic_test.go', 'TreeDB/collections/native_basis_public_vacuum_activation_test.go', 'TreeDB/collections/native_json_read.go', 'TreeDB/collections/native_ordinary_concurrent_correctness_test.go', 'TreeDB/collections/native_semantic_adapter.go', 'TreeDB/db/native_basis_refusal_diagnostic.go', 'TreeDB/internal/valuelog/cow_basis_refusal_observer.go', 'TreeDB/internal/valuelog/cow_basis_refusal_observer_test.go', 'TreeDB/internal/valuelog/cow_reader.go', 'TreeDB/native_basis_refusal_diagnostic.go')

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
         os.environ.get('GITHUB_REF')=='refs/heads/diagnostic/race8-public-vacuum-activation-01', 'fixed hosted authority')
    head=os.environ.get('REVIEWED_HEAD','')
    need(re.fullmatch('[0-9a-f]{40}',head) is not None and
         head==os.environ.get('GITHUB_SHA')==os.environ.get('WORKFLOW_COMMIT'), 'exact reviewed harness')
    encoded=os.environ['ROOT_ADMISSION_B64']
    need(len(encoded)<=16384,'bounded admission input')
    admission=json.loads(base64.b64decode(encoded,validate=True))
    need(isinstance(admission,dict),'admission object required')
    required={'schema':'race8-first-refusal-hosted-controls-admission-v1','status':'ADMITTED_CONTROLS_ONLY',
              'authority':'ROOT','authority_thread':'01a18e7c-8c23-7fd2-8d55-b0486d938cd3','actor_id':'1981537','expected_head':head,'run_attempt':'1',
              'owner_manifest_sha256':OWNER_MANIFEST,'owner_review_sha256':OWNER_REVIEW,
              'allow_product_capture':False}
    need(all(admission.get(k)==v for k,v in required.items()),'ROOT controls-only admission')
    need(re.fullmatch('[0-9a-f]{64}',admission.get('independent_binding_review_sha256','')) is not None,
         'independent binding review missing')
    need(OWNER_MANIFEST is not None and OWNER_REVIEW is not None and
         DIAGNOSTIC_HEAD is not None and DIAGNOSTIC_TREE is not None,
         'NOT_READY: exact activation source seal/review pending')
    return admission


def authenticate():
    manifest=json.loads((REPO/'.github/race8-first-refusal-inputs.json').read_text())
    for name,row in manifest['files'].items():
        path=REPO/name
        s=path.lstat()
        need(stat.S_ISREG(s.st_mode) and s.st_nlink==1 and s.st_size==row['bytes'] and sha(path)==row['sha256'],
             'fixed source/helper identity mismatch: '+name)
    need(sha(REPO/'.github/race8-first-refusal-fixture/manifest.json')==OWNER_MANIFEST,'owner fixture manifest')
    need(sha(REPO/'.github/race8-first-refusal-fixture/independent-review.md')==OWNER_REVIEW,'owner independent source review')
    bindings=json.loads((REPO/'.github/race8-first-refusal-fixture/bindings.json').read_text())
    need(bindings['head']==DIAGNOSTIC_HEAD and bindings['tree']==DIAGNOSTIC_TREE and bindings['historical_base']==C, 'exact semantic diagnostic/base binding')
    return manifest


def load(name,path):
    spec=importlib.util.spec_from_file_location(name,path)
    module=importlib.util.module_from_spec(spec)
    sys.modules[name]=module
    spec.loader.exec_module(module)
    return module


def prepare_normal_controls(go, overlay, source, root, run, write):
    """Compile the fixed activation package within the existing preparation budget; never execute this ELF."""
    started=time.monotonic()
    deadline=started+240
    directory=root/'tmp'/'prepared-normal'
    directory.mkdir()
    receipt={'status':'NORMAL_PREPARATION_INCOMPLETE','aggregate_limit_seconds':240,
             'started_monotonic':started,'test_execution':False,'binaries':[]}
    target=root/'output'/'normal-preparation.json'
    write(target,receipt)
    for package in ('collections',):
        seconds=deadline-time.monotonic()
        need(seconds>0,'aggregate normal preparation deadline exhausted')
        binary=directory/(package.replace('/','-')+'.test')
        run('prepare-normal-'+package.replace('/','-'),[go,'test','-c','-tags=treedb_test','-overlay='+str(overlay),
            '-p=1','-mod=readonly','-o',str(binary),'./TreeDB/'+package],seconds,cwd=source)
        info=binary.lstat()
        need(stat.S_ISREG(info.st_mode) and info.st_nlink==1 and info.st_size>0,
             'normal preparation ELF identity refused: '+package)
        receipt['binaries'].append({'package':'./TreeDB/'+package,'path':str(binary),
            'bytes':info.st_size,'sha256':sha(binary)})
        receipt['observed_monotonic']=time.monotonic()
        write(target,receipt)
        need(receipt['observed_monotonic']<=deadline,'aggregate normal preparation deadline exhausted')
    receipt['status']='NORMAL_PREPARATION_COMPILED_ONLY'
    receipt['ended_monotonic']=time.monotonic()
    write(target,receipt)
    need(time.monotonic()<=deadline,'aggregate normal preparation deadline exhausted')


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('mode',choices=('guard','worker'))
    parser.add_argument('--root',type=Path,required=True)
    a=parser.parse_args()
    admission=authority()  # Before any helper import, guard, Go or source acquisition.
    manifest=authenticate()
    need(not a.root.is_symlink(),'root symlink refused')
    root=a.root.resolve(strict=True)
    need(root==Path(os.environ['RUNNER_TEMP']).resolve()/'race8-first-refusal-controls' and not root.is_symlink(),
         'fixed precreated root')
    if a.mode=='guard':
        remaining=WHOLE_SECONDS-(time.monotonic()-float(os.environ['RACE8_FIRST_REFUSAL_STARTED_MONOTONIC']))
        need(remaining>0,'whole controls deadline exhausted before guard')
        guard=root/'guard'
        cmd=[sys.executable,'-B',str(REPO/'scripts/ycsb_campaign_guard.py'),'--scripts-dir',str(REPO/'scripts'),
             '--out',str(guard),'--cwd',str(REPO),'--filesystem',str(root),'--synthetic',
             '--deadline',str(remaining),'--cleanup-seconds','10','--interval','0.2',
             '--rss-cap-bytes',str(24*1024**3),'--free-floor-bytes',str(10*1024**3),'--',
             sys.executable,'-B',str(Path(__file__).resolve()),'worker','--root',str(root)]
        # Replace only this fixed entrypoint with the unchanged authenticated guard.
        os.execv(sys.executable,cmd)
    events=load('race8_first_refusal_control_events',HERE/'race8_first_refusal_events.py')
    env={k:v for k,v in os.environ.items() if k not in ('GH_TOKEN','GITHUB_TOKEN','ROOT_ADMISSION_B64') and not k.startswith('GIT_')}
    token=os.environ.pop('GH_TOKEN',None)
    os.environ.pop('GITHUB_TOKEN',None)
    for name in ('tmp','output'):
        (root/name).mkdir()
    out=root/'output'
    # Ordinary trusted hosted caches are reused and recorded, not relabeled task-local.
    env.update(TMPDIR=str(root/'tmp'),GOTMPDIR=str(root/'tmp'),TMP=str(root/'tmp'),TEMP=str(root/'tmp'),
               GOENV='off',GOTOOLCHAIN='local',GOWORK='off',GOMEMLIMIT='4GiB',GOMAXPROCS='2',
               PYTHONDONTWRITEBYTECODE='1',GIT_CONFIG_NOSYSTEM='1',GIT_CONFIG_GLOBAL=os.devnull,
               GIT_TERMINAL_PROMPT='0',GCM_INTERACTIVE='Never')
    started=float(os.environ['RACE8_FIRST_REFUSAL_STARTED_MONOTONIC'])
    rows=[]
    def write(path,value):
        path.write_text(json.dumps(value,sort_keys=True,indent=2)+'\n')
    def remaining():
        seconds=WHOLE_SECONDS-(time.monotonic()-started)
        need(seconds>0,'whole controls deadline exhausted')
        return seconds
    def run(label,argv,seconds=30,cwd=None,selected_env=env,allow_nonzero=False):
        limit=min(seconds,remaining())
        row={'label':label,'argv':argv,'limit_seconds':limit,'started_monotonic':time.monotonic()}
        rows.append(row)
        with (out/(label+'.stdout.log')).open('wb') as stdout, (out/(label+'.stderr.log')).open('wb') as stderr:
            child=subprocess.Popen(argv,cwd=cwd,env=selected_env,stdout=stdout,stderr=stderr)
            row['pid']=child.pid
            try:
                row['exit']=child.wait(timeout=limit)
                row['joined']=True
                row['normal_return']=True
            except subprocess.TimeoutExpired:
                child.kill()
                row['exit']=child.wait()
                row['joined']=True
                row['normal_return']=False
                raise RuntimeError('fixed stage timeout: '+label)
            finally:
                row['ended_monotonic']=time.monotonic()
                write(out/'stages.json',rows)
        need(allow_nonzero or row['exit']==0,'fixed source-control stage failed: '+label)
        remaining()  # Refuse dependent work after the shared whole deadline.
        return (out/(label+'.stdout.log')).read_text()
    error=None
    try:
        write(out/'admission.json',admission)
        write(out/'input-bindings.json',manifest)
        owner=REPO/'.github/race8-first-refusal-fixture'
        tools={}
        for name in ('git','go','gofmt'):
            path=Path(shutil.which(name) or '').resolve(strict=True)
            tools[name]={'path':str(path),'bytes':path.stat().st_size,'sha256':sha(path)}
        python=Path(sys.executable).resolve(strict=True)
        tools['python']={'path':str(python),'bytes':python.stat().st_size,'sha256':sha(python)}
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
        dependencies=json.loads((owner/'dependencies.json').read_text())
        need(dependencies['base']==C,'dependency base mismatch')
        for row in dependencies['files']:
            path=source/row['path']
            need(path.stat().st_size==row['bytes'] and sha(path)==row['sha256'],
                 'ROOT-selected dependency mismatch: '+row['path'])
        replacements={}
        for path in sorted((owner/'overlay').rglob('*.go')):
            name=path.relative_to(owner/'overlay')
            original=source/name
            preimage=owner/'preimage'/name
            if preimage.exists():
                need(original.is_file() and sha(original)==sha(preimage),'original fixture preimage mismatch: '+str(name))
            else:
                need(not original.exists(),'new fixture helper already exists: '+str(name))
            replacements[str(original)]=str(path)
        need(len(replacements)==15 and set(str(Path(p).relative_to(source)) for p in replacements)==set(EXPECTED_OVERLAYS),'exact fifteen replacements required')
        overlay=out/'focused-C.overlay.json'
        write(overlay,{'Replace':replacements})
        formatted=run('gofmt',[gofmt,'-l',*sorted(replacements.values())],30)
        need(not formatted.strip(),'observer source formatting failed; do not continue')
        run('pure-controls',[sys.executable,'-B','-m','unittest',
            'test_race8_first_refusal_events','test_race8_public_vacuum_activation_events'],30,cwd=HERE)
        prepare_normal_controls(go,overlay,source,root,run,write)
        need('TREEDB_NATIVE_CORRECTNESS_100K' not in env, '100k opt-in must be unset; no override')
        write(out/'activation-context.json',{'schema':'race8-public-vacuum-activation-context-v1',
             'observed_opt_in_present':'TREEDB_NATIVE_CORRECTNESS_100K' in env,
             'api':'VacuumIndexOnline','modes':['normal','race'],'selector':events.ACTIVATION_SELECTOR,'count':1,
             'profiles':False,'context_scope':'fixed child environment after explicit opt-in absence refusal'})
        common=[go,'test','-json','-tags=treedb_test','-overlay='+str(overlay),'-count=1','-p=1','-mod=readonly',
                '-run',events.ACTIVATION_SELECTOR]
        for mode,seconds,flags,deadline in (('normal',180,[], '150s'),('race',600,['-race'],'570s')):
            run(mode,common+flags+['-timeout='+deadline,'./TreeDB/collections'],seconds,cwd=source,allow_nonzero=True)
            control_exit=rows[-1]['exit']
            inventory=events.validate(out/(mode+'.stdout.log'),mode='activation',go_exit=control_exit)
            write(out/(mode+'-inventory.json'),inventory)
            if control_exit != 0:
                raise SystemExit(control_exit)
            need(inventory['status']=='PASS' and inventory.get('activation_verified') is True,
                 'public native-owner activation incomplete; no-op and skips do not qualify')
        write(out/'completion.json',{'status':'PUBLIC_VACUUM_ACTIVATION_CONTROLS_PASS_NOT_PRODUCT_ACCEPTANCE',
             'normal':{'tops':1,'outcomes':1,'leaves':1},'race':{'tops':1,'outcomes':1,'leaves':1},
             'original_witness_repeated':False,
             'meaning':'PUBLIC_CALLSITE_COVERAGE_ONLY_NOT_HISTORICAL_CAUSE_OR_REPAIR',
             'candidate_only':True,'product_capture':False,'outer_guard_result_required':True})
    except BaseException as caught:
        error=caught
        write(out/'failure.json',{'status':'SOURCE_CONTROLS_FAILED_NO_CAPTURE',
                      'error_class':type(caught).__name__,'reason':str(caught)})
    finally:
        files={str(p.relative_to(root)):{'bytes':p.stat().st_size,'sha256':sha(p)}
               for p in sorted(out.rglob('*')) if p.is_file()}
        write(out/'final-file-manifest.json',files)
        # Post-run output acceptance only: the portable guard has no storage/log cap.
        output_bytes=sum(row['bytes'] for row in files.values())+(out/'final-file-manifest.json').stat().st_size
        print(json.dumps({'stages':rows,'output_bytes':output_bytes,'product_capture':False,
              'cleanup':'only outer maintained guard can certify descendant drain'},sort_keys=True),flush=True)
        if output_bytes>32*1024**2 and error is None:
            error=RuntimeError('worker output acceptance exceeds32MiB; no live log-cap claim')
    if error is not None:
        raise error


if __name__=='__main__':
    main()
