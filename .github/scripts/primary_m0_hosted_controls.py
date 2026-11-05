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
OWNER_MANIFEST = '86f4ee083aba9e2f2dbdb87abf9ec5be9eb13d416e1bafd25f7b61139e317aa3'
OWNER_REVIEW = '863afc8d80a3f68f767b8be0f8f810f5ab646a5c1e256ea674b7894a97ef883a'
C = '4934bd6d51d9047ba0187d05c1d211cbe419811a'
C_TREE = '7f30f92674110a4544a381468677183ab3133406'
WHOLE_SECONDS = 1020


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
         os.environ.get('GITHUB_REF')=='refs/heads/diagnostic/primary-m0-controls-01', 'fixed hosted authority')
    head=os.environ.get('REVIEWED_HEAD','')
    need(re.fullmatch('[0-9a-f]{40}',head) is not None and
         head==os.environ.get('GITHUB_SHA')==os.environ.get('WORKFLOW_COMMIT'), 'exact reviewed harness')
    encoded=os.environ['ROOT_ADMISSION_B64']
    need(len(encoded)<=16384,'bounded admission input')
    admission=json.loads(base64.b64decode(encoded,validate=True))
    need(isinstance(admission,dict),'admission object required')
    required={'schema':'primary-m0-hosted-controls-admission-v1','status':'ADMITTED_CONTROLS_ONLY',
              'authority':'ROOT','actor_id':'1981537','expected_head':head,'run_attempt':'1',
              'owner_manifest_sha256':OWNER_MANIFEST,'owner_review_sha256':OWNER_REVIEW,
              'allow_product_capture':False}
    need(all(admission.get(k)==v for k,v in required.items()),'ROOT controls-only admission')
    need(re.fullmatch('[0-9a-f]{64}',admission.get('independent_binding_review_sha256','')) is not None,
         'independent binding review missing')
    return admission


def authenticate():
    manifest=json.loads((REPO/'.github/primary-m0-inputs.json').read_text())
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


def prepare_normal_controls(go, overlay, source, root, run, write):
    """Compile the three selected packages; never execute the resulting ELFs."""
    started=time.monotonic()
    deadline=started+240
    directory=root/'tmp'/'prepared-normal'
    directory.mkdir()
    receipt={'status':'NORMAL_PREPARATION_INCOMPLETE','aggregate_limit_seconds':240,
             'started_monotonic':started,'test_execution':False,'binaries':[]}
    target=root/'output'/'normal-preparation.json'
    write(target,receipt)
    for package in ('caching','mvcc','db'):
        seconds=deadline-time.monotonic()
        need(seconds>0,'aggregate normal preparation deadline exhausted')
        binary=directory/(package+'.test')
        run('prepare-normal-'+package,[go,'test','-c','-tags=treedb_test','-overlay='+str(overlay),
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
    need(root==Path(os.environ['RUNNER_TEMP']).resolve()/'primary-m0-controls' and not root.is_symlink(),
         'fixed precreated root')
    if a.mode=='guard':
        remaining=WHOLE_SECONDS-(time.monotonic()-float(os.environ['PRIMARY_M0_STARTED_MONOTONIC']))
        need(remaining>0,'whole controls deadline exhausted before guard')
        guard=root/'guard'
        cmd=[sys.executable,'-B',str(REPO/'scripts/ycsb_campaign_guard.py'),'--scripts-dir',str(REPO/'scripts'),
             '--out',str(guard),'--cwd',str(REPO),'--filesystem',str(root),'--synthetic',
             '--deadline',str(remaining),'--cleanup-seconds','10','--interval','0.2',
             '--rss-cap-bytes',str(24*1024**3),'--free-floor-bytes',str(10*1024**3),'--',
             sys.executable,'-B',str(Path(__file__).resolve()),'worker','--root',str(root)]
        # Replace only this fixed entrypoint with the unchanged authenticated guard.
        os.execv(sys.executable,cmd)
    events=load('primary_m0_control_events',HERE/'primary_m0_control_events.py')
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
    started=float(os.environ['PRIMARY_M0_STARTED_MONOTONIC'])
    rows=[]
    def write(path,value):
        path.write_text(json.dumps(value,sort_keys=True,indent=2)+'\n')
    def remaining():
        seconds=WHOLE_SECONDS-(time.monotonic()-started)
        need(seconds>0,'whole controls deadline exhausted')
        return seconds
    def run(label,argv,seconds=30,cwd=None,selected_env=env):
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
        need(row['exit']==0,'fixed source-control stage failed: '+label)
        remaining()  # Refuse dependent work after the shared whole deadline.
        return (out/(label+'.stdout.log')).read_text()
    error=None
    try:
        write(out/'admission.json',admission)
        write(out/'input-bindings.json',manifest)
        owner=REPO/'.github/primary-m0-observers'
        tools={}
        for name in ('git','go','gofmt'):
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
        for name in ('TreeDB/caching/db.go','TreeDB/mvcc/primary_generation_routing_v6_test.go',
                     'TreeDB/db/vacuum_collection_bench_test.go'):
            need(sha(source/name)==sha(owner/'base'/name),'original candidate preimage mismatch: '+name)
        descriptions=json.loads((owner/'overlays.json').read_text())
        spec=descriptions['focused_controls']['C']
        need(spec['source']==C,'focused overlay source authority')
        replacements={str(source/n):str(owner/v) for n,v in spec['Replace'].items()}
        for name in spec['Replace']:
            if name not in ('TreeDB/caching/db.go','TreeDB/mvcc/primary_generation_routing_v6_test.go',
                            'TreeDB/db/vacuum_collection_bench_test.go'):
                need(not (source/name).exists(),'new overlay helper already exists')
        overlay=out/'focused-C.overlay.json'
        write(overlay,{'Replace':replacements})
        formatted=run('gofmt',[gofmt,'-l',*sorted(replacements.values())],30)
        need(not formatted.strip(),'observer source formatting failed; do not continue')
        run('pure-controls',[sys.executable,'-B',str(HERE/'test_primary_m0_hosted_controls.py')],30)
        prepare_normal_controls(go,overlay,source,root,run,write)
        common=[go,'test','-json','-tags=treedb_test','-overlay='+str(overlay),'-count=1','-p=1','-mod=readonly',
                '-run',events.SELECTOR]
        for mode,seconds,flags,deadline in (('normal',180,[], '150s'),('race',600,['-race'],'570s')):
            run(mode,common+flags+['-timeout='+deadline,'./TreeDB/caching','./TreeDB/mvcc','./TreeDB/db'],seconds,cwd=source)
            write(out/(mode+'-inventory.json'),events.validate(out/(mode+'.stdout.log')))
        write(out/'completion.json',{'status':'SOURCE_CONTROLS_PASS_CAPTURE_NOT_ADMITTED',
             'normal':{'tops':6,'outcomes':26,'leaves':24},'race':{'tops':6,'outcomes':26,'leaves':24},
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
