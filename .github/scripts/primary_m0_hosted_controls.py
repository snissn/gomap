#!/usr/bin/env python3
"""Fixed controls-only binding to the retained Attempt API and Linux guard."""
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
OWNER_MANIFEST = '5598e7d3aa39dec908740dad4d4b7264e449655b39fdae17210acf3c3835fe80'
OWNER_REVIEW = 'ac2dec69bfb2a6f329578ff7d4dbc3aa8cb237fc4bf24147a3c964f337134b3e'
B = '29b879fbb4c4c6da2d7b18ff7c2a3a341e24e2b8'
C = '4934bd6d51d9047ba0187d05c1d211cbe419811a'
TREES = {B:'e1a871ce9fb0ce9a91706bf8d6d723cc681aad78', C:'7f30f92674110a4544a381468677183ab3133406'}


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
    need(sha(REPO/'.github/primary-m0-observers/manifest.json')==OWNER_MANIFEST,'original owner manifest')
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
    admission=authority()  # Before any helper import, guard, Go or source acquisition.
    manifest=authenticate()
    need(not a.root.is_symlink(),'root symlink refused')
    root=a.root.resolve(strict=True)
    need(root==Path(os.environ['RUNNER_TEMP']).resolve()/'primary-m0-controls' and not root.is_symlink(),
         'fixed precreated root')
    if a.mode=='guard':
        guard=root/'guard'
        cmd=[sys.executable,'-B',str(REPO/'scripts/ycsb_campaign_guard.py'),'--scripts-dir',str(REPO/'scripts'),
             '--out',str(guard),'--cwd',str(REPO),'--filesystem',str(root),'--synthetic',
             '--deadline','660','--cleanup-seconds','10','--interval','0.2',
             '--rss-cap-bytes',str(24*1024**3),'--free-floor-bytes',str(10*1024**3),'--',
             sys.executable,'-B',str(Path(__file__).resolve()),'worker','--root',str(root)]
        # Replace only this fixed entrypoint with the unchanged authenticated guard.
        os.execv(sys.executable,cmd)
    kernel=load('primary_m0_attempt',HERE/'primary_m0_attempt.py')
    events=load('primary_m0_control_events',HERE/'primary_m0_control_events.py')
    env={k:v for k,v in os.environ.items() if k not in ('GH_TOKEN','GITHUB_TOKEN','ROOT_ADMISSION_B64') and not k.startswith('GIT_')}
    token=os.environ.pop('GH_TOKEN',None)
    os.environ.pop('GITHUB_TOKEN',None)
    for name in ('tmp','gocache','modcache','xdg-cache','xdg-config','bin'):
        (root/name).mkdir()
    env.update(TMPDIR=str(root/'tmp'),GOTMPDIR=str(root/'tmp'),TMP=str(root/'tmp'),TEMP=str(root/'tmp'),
               GOCACHE=str(root/'gocache'),GOMODCACHE=str(root/'modcache'),
               XDG_CACHE_HOME=str(root/'xdg-cache'),XDG_CONFIG_HOME=str(root/'xdg-config'),
               GOENV='off',GOTOOLCHAIN='local',GOWORK='off',GOMEMLIMIT='4GiB',GOMAXPROCS='2',
               PYTHONDONTWRITEBYTECODE='1',GIT_CONFIG_NOSYSTEM='1',GIT_CONFIG_GLOBAL=os.devnull,
               GIT_TERMINAL_PROMPT='0',GCM_INTERACTIVE='Never')
    attempt=kernel.Attempt.__new__(kernel.Attempt)
    attempt.started=float(os.environ['PRIMARY_M0_STARTED_MONOTONIC'])
    need(0<=time.monotonic()-attempt.started<600,'source-control whole deadline')
    kernel.Attempt.__init__(attempt,root)
    error=None
    try:
        kernel.write(attempt.out/'admission.json',admission)
        kernel.write(attempt.out/'input-bindings.json',manifest)
        owner=root/'observer-inputs'
        attempt.check(sum(r['bytes'] for n,r in manifest['files'].items() if n.startswith('.github/primary-m0-observers/')))
        shutil.copytree(REPO/'.github/primary-m0-observers',owner)
        tools={}
        for name in ('git','go','gofmt'):
            path=Path(shutil.which(name) or '').resolve(strict=True)
            tools[name]={'path':str(path),'bytes':path.stat().st_size,'sha256':sha(path)}
        kernel.write(attempt.out/'hosted-tools.json',tools)
        go,gofmt,git=(tools[n]['path'] for n in ('go','gofmt','git'))
        def run(label,argv,seconds=30,cwd=None,selected_env=env):
            code=attempt.run(label,argv,seconds,cwd=cwd,env=selected_env)
            need(code==0,'fixed source-control stage failed: '+label)
            attempt.check()  # Refuse dependent work even if the joined child returned zero.
            return (attempt.out/(label+'.stdout.log')).read_text()
        run('go-version',[go,'version'],10)
        context=json.loads(run('go-context',[go,'env','-json','GOVERSION','GOOS','GOARCH','GOAMD64','CGO_ENABLED',
            'CC','CXX','CGO_CFLAGS','CGO_CPPFLAGS','CGO_CXXFLAGS','CGO_LDFLAGS','GOFLAGS','GOEXPERIMENT',
            'GOROOT','GOCACHE','GOMODCACHE','GOTMPDIR','GOTOOLCHAIN','GOWORK','GOENV'],10))
        need(context['GOVERSION']=='go1.26.10' and context['GOOS']=='linux' and context['GOARCH']=='amd64',
             'fixed source-control SDK/platform')
        need(context['GOFLAGS']==context['GOEXPERIMENT']=='' and env.get('GOFLAGS','')==env.get('GOEXPERIMENT','')=='',
             'unsupported inherited build flags; not overridden')
        kernel.write(attempt.out/'compile-context.json',context)
        ctools={}
        for field in ('CC','CXX'):
            command=context[field].split()[0]
            path=shutil.which(command)
            if path:
                resolved=Path(path).resolve(strict=True)
                ctools[field]={'path':str(resolved),'bytes':resolved.stat().st_size,'sha256':sha(resolved)}
            else:
                ctools[field]={'status':'NOT_RESOLVED','actual_value':context[field]}
        kernel.write(attempt.out/'cgo-tool-identities.json',ctools)
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
        run('source-fetch',[git,'-C',str(source),'-c','protocol.version=2','fetch','--depth=1','--filter=blob:none',
                            '--no-tags','--no-recurse-submodules','origin',B,C],120,selected_env=authenv)
        text=(attempt.out/'source-fetch.stderr.log').read_text().lower()
        need(not any(x in text for x in ('filtering not recognized','does not support filter','filter is not supported')),
             'partial fetch unsupported; no archive fallback')
        run('source-C-checkout',[git,'-C',str(source),'checkout','--detach',C],120,selected_env=authenv)
        # B is an original-source worktree, never C product source relabeled as baseline.
        baseline=root/'source-B'
        run('source-worktree-config',[git,'-C',str(source),'config','extensions.worktreeConfig','true'],10)
        run('source-B-add',[git,'-C',str(source),'worktree','add','--detach','--no-checkout',str(baseline),B],10)
        gitdir=Path(run('source-B-gitdir',[git,'-C',str(baseline),'rev-parse','--absolute-git-dir'],10).strip())
        need(gitdir.resolve().is_relative_to(root),'baseline Git metadata outside root')
        (gitdir/'info').mkdir(exist_ok=True)
        (gitdir/'info/sparse-checkout').write_bytes((source/'.git/info/sparse-checkout').read_bytes())
        run('source-B-checkout',[git,'-C',str(baseline),'checkout','--detach',B],120,selected_env=authenv)
        authenv.clear();token=None
        for arm,head,checkout in (('B',B,baseline),('C',C,source)):
            observed=run('source-'+arm+'-identity',[git,'-C',str(checkout),'show','-s','--format=%H%n%T','HEAD'],10).splitlines()
            need(observed==[head,TREES[head]],'original arm source/tree mismatch')
            run('source-'+arm+'-clean',[git,'-C',str(checkout),'diff','--exit-code','HEAD','--'],10)
            for name in ('TreeDB/caching/db.go','TreeDB/mvcc/primary_generation_routing_v6_test.go',
                         'TreeDB/db/vacuum_collection_bench_test.go'):
                pre=owner/('preimages/29b879fb' if arm=='B' else 'base')/name
                need(sha(checkout/name)==sha(pre),'original arm preimage mismatch: '+arm+'/'+name)
        descriptions=json.loads((owner/'overlays.json').read_text())
        overlays={}
        for label,kind,arm in (('primary-B','primary','B'),('primary-C','primary','C'),('m0-C','m0','C'),
                               ('focused-C','focused_controls','C')):
            spec=descriptions[kind][arm]; checkout=baseline if arm=='B' else source
            need(spec['source']==(B if arm=='B' else C),'overlay source authority')
            replacements={str(checkout/n):str(owner/v) for n,v in spec['Replace'].items()}
            for n in spec['Replace']:
                if n not in ('TreeDB/caching/db.go','TreeDB/mvcc/primary_generation_routing_v6_test.go',
                             'TreeDB/db/vacuum_collection_bench_test.go'):
                    need(not (checkout/n).exists(),'new overlay helper already exists')
            overlay=attempt.out/(label+'.overlay.json');kernel.write(overlay,{'Replace':replacements})
            overlays[label]=overlay
        inputs=sorted({str(owner/value) for kind in ('primary','m0') for spec in descriptions[kind].values()
                       for value in spec['Replace'].values()})
        formatted=run('gofmt',[gofmt,'-l',*inputs],30)
        need(not formatted.strip(),'observer source formatting failed; do not continue')
        run('pure-controls',[sys.executable,'-B',str(HERE/'test_primary_m0_hosted_controls.py')],30)
        overlay=str(overlays['focused-C'])
        common=[go,'test','-json','-tags=treedb_test','-overlay='+overlay,'-count=1','-p=1','-mod=readonly',
                '-run',events.SELECTOR]
        for mode,seconds,flags,deadline in (('normal',180,[], '150s'),('race',240,['-race'],'210s')):
            run(mode,common+flags+['-timeout='+deadline,'./TreeDB/caching','./TreeDB/mvcc','./TreeDB/db'],seconds,cwd=source)
            kernel.write(attempt.out/(mode+'-inventory.json'),events.validate(attempt.out/(mode+'.stdout.log')))
        # These combined-overlay normal control ELFs are NOT the isolated later capture ELFs.
        elfrows={}
        for package in ('caching','mvcc','db'):
            binary=root/'bin'/('focused-C-'+package+'.test')
            run('compile-'+package,[go,'test','-c','-tags=treedb_test','-overlay='+overlay,'-p=1','-mod=readonly',
                 '-o',str(binary),'./TreeDB/'+package],120,cwd=source)
            with binary.open('rb') as stream:
                need(stream.read(4)==b'\x7fELF','normal Linux ELF missing')
            run('buildinfo-'+package,[go,'version','-m',str(binary)],10)
            elfrows[package]={'path':str(binary),'bytes':binary.stat().st_size,'sha256':sha(binary),
                             'source':C,'overlay':'focused-C','capture_eligible':False}
        kernel.write(attempt.out/'control-elf-identities.json',elfrows)
        kernel.write(attempt.out/'completion.json',{'status':'SOURCE_BUILD_CONTROLS_PASS_CAPTURE_NOT_ADMITTED',
             'normal':{'tops':6,'outcomes':26,'leaves':24},'race':{'tops':6,'outcomes':26,'leaves':24},
             'baseline_compiled':False,'product_capture':False,'outer_guard_result_required':True})
    except BaseException as caught:
        error=caught
        kernel.write(attempt.out/'failure.json',{'status':'SOURCE_CONTROLS_FAILED_NO_CAPTURE',
                      'error_class':type(caught).__name__,'reason':str(caught)})
    finally:
        attempt.close()  # Monitor joined; this does not claim child/descendant drain.
        files={}
        for p in sorted(root.rglob('*')):
            if p.is_file() and p.relative_to(root).parts[0] in ('output','bin'):
                files[str(p.relative_to(root))]={'bytes':p.stat().st_size,'sha256':sha(p)}
        kernel.write(attempt.out/'final-file-manifest.json',files)
        final=kernel.census(root,enforce=False)
        kernel.write(attempt.out/'final-census.json',final)
        print(json.dumps({'stages':attempt.rows,'final_census':final,'product_capture':False,
              'cleanup':'only outer maintained guard can certify descendant drain'},sort_keys=True),flush=True)
        if final['errors'] and error is None:
            error=RuntimeError('final resource refusal')
    if error is not None:
        raise error


if __name__=='__main__':
    main()
