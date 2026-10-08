"""Manual same-VM C3 route. Capacity is cheap; full mode needs two root decisions.

Dispatch is the construction decision. A fresh root receipt after independent
construction review is the matched decision. Commands never silently retry.
"""
import argparse
import base64
import datetime
import json
import os
from pathlib import Path
import platform
import re
import shutil
import stat
import sys
import tarfile
import time
import urllib.request
import urllib.parse

sys.dont_write_bytecode = True
from hosted_contract import (BASELINE, CANDIDATE, CANONICAL, GO_ARCHIVE, GO_BYTES,
    GO_SHA256, POLICY, TESTS, TEST_REGEX, accepted_window, admission, capacity, inventory,
    observe_files, released, root_acceptance, seal, snapshot_files, strict_json, utc, verify_seal, verify_snapshot)
from hosted_custody import StickySignals, durable, group_exists, monitored_release, supervise
from hosted_functional import functional_events
from protocol import (build_inputs, build_toolchain, config, digest, drift,
    harness_manifest, identity, need, now, process_environment, row, schedule,
    sha, toolchain_inventory, validate_build_command, validate_go_environment, write, git_object_id)
from build import git_source_authority, objects, tree_inventory, verify_git_receipt
from prepare_config import draft

ROOT_ACTOR_ID = 1981537
REPOSITORY = 'snissn/gomap'
ACCEPTANCE_ISSUE = 5076
TREES = {'baseline': '19c8d7f1265878af4a4d9fdce52ff238c07ab4a0',
         'candidate': '52e5581718a776971d9c0f7b679d2be6d1e9f087'}
LABELS = ['race-compiler-version', 'race-go-environment',
    'candidate-characterization-normal', 'candidate-characterization-race',
    'baseline-build', 'baseline-128x', 'candidate-build', 'candidate-128x',
    'host-isolation-tests22', 'load-readiness-tests25', 'actual-linux-census', 'freeze-c3-config']


def json_read(path):
    return strict_json(Path(path).read_bytes())


def path_under(value, root):
    value, root = Path(value), Path(root)
    need(value.is_absolute() and value.resolve() == value and value.is_relative_to(root)
         and not value.is_symlink(), 'path escapes physical owned namespace')
    return value


def state_read(args):
    need(sha(args.state) == args.state_sha256, 'state hash drift')
    state = json_read(args.state)
    need(state['repository'] == REPOSITORY and state['root_actor_id'] == ROOT_ACTOR_ID
         and state['dispatch_actor_id'] == ROOT_ACTOR_ID, 'root dispatch identity mismatch')
    n = Path(state['namespace'])
    need(n.resolve() == n and not n.is_symlink() and n.is_dir(), 'physical namespace required')
    need(args.state == n / 'state.json', 'state outside owned namespace')
    need(state['policy'] == POLICY and state['baseline'] == BASELINE
         and state['candidate'] == CANDIDATE, 'literal source/policy mismatch')
    for name, descriptor in state['helpers'].items():
        file = Path(__file__).with_name(name)
        need(file.is_file() and not file.is_symlink() and sha(file) == descriptor['sha256']
             and stat.S_IMODE(file.stat().st_mode) == descriptor['mode'], 'hosted helper drift: ' + name)
    for name, expected in state['canonical'].items():
        need(sha(Path(__file__).with_name(name)) == expected, 'canonical caller drift: ' + name)
    return state


def clean_command(receipt):
    need(receipt['started_child'] and receipt['child_joined'] and receipt['custody_released']
         and receipt['exit_code'] == 0 and not receipt['timed_out'] and not receipt['signals'],
         receipt['label'] + ' failed; no retry')


def run(state, label, argv, cwd, timeout, out, sticky, env=None, proof=None, cleanup=False, input_data=None):
    if not cleanup:
        admission(state)
    need(not sticky.signals, 'cancellation before child')
    receipt = supervise(label, argv, cwd, env or process_environment(state['controls']),
                        timeout, out, sticky, owner_proof=proof, input_data=input_data)
    clean_command(receipt)
    return receipt


def safe_extract(archive, destination, prefix=''):
    """Git/official tar input only: no links, special files or traversal."""
    destination.mkdir(exist_ok=False)
    with tarfile.open(archive, 'r:*') as stream:
        seen = set()
        for member in stream:
            name = member.name.rstrip('/')
            need(name and name not in seen and not name.startswith('/')
                 and '..' not in Path(name).parts and str(Path(name)) == name,
                 'unsafe/duplicate archive member')
            seen.add(name)
            if prefix:
                need(name == prefix or name.startswith(prefix + '/'), 'unexpected archive prefix')
                name = name[len(prefix):].lstrip('/')
                if not name:
                    need(member.isdir(), 'archive root is not directory')
                    continue
            target = destination / name
            need(member.isdir() or member.isfile(), 'archive symlink/hardlink/special member refused')
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                need(member.mode in (0o644, 0o755), 'archive file mode refused')
                target.parent.mkdir(parents=True, exist_ok=True)
                with target.open('xb') as output:
                    shutil.copyfileobj(stream.extractfile(member), output)
                target.chmod(member.mode)



def tooling_authority(state, checkout, out, sticky, environment):
    """Hash-verified Git ancestry for only the executing tooling/workflow files."""
    prefix=['git','--no-replace-objects','-C',str(checkout)]
    run(state,'tooling-commit',prefix+['cat-file','commit',state['workflow_sha']],checkout,120,out,sticky,environment)
    commit=(out/'tooling-commit.stdout').read_bytes()
    need(git_object_id('commit',commit,'sha1')==state['workflow_sha'],'landed tooling commit mismatch')
    tree=commit.split(b'\n',1)[0].removeprefix(b'tree ').decode()
    run(state,'tooling-trees',prefix+['ls-tree','-r','-t','-z','--full-tree',tree],checkout,120,out,sticky,environment)
    trees={tree}
    for entry in (out/'tooling-trees.stdout').read_bytes().split(b'\0'):
        if entry:
            mode,kind,oid=entry.split(b'\t',1)[0].split(b' ')
            if kind==b'tree':trees.add(oid.decode())
    run(state,'tooling-tree-objects',prefix+['cat-file','--batch'],checkout,120,out,sticky,environment,
        input_data=('\n'.join(sorted(trees))+'\n').encode())
    payload=(out/'tooling-tree-objects.stdout').read_bytes();objects={};offset=0
    for oid in sorted(trees):
        newline=payload.find(b'\n',offset);header=payload[offset:newline].split()
        need(newline>=offset and len(header)==3 and header[:2]==[oid.encode(),b'tree'],'tooling batch header')
        size=int(header[2]);start=newline+1;end=start+size
        need(size>=0 and end<len(payload) and payload[end:end+1]==b'\n','tooling batch length')
        objects[oid]=base64.b64encode(payload[start:end]).decode();offset=end+1
    need(offset==len(payload),'extra tooling batch bytes')
    files=tree_inventory(tree,objects,'sha1')
    selected={}
    names=['scripts/cow_c3_read/'+x for x in list(state['canonical'])+list(state['helpers'])]
    names+=['.github/workflows/cow-c3-hosted-qualification.yml']
    for name in names:
        path=checkout/name;item=files[name];raw=path.read_bytes()
        need(not path.is_symlink() and path.is_file() and stat.S_IMODE(path.stat().st_mode)==int(item['git_mode'][-3:],8)
             and git_object_id('blob',raw,'sha1')==item['git_blob'],'landed executing tooling byte/mode drift: '+name)
        selected[name]=dict(item,sha256=sha(path),bytes=len(raw))
    proof=dict(head=state['workflow_sha'],tree=tree,commit_object=base64.b64encode(commit).decode(),
        tree_objects=objects,files=selected)
    durable(out/'tooling-git-proof.json',proof)
    state['tooling_proof_sha256']=sha(out/'tooling-git-proof.json')


def capacity_stage(args):
    n = args.namespace
    need(n.is_absolute() and not n.exists() and not n.is_symlink(), 'fresh namespace required')
    n.mkdir()
    (n / 'evidence').mkdir()
    temporary = n / 'tmp'
    temporary.mkdir()
    observed = dict(system=platform.system(), machine=platform.machine(), cpu_count=os.cpu_count(),
        affinity=sorted(os.sched_getaffinity(0)), free_bytes=shutil.disk_usage(temporary).free,
        tmpdir=str(temporary), device=temporary.stat().st_dev, at=now(),
        job_started_utc=args.job_started_utc, job_started_monotonic=args.job_started_monotonic,
        runtime_started=False, capacity_only=True)
    durable(n / 'evidence/capacity.json', observed)
    capacity(observed)
    print(json.dumps(observed), flush=True)


def prepare(args):
    n = args.namespace
    observed = json_read(n / 'evidence/capacity.json')
    capacity(observed)
    need(args.repository == REPOSITORY and args.actor_id == ROOT_ACTOR_ID
         and args.ref == 'refs/heads/main' and args.event == 'workflow_dispatch',
         'full construction requires root manual dispatch of landed main')
    need(re.fullmatch(r'[0-9a-f]{40}', args.workflow_sha) and args.workflow_sha == args.checkout_sha,
         'workflow/checkout source identity mismatch')
    need(re.fullmatch(r'[A-Za-z0-9_-]{1,80}', args.attempt) and args.run_id > 0 and args.run_attempt > 0,
         'explicit finite run/attempt required')
    start = utc(observed['job_started_utc'])
    state = dict(schema='gomap-hosted-c3-state-v1', namespace=str(n), repository=REPOSITORY,
        workflow_sha=args.workflow_sha, run_id=args.run_id, run_attempt=args.run_attempt,
        attempt=args.attempt, root_actor_id=ROOT_ACTOR_ID, dispatch_actor_id=args.actor_id,
        baseline=BASELINE, candidate=CANDIDATE, policy=POLICY,
        window_start_utc=start.isoformat(), window_end_utc=(start + datetime.timedelta(seconds=19800)).isoformat(),
        admission_deadline_monotonic=observed['job_started_monotonic'] + 19800,
        outer_deadline_monotonic=observed['job_started_monotonic'] + 20700,
        git_repository=str(args.checkout), canonical={x: sha(Path(__file__).with_name(x)) for x in CANONICAL},
        helpers={p.name: {'sha256':sha(p), 'mode':stat.S_IMODE(p.stat().st_mode)}
                 for p in sorted(Path(__file__).parent.glob('hosted_*.py'))},
        variants={k: dict(source=str(n / k), build=str(n / 'evidence' / (k + '-build')),
                  head=h, tree=TREES[k]) for k,h in [('baseline',BASELINE),('candidate',CANDIDATE)]},
        controls=dict(GOROOT=str(n / 'go'), GOCACHE=str(n / 'cache'),
            GOMODCACHE=str(n / 'gopath/pkg/mod'), TMPDIR=str(n / 'tmp'),
            GOWORK='off', GOMAXPROCS='4', GOGC='100', GOMEMLIMIT='off', GOFLAGS=''))
    admission(state, 10200)
    durable(n/'evidence/preparation-started.json',dict(at=now(),runtime_started=True,Go_executions=0))
    bootstrap = n / 'evidence/bootstrap'; bootstrap.mkdir()
    bootstrap_env = dict(PATH=os.defpath, LC_ALL='C', GIT_CONFIG_NOSYSTEM='1', GIT_CONFIG_GLOBAL=os.devnull)
    with StickySignals() as sticky:
        tooling_authority(state,args.checkout,bootstrap,sticky,bootstrap_env)
        for label, v in state['variants'].items():
            run(state, label+'-fetch', ['git','--no-replace-objects','-C',str(args.checkout),
                'fetch','--no-tags','origin',v['head']],args.checkout,120,bootstrap,sticky,bootstrap_env)
            archive = run(state, label+'-export', ['git','--no-replace-objects','-C',str(args.checkout),
                'archive','--format=tar',v['head']],args.checkout,120,bootstrap,sticky,bootstrap_env)
            safe_extract(bootstrap / (label+'-export.stdout'),Path(v['source']))
            run(state,label+'-authority',[sys.executable,'-B',str(Path(__file__)),
                'authority','--source',v['source'],'--repository',str(args.checkout),
                '--head',v['head'],'--tree',v['tree'],'--out',str(bootstrap),'--label',label],
                args.checkout,120,bootstrap,sticky,bootstrap_env)
            for name, expected in state['canonical'].items():
                need(sha(Path(v['source'])/'scripts/cow_c3_read'/name)==expected,'canonical seven product/caller mismatch')
        archive = bootstrap / GO_ARCHIVE
        url='https://go.dev/dl/' + GO_ARCHIVE
        deadline=time.monotonic()+120
        with urllib.request.urlopen(url,timeout=30) as response, archive.open('xb') as output:
            need(response.geturl().startswith('https://'),'non-HTTPS official toolchain')
            count=0
            while True:
                need(time.monotonic()<deadline,'finite official archive download exhausted')
                raw=response.read(1<<20)
                if not raw:break
                count+=len(raw);need(count<=GO_BYTES,'oversized official archive')
                output.write(raw)
        need(archive.stat().st_size==GO_BYTES and sha(archive)==GO_SHA256,'official Go1.26.8 archive drift')
        safe_extract(archive,n/'go',prefix='go')
        state['full_toolchain'] = inventory(n/'go')
        need(len(state['full_toolchain']) == 15036, 'complete official Go1.26.8 file census')
        compiler=[Path(shutil.which(name) or '').resolve() for name in ('gcc','as','ld')]
        state['compiler_files']=snapshot_files(compiler)
        state['CC']=str(compiler[0])
        need(not sticky.signals,'bootstrap cancelled')
    durable(n/'state.json',state)
    shutil.copyfile(n/'state.json',n/'evidence/hosted-state.json')
    helper_dir=n/'evidence/hosted-controllers';helper_dir.mkdir()
    for name in state['helpers']:shutil.copyfile(Path(__file__).with_name(name),helper_dir/name)
    shutil.copyfile(args.checkout/'.github/workflows/cow-c3-hosted-qualification.yml',helper_dir/'cow-c3-hosted-qualification.yml')
    durable(bootstrap/'preparation-result.json',dict(status='PREPARED_NOT_CONSTRUCTION_ACCEPTED',
        official_archive_sha256=GO_SHA256, full_toolchain_files=15036,
        declared_compiler_files=3, complete_C_closure=False, state_sha256=sha(n/'state.json'),
        matched_runs=0, native_qualification=False))
    if args.github_output:
        with args.github_output.open('a') as output:
            output.write('state_sha256='+sha(n/'state.json')+'\n')


def full_refresh(state, observations=None):
    proof=Path(state['namespace'])/'evidence/bootstrap/tooling-git-proof.json'
    need(sha(proof)==state['tooling_proof_sha256'],'executing tooling Git proof drift')
    for name,item in json_read(proof)['files'].items():
        file=Path(state['git_repository'])/name
        need(not file.is_symlink() and sha(file)==item['sha256']
             and stat.S_IMODE(file.stat().st_mode)==int(item['git_mode'][-3:],8), 'landed tooling drift')
    for label,v in state['variants'].items():
        root=Path(state['namespace'])/'evidence/bootstrap'
        manifest=json_read(root/(label+'-manifest.json'))
        verify_git_receipt(v['source'],manifest,json_read(root/(label+'-git.json')))
        for item in manifest['files']:
            need(stat.S_IMODE((Path(v['source'])/item['path']).stat().st_mode)==int(item['git_mode'][-3:],8),
                 'exact exported file mode drift')
        for name,expected in state['canonical'].items():
            need(sha(Path(v['source'])/'scripts/cow_c3_read'/name)==expected,'canonical script drift')
    actual_go=inventory(Path(state['controls']['GOROOT']))
    if observations is not None:
        observations.mkdir(exist_ok=False)
        write(observations/'full-toolchain-observed.json',actual_go)
    actual_compiler,compiler_errors=observe_files(list(state['compiler_files']))
    if observations is not None:
        write(observations/'declared-compiler-observed.json',actual_compiler)
        write(observations/'declared-compiler-observation-errors.json',compiler_errors)
    need(not compiler_errors, 'declared compiler observation failed')
    # Retain the actual observations before comparison; drift is failure evidence.
    verify_snapshot(actual_go,state['full_toolchain'],'full Go toolchain')
    verify_snapshot(actual_compiler,state['compiler_files'],'declared compiler')


def host_admit(state, directory, label):
    from collect import host_snapshot
    from protocol import benchmark_comms, validate_census_file
    env=process_environment(state['controls'])
    snapshot=host_snapshot(directory,label,Path(state['controls']['TMPDIR']),Path(state['variants']['baseline']['source']),env)
    capacity(dict(system=snapshot['uname']['system'],machine=snapshot['uname']['machine'],
                  cpu_count=snapshot['cpu_count'],affinity=snapshot['cpu_affinity'],free_bytes=snapshot['free_bytes']))
    need(snapshot['load'][0] <= 5.0 and snapshot['load'][1] <= 5.0,'host contention admission refused')
    from protocol import linux_comm
    names=sorted({linux_comm(v['build']+'/cowbench-normal.test') for v in state['variants'].values()})
    validate_census_file(directory/(label+'-processes.txt'),snapshot['processes_sha256'],benchmark_names=names)
    return snapshot


def build_validate(state, label, directory):
    v=state['variants'][label]; build=Path(v['build']); controls=state['controls']
    receipt=json_read(build/'build-receipt.json'); binary=build/'cowbench-normal.test'
    validate_build_command(receipt,str(Path(controls['GOROOT'])/'bin/go'),str(binary),'c3')
    need(len(receipt['artifacts'])==12,'complete twelve-artifact build provenance')
    for item in receipt['artifacts'].values():
        path=path_under(item['path'],build)
        need(path.parent==build and path.is_file() and sha(path)==item['sha256'],'build artifact drift')
    need(sha(binary)==receipt['binary_sha256'],'binary drift')
    validate_go_environment(json_read(build/'go-env.stdout'),process_environment(controls))
    ident=identity(build/'source-manifest.json')
    verify_git_receipt(v['source'],ident['original_manifest'],json_read(build/'git-source-authority.json'))
    packages=objects((build/'compiled-dependencies.stdout').read_text())
    harness=harness_manifest(packages,json_read(build/'compiled-input-closure.json'),ident,v['source'],controls,'c3')
    need(receipt['harness_input_identity']==digest(harness),'complete fixture identity drift')
    frozen=dict(external_input_identity=receipt['external_input_identity'],fixtures=harness,
                cases=[{'package':draft()['cases'][0]['package']}])
    build_inputs(receipt,frozen,packages,json_read(build/'compiled-inputs-before.json'),
        json_read(build/'compiled-input-closure.json'),json_read(build/'generated-nonpersistent-inputs.json'),v['source'],ident)
    write(directory/(label+'-build-validation.json'),dict(artifacts=12,complete_harness=harness,
        harness_input_identity=digest(harness),receipt_sha256=sha(build/'build-receipt.json')))
    return harness


def warm_validate(state,label,out):
    lines=(out/(label+'-128x.stdout')).read_text().splitlines()
    metadata=[x for x in lines if x.startswith(('goos:','goarch:','pkg:','cpu:'))]
    rows=[x for x in lines if x.startswith('Benchmark')]
    need(len(rows)==54 and sum(x=='PASS' for x in lines)==1
         and all(not x.strip() or x=='PASS' or x in metadata or x in rows for x in lines), 'full54 warm stdout census')
    decoded=[]
    for case in draft()['cases']:
        selected=[x for x in rows if re.match(re.escape(case['benchmark'])+r'-4\s',x)]
        need(len(selected)==1,'missing/duplicate128x warm cell')
        path=out/(label+'-'+case['id']+'.stdout')
        path.write_text('\n'.join(metadata+selected+['PASS'])+'\n')
        decoded.append(dict(case=case['id'],row=row(path,out/(label+'-128x.stderr'),case,label,'warmup'),
                            original_stdout_sha256=sha(out/(label+'-128x.stdout'))))
    write(out/(label+'-decoded128.json'),decoded)


def construct(args):
    state=state_read(args); admission(state,10200)
    n=Path(state['namespace']); out=n/'evidence/construction'; out.mkdir(exist_ok=False)
    commands=[]; error=None; closing=False
    with StickySignals() as sticky:
        try:
            full_refresh(state,out/'source-before'); host_admit(state,out,'initial')
            write(out/'controls.json',state['controls'])
            env=process_environment(state['controls']); race=dict(env,CGO_ENABLED='1',CC=state['CC'])
            source=Path(state['variants']['candidate']['source']); go=str(Path(env['GOROOT'])/'bin/go')
            def command(label,argv,cwd,timeout=600,effective=None,proof=None):
                host_admit(state,out,label+'-admission')
                receipt=run(state,label,argv,cwd,timeout,out,sticky,effective or env,proof)
                commands.append(receipt); write(out/'commands.json',commands)
                return receipt
            command(LABELS[0],[state['CC'],'--version'],source,30,race)
            command(LABELS[1],[go,'env','-json'],source,60,race)
            observed=json_read(out/'race-go-environment.stdout')
            validate_go_environment(observed,race);need(observed['CC']==state['CC'],'actual CC mismatch')
            fresh=[]
            for mode in ('normal','race'):
                argv=[go,'test','-json','-trimpath']+(['-race','-x'] if mode=='race' else [])+[
                    './TreeDB/mvcc','-run',TEST_REGEX,'-count=1','-timeout='+('180s' if mode=='race' else '120s')]
                label='candidate-characterization-'+mode
                command(label,argv,source,900 if mode=='race' else 600,race if mode=='race' else env)
                parsed=functional_events((out/(label+'.stdout')).read_text(),TESTS,'github.com/snissn/gomap/TreeDB/mvcc')
                fresh.append(dict(parsed,mode=mode,argv=argv,environment=race if mode=='race' else env,
                    stdout_sha256=sha(out/(label+'.stdout')),stderr_sha256=sha(out/(label+'.stderr'))))
            write(out/'fresh-characterization.json',dict(status='FRESH_NORMAL_RACE_16_PASS',rows=fresh,
                candidate=CANDIDATE,compiler_files=state['compiler_files'],full_C_closure=False))
            fixtures={}
            for label,v in state['variants'].items():
                source=Path(v['source']); binary=Path(v['build'])/'cowbench-normal.test'
                command(label+'-build',[sys.executable,'-B',str(source/'scripts/cow_c3_read/build.py'),
                    '--suite','c3','--source',str(source),'--git-head',v['head'],'--git-tree',v['tree'],
                    '--git-repository',state['git_repository'],'--controls',str(out/'controls.json'),'--out',v['build']],source)
                fixtures[label]=build_validate(state,label,out)
                command(label+'-128x',[str(binary),'-test.run=^$','-test.bench=^BenchmarkC3PublicReadAdmission$',
                    '-test.benchtime=128x','-test.benchmem','-test.count=1','-test.timeout=5m'],source)
                warm_validate(state,label,out)
            need(fixtures['baseline']==fixtures['candidate'],'complete baseline/candidate fixture mismatch')
            source=Path(state['variants']['baseline']['source'])
            for name,count,label in [('host_isolation_test.py',22,'host-isolation-tests22'),
                                      ('load_readiness_test.py',25,'load-readiness-tests25')]:
                command(label,[sys.executable,'-B',str(source/'scripts/cow_c3_read'/name)],source)
                log=(out/(label+'.stderr')).read_text()
                need(re.search(r'Ran '+str(count)+r' tests in ',log) and re.search(r'^OK$',log,re.M),'risk case census mismatch')
            census_settings=out/'census-settings.json'; write(census_settings,state)
            command('actual-linux-census',[sys.executable,'-B',str(Path(__file__).with_name('hosted_census.py')),
                str(census_settings),str(out/'live-census')],source,600,proof=lambda r:monitored_release(out/'live-census'))
            positive=json_read(out/'live-census/result.json')
            need(positive['all_children_joined'] is True and positive['samples']>=2
                 and positive['Go_executions']==0,'actual census proof incomplete')
            command('freeze-c3-config',[sys.executable,'-B',str(Path(__file__)),'freeze',
                '--state',str(args.state),'--state-sha256',args.state_sha256],source)
            released(commands,LABELS)
        except BaseException as exc:
            error=dict(type=type(exc).__name__,error=str(exc),hidden_retries=0)
            write(out/'failure.json',error)
        finally:
            try:
                full_refresh(state,out/'source-final'); closing=True
                host_admit(state,out,'finally')
            except BaseException as exc:
                write(out/'closing-failure.json',dict(type=type(exc).__name__,error=str(exc)))
                closing=False
            # Read durable supervisor files even if a child failed before append.
            actual=[json_read(out/(label+'.json')) for label in LABELS if (out/(label+'.json')).exists()]
            custody=all(x['custody_released'] for x in actual) and not sticky.signals
            write(out/'closure.json',dict(closed_utc=now(),source_toolchain_compiler_unchanged=closing,
                all_owned_children_joined=custody,signals=sticky.signals,commands=actual,
                status='CLOSED_PASS' if error is None and closing and custody else 'CLOSED_FAILURE',
                matched_runs=0,native_qualification=False,performance_acceptance=False))
    manifest_sha=seal(n/'evidence',n/'evidence/construction-manifest.json')
    package(n/'evidence',n/'construction.tar.gz')
    result=dict(manifest_sha256=manifest_sha,config_sha256=sha(out/'config.json') if (out/'config.json').exists() else None,
                closed_utc=json_read(out/'closure.json')['closed_utc'],archive_sha256=sha(n/'construction.tar.gz'),
                status=json_read(out/'closure.json')['status'])
    durable(n/'construction-result.json',result)
    need(result['status']=='CLOSED_PASS','construction failure preserved; no matched launch')


def freeze(args):
    state=state_read(args); out=Path(state['namespace'])/'evidence/construction'
    commands=json_read(out/'commands.json'); released(commands,LABELS[:11])
    full_refresh(state)
    c=draft(); c.update(status='frozen-approved',coordinator_acceptance=
        'Root manual hosted construction dispatch; independent same-VM construction receipt required before matched launch')
    c['environment']=state['controls']
    c['noise_policy'].update(max_spread_fraction=.30,material_regression_fraction=.05,minimum_effect_fraction=.10)
    u=platform.uname(); tmp=Path(state['controls']['TMPDIR'])
    c['host'].update(system=u.system,node=u.node,machine=u.machine,release=u.release,
        cpu_count=os.cpu_count(),cpu_affinity=sorted(os.sched_getaffinity(0)),max_load1=5.0,max_load5=5.0,
        min_free_bytes=20<<30,tmpdir=str(tmp),tmpdir_device=tmp.stat().st_dev)
    builds={};fixtures={}
    for label,v in state['variants'].items():
        build=Path(v['build']); receipt=json_read(build/'build-receipt.json');builds[label]=receipt
        fixtures[label]=build_validate(state,label,out)
        ident=identity(build/'source-manifest.json')
        c['variants'][label].update(production_commit=v['head'],production_git_tree=v['tree'],source=v['source'],
            manifest=str(build/'source-manifest.json'),manifest_sha256=sha(build/'source-manifest.json'),
            source_tree_sha256=ident['tree_sha256'],binary=str(build/'cowbench-normal.test'),binary_sha256=receipt['binary_sha256'],
            build_receipt=str(build/'build-receipt.json'),build_receipt_sha256=sha(build/'build-receipt.json'))
        need(receipt['environment']==state['controls'],'canonical build environment mismatch')
        need(len(json_read(out/(label+'-decoded128.json')))==54,'full warm census missing')
    for field in ('go_version','go_binary_sha256','toolchain_identity','effective_module_identity','external_input_identity'):
        need(builds['baseline'][field]==builds['candidate'][field],'unmatched '+field)
    need(builds['baseline']['go_version']=='go version go1.26.8 linux/amd64','exact Go version required')
    c.update(go_binary=str(Path(state['controls']['GOROOT'])/'bin/go'),**{k:builds['baseline'][k]
        for k in ('go_version','go_binary_sha256','toolchain_identity','external_input_identity')})
    need(fixtures['baseline']==fixtures['candidate'],'complete fixture mismatch')
    c['fixtures']=fixtures['baseline']
    for build in builds.values():build_toolchain(build,c,toolchain_inventory(state['controls']['GOROOT']))
    write(out/'config.json',c);config(out/'config.json')
    need(len(schedule(c))==756,'original full schedule required')
    write(out/'freeze-receipt.json',dict(config_sha256=sha(out/'config.json'),policy=POLICY,
        script_bindings=state['canonical'],state_sha256=args.state_sha256,source_heads=[BASELINE,CANDIDATE],
        matched_runs=0,native_qualification=False))


def package(directory,destination):
    # dereference=True materializes generated regular hardlinked Go-cache proofs.
    # inventory() refuses links to directories and special files before archiving.
    inventory(directory)
    with tarfile.open(destination,'x:gz',dereference=True) as archive:
        archive.add(directory,arcname='evidence',recursive=True)


def verify_construction(state, construction):
    n=Path(state['namespace'])
    need(construction['status']=='CLOSED_PASS','failed construction cannot be accepted')
    need(sha(n/'evidence/construction-manifest.json')==construction['manifest_sha256'],'construction seal drift')
    verify_seal(n/'evidence',json_read(n/'evidence/construction-manifest.json'))
    need(sha(n/'construction.tar.gz')==construction['archive_sha256'],'construction archive drift')


def accept(args):
    state=state_read(args); n=Path(state['namespace']); out=n/'evidence/acceptance';out.mkdir(exist_ok=False)
    construction=json_read(n/'construction-result.json')
    verify_construction(state,construction)
    started=time.monotonic();polls=[]
    with StickySignals() as sticky:
        try:
            while True:
                admission(state,9600);need(not sticky.signals,'root acceptance wait cancelled')
                host_admit(state,out,'poll-'+str(len(polls)))
                # Read-only public API. Comments are parsed JSON data, never code.
                since=urllib.parse.quote(construction['closed_utc'])
                url=f'https://api.github.com/repos/{REPOSITORY}/issues/{ACCEPTANCE_ISSUE}/comments?per_page=100&since={since}'
                headers={'Accept':'application/vnd.github+json','User-Agent':'gomap-c3-hosted'}
                token=os.environ.get('HOSTED_READ_TOKEN')
                need(type(token) is str and token,'read-only API token required; never retained')
                headers['Authorization']='Bearer '+token
                request=urllib.request.Request(url,headers=headers)
                with urllib.request.urlopen(request,timeout=30) as response:
                    need('rel="next"' not in response.headers.get('Link',''),'receipt pagination ambiguity')
                    raw=response.read(4<<20)
                path=out/('poll-'+str(len(polls))+'.json');path.write_bytes(raw)
                comments=strict_json(raw);need(type(comments) is list,'invalid comment API response')
                matches=[]
                for comment in comments:
                    if type(comment.get('user',{}).get('id')) is not int or comment['user']['id']!=ROOT_ACTOR_ID:continue
                    try:value=strict_json(comment.get('body',''))
                    except (ValueError,TypeError):continue
                    if type(value) is dict and value.get('run_id')==state['run_id'] and value.get('run_attempt')==state['run_attempt']:
                        matches.append((comment,root_acceptance(comment,state,construction)))
                polls.append(dict(at=now(),raw_sha256=sha(path),matches=len(matches)))
                write(out/'polls.json',polls)
                need(len(matches)<=1,'ambiguous root receipts refused')
                if matches:
                    comment,value=matches[0]
                    full_refresh(state);verify_construction(state,construction)
                    admission(accepted_window(state,value),9600)
                    durable(n/'root-acceptance.json',dict(api_comment=comment,receipt=value,
                        raw_poll_sha256=sha(path),state_sha256=args.state_sha256,
                        observed_at=now(),no_Go_children_active=True))
                    return
                need(time.monotonic()-started<3600,'finite one-hour root receipt wait exhausted')
                time.sleep(15)
        except BaseException as exc:
            write(out/'failure.json',dict(type=type(exc).__name__,error=str(exc),signals=sticky.signals,no_retry=True))
            raise


def matched(args):
    state=state_read(args);n=Path(state['namespace']);admission(state,9600)
    acceptance=json_read(n/'root-acceptance.json')
    construction=json_read(n/'construction-result.json')
    root=root_acceptance(acceptance['api_comment'],state,construction)
    verify_construction(state,construction)
    state=accepted_window(state,root)
    admission(state,9600);full_refresh(state,n/'evidence/matched-source-before')
    # A spent claim is retained for every outcome. Reruns need a new VM/attempt.
    durable(n/'matched-launch.claim',dict(run_id=state['run_id'],run_attempt=state['run_attempt'],
        attempt=state['attempt'],root_receipt_sha256=sha(n/'root-acceptance.json'),at=now()))
    out=n/'evidence';host_admit(state,out,'matched-initial')
    directory=out/'matched';driver=out/'matched-driver';driver.mkdir()
    error=None
    with StickySignals() as sticky:
        try:
            timeout=max(1,state['outer_deadline_monotonic']-time.monotonic())
            # This finite orchestration hang limit is distinct from the cooperative
            # 330-minute admission cutoff. The 345-minute hang ceiling leaves
            # ten minutes for analysis and five for export; unknown custody stays HELD.
            receipt=supervise('collect',[sys.executable,'-B',str(Path(__file__).with_name('hosted_collect.py')),
                '--state',str(args.state),'--state-sha256',args.state_sha256,
                '--config',str(out/'construction/config.json'),'--out',str(directory)],
                state['variants']['baseline']['source'],process_environment(state['controls']),timeout,driver,sticky,
                owner_proof=lambda r:monitored_release(directory))
            clean_command(receipt)
            run(state,'offline-analysis',[sys.executable,'-B',str(Path(__file__).with_name('analyze.py')),str(directory)],
                state['variants']['baseline']['source'],600,driver,sticky,cleanup=True)
        except BaseException as exc:
            error=dict(type=type(exc).__name__,error=str(exc));write(driver/'failure.json',error)
        finally:
            try:full_refresh(state,n/'evidence/matched-source-final');closing=True
            except BaseException as exc:closing=False;write(driver/'source-final-failure.json',dict(error=str(exc)))
            actual=[json_read(p) for p in driver.glob('*.json') if p.name in ('collect.json','offline-analysis.json')]
            write(driver/'closure.json',dict(status='CLOSED_PASS' if error is None and closing and not sticky.signals else 'CLOSED_FAILURE',
                all_owned_children_joined=bool(actual) and all(r['custody_released'] for r in actual),
                source_toolchain_compiler_unchanged=closing,signals=sticky.signals,
                performance_acceptance=False,native_qualification=False,exclusions=[],retry=False))
    need(error is None and closing and not sticky.signals,'matched failure or cancellation retained; no partial performance qualification')


def close(args):
    """Always export evidence; an interrupted/hard-killed lane remains incomplete."""
    n=args.namespace; evidence=n/'evidence';need(evidence.is_dir(),'no evidence namespace')
    status='INCOMPLETE_CUSTODY_HELD'; sources=False;joins=False;errors=[]
    if (n/'state.json').exists():
        state=json_read(n/'state.json')
        try:full_refresh(state);sources=True
        except BaseException as exc:errors.append(str(exc))
        receipts=[]
        for path in evidence.rglob('*.json'):
            try:value=json_read(path)
            except (ValueError,OSError):continue
            if type(value) is dict and 'custody_released' in value and 'owned_pid' in value:
                receipts.append(value)
        joins=bool(receipts) and all(r['custody_released'] and (r['owned_pid'] is None or not group_exists(r['owned_pid'])) for r in receipts)
        if (evidence/'construction').exists():
            closure=evidence/'construction/closure.json'
            joins=joins and closure.is_file() and json_read(closure)['all_owned_children_joined'] is True
        if (n/'matched-launch.claim').exists():
            closure=evidence/'matched-driver/closure.json'
            joins=joins and closure.is_file() and json_read(closure)['all_owned_children_joined'] is True
            joins=joins and monitored_release(evidence/'matched')
        if sources and joins:status='OWNED_RELEASE_ONLY'
    elif not (evidence/'preparation-started.json').exists():
        status='CAPACITY_ONLY_NO_GO'; joins=True
    else:
        # A missing final state after bootstrap cannot establish complete custody.
        errors.append('bootstrap incomplete; no release or construction claim')
    for name in ('state.json','construction-result.json','root-acceptance.json','matched-launch.claim'):
        original=n/name
        if original.exists():
            need(original.is_file() and not original.is_symlink(),'nonregular root authority')
            shutil.copyfile(original,evidence/('final-'+name))
    durable(evidence/'hosted-final-closure.json',dict(status=status,source_toolchain_compiler_unchanged=sources,
        all_owned_children_joined=joins,errors=errors,performance_acceptance=False,native_qualification=False,
        universal_host_exclusivity=False,hard_timeout_or_cancel_never_qualifies=True))
    seal(evidence,n/'final-manifest.json')
    shutil.copyfile(n/'final-manifest.json',evidence/'final-manifest.json')
    package(evidence,n/'final-evidence.tar.gz')
    durable(n/'final-export.json',dict(status=status,manifest_sha256=sha(n/'final-manifest.json'),
        archive_sha256=sha(n/'final-evidence.tar.gz'),archive_bytes=(n/'final-evidence.tar.gz').stat().st_size,
        native_qualification=False,performance_acceptance=False))


def authority(args):
    manifest,proof=git_source_authority(args.source,args.repository,args.head,args.tree)
    durable(args.out/(args.label+'-manifest.json'),manifest)
    durable(args.out/(args.label+'-git.json'),proof)


def parser():
    p=argparse.ArgumentParser(description=__doc__); sub=p.add_subparsers(dest='command',required=True)
    cap=sub.add_parser('capacity');cap.add_argument('--namespace',type=Path,required=True)
    cap.add_argument('--job-started-utc',required=True);cap.add_argument('--job-started-monotonic',type=float,required=True)
    prep=sub.add_parser('prepare');prep.add_argument('--namespace',type=Path,required=True)
    for name in ('repository','ref','event','workflow-sha','checkout-sha','attempt'):prep.add_argument('--'+name,required=True)
    for name in ('actor-id','run-id','run-attempt'):prep.add_argument('--'+name,type=int,required=True)
    prep.add_argument('--checkout',type=Path,required=True);prep.add_argument('--github-output',type=Path)
    for name in ('construct','freeze','accept','matched'):
        child=sub.add_parser(name);child.add_argument('--state',type=Path,required=True);child.add_argument('--state-sha256',required=True)
    auth=sub.add_parser('authority')
    for name in ('source','repository','out'):auth.add_argument('--'+name,type=Path,required=True)
    for name in ('head','tree','label'):auth.add_argument('--'+name,required=True)
    end=sub.add_parser('close');end.add_argument('--namespace',type=Path,required=True)
    return p


if __name__=='__main__':
    args=parser().parse_args()
    {'capacity':capacity_stage,'prepare':prepare,'construct':construct,'freeze':freeze,
     'accept':accept,'matched':matched,'close':close,'authority':authority}[args.command](args)
