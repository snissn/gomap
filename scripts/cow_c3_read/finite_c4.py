"""Fresh local C4 construction. Once only; every child and closure is bounded."""
import fcntl,json,os,platform,shutil,sys,tarfile,time
from pathlib import Path
from custody import StickySignals,supervise,monitored_release,durable,sha,read_release
ROOT=Path(__file__).resolve().parent
assert len(sys.argv)==3 and sha(ROOT/'binding.json')==sys.argv[2], 'exact external binding SHA required'
B=json.loads((ROOT/'binding.json').read_text())
SOURCE=Path(B['source']);OUT=ROOT/'admission';BUILD=ROOT/'build'
sys.path.insert(0,str(SOURCE/'scripts/cow_c3_read'))
from build import verify_git_receipt,objects
from protocol import need,process_environment,identity,digest,harness_manifest,toolchain_inventory,build_inputs,validate_build_command,validate_go_environment,build_toolchain,HOST_ISOLATION,process_census,linux_comm
import c4_protocol as c4
from prepare_c4_config import draft
ENV=process_environment(B['controls'])

def frozen():
    for name,row in B['bound_files'].items():
        need(sha(ROOT/name)==row['sha256'],'binding drift: '+name)
    verify_git_receipt(SOURCE,B['manifest'],B['git_authority'])
    for row in B['manifest']['files']:
        need((SOURCE/row['path']).stat().st_mode & 0o777 == (0o755 if row['git_mode']=='100755' else 0o644),'source mode drift')
    tool=Path(B['controls']['GOROOT']);inventory=B['full_toolchain']
    actual={p.relative_to(tool).as_posix() for p in tool.rglob('*') if p.is_file() or p.is_symlink()}
    need(actual==set(inventory),'full toolchain set drift')
    for name,row in inventory.items():
        p=tool/name
        need(not p.is_symlink() and sha(p)==row['sha256'] and p.stat().st_size==row['bytes'] and p.stat().st_mode & 0o777==row['mode'],'full toolchain drift: '+name)

def endpoint(label,directory,sticky):
    r=supervise(label,['ps','-eo',HOST_ISOLATION['ps_fields'],'--no-headers'],SOURCE,ENV,.5,directory,sticky,cleanup=True)
    need(r['started_child'] and r['custody_released'] and r['exit_code']==0 and not r['timed_out'],'observer custody failed')
    process_census((directory/(label+'.stdout')).read_text(),benchmark_names=(linux_comm(str(BUILD/'cowsustained-normal.test')),))
    u=platform.uname();h=B['host']
    need(all(getattr(u,k)==h[k] for k in ('system','node','release','machine')) and os.cpu_count()==h['cpu_count'] and sorted(os.sched_getaffinity(0))==h['cpu_affinity'],'host identity drift')
    load=list(os.getloadavg());free=shutil.disk_usage(ROOT).free
    durable(directory/(label+'-host.json'),{'load':load,'free_bytes':free,'host':h,'claim':'sampled observation, not exclusive-host proof'})
    need(load[0]<=5 and load[1]<=5 and free>=20<<30,'quiet load/storage admission refused')

def runtime():
    OUT.mkdir(exist_ok=False);commands=[];sticky=StickySignals();sticky.__enter__();errors=[]
    def run(label,argv,timeout=600,proof=None):
        r=supervise(label,argv,SOURCE,ENV,timeout,OUT,sticky,owner_proof=proof)
        commands.append(r)
        need(r['started_child'] and r['custody_released'] and r['exit_code']==0 and not r['timed_out'] and not sticky.signals,label+' failed; no retry')
    try:
        need(time.time()<B['launch_not_after'],'owner window expired')
        frozen();endpoint('initial-census',OUT,sticky)
        durable(OUT/'controls.json',B['controls'])
        scripts=SOURCE/'scripts/cow_c3_read'
        run('build',[sys.executable,'-B',str(scripts/'build.py'),'--suite','c4','--source',str(SOURCE),'--git-head',B['head'],'--git-tree',B['tree'],'--git-repository',B['repository'],'--controls',str(OUT/'controls.json'),'--out',str(BUILD)])
        receipt=json.loads((BUILD/'build-receipt.json').read_text());binary=BUILD/'cowsustained-normal.test'
        need(sha(binary)==receipt['binary_sha256'] and receipt['go_version']==B['go_version'],'actual fresh binary/toolchain')
        validate_build_command(receipt,str(Path(B['controls']['GOROOT'])/'bin/go'),str(binary),'c4')
        need(len(receipt['artifacts'])==12,'complete twelve provenance artifacts')
        for a in receipt['artifacts'].values():
            p=Path(a['path']);need(p.parent==BUILD and p.is_file() and not p.is_symlink() and sha(p)==a['sha256'],'build artifact drift')
        validate_go_environment(json.loads((BUILD/'go-env.stdout').read_text()),ENV)
        ident=identity(BUILD/'source-manifest.json')
        verify_git_receipt(SOURCE,ident['original_manifest'],json.loads((BUILD/'git-source-authority.json').read_text()))
        packages=objects((BUILD/'compiled-dependencies.stdout').read_text())
        harness=harness_manifest(packages,json.loads((BUILD/'compiled-input-closure.json').read_text()),ident,str(SOURCE),B['controls'],'c4')
        need(digest(harness)==receipt['harness_input_identity'],'complete harness identity')
        build_inputs(receipt,{'external_input_identity':receipt['external_input_identity'],'fixtures':harness,'cases':[{'package':c4.C4_PACKAGE}]},packages,json.loads((BUILD/'compiled-inputs-before.json').read_text()),json.loads((BUILD/'compiled-input-closure.json').read_text()),json.loads((BUILD/'generated-nonpersistent-inputs.json').read_text()),str(SOURCE),ident)
        config=draft();config.update(status='frozen-approved',coordinator_acceptance='Fresh finite ordinary construction approved by graph coordinator; no matched/native acceptance',go_binary=str(Path(B['controls']['GOROOT'])/'bin/go'),go_version=receipt['go_version'],go_binary_sha256=receipt['go_binary_sha256'],toolchain_identity=receipt['toolchain_identity'],external_input_identity=receipt['external_input_identity'],environment=B['controls'],fixtures=harness)
        tmp=Path(B['controls']['TMPDIR']);config['host'].update(**B['host'],max_load1=5,max_load5=5,min_free_bytes=20<<30,tmpdir=str(tmp),tmpdir_device=tmp.stat().st_dev)
        config['noise_policy'].update(max_spread_fraction=.30,material_regression_fraction=.05,minimum_effect_fraction=.10)
        for v in config['variants'].values():
            v.update(production_commit=B['head'],production_git_tree=B['tree'],source=str(SOURCE),manifest=str(BUILD/'source-manifest.json'),manifest_sha256=sha(BUILD/'source-manifest.json'),source_tree_sha256=ident['tree_sha256'],binary=str(binary),binary_sha256=sha(binary),build_receipt=str(BUILD/'build-receipt.json'),build_receipt_sha256=sha(BUILD/'build-receipt.json'))
        for case in config['cases']:case.update(iterations=1,warmup_iterations=1,workload_contract=c4.workload(case['keys'],1))
        need(config['result_class']=='construction','scope drift');c4.validate_config(config);build_toolchain(receipt,config,toolchain_inventory(B['controls']['GOROOT']))
        durable(OUT/'config.json',config);packet=OUT/'packet'
        run('collect36',[sys.executable,'-B',str(scripts/'collect.py'),'--suite','c4-sustained','--config',str(OUT/'config.json'),'--out',str(packet)],10800,lambda r:monitored_release(packet))
        completion=json.loads((packet/'completion.json').read_text());rows=json.loads((packet/'receipts.json').read_text());c4.validate_completion(completion)
        need(completion['schema']=='gomap-cow-sustained-public-v3' and completion['capture_root']==str(packet) and completion['runs']==36 and len(rows)==36 and all(r['phase']=='construction' and r['variant']=='candidate' and r['row']['iterations']==1 for r in rows),'genuine v3 construction census')
        run('analyze',[sys.executable,'-B',str(scripts/'c4_analyze.py'),str(packet)])
        analysis=json.loads((packet/'c4-analysis-validation.json').read_text());need(analysis['result_class']=='construction' and analysis['runs']==analysis['cases']==36,'analyzer census')
        selected=B['refusals'];refusals=OUT/'refusals'
        run('refusals37',[sys.executable,'-B',str(scripts/'c4_packet_smoke.py'),'--positive',str(packet),'--out',str(refusals)]+[arg for name in selected for arg in ('--case',name)])
        refused=json.loads((refusals/'smoke-results.json').read_text())['results'];need(len(refused)==37 and {r['case'] for r in refused}==set(selected) and all(r['refused'] is True and r['error'] for r in refused),'37 exact copied-packet refusals')
        domain=OUT/'integer-domain'
        run('integer63',[sys.executable,'-B',str(scripts/'c4_integer_domain_smoke.py'),'--positive',str(packet),'--out',str(domain)])
        d=json.loads((domain/'result.json').read_text());need(len(d['controls'])==3 and len(d['results'])==63 and d['failures']==[] and sum(r.get('accepted') is True for r in d['results'])==3 and d['script_sha256']==sha(scripts/'c4_integer_domain_smoke.py') and d['protocol_sha256']==sha(scripts/'c4_protocol.py'),'63 serialized plus3 actual controls')
        durable(OUT/'result.json',{'current_head':B['head'],'current_tree':B['tree'],'commands':5,'build_artifacts':12,'candidate_only_one_epoch_leaves':36,'schema':completion['schema'],'copied_refusals':37,'serialized_checks':63,'actual_controls':3,'normal_race_package_pass':True,'matched_runs':0,'native_qualification':False})
    except BaseException as e:
        errors.append({'type':type(e).__name__,'error':str(e)});durable(OUT/'failure.json',errors)
    finally:
        try:frozen()
        except BaseException as e:errors.append({'scope':'final-frozen','error':str(e)})
        observers=[json.loads(p.read_text()) for p in OUT.glob('*-census.json')]
        released=all(r['custody_released'] for r in commands+observers)
        durable(OUT/'runtime-finish.json',{'custody_released':released,'commands':commands,'errors':errors,'signals':sticky.signals,'all_children_joined':released})
        sticky.__exit__(None,None,None)
    return int(bool(errors or sticky.signals))

def close():
    sticky=StickySignals();sticky.__enter__();errors=[]
    try:
        finish=json.loads((OUT/'runtime-finish.json').read_text());need(finish['custody_released'],'unproven descendants; closure refuses')
        for r in finish['commands']:
            need(r==json.loads((OUT/(r['label']+'.json')).read_text()),'command receipt drift')
            for ext in ('stdout','stderr'):need(sha(OUT/(r['label']+'.'+ext))==r[ext+'_sha256'],'stream drift')
        try:frozen();endpoint('closing-census',ROOT,sticky)
        except BaseException as e:errors.append({'scope':'closing-admission','error':str(e)})
        observer=ROOT/'closing-census.json'
        need(not observer.exists() or json.loads(observer.read_text())['custody_released'],'closing observer held')
        inherited=json.loads((ROOT/'outer-cancel-before-close.json').read_text())['signals']
        passed=(OUT/'result.json').exists() and not (OUT/'failure.json').exists() and not finish['errors'] and not errors and not finish['signals'] and not sticky.signals and not inherited
        durable(ROOT/'custody-release.json',{'custody_released':True,'all_children_joined':True,'inner_functional_pass':passed,'errors':errors,'signals':sticky.signals,'runtime_signals':finish['signals'],'outer_signals_before_closure':inherited,'acceptance_requires':'outer-finish.json after both actual joins and all outer signals','matched_runs':0,'native_qualification':False})
        closed=ROOT/'closed-packet';closed.mkdir(exist_ok=False)
        for name in ('admission','build'):
            if (ROOT/name).exists():shutil.copytree(ROOT/name,closed/name,copy_function=os.link)
        for name in ('binding.json','execute.py','custody.py','functional.json','custody-release.json','outer-cancel-before-close.json','closing-census.json','closing-census.stdout','closing-census.stderr','closing-census-host.json'):
            if (ROOT/name).exists():shutil.copyfile(ROOT/name,closed/name)
        durable(closed/'manifest.json',{'self_excluded':True,'files':[{'path':p.relative_to(closed).as_posix(),'sha256':sha(p),'bytes':p.stat().st_size,'mode':p.stat().st_mode & 0o777} for p in sorted(closed.rglob('*')) if p.is_file()],'omitted':['source','Git repository','Go toolchain'],'outer_streams':'retained separately after outer joins'})
        archive=ROOT/'construction.tar.gz'
        with tarfile.open(archive,'x:gz',dereference=False) as tar:tar.add(closed,arcname='packet')
        passed=passed and not sticky.signals
        durable(ROOT/'handoff.json',{'custody_released':True,'inner_functional_pass':passed,'closure_signals':sticky.signals,'acceptance_requires':'outer-finish.json after both actual joins and all outer signals','archive_sha256':sha(archive),'archive_bytes':archive.stat().st_size,'errors':errors,'matched_runs':0,'native_qualification':False})
        return int(not passed or bool(sticky.signals))
    finally:sticky.__exit__(None,None,None)

def outer():
    lock=os.open(ROOT/'collection.lock',os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600);fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    need(time.time()<B['launch_not_after'],'owner window expired')
    durable(ROOT/'runtime-launch.claim',{'pid':os.getpid(),'at':time.time(),'binding_sha256':sha(ROOT/'binding.json')})
    sticky=StickySignals();sticky.__enter__();records=[]
    try:
        r=supervise('runtime-outer',[sys.executable,'-B',str(ROOT/'execute.py'),'runtime',sys.argv[2]],ROOT,ENV,14400,ROOT,sticky,owner_proof=lambda r:read_release(OUT/'runtime-finish.json'));records.append(r)
        if r['custody_released']:
            durable(ROOT/'outer-cancel-before-close.json',{'signals':list(sticky.signals)})
            records.append(supervise('closure-outer',[sys.executable,'-B',str(ROOT/'execute.py'),'close',sys.argv[2]],ROOT,ENV,1200,ROOT,sticky,cleanup=True,owner_proof=lambda r:read_release(ROOT/'custody-release.json')))
        passed=len(records)==2 and all(r['exit_code']==0 and r['custody_released'] for r in records) and not sticky.signals and json.loads((ROOT/'handoff.json').read_text())['inner_functional_pass']
        durable(ROOT/'outer-finish.json',{'records':records,'signals':sticky.signals,'functional_pass':bool(passed),'both_actions_joined':len(records)==2 and all(r['custody_released'] for r in records),'closure_skipped_if_runtime_custody_unproven':len(records)!=2})
        return int(not passed or bool(sticky.signals))
    finally:sticky.__exit__(None,None,None);os.close(lock)

if __name__=='__main__':sys.exit({'runtime':runtime,'close':close,'outer':outer}[sys.argv[1]]())
