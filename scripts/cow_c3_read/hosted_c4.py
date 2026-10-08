"""Provision a fresh finite C4 construction on an isolated hosted runner.

No matched or native qualification. Every attempt has its own retained namespace.
The exported source must have no Git metadata in any ancestor; process IDs must
match mounted /proc before Go starts. Never repairs those facts by weakening gates.
"""
import gzip,json,os,platform,shutil,sys,tarfile,time
from pathlib import Path
from finite_custody import StickySignals,supervise,durable,sha
from build import git_source_authority,verify_git_receipt
from protocol import process_environment,HOST_ISOLATION,process_census,need

def main():
    repository=Path(sys.argv[1]).resolve();head=sys.argv[2];root=Path(sys.argv[3]).resolve()
    need(len(head)==40 and all(c in '0123456789abcdef' for c in head),'exact source commit required')
    root.mkdir(exist_ok=False);source=root/'source';source.mkdir();receipts=root/'provision';receipts.mkdir()
    sticky=StickySignals();sticky.__enter__();commands=[];failure=None
    # setup-go selects the official pinned distribution; inventory its complete
    # bytes/modes, then the canonical builder verifies actual version/build info.
    go=Path(shutil.which('go')).resolve();goroot=go.parent.parent
    tmp=root/'tmp';tmp.mkdir()
    controls={'GOROOT':str(goroot),'GOCACHE':str(root/'go-cache'),'GOMODCACHE':str(root/'go/pkg/mod'),'GOWORK':'off','GOMAXPROCS':'4','GOGC':'100','GOMEMLIMIT':'off','GOFLAGS':'','TMPDIR':str(tmp)}
    environment=process_environment(controls)
    def toolchain_files():
        files={}
        for p in sorted(goroot.rglob('*')):
            if p.is_file() or p.is_symlink():
                need(not p.is_symlink(),'toolchain symlink requires explicit policy')
                files[p.relative_to(goroot).as_posix()]={'sha256':sha(p),'bytes':p.stat().st_size,'mode':p.stat().st_mode & 0o777}
        return files
    def run(label,argv,timeout=600,env=None):
        r=supervise(label,argv,repository,environment if env is None else env,timeout,receipts,sticky)
        commands.append(r)
        need(r['started_child'] and r['custody_released'] and r['exit_code']==0 and not r['timed_out'] and not sticky.signals,label+' refused; no retry')
        return r
    try:
        need(platform.system()=='Linux' and platform.machine()=='x86_64','supported host required')
        need(os.readlink('/proc/self')==str(os.getpid()) and Path('/proc/'+str(os.getpid())+'/status').is_file(),'PID/proc namespace mismatch')
        for p in [source,*source.parents]:need(not (p/'.git').exists() and not (p/'.git').is_symlink(),'Git metadata ancestor of export')
        run('initial-census',['ps','-eo',HOST_ISOLATION['ps_fields'],'--no-headers'],.5)
        process_census((receipts/'initial-census.stdout').read_text())
        need(len(os.sched_getaffinity(0))>=4 and all(x<=5 for x in os.getloadavg()[:2]) and shutil.disk_usage(root).free>=20<<30,'host admission refused')
        run('git-tree',['git','rev-parse',head+'^{tree}'])
        tree=(receipts/'git-tree.stdout').read_text().strip()
        run('git-export',['git','archive','--format=tar','--output='+str(root/'source.tar'),head])
        with tarfile.open(root/'source.tar') as archive:archive.extractall(source,filter='data')
        (root/'source.tar').unlink() # task-owned export transport, actual source retained
        manifest,authority=git_source_authority(source,repository,head,tree)
        inventory=toolchain_files()
        durable(root/'functional-toolchain-before.json',inventory)
        run('go-version',[str(go),'version'])
        version=(receipts/'go-version.stdout').read_text().strip()
        need(version=='go version go1.26.8 linux/amd64','pinned official Go1.26.8 required')
        testargs=[str(go),'test','-json','-count=1','-timeout=4m','./TreeDB/mvcc/cowsustained']
        # Functional commands run from the same detached export, not checkout.
        def functional(label,argv,env):
            r=supervise(label,argv,source,env,600,receipts,sticky);commands.append(r)
            need(r['started_child'] and r['custody_released'] and r['exit_code']==0 and not r['timed_out'] and not sticky.signals,label+' failed; no construction')
            verify_git_receipt(source,manifest,authority)
        functional('normal',testargs,environment)
        raceenv=dict(environment,CGO_ENABLED='1')
        functional('race',testargs[:2]+['-race']+testargs[2:],raceenv)
        after_toolchain=toolchain_files();need(after_toolchain==inventory,'functional toolchain bytes/modes drift')
        durable(root/'functional-toolchain-after.json',after_toolchain)
        functional_proof={'head':head,'tree':tree,'go_version':version,'full_toolchain_before_sha256':sha(root/'functional-toolchain-before.json'),'full_toolchain_after_sha256':sha(root/'functional-toolchain-after.json'),'normal_race_package_pass':True,'commands':[r for r in commands if r['label'] in ('normal','race')],'claim':'actual fresh normal/race with retained Go JSON and equal full toolchain; race CGO_ENABLED=1, canonical normal producer CGO_ENABLED=0; no historical phase identity transfer'}
        durable(root/'functional.json',functional_proof)
        shutil.copyfile(source/'scripts/cow_c3_read/finite_c4.py',root/'execute.py')
        shutil.copyfile(source/'scripts/cow_c3_read/finite_custody.py',root/'custody.py')
        selected=['unrelated-absolute-raw-directory','missing-capture-root','tampered-capture-root','relative-capture-root','missing-raw-directory','mismatched-output-command','float-cycles','overflow-layout-owner','overflow-pointer-bytes','overflow-call-time','float-finite-limit','overflow-backend-counter']
        selected += ['cow-budget-views-'+b for b in ('opened','seed','epoch_1_growth','epoch_1_joined','pinned_checkpoint')]
        selected += ['cow-budget-opened-'+k for k in ('generations','sources','frozen_roots','total_bytes','peak_bytes','control_bytes','history_bytes','retired_bytes','deferred_bytes','reserved_bytes','external_bytes','external_leases','active_cuts','current_roots')]
        selected += ['cow-budget-layout-'+stage+'-'+side for stage in ('seed_layout','checkpoint_layout','reopen_layout') for side in ('owners_before','owners_after')]
        need(len(selected)==37,'refusal scope')
        u=platform.uname();host={k:getattr(u,k) for k in ('system','node','release','machine')};host.update(cpu_count=os.cpu_count(),cpu_affinity=sorted(os.sched_getaffinity(0)))
        binding={'head':head,'tree':tree,'repository':str(repository),'source':str(source),'manifest':manifest,'git_authority':authority,'full_toolchain':inventory,'go_version':version,'controls':controls,'host':host,'refusals':selected,'launch_not_after':time.time()+300,'bound_files':{n:{'sha256':sha(root/n)} for n in ('execute.py','custody.py','functional.json')},'owner':'graph5044 dedicated GitHub hosted construction job','candidate_only':True,'retry_count':0,'native_qualification':False,'runtime_window_max_seconds':16320,'source_custody':'private detached export; no concurrent source writers'}
        durable(root/'binding.json',binding)
        (root/'binding.json.gz').write_bytes(gzip.compress((root/'binding.json').read_bytes(),mtime=0))
        # Outer requires runtime-finish descendant release before mandatory closer.
        def proof(r):
            try:
                v=json.loads((root/'outer-finish.json').read_text())
                return v['both_actions_joined'] is True
            except (OSError,ValueError,KeyError):return False
        r=supervise('finite-outer',[sys.executable,'-B',str(root/'execute.py'),'outer',sha(root/'binding.json')],root,environment,16320,receipts,sticky,owner_proof=proof);commands.append(r)
        need(r['started_child'] and r['custody_released'] and r['exit_code']==0 and not r['timed_out'] and not sticky.signals,'finite outer failed; retain outcome')
        final=json.loads((root/'outer-finish.json').read_text());need(final['functional_pass'] is True and not final['signals'],'final actual joined acceptance')
    except BaseException as error:
        failure={'type':type(error).__name__,'error':str(error)}
    finally:
        durable(root/'hosted-finish.json',{'commands':commands,'failure':failure,'signals':sticky.signals,'functional_pass':failure is None and not sticky.signals,'custody_released':all(r['custody_released'] for r in commands),'acceptance_requires':'actual hosted bootstrap exit0, clean outer-finish, archive/manifest independent verification','matched_runs':0,'native_qualification':False})
        sticky.__exit__(None,None,None)
    return int(failure is not None or bool(sticky.signals))

if __name__=='__main__':sys.exit(main())
