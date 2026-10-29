import importlib.util, json, pathlib, signal, subprocess, sys
ROOT=pathlib.Path('/mnt/fast4tb/gomap-mongo-execution-20261024')
WORK=ROOT/'consumer-canonical'
PACKET=pathlib.Path(__file__).parent
spec=importlib.util.spec_from_file_location('owned_process',ROOT/'evidence/native-query-work-witness1/run.py')
H=importlib.util.module_from_spec(spec);spec.loader.exec_module(H)
signal.signal(signal.SIGTERM,H.interrupted);signal.signal(signal.SIGINT,H.interrupted)
PLAN=json.loads((PACKET/'plan.json').read_text())
GO_ROOT=pathlib.Path(PLAN['toolchain']);GO=GO_ROOT/'bin/go'
ELF=ROOT/'evidence/current-feature-service-write-diagnostic2/documentservice.test'
def source():
    paths=subprocess.check_output(['git','diff','--name-only','HEAD'],cwd=WORK,text=True).splitlines()
    paths+=subprocess.check_output(['git','ls-files','--others','--exclude-standard'],cwd=WORK,text=True).splitlines()
    return {'head':H.git(WORK,'rev-parse','HEAD'),'tree':H.git(WORK,'rev-parse','HEAD^{tree}'),'staged_tree':H.git(WORK,'write-tree'),'status':H.git(WORK,'status','--porcelain'),'files':{n:H.digest(WORK/n) if (WORK/n).exists() else None for n in sorted(set(paths))},'harness_sha256':H.digest(WORK/'TreeDB/documentservice/shared_cow_write_bench_test.go')}
before=source()
expected=json.loads((ROOT/'evidence/current-feature-service-write-diagnostic2/source-binding.json').read_text())
assert before==expected and H.digest(ELF)==PLAN['elf_sha256']
assert not (PACKET/'result.json').exists()
H.write_json(PACKET/'source-before.json',before)
env=H.safe_env({'GOROOT':str(GO_ROOT),'GOTOOLCHAIN':'local','GOMAXPROCS':'4','GOCACHE':str(ROOT/'gocache'),'TMPDIR':str(ROOT/'tmp')})
receipts=[]
for n,cell in enumerate(PLAN['fixed_cells'],1):
    assert source()==before and H.digest(ELF)==PLAN['elf_sha256']
    arm,instrument,loads=cell['arm'],cell['instrument'],cell['loads']
    stem=f'{n:02}-{arm}-{instrument}'
    artifact=PACKET/(stem+'.pprof')
    argv=[str(ELF),'-test.v','-test.run=^$','-test.bench=^BenchmarkServiceSharedCOWWriteLoad$/^writers1$','-test.benchmem',f'-test.benchtime={loads}x','-test.count=1','-test.timeout=3m']
    if instrument=='cpu':argv+=['-test.cpuprofile='+str(artifact)]
    else:argv+=['-test.memprofile='+str(artifact),'-test.memprofilerate=1']
    result=H.run_process(argv,str(WORK),dict(env,GOMAP_SERVICE_BENCH_BACKEND=arm,GOMAP_SERVICE_BENCH_PROFILE='command_wal_durable'),PACKET/(stem+'.log'),240)
    raw=(PACKET/(stem+'.log')).read_text()
    result.update(cell=cell,source_unchanged=source()==before,elf_sha256=H.digest(ELF),profile_sha256=H.digest(artifact) if artifact.exists() else None,activation_verified=f'backend={arm} profile=command_wal_durable requested_memtable=cow_btree resolved_strategy=column_graph metric=cosine rows=4096 batch=128 writers=1 defer_rebuild=true' in raw,requests_verified=f'backend={arm} profile=command_wal_durable writers=1 loads={loads} attempted={32*loads} completed={32*loads} request_retries=0' in raw)
    H.write_json(PACKET/(stem+'-receipt.json'),result);receipts.append(result)
    print(json.dumps({k:result[k] for k in ('cell','process_ok','source_unchanged','activation_verified','requests_verified','elapsed_seconds','log_sha256','profile_sha256')}),flush=True)
    assert result['process_ok'] and result['source_unchanged'] and result['activation_verified'] and result['requests_verified'],'failed diagnostic; retain and stop rather than retry'
    kinds=['cpu'] if instrument=='cpu' else ['alloc_space','alloc_objects','inuse_space']
    for kind in kinds:
        for focus in ('all','upsert'):
            analysis=[str(GO),'tool','pprof','-top','-nodecount=40','-sample_index='+kind]
            if focus=='upsert':analysis+=['-focus=UpsertDocuments']
            analysis+=[str(ELF),str(artifact)]
            report=PACKET/(stem+'-'+kind+'-'+focus+'.txt')
            checked=H.run_process(analysis,str(WORK),env,report,60)
            H.write_json(PACKET/(report.stem+'-receipt.json'),checked)
            assert checked['process_ok']
result={'valid':all(x['process_ok'] and x['source_unchanged'] and x['activation_verified'] and x['requests_verified'] for x in receipts),'source_unchanged':source()==before,'qualification':None,'retries':0,'scope':'Whole process profiles include stopped-timer setup, all read/Optimize/reopen/Close phases and background work. Upsert focus is stack attribution, not an exact timed-window allocation delta; retained profiles preserve unmatched work. Instrumented timing is not acceptance or comparable uninstrumented timing.','cells':receipts}
H.write_json(PACKET/'result.json',result);H.write_json(PACKET/'source-after.json',source())
print(json.dumps({'valid':result['valid'],'source_unchanged':result['source_unchanged'],'qualification':None}),flush=True)
