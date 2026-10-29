import datetime, importlib.util, json, pathlib, signal, subprocess, sys
ROOT=pathlib.Path('/mnt/fast4tb/gomap-mongo-execution-20261024')
WORK=ROOT/'consumer-canonical'
PACKET=pathlib.Path(__file__).parent
spec=importlib.util.spec_from_file_location('owned_process',ROOT/'evidence/native-query-work-witness1/run.py')
H=importlib.util.module_from_spec(spec);spec.loader.exec_module(H)
signal.signal(signal.SIGTERM,H.interrupted);signal.signal(signal.SIGINT,H.interrupted)
plan=json.loads((PACKET/'plan.json').read_text())
def source():
    paths=subprocess.check_output(['git','diff','--name-only','HEAD'],cwd=WORK,text=True).splitlines()
    paths+=subprocess.check_output(['git','ls-files','--others','--exclude-standard'],cwd=WORK,text=True).splitlines()
    return {'head':H.git(WORK,'rev-parse','HEAD'),'staged_tree':H.git(WORK,'write-tree'),'status':H.git(WORK,'status','--porcelain'),'files':{n:H.digest(WORK/n) if (WORK/n).exists() else None for n in sorted(set(paths))}}
start=source()
assert start['head']==plan['source_head'] and H.digest(WORK/plan['harness'])==plan['harness_sha256']
assert H.digest(PACKET/'shared_cow_write_bench_test.go')==plan['overlay_sha256']
assert not (PACKET/'receipt.json').exists()
H.write_json(PACKET/'source-before.json',start)
go=pathlib.Path(plan['go_root'])/'bin/go'
env=H.safe_env({'GOROOT':plan['go_root'],'GOTOOLCHAIN':'local','GOMAXPROCS':str(plan['gomaxprocs']),'GOCACHE':str(ROOT/'gocache'),'TMPDIR':str(ROOT/'tmp')})
elf=PACKET/'documentservice.test'
build=H.run_process([str(go),'test','-c','-p=1','-overlay='+str(PACKET/'overlay.json'),'-o',str(elf),'./TreeDB/documentservice'],str(WORK),env,PACKET/'build.log',plan['build_wall_seconds'])
H.write_json(PACKET/'build-receipt.json',build)
assert build['process_ok'] and source()==start
H.write_json(PACKET/'binary-binding.json',{'elf_sha256':H.digest(elf),'go_sha256':H.digest(go),'overlay_sha256':plan['overlay_sha256'],'plan_sha256':H.digest(PACKET/'plan.json'),'runner_sha256':H.digest(pathlib.Path(__file__))})
argv=[str(elf),'-test.v','-test.run=^$','-test.bench='+plan['public_bench'],'-test.benchmem','-test.benchtime=1x','-test.count=1','-test.timeout=3m']
result=H.run_process(argv,str(WORK),dict(env,GOMAP_SERVICE_BENCH_BACKEND='native',GOMAP_SERVICE_BENCH_PROFILE=plan['profile']),PACKET/'diagnostic.log',plan['process_wall_seconds'])
result.update(source_unchanged=source()==start,qualification=None,numeric_gate=None,retries=0,scope=plan['purpose'])
H.write_json(PACKET/'source-after.json',source());H.write_json(PACKET/'receipt.json',result)
print(json.dumps(result),flush=True)
sys.exit(0 if result['process_ok'] and result['source_unchanged'] else 1)
