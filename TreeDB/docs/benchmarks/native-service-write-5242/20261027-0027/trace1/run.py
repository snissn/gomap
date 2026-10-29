import importlib.util,json,pathlib,signal,subprocess
R=pathlib.Path('/mnt/fast4tb/gomap-mongo-execution-20261024');W=R/'consumer-canonical';P=pathlib.Path(__file__).parent
s=importlib.util.spec_from_file_location('H',R/'evidence/native-query-work-witness1/run.py');H=importlib.util.module_from_spec(s);s.loader.exec_module(H)
signal.signal(signal.SIGTERM,H.interrupted);signal.signal(signal.SIGINT,H.interrupted)
plan=json.loads((P/'plan.json').read_text());G=pathlib.Path(plan['toolchain']);E=R/'evidence/current-feature-service-write-diagnostic2/documentservice.test'
def state():
 paths=subprocess.check_output(['git','diff','--name-only','HEAD'],cwd=W,text=True).splitlines()+subprocess.check_output(['git','ls-files','--others','--exclude-standard'],cwd=W,text=True).splitlines()
 return {'head':H.git(W,'rev-parse','HEAD'),'tree':H.git(W,'rev-parse','HEAD^{tree}'),'staged_tree':H.git(W,'write-tree'),'status':H.git(W,'status','--porcelain'),'files':{n:H.digest(W/n) if (W/n).exists() else None for n in sorted(set(paths))},'harness_sha256':H.digest(W/'TreeDB/documentservice/shared_cow_write_bench_test.go')}
before=state();expected=json.loads((R/'evidence/current-feature-service-write-diagnostic2/source-binding.json').read_text());assert before==expected and H.digest(E)==plan['elf_sha256']
env=H.safe_env({'GOROOT':str(G),'GOTOOLCHAIN':'local','GOMAXPROCS':'4','GOCACHE':str(R/'gocache'),'TMPDIR':str(R/'tmp')})
H.write_json(P/'source-before.json',before);results=[]
for i,arm in enumerate(plan['fixed_order'],1):
 stem=f'{i:02}-{arm}';trace=P/(stem+'.trace');assert not trace.exists()
 cmd=[str(E),'-test.v','-test.run=^$','-test.bench=^BenchmarkServiceSharedCOWWriteLoad$/^writers4$','-test.benchtime=1x','-test.benchmem','-test.count=1','-test.timeout=3m','-test.trace='+str(trace)]
 v=H.run_process(cmd,str(W),dict(env,GOMAP_SERVICE_BENCH_BACKEND=arm,GOMAP_SERVICE_BENCH_PROFILE='command_wal_durable'),P/(stem+'.log'),240)
 raw=(P/(stem+'.log')).read_text();v.update(source_unchanged=state()==before,trace_sha256=H.digest(trace),requests_verified=f'backend={arm} profile=command_wal_durable writers=4 loads=1 attempted=32 completed=32 request_retries=0' in raw)
 H.write_json(P/(stem+'-receipt.json'),v);print(json.dumps(v),flush=True);results.append(v)
 assert v['process_ok'] and v['source_unchanged'] and v['requests_verified']
 for typ in ('syscall','sync','sched'):
  profile=P/(stem+'-'+typ+'.pprof')
  a=H.run_process([str(G/'bin/go'),'tool','trace','-pprof='+typ,str(trace)],str(W),env,profile,60);H.write_json(P/(profile.stem+'-receipt.json'),a);assert a['process_ok']
  for focus in ('all','upsert'):
   cmd=[str(G/'bin/go'),'tool','pprof','-top','-nodecount=40']
   if focus=='upsert':cmd+=['-focus=UpsertDocuments']
   cmd+=[str(E),str(profile)]
   report=P/(profile.stem+'-'+focus+'.txt');a=H.run_process(cmd,str(W),env,report,60);H.write_json(P/(report.stem+'-receipt.json'),a);assert a['process_ok']
H.write_json(P/'source-after.json',state());H.write_json(P/'result.json',{'valid':all(v['process_ok'] and v['requests_verified'] and v['source_unchanged'] for v in results),'source_unchanged':state()==before,'qualification':None,'retries':0,'scope':plan['scope']})
