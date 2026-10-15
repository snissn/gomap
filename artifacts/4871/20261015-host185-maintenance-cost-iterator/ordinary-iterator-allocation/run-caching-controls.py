import pathlib
A=pathlib.Path('/mnt/fast4tb/gomap-mvcc-4871-20261015/ordinary-iterator-allocation')
source=(A/'run-validation.py').read_text(); exec(source[:source.index('initial=manifest()')])
initial=json.loads((A/'freeze-before.json').read_text()); selected=json.loads((A/'environment.json').read_text()); env=os.environ.copy(); env.update(selected)
# Called only after the first sequential batch exits, avoiding competing runtime.
validation=json.loads((A/'validation.json').read_text()); assert validation['complete'] and validation['all_pass']
results=[]; regex='^TestCOW(BasisCompletionWaitsForActualOriginalIterator|BasisPostcaptureRefusalJoinsPendingOriginalIterator|BasisOwnedIteratorScrubsConsumedWrapperAliases|PointerImmediatePointAndIteratorOldCut|SnapshotIteratorWrapperAdmissionAndLifetime)$'
for race in (False,True):
 name='caching-'+('race' if race else 'normal'); argv=[str(G/'bin/go'),'test','-p=1']+(['-race'] if race else [])+['./TreeDB/caching','-run',regex,'-count=1','-timeout=120s','-json']; path=A/'logs'/name; path.mkdir(exist_ok=True); before=manifest(); (path/'source-before.json').write_text(json.dumps(before,indent=2)+'\n'); command={'argv':argv,'cwd':str(W),'env':selected,'outer_timeout_seconds':150,'started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()}; (path/'command.json').write_text(json.dumps(command,indent=2)+'\n'); start=time.monotonic(); terminated=False
 with (path/'stdout.txt').open('wb') as out,(path/'stderr.txt').open('wb') as err:
  proc=subprocess.Popen(argv,cwd=W,env=env,stdout=out,stderr=err,start_new_session=True)
  try: code=proc.wait(timeout=150)
  except subprocess.TimeoutExpired:
   terminated=True; os.killpg(proc.pid,signal.SIGTERM)
   try: code=proc.wait(timeout=5)
   except subprocess.TimeoutExpired: os.killpg(proc.pid,signal.SIGKILL); code=proc.wait(timeout=5)
 after=manifest(); (path/'source-after.json').write_text(json.dumps(after,indent=2)+'\n'); events=[]
 for line in (path/'stdout.txt').read_text(errors='replace').splitlines():
  try: events.append(json.loads(line))
  except json.JSONDecodeError: pass
 summary={'name':name,'exit':code,'elapsed_seconds':time.monotonic()-start,'terminated_group':terminated,'source_equal':before==after==initial,'top_pass':[e['Test'] for e in events if e.get('Action')=='pass' and 'Test' in e and '/' not in e['Test']],'sub_pass':sum(e.get('Action')=='pass' and '/' in e.get('Test','') for e in events),'skips':[e.get('Test') for e in events if e.get('Action')=='skip'],'failures':[e.get('Test',e.get('Package')) for e in events if e.get('Action')=='fail'],'data_race_reports':(path/'stdout.txt').read_text(errors='replace').count('WARNING: DATA RACE'),'stdout_sha256':digest((path/'stdout.txt').read_bytes()),'stderr_sha256':digest((path/'stderr.txt').read_bytes()),'command':command}; results.append(summary); (path/'result.json').write_text(json.dumps(summary,indent=2)+'\n'); (A/'caching-validation.json').write_text(json.dumps({'checks':results},indent=2)+'\n'); print(json.dumps({'name':name,'exit':code,'source_equal':summary['source_equal'],'failures':summary['failures']}),flush=True)
 if code!=0 or not summary['source_equal']: break
final=manifest(); (A/'freeze-after.json').write_text(json.dumps(final,indent=2)+'\n'); validation['checks']+=results; validation['caching_complete']=len(results)==2; validation['all_source_equal']=initial==final; validation['all_pass']=validation['all_pass'] and len(results)==2 and all(x['exit']==0 and x['source_equal'] for x in results); (A/'validation.json').write_text(json.dumps(validation,indent=2)+'\n')
