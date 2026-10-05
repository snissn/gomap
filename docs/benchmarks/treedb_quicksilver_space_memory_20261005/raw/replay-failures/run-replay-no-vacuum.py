"""Root-only diagnostic replay caller, separate from public capture acceptance."""
import hashlib,json,os,pathlib,shutil,subprocess,sys,time
root=pathlib.Path('/mnt/fast4tb/quicksilver-space-memory-20261005')
packet=root/'5004-replay-diagnostic';plan=json.load(open(packet/'command-plan.json'))
output=root/'replay-3m-no-vacuum';db=root/'working-dbs/replay-3m-postfailure-vacuumoff'
def sha(p):return hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest()
def run(label,variant,mode):
 cell=output/label;cell.mkdir(parents=True)
 binary=root/'bin'/('unified-bench-replay-'+variant)
 builds=json.load(open(packet/'native-builds.json'));build=next(a for a in builds if a['source']['binary']=='unified-bench-'+variant)
 assert sha(binary)==build['binary_sha256']
 env={k:v for k,v in os.environ.items() if not k.startswith(('TREEDB_','GOMAP_QS_'))}
 env.update(plan['environment'],TREEDB_DISABLE_BACKGROUND_INDEX_VACUUM='1',GOMAP_QS_REPLAY_MODE=mode,GOMAP_QS_REPLAY_DB=str(db),GOMAP_QS_REPLAY_JSONL=str(cell/'effects.jsonl'),LD_PRELOAD='',LD_AUDIT='')
 argv=['/usr/bin/time','-v',str(binary),*plan['args']]
 if mode=='read':
  v2=root/'5004-replay-diagnostic-v2'
  argv=['/usr/bin/time','-v','python3',str(v2/'inplace.py'),'run','--owned-root',str(root),'--manifest',str(output/'seal.json'),'--out',str(cell/'guard'),'--',str(binary),*plan['args']]
 receipt=dict(diagnostic_only=True,mode=mode,variant=variant,command=argv,env={k:env[k] for k in set(plan['environment'])|{'GOMAP_QS_REPLAY_MODE','GOMAP_QS_REPLAY_DB','GOMAP_QS_REPLAY_JSONL','TREEDB_DISABLE_BACKGROUND_INDEX_VACUUM','LD_PRELOAD','LD_AUDIT'}},source=build['source'],binary_sha256=sha(binary),build_receipt_sha256=sha(packet/'native-builds.json'),plan_sha256=sha(packet/'command-plan.json'),caller_sha256=sha(__file__),started=time.time(),load_before=os.getloadavg())
 with (cell/('caller.stdout.txt' if mode=='read' else 'stdout.json')).open('w') as out,(cell/'stderr.log').open('w') as err:
  p=subprocess.run(argv,env=env,stdout=out,stderr=err,timeout=1800)
 if mode=='read':
  if (cell/'guard/stdout.json').exists():shutil.copyfile(cell/'guard/stdout.json',cell/'stdout.json')
  receipt.update(guard_method_sha256=sha(v2/'inplace.py'),physical_seal_sha256=sha(output/'seal.json'),guard_source_receipt_sha256=sha(v2/'source-receipt.json'))
 receipt.update(rc=p.returncode,finished=time.time(),load_after=os.getloadavg(),stdout_sha256=sha(cell/'stdout.json'),stderr_sha256=sha(cell/'stderr.log'))
 (cell/'run.json').write_text(json.dumps(receipt,indent=2)+'\n')
 assert p.returncode==0,(cell/'stderr.log').read_text()[-5000:]
 rows=json.load(open(cell/'stdout.json'));assert len(rows)==1
 a=rows[0];assert a['initial_verified_keys']==3000000 and a['initial_verified_misses']==6030000
 assert not a['profiled'] and a['gomaxprocs']==12
 if mode=='create':assert not a['phases']
 else:
  assert [p['name'] for p in a['phases']]==['quicksilver_hits','quicksilver_misses','quicksilver_mixed']
  assert all(p['ops']==6000000 for p in a['phases'])
 receipt['validated_initial_oracle_and_phase_shape']=True
 (cell/'run.json').write_text(json.dumps(receipt,indent=2)+'\n')
 print(json.dumps(dict(label=label,mode=mode,rc=p.returncode,initial_oracle='PASS',mops=[round(p['ops_per_sec']/1e6,4) for p in (a['phases'] or [])],p99=[p['p99_us'] for p in (a['phases'] or [])])),flush=True)
 return cell
if __name__=='__main__':
 if sys.argv[1]=='create':
  db.mkdir();run('create','baseline','create')
 else:run(sys.argv[1],sys.argv[2],'read')
