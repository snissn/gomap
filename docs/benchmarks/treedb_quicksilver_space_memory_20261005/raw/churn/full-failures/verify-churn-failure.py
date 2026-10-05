import hashlib,json,os,pathlib,signal,subprocess,time
r=pathlib.Path('/mnt/fast4tb/quicksilver-space-memory-20261005')
p=pathlib.Path(__import__('sys').argv[1]).resolve(); assert p.is_relative_to(r); m=json.loads((p/'run.json').read_text()); report=json.loads((p/'stdout.json').read_text())[0]
db=pathlib.Path(report['data_dir']); out=r/__import__('sys').argv[2]/'readonly-after-failure'
assert db.parent==r/'working-dbs' and db.is_dir() and not out.exists()
out.mkdir()
for f in sorted(db.rglob('manifest.json')):
 dest=out/'failed-manifests'/f.relative_to(db);dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(f.read_bytes())
def census():
 return [(str(f.relative_to(db)),s.st_size,s.st_blocks,s.st_mtime_ns) for f in sorted(db.rglob('*')) if f.is_file() for s in [f.stat()]]
binary=r/'bin/diagnostic-readonly-oracle'; prior=json.loads((r/'maintenance-3m-diagnostic/commands.json').read_text())
assert hashlib.sha256(binary.read_bytes()).hexdigest()==next(x['binary_sha256'] for x in prior if x['label']=='build-readonly-oracle')
args=iter(m['command']);next(args);cmd=[str(binary)]
for a in args:
 if a in ('-profile-dir','-quicksilver-churn-rounds','-quicksilver-churn-pause'):next(args)
 elif a!='-keep':cmd.append(a)
cmd+=['-quicksilver-verify-dir',str(db)]
env={k:v for k,v in os.environ.items() if not k.startswith('TREEDB_')};env.update(m['env']);before=census();start=time.time()
with (out/'stdout.json').open('w') as stdout,(out/'stderr.txt').open('w') as stderr:
 proc=subprocess.Popen(cmd,env=env,stdout=stdout,stderr=stderr,start_new_session=True)
 try:rc=proc.wait(timeout=1800)
 except subprocess.TimeoutExpired:
  os.killpg(proc.pid,signal.SIGTERM)
  try:proc.wait(timeout=10)
  except subprocess.TimeoutExpired:os.killpg(proc.pid,signal.SIGKILL);proc.wait()
  raise
after=census(); proof=json.loads((out/'stdout.json').read_text()) if rc==0 else None
receipt=dict(command=cmd,rc=rc,seconds=time.time()-start,before=before,after=after,unchanged=before==after,proof=proof,binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),method_sha256=hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest())
(out/'receipt.json').write_text(json.dumps(receipt,indent=2)+'\n')
assert rc==0 and before==after and proof['verification_only'] and proof['verified_keys']==3000000
print('PASS_READONLY_ALL3M_CHURN_FAILURE_UNCHANGED',round(receipt['seconds'],2),flush=True)
