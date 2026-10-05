"""Bind structural diagnostic records and existing controlled full heap profiles."""
import hashlib,json,os,pathlib,subprocess,tarfile,time
root=pathlib.Path('/mnt/fast4tb/quicksilver-space-memory-20261005')
campaign=root/'offsets-3m-structural-diagnostic';out=root/'m-diagnostic-analysis';out.mkdir()
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
env=dict(os.environ);env.update(json.load(open(root/'qualified-manifest-offsets.json'))['build_env'])
go='/home/mikers/.gvm/gos/go1.26.3/bin/go'
records=[json.loads(s) for s in (root/'offsets-3m-diagnostic-cache.jsonl').read_text().splitlines() if s.strip()]
snapshots=sorted((root/'offsets-3m-diagnostic-full-allocs').glob('*.pprof'),key=lambda p:p.stat().st_mtime_ns)
assert len(snapshots)==16
index=[];analyses=[]
for cell in sorted(p for p in campaign.iterdir() if p.is_dir()):
 run=json.load(open(cell/'run.json')); assert run['rc']==0 and run['validated']
 variant='candidate' if 'candidate' in cell.name else 'baseline'
 pids={a['PID'] for a in records if run['started']<=a['UnixNano']/1e9<=run['finished']};assert len(pids)==1
 owned=[p for p in snapshots if run['started']<=p.stat().st_mtime_ns/1e9<=run['finished']];assert len(owned)==8
 for n,p in enumerate(owned):
  kind='base' if n%2==0 else 'after';assert p.name.startswith('quicksilver_allocs_'+kind+'_')
  phase=['hits','misses','mixed','concurrent'][n//2]
  entry=dict(cell=cell.name,pid=next(iter(pids)),phase=phase,kind=kind,path=str(p.relative_to(root)),mtime_ns=p.stat().st_mtime_ns,bytes=p.stat().st_size,sha256=sha(p));index.append(entry)
  binary=root/'bin'/('unified-bench-diag-'+variant)
  argv=[go,'tool','pprof','-top','-nodecount=30','-sample_index=inuse_space',str(binary),str(p)]
  started=time.time();q=subprocess.run(argv,env=env,capture_output=True,text=True)
  dest=out/(cell.name+'-'+phase+'-'+kind+'.top.txt');dest.write_text(q.stdout+q.stderr);assert q.returncode==0,q.stderr
  analyses.append(dict(command=argv,started=started,finished=time.time(),rc=q.returncode,output=str(dest.relative_to(root)),output_sha256=sha(dest),profile_sha256=entry['sha256'],binary_sha256=sha(binary)))
(out/'full-snapshot-index.json').write_text(json.dumps(index,indent=2)+'\n')
(out/'analyses.json').write_text(json.dumps(dict(go_sha256=sha(pathlib.Path(go)),rows=analyses),indent=2)+'\n')
validator=root/'5004-diagnostic-overlay/validate_jsonl.py'
p=subprocess.run(['python3',str(validator),str(root/'offsets-3m-diagnostic-cache.jsonl')],capture_output=True,text=True);(out/'structural-validation.txt').write_text(p.stdout+p.stderr);assert p.returncode==0,p.stderr
files=[p for p in campaign.rglob('*') if p.is_file() and p.name in {'manifest.json','plan.json','source-receipt.json','build-receipt.json','runner-receipt.json','native-receipt.json','run.json','stdout.json','stderr.log','ldd.stdout.txt','ldd.stderr.txt'}]
files+=snapshots+list(out.iterdir())+list(p for p in (root/'5004-diagnostic-overlay').rglob('*') if p.is_file())
files+=[root/'diagnostic-manifest-offsets.json',root/'offsets-3m-diagnostic-cache.jsonl',root/'offsets-3m-structural-diagnostic-memory.jsonl',root/'offsets-3m-structural-diagnostic-plan.json',pathlib.Path(__file__)]
archive=root/'m-diagnostic-3m-raw.tar.gz'
with tarfile.open(archive,'w:gz') as t:
 for p in sorted(set(files)):t.add(p,arcname=str(p.relative_to(root)),recursive=False)
print(json.dumps(dict(archive=str(archive),bytes=archive.stat().st_size,sha256=sha(archive),files=len(set(files)),records=len(records),snapshots=len(index))))
