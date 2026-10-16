import os,sys,pathlib,json,subprocess,hashlib,datetime,time,tarfile,signal
D=pathlib.Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');N=D/'native';A=D/'native-build/atomic-completion-source'/sys.argv[1];A.mkdir();(A/'tmp').mkdir()
def manifest(name):
 names=subprocess.check_output(['git','ls-files','--cached','--others','--exclude-standard','-z'],cwd=N).split(b'\0')
 data={n.decode():hashlib.sha256((N/n.decode()).read_bytes()).hexdigest() for n in names if n and (N/n.decode()).is_file()}
 (A/name).write_text(json.dumps(data,sort_keys=True,indent=2)+'\n');return data
base=json.loads((D/'native-build/current-previsibility-4aa7e/command.json').read_text());env=os.environ.copy();env.update(base['env']);env['TMPDIR']=str(A/'tmp');argv=[str(D/'toolchains/go1.26.3/bin/go'),'test','-p=1']+sys.argv[2:]
cmd={'argv':argv,'env':{k:env[k] for k in base['env']},'outer_timeout_seconds':230,'at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'compiler_sha256':hashlib.sha256(pathlib.Path(argv[0]).read_bytes()).hexdigest()};(A/'command.json').write_text(json.dumps(cmd,indent=2)+'\n');before=manifest('source-before.json')
with tarfile.open(A/'source-before.tar.gz','w:gz') as archive:
 for name in sorted(before): archive.add(N/name,arcname=name,recursive=False)
with tarfile.open(A/'source-before.tar.gz','r:gz') as archive:
 recovered={item.name:hashlib.sha256(archive.extractfile(item).read()).hexdigest() for item in archive if item.isfile()}
if recovered != before: raise RuntimeError('archive does not bind exact source')
(A/'source-archive.json').write_text(json.dumps({'path':'source-before.tar.gz','sha256':hashlib.sha256((A/'source-before.tar.gz').read_bytes()).hexdigest(),'files':len(recovered),'matches_before':True},indent=2)+'\n')
start=time.monotonic()
with (A/'run.log').open('w') as log:
 p=subprocess.Popen(argv,cwd=N,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
 pgid=os.getpgid(p.pid)
 (A/'process.json').write_text(json.dumps({'pid':p.pid,'pgid':pgid,'session':os.getsid(p.pid),'spawned_at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()},indent=2)+'\n')
 try:rc=p.wait(timeout=230)
 except subprocess.TimeoutExpired:
  os.killpg(pgid,signal.SIGKILL);p.wait();rc=124
try:
 os.killpg(pgid,0);group_empty=False
except ProcessLookupError: group_empty=True
if not group_empty:
 os.killpg(pgid,signal.SIGKILL)
 for attempt in range(100):
  try:os.killpg(pgid,0)
  except ProcessLookupError:group_empty=True;break
  time.sleep(0.01)
after=manifest('source-after.json');result={'returncode':rc,'elapsed_seconds':time.monotonic()-start,'source_equal':before==after,'leader_joined':p.poll() is not None,'group_empty':group_empty,'archive_verified':True,'source_count':len(before)};(A/'result.json').write_text(json.dumps(result,indent=2)+'\n');print(A);print(json.dumps(result));print((A/'run.log').read_text());sys.exit(rc)
