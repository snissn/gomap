import os,sys,pathlib,json,subprocess,hashlib,datetime,time
D=pathlib.Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');N=D/'native';A=D/'native-build/growth-yield-repair'/sys.argv[1];A.mkdir();(A/'tmp').mkdir()
def manifest(name):
 names=subprocess.check_output(['git','ls-files','--cached','--others','--exclude-standard','-z'],cwd=N).split(b'\0')
 data={n.decode():hashlib.sha256((N/n.decode()).read_bytes()).hexdigest() for n in names if n and (N/n.decode()).is_file()}
 (A/name).write_text(json.dumps(data,sort_keys=True,indent=2)+'\n');return data
base=json.loads((D/'native-build/current-previsibility-4aa7e/command.json').read_text());env=os.environ.copy();env.update(base['env']);env['TMPDIR']=str(A/'tmp');argv=[str(D/'toolchains/go1.26.3/bin/go'),'test','-p=1']+sys.argv[2:]
cmd={'argv':argv,'env':{k:env[k] for k in base['env']},'outer_timeout_seconds':230,'at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'compiler_sha256':hashlib.sha256(pathlib.Path(argv[0]).read_bytes()).hexdigest()};(A/'command.json').write_text(json.dumps(cmd,indent=2)+'\n');before=manifest('source-before.json');start=time.monotonic()
with (A/'run.log').open('w') as log:
 p=subprocess.Popen(argv,cwd=N,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
 try:rc=p.wait(timeout=230)
 except subprocess.TimeoutExpired:
  import signal;os.killpg(p.pid,signal.SIGKILL);p.wait();rc=124
after=manifest('source-after.json');result={'returncode':rc,'elapsed_seconds':time.monotonic()-start,'source_equal':before==after,'child_joined':p.poll() is not None,'source_count':len(before)};(A/'result.json').write_text(json.dumps(result,indent=2)+'\n');print(A);print(json.dumps(result));print((A/'run.log').read_text());sys.exit(rc)
