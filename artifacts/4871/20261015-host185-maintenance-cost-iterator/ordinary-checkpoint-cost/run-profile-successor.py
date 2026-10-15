from pathlib import Path
import datetime,hashlib,json,os,signal,shutil,subprocess,time
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');A=D/'ordinary-iterator-allocation';W=D/'ordinary-repair';OUT=A/'profile-successor';GO=D/'toolchains/go1.26.3/bin/go'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def save(name,value):(OUT/name).write_text(json.dumps(value,indent=2,sort_keys=True)+'\n')
def sources():
 files=subprocess.check_output(['git','ls-files','-z'],cwd=W).split(b'\0')
 return {os.fsdecode(f):sha(W/os.fsdecode(f)) for f in files if f and (W/os.fsdecode(f)).is_file()}
assert os.path.ismount('/mnt/fast4tb') and os.access(D,os.W_OK)
assert not OUT.exists();v=json.loads((A/'validation.json').read_text());assert v['all_pass'] and v['all_source_equal']
assert sha(GO)=='d68b7abbc40d0844f673f6cf06ae3cded225c50437c6454fa37ef178d079fe65'
before=sources();assert before==json.loads((A/'freeze-after.json').read_text())['files']
original=A/'candidate-public.test';assert sha(original)==v['public_elf_sha256']
proposal=D/'ordinary-checkpoint-cost/proposal.json';assert sha(proposal)=='f7c7a70991260e81edcf7f9e0217c40de304a804c31b5bfb785a01c6b91a3ee3'
old=next(x for x in json.loads(proposal.read_text())['proposed_first_stage']['exact_calls'] if x['arm']=='candidate-public')
OUT.mkdir();(OUT/'tmp').mkdir();binary=OUT/'checkpoint.test';shutil.copy2(original,binary);binary.chmod(0o755);assert sha(binary)==sha(original)
argv=['taskset','-c','0,1',str(binary)]+['-test.memprofile='+str(OUT/'alloc.pprof') if x.startswith('-test.memprofile=') else x for x in old['argv'][1:]]
env=dict(os.environ,GOROOT=str(GO.parent.parent),GOTOOLCHAIN='local',GOWORK='off',GOENV='off',GOFLAGS='',GOAMD64='v1',CGO_ENABLED='1',GOMAXPROCS='2',GOGC='100',GODEBUG='',GOMEMLIMIT='8GiB',GOPROXY='off',GOSUMDB='off',TMPDIR=str(OUT/'tmp'))
save('authorization.json',{'coordinator':'/root','at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'maximum_calls':1,'retries':0,'outer_timeout_seconds':150,'original_proposal_sha256':sha(proposal),'source_parent':v['head'],'source_patch_sha256':v['source_patch_sha256'],'freeze_sha256':sha(A/'freeze-after.json'),'binary_sha256':sha(binary),'driver_sha256':sha(Path(__file__)),'purpose':'One exact successor whole-process allocation profile. No timing qualification. Compare discarded-wrapper flat allocation with prior 72e profile; scopes include setup, off-timer work and teardown.'})
save('source-before.json',before);save('environment.json',{k:env[k] for k in ('GOROOT','GOTOOLCHAIN','GOWORK','GOENV','GOFLAGS','GOAMD64','CGO_ENABLED','GOMAXPROCS','GOGC','GODEBUG','GOMEMLIMIT','GOPROXY','GOSUMDB','TMPDIR')})
record={'argv':argv,'cwd':str(W),'started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()};save('pending-command.json',record);start=time.monotonic();failure=None
with (OUT/'stdout.txt').open('wb') as stdout,(OUT/'stderr.txt').open('wb') as stderr:
 child=subprocess.Popen(argv,cwd=W,env=env,stdout=stdout,stderr=stderr,start_new_session=True)
 try:child.wait(timeout=150)
 except BaseException as error:
  failure=repr(error)
  try:os.killpg(child.pid,signal.SIGTERM)
  except ProcessLookupError:pass
  try:child.wait(timeout=2)
  except subprocess.TimeoutExpired:pass
  try:os.killpg(child.pid,signal.SIGKILL)
  except ProcessLookupError:pass
  child.wait()
record.update(exit=child.returncode,elapsed_s=time.monotonic()-start,interrupted=failure,stdout_sha256=sha(OUT/'stdout.txt'),stderr_sha256=sha(OUT/'stderr.txt'));save('command.json',record)
after=sources();save('source-after.json',after)
assert failure is None and child.returncode==0 and before==after
assert 'PASS' in (OUT/'stdout.txt').read_text().splitlines() and (OUT/'alloc.pprof').stat().st_size>0
save('result.json',{'status':'ONE_SUCCESSOR_PROFILE_CAPTURED','calls':1,'source_unchanged':True,'timing_qualifies':False,'profile_sha256':sha(OUT/'alloc.pprof'),'binary_sha256':sha(binary)})
print(json.dumps(record),flush=True)
