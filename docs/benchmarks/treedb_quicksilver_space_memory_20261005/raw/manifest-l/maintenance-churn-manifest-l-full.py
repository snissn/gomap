"""Run the existing public oracle and supported offline compactor on retained DBs."""
import hashlib,json,os,pathlib,signal,subprocess,sys,time

root=pathlib.Path('/mnt/fast4tb/quicksilver-space-memory-20261005')
packet=pathlib.Path(sys.argv[1]).resolve()
assert packet.is_relative_to(root)
metadata=json.loads((packet/'run.json').read_text())
report=json.loads((packet/'stdout.json').read_text())[0]
db=pathlib.Path(report['data_dir']).resolve()
assert db.parent==root/'working-dbs' and db.is_dir() and not db.is_symlink()
out=root/sys.argv[2]; assert out.parent==root and not out.exists(); out.mkdir()
env=os.environ.copy();env.update(metadata['env'])
treemap=root/'bin/treemap-manifest-l'
records=[]

def census(label):
    files=[]
    for p in sorted(db.rglob('*')):
        assert not p.is_symlink()
        if not p.is_file(): continue
        rel=p.relative_to(db);s=p.stat()
        kind=('wal' if 'wal' in rel.parts else 'dictionary' if rel.parts[0]=='dictdb' else
              'outer_leaf' if 'leaf_vlog' in rel.parts else 'user_value' if 'value_vlog' in rel.parts else
              'index' if p.name=='index.db' else 'metadata')
        files.append(dict(path=str(rel),kind=kind,apparent=s.st_size,allocated=s.st_blocks*512))
    result=dict(label=label,files=files,apparent=sum(f['apparent'] for f in files),allocated=sum(f['allocated'] for f in files),
                apparent_excluding_wal=sum(f['apparent'] for f in files if f['kind']!='wal'),
                allocated_excluding_wal=sum(f['allocated'] for f in files if f['kind']!='wal'))
    (out/(label+'-census.json')).write_text(json.dumps(result,indent=2)+'\n')
    print(label,result['apparent_excluding_wal'],result['allocated_excluding_wal'],flush=True)
    return result

def run(label,argv,timeout):
    started=time.time()
    samples=[]
    with (out/(label+'-stdout.json')).open('w') as stdout,(out/(label+'-stderr.txt')).open('w') as stderr:
        p=subprocess.Popen(['/usr/bin/time','-v']+argv,env=env,stdout=stdout,stderr=stderr,start_new_session=True)
        timed_out=False
        try:
            while p.poll() is None:
                if time.time()-started>timeout:raise subprocess.TimeoutExpired(argv,timeout)
                # Maintenance diagnostics only: one bounded census per second.
                if pathlib.Path(argv[0]).name.startswith('treemap'):
                    apparent=allocated=0
                    for f in db.rglob('*'):
                        try:
                            s=f.stat()
                            if f.is_file():apparent+=s.st_size;allocated+=s.st_blocks*512
                        except FileNotFoundError:pass # Concurrent GC removes files.
                    samples.append(dict(time=time.time(),apparent=apparent,allocated=allocated))
                time.sleep(1)
        except subprocess.TimeoutExpired:
            timed_out=True;os.killpg(p.pid,signal.SIGTERM)
            try:p.wait(timeout=10)
            except subprocess.TimeoutExpired:os.killpg(p.pid,signal.SIGKILL);p.wait()
    receipt=dict(label=label,command=argv,started=started,finished=time.time(),rc=p.returncode,timed_out=timed_out,pid=p.pid,disk_samples=samples)
    records.append(receipt);(out/'commands.json').write_text(json.dumps(records,indent=2)+'\n')
    print(label,{k:v for k,v in receipt.items() if k!='disk_samples'},flush=True)
    assert p.returncode==0 and not timed_out, 'FAILED: preserve database; do not reopen normally'
    return json.loads((out/(label+'-stdout.json')).read_text())

oracle=[];args=iter(metadata['command'])
for value in args:
    if value=='-profile-dir':next(args)
    elif value in ('-quicksilver-churn-rounds','-quicksilver-churn-pause'):next(args)
    elif value!='-keep':oracle.append(value)
oracle+=['-quicksilver-verify-dir',str(db)]
def verify(label):
    result=run(label,oracle,1800)
    assert result['verification_only'] and result['verified_keys']==report['verified_keys'] and result['verified_misses']==report['verified_misses']

census('final-before-verify');verify('pre-maintenance-verify');census('before-maintenance')
batch_size=int(sys.argv[3]); assert 1 <= batch_size <= 65536
gc_only=False
for mode in (('public-leafgen-gc',) if gc_only else ('full',)):
    command=([str(treemap),'leafgen-gc',str(db),'-rw','-json'] if gc_only else
             [str(treemap),'compact',str(db),'-rw','-json','-mode',mode.split('-')[0],'-sync-each-phase','-leaf-pack-max-passes','64','-rewrite-batch-size',str(batch_size)])
    result=run(mode,command,1800)
    census(mode+'-before-oracle')
    verify(mode+'-verify');census(mode+'-after')
    if mode!='full' and result.get('byte_minimized',False):break
(out/'identity.json').write_text(json.dumps(dict(database=str(db),packet=str(packet),source=metadata['source'],
    method_sha256=hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(),
    treemap_sha256=hashlib.sha256(treemap.read_bytes()).hexdigest(),environment=metadata['env']),indent=2)+'\n')
print('PASS_MAINTENANCE_ORACLES',flush=True)
