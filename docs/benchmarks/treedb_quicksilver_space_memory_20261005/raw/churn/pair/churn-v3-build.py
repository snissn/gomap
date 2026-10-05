import json,hashlib,os,pathlib,subprocess,sys,time
root=pathlib.Path('/mnt/fast4tb/quicksilver-space-memory-20261005');source=root/'source-churn-v3';out=root/'churn-v3-preflight'
identity=json.load(open(root/'churn-v3-source-inputs.json'));assert source.exists();out.mkdir(exist_ok=False)
def sha(p):return hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest()
assert not [f for f,h in identity['files'].items() if sha(source/f)!=h]
env={k:v for k,v in os.environ.items() if not k.startswith('TREEDB_')};env.update(GOROOT='/home/mikers/.gvm/gos/go1.26.3',PATH='/home/mikers/.gvm/gos/go1.26.3/bin:/usr/local/bin:/usr/bin:/bin',GOWORK='off',GOFLAGS='',GOENV='off',GOTOOLCHAIN='local',GOCACHE='/mnt/fast4tb/go-build-cache',GOMODCACHE='/mnt/fast4tb/go-mod-cache',GOMAXPROCS='12',TMPDIR=str(root/'tmp'),CGO_ENABLED='1',CGO_CFLAGS='-I'+str(pathlib.Path('/mnt/fast4tb/quicksilver-authoritative-20261003/native/root/usr/include')),CGO_LDFLAGS='-L'+str(pathlib.Path('/mnt/fast4tb/quicksilver-authoritative-20261003/native/root/usr/lib')),LD_LIBRARY_PATH=str(pathlib.Path('/mnt/fast4tb/quicksilver-authoritative-20261003/native/root/usr/lib'))+':'+str(pathlib.Path('/mnt/fast4tb/quicksilver-authoritative-20261003/native/root/usr/lib/x86_64-linux-gnu')))
def run(name,argv):
 start=time.time();log=out/(name+'.txt')
 with log.open('w') as f: p=subprocess.run(argv,cwd=source,env=env,stdout=f,stderr=subprocess.STDOUT)
 receipt=dict(head=identity['head'],provisional=True,command=argv,started=start,finished=time.time(),rc=p.returncode,stdout_sha256=sha(log));(out/(name+'-run.json')).write_text(json.dumps(receipt,indent=2)+'\n');print(name,receipt,flush=True)
 assert p.returncode==0,log.read_text()[-10000:]
run('native-build',['go','build','-p','1','-buildvcs=false','-tags','lmdb rocksdb','-o',str(root/'bin/unified-bench-churn-v3'),'./cmd/unified_bench'])
run('analyzer-build',['go','build','-p','1','-buildvcs=false','-o',str(root/'bin/benchprof-churn-v3'),'./cmd/benchprof'])
run('maintenance-build',['go','build','-p','1','-buildvcs=false','-o',str(root/'bin/treemap-churn-v3'),'./TreeDB/cmd/treemap'])
print('PASS_INTEGRATION_PREFLIGHT',flush=True)
