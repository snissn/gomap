import hashlib,json,os,subprocess,time
from pathlib import Path
root=Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013/native-4878-traversal')
out=Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013/evidence/native-4878-shared-lifecycle-freeze-20261014');out.mkdir(exist_ok=True)
go='/home/mikers/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go'
mode='data-terminal-normal'
argv=[go,'test','-p','2','-mod=readonly','-count=1','-timeout=180s','-json']
if mode=='race':argv+=['-race']
argv+=['-run','^Test(COWBasis|NativeMVCCCOWBasisDataCompletionCausalState|DurableRootTransaction|Coordinator.*(Consume|Recovery|Abort|Stop|Finish))','./TreeDB/internal/rootpublication','./TreeDB/caching']
env={**os.environ,'GOWORK':'off','GOMAXPROCS':'2','CGO_ENABLED':'1','GOPROXY':'off','GOSUMDB':'off','GOTOOLCHAIN':'local'}
def census():
 return {str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted((root/'TreeDB').rglob('*.go'))}
before=census();script=Path(__file__).read_bytes();start=time.time()
command={'argv':argv,'env':{k:env[k] for k in ['GOWORK','GOMAXPROCS','CGO_ENABLED','GOPROXY','GOSUMDB','GOTOOLCHAIN']},'cwd':str(root),'script':str(Path(__file__).resolve()),'script_sha256':hashlib.sha256(script).hexdigest()}
(out/(mode+'.command.json')).write_text(json.dumps(command,indent=2)+'\n')
with (out/(mode+'.jsonl')).open('wb') as f:result=subprocess.run(argv,cwd=root,env=env,stdout=f,stderr=subprocess.STDOUT)
after=census();receipt={**command,'exit':result.returncode,'duration_seconds':time.time()-start,'before':before,'after':after,'source_unchanged':before==after,'raw_sha256':hashlib.sha256((out/(mode+'.jsonl')).read_bytes()).hexdigest()}
(out/(mode+'.receipt.json')).write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps({'exit':result.returncode,'source_unchanged':before==after,'raw_sha256':receipt['raw_sha256'],'receipt':str(out/(mode+'.receipt.json'))}))
if result.returncode:print((out/(mode+'.jsonl')).read_text()[-10000:])
