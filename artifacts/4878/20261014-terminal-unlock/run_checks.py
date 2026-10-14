import os,subprocess,json,hashlib,pathlib
root=pathlib.Path.cwd(); packet=root/'artifacts/4878/20261014-terminal-unlock'
selected=['TreeDB/internal/valuelog/file_producer_work.go','TreeDB/internal/valuelog/file_producer_containing_unlock_test.go','TreeDB/internal/rootpublication/resource_token.go','TreeDB/internal/rootpublication/namespace_proof_close_work.go','TreeDB/internal/rootpublication/namespace_containing_unlock_test.go','TreeDB/internal/rootpublication/durable_root_transaction.go','TreeDB/internal/rootpublication/durable_root_transaction_native_mvcc_v6.go','TreeDB/internal/rootpublication/durable_root_transaction_native_mvcc_v6_test.go','TreeDB/internal/rootpublication/primary_transaction_construction_v6.go']
def row(p):
 b=(root/p).read_bytes();return {'path':p,'sha256':hashlib.sha256(b).hexdigest(),'bytes':len(b)}
paths=sorted(str(p.relative_to(root)) for p in root.glob('TreeDB/**/*.go'))+['go.mod','go.sum']
source={'head':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),'rows':[row(p) for p in paths]}
(packet/'source.json').write_text(json.dumps(source,indent=2)+'\n')
for p in selected:
 dst=packet/'source'/p;dst.parent.mkdir(parents=True,exist_ok=True);dst.write_bytes((root/p).read_bytes())
pattern='Test(FileProducer|Namespace|PrimaryConstruction)ContainingRefundAfterOriginalCallUnlock|TestNativeMVCCPrivateConstructorPhysicalAbortV6|TestPrimaryOwner|TestPrimarySnapshot|TestPrimaryOneShot|TestNativeMVCCSnapshotActual|TestLeafPinWork|TestOriginalDATAOnlySnapshot|TestOriginalManager|TestNativeMVCCSet|TestNativeMVCCOriginalSet|TestManagerWrapper|Test.*Namespace|Test.*CreationProof|TestEnrollment|TestOrdinaryMetadata'
envvars={'GOWORK':'off','GOMAXPROCS':'2','CGO_ENABLED':'1','GOPROXY':'off','GOSUMDB':'off','GOTOOLCHAIN':'local'}
env=os.environ.copy();env.update(envvars)
base=['/home/mikers/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go','test','-mod=readonly','-p','2']
checks={}
for mode in ['normal','race']:
 argv=base+(['-race'] if mode=='race' else [])+['./TreeDB/internal/valuelog','./TreeDB/internal/rootpublication','./TreeDB/db','./TreeDB/internal/retainedalloc','-run',pattern,'-count=1','-timeout=60s']
 raw=packet/('frozen-'+mode+'.log')
 with raw.open('wb') as f:result=subprocess.run(argv,env=env,stdout=f,stderr=subprocess.STDOUT)
 checks[mode]={'argv':argv,'exit':result.returncode,'raw':str(raw.relative_to(root))}
 print(mode,result.returncode,raw.read_text(),flush=True)
 if result.returncode:break
changed=[r['path'] for r in source['rows'] if row(r['path'])!=r]
checks['source_readback_changed']=changed
(packet/'checks.json').write_text(json.dumps({'env':envvars,'checks':checks},indent=2)+'\n')
if changed or any(c['exit']!=0 for c in checks.values() if isinstance(c,dict)):raise SystemExit(1)
manifest={'status':'UNQUALIFIED CONSTRUCTION; public ABI and all original rank gates remain open','owned_paths':[dict(row(p),before_blob=subprocess.check_output(['git','rev-parse','HEAD:'+p],text=True,stderr=subprocess.DEVNULL).strip() if subprocess.run(['git','cat-file','-e','HEAD:'+p],stderr=subprocess.DEVNULL).returncode==0 else None) for p in selected],'module':'v1.18.2-snissn.0.0.20261014112447-88a027b09341','recipe_note':'Old shared-custody checks.json reconstructed -p2 is invalid. This packet captures exact subprocess argv with -p 2; supersedes that recipe. red.log is setup failure, red-control.log is genuine three original lock controls RED. Earlier composed logs are supplementary; frozen-normal/race bind complete final source census.'}
(packet/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
