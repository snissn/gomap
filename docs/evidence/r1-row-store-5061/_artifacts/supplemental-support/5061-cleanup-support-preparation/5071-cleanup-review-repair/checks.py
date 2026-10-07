import subprocess,os,json,time,hashlib,pathlib,platform
root=pathlib.Path('/home/mikers/gomap-r1-5071-fd')
out=pathlib.Path('/home/mikers/gomap-r1-evidence-20261005/5071-cleanup-review-repair')
go='/home/mikers/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go'
def git(*args): return subprocess.check_output(['git',*args],cwd=root,text=True).strip()
expected='b4e02d903f79f60d791aec4bf205ca80576b4cd2'
assert git('rev-parse','HEAD')==expected and not git('status','--porcelain')
record={'source_commit':expected,'source_clean_before':True,'hostname':platform.node(),'toolchain_sha256':hashlib.sha256(pathlib.Path(go).read_bytes()).hexdigest(),'toolchain_version':subprocess.check_output([go,'version'],text=True).strip(),'environment':{k:os.environ[k] for k in ['GOROOT','GOCACHE','GOMODCACHE','GOWORK','GOTOOLCHAIN']},'steps':[]}
steps=[('normal',[go,'test','./TreeDB/db','-run','TestLeafManifestRevisionGC|TestLeafGenerationGCReclaimsReleasedManifest','-count=1','-timeout=3m','-v']),('race',[go,'test','-race','./TreeDB/db','-run','TestLeafManifestRevisionGC|TestLeafGenerationGCReclaimsReleasedManifest','-count=1','-timeout=3m','-v']),('vet',[go,'vet','./TreeDB/db'])]
for name,argv in steps:
 start=time.time(); log=out/(name+'.log')
 with log.open('wb') as stream: result=subprocess.run(argv,cwd=root,stdout=stream,stderr=subprocess.STDOUT)
 record['steps'].append({'name':name,'argv':argv,'exit_code':result.returncode,'started_unix':start,'finished_unix':time.time(),'log':str(log),'log_sha256':hashlib.sha256(log.read_bytes()).hexdigest()})
 print(log.read_text(),flush=True)
 if result.returncode: break
record['source_commit_after']=git('rev-parse','HEAD'); record['source_clean_after']=not git('status','--porcelain')
(out/'final-checks.json').write_text(json.dumps(record,indent=2)+'\n')
assert record['source_commit_after']==expected and record['source_clean_after']
raise SystemExit(max(step['exit_code'] for step in record['steps']))
