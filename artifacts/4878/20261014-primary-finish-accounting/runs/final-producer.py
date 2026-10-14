import os, pathlib, subprocess, hashlib, json, time, sys
root=pathlib.Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013/native-4878-traversal')
out=pathlib.Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013/evidence/native-primary-finish-20261014');out.mkdir(exist_ok=True)
paths=['TreeDB/internal/rootpublication/'+x for x in ['coordinator.go','durable_root_transaction.go','ordinary_finish_v6.go','primary_transaction_construction_v6.go','primary_terminal_completion_v6.go','primary_finish_containing_test.go']]
go='/home/mikers/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go'
env=os.environ.copy();env.update(GOWORK='off',GOMAXPROCS='2',CGO_ENABLED='1',GOPROXY='off',GOSUMDB='off',GOTOOLCHAIN='local')
def capture(name,args):
 d=out/name;d.mkdir(exist_ok=True)
 rows=[]
 for p in paths+['go.mod','go.sum']:
  if not (root/p).exists():continue
  b=(root/p).read_bytes();target=d/'source'/p;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(b);rows.append(dict(path=p,sha256=hashlib.sha256(b).hexdigest(),size=len(b)))
 census=[dict(path=str(p.relative_to(root)),sha256=hashlib.sha256(p.read_bytes()).hexdigest(),size=p.stat().st_size) for p in sorted((root/'TreeDB').rglob('*.go'))]
 (d/'source-census.json').write_text(json.dumps(census,indent=2)+'\n')
 command=dict(argv=[go,'test','-p','2','-mod=readonly']+args,cwd=str(root),environment={k:env[k] for k in ['GOWORK','GOMAXPROCS','CGO_ENABLED','GOPROXY','GOSUMDB','GOTOOLCHAIN']},source=rows,start=time.time())
 (d/'command.json').write_text(json.dumps(command,indent=2)+'\n')
 with (d/'raw.jsonl').open('wb') as f:r=subprocess.run(command['argv'],cwd=root,env=env,stdout=f,stderr=subprocess.STDOUT)
 command.update(exit=r.returncode,end=time.time(),source_unchanged=all(hashlib.sha256((root/x['path']).read_bytes()).hexdigest()==x['sha256'] for x in rows),census_unchanged=all(hashlib.sha256((root/x['path']).read_bytes()).hexdigest()==x['sha256'] for x in census),raw_sha256=hashlib.sha256((d/'raw.jsonl').read_bytes()).hexdigest())
 (d/'receipt.json').write_text(json.dumps(command,indent=2)+'\n');print(name,r.returncode,command['source_unchanged'],command['census_unchanged'],flush=True)
 return r.returncode
mode=sys.argv[1]
if mode=='red-stop':
 p=root/paths[0];original=p.read_bytes();s=original.decode();start=s.index('\t\tfor c.activeDispatch != 0 {');end=s.index('\n\t\tif !c.stopped {',start)
 p.write_text(s[:start]+s[end:])
 try: status=capture(mode,['./TreeDB/internal/rootpublication','-run','^TestPrimaryFinishAccountingBeforeAckAndStop/stop$','-count=1','-timeout=20s','-json'])
 finally:p.write_bytes(original)
 sys.exit(0 if status==1 else 2)
if mode=='red-finish':
 backup={p:(root/p).read_bytes() for p in paths}
 test=backup[paths[-1]].decode();test=test[:test.index('// This uses the maintained constructor')]
 start=test.index('// This isolates')
 test='package rootpublication\nimport ("path/filepath";"testing";"github.com/snissn/gomap/TreeDB/internal/iterator";"github.com/snissn/gomap/TreeDB/internal/memtable";"github.com/snissn/gomap/TreeDB/internal/primaryarena")\n'+test[start:]
 test=test.replace('node, err := txn.releaseFromRecovery()\n\t\t\t\te = err\n\t\t\t\tif e == nil {\n\t\t\t\t\tcompletePrimaryTerminalsV6(&node)\n\t\t\t\t}', 'e = txn.releaseFromRecovery()')
 test=test.replace('node, err := txn.finishOrdinaryPrimaryV6(&iterator.OrdinalScanWork{RecordLimit: ^uint64(0), ByteLimit: ^uint64(0)}, nil)\n\t\t\t\te = err\n\t\t\t\tif e == nil {\n\t\t\t\t\tcompletePrimaryTerminalsV6(&node)\n\t\t\t\t}', 'e = txn.finishOrdinaryPrimaryV6(&iterator.OrdinalScanWork{RecordLimit: ^uint64(0), ByteLimit: ^uint64(0)}, nil)')
 try:
  for p in paths[:4]:(root/p).write_bytes(subprocess.check_output(['git','show','84246fa509280e24b93a5e2bbfeab90e4088bc8a:'+p],cwd=root))
  (root/paths[4]).unlink();(root/paths[5]).write_text(test)
  status=capture(mode,['./TreeDB/internal/rootpublication','-run','^TestPrimaryFinishContainingRefundJoinsOriginalCall$','-count=1','-timeout=20s','-json'])
 finally:
  for p,b in backup.items():(root/p).write_bytes(b)
 sys.exit(0 if status==1 else 2)
pattern='Test(PrimaryFinish|StartupReplay|ReportPublishResult|RootPublicationBuildGroupPrimaryV6|PrimaryConstructionV6|RootPublicationOriginalSiblingFailureCustody|COWPublic(CrashAcknowledgedSyncReopen|CrashRelaxedOrdinaryReopen|EncodedMVCCPointsGroupsPhysicalDeleteReplay|ModeledInterruptedDependencyAndRootPublicationCuts))'
args=(['-race'] if 'race' in mode else [])+['-run',pattern,'-count=1','-timeout=180s','-json','./TreeDB/internal/rootpublication','./TreeDB/db','./TreeDB']
print('argv', [go,'test','-p','2','-mod=readonly']+args,flush=True)
sys.exit(capture(mode,args))
