from pathlib import Path
import json,re
W=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015/native-integrated');A=W.parent/'native-main-integration/native-registry-api-test-migration'
rows=json.loads((A/'call-context-before.json').read_text()); by={}; planned=[];skipped=[]
for r in rows:
 m=re.fullmatch(r'(\s*)(\w+) := (\S+\.Acquire(?:Snapshot|BackendSnapshotFastPath)\(\))',r['text'])
 if not m:skipped.append(r);continue
 sigs=r['scopes']; sig=sigs[-1] if sigs else ''
 if r['path']=='TreeDB/mvcc/versions_test.go' or 'currentDBSystemRootForTest4243' in sig or (r['path'],r['line']) in {('TreeDB/snapshot_fastpath_test.go',150),('TreeDB/caching/cow_cut_test.go',72),('TreeDB/caching/cow_cut_test.go',163),('TreeDB/caching/snapshot_pool_test.go',148)}:
  skipped.append(r);continue
 # Source error callbacks propagate directly, with original return arity.
 if re.search(r'\)\s*(?:\([^)]*\berror\)|error)$',sig):
  if '(inline, pointer uint64, err error)' in sig:handler='return 0, 0, snapshotErr'
  elif '([]byte, bool, error)' in sig:handler='return nil, false, snapshotErr'
  elif '(retainedPayloadPolicyStorage1876, error)' in sig:handler='return storage, snapshotErr'
  elif '(vectorIndexTemplateV1BenchmarkStatus, error)' in sig:handler='return status, snapshotErr'
  elif '(iterator.UnsafeIterator, error)' in sig:handler='return nil, snapshotErr'
  else:handler='return snapshotErr'
 else:
  tests=[]
  for s in sigs:
   tests.extend(re.findall(r'\b(\w+)\s+\*?testing\.(?:TB|T|B)\b',s))
  if not tests:skipped.append(r);continue
  handler=tests[-1]+'.Fatalf("AcquireSnapshot: %v", snapshotErr)'
 indent,var,call=m.groups();new=f'{indent}{var}, snapshotErr := {call}\n{indent}if snapshotErr != nil {{\n{indent}\t{handler}\n{indent}}}'
 by.setdefault(r['path'],[]).append((r['line'],r['text'],new));planned.append(dict(r,handler=handler))
for n,edits in by.items():
 p=W/n;ls=p.read_text().splitlines()
 for line,old,new in sorted(edits,reverse=True):assert ls[line-1]==old;(ls.__setitem__(line-1,new))
 p.write_text('\n'.join(ls)+'\n')
(A/'automatic-migration.json').write_text(json.dumps(planned,indent=2)+'\n');(A/'manual-migration-input.json').write_text(json.dumps(skipped,indent=2)+'\n')
print('migrated',len(planned),'files',len(by),'manual',len(skipped))
for r in skipped:print(r['path']+':'+str(r['line'])+':'+r['text'].strip())
