from pathlib import Path
import subprocess,json,hashlib,shutil
r=Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013/native-4878-traversal');e=r.parent/'evidence/native-primary-finish-20261014';p=r/'artifacts/4878/20261014-primary-finish-accounting';p.mkdir(parents=True,exist_ok=False)
sha=lambda b:hashlib.sha256(b).hexdigest()
base=subprocess.check_output(['git','rev-parse','HEAD'],cwd=r,text=True).strip();assert base=='84246fa509280e24b93a5e2bbfeab90e4088bc8a'
paths=['TreeDB/internal/rootpublication/'+x for x in ['coordinator.go','durable_root_transaction.go','ordinary_finish_v6.go','primary_transaction_construction_v6.go','primary_terminal_completion_v6.go','primary_finish_containing_test.go']]
rows=[]
for path in paths:
 data=(r/path).read_bytes();dst=p/'source'/path;dst.parent.mkdir(parents=True,exist_ok=True);dst.write_bytes(data)
 old=subprocess.run(['git','show',base+':'+path],cwd=r,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL)
 row=dict(path=path,after_sha256=sha(data),after_size=len(data),before_sha256=sha(old.stdout) if old.returncode==0 else None,kind='existing' if old.returncode==0 else 'new')
 if old.returncode==0:
  dst=p/'before'/path;dst.parent.mkdir(parents=True,exist_ok=True);dst.write_bytes(old.stdout)
  patch=subprocess.check_output(['git','diff','--binary',base,'--',path],cwd=r);dst=p/'patches'/(Path(path).name+'.patch');dst.parent.mkdir(exist_ok=True);dst.write_bytes(patch);row['patch_sha256']=sha(patch)
 rows.append(row)
(p/'import-map.json').write_text(json.dumps(dict(base=base,scope='four original files and two new companions; no other import',paths=rows),indent=2)+'\n')
shutil.copytree(e,p/'runs')
producer=(e/'final-producer.py').read_bytes();assert producer==Path('/tmp/native-finish-capture.py').read_bytes()
bindings=[]
for mode in ['red-finish','final-normal','final-race']:
 receipt=json.loads((e/mode/'receipt.json').read_text());expected=1 if mode=='red-finish' else 0
 assert receipt['exit']==expected and receipt['source_unchanged'] and receipt['census_unchanged']
 if mode!='red-finish':
  assert all(sha((r/x['path']).read_bytes())==x['sha256'] for x in receipt['source'])
  assert receipt['argv'].index('-json')<receipt['argv'].index('./TreeDB/internal/rootpublication')
  assert ('-race' in receipt['argv'])==(mode=='final-race')
 bindings.append(dict(run=mode,producer_path='/tmp/native-finish-capture.py',producer_sha256=sha(producer),script_artifact='runs/final-producer.py',binding='same uploaded script bytes, no edits during invocation; post-run readback verified',actual_subprocess_receipt='runs/'+mode+'/receipt.json'))
(p/'producer-bindings.json').write_text(json.dumps(bindings,indent=2)+'\n')
excluded=['TreeDB/caching/native_mvcc_prune_v6.go','TreeDB/native_mvcc_prune.go','TreeDB/internal/valuelog/native_zstd_rank_census_test.go','TreeDB/internal/valuelog/native_zstd_leaf_fixture_export_test.go']
exclude_rows=[]
for path in excluded:
 b=(r/path).read_bytes();dst=p/'excluded-prerequisites'/path;dst.parent.mkdir(parents=True,exist_ok=True);dst.write_bytes(b);exclude_rows.append(dict(path=path,sha256=sha(b),size=len(b),import_allowed=False))
(p/'excluded-prerequisites.json').write_text(json.dumps(exclude_rows,indent=2)+'\n')
fixture='artifacts/4878/20261014-native-public-request/bounded_prune_whole_current_publication_test.go';assert sha((r/fixture).read_bytes())=='77f14c80a007ce9b918e007e5cce24e5d78ffa022c6a92386cfb58bda246d55f'
summary={}
for mode in ['final-normal','final-race','red-finish','red-stop','public-race','public-race-corrected']:
 q=e/mode/'receipt.json';rec=json.loads(q.read_text());events=[]
 for line in (e/mode/'raw.jsonl').read_text().splitlines():
  try:j=json.loads(line)
  except:continue
  if j.get('Action') in ['pass','fail'] and not j.get('Test'):events.append({k:j[k] for k in ['Package','Action','Elapsed'] if k in j})
 summary[mode]=dict(exit=rec['exit'],raw_sha256=rec['raw_sha256'],packages=events)
(p/'results.json').write_text(json.dumps(summary,indent=2)+'\n')
(p/'released-roots.json').write_text(json.dumps(dict(released=[],reason='No retained task test DB roots: t.TempDir cleanup consumed them. No Go producer remains. Source/raw/artifact/temp producer files remain evidence, not cleanup targets.'),indent=2)+'\n')
(p/'README.md').write_text('''Construction checkpoint: shared PRIMARY Finish accounting (UNQUALIFIED #4878)

Physical Finish and recovery now return a constructor-born accounting node after the original transaction lock and callback scope have returned. The original dispatch caller clears activeAttempt, successful pending entries, the previous retry descriptor, and coalesced scratch descriptors before consuming the nodes. Durable/frontier/waiter ACK advances only after that accounting. Startup foreground calls join the SAME Coordinator active-dispatch state; CompleteStartup refuses outstanding work and Stop waits outside its lock. Failed physical outcomes retain the original sticky error and custody; a successful prefix detaches/refunds independently of failed siblings.

The node has no transaction/candidate/root/callback backpointer. Birth reserves three records and twice the actual containing size of DurableRootTransaction, primaryTransactionConstructionV6 and primaryTerminalCompletionV6. Before allocating them, the SAME Arena metadata owner admits AllocationCharge of each actual backing. Promotion metadata still belongs to the original constructor charge. Ordinary drives original physical stages synchronously. The external native RemovalWork binding exists on the same node; full native invocation remains incomplete and receives no bounded Finish credit here. Original unbounded pending-array/scheduler allocations receive no admission credit from this node.

import-map.json is the exact six-member import allowlist, with baseline84246 hashes and four byte patches. All other source paths stay untouched. Reconstruct the native compilation snapshot from reachable84246, the six source copies, and the four explicitly EXCLUDED prerequisite copies. The latter preserve the unfinished frontend and both protected foreign codec diagnostics; they are source-binding evidence only and must not be imported/staged at their live paths. Each executed run records literal subprocess argv/env, source copies, full TreeDB Go census, before/after unchanged receipt and raw output. final-producer.py is the actual uploaded script used for red-finish/final-normal/final-race; producer-bindings.json records its verified bytes. Earlier original red.log was not credited as an exact-source receipt: red-finish reexecutes the maintained84246 baseline plus the narrow control using its original method ABI.

Causal controls: red-finish fails both original Finish/recovery terminal paths with the lock held and owner/phase written after refund. red-stop removes ONLY the new foreground join from the candidate and fails because Stop returns while actual accounting is held. The positive control creates real maintained PRIMARY constructor/DATA certificate/candidate and uses Enqueue/WaitThrough, observing the actual containing lease Resize. It verifies no lock/attempt/pending alias or ACK survives the refund boundary, rejects CompleteStartup during accounting and verifies Stop joins it. It is paired with actual original constructor/sibling-failure controls and all four public crash/replay/interrupted-cut families in final normal/race.

Invalid and superseded runs are retained without race/focused qualification: public-race put a package before -race and additional packages afterward, unintentionally running the broad TreeDB suite. It failed TestProductionAuthorityRepeatedCapturePostShutdownPlateau/meta.index_file with stable resource ownership violation, then panicked dirtyChunkBits.takeFrom (dirty_chunks.go51) through Pager.syncDirtyChunksWithFileWork (pager.go1105), Snapshot.newStablePagerResourceToken (db/stable_resource.go534), StableResourceToken.syncThrough and StableResourceSet.SyncThrough. That source/caller finding is routed to the ordinary lane after import, with no waiver or mutation here. public-race-corrected accidentally used an older locally uploaded script and actually ran only normal coupled controls; its actual argv has no -race and no TreeDB root package, so its name is not evidence. Both actual raw/command/receipt/census files remain under runs. The unintended producer joined before subsequent commands. final-normal/final-race use all flags before every package and their actual argv/source census are validated at packaging. The first preliminary terminal-control session lost its session output and was terminated; it is not credited. No malformed -p2 recipe is credited.

Startup84246 and getter4a remain frozen predecessors. Merged codec pin88a027 is unchanged. Protected fixture77f14 is byte-identical but has NOT run: maintained public bounded PruneCursor/current-directory DATA absence/Prepare/Publish/Report/Finish remain incomplete. All native33/80/Q32/1MiB/4096-firstACK/input32/resource16/file17/two-index/all19/format/fault/shrink40%/8xoutput/cost/platform/M0 gates remain OPEN. No PR/merge/review/maturity claim. Source/raw evidence stays retained. released-roots.json releases no data or evidence root.
''')
(p/'package-recipe.py').write_bytes(Path(__file__).read_bytes())
manifest=[dict(path=str(x.relative_to(p)),sha256=sha(x.read_bytes()),size=x.stat().st_size) for x in sorted(p.rglob('*')) if x.is_file()]
(p/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
print('packet',str(p),'manifest',sha((p/'manifest.json').read_bytes()),'members',len(manifest))
