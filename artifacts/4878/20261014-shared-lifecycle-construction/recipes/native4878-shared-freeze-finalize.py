from pathlib import Path
import subprocess,json,hashlib,difflib
r=Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013');w=r/'native-4878-traversal';o=r/'ordinary-5111';ev=r/'evidence/native-4878-shared-lifecycle-freeze-20261014';p=w/'artifacts/4878/20261014-shared-lifecycle-construction';p.mkdir(exist_ok=True)
def sha(b):return hashlib.sha256(b).hexdigest()
def save(n,b):
 q=p/n;q.parent.mkdir(parents=True,exist_ok=True);q.write_bytes(b)
def js(n,v):save(n,(json.dumps(v,indent=2)+'\n').encode())
owned=[]
for directory,names in {
'caching':'db.go',
'internal/rootpublication':'primary_transaction_construction_v6.go durable_root_transaction.go coordinator.go resource_token.go resource_token_allocation.go ordinary_finish_v6.go resource_set.go resource_set_allocation.go resource_allocation.go resource_kind_allocation.go resource_entry_view.go resource_rope_allocation.go resource_logical_allocation.go resource_physical_allocation.go resource_obligation_allocation.go resource_field_allocation.go resource_entry_allocation.go resource_entry_outgoing_allocation.go resource_manifest_allocation.go dependency_directory_reader_v2.go',
'db':'db.go durable_root_runtime.go root_publication_activation.go root_publication_build_group.go primary_arena_owner_v5.go index_gen.go index_gen_db.go leaf_generation_runtime.go snapshot_read_lifetime.go snapshot_primary_retention.go stable_pager_owned_operations.go durable_primary_runtime_v5.go primary_capsule_runtime_v6.go primary_dependency_v5.go dependency_directory_v2.go pools.go original_startup_generation.go open_readonly.go',
'internal/valuelog':'manager.go original_cleanup.go stable_resource.go',
'internal/primaryarena':'arena.go',
'pager':'pager.go grow_work.go mmap_unix.go primary_read_root_v6.go',
'internal/retainedalloc':'owner.go',
'node':'primary_directory_v1.go',
'internal/memtable':'cow_tree.go cow_view.go cow_lookup_safe.go cow_lookup_unsafe.go cow_test.go cow_budget.go',
}.items():owned.extend('TreeDB/'+directory+'/'+n for n in names.split())
assert len(owned)==55,len(owned)
tests=['TreeDB/internal/retainedalloc/owner_test.go','TreeDB/db/primary_dependency_cleanup_test.go']
new=[]
for directory,names in {
'caching':'native_mvcc_prune_v6.go',
'db':'native_mvcc_snapshot_v6.go native_mvcc_snapshot_close_v6.go native_mvcc_current_delete_v6.go native_mvcc_dependency_release_v6.go native_mvcc_data_dependency_release_v6.go native_mvcc_generation_completion_v6.go primary_arena_release_work.go native_generation_containing_completion_test.go native_mvcc_snapshot_capture_test.go primary_arena_release_work_test.go',
'internal/memtable':'cow_traversal.go native_retention_work.go cow_traversal_test.go',
'internal/retainedalloc':'owner_work.go owner_enroll_work.go owner_enroll_work_test.go',
'internal/rootpublication':'durable_root_transaction_native_mvcc_v6.go primary_terminal_completion_v6.go dependency_directory_release_work.go durable_root_transaction_native_mvcc_v6_test.go data_terminal_completion_v6_test.go',
'internal/primaryarena':'native_mvcc_group_v6.go',
'lifecycle':'native_mvcc_registry.go native_mvcc_registry_test.go',
'node':'primary_directory_native_mvcc.go primary_directory_native_mvcc_test.go',
}.items():new.extend('TreeDB/'+directory+'/'+n for n in names.split())
new.extend(['TreeDB/native_mvcc_prune.go','TreeDB/tree/native_mvcc_selection_v6.go'])
# Exact maintained lower-source prerequisites absent from the ordinary lane.
for directory,names in {
'internal/cowbtree':'map.go string_cursor.go',
'internal/mvccadmission':'request.go',
'internal/m7routetrace':'common.go disabled.go detail_enabled.go admission_enabled.go enabled.go',
'internal/valuelog':'cow_leaf_continuation.go cow_zstd_continuation.go cow_block_continuation.go cow_leaf_projection.go cow_template_continuation.go',
'node':'projection_work.go projection_decode.go',
 'tree':'charged_projection.go',
}.items():new.extend('TreeDB/'+directory+'/'+n for n in names.split())
rows=[]
for n in sorted(set(owned+tests+new)):
 b=(w/n).read_bytes();old=(o/n).read_bytes() if (o/n).exists() else None
 save('source/'+n,b)
 if old is not None:save('baseline/'+n,old)
 diff=''.join(difflib.unified_diff(old.decode().splitlines(True) if old is not None else [],b.decode().splitlines(True),fromfile='a/'+n if old is not None else '/dev/null',tofile='b/'+n))
 save('patches/'+n+'.patch',diff.encode())
 rows.append({'path':n,'category':'released-original' if n in owned else 'released-test' if n in tests else 'new-declaring-or-control','bytes':len(b),'sha256':sha(b),'baseline_bytes':len(old) if old is not None else None,'baseline_sha256':sha(old) if old is not None else None,'changed':old!=b,'public_status':'UNQUALIFIED; no bounded public acceptance'})
js('import-map.json',{'ordinary_head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=o,text=True).strip(),'native_head_before':subprocess.check_output(['git','rev-parse','HEAD'],cwd=w,text=True).strip(),'rows':rows})
for q in ev.rglob('*'):
 if q.is_file():save('evidence/'+str(q.relative_to(ev)),q.read_bytes())
for q in (r/'evidence').glob('native4878-*.py'):
 if any(s in q.name for s in ['shared-corrected','shared-freeze-finalize','data-terminal','data-pending-attribution','generation']):save('recipes/'+q.name,q.read_bytes())
checks=[]
for mode in ['shared-corrected-normal','shared-corrected-race']:
 q=json.loads((ev/(mode+'.receipt.json')).read_text());assert q['exit']==0 and q['source_unchanged'];counts={};passed=[]
 for line in (ev/(mode+'.jsonl')).read_text().splitlines():
  try:e=json.loads(line)
  except ValueError:continue
  if e.get('Action')=='pass' and e.get('Test'):
   counts[e['Package']]=counts.get(e['Package'],0)+1;passed.append(e['Test'])
 checks.append({'mode':mode,'exit':q['exit'],'source_unchanged':q['source_unchanged'],'argv':q['argv'],'env':q['env'],'script_sha256':q['script_sha256'],'raw_sha256':q['raw_sha256'],'passed_test_events':len(passed),'package_counts':counts,'actual_tests':passed})
js('checks.json',checks)
normal=json.loads((ev/'shared-corrected-normal.receipt.json').read_text());race=json.loads((ev/'shared-corrected-race.receipt.json').read_text());assert normal['after']==race['before']==race['after']
js('producer-census.json',normal['after'])
status=subprocess.check_output(['git','status','--porcelain','-z'],cwd=w).decode().split('\0');checkpoint=[]
protected=['TreeDB/internal/valuelog/native_zstd_rank_census_test.go','TreeDB/internal/valuelog/native_zstd_leaf_fixture_export_test.go']
for item in status:
 if not item:continue
 n=item[3:]
 if n in protected or n.startswith('artifacts/'):continue
 if n.startswith('TreeDB/') and (w/n).is_file():
  b=(w/n).read_bytes();save('checkpoint-source/'+n,b);checkpoint.append({'path':n,'sha256':sha(b),'bytes':len(b),'ownership':'owned import member' if n in owned+tests+new else 'readonly frozen predecessor or assigned public construction; ownership unchanged','stage':True})
js('checkpoint-paths.json',checkpoint)
js('protected-inputs.json',[{'path':n,'sha256':sha((w/n).read_bytes()),'bytes':len((w/n).read_bytes()),'stage':False} for n in protected])
(p/'README.md').write_text('''# Shared lifecycle construction checkpoint — UNQUALIFIED

The finite shared packet preserves original metadata, physical owners and caller custody. It fixes selected DATA transaction/install terminal accounting, DATA/PRIMARY typed directory completion, COW/Owner retained capture, and generation/startup/ghost alias and lock barriers. Public bounded PruneVersions and protected 33/80/4096 fixture have not run and remain OPEN.

import-map.json is the exact root backintegration allowlist, with current ordinary recipient baseline bytes, frozen source bytes and byte patches. Fifty-five released originals and two released tests retain native writer ownership. NEW declarations required for compilation may include inactive public/native construction; this supplies no public qualification. Preserve unrelated ordinary vacuum changes. Ordinary migrates its own two finishOfflineReplacementV1 calls to (Owner, charge, error), consuming nonzero completion after caller alias detachment even when the operation error remains. Its separate nil-metadata offline scope stays ordinary-owned.

checks.json and actual retained receipts bind subprocess argv/environment/script SHA and all3184 TreeDB Go source rows before/after final normal/race. Original COW DATA basis and DB close-all REDs, diagnostic attribution and restored Owner source are retained. Diagnostic tracing is not acceptance. All19/cost/platform/public ACK gates remain OPEN.

Reconstruct by checking out the published construction commit. checkpoint-paths.json and checkpoint-source retain exact dirty prerequisites, including readonly predecessor imports without ownership change. Protected foreign zstd diagnostics remain excluded from staging and are hashed in protected-inputs.json; controls include their exact input bytes. Do not overwrite or claim authorship. No generated database/temp roots are released. Preserve evidence, sources and original fixtures.
''')
# Retain producer handoff receipts, without changing source ownership.
for d in (r/'evidence').iterdir():
 if d.is_dir() and any(v in d.name for v in ['handoff','declaring-companions','committed-predecessor-family']):
  for q in d.rglob('*.json'):
   save('handoff-receipts/'+str(q.relative_to(r/'evidence')),q.read_bytes())
for n in protected:save('protected-source/'+n,(w/n).read_bytes())
js('preserved-ordinary-unique-paths.json',[n for n in ['TreeDB/db/durable_primary_rebuild_v5.go','TreeDB/db/vacuum_online.go','TreeDB/db/vacuum_offline.go','TreeDB/db/vacuum_collection_roots.go','TreeDB/db/primary_joint_swap_v6.go','TreeDB/db/durable_root_owner_continuity_test.go','TreeDB/db/primary_offline_pair_v6.go','TreeDB/db/stable_resource.go','TreeDB/internal/valuelog/mmap_admission_test.go']])
js('module-binding.json',{'go_mod_sha256':sha((w/'go.mod').read_bytes()),'go_sum_sha256':sha((w/'go.sum').read_bytes()),'compress':'v1.18.2-snissn.0.0.20261014112447-88a027b09341','replace':False})
manifest=[]
for q in sorted(p.rglob('*')):
 if q.is_file() and q.name!='manifest.json':b=q.read_bytes();manifest.append({'path':str(q.relative_to(p)),'bytes':len(b),'sha256':sha(b)})
js('manifest.json',{'status':'UNQUALIFIED shared construction; public/all19/cost/platform gates OPEN','members':manifest})
print(json.dumps({'packet':str(p),'rows':len(rows),'changed_rows':sum(x['changed'] for x in rows),'checkpoint_paths':len(checkpoint),'members':len(manifest),'manifest_sha256':sha((p/'manifest.json').read_bytes())}))
