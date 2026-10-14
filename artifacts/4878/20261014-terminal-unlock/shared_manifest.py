from pathlib import Path
import json,subprocess,hashlib,difflib,collections
r=Path.cwd();out=r/'artifacts/4878/20261014-terminal-unlock';before='7a0ef65bad20694e86edd5869dcd115f0af1643c'
mappath=Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013/evidence/ordinary-5111/native-shared-import-plan-20261014/path-map.json');original=json.loads(mappath.read_text());(out/'accepted-path-map.json').write_bytes(mappath.read_bytes())
rows=original['rows']+[{'group':'A_generic','path':'TreeDB/internal/valuelog/file_producer_containing_unlock_test.go','current_sha256':None,'action':'import_exact_NEW_source'},{'group':'B_same_constructor_inactive_native_methods','path':'TreeDB/internal/rootpublication/namespace_containing_unlock_test.go','current_sha256':None,'action':'import_exact_NEW_source'},{'group':'B_same_constructor_inactive_native_methods','path':'TreeDB/internal/rootpublication/durable_root_transaction.go','current_sha256':None,'action':'apply_released_original_post_unlock_terminal_delta'}]
output=[]
for item in rows:
 p=item['path'];data=(r/p).read_bytes();row=dict(item);row['corrected_sha256']=hashlib.sha256(data).hexdigest();row['corrected_bytes']=len(data)
 if item['group'].startswith(('A_','B_','C_')):
  dst=out/'shared-source'/p;dst.parent.mkdir(parents=True,exist_ok=True);dst.write_bytes(data)
  result=subprocess.run(['git','show',before+':'+p],stdout=subprocess.PIPE,stderr=subprocess.DEVNULL)
  if result.returncode==0:
   baseline=result.stdout
   h=hashlib.sha256(baseline).hexdigest()
   if item['current_sha256'] is not None and h!=item['current_sha256']:raise RuntimeError(('ordinary baseline changed',p,h,item['current_sha256']))
   dst=out/'ordinary-baseline'/p;dst.parent.mkdir(parents=True,exist_ok=True);dst.write_bytes(baseline)
   patch=''.join(difflib.unified_diff(baseline.decode().splitlines(keepends=True),data.decode().splitlines(keepends=True),fromfile='a/'+p,tofile='b/'+p))
   dst=out/'ordinary-patches'/(p+'.patch');dst.parent.mkdir(parents=True,exist_ok=True);dst.write_text(patch)
   row['exact_ordinary_baseline_sha256']=h;row['patch']=str(dst.relative_to(r));row['patch_sha256']=hashlib.sha256(dst.read_bytes()).hexdigest()
   if p=='TreeDB/db/db.go':row['whole_file_import_authorized']=False;row['scope']='exact remaining Snapshot state/gates only; typed hook already identical; apply patch preserving ordinary unique source'
 output.append(row)
manifest={'status':'UNQUALIFIED CONSTRUCTION; root-only backintegration; no public readiness credit','ordinary_source':before,'native_before':'0865a8b40c5070a0077ef97cebc5945034821507','accepted_path_map_sha256':hashlib.sha256(mappath.read_bytes()).hexdigest(),'counts':dict(collections.Counter(x['group'] for x in output)),'rows':output,'terminal':{'file':'phase1 yield returns through File.closeMu/deleteMu unlock before external RemovePending','proof':'ordinary scalar SAME physical engine; closeMu joins before original Owner.RemovePending; native external engine unchanged','transaction':'SAME physical engine detaches/scrubs under original lock, scalar ordinary terminal charge consumed after nested locked call returns; native preborn singly claimed external PrimaryConstructionReleaseWork yields before RemovalWork','native_transaction_receipt':'parent admission includes actual sizeof(PrimaryConstructionReleaseWork); Bind before clearing caller transaction alias, ContinueWork through physical and metadata cleanup; receiver-only path fail-closed'},'validation':'checks.json exact subprocess argv/frozen normal and race; source.json unchanged full TreeDB Go/module readback; original triple RED red-control.log','excluded':['all E native-only frontend/codec/current-selector paths','runtime and activation exact7a prerequisites are replay source only and not generic import deltas','protected foreign zstd diagnostics'],'recipe_correction':'old -p2 spelling is invalid; exact run_checks.py argv -p 2 supersedes prior reconstructed checks.json'}
(out/'shared-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
print(hashlib.sha256((out/'shared-manifest.json').read_bytes()).hexdigest(),manifest['counts'])
