from pathlib import Path
import os,json,hashlib,tarfile,difflib
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');W=D/'native-integrated';B=D/'native-main-integration/index-pin-identity-repair'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def dump(p,x):p.write_text(json.dumps(x,indent=2,sort_keys=True)+'\n')
selection=D/'native-decision/root-index-pin-identity-repair-selection.md';assert sha(selection)=='3bedfce103563a51ce20f1b16d537b35d4d6796d6537a941a06a7c68111ee1ab'
base=D/'native-main-integration/index-observer-inventory-final';mp=base/'source-final.json';ap=base/'source-final.tar.gz';assert sha(mp)=='21bc95e755d65748083193171e2ba2ed4f0d72454189ac2e61b087e998d5acd3';assert sha(ap)=='00e91ecda5dfea9925f4386084bf33168b67b0d28441c31b420b8563cff832c7';old=json.loads(mp.read_text())
source={}
for basepath,ds,fs in os.walk(W):
 ds[:]=[d for d in ds if d!='.git']
 for f in fs:
  p=Path(basepath,f);q=str(p.relative_to(W))
  if q!='.git' and p.is_file():source[q]=sha(p)
source=dict(sorted(source.items()));changed=[q for q in source if old.get(q)!=source[q]];assert set(old)==set(source) and len(source)==10377
allowed={'TreeDB/internal/rootpublication/primary_empty_candidate_work_v6.go','TreeDB/internal/rootpublication/native_index_observer_v6_test.go'};assert set(changed)==allowed
with tarfile.open(ap) as t:prior={m.name:t.extractfile(m).read() for m in t if m.isfile() and m.name in changed}
assert all(hashlib.sha256(prior[q]).hexdigest()==old[q] for q in changed)
q='TreeDB/internal/rootpublication/primary_empty_candidate_work_v6.go';a=prior[q].decode();b=(W/q).read_text();start='func (token *StableResourceToken) MaintenanceNativeCaptureBindingForTestV6';end='\nfunc (custody *OriginalCleanupCustody)'
def remove(s):i=s.index(start);j=s.index(end,i);return s[:i]+s[j:]
assert remove(a)==remove(b)
dump(B/'invariance.json',{'all_other_10375_files_byte_exact':True,'same_file_except_one_observer_helper_byte_exact':True,'same_file_nonhelper_sha256':hashlib.sha256(remove(b).encode()).hexdigest(),'unchanged_DB_DATA_PRIMARY_tests_sha256':source['TreeDB/db/native_index_observer_v6_test.go'],'production_layouts_lifetime_F293_R145_caps_authority_unchanged':True,'deleted':0,'added':0})
dump(B/'source-frozen.json',source)
with tarfile.open(B/'source-frozen.tar.gz','w:gz') as t:
 for q in source:t.add(W/q,arcname=q,recursive=False)
with tarfile.open(B/'source-frozen.tar.gz') as t:assert {m.name:hashlib.sha256(t.extractfile(m).read()).hexdigest() for m in t if m.isfile()}==source
rows=[]
with (B/'index-pin-identity-repair.delta.patch').open('w') as f:
 for q in changed:
  a=prior[q];b=(W/q).read_bytes();f.writelines(difflib.unified_diff(a.decode().splitlines(True),b.decode().splitlines(True),fromfile='a/'+q,tofile='b/'+q,n=4));p=B/'delta-files'/q;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(b);rows.append({'path':q,'before_sha256':old[q],'after_sha256':source[q],'bytes':len(b)})
dump(B/'changed-files.json',rows)
r={'selection_sha256':sha(selection),'source_manifest_sha256':sha(B/'source-frozen.json'),'source_archive_sha256':sha(B/'source-frozen.tar.gz'),'source_files':10377,'changed_files':2,'added':0,'deleted':0,'base_manifest_sha256':sha(mp),'delta_sha256':sha(B/'index-pin-identity-repair.delta.patch'),'changed_inventory_sha256':sha(B/'changed-files.json'),'invariance_sha256':sha(B/'invariance.json'),'Go_executed':False,'compile_executed':False,'test_bodies_executed':False,'source_lease':'RELEASED','status':'Q32 RED/ACK0/noAllowanceChosen'};dump(B/'freeze-receipt.json',r);print(json.dumps(r,indent=2))
