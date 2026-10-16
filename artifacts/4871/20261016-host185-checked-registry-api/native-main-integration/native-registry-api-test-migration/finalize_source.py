from pathlib import Path
import json,hashlib,subprocess,difflib,re,tarfile,io
W=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015/native-integrated');A=W.parent/'native-main-integration/native-registry-api-test-migration'
before=json.loads((A/'owned-before.json').read_text());after={n:hashlib.sha256((W/n).read_bytes()).hexdigest() for n in before};changed=[n for n,h in before.items() if after[n]!=h]
plans=json.loads((A/'automatic-migration.json').read_text()); patches=[];preimages={};postimages={}
for n in changed:
 post=(W/n).read_bytes();r=subprocess.run(['git','show','HEAD:'+n],cwd=W,capture_output=True);pre=r.stdout
 if r.returncode or hashlib.sha256(pre).hexdigest()!=before[n]:
  s=post.decode()
  for row in reversed([r for r in plans if r['path']==n]):
   m=re.fullmatch(r'(\s*)(\w+) := (\S+\.Acquire(?:Snapshot|BackendSnapshotFastPath)\(\))',row['text']);ind,var,call=m.groups()
   new=f'{ind}{var}, snapshotErr := {call}\n{ind}if snapshotErr != nil {{\n{ind}\t{row["handler"]}\n{ind}}}'
   assert s.count(new)==1,(n,new);s=s.replace(new,row['text'])
  pre=s.encode()
 assert hashlib.sha256(pre).hexdigest()==before[n],n
 preimages[n]=pre;postimages[n]=post
 patches.append(''.join(difflib.unified_diff(pre.decode().splitlines(True),post.decode().splitlines(True),fromfile='a/'+n,tofile='b/'+n)))
(A/'owned-delta.patch').write_text(''.join(patches))
for name,items in [('owned-before-source.tar.gz',preimages),('owned-after-source.tar.gz',postimages)]:
 with tarfile.open(A/name,'w:gz') as archive:
  for n,b in sorted(items.items()):
   member=tarfile.TarInfo(n);member.size=len(b);member.mtime=0;archive.addfile(member,io.BytesIO(b))
(A/'owned-after.json').write_text(json.dumps(after,indent=2)+'\n')
(A/'changed-files.json').write_text(json.dumps({n:{'before':before[n],'after':after[n]} for n in changed},indent=2)+'\n')
# Parse-only formatting check; no compiler/build/test/runtime.
cmd=[str(W.parent/'toolchains/go1.26.3/bin/gofmt'),'-d',*changed];r=subprocess.run(cmd,cwd=W,capture_output=True)
(A/'gofmt-final.stdout').write_bytes(r.stdout);(A/'gofmt-final.stderr').write_bytes(r.stderr);assert r.returncode==0 and not r.stdout and not r.stderr
methods=['AcquireSnapshot','AcquireBackendSnapshotFastPath','AcquireStableSnapshot','AcquireSnapshotForRoot'];calls=[];bad=[]
for n in before:
 for num,line in enumerate((W/n).read_text().splitlines(),1):
  for method in methods:
   if '.'+method+'(' not in line:continue
   calls.append({'path':n,'line':num,'method':method,'text':line})
   if not (re.search(r'\breturn\s+',line) or re.search(r'\w+\s*,\s*\w+\s*:?=',line)):bad.append((n,num,line))
assert not bad,bad
(A/'caller-inventory-after.json').write_text(json.dumps(calls,indent=2)+'\n')
validation={'mode':'source-only; no typecheck or execution','gofmt_argv':cmd,'gofmt_exit':r.returncode,'gofmt_stdout_empty':not r.stdout,'gofmt_stderr_empty':not r.stderr,'changed_files':len(changed),'owned_preexisting_files':len(before),'target_calls':{m:sum(x['method']==m for x in calls) for m in methods},'unchecked_arity_sites':bad,'preimages_verified':True,'go_builds':0,'go_tests':0,'go_benchmarks':0,'go_profiles':0,'git_mutations':0}
(A/'validation.json').write_text(json.dumps(validation,indent=2)+'\n')
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=W,text=True).strip();branch=subprocess.check_output(['git','branch','--show-current'],cwd=W,text=True).strip()
freeze={'head':head,'branch':branch,'scope':'ONLY existing *_test.go outside TreeDB/db/** and TreeDB/lifecycle/**; not a whole-tree freeze','files':after,'changed':{n:after[n] for n in changed},'source04_manifest':'3b7e1bf114e613fe3f7cbf29f99b7dcdb6b54f8d12a3c06fef71075b8fc558f0','other_writers_active':True}
(A/'owned-freeze.json').write_text(json.dumps(freeze,indent=2)+'\n')
print(json.dumps({**validation,'gofmt_argv':['pinnedGo1.26.3/gofmt','-d','<216 owned paths>'],'head':head,'branch':branch,'freeze_sha256':hashlib.sha256((A/'owned-freeze.json').read_bytes()).hexdigest()},indent=2))
