from pathlib import Path
import subprocess,json,hashlib,datetime,shutil
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');W=D/'ordinary-repair';O=D/'ordinary-hook-work'
def git(*args): return subprocess.check_output(['git',*args],cwd=W,text=True).strip()
def digest(p):return hashlib.sha256(p.read_bytes()).hexdigest()
files=git('ls-files','--cached','--others','--exclude-standard').splitlines()
freeze={'head':git('rev-parse','HEAD'),'at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'files':{f:digest(W/f) for f in files if (W/f).is_file()}}
changed=sorted(set(git('diff','--name-only').splitlines()+git('ls-files','--others','--exclude-standard').splitlines()))
assert not git('diff','--cached','--name-only')
assert not subprocess.check_output(['git','diff','--check'],cwd=W,text=True)
(O/'freeze-final.json').write_text(json.dumps(freeze,sort_keys=True,indent=2)+'\n')
(O/'changed-files-final.json').write_text(json.dumps(changed,indent=2)+'\n')
packet=O/'source-final';packet.mkdir()
for f in changed:
 target=packet/f;target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(W/f,target)
(O/'diff-final.patch').write_bytes(subprocess.check_output(['git','diff','--binary'],cwd=W))
results=[]
for name in ['union-repair-full-normal-04','coherent-final-normal-02','coherent-final-race-02','generic-final-race-04','windows-final-build-01','reset-current-build-01','reset-prior-build-01']:
 p=O/'logs'/name
 before=json.loads((p/'before.json').read_text());after=json.loads((p/'after.json').read_text());r=json.loads((p/'result.json').read_text());raw=(p/'raw.txt').read_text().splitlines()
 assert before==after=={'head':freeze['head'],'files':freeze['files']},name
 assert r['exit']==0,(name,r)
 r.update(name=name,source_unchanged=True,source_matches_final=True,command=str(p/'command.json'),raw=str(p/'raw.txt'),top_pass=sum(s.startswith('--- PASS:') for s in raw),top_skip=sum(s.startswith('--- SKIP:') for s in raw),sub_pass=sum(s.lstrip().startswith('--- PASS:') and not s.startswith('--- PASS:') for s in raw),fail_events=[s for s in raw if s.lstrip().startswith('--- FAIL:')],packages=[s for s in raw if s.startswith('ok  ')])
 results.append(r)
(O/'validation-final.json').write_text(json.dumps(results,indent=2)+'\n')
artifacts={str(p.relative_to(O)):digest(p) for p in list((O/'windows-final').glob('*.exe'))+list((O/'get-source-audit').glob('reset-*.test'))}
receipt={'head':freeze['head'],'source_count':len(freeze['files']),'changed_count':len(changed),'freeze_sha256':digest(O/'freeze-final.json'),'changed_sha256':digest(O/'changed-files-final.json'),'artifact_sha256':artifacts,'root_owned_changed_paths':['.github/workflows/treedb-tests.yml'],'writer_lease':'released after explicit handoff','nonclaims':['No successor hosted CI or platform runtime qualification','No successor performance gate PASS','No ordinary/native/graph acceptance','No Git mutations performed']}
(O/'completion-final.json').write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps(receipt,indent=2))
