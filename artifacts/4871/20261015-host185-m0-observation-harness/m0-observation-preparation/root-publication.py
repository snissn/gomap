from pathlib import Path
import collections,hashlib,json,subprocess
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');W=D/'m0-observation-harness';A=D/'m0-observation-preparation'
head='939b10c22e54caf56dc84553b65e2435862052fb';branch='codex/4871-m0-observation-185-20261015'
def git(*args):return subprocess.check_output(['git',*args],cwd=W,text=True).strip()
assert git('rev-parse','HEAD')==head and git('status','--porcelain')==''
assert git('ls-remote','origin','refs/heads/'+branch).split()[0]==head
manifest=json.loads((A/'root-integration-final/source-final.json').read_text())
for p,h in manifest.items():assert hashlib.sha256((W/p).read_bytes()).hexdigest()==h,p
pr=json.loads(subprocess.check_output(['gh','api','repos/snissn/gomap/pulls/5136'],text=True));assert pr['head']['sha']==head and pr['draft']
(A/'root-publication.json').write_text(json.dumps({'head':head,'tree':git('rev-parse','HEAD^{tree}'),'base':pr['base']['sha'],'branch':branch,'pr':pr['html_url'],'clean':True,'remote_verified':True,'source_manifest_verified_files':len(manifest),'review_json_sha256':hashlib.sha256((A/'review.json').read_bytes()).hexdigest(),'runtime_calls':0,'not_yet_merged':True},indent=2)+'\n')
snapshot=json.loads(subprocess.check_output(['gh','pr','view','5136','--repo','snissn/gomap','--json','number,url,headRefOid,baseRefOid,mergeable,mergeStateStatus,isDraft,statusCheckRollup,reviews,comments'],text=True))
(A/'pr-snapshot-01.json').write_text(json.dumps(snapshot,indent=2)+'\n')
print(json.dumps({'head':head,'source_files':len(manifest),'pr':pr['html_url'],'checks':dict(collections.Counter(c.get('conclusion') or c.get('status') or c.get('state') for c in snapshot['statusCheckRollup']))}))
