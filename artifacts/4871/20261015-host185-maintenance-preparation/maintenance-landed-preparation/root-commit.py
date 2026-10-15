import hashlib,json,subprocess,datetime
from pathlib import Path
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');W=D/'maintenance-landed-harness';A=D/'maintenance-landed-preparation'
def git(*a):return subprocess.check_output(['git',*a],cwd=W)
def sha(x):return hashlib.sha256(x).hexdigest()
r=json.loads((A/'review.json').read_text());m=json.loads((A/'root-integration-final/source-final.json').read_text())
assert r['disposition']=='ACCEPT'
assert git('rev-parse','HEAD').decode().strip()==r['harness_base_commit']
assert not git('diff','--name-only') and not git('ls-files','--others','--exclude-standard')
assert set(git('diff','--cached','--name-only').decode().splitlines())==set(r['exact_staged_paths'])
assert sha(git('diff','--cached','--binary'))==r['frozen_staged_diff_sha256']
assert sha((A/'root-integration-final/source-final.json').read_bytes())==r['frozen_source_manifest_sha256']
for p,h in m.items():assert sha((W/p).read_bytes())==h,p
(A/'review-precommit.md').write_bytes((A/'review.md').read_bytes());(A/'review-precommit.json').write_bytes((A/'review.json').read_bytes())
subprocess.run(['git','diff','--cached','--check'],cwd=W,check=True)
subprocess.run(['git','commit','-m','ci: retain fixed maintenance cost diagnostics'],cwd=W,check=True)
head=git('rev-parse','HEAD').decode().strip();assert not git('status','--porcelain')
assert git('rev-parse','HEAD^').decode().strip()==r['harness_base_commit']
files=git('ls-tree','-r','--name-only','HEAD').decode().splitlines();assert set(files)==set(m)
p=subprocess.Popen(['git','cat-file','--batch'],cwd=W,stdin=subprocess.PIPE,stdout=subprocess.PIPE)
for f in files:
 p.stdin.write(('HEAD:'+f+'\n').encode());p.stdin.flush();fields=p.stdout.readline().split();n=int(fields[2]);data=p.stdout.read(n);assert p.stdout.read(1)==b'\n';assert sha(data)==m[f],f
p.stdin.close();p.wait();assert p.returncode==0
b={'head':head,'parent':r['harness_base_commit'],'tree':git('rev-parse','HEAD^{tree}').decode().strip(),'all_committed_blobs_verified':len(files),'source_manifest_sha256':r['frozen_source_manifest_sha256'],'staged_diff_sha256':r['frozen_staged_diff_sha256'],'review_precommit_sha256':sha((A/'review-precommit.json').read_bytes()),'clean':True,'utc':datetime.datetime.now(datetime.timezone.utc).isoformat()}
(A/'commit-binding.json').write_text(json.dumps(b,indent=2)+'\n');print(json.dumps(b))
