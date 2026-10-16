"""Root-only merge after final exact-head gate; no diagnostic dispatch."""
import datetime,json,subprocess
from pathlib import Path
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');A=D/'ordinary-main-integration';W=D/'ordinary-repair'
H='6d3fc3a6e3582810d7c80fc46b784a9b30fada80';P='repos/snissn/gomap/pulls/5127';O=A/'final-gate'
def api(ep):return json.loads(subprocess.check_output(['gh','api',ep],cwd=W,text=True))
def save(n,x):(O/n).write_text(json.dumps(x,indent=2)+'\n');return x
gate=json.loads((O/'acceptance-with-raw-review.json').read_text())
assert gate['head']==H and gate['checks_success']==56 and gate['review_disposition']=='ACCEPT' and gate['unresolved_threads']==0
assert not (A/'merge-binding.json').exists()
pr=save('pre-merge-pr.json',api(P));assert pr['head']['sha']==H and pr['base']['sha']==gate['base'] and pr['state']=='open' and not pr['merged']
body=(A/'pr-body-final.md').read_text()
# No external edit may be overwritten.
assert api(P)['body']==pr['body']
save('final-body-request.json',{'body':body})
subprocess.run(['gh','api','-X','PATCH',P,'--input',str(O/'final-body-request.json')],cwd=W,check=True,stdout=subprocess.DEVNULL)
assert api(P)['body']==body
assert (A/'pr-body-final.md').read_text()==body
if pr['draft']:
 query='mutation($id:ID!){ markPullRequestReadyForReview(input:{pullRequestId:$id}) { pullRequest { id isDraft headRefOid } } }'
 result=json.loads(subprocess.check_output(['gh','api','graphql','-f','query='+query,'-f','id='+pr['node_id']],cwd=W,text=True));save('ready-response.json',result)
 ready=result['data']['markPullRequestReadyForReview']['pullRequest'];assert not ready['isDraft'] and ready['headRefOid']==H
pr=save('ready-pr.json',api(P));assert pr['head']['sha']==H and pr['base']['sha']==gate['base'] and not pr['draft']
assert api('repos/snissn/gomap/git/ref/heads/main')['object']['sha']==gate['base']
checks=json.loads(subprocess.check_output(['gh','pr','checks','5127','--repo','snissn/gomap','--json','name,state'],cwd=W,text=True));save('pre-merge-checks.json',checks)
assert len(checks)==56 and all(c['state']=='SUCCESS' for c in checks)
query='query($owner:String!,$name:String!,$number:Int!){repository(owner:$owner,name:$name){pullRequest(number:$number){headRefOid mergeable mergeStateStatus reviewThreads(first:100){nodes{isResolved} pageInfo{hasNextPage}}}}}'
review=json.loads(subprocess.check_output(['gh','api','graphql','-f','query='+query,'-f','owner=snissn','-f','name=gomap','-F','number=5127'],cwd=W,text=True));save('pre-merge-threads.json',review)
review=review['data']['repository']['pullRequest'];assert review['headRefOid']==H and review['mergeable']=='MERGEABLE' and review['mergeStateStatus']=='CLEAN' and not review['reviewThreads']['pageInfo']['hasNextPage'] and all(t['isResolved']for t in review['reviewThreads']['nodes'])
save('merge-request.json',{'sha':H,'merge_method':'merge'})
p=subprocess.run(['gh','api','-X','PUT',P+'/merge','--input',str(O/'merge-request.json')],cwd=W,capture_output=True,text=True)
save('merge-response.json',{'exit':p.returncode,'stdout':p.stdout,'stderr':p.stderr})
assert p.returncode==0,'Inspect actual merge outcome; do not retry blindly.'
response=json.loads(p.stdout);assert response['merged']
pr=save('merged-pr.json',api(P));assert pr['merged'] and pr['state']=='closed' and pr['head']['sha']==H and pr['merge_commit_sha']==response['sha']
commit=save('merged-commit.json',api('repos/snissn/gomap/git/commits/'+response['sha']))
assert [x['sha']for x in commit['parents']]==[gate['base'],H] and commit['tree']['sha']==gate['tree']
assert api('repos/snissn/gomap/git/ref/heads/main')['object']['sha']==response['sha']
binding={'head':H,'base':gate['base'],'merged':response['sha'],'tree':gate['tree'],'merged_at':pr['merged_at'],'checks_success':56,'review':'INTERNAL_SUBAGENT_REVIEW gpt-6.1-sol ACCEPT','unresolved_threads':0,'runtime_calls':0,'worktree_retained':'Required product, review, measurement and subsequent native integration provenance.'}
(A/'merge-binding.json').write_text(json.dumps(binding,indent=2)+'\n');print(json.dumps(binding))
