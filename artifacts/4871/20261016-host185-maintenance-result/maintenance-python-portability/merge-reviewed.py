"""Root-only merge after final exact-head gate; no diagnostic dispatch."""
import datetime,json,subprocess
from pathlib import Path
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');A=D/'maintenance-python-portability';W=D/'maintenance-landed-harness'
H='8db93fec7176d23bad95168954884da807ee1da4';P='repos/snissn/gomap/pulls/5138';O=A/'final-gate'
def api(ep):return json.loads(subprocess.check_output(['gh','api',ep],cwd=W,text=True))
def save(n,x):(O/n).write_text(json.dumps(x,indent=2)+'\n');return x
gate=json.loads((O/'acceptance.json').read_text())
assert gate['head']==H and gate['checks_success']==50 and gate['review_disposition']=='ACCEPT' and gate['unresolved_threads']==0
assert not (A/'merge-binding.json').exists()
pr=save('pre-merge-pr.json',api(P));assert pr['head']['sha']==H and pr['base']['sha']==gate['base'] and pr['state']=='open' and not pr['merged']
body='''The maintenance diagnostic stopped before its first Go build because its contract test hashed Python's AST serialization, which changes in Python3.12. This repair hashes the exact source segments of the three reviewed collector functions instead. The collector, fixed baseline/candidate, parser, schedule, cleanup rules and workflow are unchanged.

The original single dispatch [38830037922](https://github.com/snissn/gomap/actions/runs/38830037922) remains a failed harness attempt with zero builds and zero benchmark calls. It is not performance evidence.

Validation at `8db93fec7176d23bad95168954884da807ee1da4`: all10 pure controls pass under actual Python3.9.23 and3.10.12; the documented Python3.12 AST shape reproduces the original hosted mismatch. All50 current-head CI checks pass. Independent `INTERNAL_SUBAGENT_REVIEW`, exact `gpt-6.1-sol`, accepts the one-file delta with zero findings; final classifier is NOT_REQUESTED and there are zero unresolved review threads. Hosted AI review was not requested because it cannot pin the user-required model; the task-local fallback policy applies.

[Retained failure, exact source comparison, local validation and review](https://github.com/snissn/gomap/tree/3c20504a46de42da8450ed10dd05d72b0ec57dd2/artifacts/4871/20261016-host185-maintenance-portability). Actual successor Python3.12 diagnostic execution remains a separate step after landing. No ordinary cost, original M0, native Q32/ACK0 or graph qualification is claimed.
'''
# No external edit may be overwritten.
assert api(P)['body']==pr['body']
save('final-body-request.json',{'body':body})
subprocess.run(['gh','api','-X','PATCH',P,'--input',str(O/'final-body-request.json')],cwd=W,check=True,stdout=subprocess.DEVNULL)
assert api(P)['body']==body
(A/'pr-body-final.md').write_text(body)
if pr['draft']:
 query='mutation($id:ID!){ markPullRequestReadyForReview(input:{pullRequestId:$id}) { pullRequest { id isDraft headRefOid } } }'
 result=json.loads(subprocess.check_output(['gh','api','graphql','-f','query='+query,'-f','id='+pr['node_id']],cwd=W,text=True));save('ready-response.json',result)
 ready=result['data']['markPullRequestReadyForReview']['pullRequest'];assert not ready['isDraft'] and ready['headRefOid']==H
pr=save('ready-pr.json',api(P));assert pr['head']['sha']==H and pr['base']['sha']==gate['base'] and not pr['draft']
assert api('repos/snissn/gomap/git/ref/heads/main')['object']['sha']==gate['base']
save('merge-request.json',{'sha':H,'merge_method':'merge'})
p=subprocess.run(['gh','api','-X','PUT',P+'/merge','--input',str(O/'merge-request.json')],cwd=W,capture_output=True,text=True)
save('merge-response.json',{'exit':p.returncode,'stdout':p.stdout,'stderr':p.stderr})
assert p.returncode==0,'Inspect actual merge outcome; do not retry blindly.'
response=json.loads(p.stdout);assert response['merged']
pr=save('merged-pr.json',api(P));assert pr['merged'] and pr['state']=='closed' and pr['head']['sha']==H and pr['merge_commit_sha']==response['sha']
commit=save('merged-commit.json',api('repos/snissn/gomap/git/commits/'+response['sha']))
assert [x['sha']for x in commit['parents']]==[gate['base'],H] and commit['tree']['sha']==gate['tree']
assert api('repos/snissn/gomap/git/ref/heads/main')['object']['sha']==response['sha']
binding={'head':H,'base':gate['base'],'merged':response['sha'],'tree':gate['tree'],'merged_at':pr['merged_at'],'checks_success':50,'review':'INTERNAL_SUBAGENT_REVIEW gpt-6.1-sol ACCEPT','unresolved_threads':0,'runtime_calls':0,'worktree_retained':'Required exact source/provenance for fresh bounded diagnostic.'}
(A/'merge-binding.json').write_text(json.dumps(binding,indent=2)+'\n');print(json.dumps(binding))
