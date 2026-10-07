import copy, datetime, hashlib, json, os, runpy, subprocess
from pathlib import Path

O=Path('/home/mikers/gomap-r1-final-observers-20261006')
S=Path('/home/mikers/gomap-r1-publication-stage-M-3325dfe-20261006')
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
load=lambda p:json.loads(p.read_text())
draft=O/'M-3325dfe-public-builder-draft-original/draft-root-inputs.json'
assert sha(draft)=='3ab4b7c15f2a0749f57a2272bb462413ee57e27d0da35406f31e9bee6f1cfc8e'
c=load(draft); original=copy.deepcopy(c)
assert c['public_artifact_git_sha']=='24ac6866992dd89ab222b8bf03d60e6c0c49a2d5'
release=load(O/'M-3325dfe-public-release-actual-original.json')
assert sha(O/'M-3325dfe-public-release-actual-original.json')=='f6edc257afe37b9afeeb2ecd21e2c922a7523ca091dc769fde890f52ed70bf90'
assert release['tag']['object']['sha']==c['public_artifact_git_sha']
asset=release['release']['assets'][0]
assert asset['digest']=='sha256:'+c['overlay']['sha256'] and asset['size']==c['overlay']['bytes'] and asset['state']=='uploaded'
assert not release['release']['draft']
acquisition=load(O/'M-3325dfe-public-git-acquisition-original.json')
assert len(acquisition['commands'])==6 and all(x['exit']==0 for x in acquisition['commands'])
assert acquisition['private_source_used'] is False
review=load(O/'handoff.json')
assert sha(O/'handoff.json')=='e05ac737f5fced266783983eef67cfcdd74d1073ad27a9495480341f04a90494'
assert review['verdict']=='CLEAN_WITH_PENDING_ROOT_APPROVAL_AND_PUBLIC_REPLAY' and review['findings']==[]
assert review['public_artifact_git_sha']==c['public_artifact_git_sha']
support={p.relative_to(S).as_posix():sha(p) for p in S.rglob('SUPPORT_SHA256SUMS')}
assert support=={x['stage_relative']:x['sha256'] for x in c['support_ledgers']} and len(support)==9
costs={}
for lane in ('A','C','D'):
    row=c['actual_lanes'][lane];p=S/row['root_acceptance_stage_relative']
    assert sha(p)==row['root_acceptance_sha256']
    cost=load(p)
    assert cost['accepted'] is True and cost['actual_main_commit']==c['selected_actual_M_commit']
    costs[lane]={'sha256':sha(p),'status':'ORIGINAL_ROOT_FINITE_COST_ACCEPTANCE_REVIEWED'}
    assert row['root_accepted'] is False
    row['root_accepted']=True
for k in ('root_approved','public_artifact_landing_reachable','support_ledger_list_complete'):
    assert c[k] is False;c[k]=True
assert c['required_final_selection']['fresh_actual_A_C_D_records_added'] is False
c['required_final_selection']['fresh_actual_A_C_D_records_added']=True
assert c['A_certificate']['approved'] is False
c['A_certificate']['approved']=True
c['status']='ROOT_APPROVED_ACTUAL_CAPTURE_GATES_PUBLIC_BYTES_REPLAY_REQUIRED'
helper=O/'M-3325dfe-public-replay-provenance-v2-preparation/replay-public.py'
assert sha(helper)=='1bcdfa4ebc4489b2292caeb25a3f5ffd38a1adb347a0e455e8f4d736c4a07fcc'
runpy.run_path(str(helper),run_name='root_input_contract_validation')['validate'](c)
p=O/'M-3325dfe-public-root-approved-inputs-original.json'
with p.open('x') as f:json.dump(c,f,indent=2,sort_keys=True);f.write('\n')
with (O/'M-3325dfe-public-replay-root-approval-original.json').open('x') as f:
    json.dump({'owner':'/root','utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'draft_sha256':sha(draft),'root_approved_inputs_sha256':sha(p),'public_artifact_commit':c['public_artifact_git_sha'],
        'original_finite_cost_gates':costs,'structural_review_sha256':sha(O/'handoff.json'),
        'release_actual_receipt_sha256':sha(O/'M-3325dfe-public-release-actual-original.json'),
        'public_git_acquisition_sha256':sha(O/'M-3325dfe-public-git-acquisition-original.json'),
        'public_replay_completed':False,'root_acceptance_authority':'Original coordinator decisions, independent structural review and actual public landing; no approval inferred from the builder.',
        'lock_owner':'Reviewed executor acquires canonical flock itself; no conflicting outer flock.'},f,indent=2,sort_keys=True);f.write('\n')
argv=['python3','-B',str(helper),'--inputs',str(p),'--expected-inputs-sha256',sha(p)]
with (O/'M-3325dfe-public-executor-start-original.json').open('x') as f:
    json.dump({'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'argv':argv,'helper_sha256':sha(helper)},f,indent=2);f.write('\n')
env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1');env.pop('PYTHONOPTIMIZE',None)
with (O/'M-3325dfe-public-executor-stdout-original.log').open('xb') as out, (O/'M-3325dfe-public-executor-stderr-original.log').open('xb') as err:
    r=subprocess.run(argv,env=env,stdout=out,stderr=err)
with (O/'M-3325dfe-public-executor-exit-original.json').open('x') as f:
    json.dump({'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'exit':r.returncode,
        'stdout_sha256':sha(O/'M-3325dfe-public-executor-stdout-original.log'),
        'stderr_sha256':sha(O/'M-3325dfe-public-executor-stderr-original.log')},f,indent=2);f.write('\n')
print(json.dumps({'exit':r.returncode,'root_inputs_sha256':sha(p),'proof':c['fresh_linux111']['proof_absolute']}))
if r.returncode: print((O/'M-3325dfe-public-executor-stderr-original.log').read_text()[-4000:])
raise SystemExit(r.returncode)
