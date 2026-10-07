import datetime, hashlib, json, os, stat
from pathlib import Path
BASE=Path('/tmp/gomap-r1-execution-20261005')
OUT=BASE/'5061-postmeta-and-root-capture-support-preparation'
AA='aa8614ea79c63b21ca9a04580ace7f77a90b7f1b'
NEW='0245d8e0f5835dcba2929e4f1ba8b5d7ea909133'
OLD='a64560e86f45bf28162a8ed99d0fa984e051fd02'
RUNTIME='cb5b0d7b3633666ae16752d8c353eeeb23542c71e5e25773e33359d3ffa79aa9'
HARNESS='706b71f11508bf9c0075b0dd9c5463cd741a49cba83996feaf753330ace77822'
def sha(b): return hashlib.sha256(b).hexdigest()
def safe_read(q):
 s=q.lstat(); assert stat.S_ISREG(s.st_mode) and not q.is_symlink(),q
 assert not stat.S_IMODE(s.st_mode)&0o111, ('executable original',str(q))
 with q.open('rb') as f: b=f.read()
 assert b'\0' not in b and b.decode('utf-8') is not None,q
 return b,stat.S_IMODE(s.st_mode)
def write(q,b):
 q.parent.mkdir(parents=True,exist_ok=True)
 with q.open('xb') as f:f.write(b)
 q.chmod(0o644)
def dump(q,x):write(q,(json.dumps(x,indent=2,sort_keys=True)+'\n').encode())
plan=json.loads((BASE/'5061-support-assembly-complete-preparation/assembly-plan.json').read_bytes())
def prior_pins():
 pins={}
 for g in plan['groups']:
  root=BASE/g['name']
  for q in sorted(root.rglob('*')):
   assert not q.is_symlink(),q
   if q.is_file():pins[str(q)]=dict(sha256=sha(q.read_bytes()),bytes=q.stat().st_size,mode=f'{stat.S_IMODE(q.stat().st_mode):04o}')
 for name in ['5061-support-assembly-complete-preparation/assembly-plan.json','5061-support-assembly-complete-preparation/assemble-support-complete.py','5061-support-assembly-preparation/assemble-support.py']:
  q=BASE/name;pins[str(q)]=dict(sha256=sha(q.read_bytes()),bytes=q.stat().st_size,mode=f'{stat.S_IMODE(q.stat().st_mode):04o}')
 return pins
prior=prior_pins()
selected=[]
def add(group,head,names,classification,rationale):
 for name in names:selected.append(dict(group=group,sourceSHA=head,name=name,classification=classification,rationale=rationale))
add('postmeta',AA,[
 'a645-cow-power-loss-ci-diagnosis.md','a645-cow-power-loss-ci-diagnosis.json','a645-cow-postmeta-classification-repair.patch',
 'aa8614-postmeta-fixture-validation-root-review.json','5071-aa8614-root-adoption.json','5071-aa8614-postmeta-final-source-original.json',
 'checkpoint-postmeta-repair.md','checkpoint-postmeta-repair-actual.json','checkpoint-postmeta-repair-payload.json',
 '5071-postmeta-repaired-pr-body.md','aa8614-postpush-original.json','aa8614-hosted-review-1941-original.json',
 'postmeta-ci-refresh-original.log','postmeta-ci-refresh-check-original.log',
 'aa8614-test_refresh_ci_impact_inventory-uv-original.log','aa8614-test_ci_impact-uv-original.log',
 'aa8614-test_refresh_ci_impact_inventory.py.log','aa8614-test_ci_impact.py.log'],
 'POSTMETA_REPAIR_SOURCE_REVIEW_AND_ROOT_ADOPTION_NOT_LANDING',
 'Narrow actual ResourceMeta post-write fixture classification and canonical inventory; preserve skipped initial Python coverage versus actual uv PASS logs.')
add('postmeta-original-failure',OLD,['a645-windows-core5-failure-rest-original.log'],
 'ORIGINAL_REQUIRED_WINDOWS_FAIL_UNWAIVED','Original before-meta-sync classification failure; no retrospective PASS or causal relabel.')
add('postmeta-main-control','3cfe2ad896cad1818f72d3d14c74e548fecc1657',['main3cfe-windows-core5-original.log'],
 'ORIGINAL_MAIN_CONTROL_LIMITED_SCHEDULE','Original exact selected case observed on main; does not prove the failed candidate interleaving.')
add('root-capture-failure',AA,[
 'aa8614-windows-core2-failure-rest-original.log','aa8614-root-capture-ci-diagnosis.md','aa8614-root-capture-ci-diagnosis.json',
 'aa8614-first-failure-rollup-original.json','aa8614-runs-initial-original.json','aa8614-rootset-failure-focused-validation-root-review.json'],
 'ORIGINAL_WINDOWS_FAIL_AND_SOURCE_SUPPORTED_FIXTURE_BARRIER_GAP_UNWAIVED',
 'Eight-attempt capture did not converge before destructive work. Missing fixture durability barrier is source-supported; exact Windows predicate/schedule is untraced. Unchanged Linux normal/race pass is a separate observation.')
add('root-capture-main-control','3cfe2ad896cad1818f72d3d14c74e548fecc1657',['main3cfe-windows-core2-original.log'],
 'ORIGINAL_MAIN_LOG_NO_SAME_CASE_CONTROL','Original named main shard contains no selected target rows; no same-case green control claim.')
add('root-capture-repair',NEW,[
 'aa8614-root-capture-fixture-repair-review.md','aa8614-root-capture-fixture-repair-review.json','0245-root-capture-fixture-validation-root-review.json',
 '5071-0245-final-source-original.json','0245-postpush-original.json','5071-root-capture-repaired-pr-body.md',
 'root-capture-ci-refresh-original.log','root-capture-ci-refresh-check-original.log','root-capture-test_impact-uv-original.log','root-capture-test_inventory-uv-original.log'],
 'REPAIRED_FIXTURE_FOCUSED_PROOF_NOT_WINDOWS_CI_OR_LANDING',
 'Six-line checked pre-GC checkpoint barrier; original pointer/value/GC/reopen assertions preserved and controlled pending-root case separately passes. Canonical refresh/check and21+15 Python OK are local observations, not current CI acceptance.')
add('a645-windows-pass',OLD,['a645-windows-cow-fixture-root-review.json','a645-windows-pass-ci-job-snapshot-original.json','a645-windows-caching-rest1-success-rest-original.log'],
 'ORIGINAL_REPAIRED_WINDOWS_FIXTURE_JOB_PASS_NOT_WHOLE_WORKFLOW',
 'Job112443391281/run37514004072 completed success, selected fixture rows PASS; original workflow snapshot ongoing status remains unchanged. Empty failed-fetch placeholder excluded.')
add('a645-M0-extraction',OLD,['a645-m0-artifact-extraction-root-record.json','a645-m0-artifacts-original.json','a645-m0-run-completed-original.json'],
 'ORIGINAL_M0_ARTIFACT_METADATA_AND_EXTRACTION_NOT_R1_RETAINED_ACCEPTANCE',
 'Run37514004070/artifact11435933005 exact zip digest and text members; archive itself intentionally excluded. No original executable available here; no semantic replay claimed.')
artifact=json.loads((BASE/'a645-m0-artifact-extraction-root-record.json').read_bytes())
for name in sorted(artifact['files']):
 add('a645-M0-original-text',OLD,['a645-m0-artifact-original/'+name],
 'ORIGINAL_M0_AVAILABLE_PASS_GATES_SCOPED_AUXILIARY_NOT_R1_ACCEPTANCE',
 'Exact original extracted M0 commands/fixture/raw samples/results/summary; Go1.26.8, ten interleaved runs, own CV gates differ from retained R1 noise contract. Preserve results unchanged; no comparison to unobserved candidate or whole-DB bound.')
clusters=[('aa8614-postmeta-fixture-validation-original',AA,'postmeta-focused'),('aa8614-rootset-failure-focused-validation-original',AA,'root-capture-unchanged-focused'),('0245-root-capture-fixture-validation-original',NEW,'root-capture-repaired-focused')]
observations=[]
for directory,head,group in clusters:
 d=BASE/directory
 for q in sorted(d.iterdir()):
  add(group,head,[str(q.relative_to(BASE))],'ORIGINAL_FOCUSED_LINUX_CHECK_RECEIPT_NOT_WINDOWS_OR_RETAINED_GATE',
      'Exact source-clean before/after, argv/env/toolchain hash, actual log hash and exit receipts for format/normal/race/vet; no benchmark/capacity or Windows rerun claim.')
 for phase in ['format','normal','race','vet']:
  start=json.loads((d/f'{phase}-start.json').read_bytes());end=json.loads((d/f'{phase}-exit.json').read_bytes());log=d/f'{phase}.log'
  before=json.loads((d/'source-before.json').read_bytes());after=json.loads((d/'source-after.json').read_bytes())
  assert before==after and before['clean'] and before['commit']==head
  assert before['runtime_sha256']==RUNTIME and before['harness_sha256']==HARNESS
  assert start['source']==before and end['source']==before
  assert end['log_sha256']==sha(log.read_bytes()) and end['exit']==0
  observations.append(dict(group=group,sourceSHA=head,phase=phase,argv=start['argv'],actual_exit=end['exit'],environment=start['environment'],toolchain_sha256=start['go_sha256'],driver_sha256=start['driver_sha256'],log=str(log),log_sha256=end['log_sha256'],start_receipt=str(d/f'{phase}-start.json'),exit_receipt=str(d/f'{phase}-exit.json')))
# Every M0 copied text member matches the preexisting extraction receipt.
for name,expected in artifact['files'].items():
 b=(BASE/'a645-m0-artifact-original'/name).read_bytes();assert len(b)==expected['bytes'] and sha(b)==expected['sha256']
files=[];missing=[]
for item in selected:
 original=BASE/item['name']
 if not original.exists():missing.append(dict(**item,reason='ORIGINAL_NOT_AVAILABLE_NO_FILE_FABRICATED'));continue
 b,mode=safe_read(original);copy='originals/'+item['name'];q=OUT/copy
 assert not q.exists(),q
 write(q,b)
 row=dict(**{k:v for k,v in item.items() if k!='name'},original=str(original),original_relative=item['name'],copy=copy,original_sha256=sha(b),copy_sha256=sha(b),original_bytes=len(b),copy_bytes=len(b),original_mode=f'{mode:04o}',copy_mode='0644',original_nonsymlink=True,copy_nonsymlink=True,utf8_non_nul_exact=True,source_kind='PREEXISTING_RETAINED_TEXT')
 if original.name.endswith('.log'):
  receipt=next((o for o in observations if o['log']==str(original)),None)
  row['actual_process_receipt']=receipt if receipt else None
  row['nonreceipt_log_limit']=None if receipt else 'Log text only unless separately retained original root/job receipt states otherwise; do not infer process exit/argv from PASS/OK text.'
 files.append(row)
assert prior==prior_pins(),'earlier support/assembler changed'
for row in files:
 b,m=safe_read(Path(row['original']));c,cm=safe_read(OUT/row['copy'])
 assert b==c and sha(b)==row['original_sha256'] and m==int(row['original_mode'],8) and cm==0o644
sources={}
for n in ['5071-aa8614-postmeta-final-source-original.json','5071-0245-final-source-original.json']:
 x=json.loads((BASE/n).read_bytes());assert x['runtime_sha256']==RUNTIME and x['harness_sha256']==HARNESS and x['clean'];sources[x['commit']]=dict(original=n,sha256=sha((BASE/n).read_bytes()),runtime_sha256=x['runtime_sha256'],harness_sha256=x['harness_sha256'],clean=x['clean'])
assert sources[AA]['runtime_sha256']==sources[NEW]['runtime_sha256']
record=dict(schema_version=1,supplemental_group_number=10,status='UNPROMOTED_PENDING_ROOT_ADOPTION',exact_candidate=NEW,prior_postmeta_head=AA,older_windows_and_M0_head=OLD,runtime_sha256=RUNTIME,A_C_harness_sha256=HARNESS,source_sidecars=sources,files=files,original_bytes=sum(r['original_bytes'] for r in files),retained_text_original_count=len(files),missing_originals=missing,frozen_nine_groups_and_assemblers_unchanged=dict(files=len(prior),bytes=sum(v['bytes'] for v in prior.values()),pins=prior),observations=observations,excluded=[dict(path=str(BASE/'a645-m0-artifact-original.zip'),reason='ZIP_BINARY_EXCLUDED_ORIGINAL_HASH_RETAINED_IN_EXTRACTION_RECORD'),dict(path=str(BASE/'a645-windows-caching-rest1-success-original.log'),reason='EMPTY_FAILED_FETCH_PLACEHOLDER_NOT_JOB_PROOF'),dict(path=str(BASE/'aa8614-required-ci-watch.log'),reason='POTENTIALLY_ACTIVE_WATCH_LOG_EXCLUDED_NO_CI_POLL'),dict(path=str(BASE/'5071-aa8614-postmeta-repair.bundle'),reason='GIT_BUNDLE_BINARY_EXCLUDED'),dict(path=str(BASE/'5071-0245-root-capture-repair.bundle'),reason='GIT_BUNDLE_BINARY_EXCLUDED')],limits=['No current0245 required CI/hosted result observed by this task; pending gate stays pending.','No actual5071 landing, retained A/C/D packet, public publication, semantic replay or performance/capacity acceptance.','Source maps/runtime/A-C harness unchanged does not certify binary/performance equality or exact historical Windows cause.','Initial aa8614/core2 FAIL and a645/core5 FAIL remain unchanged. Linux passes do not relabel them.','Existing M0 run uses own fixture/CV policy and Go1.26.8; cannot transfer measurements to R1 Go1.26.4 comparator or D.'],utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
dump(OUT/'inventory.json',record)
dump(OUT/'readiness.json',dict(status=record['status'],ready_for_root_byte_review=True,root_adoption=False,publication=False,actual_landing_observed=False,current_required_CI_observed=False,files=len(files),bytes=record['original_bytes'],all_original_copy_bytes_equal=True,UTF8_regular_nonsymlink_nonexec=True,focused_receipt_observations=len(observations),prior_files_verified_unchanged=len(prior),missing=missing))
print(json.dumps({'files':len(files),'bytes':record['original_bytes'],'observations':len(observations),'missing':missing,'inventory_sha256':sha((OUT/'inventory.json').read_bytes()),'prior_files_unchanged':len(prior)},indent=2))
