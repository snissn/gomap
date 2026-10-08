from pathlib import Path
import collections, datetime, hashlib, json, os, shlex, signal, subprocess
R=Path('/private/tmp/gomap-r1-arch-5090-local-20261008')
D=R/'state/M23-phase2-preparation-v11'
sha=lambda p: hashlib.sha256(Path(p).read_bytes()).hexdigest()
load=lambda p: json.loads(Path(p).read_text())
save=lambda p,v: Path(p).write_text(json.dumps(v,indent=2,sort_keys=True)+'\n')
rec=load(D/'fetched/receipt.json'); ev=load(D/'ROOT-live-audit-evidence.json')
outer=load(D/'outer-receipt.json'); fetch=load(D/'ROOT-fetch-outer.json')
assert outer['actual_wait'] and outer['exit']==1 and outer['group_absent'] and not outer['signals'] and outer['failure'] is None
assert fetch['actual_wait'] and fetch['exit']==0 and fetch['group_absent'] and fetch['exception'] is None
for name,h in ev['decoded_files'].items(): assert sha(D/'fetched'/name)==h
assert sha(D/'ROOT-fetch-outer.json')==ev['fetch_outer_receipt_sha256']
assert sha(D/'source.json')==rec['source_manifest_sha256']=='3491dcbae1acf2a1d8758fabd3b62020b63355e697cc44335f806abe9b10e9ff'
assert rec['source_paths']==ev['source_paths']==10074
assert all(ev[k] for k in ['source_exact','tools_inputs_binaries_exact','pinned_toolchain_exact','no_live_owned_driver_or_ELF'])
assert rec['active'] is None and rec['all_processes_joined'] and rec['final_audit_error'] is None
commands=rec['commands']; assert len(commands)==25 and [c['exit'] for c in commands]==[0]*24+[1]
assert commands[-1]['name']=='normal-rootpublication-functional'
for c in commands:
 assert c['actual_wait'] and c['group_absent'] and c['adopted_actual_wait_complete']
 assert not c['exception'] and not c['timeout'] and not c['unexpected_group_survived_parent']
 assert all(a['actual_waitpid'] for a in c['adopted_descendants'])
 assert sha(D/'fetched'/(c['name']+'.log'))==c['raw_sha256'] and sha(D/'fetched'/(c['name']+'.stderr.log'))==c['stderr_sha256']
spec=load(D/'fetched/run-spec.json');expected_all=set(map(tuple,spec['expected_tests']))
assert len(expected_all)==253 and len(spec['mandatory_risk_tests'])==57
known_fail={'TestMergeAppendOnlyLogicalObligationsCrossKindCoalescingChecks','TestMergeAppendOnlyLogicalObligationsKeepsSmallBuilderLinear','TestMergeAppendOnlyLogicalObligationsAmbiguousPhysicalCandidatesUseExactFallback','TestMergeAppendOnlyLogicalObligationsCrossKindCollisionUsesExactFallback','TestMergeAppendOnlyLogicalObligationsRepresentativeReplacementUsesExactFallback'}
top_counts=dict(run=0,pass_count=0,fail=0,skip=0)
for p in spec['packages'][:5]:
 events=[json.loads(x) for x in (D/'fetched'/('normal-'+p['tag']+'-functional.log')).read_text().splitlines()]
 assert all(e.get('Package')==p['import_path'] for e in events)
 def names(action):return [e['Test'] for e in events if e.get('Action')==action and e.get('Test') and '/' not in e['Test']]
 expected={t for pkg,t in expected_all if pkg==p['import_path']}
 assert set(names('run'))==expected and len(names('run'))==len(expected) and not names('skip')
 assert len(names('pass'))+len(names('fail'))==len(expected)
 if p['tag']=='rootpublication':
  assert set(names('fail'))==known_fail and len(names('fail'))==5
  assert all('stable resource has an admitted operation or uncertain cleanup' in ''.join(e.get('Output','') for e in events if e.get('Test')==name or e.get('Test','').startswith(name+'/')) for name in known_fail)
  assert [e['Action'] for e in events if not e.get('Test') and e.get('Action') in ['pass','fail']]==['fail']
 else:
  assert set(names('pass'))==expected and not names('fail')
  assert [e['Action'] for e in events if not e.get('Test') and e.get('Action') in ['pass','fail']]==['pass']
 for a,k in [('run','run'),('pass','pass_count'),('fail','fail'),('skip','skip')]:top_counts[k]+=len(names(a))
audit=dict(at=datetime.datetime.now(datetime.timezone.utc).isoformat(),ROOT_audited=True,
 result='REJECT five rootpublication merge disposal paths with admitted operation; DB package leak guard nowPASS; production inquiry pending',
 outer_tool_session=42386,outer_tool_exit=1,fetch_tool_session=55250,fetch_tool_exit=0,
 remote_receipt_sha256=sha(D/'fetched/receipt.json'),evidence_sha256=sha(D/'ROOT-live-audit-evidence.json'),
 all_processes_joined=True,Go_phase_actual_wait_exit=[c['exit'] for c in commands],
 source_paths=10074,source_sha256=sha(D/'source.json'),tests_executed=True,top_counts=top_counts,db_package_goleak_passed=True,
 remaining_packages_and_race_not_executed=True,reservation=ev['reservation'],
 finite=False,performance=False,server111_window_still_ROOT_owned=True)
save(D/'ROOT-RED-audit.json',audit)
remote=r'''from pathlib import Path
import datetime,hashlib,json,os
R=Path('/home/mikers/dev/codex-r1-5090-functional-linux111-20261008')
q=json.loads(__import__('sys').argv[1]);live=[]
for p in Path('/proc').iterdir():
 if not p.name.isdigit():continue
 try: exe=os.readlink(p/'exe');cmd=(p/'cmdline').read_bytes().replace(b'\0',b' ').decode(errors='replace')
 except (FileNotFoundError,PermissionError,ProcessLookupError):continue
 base=Path(exe).name
 if base in ['go','compile','link','gofmt','test2json','unified-bench','collection_workload_bench'] or base.endswith('.test') or (str(R) in exe and base not in ['python3','python3.12','python3.13']):live.append(dict(pid=int(p.name),exe=exe))
assert not live,live
assert json.loads((R/'state/reservation.json').read_text())==q['reservation']
rec=R/'state/M23-phase2-functional-v11/receipt.json'
assert hashlib.sha256(rec.read_bytes()).hexdigest()==q['remote_receipt_sha256']
proof=dict(at=datetime.datetime.now(datetime.timezone.utc).isoformat(),ROOT_audit_sha256=q['ROOT_audit_sha256'],remote_receipt_sha256=q['remote_receipt_sha256'],all_remote_phases_joined=True,outer_SSH_and_tool_actual_join=True,fresh_conflicts=live,source_retained=True,failed_packet_retained=True,finite=False,performance=False,server111_runtime_released=True,reservation=q['reservation'])
p=R/'state/ROOT-M23-phase2-v11-runtime-release.json';p.write_text(json.dumps(proof,indent=2,sort_keys=True)+'\n')
print(json.dumps(dict(proof=proof,remote_proof_sha256=hashlib.sha256(p.read_bytes()).hexdigest())))
'''
(D/'ROOT-release-remote.py').write_text(remote)
payload=dict(reservation=ev['reservation'],remote_receipt_sha256=audit['remote_receipt_sha256'],ROOT_audit_sha256=sha(D/'ROOT-RED-audit.json'))
exception=None
with (D/'ROOT-release.stdout.json').open('xb') as out,(D/'ROOT-release.stderr.log').open('xb') as err:
 p=subprocess.Popen(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10','mikers@192.168.0.111','python3 -I -B -c '+shlex.quote(remote)+' '+shlex.quote(json.dumps(payload))],stdout=out,stderr=err,start_new_session=True)
 try: p.wait(timeout=60)
 except BaseException as e: exception=repr(e)
 finally:
  if p.poll() is None:
   os.killpg(p.pid,signal.SIGTERM)
   try:p.wait(timeout=10)
   except subprocess.TimeoutExpired:os.killpg(p.pid,signal.SIGKILL);p.wait()
  else:p.wait()
 absent=False
 try:os.killpg(p.pid,0)
 except ProcessLookupError:absent=True
 save(D/'ROOT-release-outer.json',dict(pid=p.pid,exit=p.returncode,actual_wait=True,group_absent=absent,exception=exception,stdout_sha256=sha(D/'ROOT-release.stdout.json'),stderr_sha256=sha(D/'ROOT-release.stderr.log')))
 assert p.returncode==0 and absent and exception is None,(p.returncode,(D/'ROOT-release.stderr.log').read_text())
proof=load(D/'ROOT-release.stdout.json');assert proof['proof']['fresh_conflicts']==[] and proof['proof']['server111_runtime_released']
proof['release_outer_sha256']=sha(D/'ROOT-release-outer.json')
save(D/'ROOT-runtime-release.json',proof)
print('ROOT_AUDIT_FAILED_PRODUCT_AND_GENUINE_RUNTIME_RELEASE',sha(D/'ROOT-RED-audit.json'),sha(D/'ROOT-runtime-release.json'))
