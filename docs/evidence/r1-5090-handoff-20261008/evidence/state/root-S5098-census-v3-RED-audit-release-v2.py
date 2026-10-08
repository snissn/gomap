from pathlib import Path
import collections, datetime, hashlib, json, os, shlex, signal, subprocess
R=Path('/private/tmp/gomap-r1-arch-5090-local-20261008')
D=R/'state/S5098-storage-census-runtime-v3'
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
load=lambda p:json.loads(Path(p).read_text())
save=lambda p,v:Path(p).write_text(json.dumps(v,indent=2,sort_keys=True)+'\n')
ev=load(D/'ROOT-live-audit-evidence.json');rec=load(D/'fetched/receipt.json');outer=load(D/'outer-receipt.json');fo=load(D/'ROOT-fetch-outer-receipt.json')
assert outer['actual_wait'] and outer['group_absent'] and outer['exit']==1 and outer['failure'] is None and not outer['signals']
assert fo['actual_wait'] and fo['group_absent'] and fo['exit']==0 and fo['exception'] is None and not fo['signals']
assert rec['all_processes_joined'] and rec['active'] is None and rec['final_audit_error'] is None
assert all(ev[k] for k in ['source_exact','tools_inputs_binaries_exact','pinned_toolchain_exact','no_live_owned_driver_or_ELF'])
assert ev['source_paths']==rec['source_paths']==9878 and sha(D/'source.json')==rec['source_manifest_sha256']=='5e5acae9d69c8d31c1c440d7a475c5e118d00903fb7a30b7727e51d9b5c44918'
for n,h in ev['decoded_files'].items():assert sha(D/'fetched'/n)==h
cs=rec['commands'];assert len(cs)==20 and [c['exit'] for c in cs]==[0]*20 and cs[-1]['name']=='race-harness-compile'
assert cs[-1]['exception']=="AssertionError('sampled source/cache/output limit')"
for c in cs:
 assert c['actual_wait'] and c['group_absent'] and c['adopted_actual_wait_complete'] and not c['timeout'] and not c['unexpected_group_survived_parent']
 assert c['exception'] is None or c is cs[-1]
 assert all(a['actual_waitpid'] for a in c['adopted_descendants'])
 assert sha(D/'fetched'/(c['name']+'.log'))==c['raw_sha256'] and sha(D/'fetched'/(c['name']+'.stderr.log'))==c['stderr_sha256']
controls={}
for mode,tag,count in [('normal','caching',6),('normal','harness',8),('race','caching',6)]:
 name=mode+'-'+tag+'-functional';es=[json.loads(x) for x in (D/'fetched'/(name+'.log')).read_text().splitlines()]
 tops=lambda a:[e['Test'] for e in es if e.get('Action')==a and e.get('Test') and '/' not in e['Test']]
 assert len(tops('run'))==count and set(tops('pass'))==set(tops('run')) and not tops('skip') and not tops('fail')
 assert [e['Action'] for e in es if not e.get('Test') and e.get('Action') in ['pass','fail']]==['pass']
 controls[name]=tops('pass')
assert rec['preflight_result'] is None and rec['database_result'] is None
assert 'race' not in rec['inventories'] and rec['inventories']['normal']['top_pass']==14
assert not (D/'fetched/normal-storage-preflight.log').exists()
audit=dict(at=datetime.datetime.now(datetime.timezone.utc).isoformat(),ROOT_audited=True,result='REJECT resource preparation; sampled 4GiB source/cache/output guard during race harness compilation; diagnostic unexecuted; user requested handoff, no rerun',outer_tool_session=4032,outer_tool_exit=1,fetch_tool_session=42265,fetch_tool_exit=0,remote_receipt_sha256=sha(D/'fetched/receipt.json'),evidence_sha256=sha(D/'ROOT-live-audit-evidence.json'),all_processes_joined=True,Go_phase_actual_wait_exit=[c['exit'] for c in cs],controls_passed=controls,normal_top_pass=14,race_caching_top_pass=6,race_harness_executed=False,tests_executed=True,diagnostic_executed=False,retained_failed_DBs=rec['remaining_fixture_DBs'],source_paths=9878,source_sha256=sha(D/'source.json'),reservation=ev['reservation'],finite=False,performance=False,N_accepted=False,server111_window_still_ROOT_owned=True)
save(D/'ROOT-RED-audit-v2.json',audit)
remote="from pathlib import Path\nimport datetime,hashlib,json,os\nR=Path('/home/mikers/dev/codex-r1-5090-functional-linux111-20261008')\nq=json.loads(__import__('sys').argv[1]);live=[]\nfor p in Path('/proc').iterdir():\n if not p.name.isdigit():continue\n try: exe=os.readlink(p/'exe');cmd=(p/'cmdline').read_bytes().replace(b'\\0',b' ').decode(errors='replace')\n except (FileNotFoundError,PermissionError,ProcessLookupError):continue\n base=Path(exe).name\n if base in ['go','compile','link','gofmt','test2json','unified-bench','collection_workload_bench'] or base.endswith('.test') or (str(R) in exe and base not in ['python3','python3.12','python3.13']):live.append(dict(pid=int(p.name),exe=exe))\nassert not live,live\nassert json.loads((R/'state/reservation.json').read_text())==q['reservation']\nrec=R/'S5098-preflight-v2/run-census-v3/receipt.json'\nassert hashlib.sha256(rec.read_bytes()).hexdigest()==q['remote_receipt_sha256']\nproof=dict(at=datetime.datetime.now(datetime.timezone.utc).isoformat(),ROOT_audit_sha256=q['ROOT_audit_sha256'],remote_receipt_sha256=q['remote_receipt_sha256'],all_remote_phases_joined=True,outer_SSH_and_tool_actual_join=True,fresh_conflicts=live,source_retained=True,failed_packet_retained=True,finite=False,performance=False,server111_runtime_released=True,reservation=q['reservation'])\np=R/'state/ROOT-S5098-census-v3-runtime-release.json';p.write_text(json.dumps(proof,indent=2,sort_keys=True)+'\\n')\nprint(json.dumps(dict(proof=proof,remote_proof_sha256=hashlib.sha256(p.read_bytes()).hexdigest())))\n"
(D/'ROOT-release-remote-v2.py').write_text(remote)
payload=dict(reservation=ev['reservation'],remote_receipt_sha256=audit['remote_receipt_sha256'],ROOT_audit_sha256=sha(D/'ROOT-RED-audit-v2.json'))
exception=None
with (D/'ROOT-release-v2.stdout.json').open('xb') as out,(D/'ROOT-release-v2.stderr.log').open('xb') as err:
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
 save(D/'ROOT-release-v2-outer.json',dict(pid=p.pid,exit=p.returncode,actual_wait=True,group_absent=absent,exception=exception,stdout_sha256=sha(D/'ROOT-release-v2.stdout.json'),stderr_sha256=sha(D/'ROOT-release-v2.stderr.log')))
 assert p.returncode==0 and absent and exception is None,(p.returncode,(D/'ROOT-release-v2.stderr.log').read_text())
proof=load(D/'ROOT-release-v2.stdout.json');assert proof['proof']['fresh_conflicts']==[] and proof['proof']['server111_runtime_released']
proof['release_outer_sha256']=sha(D/'ROOT-release-v2-outer.json')
save(D/'ROOT-runtime-release-v2.json',proof)
print('ROOT_AUDIT_FAILED_PRODUCT_AND_GENUINE_RUNTIME_RELEASE',sha(D/'ROOT-RED-audit-v2.json'),sha(D/'ROOT-runtime-release-v2.json'))
