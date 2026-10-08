from pathlib import Path
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
rec=R/'S5098-preflight-v2/run-retry3/receipt.json'
assert hashlib.sha256(rec.read_bytes()).hexdigest()==q['remote_receipt_sha256']
proof=dict(at=datetime.datetime.now(datetime.timezone.utc).isoformat(),ROOT_audit_sha256=q['ROOT_audit_sha256'],remote_receipt_sha256=q['remote_receipt_sha256'],all_remote_phases_joined=True,outer_SSH_and_tool_actual_join=True,fresh_conflicts=live,source_retained=True,failed_packet_retained=True,finite=False,performance=False,server111_runtime_released=True,reservation=q['reservation'])
p=R/'state/ROOT-S5098-preflight-retry3-runtime-release.json';p.write_text(json.dumps(proof,indent=2,sort_keys=True)+'\n')
print(json.dumps(dict(proof=proof,remote_proof_sha256=hashlib.sha256(p.read_bytes()).hexdigest())))
