from pathlib import Path
import base64,hashlib,json,os,sys,datetime
D=Path(sys.argv[1]);W=Path(sys.argv[2]);R=Path('/home/mikers/dev/codex-r1-5090-functional-linux111-20261008')
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
source=json.loads((D/'source.json').read_text())
assert {str(p.relative_to(W)):sha(p) for p in W.rglob('*') if p.is_file()}==source
assert not any(p.is_symlink() for p in W.rglob('*'))
auth=json.loads((D/'authorization.json').read_text());assert all(sha(p)==h for p,h in auth['immutable_bindings'].items())
tools=json.loads((D/'actual-tools.json').read_text());assert all(sha(p)==h for p,h in tools['files'].items())
assert all(str(Path(p).resolve())==q for p,q in tools['symlinks'].items())
G=R/'toolchain/go1.26.3';pinned=json.loads((D/'pinned-toolchain-source.json').read_text())
assert {str(p.relative_to(G)):sha(p) for p in G.rglob('*') if p.is_file()}==pinned
counts={}
for p in D.glob('*inputs.json'):
 data=json.loads(p.read_text());assert all(sha(k)==h for k,h in data.items());counts[p.name]=len(data)
rec=json.loads((D/'receipt.json').read_text());custody=json.loads((D/'custody.json').read_text())
assert rec['all_processes_joined'] and rec['active'] is None and custody['active'] is None and custody['all_processes_joined']
assert all(sha(p)==h for p,h in rec['preserved_binaries'].items())
for c in rec['commands']:
 assert c['actual_wait'] and c['group_absent'] and c['adopted_actual_wait_complete']
 assert all(a['actual_waitpid'] for a in c['adopted_descendants'])
 assert sha(D/(c['name']+'.log'))==c['raw_sha256'] and sha(D/(c['name']+'.stderr.log'))==c['stderr_sha256'] and c['separate_stdout_stderr']
live=[]
for p in Path('/proc').iterdir():
 if not p.name.isdigit():continue
 try:exe=os.readlink(p/'exe');cmd=(p/'cmdline').read_bytes().replace(b'\0',b' ').decode(errors='replace')
 except (FileNotFoundError,PermissionError,ProcessLookupError):continue
 if exe.startswith(str(D)) or (str(D/'driver.py') in cmd and int(p.name)!=os.getpid()):live.append(dict(pid=int(p.name),exe=exe))
assert not live,live
special={}
for x in json.loads((D/'run-spec.json').read_text())['special_test_inventory']:
 p=Path(x['source']);assert sha(p)==x['sha256'];special[str(p)]=dict(sha256=sha(p),text=p.read_text())
files={p.name:dict(sha256=sha(p),data=base64.b64encode(p.read_bytes()).decode()) for p in D.iterdir() if p.is_file() and p.suffix in ['.json','.log','.py']}
print(json.dumps(dict(at=datetime.datetime.now(datetime.timezone.utc).isoformat(),files=files,source_exact=True,source_paths=len(source),tools_inputs_binaries_exact=True,pinned_toolchain_exact=True,input_counts=counts,special_sources=special,reservation=json.loads((R/'state/reservation.json').read_text()),reservation_sha256=sha(R/'state/reservation.json'),no_live_owned_driver_or_ELF=True)))
