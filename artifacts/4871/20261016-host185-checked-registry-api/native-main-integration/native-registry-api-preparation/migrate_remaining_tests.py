from pathlib import Path
import re
# Array/assignment and channel captures cannot use a multi-value expression.
for p in list(Path('TreeDB/db').glob('*_test.go'))+list(Path('TreeDB/lifecycle').glob('*_test.go')):
 s=p.read_text();lines=s.splitlines(True);out=[];n=0
 for l in lines:
  m=re.match(r'^(\s*)([\w]+(?:\[[^]]+\])?) = ((?:\w+\.)+Register\([^\n]+\))\s*$',l)
  if m:
   ind,var,call=m.groups();n+=1;e=f'registrationAssignErr{n}';out.append(f'{ind}var {e} error\n{ind}{var},{e} = {call}\n{ind}if {e}!=nil {{t.Errorf("registration: %v",{e});return}}\n');continue
  m=re.match(r'^(\s*)(\w+\[[^]]+\]) = ((?:\w+\.)+AcquireSnapshot\(\))\s*$',l)
  if m:
   ind,var,call=m.groups();n+=1;e=f'snapshotAssignErr{n}';out.append(f'{ind}var {e} error\n{ind}{var},{e} = {call}\n{ind}if {e}!=nil {{t.Fatal({e})}}\n');continue
  out.append(l)
 s=''.join(out)
 # Original nil-on-closed/poison/invalid assertions now require error as well.
 s=re.sub(r'if (\w+) := ((?:\w+\.)+AcquireSnapshot\(\)); \1 != nil \{',r'if \1, captureErr := \2; \1 != nil || captureErr == nil {',s)
 s=s.replace('len(r.seqs) != 0 || len(r.free) != 0','!r.root.empty()')
 s=s.replace('id, _ := reg.RegisterWithHint(newSeq, shard)','id, _,registerErr := reg.RegisterWithHint(newSeq, shard)\nif registerErr!=nil {t.Error(registerErr);return}')
 s=s.replace('handles = append(handles, r.Register(uint64(i+1)))','handle,err:=r.Register(uint64(i+1));if err!=nil{t.Fatal(err)}\nhandles = append(handles,handle)')
 s=s.replace('go func() { baseSnapshot <- database.startVacuumRecorderWithBaseSnapshot() }()','go func() {s,err:=database.startVacuumRecorderWithBaseSnapshot();if err!=nil{t.Error(err)};baseSnapshot <- s}()')
 s=re.sub(r'(\w+) <- (\w+)\.AcquireSnapshot\(\)',r's,err:=\2.AcquireSnapshot();if err!=nil{t.Error(err)};\1 <- s',s)
 if p.name=='primary_joint_vacuum_v6_test.go':s=s.replace('for _, snap := range []*Snapshot{held, d.AcquireSnapshot()} {','freshSnapshot,captureErr:=d.AcquireSnapshot();if captureErr!=nil{t.Fatal(captureErr)}\nfor _, snap := range []*Snapshot{held,freshSnapshot} {')
 if p.name=='native_data_dependency_release_work_v6_test.go':s=s.replace('lease := &dataDependencyLeaseV6{','registryID,registerErr:=idx.registry.Register(1);if registerErr!=nil{t.Fatal(registerErr)}\nlease := &dataDependencyLeaseV6{').replace('registryID: idx.registry.Register(1)','registryID: registryID')
 p.write_text(s)
