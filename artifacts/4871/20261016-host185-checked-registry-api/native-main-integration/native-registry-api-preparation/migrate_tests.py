from pathlib import Path
import re
for p in list(Path('TreeDB/db').glob('*_test.go'))+list(Path('TreeDB/lifecycle').glob('*_test.go')):
 s=p.read_text();lines=s.splitlines(True);out=[];arg='t';n=0
 for l in lines:
  m=re.search(r'func\b.*?\b(\w+)\s+\*testing\.(?:T|B|TB)',l)
  if m:arg=m[1]
  # Simple original declarations. Each unique error name preserves the original
  # variable scope and all existing assertions/workload statements.
  m=re.match(r'^(\s*)([\w]+) (:=|=) ((?:\w+\.)+(?:AcquireSnapshot|AcquireStableSnapshot|acquireStableSnapshotWithMaintenanceLockHeld|AcquireSnapshotForRoot)\([^\n]*\))\s*$',l)
  if m:
   indent,var,op,call=m.groups();n+=1;e=f'snapshotCaptureErr{n}'
   if op=='=':out.append(f'{indent}var {e} error\n')
   out.append(f'{indent}{var}, {e} {op} {call}\n{indent}if {e} != nil {{ {arg}.Fatalf("snapshot acquisition: %v", {e}) }}\n');continue
  # Registry declarations are similarly checked; native fast-only methods stay.
  m=re.match(r'^(\s*)(\w+) := ((?:\w+\.)+Register\([^\n]*\))\s*(//.*)?$',l)
  if m:
   indent,var,call,comment=m.groups();n+=1;e=f'registerErr{n}';out.append(f'{indent}{var}, {e} := {call}\n{indent}if {e} != nil {{ {arg}.Fatalf("reader registration: %v", {e}) }}\n');continue
  out.append(l)
 text=''.join(out)
 if text!=s:p.write_text(text)
