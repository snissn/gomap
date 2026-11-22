"""Failure-only diagnostics; leaves the tested production source tree unchanged."""
from pathlib import Path
import json
import re
import sys

root=Path(sys.argv[1]).resolve()
stage=Path(sys.argv[2]).resolve()
stage.mkdir()
overlay={}
transforms={
  'memtable': (root/'TreeDB/internal/memtable', 'cow_budget.go', 'ErrCOWCapacity'),
  'caching': (root/'TreeDB/caching', 'cow_accepted_prefix.go', 'memtable.ErrCOWCapacity'),
}
for name,(dir,anchor,symbol) in transforms.items():
 helper='diagnoseCOWRefusal'
 for path in sorted(dir.glob('cow*.go')):
  if path.name.endswith('_test.go'):
   continue
  s=path.read_text()
  lines=s.splitlines(keepends=True)
  for i,line in enumerate(lines,1):
   pat=r'^(\s*return\b[^\n]*)\b'+re.escape(symbol)+r'(?=[,\s]*\n$)'
   lines[i-1]=re.sub(pat,lambda m:m[1]+f'{helper}("{path.name}:{i}", {symbol})',line)
  t=''.join(lines)
  if path.name==anchor:
   t=t.replace(f'package {name}\n', f'package {name}\n\nimport (\n traceFmt "fmt"\n traceOS "os"\n traceDebug "runtime/debug"\n)\n', 1)+f'''
// Diagnostic overlay only. Return the original sentinel unchanged.
func {helper}(site string, err error) error {{
 traceFmt.Fprintf(traceOS.Stderr, "CAPACITY_SITE {name}/%s %v\\n%s\\n", site, err, traceDebug.Stack())
 return err
}}
'''
   if name=='memtable':
    for condition in [
     'if (nativeSource && b.stats.Sources >= b.limits.MaxSources) || b.stats.ExternalLeases == int(^uint(0)>>1) || !b.inFlightFitsLocked(charge) ||\n\t\t!b.retirementFitsLocked(charge) || !b.addLocked(charge) {',
     'if !b.inFlightFitsLocked(delta) ||\n\t\t\t!b.retirementFitsLocked(delta) || !b.addLocked(delta) {',
    ]:
     amount='delta' if 'delta' in condition else 'charge'
     assert condition in t
     t=t.replace(condition,condition+f'\ntraceFmt.Fprintf(traceOS.Stderr, "MEMTABLE_BUDGET refused %d stats=%+v limits=%+v\\n", {amount}, b.stats, b.limits)',1)
  if s!=t:
   target=stage/f'{name}-{path.name}'
   target.write_text(t)
   overlay[str(path)]=str(target)

expected = root/'TreeDB/internal/rootpublication/accepted_prefix.go'
s = expected.read_text()
t=s.replace('package rootpublication\n','package rootpublication\n\nimport (\n traceFmt "fmt"\n traceOS "os"\n traceDebug "runtime/debug"\n)\n',1)
for expr,role in [('if p.maintenanceUsed {','maintenance'),('if p.freeCount == 0 {','ordinary')]:
 assert expr+'\n\t\t\treturn AcceptedTicket{}, ErrAcceptedCapacity' in t
 t=t.replace(expr+'\n\t\t\treturn AcceptedTicket{}, ErrAcceptedCapacity',expr+f'''
traceFmt.Fprintf(traceOS.Stderr, "PREFIX_RESERVE_REFUSAL kind={role} free=%d slots=%d maintenance=%t frontiers=%+v\\n%s\\n", p.freeCount, len(p.entries)-1, p.maintenanceUsed, p.frontiers, traceDebug.Stack())
return AcceptedTicket{{}}, ErrAcceptedCapacity''',1)
target=stage/'rootpublication-accepted_prefix.go'
target.write_text(t); overlay[str(expected)]=str(target)

target=stage/'overlay.json'
target.write_text(json.dumps({"Replace":overlay},indent=2)+'\n')
print(target,len(overlay))
