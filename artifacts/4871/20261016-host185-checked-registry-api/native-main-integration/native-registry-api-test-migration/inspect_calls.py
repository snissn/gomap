from pathlib import Path
import re,json
W=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015/native-integrated')
A=W.parent/'native-main-integration/native-registry-api-test-migration'
pat=re.compile(r'//[^\n]*|/\*[\s\S]*?\*/|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'|`[^`]*`|[A-Za-z_]\w*|:=|\S')
def scopes(s):
 ts=[m for m in pat.finditer(s) if not m[0].startswith(('//','/*'))]; stack=[]; pairs={}
 for i,t in enumerate(ts):
  if t[0] in '({[':stack.append(i)
  elif t[0] in ')}]':
   if stack:pairs[stack.pop()]=i
 out=[]
 for i,t in enumerate(ts):
  if t[0]!='func':continue
  j=i+1
  while j<len(ts):
   if ts[j][0]=='{':break
   if ts[j][0] in ('(','['): j=pairs.get(j,j)
   j+=1
  if j>=len(ts) or j not in pairs:continue
  out.append((t.start(),ts[j].end(),ts[pairs[j]].end(),s[t.start():ts[j].start()].strip()))
 return out
rows=[]
for p in W.rglob('*_test.go'):
 n=str(p.relative_to(W))
 if n.startswith(('TreeDB/db/','TreeDB/lifecycle/')):continue
 s=p.read_text(); fs=scopes(s)
 for m in re.finditer(r'\.Acquire(?:Snapshot|BackendSnapshotFastPath)\(',s):
  ancestors=[f for f in fs if f[1]<=m.start()<f[2]]
  inner=ancestors[-1] if ancestors else None
  line=s.count('\n',0,m.start())+1
  rows.append({'path':n,'line':line,'text':s.splitlines()[line-1], 'scopes':[f[3] for f in ancestors], 'start':m.start()})
(A/'call-context-before.json').write_text(json.dumps(rows,indent=2)+'\n')
for r in rows:
 if not r['scopes'] or not re.search(r'\b\w+\s+\*testing\.(T|B|TB)\b',r['scopes'][-1]):
  print(r['path']+':'+str(r['line'])+':'+str(r['scopes'])+':'+r['text'].strip())
