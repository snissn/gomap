from pathlib import Path
import re,json
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');W=D/'native-integrated';O=D/'native-main-integration/native-registry-api-root-migration'
rows=[]
for p in list((W/'TreeDB/collections').glob('*.go'))+list((W/'TreeDB/cmd').glob('*/main.go'))+[W/'TreeDB/public.go',W/'TreeDB/caching/db.go',W/'TreeDB/caching/conditional_txn.go',W/'TreeDB/caching/point_successor.go',W/'TreeDB/internal/dictdb/resource_capture.go']:
 if p.name.endswith('_test.go'):continue
 s=p.read_text();before=s
 pat=re.compile(r'(?m)^(?P<indent>\t+)(?P<name>\w+) := (?P<call>[^\n]+\.Acquire(?:Stable)?Snapshot\(\))\n(?P=indent)if (?P=name) == nil \{\n(?P<body>.*?)(?P=indent)\}',re.S)
 def change(m):
  body=m['body'];name=m['name'];en=name+'Err'
  if re.search(r'\n'+re.escape(m['indent'])+r'\S',body):return m[0]
  if 'backenddb.ErrClosed' in body:body=body.replace('backenddb.ErrClosed',en)
  elif 'errCollectionDBNil' in body:body=body.replace('errCollectionDBNil',en)
  elif 'errStoreUnavailable' in body:body=body.replace('errStoreUnavailable',en)
  elif 'ErrClosed' in body:body=body.replace('ErrClosed',en)
  elif re.search(r'errors.New\("acquire snapshot: nil(?: snapshot)?"\)',body):body=re.sub(r'errors.New\("acquire snapshot: nil(?: snapshot)?"\)',en,body)
  else:return m[0]
  rows.append({'path':str(p.relative_to(W)),'line':before[:m.start()].count('\n')+1,'before':m[0],'after':f'{m["indent"]}{name}, {en} := {m["call"]}\n{m["indent"]}if {en} != nil {{\n{body}{m["indent"]}}}'})
  return rows[-1]['after']
 s=pat.sub(change,s)
 if s!=before:
  q=O/'before'/p.relative_to(W)
  if not q.exists():q.parent.mkdir(parents=True,exist_ok=True);q.write_text(before)
  p.write_text(s)
(O/'simple-migrations.json').write_text(json.dumps(rows,indent=2)+'\n');print('changed_calls',len(rows),'paths',len(set(r['path'] for r in rows)))
