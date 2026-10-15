import pathlib,json,subprocess,re,concurrent.futures,sys
D=pathlib.Path('/mnt/fast4tb/gomap-mvcc-4871-20261015/live-startup');O=D/'616fd-ci'
j=json.load(open(D/('pr5127-616fd-checks-'+sys.argv[1]+'.json')))
def one(x):
 name=x['name'];slug=re.sub(r'[^\w.-]+','_',name).strip('_');p=O/(slug+'.log');jid=x['link'].split('/')[-1]
 if not p.exists():
  with p.open('w') as f:subprocess.run(['gh','api','repos/snissn/gomap/actions/jobs/'+jid+'/logs'],stdout=f,stderr=subprocess.PIPE,check=True)
 failures=[];interesting=[];race=0;events=[];tests=set()
 for i,line in enumerate(p.read_text().splitlines(),1):
  m=re.search(r'(?:--- FAIL:|^.*?\t?FAIL\s+)\s*(\S+)',line)
  if m and m.group(1) not in failures:failures.append(m.group(1))
  if re.search(r'WARNING: DATA RACE|panic:|fatal error:|timed out|\.go:\d+:',line):interesting.append({'line':i,'text':line.rstrip()[:2500]})
  if 'WARNING: DATA RACE' in line:race+=1
  pos=line.find('{"Time"')
  if pos<0:continue
  try:e=json.loads(line[pos:])
  except json.JSONDecodeError:continue
  events.append(e)
  if e.get('Action')=='fail' and e.get('Test'):tests.add(e['Test'])
 out={t:''.join(e.get('Output','') for e in events if e.get('Test')==t) for t in sorted(tests)}
 (O/(slug+'-failed-output.json')).write_text(json.dumps(out,indent=2)+'\n')
 a={'job':name,'job_id':jid,'failures':failures,'race_reports':race,'interesting':interesting,'log':str(p)}
 (O/(slug+'-failures.json')).write_text(json.dumps(a,indent=2)+'\n');return a
xs=[x for x in j if x.get('state')=='FAILURE' and x['name']!='stable legacy/public capture']
with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:out=list(ex.map(one,xs))
(O/('failure-inventory-'+sys.argv[1]+'.json')).write_text(json.dumps(out,indent=2)+'\n')
for x in out:print(json.dumps({k:v for k,v in x.items() if k!='interesting'}),flush=True)
