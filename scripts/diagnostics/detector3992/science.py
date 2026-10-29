"""Reviewed v3 scientific arithmetic/schedule; hosted custody is separate."""
import importlib.util,json,pathlib,re,math
P=pathlib.Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location("maintained_gate",P/"frozen/mvcc_primary_cost_gate.py")
gate=importlib.util.module_from_spec(spec);spec.loader.exec_module(gate)
ARMS={'AA':(('A','raw'),('A','raw')),'CC':(('C','raw'),('C','raw')),'OVERHEAD':(('C','raw'),('I','off')),'POSITIVE':(('I','off'),('I','positive')),'AC':(('A','raw'),('C','raw'))}

def schedule():
 rows=[]; arms=list(ARMS)
 for pair in range(16):
  for arm in arms[pair%5:]+arms[:pair%5]:
   for side in ([0,1] if pair%2==0 else [1,0]):
    binary,mode=ARMS[arm][side]
    rows.append({'index':len(rows),'pair':pair,'block':pair//8,'arm':arm,'revision':['baseline','candidate'][side],'binary':binary,'mode':mode})
 return rows

def parse_raw(text,mode):
 metrics=gate.parse_sample(text)
 lines=[s.removeprefix('DETECTOR_RAW_SAMPLE ') for s in text.splitlines() if s.startswith('DETECTOR_RAW_SAMPLE ')]
 if len(lines)!=1:raise ValueError('one raw ledger required')
 r=json.loads(lines[0]); want=0 if mode=='raw' else 129
 if r['mode']!=mode or r['n']!=128 or r['activation_count']!=want:raise ValueError('raw identity/activation')
 if any(type(r[k]) is not int or r[k]<0 for k in ['t_ns','mem_bytes','mem_allocs']):raise ValueError('raw totals')
 if not 0<r['t_ns']<=90_000_000_000 or metrics['ns/op']!=r['t_ns']//128 or metrics['B/op']!=r['mem_bytes']//128 or metrics['allocs/op']!=r['mem_allocs']//128:raise ValueError('rounded/raw mismatch')
 if metrics['generation_advances/op']!=0:raise ValueError('both arms must remain PRIMARY-only')
 if mode=='raw':
  if r['calibration'] is not None or r['timed'] is not None:raise ValueError('raw hook unexpectedly active')
 else:
  if len(r['timed'])!=128 or not isinstance(r['calibration'],dict):raise ValueError('calibration/timed count')
  for x in [r['calibration']]+r['timed']:
   if set(x)!={'operation_ns','requested_ns','realized_ns'} or any(type(v)is not int for v in x.values()):raise ValueError('record shape')
   ns=x['operation_ns']; requested=(ns*8+99)//100
   if not 0<ns<=10_000_000_000:raise ValueError('operation bound')
   if mode=='positive':
    if x['requested_ns']!=requested or x['realized_ns']<requested:raise ValueError('deadline undershoot')
   elif x['requested_ns'] or x['realized_ns']:raise ValueError('disabled wait')
  work=sum(x['operation_ns'] for x in r['timed']);delay=sum(x['realized_ns'] for x in r['timed'])
  if work+delay>r['t_ns']:raise ValueError('timed operation+wait exceeds exported T')
  if mode=='positive':
   calibration=r['calibration']
   if not 7.5<=100*calibration['realized_ns']/calibration['operation_ns']<=9.0:raise ValueError('calibration realized range')
  r['measured_wait_over_operation_percent']=100*delay/work
  r['measured_wait_over_total_percent']=100*delay/r['t_ns']
  if not all(math.isfinite(r[k]) and 0<=r[k]<=100 for k in ['measured_wait_over_operation_percent','measured_wait_over_total_percent']):raise ValueError('impossible wait fraction')
  r['timed_operation_ns']=work;r['timed_wait_ns']=delay
  # Fixed range chosen before observations, never adjust toward detection.
  if mode=='positive' and not 7.5<=100*delay/work<=9.0:raise ValueError('realized control outside 7.5..9% operation range')
 return metrics,r

def analyze(rows):
 if [{k:r[k] for k in schedule()[0]} for r in rows]!=schedule():raise ValueError('exact full schedule metadata required')
 result={'diagnostic_only':True,'product_acceptance':False,'original_red_preserved':True,'blocks':{}}
 for arm in ARMS:
  result['blocks'][arm]=[]
  for block in range(2):
   selected=[dict(x,pair=x['pair']%8) for x in rows if x['arm']==arm and x['block']==block]
   result['blocks'][arm].append(gate.evaluate(selected))
 # Raw totals are descriptive, without truncation, subtraction or a new tolerance.
 result['raw_totals']=[{k:x[k] for k in ['index','arm','pair','revision','raw']} for x in rows]
 result['disabled_overhead_percent_by_block']=[x['median_paired_delta_percent'] for x in result['blocks']['OVERHEAD']]
 result['disabled_overhead_within_frozen_one_percent']=all(abs(x)<=1.0 for x in result['disabled_overhead_percent_by_block'])
 result['status']='DIAGNOSTIC_COMPLETE_REQUIRES_REVIEW' if result['disabled_overhead_within_frozen_one_percent'] else 'CALIBRATION_UNRESOLVED'
 result['calibration_valid']=result['disabled_overhead_within_frozen_one_percent']
 result['control_discrimination_resolved']=(all(x['measurement_pass'] for arm in ['AA','CC'] for x in result['blocks'][arm]) and all(not x['gates']['timing'] and x['gates']['bytes'] and x['gates']['allocations'] for x in result['blocks']['POSITIVE']))
 if result['calibration_valid'] and not result['control_discrimination_resolved']:result['status']='DETECTOR_RESOLUTION_UNRESOLVED'
 result['interpretation']='Unchanged eight-pair raw gates only; overhead >1% invalidates near-threshold calibration, without deleting any rows. No subtraction, historical attribution or merge qualification.'
 return result

def verify_test(text,name):
 if text.splitlines().count('PASS')!=1 or re.findall(r'(?m)^--- PASS: (\S+) \(',text)!=[name] or re.search(r'(?m)^(?:.*SKIP|FAIL|--- FAIL:|panic:|testing: warning: no tests to run)',text):raise ValueError('exact test success required: '+name)

def verify_discovery(text,names):
 lines=text.splitlines()
 if sorted(lines)!=sorted(names):raise ValueError('exact discovery required')
