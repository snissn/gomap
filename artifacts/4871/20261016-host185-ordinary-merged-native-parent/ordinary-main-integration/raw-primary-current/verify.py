from pathlib import Path
import json,hashlib,subprocess,re,statistics,math,collections
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');A=D/'ordinary-main-integration/raw-primary-current';P=A/'packet';W=D/'ordinary-repair'
base='637129789378c33027a234997b6cdd57f20fd0fe';head='6d3fc3a6e3582810d7c80fc46b784a9b30fada80'
def h(p):
 d=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(1<<20),b''):d.update(b)
 return d.hexdigest()
def j(p):return json.loads(p.read_text())
def eq(a,b):
 if isinstance(a,float):assert math.isclose(a,b,rel_tol=1e-14,abs_tol=1e-14),(a,b)
 else:assert a==b,(a,b)
r=j(A/'download-receipt.json');api=j(A/'artifact-api.json');assert h(A/'artifact.zip')==r['zip_sha256']==api['digest'].split(':')[1];assert (A/'artifact.zip').stat().st_size==api['size_in_bytes']
assert set(r['files'])=={str(p.relative_to(P)) for p in P.rglob('*') if p.is_file()}
for n,v in r['files'].items():assert h(P/n)==v['sha256'] and (P/n).stat().st_size==v['size'],n
s=j(P/'summary.json');assert s['baseline_sha']==base and s['candidate_sha']==head
assert (s['expected_samples'],s['max_regression_percent'],s['max_bytes_regression_percent'],s['max_bytes_regression_absolute'])==(8,5,1,64)
names=[x['benchmark'] for x in s['results']];raw={}
for rev in ['baseline','candidate']:
 t=(P/(rev+'.txt')).read_text();assert not re.search(r'(?m)^(FAIL|--- FAIL:|panic:)',t);assert t.splitlines().count('PASS')==40
 rows=collections.defaultdict(list)
 for line in t.splitlines():
  if not line.startswith('Benchmark'):continue
  tok=line.split();name=re.sub(r'-\d+$','',tok[0]);assert name in names;assert len(tok[2:])%2==0
  units=dict(zip(tok[3::2],map(float,tok[2::2])));assert all(math.isfinite(x) and x>=0 for x in units.values())
  rows[name].append({'ns_per_op':units['ns/op'],'bytes_per_op':units['B/op'],'allocs_per_op':units['allocs/op']})
 assert all(len(v)==8 for v in rows.values()) and set(rows)==set(names);raw[rev]=rows
for row in s['results']:
 n=row['benchmark'];med={rev:{k:statistics.median(v[k] for v in raw[rev][n]) for k in row[rev]} for rev in raw}
 eq(med['baseline'],row['baseline']);eq(med['candidate'],row['candidate']);b,c=med['baseline'],med['candidate']
 paired=statistics.median((v['ns_per_op']/u['ns_per_op']-1)*100 for u,v in zip(raw['baseline'][n],raw['candidate'][n]))
 eq(paired,row['paired_ns_delta_percent']);eq((c['ns_per_op']/b['ns_per_op']-1)*100,row['ns_delta_percent'])
 tol=min(b['bytes_per_op']*.01,64);eq(tol,row['bytes_tolerance']);eq(c['bytes_per_op']-b['bytes_per_op'],row['bytes_delta'])
 assert paired<=5 and c['allocs_per_op']<=b['allocs_per_op'] and c['bytes_per_op']-b['bytes_per_op']<=tol
 assert all(row[k] is True for k in ['measurement_pass','acceptance_pass','timing_pass','bytes_pass','allocs_pass'])
 assert row['binary_equivalent'] is False and row['attribution']=='CANDIDATE'
m=j(P/'measured-binaries.json');assert len(m['files'])==6
for field,n in [('environment_sha256','environment.txt'),('initial_digests_sha256','binary-sha256.txt'),('checker_summary_sha256','summary.json')]:assert m[field]==h(P/n)
initial={n:dig for dig,n in (l.split() for l in (P/'binary-sha256.txt').read_text().splitlines())}
for v in m['files']:
 p=P/v['path'];rev,pkg=p.name.removesuffix('.test').split('-');sha={'baseline':base,'candidate':head}[rev]
 assert h(p)==v['sha256']==s['binary_digests'][pkg][rev]==initial[p.name];assert p.stat().st_size==v['bytes'];assert p.read_bytes()[:4]==b'\x7fELF'
 assert v['source_sha']==sha;assert v['source_tree']==subprocess.check_output(['git','rev-parse',sha+'^{tree}'],cwd=W,text=True).strip()
 assert v['build_environment']=={'GOWORK':'off'}
# Raw collection's ps snapshots establish schedule only; they are not an interval CPU quiet certificate.
markers=re.findall(r'(?m)^--- before revision=(\w+) sample=(\d+) group=(\w+) at .+ ---$',(P/'processes.txt').read_text())
expected=[(r,str(i),g) for i in range(1,9) for g in ['get_versioned','batch_write','snapshot_seek','repeated_iterator','durable_sync'] for r in (['baseline','candidate'] if i%2 else ['candidate','baseline'])];assert markers==expected
Q=P/'primary';bind=j(Q/'bindings.json');assert bind['baseline']==base and bind['candidate']==head;assert bind['samples']==8 and bind['iterations']==128 and bind['cpu']==0 and bind['gomaxprocs']==1 and bind['max_paired_regression_percent']==5
for key,path in [('harness_sha256','TreeDB/mvcc/primary_generation_routing_v6_test.go'),('runner_sha256','scripts/mvcc_primary_cost_gate.py')]:
 b=subprocess.check_output(['git','show',head+':'+path],cwd=W);assert hashlib.sha256(b).hexdigest()==bind[key]
assert h(Q/'primary_generation_routing_v6_test.go')==bind['harness_sha256']
for rev,v in bind['binaries'].items():assert h(Q/v['path'])==v['sha256'] and (Q/v['path']).stat().st_size==v['bytes'] and v['source_sha']=={'baseline':base,'candidate':head}[rev]
commands=j(Q/'commands.json');assert len(commands)==28
for c in commands:assert c['exit']==0 and h(Q/c['output'])==c['output_sha256']
paircmd=[c for c in commands if c['label'].startswith('pair-')];expected=[(i,r) for i in range(1,9) for r in (['baseline','candidate'] if i%2 else ['candidate','baseline'])];assert [c['label'] for c in paircmd]==[f'pair-{i}-{r}' for i,r in expected]
rows=[]
for (i,rev),c in zip(expected,paircmd):
 assert c['argv']==['taskset','-c','0','/home/runner/work/_temp/mvcc-raw-path-gate/primary/'+rev+'-primary.test','-test.run=^$','-test.bench=^BenchmarkCommitAtPrimaryOnlyAfterDataGeneration$','-test.benchtime=128x','-test.count=1','-test.benchmem','-test.timeout=90s']
 t=(Q/c['output']).read_text();ls=[x for x in t.splitlines() if x.startswith('Benchmark')];assert len(ls)==1 and t.splitlines().count('PASS')==1 and 'FAIL' not in t;tok=ls[0].split();assert int(tok[1])==128
 metrics=dict(zip(tok[3::2],map(float,tok[2::2])));assert set(metrics)=={'ns/op','B/op','allocs/op','generation_advances/op'}
 rows.append({'pair':i-1,'revision':rev,'metrics':metrics})
assert rows==j(Q/'samples.json');ps=j(Q/'summary.json');by={rev:[r['metrics'] for r in rows if r['revision']==rev] for rev in ['baseline','candidate']}
med={rev:{k:statistics.median(v[k] for v in by[rev]) for k in by[rev][0]} for rev in by};assert med==ps['medians']
pairs=[(c['ns/op']/b['ns/op']-1)*100 for b,c in zip(by['baseline'],by['candidate'])];eq(pairs,ps['paired_deltas_percent']);eq(statistics.median(pairs),ps['median_paired_delta_percent']);assert ps['accepted'] and all(ps['gates'].values()) and ps['quiet'];assert all(x['generation_advances/op']==0 for x in by['candidate'])
assert med['candidate']['B/op']<=med['baseline']['B/op'] and med['candidate']['allocs/op']<=med['baseline']['allocs/op'] and statistics.median(pairs)<=5
# Rebuild all sampled external-process totals. The collector PID is bound by the retained ps command row.
process_text=(Q/'processes.txt').read_text();assert re.search(r'(?m)^\s*9013\s+9012\s+.*python3 scripts/mvcc_primary_cost_gate.py ',process_text)
quiet={}
for stem,summ in [('preflight-processes','preflight'),('measured-processes','measured-processes')]:
 snapshots=[json.loads(l) for l in (Q/(stem+'.jsonl')).read_text().splitlines()];q=j(Q/(summ+'.json'));previous={};totals={}
 for index,snap in enumerate(snapshots):
  table={int(k):v for k,v in snap['processes'].items()};allowed={9013};p=9013
  while p in table and table[p]['parent']>0:p=table[p]['parent'];allowed.add(p)
  descendants={9013}
  while True:
   added={pid for pid,v in table.items() if v['parent'] in descendants}
   if added<=descendants:break
   descendants|=added
  allowed|=descendants
  for pid,v in table.items():
   key=(pid,v['start']);old=previous.get(key,v['ticks'] if index==0 else 0)
   if pid not in allowed:
    t=totals.setdefault(key,{'pid':pid,'start':v['start'],'command':v['command'],'ticks':0});t['ticks']+=max(0,v['ticks']-old)
   previous[key]=v['ticks']
 qr={(v['pid'],v['start']):v for v in q['external_processes']};assert qr.keys()==totals.keys()
 for key,v in totals.items():
  for f in v:assert v[f]==qr[key][f]
  eq(100*v['ticks']/100/q['elapsed_s'],qr[key]['cpu_percent'])
 assert q['busy']==[] and q['cpu_threshold_percent']==2 and all(v['cpu_percent']<2 for v in qr.values())
 quiet[stem]={'snapshots':len(snapshots),'external_processes':len(qr),'elapsed_s':q['elapsed_s'],'maximum_external_cpu_percent':max(v['cpu_percent'] for v in qr.values())}
report={'accepted':True,'base':base,'head':head,'artifact_id':r['artifact_id'],'all_packet_files_verified':len(r['files']),'raw_samples':80,'raw_results':s['results'],'raw_schedule_verified':len(markers),'primary_samples':16,'primary_commands_hash_verified':len(commands),'primary_summary':ps,'primary_quiet_recomputed':quiet,'actual_elfs_hash_verified':8,'limits':['Raw ps snapshots are not interval CPU quiet certification.','PRIMARY CPU quiet uses whole-window per-process 2% threshold and may miss processes entirely between snapshots; no I/O quiet claim.','RepeatedIterator paired delta +4.978262666336053% is close to the unchanged +5% limit.','PRIMARY pair6 delta +22.519996730946822% remains in the 8-pair result.','Recorded Go1.26.9 and source/ELF bindings are not complete compiler closure.','No inference about packing causality, general throughput, native progress, or final graph qualification.']}
(A/'verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({k:v for k,v in report.items() if k not in ['raw_results','primary_summary']},indent=2))
