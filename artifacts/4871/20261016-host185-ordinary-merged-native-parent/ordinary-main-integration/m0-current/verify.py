import pathlib,json,hashlib,statistics,re,math
D=pathlib.Path(__file__).parent;P=D/'packet';x=json.loads((P/'results.json').read_text());receipt=json.loads((D/'download-receipt.json').read_text());head='6d3fc3a6e3582810d7c80fc46b784a9b30fada80'
for p,v in receipt['files'].items():
 b=(P/p).read_bytes();assert len(b)==v['size'] and hashlib.sha256(b).hexdigest()==v['sha256']
r=json.loads((D/'run.json').read_text());assert r['head_sha']==head and r['run_attempt']==1 and r['conclusion']=='success'
assert x['environment']['git_sha']==head and x['environment']['dirty_state']=='clean' and x['environment']['repetitions']==10
assert (P/'commands.txt').read_text().splitlines()==x['environment']['commands']
for arm in ['legacy','public']:
 rows=[]
 for p in sorted((P/'raw').glob(arm+'-*.txt')):
  s=p.read_text();assert s.count('\nPASS\n')==1 and 'FAIL' not in s
  ls=[l for l in s.splitlines() if l.startswith('Benchmark')];assert len(ls)==1
  f=ls[0].split();assert f[1]=='1' and len(f[2:])%2==0
  rows.append(dict(zip(f[3::2],map(float,f[2::2]))))
 assert len(rows)==10
 for metric,v in x['metrics'][arm].items():
  vs=[a[metric] for a in rows];mean=statistics.fmean(vs);cv=statistics.stdev(vs)/mean if mean else 0.0
  assert vs==v['samples'] and mean==v['mean'] and statistics.median(vs)==v['median'] and math.isclose(cv,v['cv'],rel_tol=1e-14,abs_tol=1e-15),(arm,metric)
 assert set(rows[0])==set(x['metrics'][arm])
L=x['metrics']['legacy'];Q=x['metrics']['public'];names=['vacuum-total-ns/op','max-writer-pause-ns','foreground-p99-ns/op'];assert all(L[n]['cv']<=.10 for n in names) and all(v==0 for v in L['concurrent-aborts/op']['samples'])
assert all(v==0 for n in ['vacuum-unsupported/op','vacuum-unexpected-errors/op','foreground-exposure-misses/op'] for v in Q[n]['samples']) and all(v>0 for v in Q['foreground-overlap-samples/op']['samples']) and set(Q['vacuum-concurrent-retries/op']['samples'])<=set([0,1]) and 0 in Q['vacuum-concurrent-retries/op']['samples']
assert x['public_status']=='production-index-vacuum-available' and all(x['gates'].values())
log=(D/'run.log').read_text();assert head in log and 'go1.26.9' in log
out={'head':head,'run_id':r['id'],'attempt':1,'all_raw_metric_summaries_recomputed':True,'cv_float_tolerance':'rel1e-14/abs1e-15 for one-ULP Python stdlib differences; samples/means/medians exact','raw_files':20,'cpu':'AMD EPYC 7763','go':'go1.26.9','gates':x['gates'],'CV_percent':{n:100*L[n]['cv'] for n in names},'public_success_samples':Q['vacuum-concurrent-retries/op']['samples'].count(0),'public_typed_retry_samples':Q['vacuum-concurrent-retries/op']['samples'].count(1),'minimum_public_foreground_overlap':min(Q['foreground-overlap-samples/op']['samples']),'limits':['Original current-head M0 stability/classification evidence only; no packing causal improvement.','Fixture metadata nested status remains initial offline/probe classification; final result uses actual public rows.','Hosted original workflow uses module-selected Go1.26.9; separate maintenance evidence remains pinned Go1.26.3.','No downloaded Go code executed for verification.'],'sha256':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in [D/'artifact.zip',D/'run.log',P/'results.json']}}
(D/'verification.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(out,indent=2))
