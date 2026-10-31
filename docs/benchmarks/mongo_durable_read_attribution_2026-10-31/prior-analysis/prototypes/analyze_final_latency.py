"""Source-bound arithmetic prototype, not a causal performance or throughput model."""
from pathlib import Path
import csv,hashlib,io,json,math,statistics
ROOT=Path(__file__).resolve().parent.parent
REPORT=ROOT/'context/report.json'
doc=json.loads(REPORT.read_text())
assert len(doc['samples'])==12
rows=[]
for s in doc['samples']:
    row={'name':s['name'],'class':s['class_name'],'arm':s['arm'],'pair':s['pair']}
    phase=s['phases']['run_1'];op={x['operation']:x for x in phase['rows']}
    assert phase['operation_errors']==0
    assert op['READ']['count']+op['UPDATE']['count']==op['TOTAL']['count']==10000
    assert all(math.isfinite(op[o][k]) and op[o][k]>=0 for o in ['READ','UPDATE'] for k in ['avg_us','p50_us','p99_us'])
    for o in ['READ','UPDATE']:
        for k in ['count','avg_us','p50_us','p99_us']:row[o.lower()+'_'+k]=op[o][k]
    total=sum(op[o]['count']*op[o]['avg_us'] for o in ['READ','UPDATE'])
    row['read_fraction_of_summed_latency']=op['READ']['count']*op['READ']['avg_us']/total
    row['update_fraction_of_summed_latency']=op['UPDATE']['count']*op['UPDATE']['avg_us']/total
    row['run_ops_sec']=op['TOTAL']['ops_per_sec'];row['workload_seconds']=phase['workload_seconds']
    row['mean_concurrent_timed_requests_estimate']=total/1e6/phase['workload_seconds']
    # Original means are printed integer microseconds: <=0.5us/operation rounding.
    row['latency_sum_rounding_bound_seconds']=op['TOTAL']['count']*0.5/1e6
    row['load_ops_sec']=next(x['ops_per_sec'] for x in s['phases']['load']['rows'] if x['operation']=='TOTAL')
    rows.append(row)
rows.sort(key=lambda r:(r['class'],r['pair'],r['arm']))
summary={}
for cls in ['relaxed','durable-B']:
    summary[cls]={}
    for arm in ['gomap','mongodb']:
        armrows=[r for r in rows if r['class']==cls and r['arm']==arm]
        summary[cls][arm]={k:statistics.median(r[k] for r in armrows) for k in ['read_avg_us','update_avg_us','read_fraction_of_summed_latency','run_ops_sec','load_ops_sec','mean_concurrent_timed_requests_estimate']}
result={'input_sha256':hashlib.sha256(REPORT.read_bytes()).hexdigest(),'measurement_source':'8bea490ad5a2d89a14f02f428b4283579a6dd864','publication_source':'95c6ff3de473786bd98b692375cfdb9cef951668','samples':rows,'summary':summary,'interpretation':'Count-weighted printed per-operation latency sums only. Concurrent waits overlap, workload timer and client throughput estimator are distinct, and these are independent random-mixture fixtures. No causal lock diagnosis, CPU attribution, additive wall-time fraction, projected speedup, or statistical significance is inferred.'}
(ROOT/'evidence/latency-attribution.json').write_text(json.dumps(result,indent=2)+'\n')
with (ROOT/'evidence/latency-attribution.csv').open('w') as f:
    writer=csv.DictWriter(f,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows)
print(json.dumps(summary,indent=2))
