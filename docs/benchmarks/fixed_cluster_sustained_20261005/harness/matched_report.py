"""Matched descriptive report construction; never grants artifact acceptance."""
if not __debug__: raise RuntimeError('ordinary Python required; assertions must run')
import sys
sys.dont_write_bytecode=True
from source_paths import isolate_paths, workload_source
import argparse,hashlib,json,math,statistics
from pathlib import Path
def need(ok,label):
 if not ok:raise ValueError(label)
def sha(raw):return hashlib.sha256(raw).hexdigest()
def strict(raw):
 def pairs(rows):
  out={}
  for key,value in rows:need(key not in out,'duplicate JSON key');out[key]=value
  return out
 return json.loads(raw,object_pairs_hook=pairs,parse_constant=lambda value:(_ for _ in ()).throw(ValueError(value)))
def read_ref(row):
 need(isinstance(row,dict) and set(row)=={'path','sha256'},'exact immutable raw reference')
 path=Path(row['path']);need(path.is_absolute() and path.is_file() and not path.is_symlink() and path.stat().st_size<=134217728,'bounded regular absolute raw input')
 raw=path.read_bytes();need(sha(raw)==row['sha256'],'actual raw reference hash');return raw
def scalar(value):
 need(type(value) in (int,float) and math.isfinite(value) and value>=0,'finite nonnegative measured scalar');return value
def describe(values):
 return None if not values else dict(median=statistics.median(values),minimum=min(values),maximum=max(values))
def build(manifest):
 need(set(manifest)=={'Version','windows'} and manifest['Version']==1 and isinstance(manifest['windows'],list) and len(manifest['windows'])==6,'six predeclared windows; preserve failed/incomplete entries')
 windows=[];metrics={c:[] for c in (1,4)}
 for i,(row,concurrency) in enumerate(zip(manifest['windows'],workload_source.MATCHED_ORDER),1):
  campaign='rf4matched5068w%02dc%d'%(i,concurrency)
  need(set(row)=={'campaign','status','pins'} and row['campaign']==campaign and row['status'] in ('complete','failed','incomplete'),'exact campaign order and retained outcome')
  pins=row['pins'];need(isinstance(pins,dict),'retained raw refs')
  raw={key:read_ref(value) for key,value in pins.items()}
  window=dict(campaign=campaign,concurrency=concurrency,status=row['status'],raw_refs=pins,metrics=None)
  if row['status']=='complete':
   need(set(pins)=={'stdout','read_audit','resources','artifact_review','costs'},'complete descriptive inputs and separate artifact review reference')
   events=[strict(line) for line in raw['stdout'].splitlines()]
   need(len(events)==2 and [e['Event'] for e in events]==['planned','result'],'one-shot actual event pair')
   p,r=events[0]['Report'],events[1]['Report'];profile=workload_source.Workload(campaign,6,60,5,concurrency)
   for report in (p,r):
    need(report['Admission']['RunID']==profile.query_run and report['Originals']==6 and report['Concurrency']==concurrency and report['RequestedDuration']==60000000000 and report['PaceInterval']==5000000000,'exact matched raw report profile')
   audit=strict(raw['read_audit']);resource=strict(raw['resources']);costs=strict(raw['costs'])
   # Compare exact report substrings, rather than re-encoding parsed numbers.
   result_line=raw['stdout'].splitlines()[1];marker=b'"Report":'
   need(result_line.count(marker)==1 and result_line.endswith(b'}'),'retained event report envelope')
   result_raw=result_line.split(marker,1)[1][:-1]
   need(audit['verdict']=='READ_ACCOUNTING_GATE_PASS_ONLY' and audit['campaign_acceptance'] is False and audit['raw_result_report_sha256']==sha(result_raw),'actual read-accounting proof/result join')
   need(resource['disposition']=='RESOURCE_GATE_OWNERSHIP_ACCOUNTING_ONLY' and resource['campaign_acceptance'] is False and resource['stdout_sha256']==sha(raw['stdout']),'actual resource proof/stdout join')
   attempts=[a for a in r['Attempts'] if a['Phase']=='measured'];need(attempts,'measured population')
   recalls=[scalar(a['RecallAt10']) for a in attempts];histogram={}
   for value in recalls:histogram[str(value)]=histogram.get(str(value),0)+1
   need(set(costs)=={'campaign','setup_seconds','oracle_seconds','retained_bytes'} and costs['campaign']==campaign and type(costs['retained_bytes']) is int and costs['retained_bytes']>=0,'explicit per-window observed cost receipt')
   values=dict(successful_qps=scalar(audit['successful_qps']),mean_recall=scalar(audit['mean_recall_at_10']),minimum_recall=min(recalls),success_latency=r['SuccessLatency'],writer_latency_ns=describe([scalar(v) for v in r['WriterLatencyNs']]),actual_duration_ns=scalar(r['ActualDurationNS']),overlapping_searches=r['OverlappingSearches'],completed_searches_during_mutation=r['CompletedSearchesDuringMutation'],originals=len(r['Writes']),prefixes=len(r['Prefixes']),recall_histogram=histogram,memory_peak_bytes=scalar(resource['observed_memory_peak_bytes']),process_hwm_bytes=scalar(resource['observed_process_hwm_bytes']),setup_seconds=scalar(costs['setup_seconds']),oracle_seconds=scalar(costs['oracle_seconds']),retained_bytes=costs['retained_bytes'])
   values.update(initial_population_rows=r['Admission']['PopulationRows'],final_population_rows=r['PostRecall']['PopulationRows'])
   window['metrics']=values;metrics[concurrency].append(values)
  windows.append(window)
 summaries={}
 for c,values in metrics.items():
  summaries['C'+str(c)]=dict(complete_windows=len(values),expected_windows=3,metrics={key:describe([scalar(v[key]) for v in values]) for key in ('successful_qps','mean_recall','minimum_recall','actual_duration_ns','overlapping_searches','completed_searches_during_mutation','originals','prefixes','initial_population_rows','final_population_rows','memory_peak_bytes','process_hwm_bytes','setup_seconds','oracle_seconds','retained_bytes')},window_latency_ns={key:describe([scalar(v['success_latency'][key]) for v in values]) for key in ('P50NS','P95NS','P99NS')},window_writer_latency_ns={key:describe([scalar(v['writer_latency_ns'][key]) for v in values]) for key in ('median','minimum','maximum')})
 return dict(state='DESCRIPTIVE_MATCHED_REPORT_PENDING_SEPARATE_ARTIFACT_ACCEPTANCE',campaign_acceptance=False,windows=windows,per_arm=summaries,limitations=['Latency summaries compare per-window quantiles; raw attempts are never pooled.','No significance, capacity, speedup or numeric recall gate is inferred.','Failed/incomplete windows remain in fixed order; this report does not authorize replacement or acceptance.'])
def main():
 q=argparse.ArgumentParser();q.add_argument('--windows',required=True);q.add_argument('--windows-sha256',required=True);q.add_argument('--out',required=True);a=q.parse_args()
 raw=read_ref(dict(path=a.windows,sha256=a.windows_sha256));manifest=strict(raw)
 protected=[Path(__file__).parent,a.windows]+[ref['path'] for row in manifest['windows'] for ref in row['pins'].values()]
 isolate_paths([a.out],protected);out=Path(a.out);need(not out.exists() and not out.is_symlink(),'fresh report output')
 result=build(manifest)
 with out.open('x') as f:json.dump(result,f,indent=2);f.write('\n')
 print(json.dumps(dict(state=result['state'],path=str(out),sha256=sha(out.read_bytes()))))
if __name__=='__main__':main()
