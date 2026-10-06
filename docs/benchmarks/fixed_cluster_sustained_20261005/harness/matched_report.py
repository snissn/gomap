"""Matched descriptive report construction; never grants artifact acceptance."""
if not __debug__: raise RuntimeError('ordinary Python required; assertions must run')
import sys
sys.dont_write_bytecode=True
from source_paths import isolate_paths, workload_source
import argparse,hashlib,json,math,re,statistics
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
ROLES=('node-a','node-b','node-c','node-d','client')
def role_bytes(values):
 need(isinstance(values,dict) and set(values)==set(ROLES),'exact five observed resource roles')
 need(all(type(value) is int and value>=0 for value in values.values()),'nonnegative integer observed role bytes')
 return values
def source_refs(raw):
 refs=strict(raw);need(isinstance(refs,dict) and refs,'explicit immutable source locator mapping')
 for original,ref in refs.items():
  need(isinstance(original,str) and Path(original).is_absolute(),'absolute original source identity')
  read_ref(ref)
 return refs
def field_raw(raw,key):
 text=raw.decode();obj=strict(raw);decoder=json.JSONDecoder();pos=text.index('{')+1
 for _ in obj:
  while text[pos].isspace():pos+=1
  name,pos=decoder.raw_decode(text,pos)
  while text[pos].isspace():pos+=1
  need(text[pos]==':','raw member colon');pos+=1
  while text[pos].isspace():pos+=1
  start=pos;value,pos=decoder.raw_decode(text,pos)
  if name==key:return text[start:pos].encode()
  while text[pos].isspace():pos+=1
  if text[pos]==',':pos+=1
 raise ValueError('missing raw report member '+key)
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
   need(set(pins)=={'stdout','read_audit','resources','artifact_review','costs','manifest','native_oracle','promoted_oracle','sources'},'complete descriptive inputs and separate artifact review reference')
   events=[strict(line) for line in raw['stdout'].splitlines()]
   need(len(events)==2 and [e['Event'] for e in events]==['planned','result'],'one-shot actual event pair')
   p,r=events[0]['Report'],events[1]['Report'];profile=workload_source.Workload(campaign,6,60,5,concurrency)
   for report in (p,r):
    need(report['Admission']['RunID']==profile.query_run and report['Originals']==6 and report['Concurrency']==concurrency and report['RequestedDuration']==60000000000 and report['PaceInterval']==5000000000,'exact matched raw report profile')
   outer=strict(raw['read_audit']);resource=strict(raw['resources']);costs=strict(raw['costs']);approved=strict(raw['manifest'])
   need(set(outer)=={'population','disposition','campaign_acceptance','manifest_sha256','stdout_sha256','native_oracle_sha256','promoted_oracle_sha256','source_head','source_tree','source_pins','read','audit','derivation','limits'},'complete outer read/audit provenance')
   need(outer['disposition']=='TRIAL24_READ_AUDIT_ACCOUNTING_ONLY' and outer['campaign_acceptance'] is False,'outer accounting only')
   for key,ref in [('stdout_sha256','stdout'),('manifest_sha256','manifest'),('native_oracle_sha256','native_oracle'),('promoted_oracle_sha256','promoted_oracle')]:need(outer[key]==sha(raw[ref]),'outer actual raw join '+key)
   need(approved['RunID']==campaign and all(isinstance(outer[key],str) and re.fullmatch('[0-9a-f]{40}',outer[key]) and outer[key]==approved[key] for key in ('source_head','source_tree')),'outer frozen source identity/manifest campaign')
   need(approved['local_pins'][approved['receipts']['prefix_oracles']]==outer['promoted_oracle_sha256'],'manifest promoted oracle pin')
   for ref,state in [('native_oracle','NATIVE_CANONICAL_PREFIX_ORACLES_GENERATED_PENDING_ROOT_VALIDATION'),('promoted_oracle','INDEPENDENT_CANONICAL_PREFIX_ORACLES_VERIFIED')]:
    oracle=strict(raw[ref]);need(oracle['state']==state and oracle['RunID']==profile.query_run and oracle['source_head']==outer['source_head'] and oracle['source_tree']==outer['source_tree'],'oracle source/query identity')
   refs=source_refs(raw['sources']);expected={}
   for proof in (outer,resource):
    need(isinstance(proof['source_pins'],dict) and proof['source_pins'],'retained accounting source pins')
    for path,digest in proof['source_pins'].items():
     need(path not in expected or expected[path]==digest,'consistent accounting source identity');expected[path]=digest
   need(set(refs)==set(expected) and all(refs[path]['sha256']==digest for path,digest in expected.items()),'exact authenticated accounting source mapping')
   for report in (p,r):
    for key,manifest_key in [('BinarySHA256','driver_sha256'),('ConfigSHA256','config_sha256'),('BootstrapSHA256','bootstrap_sha256')]:need(report['Admission'][key]==approved[manifest_key],'raw admission/manifest '+key)
   audit=outer['read'];attachment=outer['audit']
   # Compare exact report substrings, rather than re-encoding parsed numbers.
   result_line=raw['stdout'].splitlines()[1];marker=b'"Report":'
   need(result_line.count(marker)==1 and result_line.endswith(b'}'),'retained event report envelope')
   result_raw=result_line.split(marker,1)[1][:-1];planned_raw=field_raw(raw['stdout'].splitlines()[0],'Report')
   need(audit['raw_planned_report_sha256']==sha(planned_raw),'actual planned report join')
   need(audit['verdict']=='READ_ACCOUNTING_GATE_PASS_ONLY' and audit['campaign_acceptance'] is False and audit['raw_result_report_sha256']==sha(result_raw),'actual read-accounting proof/result join')
   need(resource['disposition']=='RESOURCE_GATE_OWNERSHIP_ACCOUNTING_ONLY' and resource['campaign_acceptance'] is False and resource['stdout_sha256']==sha(raw['stdout']),'actual resource proof/stdout join')
   need(attachment['status']=='AUDIT_ATTACHMENT_ACCOUNTING_VERIFIED_PENDING_OUTER_AUTHORITY' and attachment['plan_sha256']==sha(field_raw(result_raw,'AuditPlan')),'actual audit plan join')
   audits=field_raw(result_raw,'Audits').decode();decoder=json.JSONDecoder();pos=1;digests=[]
   for _ in r['Audits']:
    while audits[pos].isspace():pos+=1
    start=pos;value,pos=decoder.raw_decode(audits,pos);digests.append(sha(audits[start:pos].encode()))
    while audits[pos].isspace():pos+=1
    if audits[pos]==',':pos+=1
   need(len(digests)==4 and attachment['audits_sha256']==digests,'four raw audit attachment joins')
   attempts=[a for a in r['Attempts'] if a['Phase']=='measured'];need(attempts,'measured population')
   recalls=[scalar(a['RecallAt10']) for a in attempts];histogram={}
   for value in recalls:histogram[str(value)]=histogram.get(str(value),0)+1
   need(set(costs)=={'campaign','setup_seconds','oracle_seconds','retained_bytes'} and costs['campaign']==campaign and type(costs['retained_bytes']) is int and costs['retained_bytes']>=0,'explicit per-window observed cost receipt')
   need(audit['success_latency']==r['SuccessLatency'] and audit['counts']==r['Counts'] and audit['warmup_counts']==r['WarmupCounts'] and audit['overlapping_searches']==r['OverlappingSearches'] and audit['completed_searches_during_mutation']==r['CompletedSearchesDuringMutation'],'nested read/report accounting joins')
   values=dict(successful_qps=scalar(audit['successful_qps']),mean_recall=scalar(audit['mean_recall_at_10']),minimum_recall=min(recalls),success_latency=r['SuccessLatency'],writer_latency_ns=describe([scalar(v) for v in r['WriterLatencyNs']]),actual_duration_ns=scalar(r['ActualDurationNS']),overlapping_searches=r['OverlappingSearches'],completed_searches_during_mutation=r['CompletedSearchesDuringMutation'],originals=len(r['Writes']),prefixes=len(r['Prefixes']),recall_histogram=histogram,memory_peak_bytes=role_bytes(resource['observed_memory_peak_bytes']),process_hwm_bytes=role_bytes(resource['observed_process_hwm_bytes']),setup_seconds=scalar(costs['setup_seconds']),oracle_seconds=scalar(costs['oracle_seconds']),retained_bytes=costs['retained_bytes'])
   values.update(initial_population_rows=r['Admission']['PopulationRows'],final_population_rows=r['PostRecall']['PopulationRows'])
   window['accounting_provenance']=outer;window['resource_proof']=resource
   window['metrics']=values;metrics[concurrency].append(values)
  windows.append(window)
 summaries={}
 for c,values in metrics.items():
  summaries['C'+str(c)]=dict(complete_windows=len(values),expected_windows=3,metrics={key:describe([scalar(v[key]) for v in values]) for key in ('successful_qps','mean_recall','minimum_recall','actual_duration_ns','overlapping_searches','completed_searches_during_mutation','originals','prefixes','initial_population_rows','final_population_rows','setup_seconds','oracle_seconds','retained_bytes')},observed_role_bytes={key:{role:describe([v[key][role] for v in values]) for role in ROLES} for key in ('memory_peak_bytes','process_hwm_bytes')},window_latency_ns={key:describe([scalar(v['success_latency'][key]) for v in values]) for key in ('P50NS','P95NS','P99NS')},window_writer_latency_ns={key:describe([scalar(v['writer_latency_ns'][key]) for v in values]) for key in ('median','minimum','maximum')})
 return dict(state='DESCRIPTIVE_MATCHED_REPORT_PENDING_SEPARATE_ARTIFACT_ACCEPTANCE',campaign_acceptance=False,windows=windows,per_arm=summaries,limitations=['Latency summaries compare per-window quantiles; raw attempts are never pooled.','No significance, capacity, speedup or numeric recall gate is inferred.','Failed/incomplete windows remain in fixed order; this report does not authorize replacement or acceptance.'])
def main():
 q=argparse.ArgumentParser();q.add_argument('--windows',required=True);q.add_argument('--windows-sha256',required=True);q.add_argument('--out',required=True);a=q.parse_args()
 raw=read_ref(dict(path=a.windows,sha256=a.windows_sha256));manifest=strict(raw)
 protected=[Path(__file__).parent,a.windows]+[ref['path'] for row in manifest['windows'] for ref in row['pins'].values()]
 for row in manifest['windows']:
  if row['status']=='complete':protected.extend(ref['path'] for ref in source_refs(read_ref(row['pins']['sources'])).values())
 isolate_paths([a.out],protected);out=Path(a.out);need(not out.exists() and not out.is_symlink(),'fresh report output')
 result=build(manifest)
 with out.open('x') as f:json.dump(result,f,indent=2);f.write('\n')
 print(json.dumps(dict(state=result['state'],path=str(out),sha256=sha(out.read_bytes()))))
if __name__=='__main__':main()
