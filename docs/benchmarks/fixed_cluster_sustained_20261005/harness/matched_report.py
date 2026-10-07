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
# Original issued budgets are fixed SOURCE bindings, never caller-selected hashes.
ORIGINAL_BUDGETS={'rf4matched5068w01c1': {'path': '/Volumes/FlashDrive/gomap-5021-sustained-evidence-root-v1/rf4matched5068w01c1-phase-budget-root-v1.json', 'sha256': '03cb3d23d64e399731a184b88fb35b895fcca8b6bc1a1c2cee82d17670f43dd1'}, 'rf4matched5068w02c4': {'path': '/Volumes/FlashDrive/gomap-5021-sustained-evidence-root-v1/rf4matched5068w02c4-phase-budget-root-v1.json', 'sha256': 'caae9ca3786bb38a31229654bbb873c3876860d170c8c4d79cc39e207c0d637c'}, 'rf4matched5068w03c4': {'path': '/Volumes/FlashDrive/gomap-5021-sustained-evidence-root-v1/rf4matched5068w03c4-phase-budget-root-v1.json', 'sha256': '4e737bec0aa234515f3a6ed605c64f30ce79a4e60e29efd51bbce942f912a353'}, 'rf4matched5068w04c1': {'path': '/Volumes/FlashDrive/gomap-5021-sustained-evidence-root-v1/rf4matched5068w04c1-phase-budget-root-v1.json', 'sha256': '65a3cd01faf367234dc179cddfe27a8952124d23919ea39aaa30913255c92925'}}
ROLES=('node-a','node-b','node-c','node-d','client')
def role_bytes(values):
 need(isinstance(values,dict) and set(values)==set(ROLES),'exact five observed resource roles')
 need(all(type(value) is int and value>=0 for value in values.values()),'nonnegative integer observed role bytes')
 return values
def matched_profile(report,profile):
 # Go omits Originals when the default six-write count is selected. Match
 # shared_read.original_count: omitted/zero means six, never infer from calls.
 count=report.get('Originals',0)
 need(type(count) is int and count in (0,6),'matched declared original count')
 fixed={'Version':1,'Concurrency':profile.concurrency,'WarmupPlanned':64,'MaxAttempts':65536,'OutputBytes':134217728,'RequestedDuration':60000000000,'PaceInterval':5000000000}
 need(all(type(report[key]) is int and report[key]==value for key,value in fixed.items()),'exact typed matched raw report profile')
 need(report['Kind']=='fixed_cluster_mixed_window_v1' and report['Profile']=='changing-top10' and report['Admission']['RunID']==profile.query_run,'matched kind/profile/query identity')
 for key,ordinal,size in [('Writes','Ordinal',6),('Prefixes','Prefix',7)]:
  rows=report[key]
  need(isinstance(rows,list) and len(rows)==size and all(isinstance(row,dict) and type(row.get(ordinal)) is int and row[ordinal]==i for i,row in enumerate(rows)),'contiguous matched '+key)
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
def selection(manifest):
 need(isinstance(manifest,dict) and type(manifest.get('Version')) is int,'exact report manifest version')
 if manifest['Version']==1:
  need(set(manifest)=={'Version','windows'} and isinstance(manifest['windows'],list) and len(manifest['windows'])==6,'six predeclared windows; preserve failed/incomplete entries')
  return workload_source.MATCHED_CAMPAIGNS,workload_source.MATCHED_ORDER,None
 need(manifest['Version']==2 and set(manifest)=={'Version','revision','decision','historical','windows'} and manifest['revision']==workload_source.CONTINUATION_REVISION,'explicit prospective continuation manifest')
 decision=strict(read_ref(manifest['decision']))
 need(manifest['decision']['sha256']==workload_source.CONTINUATION_DECISION_SHA,'exact accepted continuation decision')
 need(decision['state']=='ROOT_ACCEPTED_PROSPECTIVE_CONTINUATION_DECISION_NOT_QUALIFICATION' and decision['predeclared_final_selection']==list(workload_source.SELECTED_CAMPAIGNS) and decision['prospective_order']==[dict(campaign=c,concurrency=n) for c,n in zip(workload_source.CONTINUATION_CAMPAIGNS,workload_source.CONTINUATION_ORDER)],'accepted predeclared continuation selection')
 rows=manifest['windows']
 need(isinstance(rows,list) and len(rows)==6,'six fixed selected windows')
 ledger=strict(read_ref(manifest['historical']))
 need(isinstance(ledger,dict) and set(ledger)=={'Version','state','windows'} and type(ledger['Version']) is int and ledger['Version']==1 and ledger['state']=='INCOMPLETE_FOREVER_FOR_THIS_ATTEMPT','original attempt remains incomplete')
 history=ledger['windows']
 need(isinstance(history,list) and len(history)==6,'complete original six-window ledger')
 statuses=('complete','complete','complete','EXPIRED_SETUP_ONLY_NOT_MEASURED','UNISSUED_PROSPECTIVELY_RETIRED','UNISSUED_PROSPECTIVELY_RETIRED')
 for i,(row,campaign,status) in enumerate(zip(history,workload_source.MATCHED_CAMPAIGNS,statuses)):
  need(isinstance(row,dict) and set(row)=={'campaign','status','pins','evidence'} and row['campaign']==campaign and row['status']==status,'exact historical campaign order and immutable outcome')
  need(isinstance(row['pins'],dict) and isinstance(row['evidence'],dict) and row['evidence'] and all(isinstance(k,str) and k for k in row['evidence']),'retained historical evidence references')
  if i<4:
   need('budget' in row['evidence'],'issued historical original budget reference')
   expected=ORIGINAL_BUDGETS[campaign]
   need(row['evidence']['budget']==expected,'unchanged predeclared original budget path/digest')
   if 'original_budget' in row['evidence']:
    need(row['evidence']['original_budget']==expected,'unchanged original budget alias path/digest')
  for ref in row['evidence'].values():read_ref(ref)
  if i<3:need(row['pins']==rows[i].get('pins') and rows[i].get('campaign')==campaign and rows[i].get('status')=='complete','unchanged selected legacy raw locators and outcomes')
  else:need(row['pins']=={},'unmeasured historical attempts have no selected measurements')
 return workload_source.SELECTED_CAMPAIGNS,workload_source.SELECTED_ORDER,ledger
def build(manifest):
 campaigns,order,ledger=selection(manifest)
 windows=[];metrics={c:[] for c in (1,4)}
 for row,campaign,concurrency in zip(manifest['windows'],campaigns,order):
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
    matched_profile(report,profile)
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
 result=dict(state='DESCRIPTIVE_MATCHED_REPORT_PENDING_SEPARATE_ARTIFACT_ACCEPTANCE',campaign_acceptance=False,windows=windows,per_arm=summaries,limitations=['Latency summaries compare per-window quantiles; raw attempts are never pooled.','No significance, capacity, speedup or numeric recall gate is inferred.','Failed/incomplete windows remain in fixed order; this report does not authorize replacement or acceptance.'])
 if ledger is not None:
  result.update(revision=manifest['revision'],decision_ref=manifest['decision'],historical_ref=manifest['historical'],historical_ledger=ledger,selection_complete=all(w['status']=='complete' for w in windows) and all(arm['complete_windows']==3 for arm in summaries.values()))
  result['limitations'].append('Original six-window attempt remains INCOMPLETE; selected continuation completeness is descriptive and is not independent acceptance or three-copy retention.')
 return result
def main():
 q=argparse.ArgumentParser();q.add_argument('--windows',required=True);q.add_argument('--windows-sha256',required=True);q.add_argument('--out',required=True);a=q.parse_args()
 raw=read_ref(dict(path=a.windows,sha256=a.windows_sha256));manifest=strict(raw)
 result=build(manifest)
 protected=[Path(__file__).parent,a.windows]+[ref['path'] for row in manifest['windows'] for ref in row['pins'].values()]
 for row in manifest['windows']:
  if row['status']=='complete':protected.extend(ref['path'] for ref in source_refs(read_ref(row['pins']['sources'])).values())
 if 'historical_ledger' in result:
  protected.extend(manifest[key]['path'] for key in ('decision','historical'))
  protected.extend(ref['path'] for row in result['historical_ledger']['windows'] for ref in row['evidence'].values())
 isolate_paths([a.out],protected);out=Path(a.out);need(not out.exists() and not out.is_symlink(),'fresh report output')
 with out.open('x') as f:json.dump(result,f,indent=2);f.write('\n')
 print(json.dumps(dict(state=result['state'],path=str(out),sha256=sha(out.read_bytes()))))
if __name__=='__main__':main()
