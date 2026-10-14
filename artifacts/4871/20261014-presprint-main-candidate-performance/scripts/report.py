from pathlib import Path
import json,hashlib,statistics,math
base=Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013/sprint-performance-20261014');e=base/'evidence'
def sha(b):return hashlib.sha256(b).hexdigest()
data=json.loads((e/'sprint-measurement-receipts.json').read_text());receipts=data['receipts']
assert len(receipts)==105
assert all(r['exit']==0 and r['rows'] for r in receipts)
rows={};parsed=[]
for r in receipts:
 assert sha((e/r['log']).read_bytes())==r['raw_sha256']
 assert sha((base/'binaries'/Path(r['argv'][3]).name).read_bytes())==r['binary_sha256']
 for line in r['rows']:
  tokens=line.split();metrics={tokens[n+1]:float(tokens[n]) for n in range(2,len(tokens),2)}
  entry={'kind':r['kind'],'label':r['label'],'group':r['group'],'sample':r['sample'],'name':tokens[0],'iterations':int(tokens[1]),'metrics':metrics}
  parsed.append(entry)
  if r['kind']!='smoke':rows[(r['kind'],r['group'],r['label'],r['sample'])]=entry
def med(kind,group,label,key):return statistics.median(rows[(kind,group,label,s)]['metrics'][key] for s in [1,2])
def delta(kind,group,control,candidate,key='ns/op'):
 return (math.sqrt(math.prod(rows[(kind,group,candidate,s)]['metrics'][key]/rows[(kind,group,control,s)]['metrics'][key] for s in [1,2]))-1)*100
summary={'diagnostic_only':True,'invocations':len(receipts),'timed_wall_seconds':data['timed_wall_seconds'],'micro':[],'public':[]}
labels=['before-sprint','recent-main','coherent-candidate']
for group in ['get_versioned','batch_write','snapshot_seek','repeated_iterator','durable_sync']:
 item={'group':group,'medians':{label:{key:med('micro',group,label,key) for key in ['ns/op','B/op','allocs/op']} for label in labels},'paired_time_change_from_presprint_percent':delta('micro',group,'before-sprint','coherent-candidate'),'paired_time_change_from_recentmain_percent':delta('micro',group,'recent-main','coherent-candidate'),'samples':{label:[rows[('micro',group,label,s)] for s in [1,2]] for label in labels}}
 guards=[]
 for control in ['before-sprint','recent-main']:
  old=item['medians'][control];new=item['medians']['coherent-candidate'];d=delta('micro',group,control,'coherent-candidate')
  byte_delta=new['B/op']-old['B/op'];byte_tolerance=min(.01*old['B/op'],64)
  reasons=[]
  if d>5:reasons.append('paired time >5%')
  if new['allocs/op']>old['allocs/op']:reasons.append('added allocations')
  if byte_delta>byte_tolerance:reasons.append('added B/op above unchanged smaller-of1%-or64B tolerance')
  guards.append({'control':control,'verdict':'FAIL' if reasons else 'NO_THRESHOLD_CROSSING_OBSERVED_SHORT_NOT_ACCEPTANCE','reasons':reasons})
 item['raw_guards']=guards;summary['micro'].append(item)
for group in sorted({r['group'] for r in receipts if r['kind']=='public'}):
 keys=rows[('public',group,'coherent-candidate',1)]['metrics'].keys()
 wanted=[key for key in ['ns/op','B/op','allocs/op','writer_p50_ns','point_p50_ns','scan_p50_ns','writer_ops/s','point_phase_ops/s','scan_phase_ops/s','wal_appends/op','wal_syncs/op','public_checkpoints/op','vlog_syncs/op','snapshot_rotations/op','cow_capture_calls_total/op','cow_publications_total/op'] if key in keys]
 summary['public'].append({'group':group,'medians':{label:{key:med('public',group,label,key) for key in wanted if key in rows[('public',group,label,1)]['metrics']} for label in labels},'paired_time_change_from_presprint_percent':delta('public',group,'before-sprint','coherent-candidate'),'paired_time_change_from_recentmain_percent':delta('public',group,'recent-main','coherent-candidate')})
(e/'sprint-parsed-rows.json').write_text(json.dumps(parsed,indent=1)+'\n');(e/'sprint-summary.json').write_text(json.dumps(summary,indent=1)+'\n')
lines=['# Bounded before-sprint and candidate comparison','', '105 invocations:3 setup/output/ACK/layout smoke invocations (36 rows),30 matched raw micro invocations,72 fixed64x public mechanism invocations. Same111 host and Go1.26.3; microCPU0/GOMAXPROCS1, publicCPU0-3/GOMAXPROCS4; balanced two forward/reverse samples. Timings are short observations, not statistical acceptance or original Dgraph/Badger M0 qualification.','', '## Matched microbenchmarks','', '| Row | Pre-sprint ns/op | Recent main ns/op | Repaired candidate ns/op | Paired time delta vs pre-sprint | B/op old→candidate | allocs/op old→candidate |','|---|---:|---:|---:|---:|---:|---:|']
for item in summary['micro']:
 a,b,c=[item['medians'][label] for label in labels]
 lines.append(f"| {item['group']} | {a['ns/op']:.3f} | {b['ns/op']:.3f} | {c['ns/op']:.3f} | {item['paired_time_change_from_presprint_percent']:+.2f}% | {a['B/op']:g}→{c['B/op']:g} | {a['allocs/op']:g}→{c['allocs/op']:g} |")
lines+=['','Raw concerns (unchanged5% time, no added allocs, bytes tolerance smallerof1%/64B;0Bstrict):']
for item in summary['micro']:
 for guard in item['raw_guards']:
  if guard['verdict']=='FAIL':lines.append(f"- {item['group']} vs {guard['control']}: "+'; '.join(guard['reasons']))
lines+=['','## Public constant-history mechanism','', 'Work unit:point=one CommitAt then oneGetAt;group_versions=one4-mutation CommitGroupAt then exact two-version owned history;concurrent=onegroup writer+onepoint reader+onehistory reader each64 calls. Equal overwrites cannot prove atomic torn-group behavior, sustained historical retention/native prune/reclaim, or growing histories. Common ownedEntry output and time.Now costs are included. Originaladaptive vs actualrecent/candidatecow_btree are intentionally distinct selectors.','', '| Profile/placement/workload | Pre-sprint ns/unit | Recent main ns/unit | Candidate ns/unit | Paired delta vs original | Paired delta vs recent |','|---|---:|---:|---:|---:|---:|']
for item in summary['public']:
 a,b,c=[item['medians'][label] for label in labels]
 lines.append(f"| {item['group']} | {a['ns/op']:.1f} | {b['ns/op']:.1f} | {c['ns/op']:.1f} | {item['paired_time_change_from_presprint_percent']:+.2f}% | {item['paired_time_change_from_recentmain_percent']:+.2f}% |")
lines+=['','Original M0 remains >10% relative TreeDB/Badger gains BOTH RELAXED+DURABLE, all3 aligned repeats improving and existing guards. No full Dgraph matrix, end-to-end conclusion, merged production or full native qualification from this packet.','',f"Measured invocation wall total {data['timed_wall_seconds']:.2f}s (excludes3smokes, compilation, inspection). Raw and every sample retained."]
(e/'sprint-report.md').write_text('\n'.join(lines)+'\n')
print('\n'.join(lines[:25]));print('PUBLIC');print(json.dumps(summary['public'],indent=1))
