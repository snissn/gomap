from pathlib import Path
import re,json,collections
p=Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013/evidence/direct-bottleneck-profiles-20261015')
def decode(name):
 text=(p/('decoded-'+name+'.txt')).read_text(); samp,loc=text.split('Locations\n',1); loc=loc.split('Mappings',1)[0]; locations={};current=None
 for line in loc.splitlines():
  m=re.match(r'\s*(\d+): 0x[0-9a-f]+ M=\d+ (.*)',line)
  if m:current=int(m[1]);locations[current]=[m[2].split(' ')[0]]
  elif current is not None and line.strip():locations[current].append(line.strip().split(' ')[0])
 rows=[]
 for line in samp.splitlines():
  m=re.match(r'\s*((?:\d+\s+)*\d+):\s+((?:\d+\s*)+)$',line)
  if not m:continue
  weights=list(map(int,m[1].split()));ids=list(map(int,m[2].split()));names=[f for i in ids for f in locations[i]]
  rows.append({'weights':weights,'names':names})
 assert rows and locations
 return rows

def summarize(name,focus=None):
 rows=decode(name);idx=1
 total=sum(r['weights'][idx] for r in rows)
 focused=[r for r in rows if not focus or any((f.endswith(focus) if focus == '(*Batch).Write' else focus in f) for f in r['names'])];den=sum(r['weights'][idx] for r in focused)
 def union(*terms):return sum(r['weights'][idx] for r in focused if any(t in f for f in r['names'] for t in terms))
 flat=collections.Counter();cum=collections.Counter()
 for r in focused:
  flat[r['names'][0]]+=r['weights'][idx]
  for f in set(r['names']):cum[f]+=r['weights'][idx]
 result={'profile':name,'focus':focus,'whole_weight':total,'focused_weight':den,'focused_rows':len(focused),'flat_top':flat.most_common(25),'cumulative_top':cum.most_common(40),'union_groups':{}}
 groups={
 'allocator_prepare':['PrepareCOWCandidateRetiringWithLimitsV1'],
 'allocator_materialize':['(*FreelistTxn).MaterializeCandidate'],
 'allocator_encoding':['freelist.encodeIndexPage','freelist.encodeChunkPage','freelist.encodeGenerationPage','freelist.encodeNormalizedReservationPages'],
 'allocator_state_cloning':['freelist.cloneStateNode','(*stateChunk).clone'],
 'mallocgc':['runtime.mallocgc'],
 'gc_work':['runtime.gcDrain','runtime.gcAssistAlloc','runtime.gcBgMarkWorker','runtime.scanobject','runtime.scanObject'],
 'gc_or_malloc_union':['runtime.mallocgc','runtime.gcDrain','runtime.gcAssistAlloc','runtime.gcBgMarkWorker','runtime.scanobject','runtime.scanObject'],
 'snapshot_acquire_or_close':['acquireOneShotReadOrErr','(*oneShotRead).close'],
 'mutex_or_atomic':['sync.(*Mutex).Lock','sync.(*Mutex).Unlock','sync/atomic.'],
 'prune_finalize':['(*RootPublicationBuildGroupV1).finalizeLocked','(*RootPublicationBuildGroup).finalizeLocked'],
 'capture_resources':['captureDurableRootResourcesFromBaseWithRefCounts','captureDurableValueLogResourcesWithMetadata'],
 'reachability_scanning':['scanCandidateExternal','scanCandidateValueLog','maintenanceReachabilityScan'],
 }
 for k,v in groups.items():
  n=union(*v);result['union_groups'][k]={'weight':n,'percent_of_focus':n/den*100 if den else None}
 return result
result={
 'ordinary_batch_cpu':summarize('ordinary-batch-cpu'),
 'ordinary_batch_actual_write_cpu':summarize('ordinary-batch-cpu','(*Batch).Write'),
 'ordinary_get_cpu':summarize('ordinary-get-cpu'),
 'ordinary_seek_cpu':summarize('ordinary-seek-cpu'),
 'native_prune_cpu':summarize('native-diagnostic-cpu','PruneVersionsUnboundedDiagnosticForTest'),
 'ordinary_batch_alloc':summarize('ordinary-batch-alloc'),
 'ordinary_batch_actual_write_alloc':summarize('ordinary-batch-alloc','(*Batch).Write'),
 'native_prune_mutex':summarize('native-diagnostic-mutex','PruneVersionsUnboundedDiagnosticForTest'),
 'native_prune_block':summarize('native-diagnostic-block','PruneVersionsUnboundedDiagnosticForTest'),
}
result['ordinary_get_mutex_atomic_callers']={}
for r in decode('ordinary-get-cpu'):
 if not any(t in f for f in r['names'] for t in ['sync.(*Mutex).Lock','sync.(*Mutex).Unlock','sync/atomic.']):continue
 owner=next((f for f in r['names'] if f.startswith('github.com/snissn/gomap/TreeDB/')), 'other')
 result['ordinary_get_mutex_atomic_callers'][owner]=result['ordinary_get_mutex_atomic_callers'].get(owner,0)+r['weights'][1]
(p/'stack-attribution.json').write_text(json.dumps(result,indent=2)+'\n')
for k,r in result.items():
 if k=='ordinary_get_mutex_atomic_callers':
  print(k,json.dumps(r));continue
 print(k, json.dumps({i:r[i] for i in ['whole_weight','focused_weight','focused_rows','union_groups']}))
 print('FLAT',json.dumps(r['flat_top'][:6]))
