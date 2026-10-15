"""Offline pprof protobuf decoding; no benchmark/binary execution."""
import gzip,json,hashlib,re,collections
from pathlib import Path
P=Path(__file__).parent

def var(b,i):
 v=s=0
 while True:
  x=b[i];i+=1;v|=(x&127)<<s
  if x<128:return v,i
  s+=7

def fields(b):
 i=0
 while i<len(b):
  k,i=var(b,i);f,w=k>>3,k&7
  if w==0:v,i=var(b,i)
  elif w==2:n,i=var(b,i);v=b[i:i+n];i+=n
  elif w==1:v=b[i:i+8];i+=8
  elif w==5:v=b[i:i+4];i+=4
  else:raise ValueError(w)
  yield f,w,v

def packed(v):
 i=0;out=[]
 while i<len(v):x,i=var(v,i);out.append(x)
 return out

def parse(path):
 m=list(fields(gzip.decompress(path.read_bytes())))
 ss=[v.decode() for f,w,v in m if f==6]
 fs={};ls={}
 for f,w,v in m:
  if f==5:
   d={a:c for a,b,c in fields(v)};fs[d[1]]={'name':ss[d[2]],'file':ss[d[4]],'start':d.get(5)}
 for f,w,v in m:
  if f==4:
   d=list(fields(v));lid=next(c for a,b,c in d if a==1)
   ls[lid]=[]
   for a,b,c in d:
    if a==4:
     q={a:c for a,b,c in fields(c)};ls[lid].append({**fs[q[1]],'line':q.get(2)})
 out=[]
 for f,w,v in m:
  if f==2:
   loc=[];vals=[]
   for a,b,c in fields(v):
    if a==1:loc.extend(packed(c) if b==2 else [c])
    if a==2:vals.extend(packed(c) if b==2 else [c])
   stack=[q for i in loc for q in ls[i]]
   out.append((vals[1],vals[0],stack))
 return out

def category(stack):
 names=[q['name'] for q in stack]
 if not any('db.BenchmarkGetVersioned.func2' in n for n in names):return 'outside benchmark worker'
 if any('tree.(*Tree).GetVersionedAppend' in n for n in names):return 'lookup'
 if any('db.(*DB).acquireOneShotReadOrErr' in n for n in names):return 'capture'
 if any('db.(*oneShotRead).close' in n for n in names):return 'close and scrub'
 if any('db.(*DB).GetVersionedAppend' in n for n in names):return 'Get dispatch and retry checks'
 return 'benchmark worker harness'

rows=[]
for p in sorted(P.glob('*.pprof')):
 samples=parse(p);sec=collections.Counter();leaf=collections.defaultdict(collections.Counter);lines=collections.defaultdict(collections.Counter)
 for ns,count,stack in samples:
  cat=category(stack);sec[cat]+=ns
  if stack:
   q=stack[0];leaf[cat][q['name']]+=ns;lines[cat][f"{q['file']}:{q['line']} {q['name']}"]+=ns
 total=sum(sec.values());worker=total-sec['outside benchmark worker']
 log=(P/(p.stem+'.log')).read_text();match=re.search(r'BenchmarkGetVersioned\s+(\d+)\s+([\d.]+) ns/op',log)
 row={'profile':p.name,'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'total_cpu_ns':total,'worker_cpu_ns':worker,'sample_count':sum(c for n,c,s in samples),'final_benchmark_N':int(match[1]),'final_benchmark_ns_op':float(match[2]),'categories':{k:{'cpu_ns':v,'percent_total':100*v/total,'percent_worker':100*v/worker if k!='outside benchmark worker' else None,'flat_leaf_cpu_ns':dict(leaf[k]),'flat_leaf_source_cpu_ns':dict(lines[k])} for k,v in sec.items()}}
 assert sum(x['cpu_ns'] for x in row['categories'].values())==total
 rows.append(row)
out={'method':'Decode original gzip pprof protobuf CPU values; assign each sample exactly once by stack ancestry, in priority outside worker, tree.GetVersionedAppend lookup, acquireOneShot capture, oneShot.close, remaining DB.GetVersionedAppend dispatch, remaining benchmark harness. Each category sums the actual flat leaf CPU samples; no overlapping cumulative totals are added. Inline frames use first/innermost line as leaf.','denominator_warning':'CPU profiles cover whole process including benchmark calibration and fixture; final printed N is not the operation denominator of all CPU samples. No CPU ns/op is calculated by dividing profile CPU by final N. Percentages use exact sampled total/worker CPU denominators. Timing rows are instrumented local diagnostics only.','rows':rows}
(P/'partitioned-flat.json').write_text(json.dumps(out,indent=2)+'\n')
for r in rows:
 print(r['profile'],r['total_cpu_ns']/1e9,r['worker_cpu_ns']/1e9,r['sample_count'],r['final_benchmark_ns_op'], {k:round(v['cpu_ns']/1e9,3) for k,v in r['categories'].items()})
 print('CAPTURE', sorted(r['categories']['capture']['flat_leaf_cpu_ns'].items(),key=lambda x:-x[1])[:12])
