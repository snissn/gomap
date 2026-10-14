from pathlib import Path
import hashlib,json,os,re,subprocess,time,shutil
base=Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013')
perf=base/'sprint-performance-20261014'
out=base/'evidence/root-primary-empty-fastpath-20261014/causal-comparison'
out.mkdir(exist_ok=False)
go=base/'toolchains/go1.26.3-from-185-20261014'
env=dict(os.environ,GOROOT=str(go),GOWORK='off',GOTOOLCHAIN='local',CGO_ENABLED='1',GOMAXPROCS='4')
def sha(b):return hashlib.sha256(b).hexdigest()
harness='TreeDB/mvcc/sprintperf/benchmark_test.go'
original=(perf/'sources/coherent-candidate'/harness).read_text()
assert sha(original.encode())=='9785066d7a74d1fb962c03ac57ea43866e036c66b92aff1c07bab0acd818edfb'
patched=original.replace('\t"strconv"\n','\t"strconv"\n\t"strings"\n')
anchor='\t\t\t\t\t\tfor _, stats := range []map[string]string{before, after} {'
assert patched.count(anchor)==1
patched=patched.replace(anchor,'\t\t\t\t\t\tsprintLaneReceipt(b, before, "before")\n\t\t\t\t\t\tsprintLaneReceipt(b, after, "after")\n'+anchor)
patched+='''
// Endpoint reporting only: the existing timed workload and stats snapshots are unchanged.
func sprintLaneReceipt(b *testing.B, stats map[string]string, phase string) {
    keys := make([]string, 0)
    for key := range stats {
        if strings.HasPrefix(key, "treedb.cache.value_log.directory_sync.") || strings.HasPrefix(key, "treedb.cache.journal_lanes.") || strings.HasPrefix(key, "treedb.cache.vlog_shape.") {
            keys = append(keys, key)
        }
    }
    sort.Strings(keys)
    for _, key := range keys {
        b.Logf("lane_probe n=%d phase=%s key=%s value=%s", b.N, phase, key, stats[key])
    }
}
'''
(out/'benchmark.original.go').write_text(original)
(out/'benchmark.counter.go').write_text(patched)
bindings={}
for label,source in [('causal-before','coherent-candidate'),('causal-after','fastpath-candidate')]:
 target=perf/'sources'/label
 shutil.copytree(perf/'sources'/source,target,copy_function=os.link)
 p=target/harness;p.unlink();p.write_text(patched)
 argv=[str(go/'bin/go'),'test','-c','-trimpath','-buildvcs=false','-o',str(perf/'binaries'/(label+'-mvcc.test')),'./TreeDB/mvcc/sprintperf']
 log=out/(label+'-build.log');start=time.monotonic()
 with log.open('wb') as f:r=subprocess.run(argv,cwd=target,env=env,stdout=f,stderr=subprocess.STDOUT,timeout=180).returncode
 binding={'source_projection':source,'argv':argv,'exit':r,'seconds':time.monotonic()-start,'log_sha256':sha(log.read_bytes()),'harness_sha256':sha(p.read_bytes()),'production_change':'TreeDB/tree/owned_iterator.go only between before and after'}
 if r==0:binding['binary_sha256']=sha((perf/'binaries'/(label+'-mvcc.test')).read_bytes())
 bindings[label]=binding;(out/'build-bindings.json').write_text(json.dumps(bindings,indent=1)+'\n')
 print(json.dumps({'build':label,**binding}),flush=True)
 if r:print(log.read_text()[-5000:]);raise SystemExit(r)
def production_census(root):
 return {str(p.relative_to(root)):sha(p.read_bytes()) for p in root.rglob('*.go') if not p.name.endswith('_test.go')}
before=production_census(perf/'sources/causal-before');after=production_census(perf/'sources/causal-after')
assert before.keys()==after.keys()
different=[x for x in before if before[x]!=after[x]]
assert different==['TreeDB/tree/owned_iterator.go'],different
(out/'production-census.json').write_text(json.dumps({'before':before,'after':after,'changed_paths':different},indent=1)+'\n')
processes=subprocess.check_output(['ps','-eo','pid,ppid,etime,pcpu,pmem,args','--sort=-pcpu'],text=True)
(out/'timing-start-processes.txt').write_text(processes)
for line in processes.splitlines()[1:]:
 command=' '.join(line.split()[5:])
 if re.match(r'.*(?:/bin/go (?:test|build)|/pkg/tool/[^ ]+/(?:compile|link)|\.test(?: |$)|unified-bench(?: |$))',command):raise SystemExit('producer active: '+line)
tmp=perf/'causal-timed-temp';tmp.mkdir(exist_ok=False)
receipts=[];wall_total=0
for sample in [1,2]:
 for placement,operation in [('inline','point'),('inline','group_versions'),('inline','concurrent'),('forced_pointer','point')]:
  for label in (['causal-before','causal-after'] if sample==1 else ['causal-after','causal-before']):
   binary=perf/'binaries'/(label+'-mvcc.test');assert sha(binary.read_bytes())==bindings[label]['binary_sha256']
   name=f'{sample}-{placement}-{operation}-{label}';temp=tmp/name;temp.mkdir()
   local=dict(env,TMPDIR=str(temp),SPRINT_MEMTABLE='cow_btree')
   regex=f'^BenchmarkSprintMVCC$/^command_wal_relaxed$/^cow_btree$/^{placement}$/^{operation}$'
   argv=['taskset','-c','0-3',str(binary),'-test.run','^$','-test.bench',regex,'-test.benchmem','-test.benchtime=64x','-test.count=1','-test.v','-test.timeout=30s']
   log=out/(name+'.log');start=time.monotonic();stamp=time.time()
   with log.open('wb') as f:
    try:r=subprocess.run(argv,env=local,stdout=f,stderr=subprocess.STDOUT,timeout=35).returncode
    except subprocess.TimeoutExpired:r=124
   wall=time.monotonic()-start;wall_total+=wall
   rows=[line for line in log.read_text().splitlines() if re.match(r'^Benchmark\S+\s+\d+\s+',line)]
   probes=[]
   for line in log.read_text().splitlines():
    m=re.search(r'lane_probe n=64 phase=(\S+) key=(\S+) value=(\S+)',line)
    if m:probes.append({'phase':m[1],'key':m[2],'value':m[3]})
   receipt={'label':label,'sample':sample,'placement':placement,'operation':operation,'argv':argv,'env':{k:local[k] for k in ['GOROOT','GOWORK','GOTOOLCHAIN','CGO_ENABLED','GOMAXPROCS','SPRINT_MEMTABLE','TMPDIR']},'exit':r,'unix_start':stamp,'wall_seconds':wall,'log':log.name,'raw_sha256':sha(log.read_bytes()),'binary_sha256':sha(binary.read_bytes()),'rows':rows,'lane_probes':probes}
   receipts.append(receipt);(out/'measurement-receipts.json').write_text(json.dumps({'diagnostic_only':True,'wall_seconds':wall_total,'receipts':receipts},indent=1)+'\n')
   print(json.dumps({'run':name,'exit':r,'rows':len(rows),'lane_probes':len(probes),'seconds':wall}),flush=True)
   if r or len(rows)!=1 or not probes:print(log.read_text()[-5000:]);raise SystemExit('FAILED_OR_UNMATCHED')
   assert wall_total<60,'bounded comparison wall exceeded'
for label,binding in bindings.items():assert sha((perf/'binaries'/(label+'-mvcc.test')).read_bytes())==binding['binary_sha256']
(out/'timing-end-processes.txt').write_text(subprocess.check_output(['ps','-eo','pid,ppid,etime,pcpu,pmem,args','--sort=-pcpu'],text=True))
print(json.dumps({'COMPLETE':True,'invocations':len(receipts),'wall_seconds':wall_total}),flush=True)
