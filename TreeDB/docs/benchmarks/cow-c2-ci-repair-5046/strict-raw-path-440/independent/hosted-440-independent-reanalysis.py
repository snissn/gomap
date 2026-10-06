from pathlib import Path
import hashlib, json, math, re, statistics, subprocess, importlib.util, sys
sys.dont_write_bytecode=True
ROOT=Path('/private/tmp/gomap-cow-execution-o2nauuzm')
REPO=ROOT/'c2'; ART=ROOT/'artifacts/c2'; INPUT=ART/'pr5069-440-ci-raw-path'
HEAD='4403776d10b6ac82eb9e1839f437f59a78058816'; BASE='edbd68d34b0fdf7152a498b87d373576de029721'
NAMES=[('get_versioned','BenchmarkGetVersioned','db'),('batch_write','BenchmarkConditionalTxnBaselineBatchWrite','db'),('snapshot_seek','BenchmarkSnapshotIteratorSeekNext/keys=1024/snapshot_seek','treedb'),('repeated_iterator','BenchmarkRepeatedIterator','caching'),('durable_sync','BenchmarkPublicCommandWALDurableTinyBatchWriteSync/placement=inline/shape=dirty_batch/ops=1','treedb')]
files={'baseline.txt','candidate.txt','binary-sha256.txt','environment.txt','processes.txt','summary.json','summary.md'}
assert {p.name for p in INPUT.iterdir()}==files
assert all(p.is_file() and not p.is_symlink() for p in INPUT.iterdir())
def sha(data):return hashlib.sha256(data).hexdigest()
summary=json.loads((INPUT/'summary.json').read_text());env={}
for line in (INPUT/'environment.txt').read_text().splitlines():
 if re.match(r'^[a-z_]+=',line):key,val=line.split('=',1);env[key]=val
assert env['candidate_sha']==HEAD and env['baseline_sha']==BASE
expected_env={'runs':'8','benchtime':'2s','batch_write_benchtime':'1000x','cpuset':'0','gomaxprocs':'1','max_regression_percent':'5','max_bytes_regression_percent':'1','max_bytes_regression_absolute':'64'}
assert all(env[k]==v for k,v in expected_env.items())
for key,val in [('expected_samples',8),('max_regression_percent',5),('max_bytes_regression_percent',1),('max_bytes_regression_absolute',64)]:
 assert summary[key]==val
 first=json.loads((ART/'hosted-raw-path-first-failure/summary.json').read_text());assert first[key]==val
assert summary['candidate_sha']==HEAD and summary['baseline_sha']==BASE
pattern=re.compile(r'^(Benchmark\S+)\s+(\d+)\s+([0-9.]+)\s+ns/op\s+(.*)$')
rows={}
for revision in ['baseline','candidate']:
 rows[revision]={name:[] for _,name,_ in NAMES}
 text=(INPUT/(revision+'.txt')).read_text()
 for number,line in enumerate(text.splitlines(),1):
  if not line.startswith('Benchmark'):continue
  m=pattern.fullmatch(line.strip());assert m,(revision,number,line)
  name,iterations,ns,tail=m.groups();assert name in rows[revision]
  tokens=tail.split();assert len(tokens)%2==0
  metrics={}
  for value,unit in zip(tokens[::2],tokens[1::2]):
   assert unit not in metrics;metrics[unit]=float(value);assert math.isfinite(metrics[unit])
  assert 'B/op' in metrics and 'allocs/op' in metrics
  vals=[float(ns),metrics['B/op'],metrics['allocs/op']];assert int(iterations)>0 and all(math.isfinite(v) and v>=0 for v in vals) and vals[0]>0
  rows[revision][name].append({'iterations':int(iterations),'ns_per_op':vals[0],'bytes_per_op':vals[1],'allocs_per_op':vals[2],'line':number})
 assert all(len(v)==8 for v in rows[revision].values())
 assert text.splitlines().count('PASS')==40
markers=[]
marker=re.compile(r'^--- before revision=(baseline|candidate) sample=(\d+) group=(\w+) at (\S+) ---$')
for line in (INPUT/'processes.txt').read_text().splitlines():
 if not line.startswith('--- before'):continue
 m=marker.fullmatch(line);assert m,line
 revision,sample,group,at=m.groups();markers.append({'revision':revision,'sample':int(sample),'group':group,'at':at})
expected=[(rev,n,g) for n in range(1,9) for g,_,_ in NAMES for rev in (['baseline','candidate'] if n%2 else ['candidate','baseline'])]
assert [(r['revision'],r['sample'],r['group']) for r in markers]==expected
reported_digests={}
for line in (INPUT/'binary-sha256.txt').read_text().splitlines():
 m=re.fullmatch(r'([a-f0-9]{64})  (baseline|candidate)-(db|caching|treedb)\.test',line);assert m,line
 digest,rev,pkg=m.groups();assert (pkg,rev) not in reported_digests;reported_digests[pkg,rev]=digest
assert len(reported_digests)==6 and len(set(reported_digests.values()))==6
for (pkg,rev),digest in reported_digests.items():assert summary['binary_digests'][pkg][rev]==digest
results=[]
for group,name,pkg in NAMES:
 base=rows['baseline'][name];head=rows['candidate'][name]
 med=lambda a:{key:statistics.median(row[key] for row in a) for key in ['ns_per_op','bytes_per_op','allocs_per_op']}
 bm,hm=med(base),med(head)
 paired=[(h['ns_per_op']/b['ns_per_op']-1)*100 for b,h in zip(base,head)]
 ns=(hm['ns_per_op']/bm['ns_per_op']-1)*100; pair=statistics.median(paired)
 delta=hm['bytes_per_op']-bm['bytes_per_op'];tol=min(bm['bytes_per_op']*.01,64)
 computed={'benchmark':name,'baseline':bm,'candidate':hm,'ns_delta_percent':ns,'paired_ns_delta_percent':pair,'bytes_delta':delta,'bytes_tolerance':tol,'timing_pass':pair<=5,'bytes_pass':delta<=tol,'allocs_pass':hm['allocs_per_op']<=bm['allocs_per_op']}
 computed['measurement_pass']=all(computed[k] for k in ['timing_pass','bytes_pass','allocs_pass'])
 original=next(r for r in summary['results'] if r['benchmark']==name)
 for k,v in computed.items():assert original[k]==v,(name,k,original[k],v)
 assert original['binary_equivalent'] is False and original['acceptance_verdict']=='PASS' and original['binary_package']==pkg
 results.append({**computed,'paired_ns_delta_percent_samples':paired,'baseline_samples':base,'candidate_samples':head,'binary_package':pkg,'reported_binary_digests':{'baseline':reported_digests[pkg,'baseline'],'candidate':reported_digests[pkg,'candidate']},'no_binary_equivalence_waiver':True})
assert all(r['measurement_pass'] for r in results) and summary['verdict']=='PASS' and summary['measurement_pass'] is True
# Reconcile the independent parser/arithmetic with current analyzer functions only;
# do not pretend that reported digests replace actual executable hashing.
checker=REPO/'.github/scripts/check_mvcc_raw_path_gate.py'
spec=importlib.util.spec_from_file_location('raw_gate_checker',checker);mod=importlib.util.module_from_spec(spec);sys.modules[spec.name]=mod;spec.loader.exec_module(mod)
passed,rerun=mod.evaluate(mod.parse_benchmarks(INPUT/'baseline.txt',8),mod.parse_benchmarks(INPUT/'candidate.txt',8),5,1,64)
assert passed and rerun==[{k:r[k] for k in rerun[0]} for r in results]
source={}
for name in ['.github/scripts/check_mvcc_raw_path_gate.py','scripts/mvcc_raw_path_gate.sh','scripts/mvcc_candidate_checkout_guard.sh','.github/workflows/treedb-tests.yml','TreeDB/command_wal_public_test.go','TreeDB/snapshot_seek_iterator_bench_test.go','TreeDB/caching/bench_test.go','TreeDB/db/conditional_kv_contract_bench_test.go']:
 actual=(REPO/name).read_bytes();head=subprocess.check_output(['git','show',HEAD+':'+name],cwd=REPO);base=subprocess.check_output(['git','show',BASE+':'+name],cwd=REPO)
 assert actual==head
 source[name]={'candidate_sha256':sha(head),'baseline_sha256':sha(base),'byte_identical_to_baseline':base==head}
assert source['.github/scripts/check_mvcc_raw_path_gate.py']['byte_identical_to_baseline']
assert source['scripts/mvcc_raw_path_gate.sh']['byte_identical_to_baseline']
job=json.loads(subprocess.check_output(['gh','api','repos/snissn/gomap/actions/jobs/112196498298','--jq','{id,run_id,head_sha,name,status,conclusion,started_at,completed_at,html_url}'],cwd=REPO,text=True))
assert job['head_sha']==HEAD and job['status']=='completed' and job['conclusion']=='success'
payload={'reanalysis_script_sha256':sha(Path(__file__).read_bytes()),'parser_calibration':'Initial independent parser rejected actual custom metric pairs in durable benchmark rows before emitting a verdict; corrected parser validates all metric pairs and extracts the exact gate fields. No benchmark values changed.','decision':'ACCEPT','scope':'current hosted strict raw-path gate measurement arithmetic and retained references; no new benchmark execution','candidate_sha':HEAD,'baseline_sha':BASE,'artifact_id':11401980581,'job':job,'total_actual_rows':80,'samples_per_revision_per_group':8,'groups':5,'balanced_AB_BA_pairs_verified':40,'actual_order_markers_only_no_process_argv':markers,'thresholds':{'median_paired_ns_delta_percent_max':5,'median_bytes_delta_max':'min(base_median_B_per_op * 0.01, 64)','median_allocations_must_not_increase':True,'unchanged_from_original_failed_gate':True},'measurement_pass':True,'binary_equivalence_waiver_used':False,'reported_executable_references':{'files':{rev+'-'+pkg+'.test':digest for (pkg,rev),digest in reported_digests.items()},'independently_rehashed_executables':False,'limitation':'The six actual executables were not transferred; digest references agree between retained manifest and summary, but actual executable bytes cannot be independently rehashed.'},'source_definitions':source,'private_input_bindings':{p.name:sha(p.read_bytes()) for p in sorted(INPUT.iterdir())},'historical_974_cost_scope':'Separate opt-in experimental diagnostic; not evidence of this current hosted gate','results':results}
(ART/'hosted-440-independent-review.json').write_text(json.dumps(payload,indent=2,sort_keys=True)+'\n')
lines=['# Exact440 hosted raw-path independent review','', '**ACCEPT — all five measurement rows pass the original unwaived thresholds.**','',f'Candidate `{HEAD}` versus base `{BASE}`; hosted job112196498298 completed successfully at '+job['completed_at']+'. Artifact11401980581 contains40 baseline and40 candidate rows; all40 AB/BA benchmark-group pairs match collector order, eight samples/revision/group.','', 'Thresholds remain paired-median timing ≤5%, median allocation count cannot increase, median bytes increase ≤min(1% baseline,64B), strict zero-byte baseline. Current collector/analyzer bytes match the base. Their thresholds match the original failed8a159 gate. All six reported binary digests differ; equivalence acceptance was neither needed nor used.','', '| Benchmark | Base/head ns/op medians | Paired timing delta | Base/head B/op | Base/head allocs/op |','|---|---:|---:|---:|---:|']
for r in results:
 b,h=r['baseline'],r['candidate'];lines.append(f"| {r['benchmark']} | {b['ns_per_op']:g} / {h['ns_per_op']:g} | {r['paired_ns_delta_percent']:+.6f}% | {b['bytes_per_op']:g} / {h['bytes_per_op']:g} | {b['allocs_per_op']:g} / {h['allocs_per_op']:g} |")
lines+=['','Independent parser/statistics reconstruction matches every original numeric/pass field and current analyzer functions. JSON retains all actual sample values, spread through paired sample deltas, private input hashes, source bindings and sanitized order markers. Go1.26.8 linux/amd64, CPU0/GOMAXPROCS1, Ubuntu24/EPYC9V45 are retained by environment.txt; batch baseline-write1000x, remaining groups2s.','', 'Limitation: six executable digest **references** agree between binary-sha256.txt and summary.json. Actual executables were not transferred and cannot be independently rehashed here. No binaries were invented or rebuilt. Unrelated process argv remains private: processes.txt was read only to verify order and hash-bound; no process rows are copied into this review. Historical974 experimental COW cost evidence remains separately scoped and is not substituted for this current gate. No source edit, new Go validation, workflow rerun, or publication action occurred.']
(ART/'hosted-440-independent-review.md').write_text('\n'.join(lines)+'\n')
print(json.dumps({'decision':payload['decision'],'rows':80,'paired_ns_deltas':{r['benchmark']:r['paired_ns_delta_percent'] for r in results},'outputs':{name:sha((ART/name).read_bytes()) for name in ['hosted-440-independent-review.json','hosted-440-independent-review.md']}},indent=2))
