from pathlib import Path
import hashlib,importlib.util,json,math,statistics,subprocess
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');O=D/'performance-185/5027d-m0-hosted';H='5027dc559f83dee04317910169c48d277fcce72f';P=O/'packet'/('treedb-vacuum-m0-'+H)
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
script=O/'exact-summarizer.py';script.write_bytes(subprocess.check_output(['git','show',H+':scripts/treedb_vacuum_m0_summarize.py'],cwd=D/'ordinary-repair'))
spec=importlib.util.spec_from_file_location('m0_exact',script);mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
r=json.loads((P/'results.json').read_text());assert r['environment']['git_sha']==H and r['environment']['dirty_state']=='clean'
for mode in ['legacy','public']:
 paths=sorted((P/'raw').glob(mode+'-*.txt'));assert len(paths)==10
 computed=mod.summarize(paths);assert computed.keys()==r['metrics'][mode].keys()
 for metric,v in computed.items():
  for k,value in v.items():
   original=r['metrics'][mode][metric][k]
   assert value==original if k=='samples' else math.isclose(value,original,rel_tol=1e-14,abs_tol=1e-15)
 raw=[]
 for p in paths:
  lines=p.read_text().splitlines();assert 'PASS' in lines
  line=[x for x in lines if x.startswith('Benchmark')];assert len(line)==1
  v=line[0].split();assert v[1]=='1';raw.append({u:float(x) for x,u in zip(v[2::2],v[3::2])})
 for metric,v in r['metrics'][mode].items():
  vals=[x[metric] for x in raw];assert vals==v['samples']
  assert statistics.fmean(vals)==v['mean'] and statistics.median(vals)==v['median']
  assert math.isclose((statistics.stdev(vals)/statistics.fmean(vals) if statistics.fmean(vals) else 0),v['cv'],rel_tol=1e-14,abs_tol=1e-15)
assert mod.evaluate_gates(r['metrics']['legacy'],r['metrics']['public'])==r['gates'] and all(r['gates'].values())
assert mod.classify_public_status(r['metrics']['public'])==r['public_status']=='production-index-vacuum-available'
f=r['fixture']['fixture'];assert f['logical_digest']=='42a8a8ef9648dced3ae61f8caa35e921058c17d73265ffdf2db47b93a9d9b599';assert (f['index_bytes'],f['offline_index_bytes'])==(917504,131072)
receipt={'head':H,'status':'ORIGINAL_M0_GATES_RECOMPUTED_PASS','summarizer_sha256':sha(script),'rows':20,'all_raw_statistics_and_original_gates_equal':True,'arithmetic_tolerance':'Original samples/mean/median exact; CV comparison permits 1e-14 relative / 1e-15 absolute rounding, no gate tolerance change','cv':{k:r['metrics']['legacy'][k]['cv'] for k in mod.REQUIRED_CV_METRICS},'public_status':r['public_status'],'fixture':f,'raw_hashes':{str(p.relative_to(P)):sha(p) for p in sorted(P.rglob('*')) if p.is_file()},'limitations':['Same-head stability and public exposure only; not matched revision cost qualification','Public p99 CV is reported but is not an original stability gate','No sampled host process/CPU interference telemetry in this maintained M0 packet']}
(O/'root-verification.json').write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps({k:receipt[k] for k in ['head','status','rows','cv','public_status']}))
