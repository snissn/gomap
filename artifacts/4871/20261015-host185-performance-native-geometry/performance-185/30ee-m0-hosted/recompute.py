import hashlib,importlib.util,json,math,subprocess,sys
from pathlib import Path
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015'); O=D/'performance-185/30ee-m0-hosted'; head='30ee954b974077b8968cdfb836e03058d9934ed1'
api=json.loads((O/'artifact-api.json').read_text()); assert api['workflow_run']['head_sha']==head
p=O/'checker-30ee.py'; assert p.read_bytes()==subprocess.check_output(['git','show',head+':scripts/treedb_vacuum_m0_summarize.py'],cwd=D/'ordinary-repair')
spec=importlib.util.spec_from_file_location('checker',p); m=importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
j=json.loads((O/'results.json').read_text()); assert j['environment']['git_sha']==head and j['environment']['dirty_state']=='clean'
metrics={k:m.summarize(sorted((O/'raw').glob(k+'-*.txt'))) for k in ['legacy','public']}
diffs=[]
for k,ms in metrics.items():
 for name,row in ms.items():
  assert row['samples']==j['metrics'][k][name]['samples']
  for stat in ['mean','median','cv']:
   a=row[stat]; b=j['metrics'][k][name][stat]; assert math.isclose(a,b,rel_tol=1e-14,abs_tol=0)
   if a!=b: diffs.append({'kind':k,'metric':name,'statistic':stat,'recomputed':a,'hosted':b})
gates=m.evaluate_gates(metrics['legacy'],metrics['public']); status=m.classify_public_status(metrics['public']);assert gates==j['gates'] and all(gates.values()) and status==j['public_status']
r={'head':head,'artifact':12000534609,'run':38793560840,'all_raw_samples_exactly_equal':True,'sample_order':'exact checker lexicographic raw filenames','aggregate_relative_tolerance':1e-14,'aggregate_rounding_differences':diffs,'python':sys.version,'gates':gates,'public_status':status,'checker_sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'files':{str(q.relative_to(O)):hashlib.sha256(q.read_bytes()).hexdigest() for q in sorted(O.rglob('*')) if q.is_file() and '__pycache__' not in q.parts and q.name!='root-verification.json'},'scope':'Exact current-head hosted M0 stability; no same-head rerun or final native qualification'}
(O/'root-verification.json').write_text(json.dumps(r,indent=2)+'\n')
print(json.dumps({'gates':gates,'status':status,'stability':{name:metrics['legacy'][name]['cv'] for name in m.REQUIRED_CV_METRICS}},indent=2))
