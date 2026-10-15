from pathlib import Path
import ast
import datetime
import difflib
import hashlib
import json
import subprocess
import sys
import yaml

D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015')
W=D/'maintenance-hosted-runner'; O=D/'maintenance-hosted-preparation'
BASE='5bca87ffe6e1bfba2d693264d2dc82936adc234f'; CAND='72e01efd767ef5d766f070e531fb53ea3f22674d'
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
def git(*args):return subprocess.check_output(['git',*args],cwd=W,text=True).strip()
def identity():
 files=git('ls-files','--cached','--others','--exclude-standard').splitlines()
 return {'head':git('rev-parse','HEAD'),'files':{f:sha(W/f) for f in files if (W/f).is_file()}}
before=identity();assert before['head']==CAND
(O/'before.json').write_text(json.dumps(before,indent=2,sort_keys=True))
changed=sorted(set(git('diff','--name-only').splitlines()+git('ls-files','--others','--exclude-standard').splitlines()))
expected={'.github/workflows/treedb-vacuum-m0.yml'}|{'.github/scripts/mvcc_maintenance_'+p for p in ['bootstrap.py','prepare.py','collect.py','test.py','README.md']}
assert set(changed)==expected,changed
argv=[sys.executable,'-B','.github/scripts/mvcc_maintenance_test.py']
with (O/'static-tests.raw.txt').open('w') as stream:
 done=subprocess.run(argv,cwd=W,stdout=stream,stderr=subprocess.STDOUT,timeout=30)
assert done.returncode==0
workflow=yaml.load((W/'.github/workflows/treedb-vacuum-m0.yml').read_text(),Loader=yaml.BaseLoader)
assert set(workflow['on'])=={'workflow_dispatch'} and workflow['on']['workflow_dispatch']==''
assert workflow['permissions']=={'contents':'read'} and set(workflow['jobs'])=={'diagnostic'}
job=workflow['jobs']['diagnostic'];assert job['runs-on']=='ubuntu-24.04'
assert job['timeout-minutes']=='120' and len(job['steps'])==6
assert job['steps'][-1]['if']=='always()'
assert job['steps'][-1]['with']['include-hidden-files']=='true'
assert not any('inputs' in str(x) for x in workflow['on'].values())
(O/'workflow-parsed.json').write_text(json.dumps(workflow,indent=2,sort_keys=True))
reviewed=D/'performance-185/maintenance-planning'
original_collect=(reviewed/'collect-02.py').read_text(); collect=(W/'.github/scripts/mvcc_maintenance_collect.py').read_text()
normalized=collect.replace("D = Path(os.environ['RUNNER_TEMP']) / 'mvcc-maintenance-72e01'","D = Path('/mnt/fast4tb/gomap-mvcc-4871-20261015')").replace("auth_path = D / 'authorization.json'","auth_path = D / 'performance-185/maintenance-planning/authorization-02.json'")
assert normalized==original_collect
functions={n.name:ast.dump(n,include_attributes=False) for n in ast.parse(collect).body if isinstance(n,ast.FunctionDef)}
oldfunctions={n.name:ast.dump(n,include_attributes=False) for n in ast.parse(original_collect).body if isinstance(n,ast.FunctionDef)}
assert all(functions[k]==oldfunctions[k] for k in ['schedule','parse','selftest','digest'])
prepare=(W/'.github/scripts/mvcc_maintenance_prepare.py').read_text(); original_prepare=(reviewed/'prepare-02.py').read_text()
oldtree=ast.parse(original_prepare);newtree=ast.parse(prepare)
def find(tree,key):
 for n in tree.body:
  if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id==key for t in n.targets):return ast.dump(n,include_attributes=False)
  if isinstance(n,ast.FunctionDef) and n.name==key:return ast.dump(n,include_attributes=False)
assert all(find(oldtree,k)==find(newtree,k) for k in ['heads','roots','sources','save','digest'])
oldenv=next(n for n in oldtree.body if isinstance(n,ast.Expr) and isinstance(n.value,ast.Call) and isinstance(n.value.func,ast.Attribute) and isinstance(n.value.func.value,ast.Name) and n.value.func.value.id=='env')
newenv=next(n for n in newtree.body if isinstance(n,ast.Expr) and isinstance(n.value,ast.Call) and isinstance(n.value.func,ast.Attribute) and isinstance(n.value.func.value,ast.Name) and n.value.func.value.id=='env')
assert ast.dump(oldenv,include_attributes=False)==ast.dump(newenv,include_attributes=False)
for short,original,adapted in [('collect',original_collect,collect),('prepare',original_prepare,prepare)]:
 (O/(short+'-vs-reviewed.diff')).write_text(''.join(difflib.unified_diff(original.splitlines(True),adapted.splitlines(True),fromfile=short+'-02.py',tofile='mvcc_maintenance_'+short+'.py')))
 dest=O/'reviewed-inputs';dest.mkdir(exist_ok=True);(dest/(short+'-02.py')).write_text(original)
equivalence={'collector_only_two_path_substitutions':True,'collector_schedule_parser_selftest_AST_equal':True,
 'preparer_sources_heads_roots_environment_AST_equal':True,'reviewed':{p.name:sha(p) for p in [reviewed/'collect-02.py',reviewed/'prepare-02.py']},
 'local_allowed_checks_only':True,'go_executions':0,'test_or_benchmark_body_executions':0,'network_or_remote_dispatches':0}
(O/'equivalence.json').write_text(json.dumps(equivalence,indent=2,sort_keys=True))
subprocess.run(['git','diff','--check'],cwd=W,check=True)
after=identity();assert before==after
(O/'after.json').write_text(json.dumps(after,indent=2,sort_keys=True))
patch=subprocess.check_output(['git','diff','--binary'],cwd=W).decode()
for path in changed:
 if path in git('ls-files').splitlines():continue
 patch+='diff --git a/'+path+' b/'+path+'\nnew file mode 100644\n'
 patch+=''.join(difflib.unified_diff([], (W/path).read_text().splitlines(True),fromfile='/dev/null',tofile='b/'+path))
(O/'final.patch').write_text(patch)
for path in changed:
 dest=O/'frozen-source'/path;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes((W/path).read_bytes())
freeze={'harness_parent_head':after['head'],'harness_commit':None,'harness_commit_pending_root':True,
 'product_heads':{'baseline':BASE,'candidate':CAND},'all_files':after['files'],
 'changed_files':{p:after['files'][p] for p in changed},'patch_sha256':sha(O/'final.patch'),
 'producer_files':{p:after['files'][p] for p in ['TreeDB/cow_public_cost_bench_test.go','TreeDB/db/vacuum_collection_bench_test.go','TreeDB/db/durable_root_bench_test.go','scripts/mvcc_primary_cost_gate.py']},
 'production_source_bytes_unchanged_from_candidate':True,'source_frozen':True,'source_runtime_lease_released':True}
(O/'freeze-final.json').write_text(json.dumps(freeze,indent=2,sort_keys=True))
(O/'changed-files-final.json').write_text(json.dumps({'paths':freeze['changed_files'],'ownership':'ordinary_completion harness-only','ordinary_product_worktree_untouched':True},indent=2))
(O/'validation-final.json').write_text(json.dumps({'created_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'argv':argv,'cwd':str(W),
 'exit':done.returncode,'raw_sha256':sha(O/'static-tests.raw.txt'),'source_before_after_equal':True,
 'seven_pure_check_groups_PASS':True,'independent_YAML_structure_PASS':True,'diff_check_PASS':True,
 'equivalence':equivalence,'remote_execution_authorized':False,'source_runtime_lease_released':True,
 'limits':['root exact harness commit + independent review + explicit dispatch grant pending','no local Go/build/test/benchmark/remote execution','ephemeral infrastructure differs; hosted quietness still required','no cost/CI/native acceptance claim']},indent=2))
print(json.dumps({'changed':changed,'all_file_count':len(after['files']),'patch_sha256':freeze['patch_sha256'],'source_before_after_equal':True,'local_checks':'PASS','Go_executions':0,'remote_dispatches':0},indent=2))
