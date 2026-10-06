"""Pure aggregation regression controls; no imports, scoring or campaign replay."""
if not __debug__: raise RuntimeError('ordinary Python required; assertions must run')
import ast, hashlib, json, math, sys
from pathlib import Path
sys.dont_write_bytecode=True

def recall_aggregation_controls(root):
 source=(root/'shared_read.py').read_text();tree=ast.parse(source)
 functions={n.name:n for n in tree.body if isinstance(n,ast.FunctionDef)}
 def need(ok,label):
  if not ok:raise AssertionError(label)
 ns={'need':need,'math':math}
 exec(compile(ast.Module(body=[functions[n] for n in ('numeric','producer_recall_mean')],type_ignores=[]),'actual-recall-reduction-functions','exec'),ns)
 checks=[]
 def good(label,fn):fn();checks.append(label)
 def bad(label,fn,expected):
  try:fn()
  except AssertionError as error:
   assert str(error)==expected,(label,str(error));checks.append(label);return
  raise AssertionError('invalid recall accepted '+label)
 short=[.9 if i in (0,3,5,6,13,14,15) else 1.0 for i in range(16)]
 # Synthetic positions reproduce the retained regression's order, not its provenance.
 long=[.9 if i in (0,3665,5664,6000,13968,14305,15504) else 1.0 for i in range(16538)]
 for label,values,want in [('paired16',short,'0x1.e99999999999bp-1'),('measured16538',long,'0x1.fffa73bfe0b19p-1')]:
  good('recall_sequential_'+label,lambda values=values,want=want:need(ns['producer_recall_mean'](values).hex()==want,'fixed producer binary64 mean'))
 good('recall_order_is_preserved',lambda:need(ns['producer_recall_mean'](long)!=ns['producer_recall_mean'](sorted(long)),'ledger order changes low bits'))
 good('recall_unit_default_unchanged',lambda:need(ns['producer_recall_mean']([1.0]*16)==1.0,'unit recall'))
 gates={}
 for function,values,variable,reported,floor,label in [
  ('paired',short,'values','rec','minrec','paired configured mean recall threshold'),
  ('verifier',long,'recalls','r','minimum','configured measured mean recall threshold')]:
  body=functions[function].body
  start=next(i for i,n in enumerate(body) if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='mean' for t in n.targets))
  nodes=body[start:start+3];assert len(nodes)==3 and isinstance(nodes[0].value,ast.Call) and nodes[0].value.func.id=='producer_recall_mean'
  code=compile(ast.Module(body=nodes,type_ignores=[]),'actual-'+function+'-aggregate-gate','exec')
  mean=ns['producer_recall_mean'](values)
  def run(value=mean,minimum=0.0,code=code,values=values,variable=variable,reported=reported,floor=floor):
   env=dict(ns);env.update({variable:values,reported:{'MeanRecallAt10':value},floor:minimum});exec(code,env)
  good('recall_actual_'+function+'_mean_pass',run)
  bad('recall_actual_'+function+'_wrong_mean',lambda run=run,mean=mean:run(math.nextafter(mean,math.inf)),label)
  bad('recall_actual_'+function+'_threshold_unchanged',lambda run=run,mean=mean:run(minimum=math.nextafter(mean,math.inf)),label)
  numeric_label=('paired' if function=='paired' else 'measured')+' mean recall finite number'
  for value in (float('nan'),float('inf'),True):bad('recall_actual_'+function+'_nonfinite_or_bool_'+str(value),lambda value=value,run=run:run(value),numeric_label)
  gates[function]=hashlib.sha256(ast.unparse(ast.Module(body=nodes,type_ignores=[])).encode()).hexdigest()
 for function,object_name,finite_label,equality_label in [('paired','q','paired recall','paired exact recall'),('verifier','x','window recall','independent single-prefix exact FP32/recall')]:
  expressions=[n for n in ast.walk(functions[function]) if isinstance(n,ast.Expr) and isinstance(n.value,ast.Call) and any(isinstance(a,ast.Constant) and a.value in (finite_label,equality_label) for a in n.value.args)]
  assert len(expressions)==2
  code=compile(ast.Module(body=expressions,type_ignores=[]),'actual-'+function+'-per-attempt-gates','exec')
  def run(value=.9,code=code,object_name=object_name):
   env=dict(ns);env.update({object_name:{'RecallAt10':value},'value':.9});exec(code,env)
  good('recall_actual_'+function+'_per_attempt_pass',run)
  bad('recall_actual_'+function+'_wrong_per_attempt',lambda run=run:run(.8),equality_label)
  for value in (float('nan'),float('inf'),True):bad('recall_actual_'+function+'_per_attempt_nonfinite_or_bool_'+str(value),lambda value=value,run=run:run(value),finite_label+' finite number')
 return {'state':'PURE_RECALL_AGGREGATION_CONTROLS_PASS_NOT_CAMPAIGN_ACCEPTANCE','checks':checks,'count':len(checks),'shared_read_sha256':hashlib.sha256(source.encode()).hexdigest(),'actual_gate_source_sha256':gates,'runtime_started':False,'native_scoring_executed':False,'network_calls':0}

if __name__=='__main__':
 print(json.dumps(recall_aggregation_controls(Path(__file__).parent),indent=2))
