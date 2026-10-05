from source_paths import source_path, isolate_paths
"""One fresh canonical-score C1 bootstrap; close only its recorded voter CIDs, never retry writes."""
import hashlib,json,pathlib,re,shlex,subprocess,time
CAMPAIGN='rf4trial24mixedchangingc1'
IMAGES=None
PLAN_SHA='94e1506fc3f5648df9082fd44ddeebf69eb6550d109afa85a2ac22e25be6536b'

def owned(x,node,cid):
 assert x['Id']==cid and x['Name']=='/treedb-4250-'+CAMPAIGN+'-'+node
 assert x['Image']==IMAGES['192.168.0.'+('111' if node in ['node-a','node-b'] else '185')]
 assert x['Config']['Labels']['treedb.fixed-cluster.run']==CAMPAIGN
 base='/home/mikers/gomap-4250-twohost-'+CAMPAIGN+'/'+node
 mounts={m['Destination']:m['Source'] for m in x['Mounts']}
 assert mounts.get('/data')==base+'/data' and mounts.get('/raft')==base+'/raft'
 h=x['HostConfig'];assert h['Memory']==h['MemorySwap']==2147483648 and h['NanoCpus']==2000000000 and h['NetworkMode']=='host' and h['RestartPolicy']['Name']=='no'
def check():
 cid='a'*64;node='node-a';base='/home/mikers/gomap-4250-twohost-'+CAMPAIGN+'/'+node
 x={'Id':cid,'Name':'/treedb-4250-'+CAMPAIGN+'-'+node,'Image':IMAGES['192.168.0.111'],'Config':{'Labels':{'treedb.fixed-cluster.run':CAMPAIGN}},'HostConfig':{'Memory':2147483648,'MemorySwap':2147483648,'NanoCpus':2000000000,'NetworkMode':'host','RestartPolicy':{'Name':'no'}},'Mounts':[{'Destination':'/data','Source':base+'/data'},{'Destination':'/raft','Source':base+'/raft'}]}
 owned(x,node,cid)
 x['Mounts'][0]['Source']='/unrelated/data'
 try:owned(x,node,cid)
 except AssertionError:return
 raise AssertionError('foreign store accepted')
def main():
 global ROOT,OUTPUT,IMAGES
 assert __debug__
 import argparse,importlib.util
 q=argparse.ArgumentParser()
 for k in ('pins','plan','preflight','preflight-sha256','out'):q.add_argument('--'+k,required=True)
 args=q.parse_args()
 module=pathlib.Path(source_path('/tmp/gomap-5021-sustained-consumers-provisional-root-v1/gomap-4997-4998-trial24-bootstrap-plan-prepare-root-v1.py'))
 assert hashlib.sha256(module.read_bytes()).hexdigest()==PLAN_SHA
 spec=importlib.util.spec_from_file_location('trial24_plan',module);pm=importlib.util.module_from_spec(spec);spec.loader.exec_module(pm)
 a,m,expected,dataset,configs=pm.context(args.pins);IMAGES=expected['image'];check()
 ROOT=pathlib.Path(args.out);OUTPUT=pathlib.Path(a['artifact_volume'])/'bootstrap-root-v1'
 assert ROOT.is_absolute() and ROOT.name.startswith(pm.PREFIX+'-bootstrap-lifecycle-root-v') and not ROOT.exists() and not OUTPUT.exists()
 isolate_paths([ROOT,OUTPUT],pm.protected_inputs(a,args.pins)+[args.plan,args.preflight])
 launcher=pathlib.Path(source_path('/tmp/gomap-4956-5fa-fixed-cluster.py'));assert hashlib.sha256(launcher.read_bytes()).hexdigest()=='21c3f9204489ae7179341772b4b856de2b9280acacfbe5ad4deea52da9750616'
 pre=pathlib.Path(args.preflight)/'proof.json';proofraw=pm.read(pre,args.preflight_sha256);proof=json.loads(proofraw);assert proof['state']=='FRESH_BOOTSTRAP_PRECOLLECTION_SOURCE_AND_INFRASTRUCTURE_ACCEPTED'
 manifest=pathlib.Path(args.plan)/'manifest.json';assert hashlib.sha256(manifest.read_bytes()).hexdigest()==proof['manifest_sha256']
 plan=pathlib.Path(args.plan)/'plan-preparation.json';assert hashlib.sha256(plan.read_bytes()).hexdigest()==proof['plan_preparation_sha256']
 assert json.loads(manifest.read_bytes())==expected and proof['runtime_source_head']==a['head'] and proof['runtime_source_tree']==a['tree']
 assert proof['pins_sha256']==hashlib.sha256(pm.read(args.pins)).hexdigest()
 frozen=json.loads(plan.read_bytes())
 for name,h in frozen['config_sha256'].items():assert hashlib.sha256(pathlib.Path(name).read_bytes()).hexdigest()==h
 isolate_paths([ROOT,OUTPUT],pm.protected_inputs(a,args.pins)+[args.plan,args.preflight]+list(pm.preparation_paths(frozen).values()))
 pm.preflight_tuple(proof,frozen,a)
 assert proof['build_receipt_sha256']==a['build_sha256'] and proof['daemon_sha256']==expected['binary_sha256']
 ROOT.mkdir()
 (ROOT/'precollection-proof.json').write_bytes(proofraw);(ROOT/'source.py').write_bytes(pathlib.Path(__file__).read_bytes())
 argv=['python3',str(launcher),'--manifest',str(manifest),'--dataset',a['dataset'],'--run-id',CAMPAIGN,'--operation-timeout-seconds','600','--output',str(OUTPUT),'--execute']
 start=time.time();code=None;stopped=[];errors=[];qualification_valid=False
 def remote(label,host,args,timeout=90):
  cmd=['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10','mikers@'+host,shlex.join(args)];beg=time.time()
  r=subprocess.run(cmd,capture_output=True,text=True,timeout=timeout);(ROOT/(label+'.json')).write_text(json.dumps({'argv':cmd,'exit_code':r.returncode,'stdout':r.stdout,'stderr':r.stderr,'started_unix':beg,'finished_unix':time.time()},indent=2)+'\n');assert r.returncode==0,(label,r.stderr);return r.stdout
 try:
  with (ROOT/'launcher.stdout').open('w') as out,(ROOT/'launcher.stderr').open('w') as err:
   code=subprocess.run(argv,stdout=out,stderr=err).returncode
  if code==0:
   receipts=sorted(OUTPUT.glob('[0-9][0-9]-*.json'));assert len(receipts)==44
   for i,f in enumerate(receipts,1):
    r=json.loads(f.read_bytes());assert f.name.startswith('%02d-'%i) and r['exit_code']==0 and not r.get('timed_out',False)
   assert json.loads((OUTPUT/'result.json').read_bytes())['status']=='PASS'
   init=json.loads(json.loads((OUTPUT/'19-initialize.json').read_bytes())['stdout']);qual=json.loads(json.loads((OUTPUT/'36-qualify.json').read_bytes())['stdout'])
   assert init['Stage']=='prepared-restart-required' and init['Prepare']['Command']['SourceRowCount']==qual['Prepare']['Command']['SourceRowCount']==10003
   assert init['Dataset']['SourceRows']==qual['Dataset']['SourceRows']==10003
   assert qual['Insert']['ProductionConsensus'] is True and qual['Insert']['CommitIndex']>0 and qual['Insert']['VisibleID']==qual['Retry']['VisibleID']
   assert qual['Insert']['VisibleID'].startswith(CAMPAIGN+'/') and qual['After']['Neighbors'][0]['ID']==qual['Insert']['VisibleID']
   assert len(qual['Readiness'])==4 and {x['NodeID'] for x in qual['Readiness']}=={'node-a','node-b','node-c','node-d'}
   assert all(x['VectorPhase']=='active' and x['Live'] is True and x['Ready'] is True and x['Draining'] is False and len(x['Groups'])==1 and x['Groups'][0]['Ready'] is True and x['Groups'][0]['LocalAppliedIndex']>=x['Groups'][0]['RequiredAppliedIndex'] for x in qual['Readiness'])
   qualification_valid=True
 finally:
  (ROOT/'launcher.json').write_text(json.dumps({'argv':argv,'exit_code':code,'started_unix':start,'finished_unix':time.time()},indent=2)+'\n')
  for node in ['node-a','node-b','node-c','node-d']:
   files=list(OUTPUT.glob('*-serve-'+node+'.json')) if OUTPUT.exists() else []
   if not files:continue
   try:
    assert len(files)==1;j=json.loads(files[0].read_bytes());cid=j['stdout'].strip();assert j['exit_code']==0 and re.fullmatch('[0-9a-f]{64}',cid)
    host='192.168.0.'+('111' if node in ['node-a','node-b'] else '185')
    x=json.loads(remote('before-stop-'+node,host,['docker','inspect',cid]))[0];owned(x,node,cid)
    if x['State']['Running']:remote('stop-'+node,host,['docker','stop','-t','60',cid])
    x=json.loads(remote('stopped-'+node,host,['docker','inspect',cid]))[0];owned(x,node,cid)
    assert not x['State']['Running'] and not x['State']['OOMKilled'] and x['State']['ExitCode']==0
    stopped.append({'node':node,'host':host,'container_id':cid,'stopped_clean':True});print('STOPPED',node,flush=True)
   except Exception as e:errors.append({'node':node,'error':repr(e)})
  result={'precollection_proof_sha256':hashlib.sha256(proofraw).hexdigest(),'state':'PASS_FRESH_BOOTSTRAP_CLOSED' if code==0 and qualification_valid and len(stopped)==4 and not errors else 'FAIL_RETAIN_NO_WORKLOAD_RETRY','launcher_exit':code,'all44receipts_verified':qualification_valid,'stopped':stopped,'errors':errors,'stores_retained':True,'no_workload_retry':True}
  (ROOT/'result.json').write_text(json.dumps(result,indent=2)+'\n');print(result['state'],flush=True)
 assert result['state']=='PASS_FRESH_BOOTSTRAP_CLOSED',result
if __name__=='__main__':main()
