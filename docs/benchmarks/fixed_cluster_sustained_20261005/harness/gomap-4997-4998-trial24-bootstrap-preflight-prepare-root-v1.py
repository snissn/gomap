from source_paths import source_path, isolate_paths
"""Root-owned Trial24 read-only bootstrap preflight; inert on import."""
import argparse,hashlib,importlib.util,json,pathlib,shlex,subprocess,time,os
PLAN_MODULE=source_path('/tmp/gomap-5021-sustained-consumers-provisional-root-v1/gomap-4997-4998-trial24-bootstrap-plan-prepare-root-v1.py')
PLAN_SHA='d6cc1986fab0d10e9ef8f9e9a474ad62f98b7c572c23540f5925e71ba2366403'
def main():
 assert __debug__
 q=argparse.ArgumentParser()
 for k in ('pins','plan','out'):q.add_argument('--'+k,required=True)
 args=q.parse_args()
 assert hashlib.sha256(pathlib.Path(PLAN_MODULE).read_bytes()).hexdigest()==PLAN_SHA
 spec=importlib.util.spec_from_file_location('trial24_plan',PLAN_MODULE);pm=importlib.util.module_from_spec(spec);spec.loader.exec_module(pm)
 a,m,manifest,dataset,configs=pm.context(args.pins)
 planroot=pathlib.Path(args.plan);assert json.loads(pm.read(planroot/'manifest.json'))==manifest
 frozen=json.loads(pm.read(planroot/'plan-preparation.json'));assert frozen['config_sha256']==configs and frozen['source_head']==a['head'] and frozen['source_tree']==a['tree']
 assert frozen['manifest_sha256']==hashlib.sha256(pm.read(planroot/'manifest.json')).hexdigest()
 assert frozen['pins_sha256']==hashlib.sha256(pm.read(args.pins)).hexdigest()
 pm.preparation_tuple(frozen,a)
 assert frozen['product']==a['product']
 wt=a['source_worktree']
 assert subprocess.check_output(['git','-C',wt,'rev-parse','HEAD'],text=True).strip()==a['head']
 assert subprocess.check_output(['git','-C',wt,'rev-parse','HEAD^{tree}'],text=True).strip()==a['tree']
 assert not subprocess.check_output(['git','-C',wt,'status','--porcelain'],text=True).strip()
 blob=subprocess.check_output(['git','-C',wt,'show',a['head']+':TreeDB/nativewire/fixed_peer_vector_initialize_client_v1.go'])
 assert hashlib.sha256(blob).hexdigest()==a['qualification_source_sha256']
 volume=pathlib.Path(a['artifact_volume']);assert volume.is_absolute() and volume.name==pm.PREFIX
 v=os.statvfs(volume);assert v.f_bavail*v.f_frsize>=2*1024**3
 assert not (volume/'bootstrap-root-v1').exists() and not (volume/'mixed-window-root-v1').exists()
 p=pathlib.Path(args.out);assert p.is_absolute() and p.name.startswith(pm.PREFIX+'-bootstrap-precollection-root-v') and not p.exists()
 isolate_paths([p,volume/'bootstrap-root-v1'],pm.protected_inputs(a,args.pins)+[planroot]+list(pm.preparation_paths(frozen).values()))
 p.mkdir()
 remote=r'''
import json,pathlib,subprocess,sys,time,os
host=sys.argv[1];image=sys.argv[2];earliest=float(sys.argv[3]);latest=float(sys.argv[4]);assert earliest<=time.time()<latest-86400;root='/home/mikers/gomap-4250-twohost-rf4trial24mixedchangingc1'
assert not pathlib.Path(root).exists()
disk=os.statvfs('/home/mikers');assert disk.f_bavail*disk.f_frsize>=50*1024**3
mem={l.split(':')[0]:int(l.split()[1])*1024 for l in pathlib.Path('/proc/meminfo').read_text().splitlines() if ':' in l};assert mem['MemAvailable']>=16*1024**3 and os.getloadavg()[0]<4
assert not subprocess.run(['pgrep','-af','(^|/)(go|compile|link|[^ ]+\\.test)( |$)'],capture_output=True,text=True).stdout.strip()
ss=subprocess.check_output(['ss','-ltnH'],text=True)
ports={19101,19102,19201,19202,19301,19302,19401,19402,19501,19502} if host.endswith('111') else {19103,19104,19203,19204,19303,19304,19403,19404,19503,19504}
for line in ss.splitlines():
 parts=line.split();port=int(parts[3].rsplit(':',1)[1]);assert port not in ports,('fixture port busy',port)
active=subprocess.check_output(['docker','ps','--format','{{.Names}}'],text=True).splitlines();assert not any(n.startswith('treedb-4250-') for n in active)
inspect=json.loads(subprocess.check_output(['docker','image','inspect',image],text=True))[0];assert inspect['Id']==image
for n in ('a','b','c','d'):
 name='treedb-4250-rf4trial24mixedchangingc1-node-'+n;r=subprocess.run(['docker','inspect',name],capture_output=True,text=True);assert r.returncode and 'no such' in r.stderr.lower()
print(json.dumps({'host':host,'image':image,'root_absent':True,'names_absent':True,'ports_free':sorted(ports),'active_fixture_daemons':0,'no_host_mutations':True,'checked_unix':time.time(),'credential_time_checked':True,'credential_not_before_unix':earliest,'credential_not_after_unix':latest}))
 '''
 receipts={}
 for host in m.HOSTS:
  argv=['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10','mikers@'+host,shlex.join(['python3','-c',remote,host,manifest['image'][host],str(a['credential_not_before_unix']),str(a['credential_not_after_unix'])])]
  start=time.time();r=subprocess.run(argv,capture_output=True,text=True,timeout=30)
  rec={'argv':argv,'exit':r.returncode,'stdout':r.stdout,'stderr':r.stderr,'started_unix':start,'finished_unix':time.time()};(p/(host+'.json')).write_text(json.dumps(rec,indent=2)+'\n');assert r.returncode==0,r.stderr;receipts[host]=json.loads(r.stdout)
 proof={'state':'FRESH_BOOTSTRAP_PRECOLLECTION_SOURCE_AND_INFRASTRUCTURE_ACCEPTED','runtime_source_head':a['head'],'runtime_source_tree':a['tree'],'build_source_head':a['head'],'daemon_sha256':manifest['binary_sha256'],'qualification_source_sha256':a['qualification_source_sha256'],'launcher_sha256':pm.LAUNCHER_SHA,'manifest_sha256':hashlib.sha256(pm.read(planroot/'manifest.json')).hexdigest(),'plan_preparation_sha256':hashlib.sha256(pm.read(planroot/'plan-preparation.json')).hexdigest(),'build_receipt_sha256':a['build_sha256'],'images_receipt_sha256':a['images_sha256'],'source_review_sha256':a['source_acceptance_sha256'],'source_inventory_sha256':'__ROOT_FROZEN_INVENTORY_SHA__','driver_sha256':a['product']['driver_sha256'],'server_images':a['product']['server_images'],'driver_image':a['product']['driver_image'],'dataset':dataset,'hosts':receipts,'credential_validation':a['credential_validation'],'root_executor':True,'no_workload_retry':True,'final_close_required':True,'bootstrap_only_not_mixed_qualification':True,'artifact_volume':str(volume),'pins_sha256':hashlib.sha256(pm.read(args.pins)).hexdigest()}
 (p/'proof.json').write_text(json.dumps(proof,indent=2)+'\n');print(proof['state'])
if __name__=='__main__':main()
