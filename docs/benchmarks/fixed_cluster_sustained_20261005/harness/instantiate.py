if not __debug__: raise RuntimeError('ordinary Python required; assertions must run')
import sys
sys.dont_write_bytecode=True
from source_paths import isolate_paths, reviewer_identity, workload_source
"""Explicit final-pin source instantiation ONLY. Never admits or runs a campaign.
Uses a frozen, reviewed provisional packet and root's exact final source evidence;
writes only a fresh declared source-output directory using exclusive creation.
"""
import argparse,ast,hashlib,json,re
from pathlib import Path
ROOT=Path(__file__).parent
PACKET_SHA='5b018ac415c8a273d2c1cd6e5260989f69adbb81f4b29aeff84822a91935e83e'
def need(ok,label):
 if not ok:raise ValueError(label)
def sha(b):return hashlib.sha256(b).hexdigest()
def strict(raw):
 def pairs(rows):
  out={}
  for k,v in rows:need(k not in out,'duplicate JSON key');out[k]=v
  return out
 return json.loads(raw,object_pairs_hook=pairs,parse_constant=lambda s:(_ for _ in ()).throw(ValueError(s)))
def read(p,cap=16<<20):
 p=Path(p);need(p.is_absolute() and p.is_file() and not p.is_symlink() and p.stat().st_size<=cap,'bounded regular absolute file')
 with p.open('rb') as f:b=f.read(cap+1)
 need(len(b)<=cap,'bounded bytes');return b
def digest(x,n=64):return isinstance(x,str) and x!='0'*n and re.fullmatch('[0-9a-f]{'+str(n)+'}',x) is not None
def product_acceptance(review,head,tree,inventory):
 need(review.get('outcome')=='ACCEPT' and review.get('candidate_head')==head and review.get('candidate_tree')==tree,'bootstrap-compatible final product acceptance')
 need(review.get('landed_source_verified') is True and review.get('required_ci_passed') is True,'actual landed source and required CI')
 refs=review.get('underlying_reviews');need(isinstance(refs,list) and len(refs)>=2,'two independent raw source reviews')
 protected=[];seen=set();reviewers=set()
 def referenced(row):
  need(isinstance(row,dict) and set(row)=={'path','sha256'} and digest(row['sha256']),'raw evidence reference')
  raw=read(row['path']);need(sha(raw)==row['sha256'],'actual referenced evidence bytes');protected.append(row['path']);return strict(raw)
 equality=None
 if 'review_source_equality' in review:
  equality=referenced(review['review_source_equality'])
  need(equality['state']=='REVIEWED_SOURCE_BLOBS_EQUAL_FINAL_SOURCE' and equality['final_head']==head and equality['final_tree']==tree,'explicit review-to-final source equality')
  blobs={r['path']:r['git_blob'] for r in inventory['rows']}
  need(isinstance(equality['reviewed_blobs'],dict) and equality['reviewed_blobs'] and all(blobs.get(p)==h for p,h in equality['reviewed_blobs'].items()),'reviewed source blobs equal final inventory')
 for row in refs:
  need(row['sha256'] not in seen,'distinct independent review receipts');seen.add(row['sha256']);obj=referenced(row)
  identity=reviewer_identity(obj);need(identity not in reviewers,'distinct independent reviewer authorities');reviewers.add(identity)
  need(obj.get('decision',obj.get('outcome',obj.get('disposition')))=='ACCEPT','actual independent source review outcome')
  findings=obj.get('findings');need(isinstance(findings,list) and all(isinstance(f,dict) and f.get('blocking') is False for f in findings) and obj.get('material_findings',[])==[],'no unresolved material source review findings')
  rh=obj.get('candidate_head',obj.get('source_head',obj.get('head')));rt=obj.get('candidate_tree',obj.get('source_tree',obj.get('tree')))
  need(digest(rh,40) and (rt is None or digest(rt,40)),'actual reviewed candidate identity')
  applicable=(rh==head and rt==tree)
  if not applicable and equality is not None:
   joined=equality['reviews'].get(row['sha256'],{})
   applicable=set(joined)=={'candidate_head','candidate_tree'} and joined['candidate_head']==rh and digest(joined['candidate_tree'],40) and (rt is None or joined['candidate_tree']==rt)
  need(applicable,'independent review applies to final source')
 landed=referenced(review['landing_evidence'])
 need(landed['state']=='LANDED_SOURCE_TREE_VERIFIED' and landed['runtime_head']==head and landed['runtime_tree']==tree,'concrete final landing identity')
 need(digest(landed['merge_commit'],40) and landed['source_inventory_sha256']==review['source_inventory_sha256'],'landing source inventory and merge reference')
 ci=referenced(review['ci_evidence'])
 need(ci['head']==head and ci['required_ci_passed'] is True and isinstance(ci['checks'],list) and ci['checks'],'actual current-head required CI evidence')
 for check in ci['checks']:
  need(check['conclusion']=='SUCCESS' and isinstance(check['name'],str) and check['name'] and isinstance(check['details_url'],str) and check['details_url'].startswith('https://github.com/'),'concrete successful required CI reference')
 return protected

def declaration(d,m):
 required={'Version','campaign','workload','source_head','source_tree','source_root','source_inventory','build','images','source_prereview','RootAcceptedFinalSourcePins','output_root'}
 need(set(d) in (required,required|{'checkpoint_harness'}) and d['Version']==1 and d['RootAcceptedFinalSourcePins'] is True,'explicit root frozen declaration')
 profile=workload_source.accepted(d['campaign'],d['workload'])
 if d['campaign']==m['campaign']:need(d['workload']==m['workload'],'exact predeclared sustained workload and caps')
 for k in ('source_head','source_tree'):need(digest(d[k],40),'final source '+k)
 for k in ('source_root','output_root'):need(isinstance(d[k],str) and Path(d[k]).is_absolute(),'absolute declared '+k)
 need(('trial24' if profile.issue=='5021' else profile.campaign) in d['output_root'],'fresh accepted campaign source namespace')
 pins={}
 for k in ('source_inventory','build','images','source_prereview'):
  row=d[k];need(set(row)=={'path','sha256'} and digest(row['sha256']),'exact final pin '+k)
  raw=read(row['path']);need(sha(raw)==row['sha256'],'actual final bytes '+k);pins[k]=strict(raw)
 inv=pins['source_inventory'];need(inv['head']==d['source_head'] and inv['tree']==d['source_tree'] and inv['overlays']=={} and isinstance(inv['rows'],list) and inv['rows'],'actual final inventory head/tree/no overlays')
 paths=set()
 for r in inv['rows']:
  need(isinstance(r['path'],str) and r['path'] not in paths and not r['path'].startswith('/') and '..' not in r['path'].split('/') and r['mode'] in ('100644','100755') and digest(r['git_blob'],40),'bounded unique Git inventory rows');paths.add(r['path'])
 for k in ('build','images'):
  x=pins[k];need(x['head']==d['source_head'] and x['tree']==d['source_tree'] and x['source_inventory_sha256']==d['source_inventory']['sha256'],'actual final '+k+' identity')
 build=pins['build'];need(build['state']=='VERIFIED_ELFS_NOT_IMAGES_OR_RUNTIME' and build['source_verified_before_after'] is True,'actual independently frozen build')
 names={'treedb-fixed-peer','treedb-query-under-write'}
 need(set(build['ELFs'])==names,'exact built ELF names')
 for name in names:
  elf=build['ELFs'][name];need(digest(elf['sha256']) and type(elf['bytes']) is int and elf['bytes']>0,'actual ELF pin and positive bytes')
 images=pins['images'];need(images['state']=='BOTH_HOSTS_PACKAGED_NOT_RUNTIME' and images['build_proof_sha256']==d['build']['sha256'],'actual packaging state and build receipt bytes')
 need(set(images['images'])=={'111','185'},'exact packaged hosts')
 for host,ip in (('111','192.168.0.111'),('185','192.168.0.185')):
  image=images['images'][host]
  need(image['state']=='SOURCE_VERIFIED_ELFS_PACKAGED_NO_CLUSTER_QUALIFICATION' and image['host']==ip,'actual per-host packaging-only state')
  need(isinstance(image['image'],str) and image['image'].startswith('sha256:') and digest(image['image'][7:]),'actual immutable image identity')
  need(image['head']==d['source_head'] and image['tree']==d['source_tree'] and image['source_inventory_sha256']==d['source_inventory']['sha256'],'actual packaged source identity')
  need(image['parent_unchanged'] is True and image['stores_mounted'] is False,'packaging without parent mutation or store mounts')
  need(set(image['ELFs'])==names,'exact packaged ELF names')
  for name in names:
   elf=image['ELFs'][name];need(digest(elf['sha256']) and type(elf['bytes']) is int and elf['bytes']>0 and elf['sha256']==build['ELFs'][name]['sha256'] and elf['bytes']==build['ELFs'][name]['bytes'],'actual packaged ELF/build equality')
 review=pins['source_prereview'];need(review.get('source_inventory_sha256')==d['source_inventory']['sha256'],'product acceptance inventory identity')
 protected_reviews=product_acceptance(review,d['source_head'],d['source_tree'],inv)
 harness_head,harness_tree=d['source_head'],d['source_tree']
 if 'checkpoint_harness' in d:
  need(d['campaign']=='rf4trial24mixedchangingc1' and (d['source_head'],d['source_tree'])==('484c6e131ba72357155d512ed06f2842c0a866a3','5a3ff182ebe4f86aca1fcf171f7d44b90643528a'),'checkpoint retains exact runtime source')
  h=d['checkpoint_harness'];need(set(h)=={'head','tree','source_inventory','source_acceptance'},'separate checkpoint harness pins')
  need(digest(h['head'],40) and digest(h['tree'],40),'exact harness identity');harness_head,harness_tree=h['head'],h['tree']
  hp={}
  for key in ('source_inventory','source_acceptance'):
   row=h[key];need(set(row)=={'path','sha256'} and digest(row['sha256']),'harness evidence pin')
   raw=read(row['path']);need(sha(raw)==row['sha256'],'actual harness evidence bytes');hp[key]=strict(raw);protected_reviews.append(row['path'])
  need(hp['source_acceptance']['source_inventory_sha256']==h['source_inventory']['sha256'],'harness acceptance inventory bytes')
  hi=hp['source_inventory'];need(hi['head']==harness_head and hi['tree']==harness_tree and hi['overlays']=={},'actual separate landed harness inventory')
  rows=hi['rows'];need(isinstance(rows,list) and rows,'complete harness source inventory')
  blobs={}
  for row in rows:
   need(set(row)=={'path','mode','git_blob'} and row['path'] not in blobs and row['mode'] in ('100644','100755') and digest(row['git_blob'],40),'unique harness Git rows')
   blobs[row['path']]=row
  prefix='docs/benchmarks/fixed_cluster_sustained_20261005/harness/'
  for path in ROOT.rglob('*'):
   if not path.is_file():continue
   need(not path.is_symlink(),'immutable harness source file')
   raw=path.read_bytes();name=prefix+path.relative_to(ROOT).as_posix()
   blob=hashlib.sha1(b'blob '+str(len(raw)).encode()+b'\0'+raw).hexdigest()
   need(name in blobs and blobs[name]['git_blob']==blob and blobs[name]['mode']==('100755' if path.stat().st_mode&0o111 else '100644'),'current template bytes equal separately landed harness inventory')
  protected_reviews+=product_acceptance(hp['source_acceptance'],harness_head,harness_tree,hi)
 isolate_paths([d['output_root']],[d['source_root'],ROOT]+protected_reviews+[d[k]['path'] for k in ('source_inventory','build','images','source_prereview')])
 return {'"__ROOT_FROZEN_CHECKPOINT_ALLOWED__"':repr('checkpoint_harness' in d),'__ROOT_FROZEN_HARNESS_HEAD__':harness_head,'__ROOT_FROZEN_HARNESS_TREE__':harness_tree,'__ROOT_FROZEN_DRIVER_SHA__':build['ELFs']['treedb-query-under-write']['sha256'],'__ROOT_FROZEN_IMAGES_SHA__':d['images']['sha256'],'__ROOT_FROZEN_ACCEPTANCE_SHA__':d['source_prereview']['sha256'],'__ROOT_FROZEN_BUILD_PATH__':d['build']['path'],'__ROOT_FROZEN_IMAGES_PATH__':d['images']['path'],'__ROOT_FROZEN_ACCEPTANCE_PATH__':d['source_prereview']['path'],'__ROOT_FROZEN_IMAGE111__':images['images']['111']['image'],'__ROOT_FROZEN_IMAGE185__':images['images']['185']['image'],'__ROOT_FROZEN_SERVER_SHA__':build['ELFs']['treedb-fixed-peer']['sha256'],'__ROOT_FROZEN_BUILD_SHA__':d['build']['sha256'],'__ROOT_FROZEN_HEAD__':d['source_head'],'__ROOT_FROZEN_TREE__':d['source_tree'],'__ROOT_FROZEN_SOURCE_ROOT__':d['source_root'],'__ROOT_FROZEN_INVENTORY_PATH__':d['source_inventory']['path'],'__ROOT_FROZEN_INVENTORY_SHA__':d['source_inventory']['sha256'],'"__ROOT_FROZEN_INVENTORY_ROWS__"':str(len(inv['rows']))}
def main():
 need(__debug__,'ordinary Python required')
 q=argparse.ArgumentParser();q.add_argument('--declaration',required=True);q.add_argument('--declaration-sha256',required=True);a=q.parse_args()
 raw=read(a.declaration,1<<20);need(digest(a.declaration_sha256) and sha(raw)==a.declaration_sha256,'exact declared source construction input')
 d=strict(raw);packetraw=read(ROOT/'packet.json',1<<20);need(sha(packetraw)==PACKET_SHA,'frozen provisional packet')
 m=strict(packetraw);bindings=declaration(d,m);out=Path(d['output_root']);need(not out.exists() and not out.is_symlink(),'fresh exclusive output')
 prepared={};emitted={}
 for name,row in m['transitive_sources'].items():
  raw=read(ROOT/row['path'],1<<20);need(sha(raw)==row['sha256'],'all frozen transitive source bytes');prepared[row['path']]=raw
 workload=m['workload_source'];raw=read(ROOT/workload['path'],16384);need(sha(raw)==workload['sha256'],'frozen workload source')
 profile=workload_source.accepted(d['campaign'],d['workload'])
 old='PROFILE='+repr(workload_source.PROFILE);need(raw.decode().count(old)==1,'single authenticated profile binding')
 bound=raw.decode().replace(old,'PROFILE='+repr((profile.campaign,profile.originals,profile.duration,profile.spacing,profile.concurrency)),1).encode()
 ast.parse(bound)
 prepared[workload['path']]=bound;workload_emitted=dict(workload,sha256=sha(bound),template_sha256=workload['sha256'])
 resolver=m['source_path_resolver'];raw=read(ROOT/resolver['path'],1<<20);need(sha(raw)==resolver['sha256'],'frozen source resolver')
 need(raw.decode().count(workload['sha256'])==1,'single resolver workload pin')
 resolved=raw.decode().replace(workload['sha256'],workload_emitted['sha256']).encode()
 prepared[resolver['path']]=resolved;resolver_emitted=dict(resolver,sha256=sha(resolved),template_sha256=resolver['sha256'])
 for role,row in m['roles'].items():
  raw=read(ROOT/row['output']['path'],1<<20);need(sha(raw)==row['output']['sha256'],'all frozen template bytes')
  s=raw.decode().replace(resolver['sha256'],resolver_emitted['sha256'])
  for old,new in bindings.items():s=s.replace(old,new)
  for dep,dep_row in m['roles'].items():
   p=dep_row['output'];s=s.replace(str(ROOT/p['path']),str(out/Path(p['path']).name))
   if p['sha256'] in s:
    need(dep in emitted,'topological source dependency');s=s.replace(p['sha256'],emitted[dep]['sha256'])
  need('__ROOT_FROZEN_' not in s,'all final runtime placeholders bound')
  ast.parse(s);b=s.encode();prepared[role]=b
  emitted[role]={'path':str(out/Path(row['output']['path']).name),'sha256':sha(b),'template_sha256':row['output']['sha256']}
 isolate_paths([out],[d['source_root'],ROOT,a.declaration]+[d[k]['path'] for k in ('source_inventory','build','images','source_prereview')])
 # Validation and complete source preparation happen before any output creation.
 out.mkdir(mode=0o700)
 for role,b in prepared.items():
  dest=Path(emitted[role]['path']) if role in emitted else out/role
  dest.parent.mkdir(parents=True,exist_ok=True)
  with dest.open('xb') as f:f.write(b)
 receipt={'state':'FROZEN_SOURCE_OUTPUT_PENDING_INDEPENDENT_REVIEW_NO_ACTIVATION','campaign':d['campaign'],'workload':d['workload'],'source_head':d['source_head'],'source_tree':d['source_tree'],'declaration_path':a.declaration,'declaration_sha256':a.declaration_sha256,'provisional_packet_sha256':PACKET_SHA,'roles':emitted,'transitive_sources':m['transitive_sources'],'source_path_resolver':resolver_emitted,'workload_source':workload_emitted,'final_source_pins':{k:d[k] for k in ('source_inventory','build','images','source_prereview')},'runtime_started':False,'admission_flags_granted':False,'root_remaining_gates':['Independent generated source review including final macro/path/hash joins','Fresh bootstrap/config/TLS/CIDs/stores/growth/post-input sealed inventory','Native declared-prefix Go preparation actual timeout/1MiB cap and independent actual oracle acceptance','Predeclared recall accounting policy, permission8raw proof, final manifest and exclusive root activation','Actual one-shot window, all initial/final populations, all declared witnesses, resource/closure and durable retention']}
 if 'checkpoint_harness' in d:receipt['checkpoint_harness']=d['checkpoint_harness']
 with (out/'instantiation.json').open('x') as f:json.dump(receipt,f,indent=2);f.write('\n')
 print(json.dumps({'state':receipt['state'],'receipt':str(out/'instantiation.json'),'sha256':sha((out/'instantiation.json').read_bytes()),'runtime_started':False}))
if __name__=='__main__':main()
