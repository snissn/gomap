from pathlib import Path
import hashlib,json,re
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');A=D/'ordinary-iterator-allocation';P=A/'profile-successor';OLD=D/'ordinary-checkpoint-cost/profiles-01'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def flat(raw_path,needle):
 raw=raw_path.read_text();locations={}
 for line in raw.splitlines():
  m=re.match(r'^\s*(\d+):\s+0x.*',line)
  if m:locations[int(m[1])]=line
 matching={i:l for i,l in locations.items() if needle in l};samples=[]
 for line in raw.splitlines():
  m=re.match(r'^\s*(\d+)\s+(\d+)\s+(\d+)\s+(\d+):\s*(\d+)(?:\s|$)',line)
  if m and int(m[5]) in matching:samples.append({'objects':int(m[1]),'bytes':int(m[2]),'location':matching[int(m[5])]})
 return {'objects':sum(x['objects'] for x in samples),'bytes':sum(x['bytes'] for x in samples),'allocation_rows':samples,'raw_sha256':sha(raw_path)}
old=flat(OLD/'analysis/candidate-public-raw.txt','tree.(*Tree).IteratorWithOptions ')
new=flat(P/'analysis/alloc_space-raw.txt','tree.(*Tree).IteratorWithOptions ')
newinit=flat(P/'analysis/alloc_space-raw.txt','tree.(*Tree).initPrimaryIterator ')
assert (old['objects'],old['bytes'])==(144,1179648)
assert new['objects']==new['bytes']==newinit['objects']==newinit['bytes']==0
v=json.loads((A/'validation.json').read_text());assert v['all_pass'] and v['all_source_equal']
current=json.loads((P/'source-before.json').read_text());assert current==json.loads((P/'source-after.json').read_text())==json.loads((A/'freeze-before.json').read_text())['files']
packet=D/'performance-185/maintenance-hosted-02/packet/bounded-maintenance-38808351333-1/mvcc-maintenance-72e01'
oldfiles=json.loads((packet/'performance-185/maintenance-build-02/sources-after.json').read_text())['candidate']['files'];assert current.keys()==oldfiles.keys()
changed=[p for p in current if current[p]!=oldfiles[p]]
expected=['.github/ci/ci_impact.json','TreeDB/cow_power_loss_test.go','TreeDB/power_loss_oracle_test.go','TreeDB/tree/iterator.go','TreeDB/tree/owned_iterator.go','TreeDB/tree/primary_iterator.go']
assert set(changed)==set(expected)
receipt={'status':'DISCARDED_WRAPPER_ALLOCATION_REMOVED','old_source_head':'72e01efd767ef5d766f070e531fb53ea3f22674d','new_source_parent':v['head'],'new_source_patch_sha256':v['source_patch_sha256'],'new_binary_sha256':sha(P/'checkpoint.test'),'old_binary_sha256':sha(OLD/'candidate-public/checkpoint.test'),'source_file_count':len(current),'changed_from_measured_predecessor':changed,'old_iterator_flat':old,'new_iterator_flat':new,'new_initializer_flat':newinit,'new_profile_sha256':sha(P/'alloc.pprof'),'old_profile_sha256':sha(OLD/'candidate-public/alloc.pprof'),'scope':'Full-process allocation attribution, same original16x public dirty-checkpoint operation/fixture counters. Setup and off-timer reset/seed/teardown included. Not a timed B/op delta, latency, or overall allocation/retention acceptance.'}
(A/'root-profile-verification.json').write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps({k:receipt[k] for k in ('status','source_file_count','changed_from_measured_predecessor')}));print('Old wrapper flat',old['objects'],old['bytes'],'new',new['objects'],new['bytes'],'initializer',newinit['objects'],newinit['bytes'])
