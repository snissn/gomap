from source_paths import source_path
"""Validate the fresh raw bootstrap and freeze recall inputs; never starts voters."""
import base64,hashlib,importlib.util,json,pathlib,shlex,shutil,struct,subprocess,tarfile,time
P=pathlib.Path('/tmp');RUN='rf4trial24mixedchangingc1'
B=pathlib.Path('/Volumes/FlashDrive/gomap-4997-4998-rf4trial24mixedchangingc1/bootstrap-root-v1');L=P/f'gomap-4997-4998-{RUN}-bootstrap-lifecycle-root-v1'
O=pathlib.Path('/Volumes/FlashDrive/gomap-4997-4998-rf4trial24mixedchangingc1/pre-inputs-root-v1');R=P/f'gomap-4997-4998-{RUN}-pre-input-preparation-root-v1'
def sha(b):return hashlib.sha256(b).hexdigest()
def load(p):return json.loads(p.read_bytes())
def remote(host,label,args):
 argv=['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10','mikers@'+host,shlex.join(args)];start=time.time()
 r=subprocess.run(argv,capture_output=True,text=True,timeout=45)
 (R/(label+'.json')).write_text(json.dumps(dict(argv=argv,exit_code=r.returncode,stdout=r.stdout,stderr=r.stderr,started_unix=start,finished_unix=time.time()),indent=2)+'\n')
 assert r.returncode==0,(label,r.stderr);return r.stdout
def checked_submit(r):
 assert r['ActualAck']==4 and r['CommittedApplied'] and r['CommittedRecoverable']
 e=r['Evidence'];entry=r['CommittedEntry'];assert e['Kind']=='production-consensus-v1' and e['ProductionConsensus'] and e['Committed'] and e['GroupID']=='group-a'
 assert e['Index']==entry['Index'] and e['Term']==entry['Term'] and e['NodeID']==e['LeaderID']
 assert r['ApplyResult']['Status'] in ['applied','already-applied'] and not r['ApplyResult']['DeterministicErrorCode']
 raw=base64.b64decode(entry['Bytes'],validate=True);assert raw==base64.b64decode(r['DecodedEntry']['Bytes'],validate=True) and r['ApplyResult']['CommandDigest']==r['DecodedEntry']['Digest']
 return entry['Index'],raw
def varint(raw,pos):
 v=0;shift=0
 while True:
  b=raw[pos];pos+=1;v|=(b&127)<<shift
  if b<128:return v,pos
  shift+=7;assert shift<=63
def byte_list(raw):
 n,pos=varint(raw,0);lens=[]
 for _ in range(n):v,pos=varint(raw,pos);lens.append(v)
 out=[]
 for n in lens:out.append(raw[pos:pos+n]);assert len(out[-1])==n;pos+=n
 assert pos==len(raw);return out
def main():
 assert __debug__ and not O.exists() and not R.exists();R.mkdir()
 final=load(L/'result.json');assert final['state']=='PASS_FRESH_BOOTSTRAP_CLOSED' and final['launcher_exit']==0 and len(final['stopped'])==4 and not final['errors']
 assert load(B/'result.json')['status']=='PASS'
 receipts=sorted(B.glob('[0-9][0-9]-*.json'));assert len(receipts)==44
 for p in receipts:
  j=load(p);assert j['exit_code']==0 and not j.get('timed_out'),p
 init=json.loads(load(B/'19-initialize.json')['stdout']);bootraw=load(B/'36-qualify.json')['stdout'].encode();boot=json.loads(bootraw)
 assert init['Stage']=='prepared-restart-required' and init['Prepare']==boot['Prepare'] and init['Dataset']==boot['Dataset']
 dataset=P/'gomap-4956-corpus10k-dataset';manifest=load(dataset/'manifest.json');corpus=(dataset/'documents.f32').read_bytes()
 for f,v in manifest['files'].items():
  raw=(dataset/f).read_bytes();assert len(raw)==v['bytes'] and sha(raw)==v['sha256']
 d=init['Dataset'];assert d['Rows']==10000 and d['SourceRows']==10003 and d['Dimensions']==128 and d['InputBytes']==5221566
 assert d['ManifestSHA256']==sha((dataset/'manifest.json').read_bytes()) and d['VectorsSHA256']==sha(corpus)
 prior,_=checked_submit(init['Create']);index,_=checked_submit(init['Seed']);assert index>prior;prior=index
 assert len(init['Chunks'])==79
 for k,ch in enumerate(init['Chunks']):
  assert ch['Ordinal']==k and ch['FirstRow']==k*128 and ch['Rows']==min(128,10000-k*128) and ch['Outcome']=='committed-applied' and not ch['Error']
  assert ch['RequestID']==RUN+'/dataset-'+d['ManifestSHA256']+f'/dataset/{k:06d}'
  index,raw=checked_submit(ch['Result']);assert index>prior and sha(raw)==ch['EntrySHA256'];prior=index
  sections={s['ID']:base64.b64decode(s['Bytes'],validate=True) for s in ch['Result']['DecodedEntry']['Decoded']['Sections']}
  assert sections[8].decode()==ch['RequestID'];ids=byte_list(sections[102]);docs=byte_list(sections[103]);assert len(ids)==len(docs)==ch['Rows'] and ch['Result']['ApplyResult']['AffectedCount']==ch['Rows']
  for j,(id,docraw) in enumerate(zip(ids,docs)):
   doc=json.loads(docraw);row=k*128+j;assert id.decode()==f'doc-{row:06d}' and doc['kind']=='dataset' and struct.pack('<128f',*doc['embedding'])==corpus[row*512:(row+1)*512]
 prepare=boot['Prepare']['Command'];assert prepare['SourceRowCount']==prepare['MaxSourceRows']==10003 and prepare['IndexPosition']>prior and prepare['Operation']=='prepare'
 fresh=RUN+'/dataset-'+d['ManifestSHA256']+'-fresh-y';first=boot['Insert'];retry=boot['Retry'];prefix=retry['CommitIndex']
 assert prepare['IndexPosition']<first['CommitIndex']<prefix and first['VisibleID']==retry['VisibleID']==fresh and first['LiveRevision']==retry['LiveRevision']==1
 for x in [first,retry]:assert x['PartitionID']==0 and x['OwnerGroup']=='group-a' and x['ProductionConsensus'] and x['AppliedIndex']>=x['CommitIndex'] and not x['VisibilityToken']
 assert {x['NodeID'] for x in boot['Readiness']}=={'node-a','node-b','node-c','node-d'}
 for x in boot['Readiness']:
  assert x['Ready'] and x['Live'] and not x['Draining'] and x['VectorPhase']=='active' and len(x['Groups'])==1
  g=x['Groups'][0];assert g['GroupID']=='group-a' and g['Ready'] and g['LocalAppliedIndex']>=g['RequiredAppliedIndex']>=prefix
 planraw=(B/'plan.json').read_bytes();plan=json.loads(planraw);frozen=load(P/f'gomap-4997-4998-{RUN}-bootstrap-plan-root-v1/plan-preparation.json');assert plan['nodes']==frozen['plan']['nodes'] and plan['binary_sha256']=='74f77485ace57056f6bd8244b467dd5fe136322ba1c2244010ebf6ae0af06042'
 launcher=pathlib.Path(source_path('/tmp/gomap-4956-5fa-fixed-cluster.py'));assert sha(launcher.read_bytes())=='21c3f9204489ae7179341772b4b856de2b9280acacfbe5ad4deea52da9750616'
 spec=importlib.util.spec_from_file_location('fixed_cluster',launcher);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
 actual=m.plan(load(P/f'gomap-4997-4998-{RUN}-bootstrap-plan-root-v1/manifest.json'),RUN,m.admit_dataset(str(dataset)))
 O.mkdir();shutil.copytree(dataset,O/'dataset');shutil.copytree(B,O/'historical-chain');shutil.copytree(L,O/'bootstrap-close')
 (O/'bootstrap.json').write_bytes(bootraw)
 c=next(n for n in actual if n['node']=='node-c');configraw=remote(c['host'],'actual-config',['cat',c['root']+'/config.json']).encode();assert configraw==json.dumps(c['config']).encode();(O/'config.json').write_bytes(configraw)
 voters=[]
 for n in plan['nodes']:
  node=n['node'];created=load(next(B.glob('*-serve-'+node+'.json')))['stdout'].strip();x=json.loads(load(L/('stopped-'+node+'.json'))['stdout'])[0]
  assert x['Id']==created and x['Image']==n['image'] and x['Name']=='/'+n['name'] and x['Config']['Labels']['treedb.fixed-cluster.run']==RUN
  assert not x['State']['Running'] and not x['State']['OOMKilled'] and x['State']['ExitCode']==0
  hc=x['HostConfig'];assert hc['Memory']==hc['MemorySwap']==2147483648 and hc['NanoCpus']==2000000000 and hc['RestartPolicy']['Name']=='no'
  assert {v['Destination']:v['Source'] for v in x['Mounts']}['/data']==n['root']+'/data' and {v['Destination']:v['Source'] for v in x['Mounts']}['/raft']==n['root']+'/raft'
  assert load(next(B.glob('*-close-exit-'+node+'.json')))['stdout'].strip()=='0' and load(next(B.glob('*-restart-'+node+'.json')))['stdout'].strip()==created
  voters.append(dict(node=node,container_id=created))
 for host in ['192.168.0.111','192.168.0.185']:
  raw=remote(host,'writer-inventory-'+host,['docker','ps','--filter','label=treedb.fixed-cluster.run='+RUN,'--format','{{.ID}} {{.Names}}']);assert not raw.strip();(O/('writer-inventory-'+host.rsplit('.',1)[1]+'.txt')).write_text(raw)
 prov=dict(Version=1,Phase='pre',CampaignID=RUN,BootstrapRequestID=RUN,ConfigSHA256=sha(configraw),BootstrapSHA256=sha(bootraw),ManifestSHA256=d['ManifestSHA256'],RuntimeSourceHead='__ROOT_FROZEN_HEAD__',ServerBinarySHA256=plan['binary_sha256'],QualificationSourceSHA256='750dde6bc3866287e3201e62be9b88dc6f5e0d7b2a947d16da0e5efba074e769',InitializationReceiptSHA256=sha((B/'19-initialize.json').read_bytes()),CleanReopenReceiptSHA256=sha((B/'result.json').read_bytes()),ProbeSHA256='',ProbeBinarySHA256='',ProbeRunID='',Roots={n['node']:n['root'] for n in plan['nodes']},Hosts={n['node']:n['host'] for n in plan['nodes']},RootAccepted=True,InitializationSucceeded=True,CleanReopenSucceeded=True,ExclusiveWriterStopped=True)
 (O/'provenance.json').write_text(json.dumps(prov,indent=2)+'\n')
 proof=dict(status='PASS_ROOT_FULL_BOOTSTRAP_CHAIN',raw_receipts=44,chunks=79,rows=10000,prepared_rows=10003,pre_live_rows=10004,qualification_prefix=prefix,initialize_sha256=prov['InitializationReceiptSHA256'],clean_reopen_summary_sha256=prov['CleanReopenReceiptSHA256'],voters=voters,scope='actual fresh campaign; bootstrap closed; no subsequent mutations issued')
 (O/'root-chain-proof.json').write_text(json.dumps(proof,indent=2)+'\n')
 files={str(f.relative_to(O)):sha(f.read_bytes()) for f in sorted(O.rglob('*')) if f.is_file()};(O/'input-inventory.json').write_text(json.dumps(files,indent=2)+'\n')
 archive=O.with_suffix('.tar.gz');assert not archive.exists()
 with tarfile.open(archive,'x:gz') as t:t.add(O,arcname=O.name)
 seal=dict(state='FRESH_PRE_INPUTS_FROZEN_NO_RECALL_EXECUTED',campaign=RUN,input_root=str(O),input_inventory_sha256=sha((O/'input-inventory.json').read_bytes()),files=len(files),archive=str(archive),archive_sha256=sha(archive.read_bytes()),qualification_prefix=prefix,bootstrap_sha256=sha(bootraw),config_sha256=sha(configraw),plan_sha256=sha(planraw),voters=voters,build_source_head=prov['RuntimeSourceHead'],product_applicability_head=None,candidate_source_tree='__ROOT_FROZEN_TREE__',candidate_pending_merge=False)
 (R/'seal.json').write_text(json.dumps(seal,indent=2)+'\n');print(json.dumps(seal))
if __name__=='__main__':main()
