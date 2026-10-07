from pathlib import Path
import datetime,hashlib,json,subprocess,os
O=Path('/home/mikers/gomap-r1-final-observers-20261006');F=Path('/home/mikers/gomap-r1-mac-evidence-preservation-20261006')
S=Path('/home/mikers/gomap-r1-publication-stage-M-3325dfe-20261006')
R=Path('/home/mikers/gomap-r1-final-results-20261006/M-3325dfe');P=Path('/home/mikers/gomap-r1-final-receipts-20261006')
M='3325dfe77940fec8587d8b61b1ac4e0b2f72caca';runtime='eab40aaf77ed307d92a7a38adf0dd80be07a7e12bdda8a2ec3118baf9cad33ff'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def freeze(p,x):
 with p.open('x') as f:json.dump(x,f,indent=2,sort_keys=True);f.write('\n')
for label in ['A','C','D']:
 x=json.loads((R/label/'root-cost-acceptance-original.json').read_text());assert x['accepted'] is True and x['actual_main_commit']==M
base=O/'final-publication-relocated-base13-root-preparation.json';assert sha(base)=='ddfa77702e3e19974c03deab5b01d40b273b05634e9d7a330b664241dbbfecd6'
records=json.loads(base.read_text());assert len(records)==13
for label,kind in [('A','comparator'),('C','mutation-sweep'),('D','lifecycle')]:
 rec=P/('r1-final-'+label+'-M-3325dfe');trusted=json.loads((rec/'trusted-completed-receipt.json').read_text());start=json.loads((rec/'validation-start.json').read_text());end=json.loads((rec/'validation-exit.json').read_text());assert end['exit']==0
 records.append({'root':'/home/mikers/gomap-r1-publication-mirror-inputs-M-3325dfe-20261006/'+label,'kind':kind,'status':'accepted','path':kind+'/final-actual-M/3325dfe','validation':{'log':'original-independent-validation.log','command':start['argv'],'exit_code':end['exit']},'expected':{'commit':M,'runtime_sha256':runtime,'harness_sha256':trusted['harness_sha256']}})
selection=O/'M-3325dfe-final-publication-selection16-original.json';freeze(selection,records)
helper=F/'prepare-evidence-publication-final.py';assert sha(helper)=='d03f6fe489ae7bd0296cd31dcff1785cbd25d855acb714afe1191ea73a0b426a'
argv=['python3','-B',str(helper),'--captures',str(selection),'--out',str(S)]
freeze(O/'M-3325dfe-core-stage-start-original.json',{'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'argv':argv,'helper_sha256':sha(helper),'selection_sha256':sha(selection),'acceptances':{label:sha(R/label/'root-cost-acceptance-original.json') for label in ['A','C','D']}})
with (O/'M-3325dfe-core-stage-stdout-original.log').open('xb') as out,(O/'M-3325dfe-core-stage-stderr-original.log').open('xb') as err:code=subprocess.call(argv,env={**os.environ,'PYTHONDONTWRITEBYTECODE':'1'},stdout=out,stderr=err)
freeze(O/'M-3325dfe-core-stage-exit-original.json',{'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'exit_code':code,'original_base13_sha256':sha(base),'selection_sha256':sha(selection)})
assert code==0
print(json.dumps({'stage':str(S),'core_SHA256SUMS_sha256':sha(S/'SHA256SUMS'),'overlay_sha256':sha(S/'binary-overlay.tar.gz'),'overlay_bytes':(S/'binary-overlay.tar.gz').stat().st_size,'binary_members':len(json.loads((S/'binary-member-index.json').read_text()))}))
