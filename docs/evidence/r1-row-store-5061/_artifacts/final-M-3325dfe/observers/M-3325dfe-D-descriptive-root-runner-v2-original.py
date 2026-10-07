import datetime,hashlib,json,os,subprocess
from pathlib import Path
F=Path('/home/mikers/gomap-r1-mac-evidence-preservation-20261006')
O=Path('/home/mikers/gomap-r1-final-observers-20261006')
R=Path('/home/mikers/gomap-r1-final-results-20261006/M-3325dfe/D')
P=Path('/home/mikers/gomap-r1-final-receipts-20261006/r1-final-D-M-3325dfe')
packet=Path('/home/mikers/gomap-r1-evidence-20261005/r1-final-D-M-3325dfe/packet.json')
assert os.environ.get('R1_RECEIPT_LOCKED')=='yes'
assert R.is_dir() and (R/'root-provenance-verification-original.json').is_file()
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def freeze(p,x):
 with p.open('x') as f:json.dump(x,f,indent=2,sort_keys=True);f.write('\n')
def now():return datetime.datetime.now(datetime.timezone.utc).isoformat()
tools={F/'final-D-report-preparation/format.py':'022c45de44e96014de9ef343b639be4abd92189152efd411314b829b54c4da58',F/'final-D-summary-preparation/summarize.py':'136462c6d6d56556e394dd26e0ce18ff2fc200443cb7bcf82598f4c3a2d0f6e4',F/'final-D-report-preparation/frozen-r1_lifecycle_validate.py':'dcfd45e233bc46a1c1f73ffaf2af840e5d8ab8ba312342ceb61b014e7c199968',packet:'b8705425f0d3b0c8344ff9cab4d33e86fb964ace3da8fab7afb8d63b796c26dc'}
originals={p:sha(p) for p in P.iterdir() if p.is_file()}
assert all(sha(p)==h for p,h in tools.items())
prefix='../../../../docs/evidence/r1-row-store-5061/_artifacts/'
namespace=prefix+'final-M-3325dfe/results/D/'
env={**os.environ,'PYTHONDONTWRITEBYTECODE':'1'}
def run(name,argv):
 freeze(R/(name+'-start-original.json'),{'utc':now(),'argv':argv,'cwd':str(R),'environment':env,'input_pins':{str(p):h for p,h in {**tools,**originals}.items()},'scope':'descriptive formatting of unchanged original raw observations only; no revalidation or acceptance'})
 with (R/(name+'-stdout-original.log')).open('xb') as out,(R/(name+'-stderr-original.log')).open('xb') as err:
  code=subprocess.call(argv,cwd=R,env=env,stdout=out,stderr=err)
 freeze(R/(name+'-exit-original.json'),{'utc':now(),'exit_code':code,'all_input_pins_unchanged':all(sha(p)==h for p,h in {**tools,**originals}.items())})
 assert code==0 and all(sha(p)==h for p,h in {**tools,**originals}.items())
run('format',['python3','-B',str(F/'final-D-report-preparation/format.py'),str(packet),'--validator',str(F/'final-D-report-preparation/frozen-r1_lifecycle_validate.py'),'--packet-link',prefix+'lifecycle/final-actual-M/3325dfe/packet.json','--raw-root-link',prefix+'lifecycle/final-actual-M/3325dfe','--validation-log-link',prefix+'final-M-3325dfe/receipts/D/independent-validation.log','--details-link',namespace+'descriptive-projection.json','--details-out',str(R/'descriptive-projection.json')])
with (R/'descriptive-report.md').open('xb') as f:f.write((R/'format-stdout-original.log').read_bytes())
run('summarize',['python3','-B',str(F/'final-D-summary-preparation/summarize.py'),str(R/'descriptive-projection.json'),'--projection-link',namespace+'descriptive-projection.json','--trajectory-report-link',namespace+'descriptive-report.md','--validation-log-link',prefix+'final-M-3325dfe/receipts/D/independent-validation.log','--summary-out',str(R/'summary.json')])
with (R/'summary.md').open('xb') as f:f.write((R/'summarize-stdout-original.log').read_bytes())
freeze(R/'descriptive-root-operation-original.json',{'utc':now(),'scope':'raw descriptive projection only; numeric acceptance remains separate','input_pins_unchanged':all(sha(p)==h for p,h in {**tools,**originals}.items()),'outputs':{p.name:{'sha256':sha(p),'bytes':p.stat().st_size} for p in R.iterdir() if p.is_file()}})
print(json.dumps({'outputs':str(R),'summary_sha256':sha(R/'summary.json'),'projection_sha256':sha(R/'descriptive-projection.json'),'report_sha256':sha(R/'descriptive-report.md')}))
