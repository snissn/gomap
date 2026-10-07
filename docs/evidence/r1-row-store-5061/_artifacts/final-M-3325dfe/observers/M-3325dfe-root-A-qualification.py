#!/usr/bin/env python3
"""Root-owned original A provenance qualification; numeric acceptance is separate."""
import datetime, hashlib, importlib.util, json, os, subprocess, sys
from pathlib import Path
sys.dont_write_bytecode = True
P = Path('/home/mikers/gomap-r1-final-receipts-20261006')
O = Path('/home/mikers/gomap-r1-final-observers-20261006')
F = Path('/home/mikers/gomap-r1-mac-evidence-preservation-20261006')
M = '3325dfe77940fec8587d8b61b1ac4e0b2f72caca'
def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def read(p): return json.loads(Path(p).read_text())
def require(v, m):
    if not v: raise ValueError(m)
def freeze(p,v):
    with Path(p).open('x') as f: json.dump(v,f,indent=2,sort_keys=True); f.write('\n')
def now(): return datetime.datetime.now(datetime.timezone.utc).isoformat()
require(os.environ.get('R1_RECEIPT_LOCKED') == 'yes', 'root qualification must hold canonical lock')
cfgp = Path('/home/mikers/gomap-r1-final-configs-20261006/M-3325dfe/A-config.json')
require(sha(cfgp) == 'f2b95b22d5e1c12a00c5cc77dc5846b199535aa3e30e2cc77ed8b8f8b03257b4', 'pre-run frozen A config changed')
cfg=read(cfgp); r=Path(cfg['receipt_dir']); out=Path(cfg['out']); repo=Path(cfg['repo'])
require(subprocess.check_output(['git','-C',str(repo),'rev-parse','HEAD'],text=True).strip()==M, 'not actual M')
require(not subprocess.check_output(['git','-C',str(repo),'status','--porcelain','--untracked-files=normal'],text=True).strip(), 'source dirty')
require(cfg==read(r/'root-frozen-inputs.json'), 'original input config mismatch')
require(sha(O/'observe-A-with-environment.py')=='f477dacf465f347afa423759cbc6f148f8e6bbb391903e333bcddc0cd323990d', 'observer changed')
source=read(r/'independent-expected-source.json'); packet=read(out/'packet.json')
for k,external in [('commit','source_commit'),('runtime_sha256','runtime_sha256'),('harness_sha256','harness_sha256')]: require(source[k]==cfg[external], 'independent source pin mismatch '+k)
for p in [out/'source.json',out/'source-after.json']: require(read(p)==source, 'original source sidecar differs')
require(packet['source']==source and source['clean'] is True, 'packet/source mismatch')
for name in ['observer-before.json','build-start.json','build-completed.json']: require(read(r/name)['source']==source, 'observed source differs '+name)
for name in ['build-completed.json','run-exit.json','trusted-completed-receipt.json','validation-exit.json']: require(read(r/name)['exit']==0, 'failed observed command '+name)
require(read(r/'observer-before.json')['observer_sha256']==sha(O/'observe-A-with-environment.py'), 'original observer pin mismatch')
require(read(r/'run-exit.json')['driver_log_sha256']==sha(r/'driver.log'), 'driver log mismatch')
require(len(packet['cells'])==30 and all(c['oracle_verified'] is True for c in packet['cells']), 'full30 oracles missing')
require(packet['config']=={'documents':4096,'batch_size':32,'operations':1000,'repetitions':5,'durability':'durable','read_state':'flushed','engines':['json','template-v1','bson','typed-row','sqlite-json','sqlite-row'],'qualification':'retained'}, 'full A contract changed')
require((r/'independent-validation.log').stat().st_size>0, 'empty original independent validation log')
trusted=read(r/'trusted-completed-receipt.json')
require(trusted['binary_sha256']==sha(out/'collection_workload_bench')==read(r/'build-completed.json')['binary_sha256'], 'original ELF mismatch')
require(trusted['packet_sha256']==sha(out/'packet.json'), 'original completed packet mismatch')
require(trusted['expected_source_sha256']==sha(r/'independent-expected-source.json'), 'original expected manifest mismatch')
helper=F/'final-A-semantic-equivalence-preparation/compare-certified.py'
require(sha(helper)=='5bc6c7eb0c00b8f78860e31769ac4731f1d1aed2f91013e2a9b1ed7633065c75', 'certified comparison helper mismatch')
spec=importlib.util.spec_from_file_location('root_certified',helper); mod=importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
require(mod.inventory(repo,M)['harness_sha256']==cfg['harness_sha256'], 'independent committed harness mismatch')
results=Path('/home/mikers/gomap-r1-final-results-20261006/M-3325dfe/A'); results.mkdir(parents=True,exist_ok=False)
freeze_path=results/'frozen-source-accepted.json'
freeze(freeze_path,{'accepted':True,'commit':M,'runtime_sha256':cfg['runtime_sha256'],'harness_sha256':cfg['harness_sha256'],'utc':now(),'coordinator':'/root','review_url':cfg['review_url'],'acceptance_scope':'Original source/build/full30-cell oracle provenance and A workload semantics only; numeric cost/performance acceptance pending','trusted_completed_receipt_sha256':sha(r/'trusted-completed-receipt.json')})
review_path=P/'M-3325dfe-A-harness-equivalence-review-original.json'; review=read(review_path)
require(review['M']==M and review['baseline']==mod.BASELINE and review['unchanged_A_blobs']==mod.UNCHANGED, 'root original semantics review differs')
certificate=read(F/'final-A-semantic-equivalence-preparation/certificate.template.json')
certificate['approved']=True
certificate['approval'].update(coordinator='/root',review_url=cfg['review_url'],utc=now(),root_semantic_review_sha256=sha(review_path))
certificate['selected'].update(commit=M,runtime_sha256=cfg['runtime_sha256'],harness_sha256=cfg['harness_sha256'],packet_sha256=sha(out/'packet.json'),receipt_sha256=sha(r/'trusted-completed-receipt.json'),freeze_sha256=sha(freeze_path),observation_sha256={n:sha(r/n) for n in sorted(mod.OBSERVATIONS)})
certificate['selected_harness_blobs']=mod.inventory(repo,M)['harness_blobs']
certificate['reviewed_changed_path_diff_sha256']=review['reviewed_changed_path_diff_sha256']
cert_path=results/'root-approved-A-semantics-certificate.json'; freeze(cert_path,certificate)
original=F/'compare_r1_packets.py'; before=Path('/home/mikers/gomap-r1-evidence-20261005/r1-baseline-landed-0216e2e/packet.json')
# Full independent original observation/certificate verification before command.
mod.verify_certificate(cert_path,sha(cert_path),repo,before,out/'packet.json',r,freeze_path,original)
mod.derive_environment(r,source)
command=[sys.executable,'-B',str(helper),'--certificate',str(cert_path),'--expected-certificate-sha256',sha(cert_path),'--repo',str(repo),'--before',str(before),'--after',str(out/'packet.json'),'--receipts',str(r),'--accepted-freeze',str(freeze_path),'--original-compare',str(original)]
inputs={str(p):sha(p) for p in [cfgp,helper,original,cert_path,freeze_path,review_path,before,out/'packet.json',out/'collection_workload_bench',out/'source.json',out/'source-after.json',r/'trusted-completed-receipt.json',r/'driver.log',*[r/n for n in sorted(mod.OBSERVATIONS)]]}
freeze(results/'comparison-start-original.json',{'utc':now(),'argv':command,'cwd':str(repo),'environment':dict(os.environ),'input_sha256':inputs})
with (results/'comparison.json').open('xb') as stdout,(results/'comparison-stderr-original.log').open('xb') as stderr:
    status=subprocess.call(command,cwd=repo,stdout=stdout,stderr=stderr)
require(all(sha(p)==h for p,h in inputs.items()), 'comparison inputs changed')
freeze(results/'comparison-exit-original.json',{'utc':now(),'exit':status,'stdout_sha256':sha(results/'comparison.json'),'stderr_sha256':sha(results/'comparison-stderr-original.log'),'input_sha256_after':inputs})
require(status==0,'certified comparison failed; preserve original outputs')
freeze(results/'root-provenance-verification-original.json',{'utc':now(),'state':'ROOT_VERIFIED_FULL_A_ORIGINAL_SOURCE_BUILD_ORACLES_VALIDATION_AND_CERTIFIED_BASELINE_COMPARISON_ONLY_NUMERIC_ACCEPTANCE_PENDING','source':source,'config':packet['config'],'cells':len(packet['cells']),'packet_sha256':sha(out/'packet.json'),'binary_sha256':sha(out/'collection_workload_bench'),'certificate_sha256':sha(cert_path),'input_pins':inputs,'comparison_sha256':sha(results/'comparison.json'),'root_qualification_program_sha256':sha(__file__)})
print(json.dumps({'results':str(results),'state':'PROVENANCE_AND_CERTIFIED_COMPARISON_PASS_NUMERIC_ACCEPTANCE_PENDING','packet_sha256':sha(out/'packet.json'),'binary_sha256':sha(out/'collection_workload_bench'),'certificate_sha256':sha(cert_path),'comparison_sha256':sha(results/'comparison.json')}))
