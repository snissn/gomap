#!/usr/bin/env python3
"""Continue preserved A qualification with the separately reviewed provenance adapter."""
import datetime, hashlib, importlib.util, json, os, subprocess, sys
from pathlib import Path
P = Path('/home/mikers/gomap-r1-final-receipts-20261006')
O = Path('/home/mikers/gomap-r1-final-observers-20261006')
F = Path('/home/mikers/gomap-r1-mac-evidence-preservation-20261006')
R = Path('/home/mikers/gomap-r1-final-results-20261006/M-3325dfe/A')
M = '3325dfe77940fec8587d8b61b1ac4e0b2f72caca'
def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def read(p): return json.loads(Path(p).read_text())
def require(v, m):
    if not v: raise ValueError(m)
def freeze(p,v):
    with Path(p).open('x') as f: json.dump(v,f,indent=2,sort_keys=True); f.write('\n')
def now(): return datetime.datetime.now(datetime.timezone.utc).isoformat()
require(os.environ.get('R1_RECEIPT_LOCKED') == 'yes', 'canonical lock required')
cfgp = Path('/home/mikers/gomap-r1-final-configs-20261006/M-3325dfe/A-config.json')
require(sha(cfgp) == 'f2b95b22d5e1c12a00c5cc77dc5846b199535aa3e30e2cc77ed8b8f8b03257b4', 'original config changed')
cfg=read(cfgp); r=Path(cfg['receipt_dir']); out=Path(cfg['out']); repo=Path(cfg['repo'])
require(subprocess.check_output(['git','-C',str(repo),'rev-parse','HEAD'],text=True).strip()==M, 'not actual M')
require(not subprocess.check_output(['git','-C',str(repo),'status','--porcelain','--untracked-files=normal'],text=True).strip(), 'source dirty')
old_start=read(R/'comparison-start-original.json'); old_exit=read(R/'comparison-exit-original.json')
require(old_exit['exit']==1 and (R/'comparison.json').stat().st_size==0, 'original failed comparison not preserved')
require(old_start['input_sha256']==old_exit['input_sha256_after'], 'original comparison input chain changed')
require(all(sha(p)==h for p,h in old_start['input_sha256'].items()), 'original qualification input changed')
require(sha(R/'environment-mismatch-diagnostic-original.json')=='e769683d0a55544898190d12a30827fdc89e1603ea503cdc5f93f7a2eedbe169', 'original diagnosis changed')
helper=O/'M-3325dfe-A-buildinfo-provenance-preparation/compare-certified-v2.py'
require(sha(helper)=='3c4502ebb4620ac010b615ca10bbdd45a4448f66e2c7ed663d57c32413c1c2c2', 'reviewed v2 helper changed')
adoptionp=P/'M-3325dfe-A-buildinfo-provenance-root-adoption-original.json'
adoption=read(adoptionp)
require(adoption['approved'] is True and adoption['helper_sha256']==sha(helper), 'separate root adoption absent')
extension=adoption['approved_certificate_extension']
certificate=read(R/'root-approved-A-semantics-certificate.json')
require(sha(R/'root-approved-A-semantics-certificate.json')=='8b1a409f13d09ad2e5cf91b0cae9c76722a9ab3f3b4d6b3b6305714edb6b4da8', 'original semantic certificate changed')
certificate['buildinfo_provenance']=extension
certp=R/'root-approved-A-semantics-and-provenance-certificate-v2.json'
freeze(certp,certificate)
spec=importlib.util.spec_from_file_location('root_certified_v2',helper); mod=importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
before=F/'captures/r1-baseline-landed-0216e2e/packet.json'; original=F/'compare_r1_packets.py'
mod.verify_certificate(certp,sha(certp),repo,before,out/'packet.json',r,R/'frozen-source-accepted.json',original)
mod.derive_environment(r,read(out/'source.json'))
command=[sys.executable,'-B',str(helper),'--certificate',str(certp),'--expected-certificate-sha256',sha(certp),'--repo',str(repo),'--before',str(before),'--after',str(out/'packet.json'),'--receipts',str(r),'--accepted-freeze',str(R/'frozen-source-accepted.json'),'--original-compare',str(original)]
pins={**old_start['input_sha256'], **{str(p):sha(p) for p in [Path(__file__),helper,adoptionp,certp,before,before.parent/'buildinfo.txt',out/'buildinfo.txt',R/'environment-mismatch-diagnostic-original.json',R/'comparison-start-original.json',R/'comparison-exit-original.json',R/'comparison-stderr-original.log',R/'comparison.json']}}
freeze(R/'comparison-v2-start-original.json',{'utc':now(),'argv':command,'cwd':str(repo),'environment':dict(os.environ),'input_sha256':pins,'scope':extension['approval']['scope']})
with (R/'comparison-v2.json').open('xb') as stdout,(R/'comparison-v2-stderr-original.log').open('xb') as stderr:
    status=subprocess.call(command,cwd=repo,stdout=stdout,stderr=stderr)
require(all(sha(p)==h for p,h in pins.items()), 'v2 comparison inputs changed')
freeze(R/'comparison-v2-exit-original.json',{'utc':now(),'exit':status,'stdout_sha256':sha(R/'comparison-v2.json'),'stderr_sha256':sha(R/'comparison-v2-stderr-original.log'),'input_sha256_after':pins})
require(status==0,'v2 comparison failed; preserve original outputs')
source=read(out/'source.json'); packet=read(out/'packet.json')
require(len(packet['cells'])==30 and all(c['oracle_verified'] is True for c in packet['cells']), 'full30 oracles missing')
freeze(R/'root-provenance-verification-original.json',{'utc':now(),'state':'ROOT_VERIFIED_FULL_A_ORIGINAL_SOURCE_BUILD_ORACLES_VALIDATION_AND_SEPARATELY_CERTIFIED_PROVENANCE_COMPARISON_ONLY_NUMERIC_ACCEPTANCE_PENDING','source':source,'config':packet['config'],'cells':len(packet['cells']),'packet_sha256':sha(out/'packet.json'),'binary_sha256':sha(out/'collection_workload_bench'),'certificate_sha256':sha(certp),'input_pins':pins,'comparison_sha256':sha(R/'comparison-v2.json'),'root_qualification_program_sha256':sha(__file__),'original_failed_comparison_preserved':True,'remeasurement':False})
print(json.dumps({'state':'PROVENANCE_AND_CERTIFIED_COMPARISON_PASS_NUMERIC_ACCEPTANCE_PENDING','comparison_sha256':sha(R/'comparison-v2.json'),'certificate_sha256':sha(certp),'root_provenance_sha256':sha(R/'root-provenance-verification-original.json')}))
