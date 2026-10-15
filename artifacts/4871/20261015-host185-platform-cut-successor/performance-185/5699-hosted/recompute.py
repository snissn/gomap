import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys

D = Path('/mnt/fast4tb/gomap-mvcc-4871-20261015')
W = D / 'ordinary-repair'
O = D / 'performance-185/5699-hosted'
P = O / 'raw-primary'
base = '5bca87ffe6e1bfba2d693264d2dc82936adc234f'
head = '5699f8e791c753f204cc297416e0b742d415afe3'
def digest(p):
    return hashlib.sha256(p.read_bytes()).hexdigest()

checker = O / 'checker-5699.py'
checker.write_bytes(subprocess.check_output(['git','show',head+':.github/scripts/check_mvcc_raw_path_gate.py'],cwd=W))
args = [sys.executable,str(checker),'--baseline',str(P/'baseline.txt'),'--candidate',str(P/'candidate.txt'),'--baseline-sha',base,'--candidate-sha',head]
for package in ['db','caching','treedb']:
    for revision in ['baseline','candidate']:
        args += ['--'+revision+'-'+package+'-binary',str(P/'measured-binaries'/(revision+'-'+package+'.test'))]
args += ['--expected-samples','8','--max-regression-percent','5','--max-bytes-regression-percent','1','--max-bytes-regression-absolute','64','--json-output',str(O/'root-raw-summary.json'),'--markdown-output',str(O/'root-raw-summary.md')]
subprocess.run(args,check=True)
raw = json.loads((P/'summary.json').read_text())
assert json.loads((O/'root-raw-summary.json').read_text()) == raw
retained = json.loads((P/'measured-binaries.json').read_text())
for f in retained['files']:
    assert digest(P/f['path']) == f['sha256']
    assert f['source_sha'] in [base,head]
assert digest(P/'summary.json') == retained['checker_summary_sha256']

primary = P/'primary'
runner = O/'primary-runner-5699.py'
runner.write_bytes(subprocess.check_output(['git','show',head+':scripts/mvcc_primary_cost_gate.py'],cwd=W))
spec = importlib.util.spec_from_file_location('primary_5699',runner)
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
rows=[]
for pair in range(8):
    for revision in (['baseline','candidate'] if pair%2==0 else ['candidate','baseline']):
        rows.append({'pair':pair,'revision':revision,'metrics':module.parse_sample((primary/f'pair-{pair+1}-{revision}.txt').read_text())})
assert rows == json.loads((primary/'samples.json').read_text())
result=module.evaluate(rows)
summary=json.loads((primary/'summary.json').read_text())
assert all(summary[k] == v for k,v in result.items())
assert summary['baseline'] == base and summary['candidate'] == head and summary['accepted'] and summary['quiet']
bindings=json.loads((primary/'bindings.json').read_text())
assert bindings['baseline'] == base and bindings['candidate'] == head
assert digest(runner) == bindings['runner_sha256']
assert digest(primary/'primary_generation_routing_v6_test.go') == bindings['harness_sha256']
for rev, f in bindings['binaries'].items():
    assert digest(primary/f['path']) == f['sha256']
    assert f['source_sha'] == (base if rev=='baseline' else head)
for name in ['preflight.json','measured-processes.json']:
    assert not json.loads((primary/name).read_text())['busy']
(O/'root-primary-summary.json').write_text(json.dumps(summary,indent=2)+'\n')
receipt={'base':base,'head':head,'raw_summary_equal':True,'raw_binaries_verified':6,'primary_samples_reparsed':16,'primary_summary_equal':True,'primary_binaries_verified':2,'primary_runner_and_harness_verified':True,'primary_quiet_observation':True,'scope':'Exact hosted matrix only; no causal attribution of Get improvement or final graph qualification','raw_paired_deltas':{r['benchmark']:r['paired_ns_delta_percent'] for r in raw['results']},'primary':summary}
(O/'root-verification.json').write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps(receipt,indent=2))
