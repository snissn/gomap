#!/usr/bin/env python3
"""Retain the exact failed comparator identities without changing any input."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import runpy

assert os.environ.get('R1_RECEIPT_LOCKED') == 'yes'
F = Path('/home/mikers/gomap-r1-mac-evidence-preservation-20261006')
R = Path('/home/mikers/gomap-r1-final-results-20261006/M-3325dfe/A')
receipts = Path('/home/mikers/gomap-r1-final-receipts-20261006/r1-final-A-M-3325dfe')
before = F / 'captures/r1-baseline-landed-0216e2e/packet.json'
after = Path('/home/mikers/gomap-r1-evidence-20261005/r1-final-A-M-3325dfe/packet.json')
original = F / 'compare_r1_packets.py'
helper = F / 'final-A-semantic-equivalence-preparation/compare-certified.py'
def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
assert sha(original) == '1a54cac528e5c8acc2501ec67b13d9a185b011e1467856741ada320a8bfdcfc8'
assert sha(helper) == '5bc6c7eb0c00b8f78860e31769ac4731f1d1aed2f91013e2a9b1ed7633065c75'
m = runpy.run_path(str(helper))
c, b, a, accepted = m['verify_certificate'](
    R / 'root-approved-A-semantics-certificate.json',
    '8b1a409f13d09ad2e5cf91b0cae9c76722a9ab3f3b4d6b3b6305714edb6b4da8',
    Path('/home/mikers/gomap-r1-final-source-20261006'), before, after,
    receipts, R / 'frozen-source-accepted.json', original)
env = m['derive_environment'](receipts, a['source'])
math = m['adapted_math'](original, c)
base = runpy.run_path(str(original))
eb = base['capture_identity'](before, b)
ea = math['candidate_identity'](after, a, accepted, env)
paths = [Path(__file__), helper, original, before, after,
         before.parent / 'buildinfo.txt', after.parent / 'buildinfo.txt',
         before.parent / 'capture-environment.json',
         R / 'root-approved-A-semantics-certificate.json',
         R / 'frozen-source-accepted.json', R / 'comparison-exit-original.json',
         R / 'comparison-stderr-original.log',
         *[receipts / n for n in sorted(m['OBSERVATIONS'])]]
pins = {str(p): sha(p) for p in paths}
diffs = []
def diff(x, y, p=''):
    if isinstance(x, dict) and isinstance(y, dict):
        for k in sorted(set(x) | set(y)):
            diff(x.get(k), y.get(k), p + '/' + str(k))
    elif x != y:
        diffs.append({'field': p, 'baseline': x, 'actual_M': y})
diff(eb, ea)
assert all(sha(p) == h for p, h in pins.items())
result = {'utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
          'state': 'ORIGINAL_ENVIRONMENT_COMPARISON_FAILED_DIAGNOSIS_ONLY_NO_ACCEPTANCE',
          'original_comparison_exit': json.loads((R / 'comparison-exit-original.json').read_text())['exit'],
          'differences': diffs, 'baseline_identity': eb, 'actual_M_identity': ea,
          'input_sha256_before_and_after': pins,
          'normalization_applied': False, 'remeasurement': False}
with (R / 'environment-mismatch-diagnostic-original.json').open('x') as f:
    json.dump(result, f, indent=2, sort_keys=True)
    f.write('\n')
print(json.dumps({'diagnostic': str(R / 'environment-mismatch-diagnostic-original.json'),
                  'sha256': sha(R / 'environment-mismatch-diagnostic-original.json'),
                  'differing_fields': [d['field'] for d in diffs]}))
