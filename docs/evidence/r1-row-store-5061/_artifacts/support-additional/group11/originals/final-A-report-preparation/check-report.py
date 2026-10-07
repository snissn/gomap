"""Bounded Python formatting/arithmetic check; no new benchmark or acceptance."""
from pathlib import Path
import copy
import hashlib
import importlib.util
import json
import subprocess
import sys

ROOT = Path(__file__).resolve().parent
BASE = ROOT.parent / 'captures/r1-baseline-landed-0216e2e'
PACKET = BASE / 'packet.json'
ORIGINAL = ROOT.parent / 'compare_r1_packets.py'


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


helper = load('report_a', ROOT / 'report-a.py')
original = load('original_compare', ORIGINAL)
inputs = {str(p): digest(p) for p in (PACKET, BASE / 'summary.json', ORIGINAL)}
checks = []
commands = []


def check(name, condition):
    assert condition, name
    checks.append(name)


def run(argv, expected):
    result = subprocess.run(argv, capture_output=True, text=True)
    commands.append({'argv': argv, 'exit_code': result.returncode,
                     'stdout': result.stdout, 'stderr': result.stderr})
    check('actual child exit ' + str(expected), result.returncode == expected)
    return result


argv = [sys.executable, str(ROOT / 'report-a.py'), str(PACKET),
        '--json-out', str(ROOT / 'verified-baseline-format.json'),
        '--markdown-out', str(ROOT / 'verified-baseline-format.md')]
run(argv, 0)
r = json.loads((ROOT / 'verified-baseline-format.json').read_text())
p = json.loads(PACKET.read_text())
check('all 30 raw original cells retained exactly', r['raw_cells'] == p['cells'] and len(r['raw_cells']) == 30)
check('all six engines and actual five repetitions', len(r['engine_cells']) == 6 and all(e['observed_repetition_ids'] == [0,1,2,3,4] for e in r['engine_cells']))
enabled = [g for g in r['phase_groups'] if not g['skipped']]
skips = [g for g in r['phase_groups'] if g['skipped']]
check('92 enabled groups and 4 skipped groups', len(enabled) == 92 and len(skips) == 4)
check('skips have original reason and no timing aggregate', all(g['metrics'] is None and g['observed_repetitions'] == 5 for g in skips))
orig = original.groups(p)
for g in enabled:
    expected = original.describe(orig[(g['engine'],g['phase'])])
    for key in ('ns_per_op','ops_per_sec','bytes_per_op','allocs_per_op','p50_ns','p95_ns','p99_ns'):
        check(f"{g['engine']}/{g['phase']} {key} original math", g['metrics'][key]['median'] == expected[key + '_median'])
    check(f"{g['engine']}/{g['phase']} original spread", g['metrics']['ops_per_sec']['spread_fraction'] == expected['throughput_spread'])
    for repetition in g['repetitions']:
        m = repetition['measurement']
        expected_rows = m['ops_per_sec'] * m['rows'] / m['operations'] if m['rows'] else None
        check(f"{g['engine']}/{g['phase']} repetition {repetition['repetition']} rows denominator", repetition['rows_per_sec'] == expected_rows)
check('69 historical timing groups still inconclusive', sum(g['descriptive_timing_spread'] == 'inconclusive_above_15pct' for g in enabled) == 69)
for storage in r['engine_storage']:
    cells = [c for c in p['cells'] if c['engine'] == storage['engine']]
    check('raw storage strings/components preserved ' + storage['engine'], storage['raw_declared_components'] == [dict(repetition=c['repetition'],persistent_bytes=c['persistent_bytes'],wal_bytes=c['wal_bytes'],transient_bytes=c['transient_bytes'],stats=c.get('stats',{})) for c in cells])
check('per-repetition percentiles and SQLite C exclusion explicit', 'not pooled' in (ROOT / 'verified-baseline-format.md').read_text() and 'exclude SQLite C allocations' in (ROOT / 'verified-baseline-format.md').read_text())
output_pins = {str(q): digest(q) for q in (ROOT / 'verified-baseline-format.json', ROOT / 'verified-baseline-format.md')}
run(argv, 2)
check('overwrite refusal leaves original outputs identical', all(digest(Path(q)) == sha for q,sha in output_pins.items()))
# Bounded synthetic formatting case: no measurement/promotion, only separation.
changed = copy.deepcopy(p)
changed['cells'][0]['phases'][0]['rows'] -= 1
synthetic = ROOT / 'synthetic-denominator-format-only.json'
with synthetic.open('x') as f:
    json.dump(changed, f)
separated = helper.report(synthetic)
load_groups = [g for g in separated['phase_groups'] if g['engine'] == 'json' and g['phase'] == 'load']
check('different denominator stays separate, never averaged', sorted(g['observed_repetitions'] for g in load_groups) == [1,4] and len({g['rows'] for g in load_groups}) == 2)
changed['cells'][0]['phases'][0]['ops_per_sec'] = float('nan')
invalid = ROOT / 'synthetic-nonfinite-format-only.json'
with invalid.open('x') as f:
    json.dump(changed, f)
run([sys.executable, str(ROOT / 'report-a.py'), str(invalid), '--format','json'], 1)
check('all original packet/summary/comparator bytes unchanged', all(digest(Path(path)) == sha for path,sha in inputs.items()))
result = {'status':'PASS_PYTHON_FORMATTING_ONLY_NOT_MEASUREMENT_ACCEPTANCE','check_count':len(checks),
          'checks':checks,'original_input_pins':inputs,'commands':commands,'output_pins':output_pins}
with (ROOT / 'checks.json').open('x') as f:
    json.dump(result,f,indent=2); f.write('\n')
print(json.dumps({'status':result['status'],'check_count':len(checks),'enabled_groups':len(enabled),'skipped_groups':len(skips)}))
