import datetime
import hashlib
import json
import math
import statistics
from pathlib import Path

D = Path(__file__).resolve().parent
R = D / 'raw'
j = json.loads((R / 'results.json').read_text())
allrows, checks, rounding = {}, {}, []
for arm, prefix in [('legacy', 'BenchmarkVacuumIndexOnlineCollectionForegroundChurn/bytes_64x-2'), ('public', 'BenchmarkPL06ExternalVacuumCollectionForegroundChurn/bytes_64x-2')]:
    rows = []
    for p in sorted((R / 'raw').glob(arm + '-*.txt')):
        raw = p.read_text()
        lines = [s for s in raw.splitlines() if s.startswith('Benchmark')]
        assert len(lines) == 1 and lines[0].split()[0] == prefix and '\nPASS\n' in raw
        t = lines[0].split()
        assert int(t[1]) == 1 and len(t[2:]) % 2 == 0
        m = {t[i+1]: float(t[i]) for i in range(2, len(t), 2)}
        assert all(math.isfinite(v) and v >= 0 for v in m.values())
        rows.append({'sample': int(p.stem.split('-')[-1]), 'metrics': m, 'path': str(p.relative_to(D)), 'sha256': hashlib.sha256(p.read_bytes()).hexdigest()})
    assert len(rows) == 10
    for metric, s in j['metrics'][arm].items():
        vals = [r['metrics'][metric] for r in rows]
        mean = statistics.fmean(vals)
        actual = {'samples': vals, 'mean': mean, 'median': statistics.median(vals), 'cv': statistics.stdev(vals) / mean if mean else 0.0}
        for k in ['samples', 'mean', 'median']:
            assert actual[k] == s[k], (arm, metric, k)
        assert math.isclose(actual['cv'], s['cv'], rel_tol=1e-14, abs_tol=1e-16), (arm, metric, 'cv')
        if actual['cv'] != s['cv']:
            rounding.append({'arm': arm, 'metric': metric, 'local_cv': actual['cv'], 'hosted_cv': s['cv'], 'absolute_difference': abs(actual['cv']-s['cv'])})
    allrows[arm] = sorted(rows, key=lambda r: r['sample'])
    checks[arm + '_all_raw_samples_means_medians_exact'] = True
for r in allrows['legacy']:
    m = r['metrics']
    assert all(m[k] == 0 for k in ['concurrent-aborts/op', 'vacuum-errors/op', 'foreground-exposure-misses/op'])
    assert m['foreground-overlap-samples/op'] == 4096 and m['foreground-points/op'] == 2048 and m['foreground-ranges/op'] == 2048
for r in allrows['public']:
    m = r['metrics']
    assert all(m[k] == 0 for k in ['vacuum-unexpected-errors/op', 'vacuum-unsupported/op', 'foreground-exposure-misses/op'])
    assert m['foreground-overlap-samples/op'] > 0
assert j['environment']['git_sha'] == '72e01efd767ef5d766f070e531fb53ea3f22674d' and j['environment']['dirty_state'] == 'clean'
assert all(j['gates'].values()) and j['public_status'] == 'production-index-vacuum-available'
metrics = {k: j['metrics']['legacy'][k] for k in ['vacuum-total-ns/op', 'max-writer-pause-ns', 'foreground-p99-ns/op']}
assert all(v['cv'] <= .1 for v in metrics.values())
out = {'head': j['environment']['git_sha'], 'at_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'gate': 'PASS', 'raw_recomputation': checks, 'cv_floating_point_rounding': rounding, 'original_cv_gate': .1, 'legacy_metrics': metrics, 'vacuum_ops_per_sec': 1e9 / metrics['vacuum-total-ns/op']['median'], 'public_status': j['public_status'], 'fixture': j['fixture']['fixture'], 'chronological_rows': allrows, 'limitations': ['M0 same-head stability and public exposure only; no matched product revision cost claim', 'Prior5699 two failed attempts retained unchanged; successor result does not retroactively qualify either', 'No quiet-host telemetry or retained ELF in original M0 harness']}
(D / 'root-verification.json').write_text(json.dumps(out, indent=2) + '\n')
print(json.dumps({k: out[k] for k in ['head', 'gate', 'raw_recomputation', 'vacuum_ops_per_sec']}, indent=2))
print({k: v['cv'] for k, v in metrics.items()})
