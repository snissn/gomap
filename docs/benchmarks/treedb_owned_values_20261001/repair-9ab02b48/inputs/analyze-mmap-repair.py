#!/usr/bin/env python3
"""Fail closed on the affected-only row/route/retention matrix, then summarize."""
from pathlib import Path
import hashlib
import json
import math
import re
import statistics
import sys

INTERNAL = {
    'BenchmarkFileReadAppendCompressedFallback/nil_dst',
    'BenchmarkFileReadAppendCompressedFallback/reused_dst',
    'BenchmarkFileReadAppendOwned/mmap_decode',
    'BenchmarkFileReadAppendOwned/cold_open_map',
}
DENIED = {'BenchmarkOwnedMmapBudgetDenied512/reused_false',
          'BenchmarkOwnedMmapBudgetDenied512/reused_true'}
ROWS = {
    'base': {'internal': INTERNAL, 'overlay': DENIED,
             'public': {'BenchmarkDBOwnedValueLogRoute'}},
    'previous': {'internal': {'BenchmarkFileReadAppendOwned/cold_open_map'},
                 'overlay': DENIED | {'BenchmarkOwnedMmapConcurrentFirstAdmission'}},
    'candidate': {'internal': INTERNAL,
                  'overlay': DENIED | {'BenchmarkOwnedMmapConcurrentFirstAdmission'},
                  'public': {'BenchmarkDBOwnedValueLogRoute'}},
}

def check_cell(root, rev, block, rep):
    expected_names = ROWS[rev][block]
    p = root / f'repair-{block}-{rev}-{rep}.txt'
    current = None
    seen = set()
    samples = []
    for line in p.read_text().splitlines():
        name = re.match(r'^(Benchmark\S+)', line)
        if name:
            current = name[1]
            line = line[len(current):]
        row = re.match(r'^\s*(\d+)\s+([0-9.eE+-]+)\s+ns/op\s+(.*)', line)
        if not row:
            continue
        assert current and current.endswith('-4'), (p, current)
        canonical = current[:-2]
        assert canonical in expected_names and canonical not in seen, (p, canonical, seen)
        seen.add(canonical)
        tokens = row[3].split()
        assert len(tokens) % 2 == 0, p
        units = tokens[1::2]
        assert len(set(units)) == len(units), (p, units)
        m = {'ns/op': float(row[2]), **{tokens[i+1]: float(tokens[i]) for i in range(0, len(tokens), 2)}}
        assert int(row[1]) > 0 and m['ns/op'] > 0 and all(math.isfinite(v) and (v >= 0 or k == 'warm_heap_delta_B') for k, v in m.items()), (p, m)
        assert {'B/op', 'allocs/op'} <= set(m), (p, m)
        if block == 'public':
            expected = {'pointer_hits/op': 1, 'inline_hits/op': 0, 'crc_checks/op': 1,
                        'cache_hits/op': 1, 'fallbacks/op': int(rev == 'base'),
                        'mmap_hits/op': int(rev == 'candidate'),
                        'retained_raw_B': 9389893, 'raw_budget_B': 67108864}
            assert all(m[k] == v for k, v in expected.items()), (p, m)
        if canonical in DENIED:
            expected = {'fallbacks/op': 1, 'crc_checks/op': 1, 'cache_hits/op': 1,
                        'retained_raw_B': 8192, 'denial_events': 1}
            assert all(m[k] == v for k, v in expected.items()), (p, m)
            reused = canonical.endswith('/reused_true')
            assert m['B/op'] == (0 if reused else 512) and m['allocs/op'] == (0 if reused else 1), (p, m)
        if canonical == 'BenchmarkOwnedMmapConcurrentFirstAdmission':
            assert all(m[k] == v for k, v in {'mmap_hits/op': 2, 'crc_checks/op': 2, 'fallbacks/op': 0}.items()), (p, m)
        if canonical.endswith('/mmap_decode'):
            assert all(m[k] == v for k, v in {'fallbacks/op': 0, 'cache_hits/op': 0, 'retained_raw_B': 0, 'crc_checks/op': 1}.items()), (p, m)
        if canonical.endswith('/cold_open_map'):
            assert m['mmap_hits/op'] == int(rev != 'base') and m['fallbacks/op'] == int(rev == 'base') and m['cache_stores/op'] == 1, (p, m)
        samples.append({'revision': rev, 'block': block, 'repetition': rep,
                        'benchmark': canonical, 'iterations': int(row[1]), 'metrics': m})
    assert seen == expected_names, (p, seen, expected_names)
    # Read benchmark stdout alone; diagnostics retain a separate stream.
    (root / f'repair-{block}-{rev}-{rep}.stderr.txt').read_bytes()
    rss = (root / f'repair-{block}-{rev}-{rep}.rss.txt').read_text()
    assert re.search(r'Exit status: 0\b', rss), p
    peak = re.search(r'Maximum resident set size \(kbytes\): (\d+)', rss)
    assert peak and int(peak[1]) > 0, p
    return samples

def main():
    root = Path(sys.argv[1])
    if len(sys.argv) == 6 and sys.argv[2] == '--cell':
        samples = check_cell(root, sys.argv[3], sys.argv[4], int(sys.argv[5]))
        print(f'PASS: {len(samples)} exact rows, route/residency/exit gates')
        return
    assert len(sys.argv) == 2
    samples = []
    expected_rss = set()
    for rev, blocks in ROWS.items():
        for block in blocks:
            for rep in range(1, 6):
                samples.extend(check_cell(root, rev, block, rep))
                expected_rss.add(f'repair-{block}-{rev}-{rep}.rss.txt')
    assert len(samples) == 95, len(samples)
    summary = {}
    for name in sorted({s['benchmark'] for s in samples}):
        by = {}
        for rev in ROWS:
            rs = [s for s in samples if (s['benchmark'], s['revision']) == (name, rev)]
            if not rs:
                continue
            assert len(rs) == 5, (name, rev, len(rs))
            assert all(set(r['metrics']) == set(rs[0]['metrics']) for r in rs), (name, rev)
            by[rev] = {k: {'median': statistics.median(r['metrics'][k] for r in rs),
                           'min': min(r['metrics'][k] for r in rs),
                           'max': max(r['metrics'][k] for r in rs)} for k in rs[0]['metrics']}
        summary[name] = by
    expected_stderr = {name.replace('.rss.txt', '.stderr.txt') for name in expected_rss}
    actual_stderr = {p.name for p in root.glob('repair-*.stderr.txt')}
    assert actual_stderr == expected_stderr, (actual_stderr, expected_stderr)
    actual_rss = {p.name for p in root.glob('repair-*.rss.txt')}
    assert actual_rss == expected_rss, (actual_rss, expected_rss)
    rss = {name: int(re.search(r'Maximum resident set size \(kbytes\): (\d+)', (root/name).read_text())[1]) for name in sorted(actual_rss)}
    assert (root/'repair-freeze-before.txt').read_bytes() == (root/'repair-freeze-after.txt').read_bytes()
    raw_names = expected_rss | expected_stderr | {name.replace('.rss.txt', '.txt') for name in expected_rss}
    raw_sha256 = {name: hashlib.sha256((root/name).read_bytes()).hexdigest() for name in sorted(raw_names)}
    (root/'repair-analysis.json').write_text(json.dumps({'samples': samples, 'summary': summary,
        'setup_inclusive_peak_RSS_KiB': rss, 'raw_stdout_stderr_rss_sha256': raw_sha256, 'analyzer_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}, indent=2)+'\n')
    print('PASS: 95 cells, 40 fresh-process RSS observations; exact route/residency gates verified')

if __name__ == '__main__':
    main()
