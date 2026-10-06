"""Strict contract for existing default-mode read diagnostics; no fabricated samples."""
import hashlib
import json
import math
from pathlib import Path
import re

COMMITS = {'main': '7f91eb6ca0dcdd6ec65703eb9dcb23371917ff5a',
           'candidate': 'b9e5587fb01cb49e6eddd576b068e30c3ca2ec63'}
FIXTURES = {'TreeDB/bench_test.go': 'dfaed5aaee7f264ac448dca5b07f9a549db90d526904d23b57a0f2b632cb232c',
            'TreeDB/snapshot_read_bench_test.go': '8d0244fab4707e68b5f96d18e4445a18dc5b419895c5c9a33e8455daeb9030e0'}
PATTERN = '^(BenchmarkReadUnderWriteCached|BenchmarkDBCheckpointedValueLogGet)$/^(W=0|Get|GetAppend)$'
CASES = ('BenchmarkReadUnderWriteCached/W=0', 'BenchmarkDBCheckpointedValueLogGet/Get',
         'BenchmarkDBCheckpointedValueLogGet/GetAppend')
ORDERS = (('main', 'candidate'), ('candidate', 'main'), ('main', 'candidate'))
METRICS = ('ns/op', 'B/op', 'allocs/op')
NAME = re.compile(r'^(Benchmark[^\s]+)-12\s+(.*)$')
OPERATIONS = 100000


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write(path, data):
    path.write_text(json.dumps(data, indent=2) + '\n')


def identity(path, role):
    data = json.loads(path.read_text())
    if data.get('commit', data.get('candidate_sha')) != COMMITS[role]:
        raise ValueError('wrong ' + role + ' commit')
    files = data['files']
    if isinstance(files, dict):
        files = [{'path': p, 'sha256': h} for p, h in sorted(files.items())]
    seen = set()
    for item in files:
        p = Path(item['path'])
        if p.is_absolute() or '..' in p.parts or item['path'] in seen or not re.fullmatch(r'[0-9a-f]{64}', item['sha256']):
            raise ValueError('invalid or duplicate full-manifest input')
        seen.add(item['path'])
    if not seen:
        raise ValueError('empty manifest')
    tree = hashlib.sha256(''.join(f'{x["sha256"]}  {x["path"]}\n' for x in sorted(files, key=lambda x:x['path'])).encode()).hexdigest()
    if 'source_tree_sha256' in data and data['source_tree_sha256'] != tree:
        raise ValueError('manifest tree hash differs')
    data.update(commit=COMMITS[role], files=files, file_count=len(files), source_tree_sha256=tree)
    for p, digest in FIXTURES.items():
        if {x['path']: x['sha256'] for x in files}.get(p) != digest:
            raise ValueError('changed default fixture: ' + p)
    return data


def drift(source, manifest):
    expected = {x['path']:x['sha256'] for x in manifest['files']}
    changed = [p for p,h in expected.items() if not (source/p).is_file() or
               (source/p).is_symlink() or sha(source/p) != h]
    actual = {p.relative_to(source).as_posix() for p in source.rglob('*') if p.is_file() or p.is_symlink()}
    return changed + ['EXTRA:' + p for p in sorted(actual - expected.keys())]


def parse_row(line):
    match = NAME.fullmatch(line)
    if not match:
        if line.startswith('Benchmark'):
            raise ValueError('malformed/unexpected benchmark row: ' + line)
        return None
    case, rest = match.groups()
    if case not in CASES:
        raise ValueError('unexpected benchmark case: ' + case)
    tokens = rest.split()
    if len(tokens) < 7 or (len(tokens)-1)%2 or tokens[0] != str(OPERATIONS):
        raise ValueError('wrong count or malformed metrics')
    metrics = {}
    for i in range(1,len(tokens),2):
        unit = tokens[i+1]
        if unit in metrics:
            raise ValueError('duplicate metric')
        value = float(tokens[i])
        if not math.isfinite(value) or value < 0:
            raise ValueError('nonfinite/negative metric')
        metrics[unit] = value
    if set(METRICS)-metrics.keys() or metrics['ns/op'] <= 0:
        raise ValueError('missing metrics or nonpositive latency')
    return {'case':case,'iterations':OPERATIONS,'metrics':metrics}


def validate_run(stdout, stderr):
    text = stdout.read_text()
    if not re.search(r'^PASS$', text, re.M):
        raise ValueError('missing package PASS')
    if re.search(r'WARNING: DATA RACE|fatal error:|panic:|^FAIL', text+'\n'+stderr.read_text(),re.M):
        raise ValueError('runtime/package failure')
    rows = [r for line in text.splitlines() if (r:=parse_row(line)) is not None]
    if len(rows)!=3 or {r['case'] for r in rows}!=set(CASES):
        raise ValueError('missing/duplicate/extra case')
    return rows
