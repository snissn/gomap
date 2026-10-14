from pathlib import Path
import datetime
import hashlib
import json
import subprocess

base = Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013')
evidence = base / 'evidence/ordinary-5111/captured-ge-production-integration-20261014'
out = base / 'evidence/coherent-captured-ge-checkpoint-20261014'

def sha(data):
    return hashlib.sha256(data).hexdigest()

assert sha((evidence / 'receipt.json').read_bytes()) == 'fabdf4e0e33c8f4f1c94fb75edeca13818156ff5fd672e52de6be591672c0478'
assert not out.exists()
targets = json.loads((evidence / 'targets.json').read_text())
assert len(targets) == 9
plans = []
for lane, count in [('ordinary-5111', 135), ('native-4878-traversal', 9)]:
    repo = base / lane
    frozen = json.loads((evidence / (lane + '.final-source.json')).read_text())
    def git(*args):
        return subprocess.check_output(['git', '-C', str(repo), *args], text=True).strip()
    assert git('rev-parse', 'HEAD') == frozen['head']
    assert not git('diff', '--cached', '--name-only')
    index = Path(git('rev-parse', '--git-path', 'index'))
    if not index.is_absolute():
        index = repo / index
    assert sha(index.read_bytes()) == frozen['index_sha256']
    current_status = subprocess.check_output(['git', '-C', str(repo), 'status', '--porcelain=v1', '-uall'], text=True)
    assert current_status == frozen['status']
    for rel, row in frozen['files'].items():
        p = repo / rel
        assert p.is_file() and not p.is_symlink(), rel
        data = p.read_bytes()
        assert len(data) == row['bytes'] and sha(data) == row['sha256'], rel
    paths = [line[3:] for line in current_status.splitlines()]
    if lane.startswith('ordinary'):
        selected = paths
        # All 135 graph-owned paths preserve the exact focused-tested source,
        # including the redundant causal diagnostic (not a completion gate).
        assert len(selected) == 135 and set(targets) <= set(selected)
        assert all(p.startswith('TreeDB/') and p.endswith(('.go', '.md')) for p in selected)
    else:
        selected = targets
        assert set(paths) - set(selected) == {
            'TreeDB/internal/valuelog/native_zstd_rank_census_test.go',
            'TreeDB/internal/valuelog/native_zstd_leaf_fixture_export_test.go',
        }
    assert len(selected) == count and len(set(selected)) == count
    git('diff', '--check', '--', *selected)
    plans.append({'lane': lane, 'repo': repo, 'frozen': frozen, 'selected': selected,
                  'unrelated': [line for line in current_status.splitlines() if line[3:] not in selected]})

out.mkdir()
result = []
for plan in plans:
    repo = plan['repo']
    def git(*args):
        return subprocess.check_output(['git', '-C', str(repo), *args], text=True).strip()
    selected = plan['selected']
    parent = plan['frozen']['head']
    assert git('rev-parse', 'HEAD') == parent
    assert not git('diff', '--cached', '--name-only')
    source = {p: plan['frozen']['files'][p] for p in selected}
    git('add', '--', *selected)
    assert set(git('diff', '--cached', '--name-only').splitlines()) == set(selected)
    git('diff', '--cached', '--check')
    message = ('Checkpoint coherent ordinary MVCC lifecycle and captured successor candidate'
               if plan['lane'].startswith('ordinary')
               else 'Use the captured fixed-scratch successor through public MVCC reads')
    git('commit', '-m', message)
    commit = git('rev-parse', 'HEAD')
    assert git('rev-parse', commit + '^') == parent
    assert set(git('diff-tree', '--no-commit-id', '--name-only', '-r', parent, commit).splitlines()) == set(selected)
    for rel, row in source.items():
        data = subprocess.check_output(['git', '-C', str(repo), 'show', commit + ':' + rel])
        assert len(data) == row['bytes'] and sha(data) == row['sha256'], rel
    after_status = subprocess.check_output(['git', '-C', str(repo), 'status', '--porcelain=v1', '-uall'], text=True).splitlines()
    assert after_status == plan['unrelated']
    for rel, row in plan['frozen']['files'].items():
        data = (repo / rel).read_bytes()
        assert len(data) == row['bytes'] and sha(data) == row['sha256'], rel
    ref = git('symbolic-ref', 'HEAD')
    git('push', 'origin', ref + ':' + ref)
    assert git('ls-remote', 'origin', ref).split()[0] == commit
    row = {'lane': plan['lane'], 'parent': parent, 'commit': commit, 'ref': ref,
           'source': source, 'source_count': len(source),
           'all_source_blobs_readback': True,
           'all_frozen_source_entries_preserved': len(plan['frozen']['files']),
           'unrelated_status': plan['unrelated'], 'merged': False,
           'qualification': 'Construction checkpoint with focused normal/race and bounded diagnostic evidence; full node/final gates OPEN'}
    result.append(row)
    (out / 'receipt.json').write_text(json.dumps({'at_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'lanes': result}, indent=1) + '\n')
    print(json.dumps({k: v for k, v in row.items() if k != 'source'}), flush=True)
