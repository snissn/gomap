from pathlib import Path
import hashlib
import json
import subprocess

base = Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013')
out = base / 'evidence/root-empty-checkpoint-production-placement-checks-20261014'
expected = {
    'TreeDB/public.go': '77f871c4a65f95648b9d2af85dbf9cc9ba50dc5fdca174eb36d2cc576ba846dc',
    'TreeDB/cow_public_primary_selection_test.go': '9f12c15b0d095ca370903644c0f0ac32198a299c06ebc7f0817ead0c48db67de',
    'TreeDB/docs/spec/write-path-and-durability.md': '5ca1f41c8a004ec6f82f75041068a72e5c506b068798a67f4f2d516e23c645ea',
}
parents = {
    'ordinary-5111': 'a99d3c8474b8898b98624c47758653e7c8cb9b7f',
    'native-4878-traversal': 'aa33e81993cd599e2fdc02f86fd0ed229880d8ec',
}
checks = json.loads((out / 'receipt.json').read_text())
assert len(checks) == 2 and all(c['exit'] == 0 and c['source_status_unchanged'] for c in checks)
assert not (out / 'production-commit-receipt.json').exists()
result = []
for lane, parent in parents.items():
    repo = base / lane
    def git(*args):
        return subprocess.check_output(['git', '-C', str(repo), *args], text=True).strip()
    assert git('rev-parse', 'HEAD') == parent
    assert not git('diff', '--cached', '--name-only')
    status = subprocess.check_output(['git', '-C', str(repo), 'status', '--porcelain=v1', '-uall'], text=True).splitlines()
    unrelated = [s for s in status if s[3:] not in expected]
    other_paths = [s[3:] for s in unrelated]
    other_hashes = {p: hashlib.sha256((repo / p).read_bytes()).hexdigest() for p in other_paths if (repo / p).is_file()}
    assert {p: hashlib.sha256((repo / p).read_bytes()).hexdigest() for p in expected} == expected
    assert set(p for p in git('diff', '--name-only').splitlines() if p in expected) == set(expected)
    git('diff', '--check', '--', *expected)
    git('add', '--', *expected)
    assert set(git('diff', '--cached', '--name-only').splitlines()) == set(expected)
    git('commit', '-m', 'Refresh the COW basis after public checkpoint fallback maintenance')
    commit = git('rev-parse', 'HEAD')
    assert git('rev-parse', commit + '^') == parent
    assert set(git('diff-tree', '--no-commit-id', '--name-only', '-r', parent, commit).splitlines()) == set(expected)
    for path, digest in expected.items():
        data = subprocess.check_output(['git', '-C', str(repo), 'show', commit + ':' + path])
        assert hashlib.sha256(data).hexdigest() == digest
    assert subprocess.check_output(['git', '-C', str(repo), 'status', '--porcelain=v1', '-uall'], text=True).splitlines() == unrelated
    assert {p: hashlib.sha256((repo / p).read_bytes()).hexdigest() for p in other_hashes} == other_hashes
    ref = git('symbolic-ref', 'HEAD')
    git('push', 'origin', ref + ':' + ref)
    assert git('ls-remote', 'origin', ref).split()[0] == commit
    result.append({'lane': lane, 'parent': parent, 'commit': commit, 'ref': ref,
                   'source': expected, 'unrelated_preserved': len(unrelated),
                   'readback_all_members': True, 'merged': False,
                   'qualification': 'Focused public-primary checks only; full node gates remain open'})
    print(json.dumps(result[-1]), flush=True)
    (out / 'production-commit-receipt.json').write_text(json.dumps(result, indent=1) + '\n')
