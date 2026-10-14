from pathlib import Path
import hashlib
import json
import os
import subprocess

base = Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013')
repo = base / 'native-4878-traversal'
evidence = base / 'evidence/native-public-bridge-20261015'
frozen = evidence / 'cow-positive-red-v2-frozen'
source_base = '16d1b29593f966ecb9159a1e93e562f115f9f3bb'
artifact_parent = '25910d9d58c27ad564285994bde8b1d2a03f05a4'
ref = 'refs/heads/codex/4878-cow-public-red-20261014'
prefix = 'artifacts/4871/20261014-cow-public-positive-red'
receipt_path = evidence / 'cow-public-red-publication-receipt.json'
assert not receipt_path.exists(), 'Already published; inspect receipt rather than rerunning.'

def sha(data):
    return hashlib.sha256(data).hexdigest()

def git(*args, data=None, env=None):
    return subprocess.check_output(['git', '-C', str(repo), *args], input=data, env=env).strip()

assert git('rev-parse', 'HEAD').decode() == source_base
original_ref = git('symbolic-ref', 'HEAD')
index_path = Path(git('rev-parse', '--git-path', 'index').decode())
if not index_path.is_absolute():
    index_path = repo / index_path
original_index = index_path.read_bytes()
foreign = ['TreeDB/internal/valuelog/native_zstd_rank_census_test.go', 'TreeDB/internal/valuelog/native_zstd_leaf_fixture_export_test.go']
foreign_before = {p: sha((repo / p).read_bytes()) for p in foreign}
assert not git('for-each-ref', '--format=%(refname)', ref)
assert not git('ls-remote', 'origin', ref)

raw_receipt = (frozen / 'receipt.json').read_bytes()
assert sha(raw_receipt) == '0b74fe2976680ca5a3f967d0c5fc016b7da4d12f998c723ae16a28f24c0e4be5'
bound = json.loads(raw_receipt)
assert bound['head'] == source_base and bound['exit'] == 1
members = {'receipt.json': raw_receipt}
sources = {}
for row in bound['source']:
    rel = Path(row['path'])
    assert not rel.is_absolute() and '..' not in rel.parts
    path = frozen / 'source' / rel
    assert path.is_file() and not path.is_symlink()
    data = path.read_bytes()
    assert len(data) == row['bytes'] and sha(data) == row['sha256']
    sources[row['path']] = data

# These four unchanged ABI/support sources are already immutable in the earlier
# capture-v3 packet. They complete replay of the RED against source_base.
support = {
    'TreeDB/mvcc/versions.go': '2a935fc65ea0b08300c61d766dd2cf974ff7bba63aede3138834b678f6915448',
    'TreeDB/db/native_mvcc_current_delete_v6.go': '745a0e98df32384dec577029a8e210020a5acbac78ef5fe9f229a21baf88bb45',
    'TreeDB/mvcc/bounded_prune_whole_current_publication_test.go': '77f14c80a007ce9b918e007e5cce24e5d78ffa022c6a92386cfb58bda246d55f',
    'TreeDB/native_mvcc_route_trace_v6.go': '501ba27fb337330c12c97a24d646cabed304fe5ae8c2266e10e4723b53bdf4c8',
}
for path, expected in support.items():
    data = (evidence / 'capture-v3-source' / path).read_bytes()
    assert sha(data) == expected
    sources[path] = data
for path, data in sources.items():
    members['source/' + path] = data
members['implementation-vs-head.patch'] = (frozen / 'implementation-vs-head.patch').read_bytes()
assert sha(members['implementation-vs-head.patch']) == bound['patch_sha256']
members['cow-positive-red-v2.raw.jsonl'] = (evidence / 'cow-positive-red-v2.raw.jsonl').read_bytes()
assert sha(members['cow-positive-red-v2.raw.jsonl']) == bound['raw_sha256']
members['cow-positive-red-v2.exit'] = (evidence / 'cow-positive-red-v2.exit').read_bytes()
assert members['cow-positive-red-v2.exit'].strip() == b'1'
events = [json.loads(line) for line in members['cow-positive-red-v2.raw.jsonl'].splitlines()]
logs = ''.join(event.get('Output', '') for event in events)
assert 'overlayBytes=123 ACK=0 writes=0 nativePrepared=0 nativeCut=0' in logs
assert 'overlayBytes=137 ACK=0 writes=0 nativePrepared=0 nativeCut=0' in logs
public_summaries = [line for line in logs.splitlines() if 'COW_NATIVE_PUBLIC pointers=' in line]
assert len(public_summaries) == 2
assert all('error=native prune phase 0: operation unsupported by cow_btree cache' in line for line in public_summaries)
assert len([e for e in events if e.get('Action') == 'fail' and e.get('Test', '').count('/') == 1]) == 2

private_index = evidence / 'cow-public-red-isolated.index'
assert not private_index.exists()
env = os.environ.copy()
env['GIT_INDEX_FILE'] = str(private_index)
git('read-tree', source_base, env=env)
git('apply', '--cached', '--whitespace=error', '-', data=members['implementation-vs-head.patch'], env=env)
for path, data in sources.items():
    blob = git('hash-object', '-w', '--stdin', data=data).decode()
    git('update-index', '--add', '--cacheinfo', '100644', blob, path, env=env)
replay_tree = git('write-tree', env=env).decode()
expected_changed = {
    'TreeDB/db/db.go', 'TreeDB/db/native_mvcc_current_delete_v6.go',
    'TreeDB/internal/rootpublication/coordinator.go',
    'TreeDB/internal/rootpublication/native_publication_v6.go',
    'TreeDB/internal/rootpublication/native_publication_v6_test.go',
    'TreeDB/mvcc/versions.go', 'TreeDB/mvcc/prune_cursor_v6.go',
    'TreeDB/mvcc/native_cow_publication_v6_test.go',
    'TreeDB/mvcc/bounded_prune_whole_current_publication_test.go',
    'TreeDB/native_mvcc_route_trace_v6.go',
}
assert set(git('diff-tree', '--no-commit-id', '--name-only', '-r', source_base, replay_tree).decode().splitlines()) == expected_changed
for path, data in sources.items():
    assert subprocess.check_output(['git', '-C', str(repo), 'cat-file', 'blob', replay_tree + ':' + path]) == data
members['source-replay.json'] = (json.dumps({'source_base': source_base, 'replay_tree': replay_tree, 'changed_paths': sorted(expected_changed), 'source': [{'path': p, 'bytes': len(d), 'sha256': sha(d)} for p, d in sorted(sources.items())], 'source_blobs_readback': True, 'rerun_performed': False}, indent=1) + '\n').encode()
members['scripts/publish-cow-public-red-20261014.py'] = Path(__file__).read_bytes()
members['README.md'] = b'''Actual public CWAL/COW positive RED -- 2026-10-14

This diagnostic deliberately FAILS. Both inline and pointer cells enter ProfileCommandWALDurable with cow_btree, four shards and real nonempty overlays (123/137 bytes), then fail native capture at the existing ErrCOWUnsupported guard. No guard was removed. ACK, intervening writes, native preparation and current physical cut remain zero. This is meaningful test-first evidence, not native activation, physical Finish, performance viability, or merge qualification.

The public test seeds a backend key and a newer overlay replacement of the same physical key, retains an old composite read cut, and requires actual current absence plus absence after reopen, keeper/future preservation and intervening durable writes. A backend-only deletion or an empty/drained-cache control cannot qualify it. It runs inline and persistent value-log-pointer cases. Checked cursor Close truthfully repeats the first refusal. Checked old-cut/DB Close and reopen diagnostics run before the final positive assertion.

Replay: create a disposable checkout of source_base in source-replay.json, apply implementation-vs-head.patch, then overlay source/ onto the checkout root. The source/ directory includes four unchanged ABI/support sources from the earlier immutable capture-v3 packet to complete the replay. The private-index replay was verified at every source blob; no second test run is claimed. Use the exact GOROOT and environment in receipt.json, its bin/go, and its recorded go test arguments. No data directory is reused. The original protected 33/80/4096 fixture remains byte-identical at SHA77f14c80a007ce9b918e007e5cce24e5d78ffa022c6a92386cfb58bda246d55f; that adaptive append-only fixture still does not qualify COW.

Next: retain the SAME installed ordered C1/C3 composite cut and its borrowed backend basis, traverse actual overlay sources with revision/duplicate/tombstone semantics, and compose through the canonical current CWAL producer. Preserve the original physical retirement receipt and charges through actual completion. Enqueue through actual publication fence cannot yield with a native pending head that blocks the next same-thread durable write. Do not replace this integration with deleting a refusal or scheduler-driven native callbacks. Accepted architecture: https://github.com/snissn/gomap/issues/4871#issuecomment-6179448232

This branch changes artifacts only. It does not move the active native HEAD, branch or index, change foreign tests, or release source/binaries/data for cleanup. Parent4871 remains INCOMPLETE and all public/physical/final performance gates remain OPEN.
'''
manifest = {'schema': 'cow-public-positive-red-v1', 'source_base': source_base, 'artifact_parent': artifact_parent, 'diagnostic_only': True, 'expected_exit': 1, 'performance_viability': False, 'parent_complete': False, 'members': [{'path': p, 'bytes': len(d), 'sha256': sha(d)} for p, d in sorted(members.items())], 'cleanup_release': []}
members['artifact-manifest.json'] = (json.dumps(manifest, indent=1) + '\n').encode()

git('read-tree', artifact_parent, env=env)
for path, data in sorted(members.items()):
    blob = git('hash-object', '-w', '--stdin', data=data).decode()
    git('update-index', '--add', '--cacheinfo', '100644', blob, prefix + '/' + path, env=env)
artifact_tree = git('write-tree', env=env).decode()
commit = git('commit-tree', artifact_tree, '-p', artifact_parent, data=b'Preserve actual public CWAL COW composite-cut RED and replay sources\n').decode()
assert set(git('diff-tree', '--no-commit-id', '--name-only', '-r', artifact_parent, commit).decode().splitlines()) == {prefix + '/' + p for p in members}
for path, data in members.items():
    assert subprocess.check_output(['git', '-C', str(repo), 'cat-file', 'blob', commit + ':' + prefix + '/' + path]) == data
git('update-ref', ref, commit, '0' * 40)
git('push', 'origin', ref + ':' + ref)
assert git('ls-remote', 'origin', ref).decode().split()[0] == commit
assert git('rev-parse', 'HEAD').decode() == source_base and git('symbolic-ref', 'HEAD') == original_ref
assert index_path.read_bytes() == original_index
assert {p: sha((repo / p).read_bytes()) for p in foreign} == foreign_before
index_sha = sha(private_index.read_bytes())
index_bytes = private_index.stat().st_size
assert git('write-tree', env=env).decode() == artifact_tree
private_index.unlink()
assert not private_index.exists() and index_path.read_bytes() == original_index
result = {'commit': commit, 'ref': ref, 'url': f'https://github.com/snissn/gomap/tree/{commit}/{prefix}', 'source_base': source_base, 'replay_tree': replay_tree, 'members': len(members), 'bytes': sum(len(d) for d in members.values()), 'manifest_sha256': sha(members['artifact-manifest.json']), 'all_git_blobs_readback': True, 'remote_ref_verified': True, 'active_head_index_ref_unchanged': True, 'foreign_tests_preserved': True, 'diagnostic_only': True, 'cleanup': {'only_path': str(private_index), 'bytes': index_bytes, 'sha256': index_sha, 'deleted_verified': True, 'containing_tree': artifact_tree}, 'cleanup_release': []}
receipt_path.write_text(json.dumps(result, indent=1) + '\n')
print(json.dumps(result), flush=True)
