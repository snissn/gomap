from pathlib import Path
import hashlib
import json
import os
import subprocess

base = Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013')
repo = base / 'native-4878-traversal'
evidence = base / 'evidence/native-public-bridge-20261015'
parent = '16d1b29593f966ecb9159a1e93e562f115f9f3bb'
ref = 'refs/heads/codex/4878-public-capture-evidence-20261014'
prefix = 'artifacts/4871/20261014-native-public-capture-preflight'
receipt_path = evidence / 'diagnostic-publication-receipt.json'
assert not receipt_path.exists(), 'Already published: inspect receipt, do not rerun.'

def sha(data):
    return hashlib.sha256(data).hexdigest()

def git(*args, data=None, env=None):
    return subprocess.check_output(['git', '-C', str(repo), *args], input=data, env=env).strip()

assert git('rev-parse', 'HEAD').decode() == parent
original_ref = git('symbolic-ref', 'HEAD')
index_path = Path(git('rev-parse', '--git-path', 'index').decode())
if not index_path.is_absolute():
    index_path = repo / index_path
original_index = index_path.read_bytes()
foreign = ['TreeDB/internal/valuelog/native_zstd_rank_census_test.go', 'TreeDB/internal/valuelog/native_zstd_leaf_fixture_export_test.go']
foreign_before = {p: sha((repo / p).read_bytes()) for p in foreign}
assert not git('for-each-ref', '--format=%(refname)', ref)
assert not git('ls-remote', 'origin', ref)

expected_hashes = {
    'abi-red-v2.raw.jsonl': 'a31e2645a2a67f6308cb9ea79051f6b169740781d3405a396ded4a231b9311ef',
    'capture-v3.raw.jsonl': 'c10c1ff0e64c5fa398d94f502530dc5523832290c28ae5cd37d1f3f3e60b3cdb',
    'capture-v3-receipt.json': '5b6f7f258888e70f42782b298d12803596c3a8b49bef4b13a03dc635c4e324ee',
    'check_native_public_activation.py': '8aa0dd1358182479e44a3530aed231c9efa282ede0afeba7227d007c7851642b',
    'abi-red-v2.activation-check.json': 'f7fe0b40a307e32ed47239c32a49b18ed732af672e7b3a14dffb9f6240e8e090',
}
names = ['abi-red-v2-command.json', 'abi-red-v2-source.json', 'abi-red-v2.exit', 'capture-v3.exit', *expected_hashes]
members = {}
for name in names:
    path = evidence / name
    assert path.is_file() and not path.is_symlink()
    members[name] = path.read_bytes()
    if name in expected_hashes:
        assert sha(members[name]) == expected_hashes[name], name
for generation, manifest in [('abi-red-v2', json.loads(members['abi-red-v2-source.json'])), ('capture-v3', {row['path']: {'bytes': row['size'], 'sha256': row['sha256']} for row in json.loads(members['capture-v3-receipt.json'])['sources']})]:
    for name, expected in manifest.items():
        rel = Path(name)
        assert not rel.is_absolute() and '..' not in rel.parts
        path = evidence / (generation + '-source') / rel
        assert path.is_file() and not path.is_symlink()
        data = path.read_bytes()
        assert len(data) == expected['bytes'] and sha(data) == expected['sha256']
        members[generation + '-source/' + name] = data
assert members['abi-red-v2.exit'].strip() == b'1' and members['capture-v3.exit'].strip() == b'1'
protected = 'TreeDB/mvcc/bounded_prune_whole_current_publication_test.go'
assert members['abi-red-v2-source/' + protected] == members['capture-v3-source/' + protected]
assert sha(members['capture-v3-source/' + protected]) == '77f14c80a007ce9b918e007e5cce24e5d78ffa022c6a92386cfb58bda246d55f'
assert not json.loads(members['abi-red-v2.activation-check.json'])['pass']

v3_check = subprocess.run(['python3', str(evidence / 'check_native_public_activation.py'), str(evidence / 'capture-v3.raw.jsonl')], capture_output=True, check=False)
assert v3_check.returncode == 1 and not v3_check.stderr
report = json.loads(v3_check.stdout)
assert not report['pass'] and len(report['cells']) == 4
assert all(c['request_captured'] and c['native_prepared'] == 2 and c['writes'] > 0 and c['ack'] == 0 and c['native_cut'] == 0 for c in report['cells'])
members['capture-v3.activation-check.json'] = v3_check.stdout
members['scripts/publish-native-public-capture-evidence-20261014.py'] = Path(__file__).read_bytes()
members['README.md'] = b'''Native public capture/private preparation diagnostic -- 2026-10-14

This immutable artifact-only branch preserves the first meaningful ABI-only legacy RED and the connected capture-v3 diagnostic. Both original public package runs exit1. No native publication, native ACK, physical Finish qualification, bounded whole-path acceptance, performance viability or merge readiness is claimed.

The unchanged original protected fixture is 33/80 rows x inline/pointer, 4096 calls, Q32/1MiB and existing input/resource/file/two-index caps. Its SHA256 is 77f14c80a007ce9b918e007e5cce24e5d78ffa022c6a92386cfb58bda246d55f in both generations. It performs intervening synchronous durable writes. The default NoWALFast/adaptive->append_only fixture does not prove Dgraph COW-profile activation; a small actual target-profile/exclusion check remains required.

ABI-red-v2 reached legacy PruneVersions, returned ACK1 with writes0, no captured Request/native preparation and reported work0; it fails the actual sustained fixture. Capture-v3 uses the SAME original Request/capture/projection/private constructor. All4 cells capture the Request, select input and prepare2 native rows while allowing4/3/8/7 writes. Each intentionally refuses at phase16 publication-notconnected with ACK0/current-cut0. File-ID namespace and budget-pause faults were fixed before this diagnostic. It is construction progress, not GREEN.

Base source is parent16d1b29593f966ecb9159a1e93e562f115f9f3bb. Each generation retains its exact changed public ABI/source files in <generation>-source/ and the command/environment receipt. To reconstruct a generation, use a disposable checkout of that parent and overlay its source directory onto the checkout root. Use Go1.26.3, GOWORK=off, GOTOOLCHAIN=local, CGO_ENABLED=1, GOMAXPROCS=4 and M7_CURRENT_SEGMENT_CONSTRUCTION=1. Invoke go test -json -count=1 -tags=treedb_test -run=^TestPruneWholeCurrentPublicationSustainedPreflight$ ./TreeDB/mvcc. The retained command binds the exact original toolchain path. These diagnostic runs are not a replacement for a final exact-source dependency census or qualification.

check_native_public_activation.py parses chunked Go JSON output and independently rejects fallback-only or missing native activation. Both retained check reports FAIL as expected. This evidence-only checker supplements the byte-identical protected fixture; it does not establish physical Finish, full budget coverage or performance. No product counter was added solely for this checker.

Accepted next seam: SAME coordinator publication fence advances frontier/lineage once and releases the publication turn; original native physical receipt, resources, charges, reachability and lifetime join persist until Finish. Ordinary writes may proceed while native Finish pauses. Delayed completion cannot regress a newer frontier. Original pre-Stop Close joins/finishes paused receipt without another user call. Actual prepare/install/promotion must also become connected finite stages, not scalar-wrapped synchronous work. Decision: https://github.com/snissn/gomap/issues/4871#issuecomment-6179448232

All graph gates remain OPEN and parent4871 INCOMPLETE. No binary, DB directory or source is released for deletion. This ref changes artifact paths only and does not move the active native worktree HEAD, index or branch.
'''
rows = [{'path': p, 'bytes': len(data), 'sha256': sha(data)} for p, data in sorted(members.items())]
manifest = {'schema': 'native-public-capture-preflight-v1', 'source_parent': parent, 'source_generations': ['abi-red-v2', 'capture-v3'], 'members': rows, 'bytes': sum(r['bytes'] for r in rows), 'diagnostic_only': True, 'native_ack_qualified': False, 'performance_viability': False, 'parent_complete': False, 'cleanup_release': []}
members['artifact-manifest.json'] = (json.dumps(manifest, indent=1) + '\n').encode()

private_index = evidence / 'diagnostic-isolated.index'
assert not private_index.exists()
env = os.environ.copy()
env['GIT_INDEX_FILE'] = str(private_index)
git('read-tree', parent, env=env)
for path, data in sorted(members.items()):
    blob = git('hash-object', '-w', '--stdin', data=data).decode()
    git('update-index', '--add', '--cacheinfo', '100644', blob, prefix + '/' + path, env=env)
tree = git('write-tree', env=env).decode()
commit = git('commit-tree', tree, '-p', parent, data=b'Preserve native public capture and meaningful legacy RED diagnostics\n').decode()
expected = {prefix + '/' + p for p in members}
changed = set(git('diff-tree', '--no-commit-id', '--name-only', '-r', parent, commit).decode().splitlines())
assert changed == expected
for path, data in members.items():
    actual = subprocess.check_output(['git', '-C', str(repo), 'cat-file', 'blob', commit + ':' + prefix + '/' + path])
    assert actual == data, path
git('update-ref', ref, commit, '0' * 40)
git('push', 'origin', ref + ':' + ref)
assert git('ls-remote', 'origin', ref).decode().split()[0] == commit
assert git('rev-parse', 'HEAD').decode() == parent and git('symbolic-ref', 'HEAD') == original_ref
assert index_path.read_bytes() == original_index
assert {p: sha((repo / p).read_bytes()) for p in foreign} == foreign_before
receipt = {'commit': commit, 'parent': parent, 'ref': ref, 'url': f'https://github.com/snissn/gomap/tree/{commit}/{prefix}', 'members': len(members), 'bytes': manifest['bytes'], 'manifest_sha256': sha(members['artifact-manifest.json']), 'all_git_blobs_readback': True, 'active_head_index_ref_unchanged': True, 'foreign_tests_preserved': True, 'diagnostic_only': True, 'cleanup_release': []}
receipt_path.write_text(json.dumps(receipt, indent=1) + '\n')
print(json.dumps(receipt), flush=True)
