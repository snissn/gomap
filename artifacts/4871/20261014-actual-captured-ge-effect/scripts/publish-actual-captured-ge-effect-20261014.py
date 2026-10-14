from pathlib import Path
import hashlib
import json
import subprocess

base = Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013')
repo = base / 'native-4878-traversal'
ge = base / 'evidence/ordinary-5111/captured-ge-production-integration-20261014'
effect = base / 'evidence/successor-4876-actual-effect-20261014'
checkpoint = base / 'evidence/coherent-captured-ge-checkpoint-20261014/receipt.json'
prefix = 'artifacts/4871/20261014-actual-captured-ge-effect'
parent = 'e666cb8ca53f0843247aa41e9279abc932348d0d'
foreign = ['TreeDB/internal/valuelog/native_zstd_rank_census_test.go',
           'TreeDB/internal/valuelog/native_zstd_leaf_fixture_export_test.go']

def sha(data):
    return hashlib.sha256(data).hexdigest()

def git(*args):
    return subprocess.check_output(['git', '-C', str(repo), *args], text=True).strip()

def status():
    return subprocess.check_output(['git', '-C', str(repo), 'status', '--porcelain=v1', '-uall'], text=True).splitlines()

assert git('rev-parse', 'HEAD') == parent
assert not git('diff', '--cached', '--name-only')
before = status()
assert {s[3:] for s in before} == set(foreign)
foreign_hashes = {p: sha((repo / p).read_bytes()) for p in foreign}
commits = json.loads(checkpoint.read_text())
assert [row['commit'] for row in commits['lanes']] == ['a934ea4c7a90b95bc15d9feb6ee9a3edde6502eb', parent]
assert [row['source_count'] for row in commits['lanes']] == [135, 9]
assert sha((ge / 'manifest.json').read_bytes()) == '008f94735f1eb1d61a0502663d4f48a7ba80c4225c9059d7ba9a4a1d300de313'
assert sha((effect / 'manifest.json').read_bytes()) == '23cff07ddab5d40f1f6c9a4b06142b57956228a82983cca2e67f00c8f0a0ffc0'
receipt = json.loads((effect / 'receipt.json').read_text())
assert receipt['benchmark_invocations'] == 16
assert receipt['actual_source_head_index_status_unchanged'] and receipt['go_quiet']
assert receipt['source_head_index_freeze_released_to_root_checkpoint']
packet = repo / prefix
assert not packet.exists()
packet.mkdir(parents=True)
for source, dest, key in [(ge, 'public-red-green', 'members'), (effect, 'actual-effect', 'publishable')]:
    manifest = json.loads((source / 'manifest.json').read_text())
    for row in manifest[key]:
        rel = Path(row['path'])
        assert not rel.is_absolute() and '..' not in rel.parts
        path = source / rel
        assert path.is_file() and not path.is_symlink()
        data = path.read_bytes()
        assert len(data) == row['bytes'] and sha(data) == row['sha256'], rel
        target = packet / dest / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
    (packet / dest / 'manifest.json').write_bytes((source / 'manifest.json').read_bytes())
(packet / 'source-checkpoint-receipt.json').write_bytes(checkpoint.read_bytes())
(packet / 'scripts').mkdir()
for name in ['checkpoint-coherent-captured-ge-20261014.py', 'publish-actual-captured-ge-effect-20261014.py']:
    (packet / 'scripts' / name).write_bytes((base / name).read_bytes())
(packet / 'README.md').write_text('''Actual captured-successor production integration and bounded effect — 2026-10-14

The actual ordinary and native candidates now contain the maintained captured fixed-scratch successor path. Both real public MVCC tests first failed the existing 48KiB guard for the intended reason (58,500B small /68,104B multilevel), then passed with historical values, structural multi-leaf proof and checked Close. Four focused normal/race runs each contain29 selected top-level cases:28PASS and the unchanged opt-in retained-heap SKIP; all5 package exits are0. Normal public allocation is11,796B/21,264B. The integration reuses the same captured cut, complete fallback, directory integrity checks and charged workspace; no pool, recapture or new governor.

Source checkpoints are pushed, every selected Git blob read back, and frozen source inventories preserved. Ordinary a934ea4c7a90b95bc15d9feb6ee9a3edde6502eb includes all135 graph-owned paths (including one redundant causal diagnostic, to preserve exact tested source). Native e666cb8ca53f0843247aa41e9279abc932348d0d includes exactly9 GE paths and preserves its two foreign tests. These are coherent construction checkpoints, not final-candidate acceptance or mainline merges.

Exactly16 fresh actual-source public ABBA calls completed,64x each, same Go1.26.3/CPU0–3/GOMAXPROCS4 and command_wal_relaxed/cow_btree. The three frozen common harness files live solely in an evidence-owned temporary module; compiled dependency census proves exactly6 production differences BEFORE/AFTER. All public selector, PRIMARY layout, physical inline/pointer placement, historical output, ACK counters and Close checks passed. All16 raw outputs and commands remain included. No extra trace, profile or repeated campaign followed.

Median total ns/op BEFORE -> AFTER:
- small inline17,810 ->15,536.5 (-12.77%)
- small pointer174,972.5 ->143,108.5 (-18.21%)
- multilevel inline23,180 ->17,758 (-23.39%)
- multilevel pointer98,043.5 ->113,620.5 (+15.89% concern)

Reader p50 and allocation bytes/count improve in all4 fixtures; reported residency endpoints match. Both inline pairs improve. Pointer pair totals conflict, and the multilevel whole-unit regression remains visible. Writer stalls are retained, not discarded or subtracted. An earlier one-event Dup attribution does not establish the cause of these untraced calls. This is scoped reader/allocation evidence, not general viability or M0 qualification. No extra FD rewrite prerequisite or micro-allocation loop is introduced.

Next critical work is native4878's actual public bridge and original SAMERequest33/80/4096 protected fixture using the same Authority/Capability/transaction/BuildGroup/Coordinator/Arena. Existing Q32/1MiB/input32/resources16/files17/two-index caps, physical Finish-before-ACK, custody and all final gates remain unchanged. M5 remains NO-GO with its original M7 prerequisite; parent4871 INCOMPLETE. Full ordinary/native19, cost, platform, review/CI, merge and final3-pair qualification remain OPEN.

The standing skills owner reviewed the production-path miss and the subsequent findings. Generalized rules now require a meaningful public-entry RED/GREEN or justified exception, activation evidence where outputs hide fallback, proportionate target/exclusion/lifecycle cases, and bounded actual-path performance before exhaustive qualification. No extra generic rule was justified by the syscall diagnostic or pointer noise. Skill-source finalization is tracked separately.

All evidence and binaries remain retained. Binaries are excluded from this Git packet; identities are in actual-effect/receipt.json. No data directory or source is released for deletion by this packet.
''')
files = []
for path in sorted(packet.rglob('*')):
    if path.is_file():
        data = path.read_bytes()
        files.append({'path': str(path.relative_to(packet)), 'bytes': len(data), 'sha256': sha(data)})
manifest = {'schema': 'actual-captured-ge-public-integration-effect-v1', 'parent': parent,
            'source_checkpoints': [row['commit'] for row in commits['lanes']],
            'benchmark_invocations': 16, 'public_path_focused_tests_only': True,
            'general_performance_viability': False, 'parent_complete': False,
            'files': files, 'bytes': sum(row['bytes'] for row in files), 'cleanup_release': []}
(packet / 'artifact-manifest.json').write_text(json.dumps(manifest, indent=1) + '\n')
git('add', '-f', '--', prefix)
expected = {prefix + '/' + row['path'] for row in files} | {prefix + '/artifact-manifest.json'}
assert set(git('diff', '--cached', '--name-only').splitlines()) == expected
git('commit', '-m', 'Preserve actual public MVCC regression and bounded source comparison')
commit = git('rev-parse', 'HEAD')
assert git('rev-parse', commit + '^') == parent
assert set(git('diff-tree', '--no-commit-id', '--name-only', '-r', parent, commit).splitlines()) == expected
for row in files:
    data = subprocess.check_output(['git', '-C', str(repo), 'show', commit + ':' + prefix + '/' + row['path']])
    assert len(data) == row['bytes'] and sha(data) == row['sha256'], row['path']
assert subprocess.check_output(['git', '-C', str(repo), 'show', commit + ':' + prefix + '/artifact-manifest.json']) == (packet / 'artifact-manifest.json').read_bytes()
assert status() == before and {p: sha((repo / p).read_bytes()) for p in foreign} == foreign_hashes
ref = git('symbolic-ref', 'HEAD')
git('push', 'origin', ref + ':' + ref)
assert git('ls-remote', 'origin', ref).split()[0] == commit
published = {'commit': commit, 'parent': parent, 'url': f'https://github.com/snissn/gomap/tree/{commit}/{prefix}',
             'members': len(files) + 1, 'bytes': manifest['bytes'], 'manifest_sha256': sha((packet / 'artifact-manifest.json').read_bytes()),
             'all_members_git_readback': True, 'foreign_preserved': True, 'source_checkpoints': manifest['source_checkpoints'], 'cleanup_release': []}
(effect / 'publication-receipt.json').write_text(json.dumps(published, indent=1) + '\n')
print(json.dumps(published), flush=True)
