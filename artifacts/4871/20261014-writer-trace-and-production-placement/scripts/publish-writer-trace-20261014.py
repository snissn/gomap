from pathlib import Path
import hashlib
import json
import shutil
import subprocess

base = Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013')
repo = base / 'native-4878-traversal'
trace = base / 'evidence/successor-4876-writer-trace-20261014'
checks = base / 'evidence/root-empty-checkpoint-production-placement-checks-20261014'
placement = base / 'evidence/ordinary-5111/empty-checkpoint-production-placement-20261014T211306Z'
prefix = 'artifacts/4871/20261014-writer-trace-and-production-placement'
parent = 'fb20b379507f50f9b7eb4b9776907e2df8fb3a6b'
foreign = ['TreeDB/internal/valuelog/native_zstd_rank_census_test.go', 'TreeDB/internal/valuelog/native_zstd_leaf_fixture_export_test.go']

def sha(data):
    return hashlib.sha256(data).hexdigest()

def git(*args):
    return subprocess.check_output(['git', '-C', str(repo), *args], text=True).strip()

def status():
    return subprocess.check_output(['git', '-C', str(repo), 'status', '--porcelain=v1', '-uall'], text=True).splitlines()

assert git('rev-parse', 'HEAD') == parent
assert git('symbolic-ref', 'HEAD') == 'refs/heads/codex/4878-native-traversal-20261014'
assert not git('diff', '--cached', '--name-only')
before = status()
assert set(s[3:] for s in before) == set(foreign)
foreign_hashes = {p: sha((repo / p).read_bytes()) for p in foreign}
assert sha((trace / 'manifest.json').read_bytes()) == '2a51d0999f82d00d719cd0c4a001104dfa97dd791ab994d0f1522bc11d31175b'
assert sha((trace / 'report.md').read_bytes()) == '031e897b98b14ec64c9785d04a6620f0b1633637618976a2e830ee9830fe6e8b'
manifest = json.loads((trace / 'manifest.json').read_text())
assert manifest['benchmark_invocations'] == 1 and manifest['builds'] == 0
assert len(manifest['publishable']) == 27
packet = repo / prefix
assert not packet.exists()
packet.mkdir(parents=True)
retained = []
for item in manifest['publishable']:
    rel = Path(item['path'])
    assert not rel.is_absolute() and '..' not in rel.parts
    src = trace / rel
    assert src.is_file() and not src.is_symlink()
    data = src.read_bytes()
    assert len(data) == item['bytes'] and sha(data) == item['sha256']
    if rel.name == 'parsed.raw':
        retained.append({**item, 'reason': 'Reproducible 25.8MB derived text; original trace, exact parse command/toolchain and derived attribution retained in Git. Original dump remains on host; no deletion release.'})
        continue
    out = packet / 'trace' / rel
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(data)
shutil.copyfile(trace / 'manifest.json', packet / 'trace/manifest.json')
assert len(retained) == 1
assert sha((placement / 'receipt.json').read_bytes()) == '47c70e960734ec7f50081692ca5ccffd1a472ae15a69c2f9b4533cba0bdd128e'
(packet / 'placement').mkdir()
shutil.copyfile(placement / 'receipt.json', packet / 'placement/source-placement-receipt.json')
for name in ['receipt.json', 'ordinary-5111.raw', 'native-4878-traversal.raw', 'production-commit-receipt.json']:
    shutil.copyfile(checks / name, packet / 'placement' / name)
check_rows = json.loads((checks / 'receipt.json').read_text())
assert len(check_rows) == 2 and all(row['exit'] == 0 and row['source_status_unchanged'] for row in check_rows)
commits = json.loads((checks / 'production-commit-receipt.json').read_text())
assert [row['commit'] for row in commits] == ['9bd10594dc87e0ccc9f4d1c59ce6aabbf5e69e85', parent]
(packet / 'scripts').mkdir()
for name in ['publish-writer-trace-20261014.py', 'checkpoint-repair-production-commit-20261014.py']:
    shutil.copyfile(base / name, packet / 'scripts' / name)
(packet / 'README.md').write_text('''One writer-tail trace and actual production placement — 2026-10-14

This is one source-identical frozen BEFORE small64x diagnostic. It attributes 13.318528ms of a13.338769ms writer maximum (99.85%) to the unix.Dup syscall interval in canonical command-WAL stable external-RID capture. No GC range overlaps; post-syscall runnable delay is64ns. Kernel work, waiting and descheduling cannot be distinguished by this trace. Whole-trace fsync belongs to Open/setup rather than this timed maximum. Traced timing is not comparable performance evidence and does not waive the earlier small total regression.

The original trace, binary/source/toolchain identities, complete commands, attribution, GC ranges, selected events and syscall/scheduler profiles are in trace/. parsed.raw (25.8MB) remains on the host and is omitted from Git as reproducible derived text. Regenerate with the exact Go1.26.3 command in trace/analysis-receipts.json: go tool trace -d=parsed trace.out. Its bytes/SHA remain in both manifests. No source, binary or evidence is released for deletion.

Read-only source review confirms every new external-RID intent constructs fresh stable producer tokens that duplicate the Manager child handle; retry of the SAME intent already reuses its dependencies. This is inherited behavior, not a GE-only regression. Fresh RID/frontier validation is required. Existing cloneSharedPinned preserves an earlier content certificate and cannot simply certify newly appended RIDs. A future coordinated optimization may retain one private physical capability on the existing incarnation-bound File and derive independently admitted fresh descriptors/certificates, preserving Windows exact writable pins, final-close custody, accounting and retirement. Its performance benefit is not measured; it is not a new prerequisite to assembling the coherent candidate. No FD/GC/buffer implementation follows from this diagnostic.

The accepted public empty-Checkpoint handoff repair is now placed in BOTH actual production lanes, not only the performance projections. Exactly three files changed per lane; unrelated entries, indexes and status were preserved. One focused ^TestCOWPublicPrimary batch passed on each lane (four top-level cases, including the real four-profile empty-Checkpoint regression). Original RED, eight normal and ten race controls remain in the prior checkpoint packet. Source-only checkpoint commits are pushed and all blobs read back:

- ordinary:9bd10594dc87e0ccc9f4d1c59ce6aabbf5e69e85
- native:fb20b379507f50f9b7eb4b9776907e2df8fb3a6b

These are candidate-branch checkpoints, not mainline merges or completed node qualification. Ordinary retains other unrelated dirty construction work. Native retains its two foreign test files unchanged. Per-PR gates and original all19/cost/platform/final M0 gates remain open; parent4871 INCOMPLETE and production M5 remains NO-GO.

Next: compare the already focused-green PRIMARY empty-root/affinity and provisional GE deltas to the strongest actual production seams; assemble one coherent production candidate without whole projection/harness import or an unnecessary FD prerequisite. Then check the actual public path and bounded effect before exhaustive final qualification. The standing skills owner reviewed the findings and found no further generalized rule needed beyond the now-required meaningful public-entry RED/GREEN, target activation, proportional lifecycle checks, causal profiles and resource reuse review.
''')
files = []
for path in sorted(packet.rglob('*')):
    if path.is_file():
        data = path.read_bytes()
        files.append({'path': str(path.relative_to(packet)), 'bytes': len(data), 'sha256': sha(data)})
result = {'schema': 'mvcc-writer-trace-production-placement-v1', 'parent': parent,
          'diagnostic_invocations': 1, 'comparison_acceptance': False,
          'production_checkpoint_commits': [row['commit'] for row in commits],
          'retained_derived_outputs': retained, 'excluded_binaries': manifest['excluded_binaries'],
          'cleanup_release': [], 'files': files, 'bytes': sum(row['bytes'] for row in files)}
(packet / 'artifact-manifest.json').write_text(json.dumps(result, indent=1) + '\n')
git('add', '-f', '--', prefix)
expected = {prefix + '/' + row['path'] for row in files} | {prefix + '/artifact-manifest.json'}
assert set(git('diff', '--cached', '--name-only').splitlines()) == expected
git('commit', '-m', 'Preserve one writer trace and actual checkpoint repair placement')
commit = git('rev-parse', 'HEAD')
assert git('rev-parse', commit + '^') == parent
assert set(git('diff-tree', '--no-commit-id', '--name-only', '-r', parent, commit).splitlines()) == expected
for row in files:
    data = subprocess.check_output(['git', '-C', str(repo), 'show', commit + ':' + prefix + '/' + row['path']])
    assert len(data) == row['bytes'] and sha(data) == row['sha256']
assert subprocess.check_output(['git', '-C', str(repo), 'show', commit + ':' + prefix + '/artifact-manifest.json']) == (packet / 'artifact-manifest.json').read_bytes()
assert status() == before and {p: sha((repo / p).read_bytes()) for p in foreign} == foreign_hashes
ref = git('symbolic-ref', 'HEAD')
git('push', 'origin', ref + ':' + ref)
assert git('ls-remote', 'origin', ref).split()[0] == commit
receipt = {'commit': commit, 'parent': parent, 'url': f'https://github.com/snissn/gomap/tree/{commit}/{prefix}',
           'members': len(files) + 1, 'bytes': result['bytes'], 'manifest_sha256': sha((packet / 'artifact-manifest.json').read_bytes()),
           'all_git_members_readback': True, 'foreign_preserved': True, 'cleanup_release': []}
(trace / 'publication-receipt.json').write_text(json.dumps(receipt, indent=1) + '\n')
print(json.dumps(receipt), flush=True)
