"""Root verification of a frozen source packet; performs no Go or Git mutation."""
import argparse
import hashlib
import json
import os
import pathlib
import subprocess
import tarfile

D = pathlib.Path('/mnt/fast4tb/gomap-mvcc-4871-20261015')
N = D / 'native-integrated'
P = D / 'native-main-integration/native-minimum-scan-source01'
parser = argparse.ArgumentParser()
parser.add_argument('--packet', type=pathlib.Path, required=True)
parser.add_argument('--manifest-sha256', required=True)
parser.add_argument('--archive-sha256', required=True)
parser.add_argument('--output', type=pathlib.Path, required=True)
args = parser.parse_args()
B = args.packet.resolve()
assert B.is_relative_to(D / 'native-main-integration')
assert args.output.resolve().is_relative_to(D / 'native-decision')

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def read(path):
    return json.loads(path.read_text())

assert sha(P / 'source-frozen.json') == 'b9d1baa055e9b821106d09cae9f562537610c40e75e2596efd9a18eda4759c91'
assert sha(B / 'source-frozen.json') == args.manifest_sha256
assert sha(B / 'source-frozen.tar.gz') == args.archive_sha256
parent = read(P / 'source-frozen.json')
current = read(B / 'source-frozen.json')
modes = read(B / 'source-modes.json')
assert set(current) == set(modes)
for name in current:
    path = pathlib.PurePosixPath(name)
    assert not path.is_absolute() and '..' not in path.parts
    assert path.parts[0] != '.git'
actual = {}
for root, dirs, files in os.walk(N):
    dirs[:] = [name for name in dirs if name != '.git']
    for name in files:
        path = pathlib.Path(root, name)
        relative = str(path.relative_to(N))
        if relative == '.git':
            continue
        assert not path.is_symlink(), relative
        if path.is_file():
            assert oct(path.stat().st_mode & 0o7777) == modes[relative], relative
            actual[relative] = sha(path)
assert actual == current, 'live source differs from frozen manifest'
with tarfile.open(B / 'source-frozen.tar.gz', 'r:gz') as archive:
    members = archive.getmembers()
    assert all(member.isfile() for member in members)
    assert len({member.name for member in members}) == len(members)
    archive_hashes = {}
    for member in members:
        assert oct(member.mode & 0o7777) == modes[member.name], member.name
        archive_hashes[member.name] = hashlib.sha256(archive.extractfile(member).read()).hexdigest()
    assert archive_hashes == current
expected_changes = {}
for name in sorted(set(parent) | set(current)):
    if parent.get(name) != current.get(name):
        expected_changes[name] = {
            'before_sha256': parent.get(name),
            'after_sha256': current.get(name),
            'change': 'add' if name not in parent else 'delete' if name not in current else 'modify',
        }
changes = read(B / 'changed-files.json')
assert len({row['path'] for row in changes}) == len(changes)
assert set(row['path'] for row in changes) == set(expected_changes)
for row in changes:
    for key in ('before_sha256', 'after_sha256'):
        assert row[key] == expected_changes[row['path']][key], (row['path'], key)
    if 'change' in row:
        assert row['change'] == expected_changes[row['path']]['change']
    if row['path'] in current and 'mode' in row:
        assert row['mode'] == modes[row['path']]
with tarfile.open(B / 'changed-source.tar.gz', 'r:gz') as archive:
    members = archive.getmembers()
    assert all(member.isfile() for member in members)
    assert len({member.name for member in members}) == len(members)
    changed_hashes = {member.name: hashlib.sha256(archive.extractfile(member).read()).hexdigest() for member in members}
    assert changed_hashes == {name: current[name] for name in expected_changes if name in current}
plan = read(B / 'proposed-commands.json')
assert plan['source_manifest_sha256'] == args.manifest_sha256
assert plan['source_archive_sha256'] == args.archive_sha256
assert all(not command['executed'] for command in plan['commands'])
for name, digest in plan['test_file_bindings'].items():
    assert current[name] == digest
head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=N).decode().strip()
index = subprocess.check_output(['git', 'ls-files', '--stage', '-z'], cwd=N)
index_path = pathlib.Path(subprocess.check_output(['git', 'rev-parse', '--git-path', 'index'], cwd=N).decode().strip())
if not index_path.is_absolute():
    index_path = N / index_path
raw_index_sha256 = sha(index_path)
freeze = read(B / 'freeze.json')
assert freeze['index_sha256'] == raw_index_sha256
result = {
    'packet': str(B),
    'source_manifest_sha256': args.manifest_sha256,
    'source_archive_sha256': args.archive_sha256,
    'source_files': len(current),
    'changed_files': len(changes),
    'live_source_and_modes_match': True,
    'full_archive_bytes_and_modes_match': True,
    'changed_archive_matches_exact_parent_delta': True,
    'proposal_test_bindings_match': True,
    'git_identity': {'HEAD': head, 'index_sha256': hashlib.sha256(index).hexdigest()},
    'raw_index_sha256': raw_index_sha256,
    'root_verifier_sha256': sha(pathlib.Path(__file__)),
    'runtime_executed': False,
    'source_semantic_acceptance': False,
}
with args.output.open('x') as output:
    json.dump(result, output, indent=2, sort_keys=True)
    output.write('\n')
print(json.dumps(result, indent=2, sort_keys=True))
