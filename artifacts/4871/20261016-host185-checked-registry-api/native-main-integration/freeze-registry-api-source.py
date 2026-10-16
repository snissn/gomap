import argparse
import datetime
import difflib
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tarfile

D = Path('/mnt/fast4tb/gomap-mvcc-4871-20261015')
W = D / 'native-integrated'
P = D / 'native-main-integration/native-ledger-source04'
R = D / 'native-main-integration/native-registry-api-root-migration'
T = D / 'native-main-integration/native-registry-api-test-migration'
B = D / 'native-main-integration/native-registry-api-source01'
p = argparse.ArgumentParser()
p.add_argument('--core-before', type=Path, required=True)
p.add_argument('--core-after', type=Path, required=True)
p.add_argument('--core-modes', type=Path, required=True)
p.add_argument('--core-handoff', type=Path, required=True)
a = p.parse_args()

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def read(path):
    return json.loads(path.read_text())

def dump(name, value):
    (B / name).write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')

def git(args):
    return subprocess.check_output(['git'] + args, cwd=W)

def identity():
    return {'HEAD': git(['rev-parse', 'HEAD']).decode().strip(),
        'index_sha256': hashlib.sha256(git(['ls-files', '--stage', '-z'])).hexdigest()}

assert os.path.ismount('/mnt/fast4tb') and os.access(D, os.W_OK)
assert not B.exists()
assert sha(P / 'source-frozen.json') == '3b7e1bf114e613fe3f7cbf29f99b7dcdb6b54f8d12a3c06fef71075b8fc558f0'
old = read(P / 'source-frozen.json')
old_modes = read(P / 'source-modes.json')
original_identity = identity()
assert original_identity['HEAD'] == '0149d78dba2574b09d7119d2631ec78a144e088d'
inputs = [a.core_before, a.core_after, a.core_modes, a.core_handoff,
    R / 'root-before.json', R / 'root-after.json', R / 'root-freeze.json',
    T / 'owned-before.json', T / 'owned-after.json', T / 'owned-freeze.json', T / 'HANDOFF.md']
input_hashes = {str(path.relative_to(D)): sha(path) for path in inputs}
core_before, core_after, core_modes = read(a.core_before), read(a.core_after), read(a.core_modes)
root_before, root_after = read(R / 'root-before.json'), read(R / 'root-after.json')
test_before, test_after = read(T / 'owned-before.json'), read(T / 'owned-after.json')
partitions = [('core', core_before, core_after), ('outside-root', root_before, root_after),
    ('outside-existing-tests', test_before, test_after)]
owners = {}
for label, before, after in partitions:
    assert not set(before) - set(after), label
    for name, value in before.items():
        assert value == old[name], (label, name, 'preimage')
    for name, value in after.items():
        assert isinstance(value, str) and len(value) == 64, (label, name, 'manifest shape')
        assert name not in owners, (label, name, 'overlap')
        owners[name] = label
        assert sha(W / name) == value, (label, name, 'live source')
for name, mode in core_modes.items():
    assert oct((W / name).stat().st_mode & 0o7777) == mode, (name, 'core mode')
assert all(name.startswith(('TreeDB/db/', 'TreeDB/lifecycle/')) for name in core_after)
assert all(name.endswith('_test.go') and not name.startswith(('TreeDB/db/', 'TreeDB/lifecycle/')) for name in test_after)
names = []
for base, dirs, files in os.walk(W):
    dirs[:] = [name for name in dirs if name != '.git']
    names.extend((Path(base) / name).relative_to(W).as_posix() for name in files
        if (Path(base) / name).relative_to(W).as_posix() != '.git')
names.sort()
current = {name: sha(W / name) for name in names}
modes = {name: oct((W / name).stat().st_mode & 0o7777) for name in names}
changed = [name for name in names if old.get(name) != current[name]]
assert not set(old) - set(current)
assert all(name in owners for name in changed)
assert all(modes[name] == old_modes[name] for name in old)
B.mkdir()
dump('source-frozen.json', current)
dump('source-modes.json', modes)
dump('changed-files.json', [{'path': name, 'before_sha256': old.get(name),
    'after_sha256': current[name], 'mode': modes[name], 'owner': owners[name]} for name in changed])
for filename, members in [('source-frozen.tar.gz', names), ('changed-source.tar.gz', changed)]:
    with tarfile.open(B / filename, 'w:gz', compresslevel=6) as archive:
        for name in members:
            archive.add(W / name, arcname=name, recursive=False)
    with tarfile.open(B / filename, 'r|gz') as archive:
        seen = []
        for member in archive:
            seen.append(member.name)
            assert hashlib.sha256(archive.extractfile(member).read()).hexdigest() == current[member.name]
            assert oct(member.mode) == modes[member.name]
        assert seen == members
preimages = {}
selected = set(changed)
with tarfile.open(P / 'source-frozen.tar.gz', 'r|gz') as archive:
    for member in archive:
        if member.name in selected:
            raw = archive.extractfile(member).read()
            assert hashlib.sha256(raw).hexdigest() == old[member.name]
            preimages[member.name] = raw
delta = []
for name in changed:
    delta.extend(difflib.unified_diff(preimages.get(name, b'').decode().splitlines(True),
        (W / name).read_text().splitlines(True),
        fromfile='ledger04/' + name if name in old else '/dev/null', tofile='registry-api/' + name))
(B / 'source.delta.patch').write_text(''.join(delta))
assert all(sha(W / name) == value for name, value in current.items())
assert identity() == original_identity
assert {str(path.relative_to(D)): sha(path) for path in inputs} == input_hashes
freeze = {'scope': 'coherent checked registry and Snapshot API source01',
    'created_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'source_root': str(W), 'git_identity': original_identity,
    'source_files': len(current), 'changed_paths': len(changed),
    'added_paths': sum(name not in old for name in changed), 'deleted_paths': 0,
    'partition_changed_counts': {label: sum(owners[name] == label for name in changed) for label, _, _ in partitions},
    'source_manifest_sha256': sha(B / 'source-frozen.json'), 'modes_sha256': sha(B / 'source-modes.json'),
    'source_archive_sha256': sha(B / 'source-frozen.tar.gz'),
    'changed_archive_sha256': sha(B / 'changed-source.tar.gz'), 'delta_sha256': sha(B / 'source.delta.patch'),
    'parent_manifest_sha256': sha(P / 'source-frozen.json'),
    'input_sha256': input_hashes, 'all_source_writers_released': True,
    'archive_members_verified': True, 'source_and_index_unchanged': True,
    'compile_executed': False, 'runtime_executed': False}
dump('freeze.json', freeze)
print(json.dumps(freeze, indent=2))
