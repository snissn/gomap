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
P = D / 'native-main-integration/native-registry-api-source01'
R = D / 'native-main-integration/native-registry-api-c1-root'
C = D / 'native-main-integration/native-registry-api-c1-core'
B = D / 'native-main-integration/native-registry-api-source02'
p = argparse.ArgumentParser()
p.add_argument('--core-handoff-sha256', required=True)
p.add_argument('--core-handoff', type=Path, required=True)
p.add_argument('--all-source-writers-released', action='store_true', required=True)
a = p.parse_args()


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(path):
    return json.loads(path.read_text())


def dump(name, value):
    (B / name).write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def identity():
    def git(args):
        return subprocess.check_output(['git'] + args, cwd=W)
    return {'HEAD': git(['rev-parse', 'HEAD']).decode().strip(),
        'index_sha256': hashlib.sha256(git(['ls-files', '--stage', '-z'])).hexdigest()}


assert os.path.ismount('/mnt/fast4tb') and os.access(D, os.W_OK)
assert not B.exists()
assert sha(P / 'source-frozen.json') == '98b09c2ca598a66558462384ed966930e2161fc2da49e76ee7ff844bb2427bb5'
assert sha(R / 'freeze.json') == 'ca5ceb03e0b8d7b662568e7edb23838f20c125ecce8ee7d4b38a7a09f476291a'
assert a.core_handoff.resolve().is_relative_to(C)
assert sha(a.core_handoff) == a.core_handoff_sha256
assert read(R / 'freeze.json')['source_writes_released']
old = read(P / 'source-frozen.json')
old_modes = read(P / 'source-modes.json')
original_identity = identity()
assert original_identity == read(P / 'freeze.json')['git_identity']
inputs = [P / 'freeze.json', R / 'before.json', R / 'after.json', R / 'freeze.json',
    C / 'owned-before.json', C / 'owned-after.json', C / 'owned-modes.json', a.core_handoff]
input_hashes = {str(path.relative_to(D)): sha(path) for path in inputs}
partitions = [('core', read(C / 'owned-before.json'), read(C / 'owned-after.json')),
    ('root-reset', read(R / 'before.json'), read(R / 'after.json'))]
owners = {}
for label, before, after in partitions:
    assert not set(before) - set(after), label
    for name, digest in before.items():
        assert digest == old[name], (label, name, 'preimage')
    for name, digest in after.items():
        assert name not in owners, (name, 'ownership overlap')
        assert len(digest) == 64 and sha(W / name) == digest, (name, 'live source')
        if label == 'core':
            assert name.startswith(('TreeDB/db/', 'TreeDB/lifecycle/'))
        owners[name] = label
for name, mode in read(C / 'owned-modes.json').items():
    assert oct((W / name).stat().st_mode & 0o7777) == mode, (name, 'core mode')
names = []
for base, dirs, files in os.walk(W):
    dirs[:] = [name for name in dirs if name != '.git']
    names.extend((Path(base) / name).relative_to(W).as_posix()
        for name in files if (Path(base) / name).relative_to(W).as_posix() != '.git')
names.sort()
assert all(not name.startswith('.git/') for name in names)
current = {name: sha(W / name) for name in names}
modes = {name: oct((W / name).stat().st_mode & 0o7777) for name in names}
assert not set(old) - set(current)
changed = [name for name in names if old.get(name) != current[name]]
assert changed and all(name in owners for name in changed)
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
with tarfile.open(P / 'source-frozen.tar.gz', 'r|gz') as archive:
    for member in archive:
        if member.name in changed:
            raw = archive.extractfile(member).read()
            assert hashlib.sha256(raw).hexdigest() == old[member.name]
            preimages[member.name] = raw
delta = []
for name in changed:
    delta.extend(difflib.unified_diff(preimages.get(name, b'').decode().splitlines(True),
        (W / name).read_text().splitlines(True),
        fromfile='source01/' + name if name in old else '/dev/null', tofile='source02/' + name))
(B / 'source.delta.patch').write_text(''.join(delta))
assert all(sha(W / name) == digest for name, digest in current.items())
assert identity() == original_identity
assert {str(path.relative_to(D)): sha(path) for path in inputs} == input_hashes
freeze = {'scope': 'coherent checked registry API source02 C1 correction',
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
