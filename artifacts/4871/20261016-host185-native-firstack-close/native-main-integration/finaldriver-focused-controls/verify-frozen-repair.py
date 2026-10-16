import argparse
import datetime
import difflib
import hashlib
import json
import os
import subprocess
import tarfile
from pathlib import Path

D = Path('/mnt/fast4tb/gomap-mvcc-4871-20261015')
N = D / 'native-integrated'
p = argparse.ArgumentParser()
p.add_argument('--packet', type=Path, required=True)
p.add_argument('--parent', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
a = p.parse_args()
B, P, O = a.packet.resolve(), a.parent.resolve(), a.output.resolve()
assert B.is_relative_to(D / 'native-main-integration')
assert P.is_relative_to(D / 'native-main-integration')
assert O.is_relative_to(D / 'native-decision') and not O.exists()

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def read(path):
    return json.loads(path.read_text())

def archive(path, expected, retain):
    result = {}
    contents = {}
    with tarfile.open(path, 'r:gz') as tf:
        for member in tf:
            assert member.isfile(), member.name
            assert member.name not in result, member.name
            assert not member.name.startswith('/') and '..' not in Path(member.name).parts
            raw = tf.extractfile(member).read()
            result[member.name] = hashlib.sha256(raw).hexdigest()
            if member.name in retain:
                contents[member.name] = raw
    assert result == expected, str(path)
    return contents

f = read(B / 'freeze.json')
m, old = read(B / 'source-frozen.json'), read(P / 'source-frozen.json')
for name, key in [('source-frozen.json', 'source_manifest_sha256'),
                  ('source-frozen.tar.gz', 'source_archive_sha256'),
                  ('changed-source.tar.gz', 'changed_archive_sha256'),
                  ('source.delta.patch', 'delta_sha256')]:
    assert sha(B / name) == f[key], name
assert sha(P / 'source-frozen.json') == f['parent_manifest_sha256']
assert sha(P / 'source-frozen.tar.gz') == f['parent_archive_sha256']
changed = sorted(n for n in m if old.get(n) != m[n])
deleted = sorted(set(old) - set(m))
assert not deleted
rows = [{'path': n, 'before_sha256': old.get(n), 'after_sha256': m[n],
         'change': 'modify' if n in old else 'add'} for n in changed]
assert rows == read(B / 'changed-files.json')
assert len(m) == f['source_files']
assert len(changed) == f['changed_paths']
added = sum(n not in old for n in changed)
modified = len(changed) - added
assert added == f['added_paths'] and modified == f['modified_paths']
assert len(old) - modified == f['unchanged_paths']
current_bytes = archive(B / 'source-frozen.tar.gz', m, changed)
old_bytes = archive(P / 'source-frozen.tar.gz', old, changed)
archive(B / 'changed-source.tar.gz', {n: m[n] for n in changed}, [])
delta = []
for n in changed:
    delta.extend(difflib.unified_diff(
        old_bytes.get(n, b'').decode().splitlines(True),
        current_bytes[n].decode().splitlines(True),
        fromfile='a/' + n if n in old else '/dev/null', tofile='b/' + n))
assert ''.join(delta).encode() == (B / 'source.delta.patch').read_bytes()
live = {}
for base, ds, fs in os.walk(N):
    ds[:] = [d for d in ds if d != '.git']
    for name in fs:
        path = Path(base) / name
        rel = path.relative_to(N).as_posix()
        if rel != '.git' and path.is_file():
            live[rel] = sha(path)
assert live == m
index = Path(f['index_path'])
assert sha(index) == f['index_sha256'] == f['index_after_sha256']
def git(args):
    return subprocess.check_output(['git'] + args, cwd=N)
identity = {'HEAD': git(['rev-parse', 'HEAD']).decode().strip(),
            'index_sha256': hashlib.sha256(git(['ls-files', '--stage', '-z'])).hexdigest()}
assert identity == {'HEAD': '0149d78dba2574b09d7119d2631ec78a144e088d',
                    'index_sha256': '07161c1dc6c88fb230c74700b4ddd76b975a613540949423512d6786f7c86823'}
receipt = {'utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
           'disposition': 'EXACT_FROZEN_REPAIR_BYTES_VERIFIED',
           'packet': str(B), 'parent': str(P), 'source_files': len(m),
           'changed_paths': changed, 'unchanged_paths': len(old) - modified,
           'source_manifest_sha256': sha(B / 'source-frozen.json'),
           'source_archive_sha256': sha(B / 'source-frozen.tar.gz'),
           'full_parent_and_successor_archives_verified': True,
           'exact_patch_reproduced': True, 'live_source_equal': True,
           'raw_index_sha256': sha(index), 'git_identity': identity,
           'verifier_sha256': sha(Path(__file__)),
           'source_review_separate': True, 'runtime_authorized': False}
O.write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps({'receipt': str(O), 'sha256': sha(O),
                  'files': len(m), 'changed': len(changed)}))
