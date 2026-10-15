import datetime
import hashlib
import json
import pathlib
import subprocess
import sys

D = pathlib.Path('/mnt/fast4tb/gomap-mvcc-4871-20261015')
W = D / 'native'
manifest = pathlib.Path(sys.argv[1]).resolve()
assert manifest.is_relative_to(D / 'native-build')
expected = json.loads(manifest.read_text())
assert isinstance(expected, dict) and all(isinstance(v, str) and len(v) == 64 for v in expected.values())
def git(*args):
    return subprocess.check_output(['git', *args], cwd=W, text=True).strip()
def source():
    return {f: hashlib.sha256((W / f).read_bytes()).hexdigest() for f in git('ls-files', '--cached', '--others', '--exclude-standard').splitlines() if (W / f).is_file()}
assert git('branch', '--show-current') == 'codex/4878-native-185-20261015'
assert not git('diff', '--cached', '--name-only')
assert source() == expected, 'source changed after worker freeze'
changed = sorted(set(git('diff', '--name-only').splitlines()) | set(git('ls-files', '--others', '--exclude-standard').splitlines()))
assert changed and all(f.startswith('TreeDB/') for f in changed), changed
parent = git('rev-parse', 'HEAD')
subprocess.run(['git', 'diff', '--check'], cwd=W, check=True)
subprocess.run(['git', 'add', '--', *changed], cwd=W, check=True)
subprocess.run(['git', 'diff', '--cached', '--check'], cwd=W, check=True)
tree = git('write-tree')
assert source() == expected
subprocess.run(['git', 'commit', '-m', 'wip(mvcc): preserve admitted pre-effect publication geometry probe'], cwd=W, check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
head = git('rev-parse', 'HEAD')
assert git('rev-parse', 'HEAD^{tree}') == tree and not git('status', '--porcelain')
assert source() == expected
O = D / 'native-build' / ('recovery-' + head[:12])
O.mkdir()
(O / 'changed-files.json').write_text(json.dumps(changed, indent=2) + '\n')
receipt = {'head': head, 'tree': tree, 'parent': parent, 'branch': git('branch', '--show-current'), 'source_files': len(expected), 'source_manifest': str(manifest), 'source_manifest_sha256': hashlib.sha256(manifest.read_bytes()).hexdigest(), 'source_unchanged': True, 'qualified': False, 'gate': 'Q32 RED, ACK0; actual publication/Consume/Finish and paused-Finish/newer-writer remain outstanding', 'remote_verified': False, 'at_utc': datetime.datetime.now(datetime.timezone.utc).isoformat()}
(O / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
p = subprocess.run(['git', 'push', 'origin', receipt['branch']], cwd=W, capture_output=True, text=True)
(O / 'push.log').write_text(p.stdout + p.stderr)
assert p.returncode == 0, p.stderr
remote = git('ls-remote', 'origin', 'refs/heads/' + receipt['branch'])
assert remote.split()[0] == head
receipt['remote_verified'] = True
receipt['remote_ref'] = remote
assert source() == expected and not git('status', '--porcelain')
(O / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps(receipt, indent=2))
