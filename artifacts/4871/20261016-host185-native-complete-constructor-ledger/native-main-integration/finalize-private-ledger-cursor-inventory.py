from pathlib import Path
import argparse
import datetime
import hashlib
import json
import os
import subprocess
import tarfile

D = Path('/mnt/fast4tb/gomap-mvcc-4871-20261015')
W = D / 'native-integrated'
A = D / 'native-main-integration/private-ledger-cursor-inventory-final'
parser = argparse.ArgumentParser()
parser.add_argument('snapshot')
parser.add_argument('manifest_sha256')
args = parser.parse_args()
B = D / 'native-main-integration' / args.snapshot

def fh(p):
    return hashlib.sha256(p.read_bytes()).hexdigest()

def dump(p, x):
    p.write_text(json.dumps(x, indent=2, sort_keys=True) + '\n')

def run(name, argv):
    r = subprocess.run(argv, cwd=W, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=180)
    (A / (name + '.log')).write_bytes(r.stdout)
    dump(A / (name + '.json'), dict(argv=argv, exit_code=r.returncode, log_sha256=hashlib.sha256(r.stdout).hexdigest()))
    assert r.returncode == 0, (name, r.stdout.decode(errors='replace'))
    return r.stdout

assert fh(B / 'source-frozen.json') == args.manifest_sha256
I = json.loads((B / 'source-frozen.json').read_text())
assert len(I) == 10389 and all(fh(W / n) == h for n, h in I.items())
assert subprocess.check_output(['git', 'write-tree'], cwd=W).decode().strip() == 'd63bc97e893e518c87687831a6e0a01a357268f2'
previous_path = D / 'native-main-integration/private-hash-inventory-final/source-final.json'
assert fh(previous_path) == '8f3a55720f95ec0a9cdf7e18a0360fd09b9385d41a25b28076a3d95157db9889'
previous = json.loads(previous_path.read_text())
assert set(previous) == set(I)
names = sorted(n for n in I if previous.get(n) != I[n])
assert names == ['TreeDB/mvcc/native_prune_backend_route_v6_test.go', 'TreeDB/mvcc/native_selected_ledger_test.go']
prior = set(subprocess.check_output(['git', 'diff', '--cached', '--name-only', '-z'], cwd=W).decode().split('\0')[:-1])
assert len(prior) == 216 and len(prior | set(names)) == 216
assert not A.exists()
A.mkdir()
(A / 'tmp').mkdir()
env = dict(os.environ, TMPDIR=str(A / 'tmp'), PYTHONDONTWRITEBYTECODE='1', GIT_OPTIONAL_LOCKS='0')
policy = '.github/ci/ci_impact.json'
before = (W / policy).read_bytes()
(A / 'policy-before.json').write_bytes(before)
run('stage-frozen', ['git', 'add', '--', *names])
actual = set(run('staged-paths', ['git', 'diff', '--cached', '--name-only', '-z']).decode().split('\0')[:-1])
assert actual == prior | set(names)
run('staged-check', ['git', 'diff', '--cached', '--check'])
run('staged-stat', ['git', 'diff', '--cached', '--stat'])
tree_before = run('tree-before', ['git', 'write-tree']).decode().strip()
run('refresh', ['python3', '.github/scripts/refresh_ci_impact_inventory.py'])
after = (W / policy).read_bytes()
(A / 'policy-after.json').write_bytes(after)
old, new = json.loads(before), json.loads(after)
keys = sorted(k for k in set(old) | set(new) if old.get(k) != new.get(k))
assert keys == ['discovery_source_sha256'], keys
assert all(fh(W / n) == h for n, h in I.items() if n != policy)
run('source-contract', ['python3', '.github/scripts/refresh_ci_impact_inventory.py', '--check'])
assert all(I[n] == previous[n] for n in I if n.startswith('.github/scripts/'))
reuse = {}
for name in ['test_refresh_ci_impact_inventory', 'test_ci_impact', 'test_treedb_ci_contract', 'test_treedb_windows_core_headroom']:
    p = D / 'native-main-integration/origin-inventory-final' / (name + '.json')
    r = json.loads(p.read_text())
    assert r['exit_code'] == 0 and fh(p.with_suffix('.log')) == r['log_sha256']
    reuse[name] = dict(receipt=str(p), receipt_sha256=fh(p), unchanged_script_and_tests=True)
dump(A / 'unchanged-python-check-reuse.json', reuse)
run('stage-policy', ['git', 'add', '--', policy])
run('complete-staged-check', ['git', 'diff', '--cached', '--check'])
tree_after = run('tree-after', ['git', 'write-tree']).decode().strip()
assert subprocess.check_output(['git', 'diff', '--name-only'], cwd=W) == b''
assert subprocess.check_output(['git', 'ls-files', '--others', '--exclude-standard'], cwd=W) == b''
I[policy] = hashlib.sha256(after).hexdigest()
dump(A / 'source-final.json', I)
with tarfile.open(A / 'source-final.tar.gz', 'w:gz', compresslevel=1) as tf:
    for n in sorted(I):
        tf.add(W / n, arcname=n, recursive=False)
receipt = dict(at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(), base_manifest_sha256=args.manifest_sha256, source_manifest_sha256=fh(A / 'source-final.json'), source_archive_sha256=fh(A / 'source-final.tar.gz'), source_files=len(I), source_delta=[policy], policy_before_sha256=hashlib.sha256(before).hexdigest(), policy_after_sha256=hashlib.sha256(after).hexdigest(), changed_policy_fields=keys, tree_before=tree_before, tree_after=tree_after, staged_path_count=len(actual), source_changes_beyond_policy=False, go_runtime_executed=False)
dump(A / 'receipt.json', receipt)
print(json.dumps(receipt, indent=2))
