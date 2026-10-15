import datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

D = Path('/mnt/fast4tb/gomap-mvcc-4871-20261015')
W = D / 'ordinary-repair'
O = D / 'ordinary-hook-work' / 'root-metadata'
O.mkdir()
freeze = json.loads((D / 'ordinary-hook-work/freeze-final.json').read_text())
changed = json.loads((D / 'ordinary-hook-work/changed-files-final.json').read_text())

def git(*args):
    return subprocess.check_output(['git', *args], cwd=W, text=True).strip()

def source():
    files = git('ls-files', '--cached', '--others', '--exclude-standard').splitlines()
    return {f: hashlib.sha256((W / f).read_bytes()).hexdigest()
            for f in files if (W / f).is_file()}

assert git('rev-parse', 'HEAD') == freeze['head']
assert source() == freeze['files']
assert not git('diff', '--cached', '--name-only')
actual = set(git('diff', '--name-only').splitlines()) | set(git('ls-files', '--others', '--exclude-standard').splitlines())
assert actual == set(changed)
subprocess.run(['git', 'add', '--', *sorted(changed)], cwd=W, check=True)
subprocess.run(['git', 'diff', '--cached', '--check'], cwd=W, check=True)
(O / 'staged-product.patch').write_bytes(subprocess.check_output(['git', 'diff', '--cached', '--binary'], cwd=W))
env = os.environ.copy()
env.update(PYTHONDONTWRITEBYTECODE='1', TMPDIR='/mnt/fast4tb/gomap-mvcc-4871-20261015/ordinary-hook-work/tmp')
commands = [
    ['python3', '.github/scripts/refresh_ci_impact_inventory.py'],
    ['python3', '.github/scripts/refresh_ci_impact_inventory.py', '--check'],
    ['python3', '.github/scripts/test_refresh_ci_impact_inventory.py'],
    ['python3', '.github/scripts/test_ci_impact.py'],
    ['python3', '.github/scripts/test_treedb_ci_contract.py'],
    ['python3', '.github/scripts/test_treedb_windows_core_headroom.py'],
]
results = []
for i, argv in enumerate(commands):
    start = time.monotonic()
    p = subprocess.run(argv, cwd=W, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    (O / f'{i:02d}.log').write_text(p.stdout)
    results.append({'argv': argv, 'exit': p.returncode, 'elapsed_s': time.monotonic() - start})
    (O / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
    print(argv[-1], p.returncode, flush=True)
    assert p.returncode == 0
after = source()
delta = sorted(f for f in after if after[f] != freeze['files'].get(f))
assert delta == ['.github/ci/ci_impact.json'], delta
(O / 'ci-inventory.patch').write_bytes(subprocess.check_output(['git', 'diff', '--', '.github/ci/ci_impact.json'], cwd=W))
subprocess.run(['git', 'add', '--', '.github/ci/ci_impact.json'], cwd=W, check=True)
subprocess.run(['git', 'diff', '--cached', '--check'], cwd=W, check=True)
assert not git('diff', '--name-only')
assert not git('ls-files', '--others', '--exclude-standard')
receipt = {'staged_tree': git('write-tree'),
           'parent': freeze['head'], 'base': '5bca87ffe6e1bfba2d693264d2dc82936adc234f',
           'source_files': len(after), 'product_changes': changed,
           'freeze_sha256': hashlib.sha256((D / 'ordinary-hook-work/freeze-final.json').read_bytes()).hexdigest(),
           'source_diff_from_freeze': delta, 'metadata_checks': results,
           'at_utc': datetime.datetime.now(datetime.timezone.utc).isoformat()}
(D / 'ordinary-hook-work/staged-candidate.json').write_text(json.dumps(receipt, indent=2) + '\n')
print('staged candidate ready for manifest review', receipt['staged_tree'], flush=True)
