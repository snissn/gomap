#!/usr/bin/env python3
"""Pure source/closure contract only; never runs a diagnostic or imported tool."""
import ast
import hashlib
import json
from pathlib import Path
import sys

root = Path(sys.argv[1]).resolve(strict=True)
manifest = json.loads((root/'.github/scripts/original_elf_guard_inputs.json').read_text())
for name, digest in manifest.items():
    path = root/name
    assert path.is_file() and not path.is_symlink(), name
    assert hashlib.sha256(path.read_bytes()).hexdigest() == digest, name
source = (root/'.github/scripts/original_elf_guard_diagnostic.py').read_text()
ast.parse(source)
assert "('B1','B'), ('C1','C'), ('C2','C'), ('B2','B')" in source
assert "'-test.benchtime=20s'" in source and "'-test.count=1'" in source
assert "'-symbolize=none'" in source and "('-top','-cum')" not in source
assert "('cum',['-top','-cum'])" in source
assert "'-test.trace='" in source and "'-test.timeout=120s'" in source
assert "'GOMAXPROCS='" not in source  # Environment, never arbitrary command interpolation.
assert "subprocess.run(['taskkill', '/PID', str(self.child.pid), '/T', '/F']" in source
assert 'setup-go' not in source and 'go1.26.10.windows-amd64.zip' in source
assert "ROOT_CAP = GIB" in source and "LOG_CAP = 32*1024**2" in source
assert "os.environ['HOME']" not in source
assert "APPDATA=str(root/'appdata'), LOCALAPPDATA=str(root/'localappdata')" in source
assert 'timeout=min(180, attempt.check(ZIP_BYTES))' in source
assert 'package.extractall' not in source and 'shutil.copyfileobj' not in source
assert "HOLD_DESCENDANT_DRAIN_UNPROVED" in source and "row.get('taskkill_exit', 0) == 0" in source
assert "'static'" in source and "'There is no dynamic section in this file.'" in source
workflow = (root/'.github/workflows/treedb-mvcc-maintenance-diagnostic.yml').read_text()
assert 'actions/checkout@' not in workflow
assert "acquire_windows_source(attempt, root, token)" in source
assert "'--depth=1','--filter=blob:none','--no-tags','--no-recurse-submodules'" in source
assert "fetch_env.clear()" in source and "checkout_env.clear()" in source
assert "harness_is_git_worktree':False" in source
assert "(source/'.git/info').mkdir(parents=True, exist_ok=True)" in source
assert "def pulse(self):" in source and "attempt.check(total+1024)" in source
assert "member_bytes <= declared and expanded <= total" in source
assert "selected_written <= selected_total" in source
assert "system == 'Linux' and n == 'go' and m.isdir() and m.size == 0" in source
# Extract only the two original header predicates; never invoke provisioning.
from types import SimpleNamespace
header_checks = {node.args[1].value: compile(ast.Expression(node.args[0]), '<source-header-predicate>', 'eval')
                 for node in ast.walk(ast.parse(source)) if isinstance(node, ast.Call)
                 and isinstance(node.func, ast.Name) and node.func.id == 'need' and len(node.args) == 2
                 and isinstance(node.args[1], ast.Constant) and node.args[1].value in ('tool archive nonregular member', 'unsafe tool archive')}
def member(name, kind='file', size=0):
    return SimpleNamespace(name=name, size=size, isdir=lambda: kind == 'dir', isfile=lambda: kind == 'file')
header_cases = [
    ('root_directory', [member('go', 'dir'), member('go/bin/go', size=4)], True),
    ('root_file', [member('go')], False),
    ('nonzero_root_directory', [member('go', 'dir', 1)], False),
    ('duplicate', [member('go', 'dir'), member('go', 'dir')], False),
    ('traversal', [member('go/../escape')], False),
    ('link', [member('go/link', 'link')], False),
    ('wrong_prefix', [member('good/bin/go')], False),
]
assert len(header_checks) == 2
for label, members, expected in header_cases:
    scope = dict(system='Linux', members=members, names=[m.name for m in members], Path=Path)
    observed = all(eval(expr, scope) for expr in header_checks.values())
    assert observed == expected, label
print(json.dumps({'status':'FIXED_HOSTED_SOURCE_CONTRACT_ONLY', 'checks':27, 'header_predicate_cases':len(header_cases), 'closure_files':len(manifest), 'Go_executions':0}))
