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
assert "authenticate_cached_windows_go(attempt, root)\n        else:\n            provision_go(attempt, root)" in source
assert "'ImageOS') == 'win25-vs2026'" in source and "'ImageVersion') == '20261030.294.1'" in source
assert "stat.FILE_ATTRIBUTE_REPARSE_POINT" in source
assert "directories == implicit" in source and "observed_files == set(identities)" in source
assert "sdk_writes_selected=False" in source
assert "'size': count" in source and "ensure_ascii=True).encode('ascii')" in source
assert "argv = [r'C:\\hostedtoolcache\\windows\\go\\1.26.10\\x64\\bin\\go.exe', *argv[1:]]" in source
# Pure canonical data controls; no SDK walk or tool invocation.
def identity(rows):
    return hashlib.sha256(json.dumps(sorted(rows, key=lambda row: row['path']), sort_keys=True,
        separators=(',', ':'), ensure_ascii=True).encode('ascii')).hexdigest()
fixture = [{'path':'bin/go.exe','size':2,'sha256':'a'*64}, {'path':'src/x.go','size':3,'sha256':'b'*64}]
expected_identity = identity(fixture)
assert identity(list(reversed(fixture))) == expected_identity
for changed in (fixture+[{'path':'extra','size':0,'sha256':'c'*64}], fixture[:-1],
                [dict(fixture[0],size=3),fixture[1]], [dict(fixture[0],sha256='d'*64),fixture[1]],
                [dict(fixture[0],path='bin/GO.exe'),fixture[1]]):
    assert identity(changed) != expected_identity

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
print(json.dumps({'status':'FIXED_HOSTED_SOURCE_CONTRACT_ONLY', 'checks':34, 'header_predicate_cases':len(header_cases), 'closure_files':len(manifest), 'canonical_data_cases':6, 'Go_executions':0}))

# Focused Windows compile-fit source and finite data controls, no subprocess/tool execution.
assert '\n  original-elf:' not in workflow and '\n  windows-guard:' in workflow
assert "['go','build','-p','1','-mod=readonly','net/http']" in source
assert source.index("'compile-only',") < source.index("'build', ['go','test','-c'")
assert "need(not after, 'compile-only WorkDir not removed" in source
assert "compiler_env['GOFLAGS'] == ''" in source and "compiler_env['GOEXPERIMENT'] == ''" in source
assert "env.get('GOFLAGS', '') == ''" in source and "env.get('GOEXPERIMENT', '') == ''" in source
assert "write(attempt.out/'tool-input.json'" in source
assert "write(attempt.out/'test-binary-identity.json'" in source
assert "census(self.root, enforce=False)" in source and "census(root, enforce=False)" in source
assert "self.fail('; '.join(row['errors']), row)" in source
assert "self.failure = message" in source and "if self.failure is None:" in source
assert "FINAL_RESOURCE_CENSUS" in source
assert "need(not retained['errors'], 'final resource refusal retained after partial receipt emission')" in source
# Execute only extracted pure category function, against paths/data, never the runner.
category_node = next(n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef) and n.name == 'census_category')
category_scope = {}
exec(compile(ast.Module(body=[category_node], type_ignores=[]), '<pure-category>', 'exec'), category_scope)
cases = {'source/x.go':'source','source/.git/objects/pack/x':'git','modcache/cache/download/x':'modules',
         'gocache/a/b':'go_cache','tmp/go-build1/x':'tmp','nativewire.test.exe':'binary',
         'output/receipt.json':'output','appdata/go/telemetry/x':'rest'}
observed = {name:category_scope['census_category'](Path(name)) for name in cases}
assert observed == cases
assert len(set(observed.values())) == 8
# Pure in-memory Attempt.sample/fail controls: stub write/census only; no process/thread startup.
import threading
nodes = [n for n in ast.parse(source).body if isinstance(n, ast.ClassDef) and n.name == 'Attempt']
methods = [n for n in nodes[0].body if isinstance(n, ast.FunctionDef) and n.name in ('fail','sample')]
recorded = []
row = {'whole_bytes':1073741825,'log_trace_json_bytes':7,'errors':['whole root exceeds 1GiB'],
       'categories':{key:0 for key in set(cases.values())}}
row['categories']['tmp']=row['whole_bytes']
assert sum(row['categories'].values()) == row['whole_bytes']
scope = {'write':lambda p,v:recorded.append((str(p),v)), 'census':lambda p,enforce:dict(row)}
exec(compile(ast.fix_missing_locations(ast.Module(body=[ast.ClassDef(name='PureAttempt', bases=[], keywords=[], body=methods, decorator_list=[])], type_ignores=[])), '<pure-failure>', 'exec'),scope)
owner = scope['PureAttempt']()
owner.root=Path('/source-fixture-only');owner.out=owner.root/'output';owner.stage='compile-only'
owner.failure=None;owner.first_failure=None;owner.state_lock=threading.Lock();owner.stop=threading.Event()
owner.maximum={'whole_bytes':0,'log_trace_json_bytes':0}
try:
    owner.sample()
    raise AssertionError('expected first failure')
except RuntimeError:
    pass
assert len(recorded)==1 and recorded[0][1]['census']['whole_bytes']==1073741825
assert owner.failure == 'whole root exceeds 1GiB'
owner.fail('later cleanup error')
assert len(recorded)==1 and owner.failure=='whole root exceeds 1GiB'
assert owner.maximum['whole_bytes']==1073741825
print(json.dumps({'status':'WINDOWS_COMPILE_FIT_SOURCE_AND_PURE_DATA_ONLY','category_cases':8,
                  'first_failure_and_sticky_cases':2,'Go_executions':0,'controller_executions':0}))

# A failed first-failure write still signals stop and preserves the in-memory first failure.
owner.failure=None;owner.first_failure=None;owner.stop.clear()
def failed_write(path, value):
    raise OSError('synthetic receipt write error')
scope['write']=failed_write
try:
    owner.fail('first failure with receipt I/O failure')
    raise AssertionError('expected receipt I/O failure')
except OSError:
    pass
assert owner.stop.is_set() and owner.failure=='first failure with receipt I/O failure'
assert "'primary_error': primary_error" in source
assert "if primary_error is None:" in source
print(json.dumps({'status':'FIRST_FAILURE_WRITE_ERROR_STOP_CONTROL_PASS','cases':1,'Go_executions':0}))
