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
print(json.dumps({'status' :'FIXED_HOSTED_SOURCE_CONTRACT_ONLY', 'checks':17, 'closure_files':len(manifest), 'Go_executions':0}))
