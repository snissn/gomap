import hashlib, json, sys
from pathlib import Path
artifacts, source = map(Path, sys.argv[1:])
expected = json.loads((artifacts / 'source-identity.json').read_text())['files']
actual = {str(p.relative_to(source)): p for p in source.rglob('*') if p.is_file()}
extras = sorted(set(actual)-set(expected))
prefixes = ('ci_impact.', 'test_ci_impact.', 'test_refresh_ci_impact_inventory.', 'refresh_ci_impact_inventory.')
assert extras and all(Path(name).parent.as_posix()=='.github/scripts/__pycache__' and Path(name).suffix=='.pyc' and Path(name).name.startswith(prefixes) for name in extras), extras
assert all(hashlib.sha256(actual[name].read_bytes()).hexdigest()==digest for name,digest in expected.items())
receipt = dict(kind='owned isolated runner Python bytecode cleanup; no tracked source modification',removed=[dict(path=name,sha256=hashlib.sha256(actual[name].read_bytes()).hexdigest(),bytes=actual[name].stat().st_size) for name in extras])
(artifacts/'owned-bytecode-cleanup.json').write_text(json.dumps(receipt,indent=2)+'\n')
for name in extras: actual[name].unlink()
(source/'.github/scripts/__pycache__').rmdir()
