import hashlib
import importlib.util
import json
import pathlib
import signal
import subprocess
import sys

ROOT = pathlib.Path('/mnt/fast4tb/gomap-mongo-execution-20261024')
WORK = ROOT / 'consumer-canonical'
PACKET = pathlib.Path(__file__).parent
spec = importlib.util.spec_from_file_location('owned_process', ROOT / 'evidence/native-query-work-witness1/run.py')
H = importlib.util.module_from_spec(spec)
spec.loader.exec_module(H)
signal.signal(signal.SIGTERM, H.interrupted)
signal.signal(signal.SIGINT, H.interrupted)

def source():
    paths = subprocess.check_output(['git', 'diff', '--name-only', 'HEAD'], cwd=WORK, text=True).splitlines()
    paths += subprocess.check_output(['git', 'ls-files', '--others', '--exclude-standard'], cwd=WORK, text=True).splitlines()
    return {'head': H.git(WORK, 'rev-parse', 'HEAD'), 'tree': H.git(WORK, 'rev-parse', 'HEAD^{tree}'),
            'status': H.git(WORK, 'status', '--porcelain'),
            'changed_files': {n: H.digest(WORK / n) if (WORK / n).exists() else None for n in sorted(set(paths))}}

name, args = sys.argv[1], sys.argv[2:]
assert name.replace('-', '').isalnum()
receipt, log = PACKET / (name + '-receipt.json'), PACKET / (name + '.log')
assert not receipt.exists() and not log.exists()
before = source()
command = ['/home/mikers/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.0.linux-amd64/bin/go', 'test', '-json', '-count=1', '-timeout=6m', *args]
H.write_json(PACKET / (name + '-plan.json'), {'source': before, 'command': command, 'outer_wall_seconds': 600, 'scope': 'Source-bound public cold Hybrid and queued producer-pressure regression repair; preserve actual REDs, sameassertion GREEN, original cap/corpus/strategy/durableACK; no numerical/default qualification'})
env = H.safe_env({'GOROOT': '/home/mikers/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.0.linux-amd64', 'GOTOOLCHAIN': 'local', 'GOMAXPROCS': '4', 'GOCACHE': str(ROOT / 'gocache'), 'TMPDIR': str(ROOT / 'tmp')})
result = H.run_process(command, str(WORK), env, log, 600)
result.update(source_before=before, source_after=source())
result['source_unchanged'] = result['source_before'] == result['source_after']
H.write_json(receipt, result)
print(json.dumps({k: v for k, v in result.items() if k not in ('source_before', 'source_after')}))
sys.exit(0 if result['process_ok'] and result['source_unchanged'] else 1)
