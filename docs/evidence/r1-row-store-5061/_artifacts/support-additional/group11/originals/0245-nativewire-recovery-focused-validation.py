#!/usr/bin/env python3
import datetime, hashlib, json, os, pathlib, subprocess, sys

repo = pathlib.Path('/home/mikers/gomap-r1-5071-cow-repair-a645')
out = pathlib.Path('/home/mikers/gomap-r1-evidence-20261005/0245-nativewire-recovery-focused-validation')
head = '0245d8e0f5835dcba2929e4f1ba8b5d7ea909133'
runtime = 'cb5b0d7b3633666ae16752d8c353eeeb23542c71e5e25773e33359d3ffa79aa9'
goroot = '/home/mikers/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64'
go = goroot + '/bin/go'
env = {k: os.environ[k] for k in ('HOME', 'PATH') if k in os.environ}
env.update(GOROOT=goroot, GOCACHE='/home/mikers/.cache/gomap-r1-5071-fd-go1264',
           GOMODCACHE='/home/mikers/go/pkg/mod', GOTOOLCHAIN='local', GOWORK='off',
           GOENV='off', GOFLAGS='', GODEBUG='', GOMAXPROCS='12', GOGC='100',
           GOMEMLIMIT='off', PYTHONDONTWRITEBYTECODE='1')
os.environ.clear()
os.environ.update(env)
os.chdir(repo)
sys.path.insert(0, str(repo / 'scripts'))
import r1_collection_source

def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def source():
    x = r1_collection_source.source_identity()
    assert x['commit'] == head and x['runtime_sha256'] == runtime and x['clean']
    return x

def write(name, value):
    with (out / name).open('x') as f:
        json.dump(value, f, indent=2, sort_keys=True)
        f.write('\n')

assert subprocess.check_output([go, 'version'], text=True).strip() == 'go version go1.26.4 linux/amd64'
before = source()
out.mkdir(mode=0o700, exist_ok=False)
write('source-before.json', before)
packages = ['./TreeDB/nativewire']
regex = '^TestFixedPeerVectorInitializationRealRaftPrepareSourceConvergenceV1$'
commands = [('format', [goroot + '/bin/gofmt', '-l', 'TreeDB/nativewire/fixed_peer_vector_prepare_v1_test.go']),
            ('normal', [go, 'test', '-json', *packages, '-run', regex, '-count=1', '-timeout=10m']),
            ('race', [go, 'test', '-race', '-json', *packages, '-run', regex, '-count=1', '-timeout=10m']),
            ('vet', [go, 'vet', './TreeDB/nativewire'])]
for name, argv in commands:
    start = {'utc': now(), 'argv': argv, 'cwd': str(repo), 'environment': env, 'source': source(),
             'driver_sha256': sha(pathlib.Path(__file__)), 'go_sha256': sha(pathlib.Path(go))}
    write(name + '-start.json', start)
    log = out / (name + '.log')
    with log.open('xb') as f:
        result = subprocess.run(argv, cwd=repo, env=env, stdout=f, stderr=subprocess.STDOUT)
    write(name + '-exit.json', {'utc': now(), 'exit': result.returncode, 'log_sha256': sha(log), 'source': source()})
    print(name, 'exit', result.returncode, flush=True)
    if name == 'format' and log.read_bytes():
        raise RuntimeError('gofmt reported unformatted source')
    if result.returncode:
        sys.exit(result.returncode)
write('source-after.json', source())
print('FOCUSED_VALIDATION_PASS', head, flush=True)
