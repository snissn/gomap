#!/usr/bin/env python3
"""Root-private final A observer with independently observed host environment; freeze independent reviewed landed expectations before capture."""
import hashlib
import importlib.util
import json
import os
import re
from pathlib import Path
import subprocess
import sys
import time

# Keep source inventory imports read-only even before child environment freeze.
sys.dont_write_bytecode = True


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def freeze(path, value):
    with Path(path).open('x') as output:
        json.dump(value, output, indent=2, sort_keys=True)
        output.write('\n')


def now():
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())


def runner_observation(config, environment):
    temporary = Path(environment['TMPDIR']).resolve()
    output_parent = Path(config['out']).resolve().parent
    return {'utc': now(), 'cpu_count': os.cpu_count(), 'uname': list(os.uname()),
            'loadavg': list(os.getloadavg()),
            'temp_device': temporary.stat().st_dev,
            'output_parent_device': output_parent.stat().st_dev,
            'temp_filesystem': subprocess.check_output(['df', '-Pk', str(temporary)], text=True),
            'output_filesystem': subprocess.check_output(['df', '-Pk', str(output_parent)], text=True),
            'top_processes': subprocess.check_output(['ps', '-eo', 'pcpu,pmem,comm', '--sort=-pcpu'], text=True).splitlines()[:12],
            'environment': dict(environment)}


def checked_source(config):
    # Independently observe checkout bytes using reviewed inventory logic, not
    # packet declarations. Exact clean commit binds the imported helper itself.
    repo = Path(config['repo']).resolve()
    os.chdir(repo)
    actual = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
    dirty = subprocess.check_output(['git', 'status', '--porcelain', '--untracked-files=normal'], text=True).strip()
    if actual != config['source_commit'] or dirty:
        raise ValueError('expected exact clean source is absent')
    subprocess.run(['git', 'merge-base', '--is-ancestor', config['landed_tooling_commit'], actual], check=True)
    sys.path.insert(0, str(repo / 'scripts'))
    spec = importlib.util.spec_from_file_location('audited_capture', repo / 'scripts/r1_collection_source.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    observed = module.source_identity()
    for key, external in [('commit', 'source_commit'), ('runtime_sha256', 'runtime_sha256'), ('harness_sha256', 'harness_sha256')]:
        if observed[key] != config[external]:
            raise ValueError('independent frozen ' + external + ' mismatch')
    if not observed['clean']:
        raise ValueError('source changed during inventory')
    return observed


def wrapper(config_path):
    config = json.loads(Path(config_path).read_text())
    args = sys.argv[3:]
    real_go = config['real_go']
    if not args or args[0] != 'build':
        return subprocess.call([real_go, *args])
    binary = Path(config['out']).resolve() / 'collection_workload_bench'
    if args != ['build', '-o', str(binary), './cmd/collection_workload_bench']:
        raise ValueError('unexpected compiler invocation')
    if binary.exists():
        raise ValueError('compiler output already exists')
    receipt = Path(config['receipt_dir'])
    before = checked_source(config)
    go_build_environment = json.loads(subprocess.check_output([real_go, 'env', '-json',
        'GOOS', 'GOARCH', 'CGO_ENABLED', 'GOROOT', 'GOTOOLCHAIN', 'GOFLAGS',
        'CC', 'CXX', 'CGO_CFLAGS', 'CGO_CPPFLAGS', 'CGO_CXXFLAGS', 'CGO_LDFLAGS'], text=True))
    freeze(receipt / 'build-start.json', {'utc': now(), 'argv': [real_go, *args],
           'cwd': str(Path.cwd()), 'source': before, 'go_executable_sha256': sha(real_go),
           'environment': dict(os.environ), 'go_build_environment': go_build_environment})
    status = subprocess.call([real_go, *args])
    after = checked_source(config)
    record = {'utc': now(), 'exit': status, 'source': after, 'argv': [real_go, *args]}
    if status == 0:
        if before != after:
            raise ValueError('source changed during build')
        record.update(binary=str(binary), binary_sha256=sha(binary),
                      runner=runner_observation(config, os.environ))
    freeze(receipt / 'build-completed.json', record)
    # Driver cannot begin benchmark processes until this independently observed
    # completed-build receipt exists and the wrapper has returned success.
    return status


def check_validation_bindings(config, trusted, observed_source, expected_source, out):
    if checked_source(config) != observed_source:
        raise ValueError('source changed before/after independent validation')
    if sha(expected_source) != trusted['expected_source_sha256'] or json.loads(expected_source.read_text()) != observed_source:
        raise ValueError('independently frozen source manifest changed')
    if sha(out / 'collection_workload_bench') != trusted['binary_sha256']:
        raise ValueError('observed executable byte identity mismatch')
    if sha(out / 'packet.json') != trusted['packet_sha256']:
        raise ValueError('observed completed packet byte identity mismatch')
    # These are consistency comparisons to independent observations, not a way
    # to obtain expected identities from the measured packet/capture metadata.
    for name in ('source.json', 'source-after.json'):
        if json.loads((out / name).read_text()) != observed_source:
            raise ValueError('capture source sidecar differs from independent source')
    if json.loads((out / 'packet.json').read_text()).get('source') != observed_source:
        raise ValueError('packet source differs from independent source')


def parent(config_path):
    original = Path(config_path).resolve()
    config = json.loads(original.read_text())
    required = ('source_commit', 'runtime_sha256', 'harness_sha256', 'landed_tooling_commit',
                'review_url', 'landing_observation', 'repo', 'out', 'receipt_dir', 'real_go',
                'goroot', 'gocache', 'gomodcache', 'lock')
    if not all(config.get(key) for key in required):
        raise ValueError('root must fill every source/landing/environment expectation before authorization')
    if config['source_commit'] != config['landed_tooling_commit']:
        raise ValueError('A retained source must be the selected exact landed runtime and tooling commit')
    for key, length in [('source_commit', 40), ('landed_tooling_commit', 40), ('runtime_sha256', 64), ('harness_sha256', 64)]:
        if not re.fullmatch('[0-9a-f]{' + str(length) + '}', config[key]):
            raise ValueError('invalid independently frozen ' + key)
    receipt = Path(config['receipt_dir']).resolve()
    repo, out = Path(config['repo']).resolve(), Path(config['out']).resolve()
    if repo == receipt or repo in receipt.parents or receipt == out or receipt in out.parents or out in receipt.parents:
        raise ValueError('receipt must be separate from checkout and capture output')
    if Path(config['lock']).resolve() not in (Path('/home/mikers/gomap-r1-evidence-20261005/timed-capture.lock'), Path('/home/mikers/gomap-r1-correctness-185.lock')):
        raise ValueError('use the existing coordinator lock')
    if os.environ.get('R1_RECEIPT_LOCKED') != 'yes':
        raise ValueError('invoke under configured flock with R1_RECEIPT_LOCKED=yes')
    if out.exists():
        raise ValueError('capture output already exists; preserve it')
    receipt.mkdir(mode=0o700, parents=True, exist_ok=False)
    frozen_config = receipt / 'root-frozen-inputs.json'
    freeze(frozen_config, config)
    environment = {key: os.environ[key] for key in ('HOME', 'PATH') if key in os.environ}
    environment.update(GOROOT=config['goroot'], GOCACHE=config['gocache'], GOMODCACHE=config['gomodcache'],
                       GOTOOLCHAIN='local', GOWORK='off', GOENV='off', GOFLAGS='', GODEBUG='',
                       GOMAXPROCS='16', GOGC='100', GOMEMLIMIT='off', PYTHONDONTWRITEBYTECODE='1')
    os.environ.clear()
    os.environ.update(environment)
    if subprocess.check_output([config['real_go'], 'version'], text=True).strip() != 'go version go1.26.4 linux/amd64':
        raise ValueError('matching frozen Linux Go toolchain is required')
    before = checked_source(config)
    freeze(receipt / 'observer-before.json', {'utc': now(), 'source': before,
           'observer_sha256': sha(__file__), 'root_config_sha256': sha(original), 'environment': environment})
    wrapper_path = receipt / 'go'
    wrapper_path.write_text('#!' + sys.executable + '\nimport os\nos.execv(' + repr(sys.executable) + ', ' + repr([sys.executable, str(Path(__file__).resolve()), '--wrapper', str(frozen_config)]) + ' + __import__("sys").argv[1:])\n')
    wrapper_path.chmod(0o700)
    environment['R1_GO'] = str(wrapper_path)
    expected_source = receipt / 'independent-expected-source.json'
    freeze(expected_source, before)
    expected_source_sha256 = sha(expected_source)
    environment.update(R1_MODE='r1', R1_OUT=str(out), TMPDIR=str(receipt / 'temporary-db-roots'))
    Path(environment['TMPDIR']).mkdir(mode=0o700)
    freeze(receipt / 'runner-before.json', runner_observation(config, environment))
    command = ['bash', str(repo / 'scripts/r1_collection_capture.sh'), '-qualification', 'retained',
               '-documents', '4096', '-batch-size', '32', '-operations', '1000', '-repetitions', '5',
               '-durability', 'durable', '-read-state', 'flushed',
               '-engines', 'json,template-v1,bson,typed-row,sqlite-json,sqlite-row']
    freeze(receipt / 'run-start.json', {'utc': now(), 'argv': command, 'environment': environment})
    with (receipt / 'driver.log').open('x') as log:
        status = subprocess.call(command, cwd=repo, env=environment, stdout=log, stderr=subprocess.STDOUT)
    freeze(receipt / 'runner-after.json', runner_observation(config, environment))
    freeze(receipt / 'run-exit.json', {'utc': now(), 'exit': status, 'driver_log_sha256': sha(receipt / 'driver.log')})
    if status:
        return status  # Preserve all failed observations; no acceptance receipt.
    after = checked_source(config)
    build = json.loads((receipt / 'build-completed.json').read_text())
    if before != after or build['source'] != before or build['exit'] != 0 or sha(out / 'collection_workload_bench') != build['binary_sha256']:
        raise ValueError('source or observed executable changed')
    # No expected value is obtained from packet metadata. Hash the original
    # completed packet bytes after the observed audited-driver successful run.
    trusted = {key: config[key] for key in ('source_commit', 'runtime_sha256', 'harness_sha256', 'landed_tooling_commit', 'review_url', 'landing_observation')}
    trusted.update(binary_sha256=build['binary_sha256'], packet_sha256=sha(out / 'packet.json'),
                   expected_source_sha256=expected_source_sha256,
                   utc=now(), observer_sha256=sha(__file__), capture_directory=str(out), exit=0)
    freeze(receipt / 'trusted-completed-receipt.json', trusted)
    # A's original CLI accepts only an independently frozen source manifest.
    # The root observer verifies executable/packet byte pins OUTSIDE that CLI.
    check_validation_bindings(config, trusted, before, expected_source, out)
    validation = [str(out / 'collection_workload_bench'), 'r1-validate',
                  '-source-manifest', str(expected_source), str(out / 'packet.json')]
    freeze(receipt / 'validation-start.json', {'utc': now(), 'argv': validation, 'cwd': str(repo), 'environment': environment})
    with (receipt / 'independent-validation.log').open('x') as log:
        status = subprocess.call(validation, cwd=repo, env=environment, stdout=log, stderr=subprocess.STDOUT)
    check_validation_bindings(config, trusted, before, expected_source, out)
    freeze(receipt / 'validation-exit.json', {'utc': now(), 'exit': status,
           'binary_sha256': sha(out / 'collection_workload_bench'),
           'packet_sha256': sha(out / 'packet.json'),
           'validation_log_sha256': sha(receipt / 'independent-validation.log')})
    return status


if __name__ == '__main__':
    if len(sys.argv) >= 3 and sys.argv[1] == '--wrapper':
        sys.exit(wrapper(sys.argv[2]))
    if len(sys.argv) != 2:
        raise SystemExit('usage: observe.py ROOT_PRIVATE_CONFIG.json')
    sys.exit(parent(sys.argv[1]))
