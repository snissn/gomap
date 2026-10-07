#!/usr/bin/env python3
"""Root-private template; run only after root verifies landing and authorizes capture."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def freeze(path, value):
    with Path(path).open('x') as output:
        json.dump(value, output, indent=2, sort_keys=True)
        output.write('\n')


def now():
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())


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
    spec = importlib.util.spec_from_file_location('audited_capture', repo / 'scripts/r1_lifecycle_capture.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    observed = module.source(config['real_go'])
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
    if not args or args[0] != 'test':
        return subprocess.call([real_go, *args])
    binary = Path(config['out']).resolve() / 'collections.test'
    if args != ['test', '-c', '-o', str(binary), './TreeDB/collections']:
        raise ValueError('unexpected compiler invocation')
    receipt = Path(config['receipt_dir'])
    before = checked_source(config)
    freeze(receipt / 'build-start.json', {'utc': now(), 'argv': [real_go, *args],
           'cwd': str(Path.cwd()), 'source': before, 'go_executable_sha256': sha(real_go),
           'environment': dict(os.environ)})
    status = subprocess.call([real_go, *args])
    after = checked_source(config)
    record = {'utc': now(), 'exit': status, 'source': after, 'argv': [real_go, *args]}
    if status == 0:
        if before != after:
            raise ValueError('source changed during build')
        record.update(binary=str(binary), binary_sha256=sha(binary))
    freeze(receipt / 'build-completed.json', record)
    # Driver cannot begin benchmark processes until this independently observed
    # completed-build receipt exists and the wrapper has returned success.
    return status


def parent(config_path):
    original = Path(config_path).resolve()
    config = json.loads(original.read_text())
    required = ('source_commit', 'runtime_sha256', 'harness_sha256', 'landed_tooling_commit',
                'review_url', 'landing_observation', 'repo', 'out', 'receipt_dir', 'real_go',
                'goroot', 'gocache', 'gomodcache', 'lock')
    if not all(config.get(key) for key in required):
        raise ValueError('root must fill every source/landing/environment expectation before authorization')
    receipt = Path(config['receipt_dir']).resolve()
    repo, out = Path(config['repo']).resolve(), Path(config['out']).resolve()
    if repo == receipt or repo in receipt.parents or receipt == out or receipt in out.parents or out in receipt.parents:
        raise ValueError('receipt must be separate from checkout and capture output')
    if Path(config['lock']).resolve() not in (Path('/home/mikers/gomap-r1-evidence-20261005/timed-capture.lock'), Path('/home/mikers/gomap-r1-correctness-185.lock')):
        raise ValueError('use the existing coordinator lock')
    if os.environ.get('R1_RECEIPT_LOCKED') != 'yes':
        raise ValueError('invoke under configured flock with R1_RECEIPT_LOCKED=yes')
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
    command = [sys.executable, str(repo / 'scripts/r1_lifecycle_capture.py'), '--qualification', 'retained',
               '--out', str(out), '--repetitions', '5', '--epochs', '5', '--documents', '4096', '--calls-per-epoch', '1024']
    for flag, key in [('source-commit', 'source_commit'), ('runtime-sha256', 'runtime_sha256'),
                      ('harness-sha256', 'harness_sha256'), ('landed-tooling-commit', 'landed_tooling_commit'), ('review-url', 'review_url')]:
        command.extend(['--' + flag, config[key]])
    freeze(receipt / 'run-start.json', {'utc': now(), 'argv': command, 'environment': environment})
    with (receipt / 'driver.log').open('x') as log:
        status = subprocess.call(command, cwd=repo, env=environment, stdout=log, stderr=subprocess.STDOUT)
    freeze(receipt / 'run-exit.json', {'utc': now(), 'exit': status, 'driver_log_sha256': sha(receipt / 'driver.log')})
    if status:
        return status  # Preserve all failed observations; no acceptance receipt.
    after = checked_source(config)
    build = json.loads((receipt / 'build-completed.json').read_text())
    if before != after or build['source'] != before or build['exit'] != 0 or sha(out / 'collections.test') != build['binary_sha256']:
        raise ValueError('source or observed executable changed')
    # No expected value is obtained from packet metadata. Hash the original
    # completed packet bytes after the observed audited-driver successful run.
    trusted = {key: config[key] for key in ('source_commit', 'runtime_sha256', 'harness_sha256', 'landed_tooling_commit', 'review_url', 'landing_observation')}
    trusted.update(binary_sha256=build['binary_sha256'], packet_sha256=sha(out / 'packet.json'),
                   utc=now(), observer_sha256=sha(__file__), capture_directory=str(out), exit=0)
    freeze(receipt / 'trusted-completed-receipt.json', trusted)
    # Independent acceptance validator uses this separate root receipt.
    validation = [sys.executable, str(repo / 'scripts/r1_lifecycle_validate.py'), str(out / 'packet.json')]
    for flag, key in [('expected-commit', 'source_commit'), ('expected-runtime', 'runtime_sha256'), ('expected-harness', 'harness_sha256'),
                      ('expected-landed-tooling-commit', 'landed_tooling_commit'), ('expected-binary-sha256', 'binary_sha256'), ('expected-packet-sha256', 'packet_sha256')]:
        validation.extend(['--' + flag, trusted[key]])
    freeze(receipt / 'validation-start.json', {'utc': now(), 'argv': validation, 'cwd': str(repo), 'environment': environment})
    with (receipt / 'independent-validation.log').open('x') as log:
        status = subprocess.call(validation, cwd=repo, env=environment, stdout=log, stderr=subprocess.STDOUT)
    freeze(receipt / 'validation-exit.json', {'utc': now(), 'exit': status})
    return status


if __name__ == '__main__':
    if len(sys.argv) >= 3 and sys.argv[1] == '--wrapper':
        sys.exit(wrapper(sys.argv[2]))
    if len(sys.argv) != 2:
        raise SystemExit('usage: observe.py ROOT_PRIVATE_CONFIG.json')
    sys.exit(parent(sys.argv[1]))
