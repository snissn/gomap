#!/usr/bin/env python3
"""Verify existing original D provenance only. Never run Go or the validator."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

M = '3325dfe77940fec8587d8b61b1ac4e0b2f72caca'
RUNTIME = 'eab40aaf77ed307d92a7a38adf0dd80be07a7e12bdda8a2ec3118baf9cad33ff'
HARNESS = '325d5b41579a492a430da6cbb6162363ff82b42bb6553eddf2e325584fab4c13'
CONFIG = Path('/home/mikers/gomap-r1-final-configs-20261006/M-3325dfe/D-config.json')
CONFIG_SHA = 'b9998acfae072641810e625bb6e6ce1e7698e78a87e5d7f2f6b73c061f2b31df'
MANIFEST = CONFIG.parent / 'derived-D-source.json'
MANIFEST_SHA = '96a3f3feb2ed3d770e082fe5eda63c2faed1804b29e6e8a9c0f3d8740a595e04'
OBSERVER_SHA = '03c9807292fb42a7c09fcbafed4895703a5477a92e7f827331b93d9084282c68'
PARSER_SHA = 'dcfd45e233bc46a1c1f73ffaf2af840e5d8ab8ba312342ceb61b014e7c199968'
RESULTS = Path('/home/mikers/gomap-r1-final-results-20261006/M-3325dfe/D')
LOCK = '/home/mikers/gomap-r1-evidence-20261005/timed-capture.lock'
STATE = 'ROOT_PROVENANCE_QUALIFIED_NUMERIC_CAPACITY_AND_COST_ACCEPTANCE_PENDING'

def require(value, message):
    if not value:
        raise ValueError(message)

def sha_bytes(data):
    return hashlib.sha256(data).hexdigest()

def sha(path):
    return sha_bytes(Path(path).read_bytes())

def pairs(entries):
    result = {}
    for key, value in entries:
        require(key not in result, 'duplicate JSON key: ' + key)
        result[key] = value
    return result

def read(path):
    def invalid(value):
        raise ValueError('nonfinite JSON: ' + value)
    return json.loads(Path(path).read_text(), object_pairs_hook=pairs, parse_constant=invalid)

def manifest_hash(files):
    return sha_bytes(b''.join(p.encode() + b'\0' + h.encode() + b'\0' for p, h in sorted(files.items())))

def freeze(path, value):
    with Path(path).open('x') as output:
        json.dump(value, output, indent=2, sort_keys=True, allow_nan=False)
        output.write('\n')

def utc(record):
    value = record['utc']
    require(isinstance(value, str) and value.endswith('Z'), 'missing original UTC')
    return datetime.datetime.strptime(value, '%Y-%m-%dT%H:%M:%SZ')

def git(repo, *args):
    return subprocess.check_output(['git', '-C', str(repo), *args])

def clean(repo):
    require(git(repo, 'rev-parse', 'HEAD').decode().strip() == M, 'current source is not exact M')
    require(not git(repo, 'status', '--porcelain', '--untracked-files=normal').strip(), 'current source is dirty')

def source_from_git(repo, expected):
    # Pre-D independently pinned go-list selection is authoritative; do not rerun Go.
    tree = {}
    for entry in git(repo, 'ls-tree', '-rz', M).split(b'\0'):
        if entry:
            metadata, path = entry.split(b'\t', 1)
            mode, kind, blob = metadata.decode().split()
            tree[path.decode()] = (mode, kind, blob)
    runtime = {p: m[2] for p, m in tree.items() if p in ('go.mod', 'go.sum') or
               (p.startswith(('TreeDB/', 'cmd/internal/treedbstats/')) and
                Path(p).suffix in ('.go', '.s', '.S', '.c', '.h', '.syso') and not p.endswith('_test.go'))}
    tests = expected['compiled_test_files']
    require(isinstance(tests, list) and tests == sorted(set(tests)) and bool(tests), 'invalid pre-D compiled test selection')
    require(all(p.startswith('TreeDB/collections/') and p.endswith('_test.go') and p in tree for p in tests), 'wrong compiled test package')
    harness = set(tests) | {p for p in tree if p.startswith('scripts/r1_lifecycle') and
                            '/' not in p[len('scripts/'):] and Path(p).suffix in ('.py', '.sh')} | {'scripts/r1_collection_source.py'}
    require(set(expected['harness_files']) == harness, 'pre-D harness scope disagrees with actual Git paths')
    paths = set(runtime) | harness
    require(all(tree[p][0] in ('100644', '100755') and tree[p][1] == 'blob' for p in paths), 'source is not a regular Git blob')
    ids = sorted({tree[p][2] for p in paths})
    raw = subprocess.check_output(['git', '-C', str(repo), 'cat-file', '--batch'], input=('\n'.join(ids) + '\n').encode())
    objects, offset = {}, 0
    for identity in ids:
        end = raw.index(b'\n', offset)
        actual, kind, size = raw[offset:end].decode().split()
        require(actual == identity and kind == 'blob', 'unexpected Git object response')
        size = int(size)
        body = raw[end + 1:end + 1 + size]
        require(len(body) == size and raw[end + 1 + size:end + 2 + size] == b'\n', 'truncated Git blob')
        objects[identity] = body
        offset = end + 2 + size
    require(offset == len(raw), 'extra Git batch bytes')
    hashes = {p: sha_bytes(objects[tree[p][2]]) for p in paths}
    require(all(sha(repo / p) == hashes[p] for p in paths), 'working bytes differ from committed source')
    working = {p: hashes[p] for p in runtime}
    files = {p: hashes[p] for p in harness}
    actual = {'commit': M, 'clean': True, 'runtime_sha256': manifest_hash(runtime), 'runtime_blobs': runtime,
              'runtime_working_sha256': manifest_hash(working), 'runtime_files': working,
              'compiled_test_files': tests, 'harness_files': files, 'harness_sha256': manifest_hash(files)}
    require(actual == expected, 'actual Git/file source differs from independently frozen pre-D manifest')
    require(actual['runtime_sha256'] == RUNTIME and actual['harness_sha256'] == HARNESS, 'actual inventory pin mismatch')
    return actual

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--observer', type=Path, required=True, help='actual original observe-D.py path; bytes are pinned')
    args = parser.parse_args()
    require(os.environ.get('R1_RECEIPT_LOCKED') == 'yes', 'root must externally hold the canonical flock')
    require(not RESULTS.exists(), 'results directory must be NEW; never replace a previous qualification')
    require(sha(CONFIG) == CONFIG_SHA and sha(MANIFEST) == MANIFEST_SHA, 'independent BEFORE-D config/manifest changed')
    require(sha(args.observer) == OBSERVER_SHA, 'actual observer changed')
    cfg, expected = read(CONFIG), read(MANIFEST)
    require(cfg['source_commit'] == M and cfg['runtime_sha256'] == RUNTIME and cfg['harness_sha256'] == HARNESS, 'root config source pin mismatch')
    require(str(Path(cfg['lock']).resolve()) == LOCK, 'wrong canonical lock')
    repo, out, receipts = (Path(cfg[key]).resolve() for key in ('repo', 'out', 'receipt_dir'))
    require(len({repo, out, receipts, RESULTS}) == 4 and all(a not in b.parents for a in (repo, out, receipts, RESULTS) for b in (repo, out, receipts, RESULTS) if a != b), 'overlapping source/capture/receipt/results paths')
    clean(repo)
    subprocess.run(['git', '-C', str(repo), 'merge-base', '--is-ancestor', cfg['landed_tooling_commit'], M], check=True)
    source = source_from_git(repo, expected)
    require(read(receipts / 'root-frozen-inputs.json') == cfg, 'observer frozen config differs')
    before = read(receipts / 'observer-before.json')
    build_start, build = read(receipts / 'build-start.json'), read(receipts / 'build-completed.json')
    run_start, run_exit = read(receipts / 'run-start.json'), read(receipts / 'run-exit.json')
    trusted = read(receipts / 'trusted-completed-receipt.json')
    validation_start, validation_exit = read(receipts / 'validation-start.json'), read(receipts / 'validation-exit.json')
    packet = read(out / 'packet.json')
    require(before['source'] == build_start['source'] == build['source'] == source, 'observer/compiler source differs')
    require(read(out / 'source-before.json') == read(out / 'source-after.json') == packet['source_before'] == packet['source_after'] == source, 'packet/source sidecars differ')
    require(before['observer_sha256'] == trusted['observer_sha256'] == OBSERVER_SHA and before['root_config_sha256'] == CONFIG_SHA, 'original observer/config pins differ')
    require(all(type(r['exit']) is int and r['exit'] == 0 for r in (build, run_exit, trusted, validation_exit)), 'original command failed; preserve all originals')
    environment = before['environment']
    fixed = {'GOROOT': cfg['goroot'], 'GOCACHE': cfg['gocache'], 'GOMODCACHE': cfg['gomodcache'],
             'GOTOOLCHAIN': 'local', 'GOWORK': 'off', 'GOENV': 'off', 'GOFLAGS': '', 'GODEBUG': '',
             'GOMAXPROCS': '16', 'GOGC': '100', 'GOMEMLIMIT': 'off', 'PYTHONDONTWRITEBYTECODE': '1'}
    require(all(environment.get(k) == v for k, v in fixed.items()) and set(environment) <= set(fixed) | {'HOME', 'PATH'}, 'observer environment not frozen')
    effective = {**environment, 'R1_GO': str(receipts / 'go')}
    require(run_start['environment'] == build_start['environment'] == validation_start['environment'] == effective, 'original compiler/driver/validator environments disagree')
    binary = out / 'collections.test'
    compiler = [cfg['real_go'], 'test', '-c', '-o', str(binary), './TreeDB/collections']
    require(build_start['argv'] == build['argv'] == compiler and build_start['cwd'] == str(repo), 'original real compiler command differs')
    require(build_start['go_executable_sha256'] == sha(cfg['real_go']), 'original compiler executable differs')
    require(build['binary'] == str(binary) and build['binary_sha256'] == trusted['binary_sha256'] == packet['toolchain']['binary_sha256'] == sha(binary), 'original ELF differs')
    require(binary.read_bytes()[:4] == b'\x7fELF', 'compiled test is not an ELF')
    require(trusted['packet_sha256'] == sha(out / 'packet.json') and trusted['capture_directory'] == str(out), 'trusted original packet/output differs')
    for key in ('source_commit', 'runtime_sha256', 'harness_sha256', 'landed_tooling_commit', 'review_url', 'landing_observation'):
        require(trusted[key] == cfg[key], 'trusted BEFORE-validation root input differs: ' + key)
    python = run_start['argv'][0]
    require(isinstance(python, str) and Path(python).is_absolute(), 'original Python executable path is missing')
    wrapper_body = ('#!' + python + '\nimport os\nos.execv(' + repr(python) + ', ' +
                    repr([python, str(args.observer.resolve()), '--wrapper', str(receipts / 'root-frozen-inputs.json')]) +
                    ' + __import__("sys").argv[1:])\n')
    require((receipts / 'go').read_text() == wrapper_body, 'original compiler wrapper does not match frozen observer')
    command = [python, str(repo / 'scripts/r1_lifecycle_capture.py'), '--qualification', 'retained', '--out', str(out), '--repetitions', '5', '--epochs', '5', '--documents', '4096', '--calls-per-epoch', '1024']
    for flag, key in [('source-commit', 'source_commit'), ('runtime-sha256', 'runtime_sha256'), ('harness-sha256', 'harness_sha256'), ('landed-tooling-commit', 'landed_tooling_commit'), ('review-url', 'review_url')]:
        command.extend(['--' + flag, cfg[key]])
    require(run_start['argv'] == command and run_exit['driver_log_sha256'] == sha(receipts / 'driver.log'), 'original driver command/log differs')
    require((receipts / 'driver.log').stat().st_size > 0, 'empty original driver log')
    validator = repo / 'scripts/r1_lifecycle_validate.py'
    require(sha(validator) == PARSER_SHA and source['harness_files']['scripts/r1_lifecycle_validate.py'] == PARSER_SHA, 'frozen original validator changed')
    validation = [python, str(validator), str(out / 'packet.json')]
    for flag, key in [('expected-commit', 'source_commit'), ('expected-runtime', 'runtime_sha256'), ('expected-harness', 'harness_sha256'), ('expected-landed-tooling-commit', 'landed_tooling_commit'), ('expected-binary-sha256', 'binary_sha256'), ('expected-packet-sha256', 'packet_sha256')]:
        validation.extend(['--' + flag, trusted[key]])
    require(validation_start['argv'] == validation and validation_start['cwd'] == str(repo), 'original validator did not receive exactly six independent pins')
    require((receipts / 'independent-validation.log').stat().st_size > 0, 'empty original validation log')
    chronology = [before, run_start, build_start, build, run_exit, trusted, validation_start, validation_exit]
    require(all(utc(a) <= utc(b) for a, b in zip(chronology, chronology[1:])), 'original observer chronology differs')
    config = packet['config']
    require(packet['schema'] == 'gomap-r1-lifecycle-packet-v3' and all(config.get(k) == v for k, v in {'qualification': 'retained', 'repetitions': 5, 'epochs': 5, 'documents': 4096, 'calls_per_epoch': 1024, 'landed_tooling_commit': cfg['landed_tooling_commit'], 'review_url': cfg['review_url']}.items()), 'full D retained dimensions differ')
    toolchain = packet['toolchain']
    require(toolchain == read(out / 'toolchain.json'), 'original toolchain sidecar differs')
    require(toolchain['capture_directory'] == str(out) and toolchain['benchmark_tmpdir'] == str(out / 'benchmark-tmp') and
            toolchain['capture_filesystem_device'] == out.stat().st_dev == toolchain['benchmark_filesystem_device'] == (out / 'benchmark-tmp').stat().st_dev, 'original capture device/path observations differ')
    require(toolchain['go'] == 'go version go1.26.4 linux/amd64' and toolchain['go_env']['GOROOT'] == cfg['goroot'] and toolchain['go_env']['GOCACHE'] == cfg['gocache'] and toolchain['go_env']['GOMODCACHE'] == cfg['gomodcache'], 'original Go build configuration differs')
    require(packet['build_command'] == [str(receipts / 'go'), *compiler[1:]], 'driver did not use observed compiler wrapper')
    require(packet['build_log_sha256'] == sha(out / 'build.log'), 'original build log differs')
    require(packet['invocation'] == [str(binary), '-test.run=^$', '-test.bench=^BenchmarkR1Lifecycle5060$', '-test.benchtime=5x', '-test.count=1', '-test.benchmem', '-test.v'], 'original benchmark invocation differs')
    runs = packet['runs']
    require(runs == read(out / 'runs.json') and len(runs) == 5 and len({r['pid'] for r in runs}) == 5, 'missing five fresh original processes')
    previous_end = 0
    raw_paths = []
    for index, record in enumerate(runs, 1):
        require(record['repetition'] == index and record['log'] == f'run-{index:03d}.log' and type(record['exit_code']) is int and record['exit_code'] == 0, 'original raw run failed/misordered')
        raw = out / record['log']
        require(sha(raw) == record['log_sha256'], 'original raw run hash differs')
        require(record['before']['monotonic_ns'] >= previous_end and record['after']['monotonic_ns'] > record['before']['monotonic_ns'], 'original processes overlapped')
        require(utc(build) <= utc(record['before']) <= utc(record['after']) <= utc(run_exit), 'raw run outside original build/driver interval')
        previous_end = record['after']['monotonic_ns']
        raw_paths.append(raw)
    inputs = [CONFIG, MANIFEST, args.observer, validator, binary, out / 'packet.json', out / 'source-before.json', out / 'source-after.json', out / 'toolchain.json', out / 'runs.json', out / 'build.log', out / 'summary.md', *raw_paths,
              *[receipts / n for n in ('root-frozen-inputs.json', 'observer-before.json', 'build-start.json', 'build-completed.json', 'run-start.json', 'run-exit.json', 'trusted-completed-receipt.json', 'validation-start.json', 'validation-exit.json', 'driver.log', 'independent-validation.log', 'go')]]
    original_hashes = {str(p): {'sha256': sha(p), 'bytes': p.stat().st_size} for p in inputs}
    clean(repo)
    destination = receipts / 'independent-expected-source.json'
    require(not destination.exists(), 'replay expected-source destination must be NEW')
    RESULTS.mkdir(parents=True, exist_ok=False)
    with destination.open('xb') as output:
        output.write(MANIFEST.read_bytes())
    require(sha(destination) == MANIFEST_SHA, 'bytecopy of BEFORE-D independent manifest differs')
    require(all(sha(p) == metadata['sha256'] for p, metadata in original_hashes.items()), 'an original input changed during qualification')
    record = {'utc': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'state': STATE,
              'scope': 'Original source/build/five-process retained fixture and observed six-pin validator provenance only. Numeric finite-capacity and cost acceptance pending; no measurement or validation rerun.',
              'lock': {'path': LOCK, 'external_flock_required': True, 'marker_is_not_lock_proof': True},
              'source': source, 'config': config, 'packet_sha256': trusted['packet_sha256'],
              'binary_sha256': trusted['binary_sha256'], 'expected_source_sha256': MANIFEST_SHA,
              'validation_original_argv': validation, 'validation_original_exit': validation_exit['exit'],
              'validation_original_log_sha256': sha(receipts / 'independent-validation.log'),
              'input_pins': original_hashes, 'root_qualification_program_sha256': sha(__file__)}
    freeze(RESULTS / 'root-provenance-verification-original.json', record)
    print(json.dumps({'results': str(RESULTS), 'state': STATE, 'packet_sha256': trusted['packet_sha256'], 'binary_sha256': trusted['binary_sha256'], 'expected_source_sha256': MANIFEST_SHA}))

if __name__ == '__main__':
    try:
        main()
    except (ValueError, KeyError, OSError, subprocess.CalledProcessError) as error:
        print('D original-provenance qualification refused: ' + str(error), file=sys.stderr)
        sys.exit(1)
