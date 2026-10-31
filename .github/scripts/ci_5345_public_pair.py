#!/usr/bin/env python3
"""Private diagnostic: paired public graph costs, not a qualification gate."""
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time

BASE = 'a2d752e5e26f60bf1e6908c0d3be6515b8376755'
HEAD = '25a6f09e43ca18220f9c032f92dc3c686240e226'
TREES = {BASE: '219a6ca9613a20a648879bf5bd5de46ac3bc242f',
         HEAD: '128a4debda6d46385d20b5470fad52c5f4872e4c'}
BENCH = 'TreeDB/collections/native_ordinary_graph_public_5345_bench_test.go'
OBSERVER = 'scripts/mvcc_primary_cost_gate.py'
HELPER_SOURCES = ('TreeDB/internal/typedcolumn/fp32_image_bound_test.go',
                  'TreeDB/internal/typedcolumn/fp32_bound_cost_audit_test.go')
HELPER_NAME = 'BenchmarkFP32ImageEncodedUpperBound'
HELPER_TEST = '^' + HELPER_NAME + '(Policies)?$'
HELPER_CASES = {HELPER_NAME} | {f'{HELPER_NAME}Policies/{policy}/rows={rows}'
    for policy in ('none', 'lz4', 'snappy') for rows in (1, 256, 257, 4097)}
NAMES = ('BenchmarkOrdinaryGraphPublic5345', 'BenchmarkTypedGraphEncodedPublic5345')
TEST = '^BenchmarkTypedGraphEncodedPublic5345$/^(none|lz4)$/^rows4096$'
CASES = {f'BenchmarkTypedGraphEncodedPublic5345/{policy}/rows4096' for policy in ('none', 'lz4')}



def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def census(text, revision):
    """Require every leaf exactly once, as a measured row or explicit refusal."""
    measured, skipped, refusals, specific_refusals, storage = {}, {}, {}, {}, {}
    current = None
    for line in text.splitlines():
        announced = re.fullmatch(r'(Benchmark\S+)', line)
        if announced:
            current = announced[1]
        if '5345_ACTIVE_STORAGE_CENSUS: ' in line:
            if current not in CASES or current in storage:
                raise RuntimeError('unbound or duplicate active storage census')
            receipt = json.loads(line.split('5345_ACTIVE_STORAGE_CENSUS: ', 1)[1])
            accounting = receipt['accounting']
            parts = accounting.get('typed_column_parts', [])
            policy, expected_rows = current.rsplit('/', 2)[1:]
            details = [detail for part in parts for detail in part['image'].get('compression_detail', [])]
            if (receipt.get('basis') != 'active_persisted_descriptors_not_all_history_events'
                    or accounting.get('complete') is not True or not parts
                    or accounting.get('typed_column_part_refs') != len(parts)
                    or sum(part['image'].get('rows', 0) for part in parts) != int(expected_rows.removeprefix('rows'))
                    or any(not part['image'].get('compression_detail') or not part['image'].get('serialized_sections') for part in parts)
                    or not any(detail.get('requested_compression') == policy for detail in details)):
                raise RuntimeError('incomplete/mismatched active storage census for ' + current)
            storage[current] = receipt
        if 'EXPECTED_CAPABILITY_REFUSAL: Snappy FP32 image bound rejected before assignment without publication or debt change:' in line:
            if current not in CASES:
                raise RuntimeError('specific refusal lacks exact benchmark context')
            specific_refusals[current] = specific_refusals.get(current, 0) + 1
        if 'INCOMPLETE: public typed insert refused before command assignment:' in line:
            if current not in CASES:
                raise RuntimeError('refusal output lacks exact benchmark context')
            refusals[current] = refusals.get(current, 0) + 1
        match = re.match(r'^(Benchmark\S+)\s+(\d+)\s+(.+)$', line)
        if match:
            name = re.sub(r'-\d+$', '', match[1])
            if name not in CASES or name in measured or name in skipped or match[2] != '1':
                raise RuntimeError('unexpected/duplicate benchmark row: ' + line)
            fields = match[3].split()
            if len(fields) % 2:
                raise RuntimeError('malformed benchmark metrics: ' + line)
            metrics = {}
            for value, unit in zip(fields[::2], fields[1::2]):
                number = float(value)
                if unit in metrics or not math.isfinite(number):
                    raise RuntimeError('invalid/duplicate metric: ' + line)
                metrics[unit] = number
            count = int(name.rsplit('rows', 1)[1])
            required = {'ns/op', 'B/op', 'allocs/op', 'rows/op'}
            if not required <= metrics.keys() or metrics['rows/op'] != count or metrics['ns/op'] <= 0:
                raise RuntimeError('missing/invalid benchmark metrics: ' + line)
            if name.startswith(NAMES[0] + '/'):
                if metrics.get('accepted-insert-batches/op') != math.ceil(count / 128) or metrics.get('loaded-graphs/op') != 1:
                    raise RuntimeError('incomplete ordinary public operations: ' + line)
            elif metrics.get('accepted-inserts/op') != 1 or metrics.get('reserved-encoded-B/op', 0) <= 0:
                raise RuntimeError('missing positive encoded-publication delta: ' + line)
            if any(metrics[k] < 0 for k in ('B/op', 'allocs/op')):
                raise RuntimeError('negative allocation metric: ' + line)
            measured[name] = metrics
        skip = re.fullmatch(r'\s*--- SKIP: (\S+)(?: \([0-9]+(?:\.[0-9]+)?s\))?\s*', line)
        if skip:
            name = skip[1]
            if (revision != 'base' or name not in CASES or '/snappy/' not in name
                    or not name.startswith(NAMES[1] + '/') or name in measured or name in skipped):
                raise RuntimeError('unexpected or duplicate skip: ' + line)
            if specific_refusals.get(name) == 1:
                skipped[name] = {'status': 'EXPECTED_SNAPPY_BOUND_CAPABILITY_REFUSAL',
                                 'cause': 'same metadata-only bound error identity; no assignment, publication or debt change'}
            else:
                skipped[name] = {'status': 'INCOMPLETE_GENERIC_PREASSIGNMENT_REFUSAL',
                                 'cause': 'not established by this error class'}
    if 'PASS' not in text.splitlines() or re.search(r'(?m)^\s*(FAIL|--- FAIL:|panic:)', text):
        raise RuntimeError('benchmark process did not pass')
    # Specific refusals have identity and publication checks in the fixture;
    # a generic preassignment sentinel remains incomplete and unidentified.
    if (refusals != {name: 1 for name in skipped if name not in specific_refusals}
            or specific_refusals != {name: 1 for name in skipped if name in specific_refusals}
            or measured.keys() | skipped.keys() != CASES):
        raise RuntimeError('missing cases or unbound refusal output')
    if storage.keys() != measured.keys():
        raise RuntimeError('active storage census differs from successful public rows')
    return {'measured': measured, 'incomplete': skipped, 'active_storage': storage,
            'status': 'COMPLETE_ROWS' if not skipped else 'INCOMPLETE_CAPABILITY_ROWS'}


def helper_census(text, revision):
    """Keep the original helper control and all policy outcomes distinct."""
    measured = {}
    if ('PASS' not in text.splitlines()
            or re.search(r'(?m)^\s*(FAIL|--- FAIL:|--- SKIP:|panic:)', text)):
        raise RuntimeError('helper benchmark did not pass without skips')
    for line in text.splitlines():
        match = re.match(r'^(Benchmark\S+)\s+(\d+)\s+(.+)$', line)
        if not match:
            continue
        name = re.sub(r'-\d+$', '', match[1])
        if name not in HELPER_CASES or name in measured or int(match[2]) <= 0:
            raise RuntimeError('unexpected/duplicate helper benchmark row: ' + line)
        fields = match[3].split()
        if len(fields) % 2:
            raise RuntimeError('malformed helper benchmark metrics: ' + line)
        metrics = {}
        for value, unit in zip(fields[::2], fields[1::2]):
            number = float(value)
            if unit in metrics or not math.isfinite(number):
                raise RuntimeError('invalid/duplicate helper metric: ' + line)
            metrics[unit] = number
        if (not {'ns/op', 'B/op', 'allocs/op'} <= metrics.keys()
                or metrics['ns/op'] <= 0 or metrics['B/op'] < 0 or metrics['allocs/op'] < 0):
            raise RuntimeError('missing/invalid helper metrics: ' + line)
        status = 'ORIGINAL_SUPPORTED_CONTROL'
        if name != HELPER_NAME:
            refused = revision == 'base' and '/snappy/' in name
            if (metrics.get('rows/op') != int(name.rsplit('=', 1)[1])
                    or metrics.get('admit/op') != (0 if refused else 1)
                    or metrics.get('reject/op') != (1 if refused else 0)
                    or (metrics.get('bound-bytes/op') != 0 if refused
                        else metrics.get('bound-bytes/op', 0) <= 0)):
                raise RuntimeError('incorrect helper admission disposition: ' + line)
            status = 'EXPECTED_SNAPPY_REFUSAL' if refused else 'ADMITTED'
        measured[name] = {'iterations': int(match[2]), 'metrics': metrics, 'status': status}
    if measured.keys() != HELPER_CASES:
        raise RuntimeError('helper benchmark case census incomplete')
    return {'measured': measured, 'status': 'COMPLETE_DISPOSITIONS',
            'comparison_scope': 'original and none/LZ4 matched controls; baseline Snappy refusal is separate capability work'}


def main():
    if len(sys.argv) != 4:
        raise RuntimeError('usage: runner BASE_CHECKOUT HEAD_CHECKOUT FRESH_EXTERNAL_OUTPUT')
    base, head, out = (Path(x).resolve() for x in sys.argv[1:4])
    if base == head or any(out == root or root in out.parents for root in (base, head)):
        raise RuntimeError('output must be external to both distinct checkouts')
    out.mkdir(parents=True, exist_ok=False)
    write_json(out / 'disposition.json', {'status': 'INCOMPLETE', 'reason': 'campaign not complete'})
    (out / 'tmp').mkdir()
    env = dict(os.environ, GOWORK='off', GOTOOLCHAIN='local', GOMAXPROCS='1', GOFLAGS='', TMPDIR=str(out / 'tmp'))
    env_keys = ('GOWORK', 'GOTOOLCHAIN', 'GOMAXPROCS', 'GOMEMLIMIT', 'GOGC', 'GODEBUG', 'GOFLAGS', 'CGO_ENABLED', 'TMPDIR', 'GOEXPERIMENT', 'GOAMD64', 'CGO_CFLAGS', 'CGO_CPPFLAGS', 'CGO_CXXFLAGS', 'CGO_LDFLAGS', 'CC', 'CXX')
    write_json(out / 'environment.json', {k: env.get(k) for k in env_keys})
    receipts, rows, observers = [], [], {}

    def interrupted(signum, _frame):
        # Unwind command() so its detached process group cannot outlive us.
        # Ignore repeated cancellation while that bounded cleanup completes.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        raise InterruptedError('external signal ' + str(signum))

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)

    def finish_observer(name):
        observer = observers.pop(name, None)
        if observer is not None:
            result = observer.finish()
            write_json(out / (name + '.json'), result)
            return result

    def command(name, argv, cwd, limit, binary=False):
        if limit <= 0:
            raise subprocess.TimeoutExpired(argv, limit)
        started = time.monotonic()
        path = out / (name + '.log')
        with path.open('wb') as log, (out / (name + '.stderr')).open('wb') as errlog:
            p = subprocess.Popen([str(x) for x in argv], cwd=cwd, env=env,
                                 stdout=log, stderr=errlog if binary else subprocess.STDOUT, start_new_session=True)
            try:
                while True:
                    for observer in observers.values():
                        observer.observe()
                    remaining = limit - (time.monotonic() - started)
                    if remaining <= 0:
                        raise subprocess.TimeoutExpired(argv, limit)
                    try:
                        status = p.wait(timeout=min(0.25, remaining))
                        break
                    except subprocess.TimeoutExpired:
                        continue
            except BaseException:
                try:
                    os.killpg(p.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                cleanup_error = None
                try:
                    p.wait(timeout=10)
                except subprocess.TimeoutExpired as error:
                    cleanup_error = error
                receipts.append({'name': name, 'argv': list(map(str, argv)), 'cwd': str(cwd),
                                 'status': 'interrupted', 'output': path.name, 'sha256': sha(path),
                                 'cleanup_wait_complete': cleanup_error is None})
                write_json(out / 'commands.json', receipts)
                if cleanup_error is not None:
                    raise RuntimeError('owned command cleanup wait timed out') from cleanup_error
                raise
        receipts.append({'name': name, 'argv': list(map(str, argv)), 'cwd': str(cwd),
                         'status': status, 'elapsed_s': time.monotonic() - started,
                         'output': path.name, 'sha256': sha(path),
                         'stderr': name + '.stderr', 'stderr_sha256': sha(out / (name + '.stderr'))})
        write_json(out / 'commands.json', receipts)
        if status:
            raise RuntimeError(name + ' failed: exit ' + str(status))
        return '' if binary else path.read_text(errors='replace')

    def checked_source(root, expected, phase):
        label = phase + '-' + expected[:8]
        actual = command('source-' + label, ['git', 'rev-parse', 'HEAD'], root, 20).strip()
        tree = command('tree-' + label, ['git', 'rev-parse', 'HEAD^{tree}'], root, 20).strip()
        status = command('status-' + label, ['git', 'status', '--porcelain', '--untracked-files=all', '--ignored'], root, 20).strip()
        # Fresh hosted checkouts must contain no ignored/untracked inputs either.
        if actual != expected or tree != TREES[expected] or status:
            raise RuntimeError('source identity or complete cleanliness failed: ' + str(root))
        return {'sha': actual, 'tree': tree, 'go_mod_sha256': sha(root / 'go.mod'), 'go_sum_sha256': sha(root / 'go.sum')}

    try:
        source_before = {name: checked_source(root, expected, 'before') for name, root, expected in (('base', base, BASE), ('head', head, HEAD))}
        if any(sha(base / path) != sha(head / path) for path in (BENCH, OBSERVER, *HELPER_SOURCES)):
            raise RuntimeError('benchmark or observer sources differ')
        if 0 not in os.sched_getaffinity(0):
            raise RuntimeError('CPU 0 is unavailable; no benchmark started')
        for name, args in [('go-version', ['go', 'version']), ('go-env', ['go', 'env', '-json', 'GOOS', 'GOARCH', 'GOVERSION', 'GOROOT', 'GOTOOLDIR', 'GOWORK', 'GOTOOLCHAIN', 'CGO_ENABLED', 'GOFLAGS', 'GOEXPERIMENT', 'GOAMD64', 'GOMOD', 'GOCACHE', 'GOMODCACHE', 'GOSUMDB', 'CC', 'CXX', 'CGO_CFLAGS', 'CGO_CPPFLAGS', 'CGO_CXXFLAGS', 'CGO_LDFLAGS']),
                           ('compiler', ['go', 'tool', 'compile', '-V']), ('uname', ['uname', '-a']), ('lscpu', ['lscpu']),
                           ('filesystem', ['df', '-T', str(out)]), ('memory', ['cat', '/proc/meminfo'])]:
            command(name, args, head, 30)
        # Credentials in proxy/private-module configuration must not be printed.
        policy = subprocess.check_output(['go', 'env', '-json', 'GOPROXY', 'GOPRIVATE', 'GONOPROXY', 'GONOSUMDB', 'GOENV'], cwd=head, env=env, timeout=30)
        write_json(out / 'module-policy-hashes.json', {k: hashlib.sha256(str(v).encode()).hexdigest() for k, v in json.loads(policy).items()})
        go_env = json.loads((out / 'go-env.log').read_text())
        if (go_env['GOOS'], go_env['GOARCH'], go_env['GOVERSION'], go_env['GOWORK'], go_env['GOTOOLCHAIN']) != ('linux', 'amd64', 'go1.26.10', 'off', 'local'):
            raise RuntimeError('unexpected Go environment')
        if (out / 'compiler.log').read_text().strip() != 'compile version go1.26.10':
            raise RuntimeError('Go compiler version mismatch')
        go_executable = Path(shutil.which('go')).resolve(strict=True)
        if go_executable != (Path(go_env['GOROOT']) / 'bin' / 'go').resolve(strict=True):
            raise RuntimeError('Go driver and resolved GOROOT differ')
        binaries = {}
        for name, root in [('base', base), ('head', head)]:
            deadline = time.monotonic() + 900
            binary = out / (name + '.test')
            command('build-' + name, ['go', 'test', '-c', '-p=1', '-trimpath', '-buildvcs=false', '-o', binary, './TreeDB/collections'], root, deadline - time.monotonic())
            command('binary-buildinfo-' + name, ['go', 'version', '-m', binary], head, min(30, deadline - time.monotonic()))
            binaries[name] = {'sha256': sha(binary), 'size': binary.stat().st_size}
        order = [('base', 'head') if pair % 2 == 0 else ('head', 'base') for pair in range(5)]
        write_json(out / 'bindings.json', {'sources': source_before, 'binaries': binaries,
            'benchmark_sha256': sha(base / BENCH), 'observer_sha256': sha(base / OBSERVER),
            'go_sha256': sha(go_executable), 'pairs': 5, 'order': order,
            'benchtime': '1x', 'cpu': 0, 'gomaxprocs': 1,
            'limitations': 'trace-instrumented diagnostic; whole-trace pprof is not region-filtered; not replacement cost samples'})

        # Reuse unchanged PRIMARY observer and limits for global AND each sample.
        # Keep the immutable product checkout free of import-generated caches.
        sys.dont_write_bytecode = True
        sys.path.insert(0, str(head / 'scripts'))
        import mvcc_primary_cost_gate as primary
        def runtime_bindings(phase):
            result = {}
            for name in binaries:
                result[name] = primary.runtime_binding(out / (name + '.test'),
                    lambda label, argv: command(label, argv, head, 30), phase + '-' + name, env)
            write_json(out / ('runtime-' + phase + '.json'), result)
            return result
        before_runtime = runtime_bindings('before')
        primary.acquire_quiet_window(out)
        observers['measured-processes'] = primary.ProcessObservation(out / 'measured-processes.jsonl')
        profile_sets = {(name, policy): [] for name in ('base', 'head') for policy in ('none', 'lz4')}
        for pair, revisions in enumerate(order, 1):
            for name in revisions:
                label = f'pair-{pair}-{name}'
                profile_dir = out / ('profiles-' + label)
                profile_dir.mkdir()
                env['GOMAP_5345_INSERT_TRACE_DIR'] = str(profile_dir)
                sample_observer = label + '-processes'
                observers[sample_observer] = primary.ProcessObservation(out / (sample_observer + '.jsonl'))
                try:
                    result_text = command(label, ['taskset', '-c', '0', out / (name + '.test'),
                        '-test.run=^$', '-test.bench=' + TEST, '-test.benchtime=1x', '-test.count=1',
                        '-test.benchmem', '-test.timeout=8m', '-test.v'], head, 540)
                finally:
                    observed_sample = finish_observer(sample_observer)
                    env.pop('GOMAP_5345_INSERT_TRACE_DIR', None)
                if observed_sample['busy']:
                    raise RuntimeError(label + ' external CPU violated unchanged quiet policy')
                result = census(result_text, name)
                if result['incomplete']:
                    raise RuntimeError(label + ' profile sample skipped')
                actual = list(profile_dir.iterdir())
                if len(actual) != 2 or any(not p.is_file() or p.stat().st_size == 0 for p in actual):
                    raise RuntimeError(label + ' incomplete profile files')
                files = {}
                for policy in ('none', 'lz4'):
                    selected = [p for p in actual if p.name.startswith('typed-' + policy + '-') and p.suffix == '.trace']
                    if len(selected) != 1:
                        raise RuntimeError(label + ' missing or duplicate policy profile')
                    path = selected[0]
                    files[policy] = {'path': str(path.relative_to(out)), 'sha256': sha(path), 'bytes': path.stat().st_size}
                    profile_sets[name, policy].append(path)
                rows.append({'pair': pair, 'revision': name, 'log': label + '.log', 'census': result, 'profiles': files})
                write_json(out / 'samples.json', rows)
        observed = finish_observer('measured-processes')
        if observed['busy']:
            raise RuntimeError('campaign external CPU violated unchanged quiet policy')
        source_after = {name: checked_source(root, expected, 'after') for name, root, expected in (('base', base, BASE), ('head', head, HEAD))}
        if source_after != source_before:
            raise RuntimeError('source binding changed')
        for name in binaries:
            if sha(out / (name + '.test')) != binaries[name]['sha256']:
                raise RuntimeError('measured binary changed')
        if runtime_bindings('after') != before_runtime:
            raise RuntimeError('measured ELF runtime inputs changed')
        if sha(go_executable) != json.loads((out / 'bindings.json').read_text())['go_sha256']:
            raise RuntimeError('Go driver changed')
        after_go_env = command('go-env-after', ['go', 'env', '-json', 'GOOS', 'GOARCH', 'GOVERSION', 'GOROOT', 'GOTOOLDIR', 'GOWORK', 'GOTOOLCHAIN', 'CGO_ENABLED', 'GOFLAGS', 'GOEXPERIMENT', 'GOAMD64', 'GOMOD', 'GOCACHE', 'GOMODCACHE', 'GOSUMDB', 'CC', 'CXX', 'CGO_CFLAGS', 'CGO_CPPFLAGS', 'CGO_CXXFLAGS', 'CGO_LDFLAGS'], head, 30)
        if json.loads(after_go_env) != go_env:
            raise RuntimeError('Go environment changed')
        if len(rows) != 10 or any(len(v) != 5 for v in profile_sets.values()):
            raise RuntimeError('twenty-trace census incomplete')
        regions = []
        for (name, policy), paths in profile_sets.items():
            for index, path in enumerate(paths, 1):
                label = f'trace-{name}-{policy}-{index}'
                for kind in ('syscall', 'sync', 'sched'):
                    command(label + '-' + kind, ['go', 'tool', 'trace', '-pprof=' + kind, path], head, 120, binary=True)
                dump = command(label + '-parsed', ['go', 'tool', 'trace', '-d=parsed', path], head, 120)
                matches = re.findall(r'^M=-?\d+ P=-?\d+ G=(\d+) Region(Begin|End) Time=(\d+) Task=\d+ Type="5345-public-insert"$', dump, re.MULTILINE)
                if len(matches) != 2 or matches[0][1] != 'Begin' or matches[1][1] != 'End' or matches[0][0] != matches[1][0] or int(matches[1][2]) <= int(matches[0][2]):
                    raise RuntimeError(label + ' missing or invalid insertion region')
                regions.append({'trace': str(path.relative_to(out)), 'goroutine': int(matches[0][0]),
                                'start_ns': int(matches[0][2]), 'end_ns': int(matches[1][2]),
                                'elapsed_ns': int(matches[1][2]) - int(matches[0][2])})
        write_json(out / 'regions.json', regions)
        if any(sha(p) != row['profiles'][policy]['sha256'] for row in rows for policy in ('none', 'lz4') for p in [out / row['profiles'][policy]['path']]):
            raise RuntimeError('profile changed after collection')
        write_json(out / 'disposition.json', {'status': 'INCOMPLETE_PENDING_INDEPENDENT_AUDIT',
            'reason': 'trace data collected; region-specific attribution requires independent audit',
            'comparison_scope': 'instrumented diagnostic only; does not replace V6 matched costs'})
    except BaseException as error:
        summary_errors = []
        for name in list(observers):
            try:
                finish_observer(name)
            except BaseException as observer_error:
                summary_errors.append({'observer': name, 'error': str(observer_error)})
        write_json(out / 'disposition.json', {'status': 'INCOMPLETE', 'error_type': type(error).__name__,
            'reason': str(error), 'complete_samples': len(rows), 'observer_summary_errors': summary_errors})
        raise


if __name__ == '__main__':
    try:
        main()
    except BaseException as error:
        print('INCOMPLETE:', type(error).__name__, str(error), file=sys.stderr)
        raise
