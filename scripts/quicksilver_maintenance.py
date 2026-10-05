#!/usr/bin/env python3
"""Capture native compact runs and analyze pre-calibrated matched pairs.

Use the same reviewed manifest as unified_bench_quicksilver_capture.py. A plan
has a new root-relative output directory and cells {label, source, fixture,
fixture_receipt, mode, batch_size}. Fixture receipts attest a closed, verified
fixture and its content fingerprint; their contents remain reviewer authority.
This command copies every fixture independently, never hardlinks or reopens the
original. Failures and timed-out copies remain available for diagnosis.
"""
import argparse
import hashlib
import json
import math
import os
import pathlib
import re
import shutil
import statistics
import subprocess
import sys
import time
import traceback

import owned_process_rss
import unified_bench_quicksilver_capture as unified


def require(condition, message):
    if not condition:
        raise ValueError(message)


def save(path, value, exclusive=False):
    with pathlib.Path(path).open('x' if exclusive else 'w') as stream:
        json.dump(value, stream, indent=2, allow_nan=False)
        stream.write('\n')


def integer(value, name, minimum=0):
    require(type(value) is int and value >= minimum, name)
    return value


def fingerprint(directory, payload_only=False):
    directory = pathlib.Path(directory)
    require(directory.is_dir() and not directory.is_symlink(), 'fixture must be a directory')
    entries = []
    for path in sorted(directory.rglob('*')):
        require(not path.is_symlink(), 'fixture contains symlink: '+str(path))
        if path.is_dir():
            continue
        require(path.is_file(), 'fixture contains nonregular entry: '+str(path))
        if payload_only and path.relative_to(directory).as_posix() in (
                'index.db', 'maindb/index.db', 'dictdb/index.db', 'templatedb/index.db'):
            continue
        entries.append([path.relative_to(directory).as_posix(), path.stat().st_size, unified.sha256(path)])
    require(entries or payload_only, 'empty fixture')
    raw = json.dumps(entries, separators=(',', ':'), ensure_ascii=True).encode()
    return dict(sha256=hashlib.sha256(raw).hexdigest(), files=len(entries), bytes=sum(v[1] for v in entries))


def restore_snapshot(root, manifest, database, directory, env, metadata):
    # Ordinary open must retain physical-identity validation. Only the existing
    # explicit restore API may rebind the private copy, before measured work.
    source = manifest['sources'][manifest['snapshot_restore']]
    require(pathlib.Path(source['binary']).name == source['binary'], 'restore binary basename')
    binary = root/'bin'/source['binary']
    require(source['head'] and unified.sha256(binary) == source['binary_sha256'], 'restore binary binding')
    native = {}
    unified.validate_native(binary, source, manifest['libraries'], env, root, directory, native)
    for name in ('ldd.stdout.txt', 'ldd.stderr.txt'):
        (directory/name).rename(directory/('restore.'+name))
    command = [str(binary), str(database)]
    with (directory/'restore.stdout.json').open('w') as stdout, (directory/'restore.stderr.log').open('w') as stderr:
        result = subprocess.run(command, cwd=root, env=env, stdout=stdout, stderr=stderr, timeout=1800)
    metadata['snapshot_restore'] = dict(source=source, command=command, native=native, rc=result.returncode)
    require(result.returncode == 0, 'snapshot restore exit: '+str(result.returncode))
    restored = json.loads((directory/'restore.stdout.json').read_bytes())
    require(restored['rebound'] is True and restored['operation'] == 'RebindDurableRootSnapshotLayoutWithContextV1', 'wrong restore operation')
    require(restored['stores'] in (['maindb'], ['dictdb', 'maindb'], ['templatedb', 'maindb'],
            ['dictdb', 'templatedb', 'maindb'], ['backend']), 'restore store order')


def compact_command(binary, database, cell):
    require(cell['mode'] in ('full', 'exhaustive'), 'unsupported maintenance mode')
    integer(cell['batch_size'], 'batch_size', 1)
    return [str(binary), 'compact', str(database), '-rw', '-json', '-mode', cell['mode'],
            '-sync-each-phase', '-leaf-pack-max-passes', '64', '-rewrite-batch-size', str(cell['batch_size'])]


def validate_report(report, cell):
    require(report['mode'] == cell['mode'] and report['dry_run'] is False, 'wrong mode/dry-run')
    for flag in ('fully_compacted', 'policy_fully_compacted', 'byte_minimized'):
        require(type(report[flag]) is bool, 'missing completion flag: '+flag)
    require(report['fully_compacted'] == report['policy_fully_compacted'], 'legacy policy flag drift')
    require(not report['byte_minimized'] or (cell['mode'] == 'exhaustive' and report['fully_compacted']),
            'byte-minimized flag inconsistent with mode/policy')
    require(cell['mode'] != 'exhaustive' or report['byte_minimized'] == report['fully_compacted'],
            'exhaustive completion flag drift')
    for label in ('before', 'after'):
        domains = report[label]
        require(isinstance(domains, list) and len(domains) == 5, 'incomplete storage domains')
        require({v['name'] for v in domains} == {'index', 'wal', 'value_vlog', 'leaf_vlog', 'total'}, 'domain drift')
        for value in domains:
            for key in ('bytes', 'files', 'zero_byte_files'):
                integer(value[key], label+'.'+key)
            require(value['zero_byte_files'] <= value['files'], 'zero file accounting')
        total = next(v for v in domains if v['name'] == 'total')
        for key in ('bytes', 'files', 'zero_byte_files'):
            require(total[key] == sum(v[key] for v in domains if v['name'] != 'total'), 'component accounting drift')
    phases = report['phases']
    require(isinstance(phases, list) and phases, 'missing phase records')
    for phase in phases:
        require(isinstance(phase['name'], str) and bool(phase['name']), 'missing phase name')
        integer(phase['wall_time_nanos'], 'phase duration')
        # Legacy successful phases have no status. Preserve deferrals/unsupported
        # work instead of silently treating every phase as completed.
        require(phase.get('status', '') in ('', 'not_required', 'succeeded', 'deferred', 'unsupported'), 'nonterminal/failed phase')
    require(isinstance(report['remaining_debt'], dict), 'missing remaining debt')
    debt = report['remaining_debt']
    debts = ('value_log_rewrite_segments', 'value_log_rewrite_bytes', 'value_log_gc_segments', 'value_log_gc_bytes',
             'leaf_pack_generations', 'leaf_pack_bytes', 'leaf_gc_generations', 'leaf_gc_bytes', 'zero_byte_value_log_files')
    for key in debts:
        integer(debt[key], 'remaining debt '+key)
    require(type(debt['index_vacuum_required']) is bool, 'index debt flag')
    require(report['fully_compacted'] == (not any(debt[k] for k in debts) and not debt['index_vacuum_required']), 'debt/completion drift')
    return report


class Deadline:
    def __init__(self, seconds):
        self.end = time.monotonic()+seconds

    def is_set(self):
        return time.monotonic() >= self.end


def rss_metrics(directory, summary, started_ns, finished_ns):
    require(summary['complete'] and not summary['cancelled'] and not summary['errors'], 'incomplete owned RSS')
    require(summary['interval_ms'] == 200, 'RSS interval drift')
    rows = [json.loads(v) for v in (directory/'rss_samples.jsonl').read_text().splitlines()]
    require(len(rows) == integer(summary['samples'], 'RSS sample count', 1), 'RSS count drift')
    identity = (rows[0]['pid'], rows[0]['process_start_ticks'])
    previous = started_ns
    for row in rows:
        require((row['pid'], row['process_start_ticks']) == identity, 'RSS identity drift')
        require(previous <= row['started_unix_nano'] <= row['finished_unix_nano'] <= finished_ns, 'RSS time drift')
        previous = row['finished_unix_nano']
        for name in ('rss_bytes', 'anonymous_bytes', 'file_bytes', 'shared_memory_bytes'):
            integer(row[name], 'RSS '+name)
    peaks = re.findall(r'^\s*Maximum resident set size \(kbytes\): (\d+)\s*$', (directory/'stderr.log').read_text(), re.M)
    require(len(peaks) == 1 and int(peaks[0]) > 0, 'missing time-v HWM')
    peak = max(rows, key=lambda v: v['rss_bytes'])
    return dict(peak_rss_bytes=int(peaks[0])*1024, sampled_peak=peak, sample_count=len(rows),
                scope='whole compact command; sampled components are co-timed; time-v HWM is separate')


def capture(manifest_path, plan_path):
    require(__debug__, 'native validator requires assertions; do not use Python -O')
    manifest_path, plan_path = pathlib.Path(manifest_path), pathlib.Path(plan_path)
    require(manifest_path.is_absolute() and plan_path.is_absolute(), 'absolute manifest/plan required')
    root = manifest_path.resolve(strict=True).parent
    manifest_raw, plan_raw = manifest_path.read_bytes(), plan_path.read_bytes()
    manifest, plan = json.loads(manifest_raw), json.loads(plan_raw)
    output = pathlib.Path(plan['output'])
    require(not output.is_absolute() and '..' not in output.parts and output.parts, 'new root-relative output required')
    out = (root/output).resolve()
    require(out != root and out.is_relative_to(root), 'output escaped manifest root')
    labels = [v['label'] for v in plan['cells']]
    require(labels and len(set(labels)) == len(labels), 'missing/duplicate labels')
    require(all(pathlib.Path(v).name == v and v not in ('.', '..') for v in labels), 'invalid label')
    fixtures = []
    # Validate every input/output relationship before creating any output. A
    # rejected alias or ancestor must not change the frozen physical fixture.
    for cell in plan['cells']:
        fixture = pathlib.Path(cell['fixture'])
        require(fixture.is_absolute() and not fixture.is_symlink(), 'absolute nonsymlink fixture required')
        fixture = fixture.resolve(strict=True)
        require(fixture.is_dir() and not out.is_relative_to(fixture) and not fixture.is_relative_to(out), 'fixture/output overlap')
        fixtures.append(fixture)
    out.mkdir(exist_ok=False)
    (out/'manifest.json').write_bytes(manifest_raw)
    (out/'plan.json').write_bytes(plan_raw)
    for role in ('source', 'build', 'native', 'runner'):
        receipt = manifest['receipts'][role]
        raw = (root/receipt['path']).read_bytes()
        require(hashlib.sha256(raw).hexdigest() == receipt['sha256'], 'receipt hash: '+role)
        (out/(role+'-receipt.json')).write_bytes(raw)
    env = {k: v for k, v in os.environ.items() if not k.startswith('TREEDB_')}
    env.update(manifest['build_env'])
    require(env['GOWORK'] == 'off', 'workspace drift')
    env.update(GOMAXPROCS='12', GOGC='100', GODEBUG='', GOMEMLIMIT='2GiB',
               TREEDB_VLOG_MAX_MAPPED_SEALED_BYTES='1073741824', TMPDIR=str(root/'working-dbs'))
    (root/'working-dbs').mkdir(exist_ok=True)
    for cell, fixture in zip(plan['cells'], fixtures):
        directory = out/cell['label']
        directory.mkdir()
        source = manifest['sources'][cell['source']]
        require(pathlib.Path(source['binary']).name == source['binary'], 'binary must be root/bin basename')
        binary, database = root/'bin'/source['binary'], directory/'db'
        metadata = dict(schema=1, cell=cell, source=source, command=compact_command(binary, database, cell),
                        env={k: v for k, v in env.items() if k.startswith(('TREEDB_', 'LD_')) or k in
                             ('GOROOT', 'GOWORK', 'GOGC', 'GODEBUG', 'GOMAXPROCS', 'GOMEMLIMIT', 'TMPDIR', 'GLIBC_TUNABLES',
                              'PATH', 'CGO_ENABLED', 'GOFLAGS', 'GOENV', 'GOTOOLCHAIN', 'CC', 'CXX', 'CGO_CFLAGS', 'CGO_LDFLAGS')},
                        manifest_sha256=hashlib.sha256(manifest_raw).hexdigest(), plan_sha256=hashlib.sha256(plan_raw).hexdigest(),
                        harness={name: unified.sha256(pathlib.Path(module.__file__)) for name, module in
                                 [('collector', sys.modules[__name__]), ('unified_collector', unified), ('rss_observer', owned_process_rss)]},
                        unavailable=['peak temporary disk', 'per-phase CRC/decode bytes'])
        try:
            receipt = cell['fixture_receipt']
            raw = pathlib.Path(receipt['path']).read_bytes()
            require(hashlib.sha256(raw).hexdigest() == receipt['sha256'], 'fixture receipt hash')
            attestation = json.loads(raw)
            require(attestation['closed'] is True and attestation['verified'] is True, 'unverified/open fixture attestation')
            metadata['fixture'] = fingerprint(fixture)
            require(metadata['fixture']['sha256'] == attestation['fixture_sha256'], 'fixture fingerprint drift')
            (directory/'fixture-receipt.json').write_bytes(raw)
            shutil.copytree(fixture, database)
            require(fingerprint(database) == metadata['fixture'] == fingerprint(fixture), 'copy or concurrent fixture drift')
            for path in database.rglob('*'):
                if path.is_file():
                    original = fixture/path.relative_to(database)
                    require((path.stat().st_dev, path.stat().st_ino) != (original.stat().st_dev, original.stat().st_ino), 'hardlinked fixture')
            metadata['payload'] = fingerprint(database, payload_only=True)
            restore_snapshot(root, manifest, database, directory, env, metadata)
            metadata['restored_fixture'] = fingerprint(database)
            require(metadata['restored_fixture']['files'] == metadata['fixture']['files'] and
                    metadata['restored_fixture']['bytes'] == metadata['fixture']['bytes'], 'restore extent drift')
            require(fingerprint(database, payload_only=True) == metadata['payload'], 'restore changed payload')
            require(fingerprint(fixture) == metadata['fixture'], 'restore changed original fixture')
            require(source['head'] and unified.sha256(binary) == source['binary_sha256'], 'binary/source binding')
            unified.validate_native(binary, source, manifest['libraries'], env, root, directory, metadata)
            metadata.update(started_ns=time.time_ns(), load_before=os.getloadavg())
            started = time.monotonic_ns()
            with (directory/'stdout.json').open('w') as stdout, (directory/'stderr.log').open('w') as stderr:
                result, sampling = owned_process_rss.run_with_rss(['/usr/bin/time', '-v', *metadata['command']], binary,
                     directory/'rss_samples.jsonl', interval_ms=200, cancel_event=Deadline(integer(cell.get('timeout_seconds', 1800), 'timeout', 1)),
                     cwd=root, env=env, stdout=stdout, stderr=stderr)
            metadata.update(finished_ns=time.time_ns(), elapsed_seconds=(time.monotonic_ns()-started)/1e9,
                            rc=result.returncode, rss_sampling=sampling, load_after=os.getloadavg())
            require(result.returncode == 0, 'compact exit: '+str(result.returncode))
            validate_report(json.loads((directory/'stdout.json').read_bytes()), cell)
            metadata['memory'] = rss_metrics(directory, sampling, metadata['started_ns'], metadata['finished_ns'])
            metadata['validated'] = True
        except BaseException:
            metadata['error'] = traceback.format_exc()
            raise
        finally:
            metadata['artifacts'] = {p.name: unified.sha256(p) for p in directory.iterdir() if p.is_file() and p.name != 'run.json'}
            save(directory/'run.json', metadata)
        print(json.dumps(dict(label=cell['label'], elapsed_seconds=metadata['elapsed_seconds'], memory=metadata['memory'])), flush=True)


def load_run(path):
    path = pathlib.Path(path).resolve(strict=True)
    run = json.loads(path.read_bytes())
    require(run['schema'] == 1 and run.get('validated') is True and run['rc'] == 0 and 'error' not in run, 'invalid/failed run')
    for name, expected in run['artifacts'].items():
        require(pathlib.Path(name).name == name and unified.sha256(path.parent/name) == expected, 'raw artifact drift: '+name)
    required = {'stdout.json', 'stderr.log', 'rss_samples.jsonl', 'rss_samples.jsonl.summary.json', 'fixture-receipt.json',
                'ldd.stdout.txt', 'ldd.stderr.txt', 'restore.stdout.json', 'restore.stderr.log',
                'restore.ldd.stdout.txt', 'restore.ldd.stderr.txt'}
    require(required <= set(run['artifacts']), 'missing raw artifacts')
    parent = path.parent.parent
    require(unified.sha256(parent/'manifest.json') == run['manifest_sha256'] and unified.sha256(parent/'plan.json') == run['plan_sha256'], 'manifest/plan drift')
    manifest, plan = json.loads((parent/'manifest.json').read_bytes()), json.loads((parent/'plan.json').read_bytes())
    require(run['cell'] in plan['cells'] and run['source'] == manifest['sources'][run['cell']['source']], 'source/cell binding drift')
    require(set(manifest['receipts']) == {'source', 'build', 'native', 'runner'}, 'missing campaign receipts')
    for role, receipt in manifest['receipts'].items():
        require(unified.sha256(parent/(role+'-receipt.json')) == receipt['sha256'], 'source receipt drift: '+role)
    # Derive common identities from the validated manifest, not run metadata.
    # Source/build receipts inventory every frozen A/B variant in the campaign.
    run['comparison_receipts'] = {role: receipt['sha256'] for role, receipt in manifest['receipts'].items()}
    require(run['command'] == compact_command(run['command'][0], path.parent/'db', run['cell']), 'CLI flag drift')
    require(pathlib.Path(run['command'][0]).name == run['source']['binary'], 'compact binary basename drift')
    restore = run['snapshot_restore']
    require(restore['source'] == manifest['sources'][manifest['snapshot_restore']] and restore['rc'] == 0 and
            restore['command'] == [restore['command'][0], str(path.parent/'db')], 'restore source/command drift')
    require(pathlib.Path(restore['command'][0]).name == restore['source']['binary'], 'restore binary basename drift')
    restored = json.loads((path.parent/'restore.stdout.json').read_bytes())
    require(restored['rebound'] is True and restored['operation'] == 'RebindDurableRootSnapshotLayoutWithContextV1', 'restore receipt drift')
    require(run['restored_fixture']['files'] == run['fixture']['files'] and
            run['restored_fixture']['bytes'] == run['fixture']['bytes'], 'restore extent receipt drift')
    attest = json.loads((path.parent/'fixture-receipt.json').read_bytes())
    require(attest['closed'] is True and attest['verified'] is True and attest['fixture_sha256'] == run['fixture']['sha256'], 'fixture binding drift')
    require(run['artifacts']['fixture-receipt.json'] == run['cell']['fixture_receipt']['sha256'], 'fixture receipt drift')
    report = validate_report(json.loads((path.parent/'stdout.json').read_bytes()), run['cell'])
    require(json.loads((path.parent/'rss_samples.jsonl.summary.json').read_bytes()) == run['rss_sampling'], 'RSS summary drift')
    require(rss_metrics(path.parent, run['rss_sampling'], run['started_ns'], run['finished_ns']) == run['memory'], 'RSS metric drift')
    require(run['started_ns'] < run['finished_ns'] <= time.time_ns(), 'run time ordering')
    require(type(run['elapsed_seconds']) in (int, float) and math.isfinite(run['elapsed_seconds']) and run['elapsed_seconds'] > 0, 'elapsed time invalid')
    return run, report


def contract(run):
    # Product identity may vary A/B. All other workload/observer/runner controls
    # must match, including explicit timeout. Fixture contents identify workload.
    return dict(fixture=run['fixture'], mode=run['cell']['mode'], batch_size=run['cell']['batch_size'],
                timeout_seconds=run['cell'].get('timeout_seconds', 1800), harness=run['harness'], env=run['env'],
                loader=run['native_resolution']['libraries'], interval=run['rss_sampling']['interval_ms'],
                restore=run['snapshot_restore']['source'], restore_loader=run['snapshot_restore']['native'],
                campaign_receipts=run['comparison_receipts'])


def policy_complete(report):
    return report['policy_fully_compacted'] and all(phase.get('status', '') not in ('deferred', 'unsupported')
                                                   for phase in report['phases'])


def metric(run, name):
    require(name in ('elapsed_seconds', 'peak_rss_bytes'), 'unsupported metric')
    return run['elapsed_seconds'] if name == 'elapsed_seconds' else run['memory']['peak_rss_bytes']


def calibrate(paths, name, output):
    require(len(paths) == 3, 'exactly three baseline characterizations required')
    loaded = [load_run(p) for p in paths]
    require(all(policy_complete(report) for _, report in loaded), 'incomplete calibration run')
    runs = [run for run, _ in loaded]
    require(all(contract(v) == contract(runs[0]) and v['source'] == runs[0]['source'] for v in runs), 'calibration contract/source drift')
    require(all(a['finished_ns'] <= b['started_ns'] for a, b in zip(runs, runs[1:])), 'overlapping/reordered calibration')
    values = [metric(v, name) for v in runs]
    packet = dict(schema=1, metric=name, contract=contract(runs[0]), baseline_source=runs[0]['source'],
                  paths=[str(pathlib.Path(p).resolve()) for p in paths], hashes=[unified.sha256(pathlib.Path(p)) for p in paths],
                  values=values, E=(max(values)-min(values))/statistics.median(values), created_ns=time.time_ns())
    require(packet['created_ns'] > runs[-1]['finished_ns'], 'calibration clock order')
    save(output, packet, exclusive=True)
    return packet


def analyze(bundle_path):
    bundle = json.loads(pathlib.Path(bundle_path).read_bytes())
    noise_path = pathlib.Path(bundle['calibration'])
    require(unified.sha256(noise_path) == bundle['calibration_sha256'], 'calibration hash drift')
    noise = json.loads(noise_path.read_bytes())
    require(len(noise['paths']) == 3 and len(noise['hashes']) == 3, 'missing baseline characterizations')
    for path, expected in zip(noise['paths'], noise['hashes']):
        require(unified.sha256(pathlib.Path(path)) == expected, 'calibration run drift')
    calibration = [load_run(v) for v in noise['paths']]
    require(all(policy_complete(report) for _, report in calibration), 'incomplete calibration run')
    bases = [run for run, _ in calibration]
    require(all(contract(v) == noise['contract'] and v['source'] == noise['baseline_source'] for v in bases), 'calibration identity drift')
    values = [metric(v, noise['metric']) for v in bases]
    require(values == noise['values'] and noise['E'] == (max(values)-min(values))/statistics.median(values), 'noise calibration drift')
    require(all(a['finished_ns'] <= b['started_ns'] for a, b in zip(bases, bases[1:])) and bases[-1]['finished_ns'] < noise['created_ns'], 'calibration order drift')
    pairs = bundle['pairs']
    require(len(pairs) == 3, 'exactly three matched pairs required')
    for pair in pairs:
        for role in ('A', 'B'):
            expected = pair.get(role+'_sha256')
            require(isinstance(expected, str) and re.fullmatch('[0-9a-f]{64}', expected) and
                    unified.sha256(pathlib.Path(pair[role])) == expected, 'matched run digest drift: '+role)
    loaded = [(load_run(v['A']), load_run(v['B'])) for v in pairs]
    runs = [r for pair in loaded for r, _ in pair]
    all_paths = [*noise['paths'], *[str(pathlib.Path(p[k]).resolve()) for p in pairs for k in ('A', 'B')]]
    require(len(set(all_paths)) == 9, 'reused calibration/pair run')
    require(all(contract(v) == noise['contract'] for v in runs), 'matched workload/environment/harness drift')
    require(all(pair[0][0]['source'] == noise['baseline_source'] for pair in loaded), 'baseline source drift')
    require(all(pair[1][0]['source'] == loaded[0][1][0]['source'] for pair in loaded), 'candidate source drift')
    require(loaded[0][1][0]['source'] != noise['baseline_source'], 'candidate equals baseline source')
    ordered = [loaded[0][0][0], loaded[0][1][0], loaded[1][1][0], loaded[1][0][0], loaded[2][0][0], loaded[2][1][0]]
    require(noise['created_ns'] < ordered[0]['started_ns'] and all(a['finished_ns'] <= b['started_ns'] for a, b in zip(ordered, ordered[1:])), 'expected A1/B1, B2/A2, A3/B3 after calibration')
    effects = [(metric(a[0], noise['metric'])-metric(b[0], noise['metric']))/metric(a[0], noise['metric']) for a, b in loaded]
    completion = [[{k: report[k] for k in ('fully_compacted', 'policy_fully_compacted', 'byte_minimized')} for _, report in pair] for pair in loaded]
    equal_completion = all(a == b for a, b in completion)
    complete_phase_dispositions = all(phase.get('status', '') not in ('deferred', 'unsupported')
                                     for pair in loaded for _, report in pair for phase in report['phases'])
    complete_runs = all(policy_complete(report) for pair in loaded for _, report in pair)
    material = all(v > 0 for v in effects) and statistics.median(effects) > 2*noise['E'] and equal_completion and complete_runs
    return dict(metric=noise['metric'], E=noise['E'], threshold=2*noise['E'], pair_effects=effects,
                median_effect=statistics.median(effects), all_favourable=all(v > 0 for v in effects),
                equal_completion=equal_completion, complete_runs=complete_runs,
                complete_phase_dispositions=complete_phase_dispositions, completion=completion, material=material,
                conclusion='material improvement' if material else 'inconclusive or negative; parent gate remains unmet',
                unavailable=['peak temporary disk', 'per-phase CRC/decode bytes'],
                scope='one maintenance metric only; correctness, service-read and online-progress gates are separate')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    p = commands.add_parser('capture'); p.add_argument('manifest'); p.add_argument('plan')
    p = commands.add_parser('calibrate'); p.add_argument('--metric', choices=['elapsed_seconds', 'peak_rss_bytes'], required=True); p.add_argument('--out', required=True); p.add_argument('runs', nargs=3)
    p = commands.add_parser('analyze'); p.add_argument('bundle'); p.add_argument('--out', required=True)
    args = parser.parse_args()
    if args.command == 'capture':
        capture(args.manifest, args.plan)
    elif args.command == 'calibrate':
        print(json.dumps(calibrate(args.runs, args.metric, args.out), indent=2))
    else:
        result = analyze(args.bundle)
        save(args.out, result, exclusive=True)
        print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
