#!/usr/bin/env python3
"""Capture fresh-process Quicksilver cells; Python 3 stdlib, Linux /usr/bin/time -v.

Usage: python3 scripts/unified_bench_quicksilver_capture.py /abs/manifest.json /abs/plan.json
Root is manifest.parent. Root-owned manifest: build_env (GOWORK=off), sources
{name: {head, binary, binary_sha256}}, libraries {absolute_path: {sha256}}, and
receipts {source|build|native|runner: {path, sha256}}. Relative receipt paths are
root-relative; source binaries are under root/bin. Receipts attest landed build
inputs, build command/toolchain/tags, native identities and runner respectively;
the coordinator reviews their contents, this wrapper verifies their hashes.
Libraries must include every same-environment ldd resolution, including libc and
the dynamic loader. Only LD_LIBRARY_PATH may be nonempty among LD_* controls.
Source linkage defaults to dynamic; linkage=static explicitly requires a static
ldd result. Benchmark binaries must be Linux ELF executables, not scripts.
Raw ldd output and actual loader environment are retained per cell.
Plan: output (new root-relative directory), repeats (default 1), cells [{label,
source, engine, profile, keys, reads, updates, workers, case, duration, duration_ns,
commit, seed, mixture, working_set, miss_percent, profiled, flags}]. Defaults match
the prior 3M capture flow. Custom duration requires duration_ns and Go's canonical
duration string. Extra flags must be -<engine>-<name>=<value>. Profiles are durable,
fast, wal_on_fast; auto commit resolves ordinary for realistic, sync for history.
Raw stdout/stderr (including time-v), metadata, manifest and plan survive failures.
Realistic mixed/concurrent miss ratios use Bernoulli configured p: endpoints are
exact, otherwise a two-sided Hoeffding screening tolerance at alpha=1e-12.
Actual counts and tolerance are retained in run.json even on ratio rejection.
Concurrent readers stop before drawing at 256-request boundaries; time-dependent
prefixes can include 255 tail requests per worker. This screening tolerance is
not an optional-stopping confidence certificate or an exact seeded-trace proof.
This captures evidence; it does not certify receipt contents or power-loss safety.
"""
import hashlib
import json
import math
import os
import pathlib
import re
import subprocess
import sys
import time
import traceback

PHASES = ['quicksilver_hits', 'quicksilver_misses', 'quicksilver_mixed', 'quicksilver_concurrent']
CONTRACTS = {'durable': ('command_wal_durable', 'wal_on_sync', 'durable_wal_prefix'),
             'fast': ('no_wal_fast', 'wal_off_relaxed_sync', 'relaxed'),
             'wal_on_fast': ('command_wal_relaxed', 'wal_on_relaxed_sync', 'relaxed')}


def sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1 << 20), b''):
            digest.update(block)
    return digest.hexdigest()


def validate_native(binary, source, libraries, env, root, directory, metadata):
    loader_env = {k: v for k, v in env.items() if k.startswith('LD_') or k == 'GLIBC_TUNABLES'}
    for name in ('LD_LIBRARY_PATH', 'LD_PRELOAD', 'LD_AUDIT'):
        loader_env.setdefault(name, '')
    native = dict(command=['/usr/bin/ldd', str(binary)], loader_env=loader_env, libraries={}, virtual=[])
    metadata['native_resolution'] = native
    stdout_path, stderr_path = directory/'ldd.stdout.txt', directory/'ldd.stderr.txt'
    stdout_path.touch()
    stderr_path.touch()
    assert not any(v for k, v in loader_env.items() if k.startswith('LD_') and k != 'LD_LIBRARY_PATH'), 'unsupported loader injection/control'
    with binary.open('rb') as executable:
        assert executable.read(4) == b'\x7fELF', 'benchmark executable must be ELF'
    with stdout_path.open('w') as stdout, stderr_path.open('w') as stderr:
        result = subprocess.run(native['command'], cwd=root, env=env, stdout=stdout, stderr=stderr, timeout=30)
    native['rc'] = result.returncode
    output, errors = stdout_path.read_text().strip(), stderr_path.read_text().strip()
    linkage = source.get('linkage', 'dynamic')
    native['linkage'] = linkage
    if linkage == 'static':
        assert result.returncode in (0, 1) and (output, errors) in (
            ('statically linked', ''), ('', 'statically linked'), ('', 'not a dynamic executable'), ('not a dynamic executable', '')), 'unrecognized static resolution'
        return
    assert linkage == 'dynamic' and result.returncode == 0 and not errors, 'dynamic resolution failed'
    for line in output.splitlines():
        fields = line.split()
        if fields and fields[0] in ('linux-vdso.so.1', 'linux-gate.so.1'):
            assert len(fields) == 2 and re.fullmatch(r'\(0x[0-9a-fA-F]+\)', fields[1]), line
            native['virtual'].append(fields[0])
            continue
        if '=>' in fields:
            assert len(fields) == 4 and fields[1] == '=>', line
            path, address = fields[2:]
        else:
            assert len(fields) == 2, line
            path, address = fields
        assert pathlib.Path(path).is_absolute() and re.fullmatch(r'\(0x[0-9a-fA-F]+\)', address), line
        native['libraries'][path] = sha256(pathlib.Path(path))
        assert native['libraries'][path] == libraries.get(path, {}).get('sha256'), 'unattested resolved dependency: '+path
    assert native['libraries'], 'dynamic resolution contained no absolute dependencies'


def validate(reports, cell, directory, fixtures, metadata):
    assert len(reports) == 1
    r, c = reports[0], reports[0]['config']
    keys, reads = cell.get('keys', 3000000), cell.get('reads', 6000000)
    updates, workers = cell.get('updates', 40000), cell.get('workers', 4)
    case, profile = cell.get('case', 'realistic'), cell.get('profile', 'durable')
    commit = cell.get('commit', 'auto')
    if commit == 'auto':
        commit = 'ordinary' if case == 'realistic' else 'sync'
    assert r['engine'] == cell['engine'] and r['gomaxprocs'] == 12
    assert r['profiled'] == cell.get('profiled', False)
    assert (c['case'], c['keys'], c['aggregate_reads'], c['workers'], c['updates']) == (case, keys, reads, workers, updates)
    assert c['commit_mode'] == commit and c['reads_per_snapshot'] == 64
    assert c['concurrent_duration_ns'] == cell.get('duration_ns', 8000000000)
    assert c['barrier_policy'] == 'initial/final Checkpoint; up to four separately timed concurrent Checkpoints at mutation-batch quarters'
    assert r['updated_keys'] == updates and len(r['update_batch_ms']) == math.ceil(updates/1000)
    assert len(r['checkpoint_ms']) == min(4, math.ceil(updates/1000))
    flags = r['registered_cli_flags']
    assert flags['profile'] == profile and flags['keep'] == 'false'
    assert flags['quicksilver-duration'] == cell.get('duration', '8s')
    if case == 'realistic':
        seed, mixture = cell.get('seed', 24), cell.get('mixture', 'primary')
        assert seed != 0 and c['generation'] == 'generic-v1' and c['seed'] == seed
        assert c['mixture'] == mixture and c['working_set'] == cell.get('working_set', 'uniform')
        assert c['miss_percent'] == cell.get('miss_percent', 90)
        assert r['mutations'] == dict(updates=(updates+3)//4, deletes=(updates+2)//4,
                                     inserts=(updates+1)//4, overwrite_targets=updates//4, overwrite_sets=4*(updates//4))
        assert r['mutation_commit_batches'] == sum(1+3*(min(1000, updates-off) >= 4) for off in range(0, updates, 1000))
        deleted = max(1, keys//100)
        assert (r['initial_verified_keys'], r['initial_verified_misses']) == (keys, 2*keys+deleted)
        assert (r['verified_keys'], r['verified_misses']) == (keys-(updates+2)//4+(updates+1)//4, 2*keys+deleted+(updates+2)//4)
        assert r['trace_bytes'] == 0 and r['oracle_state_bytes'] == keys
        assert r['distinct_tracking_bytes'] == ((5*keys+63)//64)*8*(workers+1)
        d, z = r['loaded_distribution'], r['compressibility']
        assert sum(d['small_medium_large_values']) == sum(d['namespace_hostname_opaque_keys']) == keys
        assert d['structured_values'] + d['opaque_values'] == keys
        assert 0 < d['min_key_bytes'] <= d['max_key_bytes'] <= 128
        assert 32 <= d['min_value_bytes'] <= d['max_value_bytes'] <= 32768
        assert keys*d['min_key_bytes'] <= d['key_bytes'] <= keys*d['max_key_bytes']
        assert keys*d['min_value_bytes'] <= d['value_bytes'] <= keys*d['max_value_bytes']
        assert z['codec'] == 'stdlib DEFLATE BestSpeed'
        assert z['basis'] == 'up to 4096 distinct loaded records, seeded coprime-stride sample; each record compressed independently including identity/generation header; setup only, not an engine codec claim'
        assert z['total']['records'] == sum(z['sample_small_medium_large_records']) == min(4096, keys)
        for field in ('records', 'raw_bytes', 'compressed_bytes'):
            assert z['total'][field] == z['structured'][field] + z['opaque'][field]
        for group in ('total', 'structured', 'opaque'):
            g = z[group]
            assert g['raw_bytes'] >= 0 and g['compressed_bytes'] >= 0
            ratio = g['compressed_bytes']/g['raw_bytes'] if g['raw_bytes'] else 0
            assert math.isclose(g['compressed_to_raw_ratio'], ratio, rel_tol=1e-9, abs_tol=1e-9)
        fixture = (keys, seed, mixture)
        assert fixtures.setdefault(fixture, (d, z)) == (d, z), 'fixture changed across cells'
    else:
        assert c['generation'] == 'legacy-v1' and c['seed'] == 24 and c['working_set'] == 'legacy-65536-trace'
        assert r['verified_keys'] == r['verified_misses'] == keys
    stats = [r.get('initial_stats', {}), r.get('final_stats', {})]
    for p in r['phases']:
        stats += [p.get('stats_before', {}), p.get('stats_after', {})]
    if cell['engine'] == 'treedb':
        assert flags['treedb-disable-read-checksum'] == 'false'
        for s in stats:
            assert tuple(s[k] for k in ('treedb.profile.resolved', 'treedb.durability_mode', 'treedb.profile.ordinary_ack_class')) == CONTRACTS[profile]
            assert s['treedb.profile.production'] == 'true' and s['treedb.profile.bench_unsafe'] == 'false'
            assert s['treedb.vlog.read_integrity'] == 'verify' and s['treedb.negative_lookup_filter.active_bytes'] == '0'
    if cell['engine'] == 'lmdb':
        expected = 'false' if profile == 'durable' else 'true'
        assert flags['lmdb-nosync'] == flags['lmdb-nometasync'] == expected and flags['lmdb-writemap'] == 'false'
    if cell['engine'] == 'rocksdb':
        for s in stats:
            assert s['rocksdb.sync_writes'] == s['rocksdb.verify_checksums'] == 'true'
    assert [p['name'] for p in r['phases']] == PHASES
    for i, p in enumerate(r['phases']):
        assert math.isfinite(p['seconds']) and p['seconds'] > 0 and p['ops'] > 0
        assert math.isfinite(p['composition_seconds']) and p['composition_seconds'] >= p['seconds']
        assert math.isclose(p['ops_per_sec'], p['ops']/p['seconds'], rel_tol=1e-9)
        assert 0 <= p['p50_us'] <= p['p95_us'] <= p['p99_us'] <= p['p999_us'] <= p['max_us'] < math.inf
        assert p['requested_present'] + p['requested_absent'] == p['ops']
        assert 0 <= p['observed_hits'] <= p['requested_present']
        kinds = p['miss_kind_requests_arbitrary_common_prefix_deleted']
        assert len(kinds) == 3 and min(kinds) >= 0 and sum(kinds) == p['requested_absent']
        assert 0 <= p['distinct_present_requests'] <= p['requested_present']
        assert 0 <= p['distinct_absent_requests'] <= p['requested_absent']
        assert p['distinct_accesses'] == p['distinct_present_requests'] + p['distinct_absent_requests']
        if i < 3:
            assert p['ops'] == reads and p['observed_hits'] == p['requested_present']
        if i == 0:
            assert p['requested_present'] == p['ops']
        if i == 1:
            assert p['requested_absent'] == p['ops']
        if case == 'realistic' and i >= 2:
            # A1 draws IntN(100) < miss_percent for every request; no rounding
            # contract. The timed phase retains every selected successful read.
            percent, alpha = c['miss_percent'], 1e-12
            expected = p['ops']*percent/100
            tolerance = 0 if percent in (0, 100) else math.sqrt(p['ops']*math.log(2/alpha)/2)
            check = dict(phase=p['name'], model='Bernoulli configured p; Hoeffding screening',
                         alpha=alpha, configured_miss_percent=percent, ops=p['ops'],
                         requested_present=p['requested_present'], requested_absent=p['requested_absent'],
                         expected_absent=expected, tolerance_requests=tolerance)
            metadata.setdefault('miss_ratio_validation', []).append(check)
            assert abs(p['requested_absent']-expected) <= tolerance, ('configured miss ratio rejected', check)
        if case == 'realistic' and (i == 1 or (i >= 2 and c['miss_percent'] == 100)):
            assert max(kinds)-min(kinds) <= workers
        for measured, total in [('process_bytes_per_op', 'process_allocated_bytes'), ('process_allocs_per_op', 'process_mallocs')]:
            assert math.isclose(p[measured], p[total]/p['ops'], rel_tol=1e-9, abs_tol=1e-9)
    for name in ('load_seconds', 'initial_checkpoint_ms', 'reopen_ms', 'final_checkpoint_ms', 'final_reopen_ms'):
        assert math.isfinite(r[name]) and r[name] >= 0
    assert all(math.isfinite(v) and v >= 0 for v in r['update_batch_ms'] + r['checkpoint_ms'])
    assert all(v >= 0 for files in ('initial_files', 'final_files') for v in r[files].values())
    if cell.get('profiled', False):
        assert json.loads((directory/'quicksilver_results.json').read_text()) == reports
        names = ['benchprof_results.json', 'benchprof_results.md', 'insights.json', 'insights.md', 'insights.html', 'block.pprof', 'mutex.pprof', 'trace.out']
        names += [f'{prefix}_{phase}_{cell["engine"]}.pprof' for prefix in ('cpu', 'allocs') for phase in PHASES]
        names += [f'checkpoint_cpu_checkpoint_quicksilver_{boundary}_{cell["engine"]}.pprof' for boundary in ('initial', 'final')]
        assert all((directory/name).stat().st_size > 0 for name in names)


def main():
    if not __debug__:
        raise RuntimeError('capture validation requires Python assertions; do not use -O')
    manifest_path, plan_path = map(pathlib.Path, sys.argv[1:])
    assert manifest_path.is_absolute() and plan_path.is_absolute()
    root = manifest_path.parent
    manifest_raw, plan_raw = manifest_path.read_bytes(), plan_path.read_bytes()
    manifest, plan = json.loads(manifest_raw), json.loads(plan_raw)
    output = pathlib.Path(plan['output'])
    assert not output.is_absolute() and '..' not in output.parts
    out = root/output
    out.mkdir(exist_ok=False)
    (out/'manifest.json').write_bytes(manifest_raw)
    (out/'plan.json').write_bytes(plan_raw)
    for role in ('source', 'build', 'native', 'runner'):
        receipt = manifest['receipts'][role]
        raw = (root/receipt['path']).read_bytes()
        assert hashlib.sha256(raw).hexdigest() == receipt['sha256'], role
        (out/(role+'-receipt.json')).write_bytes(raw)
    for lib, identity in manifest['libraries'].items():
        assert pathlib.Path(lib).is_absolute() and sha256(pathlib.Path(lib)) == identity['sha256'], lib
    env = {k: v for k, v in os.environ.items() if not k.startswith('TREEDB_')}
    env.update(manifest['build_env'])
    assert env['GOWORK'] == 'off'
    env.update(GOMAXPROCS='12', GOGC='100', GODEBUG='', GOMEMLIMIT='2GiB',
               TREEDB_VLOG_MAX_MAPPED_SEALED_BYTES='1073741824', TMPDIR=str(root/'working-dbs'))
    (root/'working-dbs').mkdir(exist_ok=True)
    assert plan.get('repeats', 1) >= 1 and plan['cells']
    labels = [cell['label'] for cell in plan['cells']]
    assert len(set(labels)) == len(labels) and all(pathlib.Path(v).name == v and v not in ('.', '..') for v in labels)
    fixtures = {}
    for repeat in range(plan.get('repeats', 1)):
        cells = plan['cells']
        cells = cells[repeat % len(cells):] + cells[:repeat % len(cells)]
        for cell in cells:
            directory = out/(str(repeat+1)+'-'+cell['label'])
            directory.mkdir()
            source = manifest['sources'][cell['source']]
            binary = root/'bin'/source['binary']
            case, duration = cell.get('case', 'realistic'), cell.get('duration', '8s')
            extra = cell.get('flags', [])
            command = [str(binary), '-suite', 'quicksilver', '-dbs', cell['engine'], '-profile', cell.get('profile', 'durable'),
                       '-quicksilver-case', case, '-keys', str(cell.get('keys', 3000000)), '-read-workers', str(cell.get('workers', 4)),
                       '-quicksilver-reads', str(cell.get('reads', 6000000)), '-quicksilver-updates', str(cell.get('updates', 40000)),
                       '-quicksilver-duration', duration, '-quicksilver-read-batch', '64', '-quicksilver-commit', cell.get('commit', 'auto'), '-max-wall', '30m']
            if case == 'realistic':
                command += ['-seed', str(cell.get('seed', 24)), '-quicksilver-mixture', cell.get('mixture', 'primary'),
                            '-quicksilver-working-set', cell.get('working_set', 'uniform'), '-quicksilver-miss-percent', str(cell.get('miss_percent', 90))]
            command += extra
            if cell.get('profiled', False):
                command += ['-profile-dir', str(directory)]
            metadata = dict(cell=cell, repeat=repeat+1, command=command, source=source,
                            manifest_sha256=hashlib.sha256(manifest_raw).hexdigest(), plan_sha256=hashlib.sha256(plan_raw).hexdigest(),
                            collector_sha256=sha256(pathlib.Path(__file__)),
                            env={k: v for k, v in env.items() if k.startswith(('TREEDB_', 'LD_')) or k in
                                 ('GOROOT', 'GOWORK', 'GOGC', 'GODEBUG', 'GOMAXPROCS', 'GOMEMLIMIT', 'TMPDIR', 'LD_LIBRARY_PATH',
                                  'GLIBC_TUNABLES', 'PATH', 'CGO_ENABLED', 'GOFLAGS', 'CC', 'CXX', 'CGO_CFLAGS', 'CGO_LDFLAGS')},
                            load_before=os.getloadavg(), started=time.time())
            metadata_path = directory/'run.json'
            metadata_path.write_text(json.dumps(metadata, indent=2)+'\n')
            (directory/'stdout.json').touch()
            (directory/'stderr.log').touch()
            try:
                assert duration == '8s' or 'duration_ns' in cell
                assert cell['engine'] in ('treedb', 'lmdb', 'rocksdb') and cell.get('profile', 'durable') in CONTRACTS
                assert all(v.startswith('-'+cell['engine']+'-') and '=' in v for v in extra), 'extra flags must be engine tuning only'
                assert case != 'realistic' or cell.get('seed', 24) != 0
                assert source['head'] and sha256(binary) == source['binary_sha256']
                validate_native(binary, source, manifest['libraries'], env, root, directory, metadata)
                with (directory/'stdout.json').open('w') as stdout, (directory/'stderr.log').open('w') as stderr:
                    result = subprocess.run(['/usr/bin/time', '-v', *command], cwd=root, env=env, stdout=stdout, stderr=stderr)
                metadata['rc'] = result.returncode
                assert result.returncode == 0, (directory, result.returncode)
                reports = json.loads((directory/'stdout.json').read_text())
                validate(reports, cell, directory, fixtures, metadata)
                metadata['validated'] = True
            except BaseException as error:
                metadata['error'] = traceback.format_exc()
                raise
            finally:
                metadata.update(finished=time.time(), load_after=os.getloadavg())
                metadata_path.write_text(json.dumps(metadata, indent=2)+'\n')
            print(json.dumps(dict(cell=cell['label'], repeat=repeat+1,
                                  mops=[round(p['ops_per_sec']/1e6, 4) for p in reports[0]['phases']])), flush=True)
    print('VALIDATED_ALL', flush=True)


if __name__ == '__main__':
    main()
