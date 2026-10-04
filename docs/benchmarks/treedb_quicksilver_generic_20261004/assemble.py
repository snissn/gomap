#!/usr/bin/env python3
"""Assemble retained Quicksilver evidence; never run a benchmark or change inputs.

python3 assemble.py ABS_EVIDENCE_ROOT ABS_OUTPUT_DIR [--bundle ABS_CAPTURE_DIR ...]
    [--repo ABS_GOMAP_REPO --landed-final FROZEN_MAIN_SHA --finalize]

Capture directories contain plan/manifest.json, four *-receipt.json files and
*/run.json, stdout.json, stderr.log, ldd.stdout.txt, ldd.stderr.txt. Original
candidate SHAs remain in receipts. Finalization requires their compiled project
inputs and TreeDB/harness to equal the explicitly supplied landed main SHA. Missing/failed cells
remain PENDING; --finalize refuses them. Review/performance acceptance is external.
"""
import argparse
import hashlib
import json
import pathlib
import re
import statistics
import subprocess

BASE = '6137db44b66e0323daba05ae891db44a8055c0fd'
BASE_PACKET = '52959db2d657b5a57d992a677e27c2d1f676654891a4cfaa4680480bf24631a9'
HERE = pathlib.Path(__file__).resolve().parent


def require(ok, message):
    if not ok:
        raise ValueError(message)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def read(path):
    return json.loads(path.read_text(), parse_constant=lambda v: fail('nonfinite JSON ' + v))


def fail(message):
    raise ValueError(message)


def semantic(cell):
    return json.dumps({k: v for k, v in cell.items() if k not in ('source', 'label')}, sort_keys=True)


def git(repo, *args):
    return subprocess.check_output(['git', '-C', str(repo), *args])


def contract(repo):
    raw = git(repo, 'show', BASE + ':scripts/unified_bench_quicksilver_capture.py')
    namespace = {'__name__': 'frozen_capture_contract'}
    # The landed validator uses assertions. Preserve them under python -O too.
    exec(compile(raw, 'frozen_capture_contract.py', 'exec', optimize=0), namespace)
    return namespace['validate'], digest(raw)


def project_inputs(repo, head, files):
    """Check receipt bytes against Git objects in one bounded stdlib subprocess."""
    with subprocess.Popen(['git', '-C', str(repo), 'cat-file', '--batch'],
                          stdin=subprocess.PIPE, stdout=subprocess.PIPE) as process:
        for name, expected in files.items():
            require('\n' not in name, 'invalid source path')
            process.stdin.write((head + ':' + name + '\n').encode())
            process.stdin.flush()
            header = process.stdout.readline().split()
            require(len(header) == 3 and header[1] == b'blob', 'missing source blob ' + name)
            raw = process.stdout.read(int(header[2]))
            require(process.stdout.read(1) == b'\n' and digest(raw) == expected, 'source bytes ' + name)
        process.stdin.close()
        require(process.wait() == 0, 'git source verification')


def compact(result):
    output = {k: v for k, v in result.items() if k not in ('initial_stats', 'final_stats', 'registered_cli_flags')}
    output['phases'] = [{k: v for k, v in p.items() if k not in ('stats_before', 'stats_after')}
                        for p in result['phases']]
    return output


def load_bundle(directory, repo, landed, baseline, validate, collector, raw_hashes, fixtures, evidence):
    directory = directory.resolve()
    for path in sorted(directory.rglob('*')):
        if path.is_file():
            raw_hashes[str(path)] = digest(path.read_bytes())
    manifest, plan = read(directory / 'manifest.json'), read(directory / 'plan.json')
    receipts = {}
    for name, entry in manifest['receipts'].items():
        path = directory / (name + '-receipt.json')
        require(digest(path.read_bytes()) == entry['sha256'], 'receipt hash ' + str(path))
        receipts[name] = read(path)
    require(set(receipts) == {'source', 'build', 'native', 'runner'}, 'complete receipts')
    require(manifest['libraries'] == baseline['manifest']['libraries'], 'matched native inventory')
    require(receipts['native']['libraries'] == manifest['libraries'], 'native receipt')
    baseline_directory = evidence / 'baseline-primary-local'
    native_before = read(baseline_directory / 'native-receipt.json')
    for key in ('headers', 'versions'):
        require(receipts['native'][key] == native_before[key], 'matched native ' + key)
    runner_before = read(baseline_directory / 'runner-receipt.json')
    for key in ('uname', 'lscpu', 'cpu_governors', 'shared_host'):
        require(receipts['runner'][key] == runner_before[key], 'matched runner ' + key)
    require(receipts['build']['go_version'].strip() == baseline['source_identity']['build_go_version'].strip(), 'matched Go version')
    source = receipts['source']
    require(landed, '--landed-final required for final raw inputs')
    head = source['head']
    require(git(repo, 'rev-parse', head + ':TreeDB') == git(repo, 'rev-parse', landed + ':TreeDB'), 'final landed TreeDB equality')
    for subtree in ('cmd/unified_bench', 'cmd/benchprof'):
        require(git(repo, 'rev-parse', head + ':' + subtree) == git(repo, 'rev-parse', BASE + ':' + subtree), 'unchanged harness ' + subtree)
        require(git(repo, 'rev-parse', head + ':' + subtree) == git(repo, 'rev-parse', landed + ':' + subtree), 'landed harness ' + subtree)
    require(git(repo, 'show', head + ':scripts/unified_bench_quicksilver_capture.py') ==
            git(repo, 'show', BASE + ':scripts/unified_bench_quicksilver_capture.py'), 'unchanged capture contract')
    require(git(repo, 'show', landed + ':scripts/unified_bench_quicksilver_capture.py') ==
            git(repo, 'show', BASE + ':scripts/unified_bench_quicksilver_capture.py'), 'landed capture contract')
    require(all(source['files'][k] == v for k, v in source['compiled_project_inputs'].items()), 'project input inventory')
    project_inputs(repo, head, source['compiled_project_inputs'])
    project_inputs(repo, landed, source['compiled_project_inputs'])
    require(receipts['build']['head'] == head and all(b['rc'] == 0 for b in receipts['build']['builds'].values()), 'successful matching build')
    expected_env = next(r for r in baseline['run_records'] if r['group'] == 'primary')['run_metadata']['env']
    libraries = {k: v['sha256'] for k, v in manifest['libraries'].items()}
    records = []
    for path in sorted(directory.glob('*/run.json')):
        meta = read(path)
        cell = meta['cell']
        require(cell in plan['cells'] and meta['repeat'] == 1, 'declared single-repeat cell')
        require(meta['plan_sha256'] == digest((directory / 'plan.json').read_bytes()), 'plan binding')
        require(meta['manifest_sha256'] == digest((directory / 'manifest.json').read_bytes()), 'manifest binding')
        require(meta['collector_sha256'] == collector, 'collector identity')
        require(meta['source'] == manifest['sources'][cell['source']] and meta['source']['head'] == head, 'run source identity')
        require(receipts['build']['binaries'][meta['source']['binary']]['binary_sha256'] == meta['source']['binary_sha256'], 'built executable binding')
        require(meta['env'] == expected_env, 'matched final runtime environment')
        loader = meta['native_resolution']
        require(loader['rc'] == 0 and loader['libraries'] == libraries and loader['linkage'] == 'dynamic', 'actual native loader binding')
        require(all(not v for k, v in loader['loader_env'].items() if k != 'LD_LIBRARY_PATH'), 'loader injection')
        require(loader['loader_env']['LD_LIBRARY_PATH'] == meta['env']['LD_LIBRARY_PATH'], 'loader environment')
        for name in ('stdout.json', 'stderr.log', 'ldd.stdout.txt', 'ldd.stderr.txt'):
            require((path.parent / name).is_file(), 'missing raw receipt ' + name)
        stderr = (path.parent / 'stderr.log').read_text()
        record = {'cell': cell, 'raw_directory': str(path.parent), 'run_metadata': meta, 'status': 'FAILED_OR_CENSORED', 'result': None}
        if meta['rc'] == 0:
            require(meta.get('validated') is True and 'SKIP' not in stderr and 'Exit status: 0' in stderr, 'successful non-SKIP receipt')
            reports = read(path.parent / 'stdout.json')
            validate(reports, cell, path.parent, fixtures, {})
            flags = reports[0]['registered_cli_flags']
            for override in cell.get('flags', []):
                key, value = override.lstrip('-').split('=', 1)
                require(flags[key] == value, 'executed override ' + key)
            record.update(status='PASS_UNPROFILED', result=compact(reports[0]),
                          peak_rss_kib=int(re.search(r'Maximum resident set size \(kbytes\): (\d+)', stderr)[1]),
                          wall_seconds=meta['finished'] - meta['started'])
        records.append(record)
    return records


def describe(records, engine, profile, phase, metric):
    values = [next(p for p in r['result']['phases'] if p['name'] == phase)[metric]
              for r in records if r['result'] and r['cell']['engine'] == engine and r['cell']['profile'] == profile]
    return {'median': statistics.median(values), 'min': min(values), 'max': max(values)} if values else None


def assemble(args):
    root, output, repo = args.evidence.resolve(), args.output.resolve(), args.repo.resolve()
    packet = root / 'A2-full-baseline-completion.json'
    require(digest(packet.read_bytes()) == BASE_PACKET, 'accepted baseline packet identity')
    baseline = read(packet)
    # Campaign amendments/optimization diagnostics may evolve; the accepted raw
    # captures and baseline binding remain immutable. Verify all capture entries.
    hashes = {str(packet): BASE_PACKET}
    for name, expected in baseline['raw_file_sha256'].items():
        if name.startswith('baseline-') or name.startswith('landed-source-equality'):
            path = root / name
            require(digest(path.read_bytes()) == expected, 'accepted baseline raw hash ' + name)
            hashes[str(path)] = expected
    plans = read(HERE / 'plans.json')
    for path in (HERE / 'plans.json', HERE / 'assemble.py'):
        hashes[str(path)] = digest(path.read_bytes())
    expected = {semantic(c): {'plan': name, 'cell': c} for name, p in plans.items() for c in p['cells']}
    require(len(expected) == 43, '43 distinct declared final cells')
    validate, collector = contract(repo)
    require(collector == baseline['source_identity']['collector_sha256'], 'baseline collector contract')
    final, fixtures = {}, {}
    for directory in args.bundle:
        for record in load_bundle(directory, repo, args.landed_final, baseline, validate, collector, hashes, fixtures, root):
            key = semantic(record['cell'])
            require(key in expected and key not in final, 'unexpected or duplicate final cell')
            require(not record['cell'].get('profiled'), 'profiles excluded from unprofiled report')
            final[key] = record
    before = {semantic(r['run_metadata']['cell']): r for r in baseline['run_records'] if r['status'] == 'PASS_UNPROFILED'}
    pairs = []
    for key, after in final.items():
        if key in before and after['result']:
            old = before[key]['result']
            for field in ('config', 'loaded_distribution', 'compressibility'):
                require(after['result'].get(field) == old.get(field), 'paired actual workload ' + field)
            pairs.append({'cell': after['cell'], 'before_raw': before[key]['raw_directory'], 'after_raw': after['raw_directory']})
    missing = [v for k, v in expected.items() if k not in final or final[k]['status'] != 'PASS_UNPROFILED']
    require(not args.finalize or (not missing and args.landed_final), 'finalization requires all 43 PASS cells and landed binding')
    primary_before = [{'cell': r['run_metadata']['cell'], 'result': r['result']} for r in baseline['run_records'] if r['group'] == 'primary']
    primary_after = [r for k, r in final.items() if expected[k]['plan'] == 'plan-final-primary-3m.json']
    data = {'schema': 'quicksilver-generic-evidence-v1', 'status': 'COMPLETE_EVIDENCE' if not missing and args.landed_final else 'PENDING',
            'optimization_acceptance': 'PENDING_COORDINATOR_REVIEW', 'baseline_source': baseline['source_identity'],
            'final_landed_sha': args.landed_final, 'required_cells': 43, 'missing_or_failed': missing,
            'plans': plans, 'matched_pairs': pairs, 'final_records': list(final.values()),
            'baseline_records': baseline['run_records'], 'baseline_profile_diagnostics_only': baseline['profile_diagnostics_only'],
            'baseline_pareto_targets': baseline['pareto_targets'],
            'baseline_original_raw_file_sha256': baseline['raw_file_sha256'], 'raw_file_sha256': hashes}
    lines = ['# Generic Quicksilver owned-value evidence', '', '**' + data['status'] + '** — optimization acceptance remains coordinator-owned.', '',
             'The 3M primary table pairs seeds 24/91/2027. Entries show median [min, max] over different fixture seeds; these are descriptive variation, not confidence or noise intervals. Final columns remain PENDING until all three observations exist.', '',
             '| ACK / engine / phase | before Mops/s | final Mops/s | before p99 µs | final p99 µs | before Go B/op / allocs/op | final Go B/op / allocs/op |',
             '|---|---:|---:|---:|---:|---:|---:|']
    def shown(records, e, p, phase, metric, scale=1):
        d = describe(records, e, p, phase, metric)
        if d is None or len([r for r in records if r['result'] and r['cell']['engine'] == e and r['cell']['profile'] == p]) != 3:
            return 'PENDING'
        return f"{d['median']/scale:.4f} [{d['min']/scale:.4f}, {d['max']/scale:.4f}]"
    for e, p in [('treedb', 'durable'), ('lmdb', 'durable'), ('rocksdb', 'durable'), ('treedb', 'fast')]:
        if p == 'fast':
            lines += ['', '**TreeDB fast below acknowledges volatile ordinary writes; it is a separate policy comparison.**', '']
            lines += ['| ACK / engine / phase | before Mops/s | final Mops/s | before p99 µs | final p99 µs | before Go B/op / allocs/op | final Go B/op / allocs/op |', '|---|---:|---:|---:|---:|---:|---:|']
        for phase in ('quicksilver_hits', 'quicksilver_misses', 'quicksilver_mixed', 'quicksilver_concurrent'):
            cols = [shown(rs, e, p, phase, metric, scale) for rs in (primary_before, primary_after) for metric, scale in [('ops_per_sec', 1e6), ('p99_us', 1)]]
            alloc = [' / '.join(shown(rs, e, p, phase, m) for m in ('process_bytes_per_op', 'process_allocs_per_op')) for rs in (primary_before, primary_after)]
            lines.append('| ' + ' | '.join([e + ' ' + p + ' ' + phase.removeprefix('quicksilver_'), cols[0], cols[2], cols[1], cols[3], *alloc]) + ' |')
    lines += ['', '## Coverage and interpretation', '',
              f'{43-len(missing)}/43 final unprofiled cells PASS. Required plans: primary 12, holdout 4, 1% working set 4, controls 2, sync 1, 10M capacity 4, scaling 16. The four-reader scaling point reuses holdout seed173; 8/16/32/64 use that same holdout20%70miss fixture. Capacity also uses holdout173. Raw labels do not define workload identity.', '',
              'Readers run about 8 seconds in concurrent phases, while writer composition may last longer. Baseline durable TreeDB reader/composition is about 8s / 24.213–28.878s; fast is about 8s / 21.311–21.698s. Go MemStats bracket reader start through reader join, before writer completion: they include overlapping writer/harness work and exclude the writer-only tail. Raw data does not expose completed mutation/checkpoint counts within the reader window; sustained overlap is not inferred.', '',
              'Throughput includes PRNG/key generation, distinct bitmap work and full-value checking. Per-request latency excludes key/bitmap preparation and includes Get/snapshot maintenance. p99 uses capped deterministic prefix sampling, not an unbiased whole-duration sample. The full oracle/checkpoint/reopen warms caches before reads. No cold-cache claim is made.', '',
              'Go allocations exclude native C allocations and mmap; GOMEMLIMIT2GiB is not a process RSS cap. Peak RSS includes load/verification and native work. LMDB64GiB is virtual map capacity; RocksDB64MiB is its cache control. Shared-host CPU/OS/native receipts and load observations remain in raw data. Native ACK flags differ; suite ordinary/sync dispatch does not establish cross-engine durability equivalence.', '',
              'Actual initial/final storage sums, checkpoint/update-batch/reopen timings, distributions, verification/request counts, four-phase allocations and original receipts are retained in RESULTS.json. TreeDB fast column-physical durability/storage accounting is unsupported and must not be inferred as zero.', '',
              'The baseline 3M explicit-sync holdout173 capture is retained as a 30-minute censored performance failure (rc1, empty JSON); it supplies no completed throughput, correctness or corruption finding. Its failed database remains retained. A completed final sync cell is mandatory. Historical random4k control is separate from the new generic baseline.', '',
              'Profiles are diagnostic attribution only and excluded from unprofiled performance. Baseline outer-leaf/frame allocation Pareto targets O1/O2 are retained in JSON; O3 sync work and matched candidate attribution require final evidence. Original candidate receipts preserve their original SHAs; publication requires exact landed tree, compiled project input, unchanged harness, raw hash, build/native/loader and actual config binding.', '',
              'Missing or failed cells: ' + ', '.join(v['cell']['label'] for v in missing), '',
              'Independent review, CI, performance/noise/checkpoint/storage guardrails and graph acceptance remain external gates even when evidence coverage is complete.', '']
    output.mkdir(parents=True, exist_ok=True)
    (output / 'RESULTS.json').write_text(json.dumps(data, indent=2, allow_nan=False) + '\n')
    (output / 'REPORT.md').write_text('\n'.join(lines))
    return data


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('evidence', type=pathlib.Path)
    parser.add_argument('output', type=pathlib.Path)
    parser.add_argument('--bundle', type=pathlib.Path, action='append', default=[])
    parser.add_argument('--repo', type=pathlib.Path, default=HERE.parents[2])
    parser.add_argument('--landed-final')
    parser.add_argument('--finalize', action='store_true')
    args = parser.parse_args()
    result = assemble(args)
    print(json.dumps({'status': result['status'], 'required': 43, 'remaining': len(result['missing_or_failed'])}))
