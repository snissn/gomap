#!/usr/bin/env python3
"""Derive comparisons from separately CLI-validated, frozen R1 packets."""
import argparse
import json
import statistics
from pathlib import Path


def capture_identity(path, packet):
    root = path.parent
    before = json.loads((root / 'source.json').read_text())
    after = json.loads((root / 'source-after.json').read_text())
    accepted = json.loads((root / 'frozen-source-accepted.json').read_text())
    environment = json.loads((root / 'capture-environment.json').read_text())
    assert before == after == packet['source'], 'source sidecar mismatch'
    assert accepted['accepted'] and accepted['commit'] == before['commit'] == environment['frozen_source'], 'unaccepted frozen source'
    assert accepted['runtime_sha256'] == before['runtime_sha256'] and accepted['harness_sha256'] == before['harness_sha256'], 'freeze digest mismatch'
    assert environment['temp_device'] == environment['output_parent_device'], 'database filesystem mismatch'
    assert (root / 'validation.txt').read_text().strip(), 'missing independent CLI validation'
    buildinfo = (root / 'buildinfo.txt').read_text().splitlines()[1:]
    # Revision/time legitimately differ for before/after product commits. Keep
    # module, compiler, tags, CGO and all other actual binary build settings.
    buildinfo = [line for line in buildinfo if not any(key in line for key in ('vcs.revision=', 'vcs.time='))]
    sqlite = sorted({(cell['engine'], tuple(sorted((key, value) for key, value in cell.get('stats', {}).items() if key.startswith('sqlite.'))))
                     for cell in packet['cells'] if cell['engine'].startswith('sqlite') and not cell.get('unsupported')})
    return {
        **{key: packet[key] for key in ('go_version', 'hostname', 'goos', 'goarch')},
        'cpu_count': environment['cpu_count'], 'uname': environment['uname'],
        'go_build_environment': environment['go_build_environment'],
        'go_runtime_environment': {key: environment['environment'][key]
                                   for key in ('GOMAXPROCS', 'GOGC', 'GOMEMLIMIT', 'GODEBUG')},
        'database_device': environment['temp_device'],
        'compiler_version': (root / 'cc-version.txt').read_text(),
        'buildinfo': buildinfo, 'sqlite_settings': sqlite,
    }


def groups(packet):
    result = {}
    for cell in packet['cells']:
        if cell.get('unsupported'):
            continue
        for phase in [cell['state_transition'], cell['setup'], cell['warmup'], *cell['phases']]:
            if not phase.get('skipped'):
                result.setdefault((cell['engine'], phase['name']), []).append(phase)
    return result


def describe(rows):
    throughput = [r['ops_per_sec'] for r in rows]
    med = statistics.median(throughput)
    spread = (max(throughput) - min(throughput)) / med
    return {
        'repetitions': len(rows), 'ops_per_sec_median': med,
        'throughput_spread': spread,
        'noise_status': 'inconclusive' if spread > .15 else 'within_15pct',
        **{key + '_median': statistics.median(r[key] for r in rows)
           for key in ('ns_per_op', 'bytes_per_op', 'allocs_per_op', 'p50_ns', 'p95_ns', 'p99_ns')},
    }


def compare(before, after, environment_before, environment_after):
    assert environment_before == environment_after, 'host/toolchain/compiler/Go-runtime/filesystem/SQLite mismatch'
    assert before['schema'] == after['schema'], 'schema mismatch'
    assert before['config'] == after['config'], 'configuration/timer scope mismatch'
    assert before['fixture_sha256'] == after['fixture_sha256'], 'fixture mismatch'
    assert before['source']['harness_sha256'] == after['source']['harness_sha256'], 'harness mismatch'
    assert before['source']['clean'] and after['source']['clean'], 'dirty source'
    first, second = groups(before), groups(after)
    rows = []
    for engine, phase in sorted(first.keys() | second.keys()):
        b, a = first.get((engine, phase)), second.get((engine, phase))
        record = {'engine': engine, 'phase': phase}
        if b is None or a is None:
            record.update(comparison='newly_enabled' if b is None else 'removed',
                          before=describe(b) if b else None,
                          after=describe(a) if a else None)
        else:
            assert len(b) == len(a) == before['config']['repetitions'], 'repetition mismatch'
            assert {(r['operations'], r['rows']) for r in b} == {(r['operations'], r['rows']) for r in a}, 'denominator mismatch'
            bs, actual = describe(b), describe(a)
            record.update(before=bs, after=actual,
                          comparison='inconclusive' if any(r['noise_status'] == 'inconclusive' for r in (bs, actual)) else 'descriptive_matched',
                          throughput_ratio=actual['ops_per_sec_median']/bs['ops_per_sec_median'],
                          bytes_per_op_delta=actual['bytes_per_op_median']-bs['bytes_per_op_median'],
                          allocs_per_op_delta=actual['allocs_per_op_median']-bs['allocs_per_op_median'])
        rows.append(record)
    return {'source_before': before['source'], 'source_after': after['source'],
            'matched_capture_environment': environment_before,
            'config': before['config'], 'fixture_sha256': before['fixture_sha256'],
            'note': 'Derived medians; validate source-bound raw packets independently. Spread >15% leaves throughput inconclusive. Go allocation deltas exclude SQLite C allocations.',
            'comparisons': rows}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('before', type=Path)
    parser.add_argument('after', type=Path)
    args = parser.parse_args()
    before = json.loads(args.before.read_text())
    after = json.loads(args.after.read_text())
    print(json.dumps(compare(before, after, capture_identity(args.before, before), capture_identity(args.after, after)), indent=2, allow_nan=False))
