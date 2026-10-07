#!/usr/bin/env python3
"""Descriptive tables from original gomap-r1-row-v1 bytes; no qualification/comparison."""
import argparse
import hashlib
import json
import math
from pathlib import Path
import statistics
import sys

METRICS = ('ns_per_op', 'ops_per_sec', 'bytes_per_op', 'allocs_per_op',
           'p50_ns', 'p95_ns', 'p99_ns', 'heap_after_bytes')
MUTATIONS = {'update_nonindexed', 'update_indexed', 'replace', 'delete', 'mixed_churn', 'upsert'}


def finite(value, name):
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
        raise ValueError(f'invalid nonnegative finite {name}')
    return value


def describe(values):
    if not values:
        return None
    middle = statistics.median(values)
    return {'median': middle, 'min': min(values), 'max': max(values),
            'spread_fraction': (max(values) - min(values)) / middle if middle else None}


def category(route, name):
    if route == 'state_transition':
        return 'read-state transition'
    if route == 'setup':
        return 'view setup'
    if route == 'warmup':
        return 'first fetch'
    if name == 'load':
        return 'load'
    if name == 'checkpoint':
        return 'checkpoint'
    if name in MUTATIONS:
        return 'mutation'
    return 'read' if name in {'point_get_into_complete', 'point_complete', 'batch_complete', 'range_complete', 'range_public_complete'} else 'other declared phase'


def report(path):
    raw = path.read_bytes()
    packet = json.loads(raw)
    if packet.get('schema') != 'gomap-r1-row-v1':
        raise ValueError('expected original A gomap-r1-row-v1 packet')
    config = packet['config']
    engines = config['engines']
    repetitions = config['repetitions']
    if type(repetitions) is not int or repetitions < 1 or len(set(engines)) != len(engines):
        raise ValueError('invalid declared engines/repetitions')
    by_engine = {e: [] for e in engines}
    seen = set()
    phase_groups = {}
    storage_groups = {}
    for cell in packet['cells']:
        engine, rep = cell['engine'], cell['repetition']
        if engine not in by_engine or type(rep) is not int or not 0 <= rep < repetitions or (engine, rep) in seen:
            raise ValueError('invalid/duplicate cell engine/repetition')
        seen.add((engine, rep))
        by_engine[engine].append(cell)
        if cell.get('unsupported'):
            continue  # Unsupported zero placeholders are never measured data.
        routes = [(r, cell[r]) for r in ('state_transition', 'setup', 'warmup')]
        routes += [('phases', phase) for phase in cell['phases']]
        phase_seen = set()
        for route, phase in routes:
            name = phase['name']
            if (route, name) in phase_seen:
                raise ValueError('duplicate phase in a cell')
            phase_seen.add((route, name))
            ops, rows = phase['operations'], phase['rows']
            if type(ops) is not int or type(rows) is not int or ops < 0 or rows < 0:
                raise ValueError('invalid raw operations/rows denominator')
            skipped = phase.get('skipped', '')
            if not isinstance(skipped, str):
                raise ValueError('skip must be original string reason')
            if not skipped:
                if ops <= 0:
                    raise ValueError('enabled measurement has no calls')
                for key in METRICS:
                    finite(phase[key], key)
                if phase['ns_per_op'] <= 0 or phase['ops_per_sec'] <= 0:
                    raise ValueError('enabled timing is nonpositive')
            # Never average different denominators, enabled/skipped states or skip reasons.
            signature = (engine, route, name, ops, rows, skipped)
            group = phase_groups.setdefault(signature, {'engine': engine, 'route': route,
                'category': category(route, name), 'phase': name, 'operations': ops, 'rows': rows,
                'skipped': skipped or None, 'repetitions': []})
            group['repetitions'].append({'repetition': rep, 'measurement': phase,
                'rows_per_sec': phase['ops_per_sec'] * rows / ops if not skipped and rows else None})
        boundary = cell['storage_boundary']
        storage_groups.setdefault((engine, boundary), []).append(cell)
    phases = []
    for group in phase_groups.values():
        group['repetitions'].sort(key=lambda x: x['repetition'])
        group['observed_repetitions'] = len(group['repetitions'])
        group['matches_declared_repetition_count'] = group['observed_repetitions'] == repetitions
        group['metrics'] = None if group['skipped'] else {
            key: describe([r['measurement'][key] for r in group['repetitions']]) for key in METRICS}
        if group['metrics'] is not None:
            group['metrics']['rows_per_sec'] = describe([r['rows_per_sec'] for r in group['repetitions'] if r['rows_per_sec'] is not None])
            spread = group['metrics']['ops_per_sec']['spread_fraction']
            group['descriptive_timing_spread'] = 'inconclusive_above_15pct' if spread > .15 else 'within_15pct'
        phases.append(group)
    storage = []
    for (engine, boundary), cells in storage_groups.items():
        storage.append({'engine': engine, 'boundary': boundary, 'observed_repetitions': len(cells),
            'metrics': {key: describe([finite(c[key], key) for c in cells])
                        for key in ('persistent_bytes', 'wal_bytes', 'transient_bytes')},
            'raw_declared_components': [{'repetition': c['repetition'],
                'persistent_bytes': c['persistent_bytes'], 'wal_bytes': c['wal_bytes'],
                'transient_bytes': c['transient_bytes'], 'stats': c.get('stats', {})}
                for c in sorted(cells, key=lambda c: c['repetition'])]})
    return {'report_schema': 'private-r1-A-descriptive-report-v1',
        'status': 'DESCRIPTIVE_ONLY_NOT_ACCEPTANCE_OR_COMPARISON',
        'input': {'path': str(path.resolve()), 'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw)},
        'original_packet_metadata': {k: v for k, v in packet.items() if k != 'cells'},
        'limitations': [
            'Go bytes/call and objects/call exclude SQLite C allocations; they are not total memory.',
            'Latency columns are medians of per-repetition p50/p95/p99, not pooled operation percentiles.',
            'Each metric median is computed independently; median calls/s need not be the reciprocal of median ns/call.',
            'Rows/s is derived within each repetition from actual rows/calls and calls/s; zero-row phases have no rows/s.',
            'Groups with different denominators, skip state/reason or storage boundary remain separate.',
            'Storage stats are raw declared strings, not additive components; no undeclared component is invented.',
            'Heap-after is a runtime snapshot including harness state, not retained engine-only memory.',
            'Above-15% throughput spread is descriptive timing inconclusive, never acceptance.',
            'This helper does not establish source trust, oracle acceptance, compatibility or speedup.'],
        'engine_cells': [{'engine': e, 'expected_repetitions': repetitions,
             'observed_repetition_ids': sorted(c['repetition'] for c in by_engine[e]),
             'repetitions': [{'repetition': c['repetition'], 'unsupported': c.get('unsupported'),
                 'rejection': c.get('rejection'), 'capabilities': c.get('capabilities', {}),
                 'oracle_verified': c.get('oracle_verified'), 'acknowledgement': c.get('acknowledgement')}
                 for c in sorted(by_engine[e], key=lambda c: c['repetition'])]} for e in engines],
        'phase_groups': phases, 'engine_storage': storage, 'raw_cells': packet['cells']}


def escaped(value):
    return str(value).replace('|', '\\|').replace('\n', ' ')


def num(value):
    return '—' if value is None else f'{value:,.3f}'.rstrip('0').rstrip('.')


def markdown(result):
    meta = result['original_packet_metadata']
    lines = ['# A packet descriptive report', '',
        f"Original packet SHA256: `{result['input']['sha256']}`; source: `{meta['source']['commit']}`.", '',
        f"Declared configuration: `{json.dumps(meta['config'], sort_keys=True)}`.", '',
        'Descriptive output only; no acceptance, compatibility or speedup claim.', '']
    lines += ['- ' + x for x in result['limitations']] + ['', '## Cells and capabilities', '',
        '| Engine | Observed/declared repetitions | Unsupported/rejected | Capabilities by repetition |',
        '| --- | ---: | --- | --- |']
    for engine in result['engine_cells']:
        rejected = [{'repetition': r['repetition'], 'unsupported': r['unsupported'], 'rejection': r['rejection']}
                    for r in engine['repetitions'] if r['unsupported']]
        capability_groups = {}
        for repetition in engine['repetitions']:
            encoded = json.dumps(repetition['capabilities'], sort_keys=True)
            capability_groups.setdefault(encoded, []).append(repetition['repetition'])
        capabilities = [{'repetitions': reps, 'capabilities': json.loads(encoded)} for encoded, reps in capability_groups.items()]
        lines.append(f"| {escaped(engine['engine'])} | {len(engine['observed_repetition_ids'])}/{engine['expected_repetitions']} | {escaped(json.dumps(rejected))} | {escaped(json.dumps(capabilities, sort_keys=True))} |")
    for cat in ('read-state transition', 'view setup', 'first fetch', 'load', 'read', 'mutation', 'checkpoint', 'other declared phase'):
        groups = [g for g in result['phase_groups'] if g['category'] == cat]
        if not groups:
            continue
        lines += ['', '## ' + cat.capitalize(), '',
            '| Engine | Phase | Reps | Calls / rows per repetition | ns/call median [min–max] | Calls/s | Rows/s | Go B/call | Go objects/call | p50 / p95 / p99 ns medians | Calls/s spread | State / skip reason |',
            '| --- | --- | ---: | --- | --- | ---: | ---: | ---: | ---: | --- | ---: | --- |']
        for g in groups:
            prefix = f"| {escaped(g['engine'])} | {escaped(g['phase'])} | {g['observed_repetitions']} | {g['operations']} / {g['rows']} |"
            if g['skipped']:
                lines.append(prefix + ' — | — | — | — | — | — | — | skipped: ' + escaped(g['skipped']) + ' |')
                continue
            m = g['metrics']
            timing = m['ns_per_op']
            percentiles = ' / '.join(num(m[k]['median']) for k in ('p50_ns', 'p95_ns', 'p99_ns'))
            rows = m['rows_per_sec']
            lines.append(prefix + f" {num(timing['median'])} [{num(timing['min'])}–{num(timing['max'])}] | {num(m['ops_per_sec']['median'])} | {num(rows['median'] if rows else None)} | {num(m['bytes_per_op']['median'])} | {num(m['allocs_per_op']['median'])} | {percentiles} | {100*m['ops_per_sec']['spread_fraction']:.2f}% | {g['descriptive_timing_spread']} |")
    lines += ['', '## Engine storage', '',
        'Bytes at the original declared boundary; each entry is median [minimum–maximum].', '',
        '| Engine | Boundary | Reps | Persistent bytes | WAL bytes | Transient bytes |',
        '| --- | --- | ---: | --- | --- | --- |']
    for s in result['engine_storage']:
        values = [f"{num(s['metrics'][k]['median'])} [{num(s['metrics'][k]['min'])}–{num(s['metrics'][k]['max'])}]" for k in ('persistent_bytes', 'wal_bytes', 'transient_bytes')]
        lines.append(f"| {s['engine']} | {escaped(s['boundary'])} | {s['observed_repetitions']} | " + ' | '.join(values) + ' |')
    lines += ['', 'Raw declared storage/stat fields are retained per repetition in JSON (`engine_storage.raw_declared_components`), without summing or interpreting them.', '']
    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('packet', type=Path)
    parser.add_argument('--format', choices=('json', 'markdown'), default='markdown', help='stdout format when no output paths supplied')
    parser.add_argument('--json-out', type=Path)
    parser.add_argument('--markdown-out', type=Path)
    args = parser.parse_args()
    outputs = [p for p in (args.json_out, args.markdown_out) if p is not None]
    if len({str(p.resolve()) for p in outputs}) != len(outputs) or any(p.exists() or p.is_symlink() for p in outputs):
        parser.error('output paths must be distinct NEW files; refusing overwrite')
    result = report(args.packet)
    json_text = json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + '\n'
    markdown_text = markdown(result)
    if outputs:
        for path, contents in ((args.json_out, json_text), (args.markdown_out, markdown_text)):
            if path is not None:
                with path.open('x', encoding='utf-8') as handle:
                    handle.write(contents)
    else:
        sys.stdout.write(json_text if args.format == 'json' else markdown_text)


if __name__ == '__main__':
    try:
        main()
    except (ValueError, KeyError, TypeError) as error:
        sys.exit(f'report refused malformed input: {error}')
