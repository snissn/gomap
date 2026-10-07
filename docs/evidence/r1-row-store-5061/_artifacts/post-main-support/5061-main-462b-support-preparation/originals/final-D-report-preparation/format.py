#!/usr/bin/env python3
"""Describe an original finite D packet; never qualify or accept evidence."""
import argparse
import hashlib
import json
from pathlib import Path
import statistics
from urllib.parse import quote

VALIDATOR_SHA256 = 'dcfd45e233bc46a1c1f73ffaf2af840e5d8ab8ba312342ceb61b014e7c199968'
COMPONENTS = ['index', 'persistent_vlog', 'persistent_leaf_log', 'typed_assets',
              'redo_wal', 'dictionary_store', 'template_store', 'immutable_manifest_metadata', 'other']


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def cell(value):
    if value is None:
        return '—'
    if isinstance(value, (dict, list)):
        value = json.dumps(value, sort_keys=True, separators=(',', ':'))
    return str(value).replace('|', '&#124;').replace('\n', ' ')


def table(lines, headers, rows):
    lines.extend(['', '| ' + ' | '.join(headers) + ' |',
                  '| ' + ' | '.join(['---'] * len(headers)) + ' |'])
    for row in rows:
        if len(row) != len(headers):
            raise ValueError('table width mismatch')
        lines.append('| ' + ' | '.join(map(cell, row)) + ' |')
    lines.append('')


def trajectory(census):
    # Subtract inside each process; never subtract cross-process medians.
    result = []
    first, previous = census[0], None
    for phase in census:
        result.append({'phase': phase['phase'],
            'adjacent_bytes': {key: None if previous is None else
                phase['logical_bytes'][key] - previous['logical_bytes'][key] for key in COMPONENTS},
            'growth_from_ingest_bytes': {key: phase['logical_bytes'][key] -
                first['logical_bytes'][key] for key in COMPONENTS},
            'adjacent_total_bytes': None if previous is None else phase['total_bytes'] - previous['total_bytes'],
            'growth_from_ingest_total_bytes': phase['total_bytes'] - first['total_bytes']})
        previous = phase
    return result


def run(packet_path, validator_path, packet_link, raw_root_link, validation_log_link, details_link):
    validator_bytes = validator_path.read_bytes()
    if sha(validator_bytes) != VALIDATOR_SHA256:
        raise ValueError('formatter requires the reviewed frozen validator hash; reassess changed schemas')
    module = {'__name__': 'private_d_report_parser', '__file__': str(validator_path)}
    exec(compile(validator_bytes, str(validator_path), 'exec'), module)
    packet_bytes = packet_path.read_bytes()
    packet = module['decode'](packet_bytes.decode())
    if packet['schema'] != 'gomap-r1-lifecycle-packet-v3':
        raise ValueError('unsupported packet schema')
    if packet['source_before']['harness_files']['scripts/r1_lifecycle_validate.py'] != VALIDATOR_SHA256:
        raise ValueError('packet validator binding differs from frozen parser')
    if packet['source_before'] != packet['source_after']:
        raise ValueError('source changed during capture')
    config = packet['config']
    if config['execution_scope'] != module['SCOPE'] or config['schedule'] != module['SCHEDULE']:
        raise ValueError('wrong direct-backend scope/schedule')
    if len(packet['runs']) != config['repetitions']:
        raise ValueError('missing process record')
    runs = []
    for number, record in enumerate(packet['runs'], 1):
        # Narrow neighboring paths only; no alternate logs or rewritten payloads.
        if record['repetition'] != number or record['log'] != f'run-{number:03d}.log' or record['exit_code'] != 0:
            raise ValueError('wrong/failed raw process record')
        raw = (packet_path.parent / record['log']).read_bytes()
        if sha(raw) != record['log_sha256']:
            raise ValueError('raw log hash mismatch')
        parsed = module['raw_results'](raw.decode(), config, record['pid'])
        emitted = [module['decode'](line.split('R1_LIFECYCLE_RESULT ', 1)[1])
                   for line in raw.decode().splitlines() if 'R1_LIFECYCLE_RESULT ' in line]
        runs.append({'record': record, 'emitted_results': emitted, 'parsed': parsed,
                     'trajectory': trajectory(parsed['result']['census'])})
    if len({r['record']['pid'] for r in runs}) != len(runs):
        raise ValueError('duplicate process IDs')
    phases = [p['phase'] for p in runs[0]['parsed']['result']['census']]
    if any([p['phase'] for p in r['parsed']['result']['census']] != phases for r in runs):
        raise ValueError('process phase order differs')
    details = {'purpose': 'descriptive projection only; not validation or acceptance',
               'packet_sha256': sha(packet_bytes), 'validator_sha256': VALIDATOR_SHA256,
               'original_packet': packet, 'runs': runs}
    source = packet['source_before']
    lines = ['# Finite direct-backend lifecycle observations', '',
        '**Descriptive formatting only. This command does not qualify or accept evidence.**', '',
        f"[Original packet]({packet_link}) · [Independent validation log]({validation_log_link}) · [Full raw-value projection]({details_link})", '',
        f"Packet SHA256 `{sha(packet_bytes)}`. Producer label `{config['qualification']}` is copied, not endorsed.",
        f"Captured source `{source['commit']}`; runtime `{source['runtime_sha256']}`; harness `{source['harness_sha256']}`.",
        f"{len(runs)} fresh processes; {config['epochs']} final epochs/process; {config['documents']} live rows; {config['calls_per_epoch']} calls/epoch.",
        f"Working set repeats {config['working_set']['distinct_ids_per_epoch']} IDs/epoch. Recorded working-set policy: `{config['working_set']['policy']}`.", '',
        'The final benchmark results below exclude the Go calibration result. All emitted calibration and final JSON values, full file census, phase statistics, eligibility, debt, owner/options, and process observations are preserved in the linked projection and original logs. The projection keeps nested counters separate and changes no original packet fields.', '',
        'Direct `OptionsFor(ProfileCommandWALDurable)+OpenBackend`; command-WAL durable; background prune disabled. This finite hot-set fixture excludes the A public cached-wrapper overhead. Sampled heap is not RSS or an unsampled peak. Retained heap after GC includes live fixture/oracle maps and latency samples.', '',
        'Whole-call timers include encoding, full-row decode/oracle and callback bookkeeping. Epoch metrics additionally include preparation and latency/count/visited-ID tracking. Maintenance and phase oracles are excluded. Go allocation differences observe the process during the loop, including concurrent background activity; they are not exclusively attributed to user operations. Operation counts do not allocate mixed-call time to individual operations. No speed comparison, growth ratio, unlimited bound, or acceptance threshold is supplied.']
    table(lines, ['Process', 'PID', 'Raw log', 'Total mixed calls', 'Mixed call ns', 'Go bytes (loop)',
                  'Go objects (loop)', 'Mixed p95 ns/call', 'Mixed p99 ns/call',
                  'Sampled heap high B', 'Post-GC retained heap B'],
        ([r['record']['repetition'], r['record']['pid'],
          f"[log]({raw_root_link.rstrip('/')}/{quote(r['record']['log'])})"] +
         [r['parsed']['result'][k] for k in ['total_calls', 'call_ns', 'loop_bytes', 'loop_allocs',
             'p95_ns', 'p99_ns', 'sampled_heap_high_bytes', 'process_retained_heap_bytes']] for r in runs))
    lines += ['Cross-process phase medians follow. Component sizes are logical file lengths; redo WAL remains separate. All differences are computed within each process before taking medians. An absent predecessor is shown as —.']
    for label, getter in [
        ('Logical component bytes', lambda r, i: r['parsed']['result']['census'][i]['logical_bytes']),
        ('Adjacent-phase component byte differences', lambda r, i: r['trajectory'][i]['adjacent_bytes']),
        ('Component byte growth since that process\'s ingest', lambda r, i: r['trajectory'][i]['growth_from_ingest_bytes'])]:
        lines += ['', label + ':']
        table(lines, ['Phase'] + COMPONENTS,
            ([phase] + [None if getter(runs[0], i)[key] is None else
                statistics.median(getter(r, i)[key] for r in runs) for key in COMPONENTS]
                for i, phase in enumerate(phases)))
    table(lines, ['Phase', 'Median total bytes', 'Median regular files', 'Median adjacent total byte Δ',
                  'Median total byte growth since ingest'],
        ([phase, statistics.median(r['parsed']['result']['census'][i]['total_bytes'] for r in runs),
          statistics.median(r['parsed']['result']['census'][i]['total_files'] for r in runs),
          None if i == 0 else statistics.median(r['trajectory'][i]['adjacent_total_bytes'] for r in runs),
          statistics.median(r['trajectory'][i]['growth_from_ingest_total_bytes'] for r in runs)]
         for i, phase in enumerate(phases)))
    for r in runs:
        result = r['parsed']['result']
        lines += ['', f"<details><summary>Process {r['record']['repetition']}: complete phase trajectory and maintenance</summary>", '',
                  'All tables in this block are this process\'s actual cells. Heap cuts are census-boundary samples.']
        table(lines, ['Phase'] + COMPONENTS + ['Total B', 'Files', 'Adjacent total ΔB', 'Since ingest ΔB',
                    'HeapAlloc B', 'HeapInuse B', 'HeapObjects', 'NumGC'],
            ([c['phase']] + [c['logical_bytes'][k] for k in COMPONENTS] +
             [c['total_bytes'], c['total_files'], t['adjacent_total_bytes'], t['growth_from_ingest_total_bytes'],
              c['heap_alloc'], c['heap_inuse'], c['heap_objects'], c['num_gc']]
             for c, t in zip(result['census'], r['trajectory'])))
        table(lines, ['Operation', 'Calls'], sorted(result['operations'].items()))
        lines += ['API durations (ns); aggregate maintenance includes its constituent stages. These overlapping timer columns must not be added again. All other raw API timers and attribution remain in the projection.']
        table(lines, ['Epoch', 'Maintenance', 'Flush', 'Checkpoint', 'Fold', 'Fold checkpoint',
                      'Overlay', 'Overlay checkpoint', 'Vlog GC', 'Vacuum', 'Exhaustive plan', 'Exhaustive work',
                      'Final refresh', 'Final typed GC', 'Final leaf GC'],
            ([m['epoch']] + [m[k] for k in ['maintenance_ns', 'flush_ns', 'checkpoint_ns', 'fold_ns',
              'fold_checkpoint_ns', 'overlay_ns', 'overlay_checkpoint_ns', 'vlog_gc_ns', 'vacuum_ns']] +
              [m['full']['plan_ns'], m['full']['work_ns'], m['final']['refresh_ns'],
               m['final']['typed_gc_ns'], m['final']['leaf_gc_ns']] for m in result['maintenance']))
        typed_rows, leaf_rows, refresh_rows, debt_rows, vlog_rows, reclaim_rows = [], [], [], [], [], []
        for m in result['maintenance']:
            label = f"epoch-{m['epoch']}"
            typed_rows += [(label + '/before-fold', m['before_fold_gc']),
                           (label + '/reclaim-plan', m['reclaim']['plan_gc']),
                           (label + '/reclaim-GC', m['reclaim']['typed_gc'])]
            vlog_rows.append([label] + [m['vlog_gc'][k] for k in ['SegmentsTotal', 'SegmentsActive',
                'SegmentsReferenced', 'SegmentsProtected', 'SegmentsPending', 'SegmentsEligible',
                'SegmentsDeleted', 'BytesEligible', 'BytesDeleted']])
            reclaim_rows.append([label, m['reclaim']['decision']] + [m['reclaim'][k] for k in
                ['plan_ns', 'probe_ns', 'rewrite_ns', 'checkpoint_ns', 'gc_ns', 'candidate_refs']])
            debt_rows.append([label, m['full']['owner'], m['full']['options'],
                              {k: v for k, v in m['full']['work']['remaining_debt'].items() if not k.endswith('_ratio_ppm')},
                              m['full']['work']['fully_compacted'],
                              m['full']['work']['policy_fully_compacted'], m['full']['work']['byte_minimized']])
        release = result['after_view_release_gc']
        typed_rows += [('after-view-release/reclaim-plan', release['plan_gc']),
                       ('after-view-release/reclaim-GC', release['typed_gc'])]
        reclaim_rows.append(['after-view-release', release['decision']] + [release[k] for k in
                            ['plan_ns', 'probe_ns', 'rewrite_ns', 'checkpoint_ns', 'gc_ns', 'candidate_refs']])
        finals = [(f"epoch-{m['epoch']}", m['final']) for m in result['maintenance']]
        finals.append(('after-view-release', result['after_view_release_final']))
        for label, final in finals:
            typed_rows.append((label + '/final-GC', final['typed_gc']))
            leaf_rows.append([label, final['leaf_gc_ns']] + [final['leaf_gc'][k] for k in
                ['GenerationsEligible', 'GenerationsDeleted', 'FilesDeleted', 'BytesDeleted',
                 'ManifestRevisionGCUnsupported', 'ManifestRevisionsTotal', 'ManifestRevisionsProtected',
                 'ManifestRevisionsEligible', 'ManifestRevisionsDeleted', 'ManifestRevisionBytesDeleted']])
            for boundary in ['before', 'after']:
                s = final[boundary]
                refresh_rows.append([label, boundary, final['refresh_ns'], s['commit_seq'], s['user_root'],
                    s['system_root'], s['applied_lsn'], s['next_lsn'],
                    s['slots']['treedb.durable_root.selected_slot'],
                    s['slots']['treedb.durable_root.slot0.commit_seq'], s['slots']['treedb.durable_root.slot1.commit_seq']])
        lines += ['Typed reachability and unlink attribution are separate actual API cells. Sources/ref/segment classes may overlap; they are not unique retained bytes. No-op work is not reclamation.']
        table(lines, ['Stage', 'Eligible segments', 'Deleted segments', 'Retained segments',
                      'Eligible B', 'Deleted B', 'Retained B', 'Protected refs', 'Protected ref B',
                      'Mapped handles', 'Pinned refs', 'Pinned B', 'Rewrite debt B'],
            ([label] + [s[k] for k in ['SegmentsEligible', 'SegmentsDeleted', 'SegmentsRetained',
                'BytesEligible', 'BytesDeleted', 'BytesRetained']] +
             [s['Plan']['Refs']['Protected'], s['Plan']['Refs']['BytesProtected'],
              s['Plan']['MappedResources']['ActiveHandles'], s['Plan']['MappedResources']['PinnedRefs'],
              s['Plan']['MappedResources']['PinnedBytes'], s['Plan']['RewriteDebtBytes']] for label, s in typed_rows))
        table(lines, ['Stage', 'Decision', 'Plan ns', 'Probe ns', 'Rewrite ns', 'Checkpoint ns', 'GC ns', 'Candidate refs'], reclaim_rows)
        table(lines, ['Epoch', 'Vlog segments', 'Active', 'Referenced', 'Protected', 'Pending', 'Eligible',
                      'Deleted', 'Eligible B', 'Deleted B'], vlog_rows)
        table(lines, ['Final stage', 'Leaf GC ns', 'Eligible generations', 'Deleted generations', 'Files deleted',
                      'Leaf bytes deleted', 'Revision GC unsupported', 'Revisions total', 'Protected revisions',
                      'Eligible revisions', 'Deleted revisions', 'Revision bytes deleted'], leaf_rows)
        lines += ['Refresh snapshots expose both durable slots, root IDs and command coverage. Duration repeats across its before/after pair; count it once. Full root records and all slot metadata remain in the projection.']
        table(lines, ['Stage', 'Boundary', 'Refresh ns', 'CommitSeq', 'User root', 'System root', 'AppliedLSN',
                      'NextLSN', 'Selected slot', 'Slot0 CommitSeq', 'Slot1 CommitSeq'], refresh_rows)
        lines += ['Recorded exhaustive maintenance owner, work limits and remaining debt follow. Work limits are not storage-capacity bounds. An internal replay-inline owner classification does not change the public direct-backend opener. All audit/rewrite/leaf/index phases and counters remain in the projection. Existing raw ratio fields are retained there rather than interpreted in these tables.']
        table(lines, ['Epoch', 'Owner', 'Options / work limits', 'Remaining debt', 'Fully compacted',
                      'Policy fully compacted', 'Byte minimized'], debt_rows)
        lines += ['', '</details>', '']
    lines += ['Held-view assertions, release/lifetime checks, actual deleted-asset checks and final reopen row/posting parity are performed by the benchmark source. This formatter checks the emitted raw schema and log hashes, not those assertions independently. Zero active handles immediately after closing the held view is a source assertion, not a new emitted sample.',
        'The independently pinned frozen validator and coordinator own qualification. The linked validation log is supplied by the caller; this formatter does not infer its verdict. Sampled trajectories establish finite observations only; lawful current/old-view/recovery retention and maintenance debt remain explicit, with no infinite-growth or total physical-bound claim.', '']
    return '\n'.join(lines), details


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('packet', type=Path)
    p.add_argument('--validator', type=Path, required=True)
    for name in ['packet-link', 'raw-root-link', 'validation-log-link', 'details-link']:
        p.add_argument('--' + name, required=True)
    p.add_argument('--details-out', type=Path, required=True)
    a = p.parse_args()
    # Fresh derived output only, never replace an original or earlier projection.
    if a.details_out.exists():
        p.error('--details-out must be a new file')
    try:
        text, details = run(a.packet, a.validator, a.packet_link, a.raw_root_link,
                            a.validation_log_link, a.details_link)
        with a.details_out.open('x') as f:
            json.dump(details, f, indent=2, sort_keys=True, allow_nan=False)
            f.write('\n')
    except (ValueError, KeyError, OSError, AssertionError) as e:
        p.exit(1, f'descriptive formatting failed: {e}\n')
    print(text)


if __name__ == '__main__':
    main()
