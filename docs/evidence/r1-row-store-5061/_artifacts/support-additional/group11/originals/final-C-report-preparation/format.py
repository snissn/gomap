#!/usr/bin/env python3
"""Render an externally supplied C summary; formatting never accepts evidence."""
import argparse
import hashlib
import itertools
import json
import math
from pathlib import Path
import re
import sys

SCHEMA = 'gomap-r1-mutation-sweep-summary-v1'
DURABILITY = [
    ('Append calls', 'treedb.command_wal.append.count_total'),
    ('Logical sync calls', 'treedb.command_wal.sync.count_total'),
    ('Physical sync calls', 'treedb.command_wal.file_sync.calls_total'),
    ('WAL bytes', 'treedb.command_wal.write.bytes_total'),
    ('Physical sync ns', 'treedb.command_wal.file_sync.ns_total'),
    ('Logical sync ns', 'treedb.command_wal.sync.ns_total'),
]
PUBLICATION = [
    ('Update calls', 'treedb.collections.write_domain.update_batch.calls_total'),
    ('Update rows', 'treedb.collections.write_domain.update_batch.items_total'),
    ('Current read ns', 'treedb.collections.write_domain.update_batch.current_read_ns_total'),
    ('Prepare ns', 'treedb.collections.write_domain.update_batch.prepare_ns_total'),
    ('Publish ns', 'treedb.collections.write_domain.update_batch.publish_ns_total'),
    ('Indexed Flush calls', 'treedb.collections.write_domain.indexed_flush.calls_total'),
    ('Indexed Flush publish ns', 'treedb.collections.write_domain.indexed_flush.publish_ns_total'),
]
MEDIANS = ['ns_per_op', 'bytes_per_op', 'allocs_per_op', 'p50_ns', 'p95_ns', 'p99_ns']
GROUP_NUMBERS = ['requests_per_sec_median', 'rows_per_sec_median',
                 'throughput_spread', 'explicit_flush_ns_median', 'explicit_flush_spread']


def number(value):
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
        raise ValueError('presentation field must be a finite nonnegative number')
    return value


def integer(value):
    if type(value) is not int or value <= 0:
        raise ValueError('presentation count must be a positive integer')
    return value


def link(label, target):
    if not target or any(c in target for c in '\r\n<>'):
        raise ValueError('provide a nonempty Markdown link target without newline or angle brackets')
    return f'[{label}](<{target}>)'


def fmt(value):
    value = number(value)
    if type(value) is int:
        return f'{value:,}'
    return f'{value:,.3f}'.rstrip('0').rstrip('.')


def table(headers, rows):
    return '\n'.join(['| ' + ' | '.join(headers) + ' |',
                      '| ' + ' | '.join(['---'] * len(headers)) + ' |',
                      *['| ' + ' | '.join(map(str, row)) + ' |' for row in rows]])


def dimensions(group):
    return [group['bio_bytes'], group['request_rows'],
            'yes' if group['indexed'] else 'no', group['change']]


def render(summary, raw, summary_link, packet_link):
    if summary['schema'] != SCHEMA:
        raise ValueError('unsupported summary schema')
    config = summary['config']
    for key in ['documents', 'operations', 'repetitions']:
        integer(config[key])
    if config['qualification'] not in ('retained', 'rehearsal'):
        raise ValueError('unknown producer qualification label')
    source = summary['source']
    for key, width in [('commit', 40), ('runtime_sha256', 64), ('harness_sha256', 64)]:
        if not re.fullmatch('[0-9a-f]{' + str(width) + '}', source[key]):
            raise ValueError('missing/malformed source identity')
    packet_sha = summary['original_packet_sha256']
    if not re.fullmatch('[0-9a-f]{64}', packet_sha):
        raise ValueError('missing/malformed original packet byte hash')
    groups = summary['summaries']
    keys = []
    counter_keys = {key for _, key in DURABILITY + PUBLICATION}
    for group in groups:
        if type(group['indexed']) is not bool:
            raise ValueError('indexed must be boolean')
        key = (group['bio_bytes'], group['request_rows'], group['indexed'], group['change'])
        if type(key[0]) is not int or type(key[1]) is not int:
            raise ValueError('width and request rows must be integers')
        keys.append(key)
        if integer(group['repetitions']) != config['repetitions']:
            raise ValueError('summary repetition count differs from supplied config')
        if len(group['original_repetitions']) != config['repetitions']:
            raise ValueError('original repetitions must remain available in supplied summary')
        for name in MEDIANS:
            number(group['acknowledgement_medians'][name])
        for name in GROUP_NUMBERS:
            number(group[name])
        for name in ['noise_status', 'explicit_flush_noise_status']:
            if group[name] not in ('within_15pct', 'inconclusive'):
                raise ValueError('unknown supplied noise label')
        for phase in ['ack_counter_delta', 'flush_counter_delta']:
            counters = group['aggregate_counter_delta_medians'][phase]
            if set(counters) != counter_keys:
                raise ValueError('complete exact thirteen-counter inventory required in each phase')
            for value in counters.values():
                number(value)
    expected = set(itertools.product([96, 4096], [1, 32], [False, True], ['bio', 'email_city']))
    if len(keys) != 16 or set(keys) != expected:
        raise ValueError('complete unique sixteen-cell matrix required')
    groups = sorted(groups, key=lambda g: (g['bio_bytes'], g['request_rows'], g['indexed'], g['change']))
    dim_headers = ['Bio bytes', 'Request rows', 'Indexed', 'Changed fields']
    rows = []
    for g in groups:
        m = g['acknowledgement_medians']
        rows.append(dimensions(g) + [fmt(m['ns_per_op']), fmt(g['requests_per_sec_median']),
                    fmt(g['rows_per_sec_median']), fmt(m['bytes_per_op']), fmt(m['allocs_per_op']),
                    fmt(m['p50_ns']), fmt(m['p95_ns']), fmt(m['p99_ns']),
                    f"{fmt(100 * g['throughput_spread'])}% ({g['noise_status']})",
                    fmt(g['explicit_flush_ns_median']),
                    f"{fmt(100 * g['explicit_flush_spread'])}% ({g['explicit_flush_noise_status']})"])
    parts = [
        'Generic UpdateBatch width and request-size sweep — supplied summary.',
        'Formatting only: this report does not validate original packets, verify landing/receipts, or grant acceptance. '
        f"Producer label: `{config['qualification']}`. Population: {config['documents']:,}; "
        f"serial requests per cell/repetition: {config['operations']:,}; fresh-DB repetitions per cell: {config['repetitions']}.",
        f"Source commit: `{source['commit']}`. Runtime SHA256: `{source['runtime_sha256']}`. "
        f"Harness SHA256: `{source['harness_sha256']}`.",
        link('Supplied raw summary, including every original repetition', summary_link) + '; ' +
        link('Original raw packet', packet_link) + '. '
        f"Supplied summary byte SHA256: `{hashlib.sha256(raw).hexdigest()}`. "
        f"Original packet byte SHA256 as recorded by supplied summary: `{packet_sha}`.",
        'Each value below is supplied by the reviewed summarizer. ACK ns/request is the median of per-repetition '
        'mean ACK costs. Requests/s is the median of per-repetition request throughput, not the reciprocal of median ACK cost. '
        'Rows/s uses actual request rows. p50/p95/p99 are medians of per-repetition percentiles, not pooled percentiles. '
        'Go bytes and objects are per acknowledged request and include process background/observer work; they are not owned memory. '
        'ACK spread is relative request-throughput spread; Flush spread is relative explicit-Flush-duration spread. '
        'The supplied >15% inconclusive labels remain visible; within_15pct is not an acceptance decision.',
        table(dim_headers + ['Median ACK ns/request', 'Requests/s', 'Rows/s', 'Go B/request', 'Objects/request',
                            'Median rep p50 ns', 'Median rep p95 ns', 'Median rep p99 ns', 'ACK spread/status',
                            'Median explicit Flush ns', 'Flush spread/status'], rows),
        'Counter tables retain unnormalized medians of aggregate per-repetition deltas. ACK covers the entire serial '
        'request loop; Flush covers the separate explicit Flush boundary after that loop. Calls, rows, bytes and ns are '
        'aggregate totals per repetition, not per-request or per-row estimates. Nested timers overlap and must not be added '
        'into a total. Async/default-maintenance work may contribute across boundaries. Zero is a supplied observed delta; '
        'missing counters are refused, never filled with zero.',
    ]
    for phase, label in [('ack_counter_delta', 'ACK'), ('flush_counter_delta', 'Flush')]:
        for columns, scope in [(DURABILITY, 'WAL/sync'), (PUBLICATION, 'publication')]:
            parts += [f'{label} {scope}: medians of aggregate per-repetition deltas.',
                      table(dim_headers + [name for name, _ in columns],
                            [dimensions(g) + [fmt(g['aggregate_counter_delta_medians'][phase][key])
                                              for _, key in columns] for g in groups])]
    parts.append('Scope is generic UpdateBatch costs for this supplied finite serial fixture. No native partial-setter, '
                 'metadata-reference-only mutation, larger-population, concurrency, before/after optimization ratio, '
                 'or storage-capacity qualification follows. Rehearsal values remain historical diagnostics. '
                 'Retained acceptance requires the existing frozen validator and separate coordinator-observed receipts.')
    return '\n\n'.join(parts) + '\n'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('summary', type=Path)
    parser.add_argument('--summary-link', required=True)
    parser.add_argument('--packet-link', required=True)
    args = parser.parse_args()
    raw = args.summary.read_bytes()
    # No partial output on missing fields or malformed presentation input.
    text = render(json.loads(raw), raw, args.summary_link, args.packet_link)
    sys.stdout.write(text)


if __name__ == '__main__':
    try:
        main()
    except (ValueError, KeyError, TypeError) as error:
        raise SystemExit(f'format refused: {error}')
