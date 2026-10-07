#!/usr/bin/env python3
"""Summarize already validated R1 raw cells without discarding noisy results."""
import json
import statistics
import sys


def summarize(packet):
    groups = {}
    rejected = []
    for cell in packet['cells']:
        if cell.get('unsupported'):
            rejected.append({'engine': cell['engine'], 'repetition': cell['repetition'],
                             'unsupported': cell['unsupported'], 'rejection': cell['rejection']})
            continue
        for phase in [cell['state_transition'], cell['setup'], cell['warmup'], *cell['phases']]:
            if phase.get('skipped'):
                continue
            groups.setdefault((cell['engine'], phase['name']), []).append(phase)
    summaries = []
    for (engine, phase), rows in sorted(groups.items()):
        values = [row['ops_per_sec'] for row in rows]
        median = statistics.median(values)
        spread = (max(values) - min(values)) / median
        latencies = [row["p50_ns"] for row in rows]
        mean_latency = statistics.mean(latencies)
        cv = statistics.stdev(latencies) / mean_latency if len(latencies) > 1 and mean_latency > 0 else 0.0
        summaries.append({'engine': engine, 'phase': phase, 'repetitions': len(rows),
                          'ops_per_sec_median': median, 'ops_per_sec_min': min(values),
                          'ops_per_sec_max': max(values), 'throughput_spread': spread,
                          'p50_cv': cv,
                          'noise_status': 'inconclusive' if spread > 0.15 or cv > 0.10 or mean_latency <= 0 else 'within_limits',
                          'ns_per_op_median': statistics.median(r['ns_per_op'] for r in rows),
                          'bytes_per_op_median': statistics.median(r['bytes_per_op'] for r in rows),
                          'allocs_per_op_median': statistics.median(r['allocs_per_op'] for r in rows)})
    return {'schema': packet['schema'], 'qualification': packet['config']['qualification'],
            'source': packet['source'], 'fixture_sha256': packet['fixture_sha256'],
            'noise_rule': 'inconclusive if throughput (max-min)/median > 0.15 or sample CV(p50) > 0.10; preserve all raw cells',
            'summaries': summaries, 'rejected_capabilities': rejected}


if __name__ == '__main__':
    with open(sys.argv[1], encoding='utf-8') as source:
        packet = json.load(source)
    json.dump(summarize(packet), sys.stdout, indent=2, allow_nan=False)
    print()
