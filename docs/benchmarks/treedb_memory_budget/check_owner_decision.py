#!/usr/bin/env python3
"""Verify the committed decision tables against hash-pinned retained analyses; no Go runs."""
import argparse
import hashlib
import json
from pathlib import Path
import re

BASE = 'a51b4795cebc194c7a158b262242668877932934'
TRIAL = 'ac476e43addd04cb307357ba9abab10c601d39da'
HASHES = {
    'memory-full-analysis-output.json': 'e2c59b89f41827ce4ed0c69e8b083a28c9137f4a6061b8738fe0dcc23cd3a10f',
    'checkpoint-paired-4915-analysis-v2-output.json': '23dcc8d798882329d7e572617c83f614eedb56367142ea9ad908c50025d2ac01',
    'checkpoint-paired-4915-five-repeat-compact-summary.json': '65b62c98adf151f197c50a7bf4653ae99ac4b5227ddc331a18d00c1237ceb7cb',
    'memory-full-owner-decision-review.md': 'cf403785770076762ed2c12b3fb82735ee84ce7b92f327ff99449dc96ab2312f',
    '4915-independent-source-review.md': '8cc9baffcb7144181ff8486191d170977feb22a58b4a2837a1fccccf0769d74a',
    '4914-measured-disposition-independent-review.md': '53b791781695abb3b708f882c3d6755cf1114d2e57bec4bdf7438a4694f8f6c8',
    '4915-phase-cpu-independent-attribution-review.md': '06c4c8f43cf21a781a4c0e6dffd8b4a2d568a0a3f9d928573065194746aaaca1',
}
AFTER = 'phase/read_gc2/after/treedb.'

def table(headers, rows):
    return '\n'.join(['| ' + ' | '.join(headers) + ' |', '| ' + ' | '.join(['---'] * len(headers)) + ' |'] +
                     ['| ' + ' | '.join(map(str, row)) + ' |' for row in rows])

def render(original, paired):
    assert original['runtime_head'] == BASE and len(original['cells']) == 60
    assert len(original['cohorts']) == 20
    assert paired['canonical_retained_cells_validated'] == 70 and paired['repeats'] == 5
    assert paired['products']['control']['head'] == BASE and paired['products']['candidate']['head'] == TRIAL
    owner_rows, placement_rows, heap_rows, guard_rows = [], [], [], []
    for cohort, metrics in original['cohorts'].items():
        def m(key, scale=1):
            stat = metrics[key]
            assert stat['n'] == 3 and len(stat['values']) == 3
            return f"{stat['median'] / scale:.2f}"
        owner_rows.append([cohort, m('phase/read_gc2/heap_bytes', 2**20),
            m(AFTER+'process.memory.rss_bytes', 2**20), m(AFTER+'process.memory.rss_hwm_bytes', 2**20),
            m(AFTER+'process.append_only.entry_pool_retained_bytes_estimate', 2**20),
            m(AFTER+'process.append_only.mem_lease_entry_backing_bytes', 2**20),
            m(AFTER+'process.read_path.outer_leaf.cache.bytes', 2**20),
            m(AFTER+'vlog.grouped_frame_cache.retained_bytes', 2**20)])
        placement_rows.append([cohort] + [m('phase/'+phase+'/'+metric, scale) for phase, metric, scale in [
            ('owned_get_sweep','elapsed_ns_per_op',1000),('reused_append_sweep','elapsed_ns_per_op',1000),
            ('owned_get_sweep','allocated_bytes_per_op',1),('reused_append_sweep','allocated_bytes_per_op',1),
            ('load_sync','elapsed_ns_per_op',1000),('updates_sync','elapsed_ns_per_op',1000),
            ('load_sync','allocated_bytes_per_op',1),('updates_sync','allocated_bytes_per_op',1)]])
    for cohort, metrics in paired['cohorts'].items():
        for stat in metrics.values():
            for product in ('control', 'candidate'):
                assert stat[product]['n'] == 5 and len(stat[product]['values']) == 5
            assert len(stat['paired_change_pct']) == 5
        heap = metrics['phase/read_gc2/heap_bytes']
        heap_rows.append([cohort, f"{heap['control']['median']/2**20:.2f}",
            f"{heap['candidate']['median']/2**20:.2f}",
            f"{(heap['candidate']['median']-heap['control']['median'])/2**20:+.2f}"])
        for key in ('phase/updates_sync/elapsed_ns_per_op', 'phase/update_checkpoint/elapsed_ns_per_op',
                    'phase/updates_sync/allocated_bytes_per_op', 'phase/updates_sync/allocations_per_op'):
            stat = metrics[key]
            guard_rows.append([cohort, key.removeprefix('phase/'),
                f"{stat['control']['median']:.4f}", f"{stat['candidate']['median']:.4f}",
                f"{stat['median_change_pct']:+.2f}",
                f"{stat['control']['range_over_abs_median_pct']:.2f}/{stat['candidate']['range_over_abs_median_pct']:.2f}",
                f"{sum(x > 0 for x in stat['paired_change_pct'])}/5"])
    return '\n\n'.join([
        table(['Original60 cohort','Heap MiB','RSS MiB','HWM MiB','Free bins MiB','DB idle backing MiB','Leaf MiB','Frame MiB'],owner_rows),
        table(['Original60 cohort','Get µs/op','Append µs/op','Get B/op','Append B/op','Load µs/op','Update µs/op','Load B/op','Update B/op'],placement_rows),
        table(['Paired70 cohort','Control heap MiB','Trial heap MiB','Trial−control MiB'],heap_rows),
        table(['Paired70 cohort','Metric','Control median','Trial median','Change %','Spread C/T %','Positive paired changes'],guard_rows)])

def main():
    if not __debug__:
        raise RuntimeError('validation requires Python assertions; run without -O')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence-root', required=True, type=Path)
    parser.add_argument('--print-tables', action='store_true', help='print expected tables for editorial use')
    args = parser.parse_args()
    doc = Path(__file__).with_name('OWNER_DECISION.md')
    text = doc.read_text(encoding='utf-8')
    for name, expected in HASHES.items():
        assert hashlib.sha256((args.evidence_root/name).read_bytes()).hexdigest() == expected, name
        assert [line for line in text.splitlines() if line.startswith(f'| `{name}` |')] == [f'| `{name}` | `{expected}` |'], f'wrong documented artifact row: {name}'
    original = json.loads((args.evidence_root/'memory-full-analysis-output.json').read_text(encoding='utf-8'))
    paired = json.loads((args.evidence_root/'checkpoint-paired-4915-five-repeat-compact-summary.json').read_text(encoding='utf-8'))
    full = json.loads((args.evidence_root/'checkpoint-paired-4915-analysis-v2-output.json').read_text(encoding='utf-8'))
    assert len(full['runs']) == 70
    for cohort, metrics in paired['cohorts'].items():
        for key, stat in metrics.items():
            assert stat == full['cohorts'][cohort][key], (cohort, key)
    expected = render(original, paired)
    if args.print_tables:
        print(expected)
        return
    actual = text.split('<!-- canonical-tables:start -->\n',1)[1].split('\n<!-- canonical-tables:end -->',1)[0]
    assert actual == expected, 'canonical table differs'
    repo = Path(__file__).resolve().parents[3]
    for target in re.findall(r'\]\(([^)#]+)(?:#[^)]*)?\)', text):
        if not target.startswith('https://'):
            assert (doc.parent/target).resolve().exists(), target
    for relative, targets in [
        ('TreeDB/docs/guides/typed-storage-performance.md', ['../../../docs/benchmarks/treedb_memory_budget/OWNER_DECISION.md', '../../../docs/benchmarks/treedb_memory_budget/README.md']),
        ('docs/benchmarks/treedb_memory_budget/README.md', ['OWNER_DECISION.md'])]:
        page = repo/relative
        for target in targets:
            assert ']('+target+')' in page.read_text(encoding='utf-8') and (page.parent/target).resolve().exists(), target
    assert hashlib.sha256((repo/'TreeDB/memory_budget_bench_test.go').read_bytes()).hexdigest() == \
        '03b389e475374af7157111b4a8feea72a22124aa4fec5f3342d512ea6cbad462'
    pool = (repo/'TreeDB/internal/memtable/append_only.go').read_text(encoding='utf-8')
    caching = (repo/'TreeDB/caching/db.go').read_text(encoding='utf-8')
    assert 'appendOnlyEntryPoolRetainBudgetBytes = uint64(256 << 20)' in pool
    assert 'TrimAppendOnlyEntryPoolsToTargetBytes' not in pool
    for name, value in [('postCheckpointBatchArenaTargetBytes','int64(32 << 20)'),
                        ('postCheckpointEntrySliceTargetBytes','int64(32 << 20)'),
                        ('postCheckpointAppendOnlyMemLeaseKeep','8'),('postFlushAppendOnlyMemLeaseKeep','24')]:
        assert re.search(r'\b'+name+r'\s*=\s*'+re.escape(value)+r'\b' if value.isdigit() else
                         r'\b'+name+r'\s*=\s*'+re.escape(value), caching), name
    print('PASS: pinned analyses/reviews; 20x3 owner/placement and 7x5x2 trial tables; local links; existing owner policy')

if __name__ == '__main__':
    main()
