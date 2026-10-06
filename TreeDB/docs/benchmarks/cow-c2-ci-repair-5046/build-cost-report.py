"""Render only complete analyzer-accepted bounded C2 packets for publication."""
import argparse
import json
from pathlib import Path


def read(packet, name):
    return json.loads((packet / name).read_text())


def cell(metrics):
    ns = metrics['ns/op']['median']
    return f"{ns:,.0f} / {1e9 / ns:,.0f} / {metrics['B/op']['median']:,.0f} / {metrics['allocs/op']['median']:g}"


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--rawkv', type=Path, required=True)
    p.add_argument('--mvcc', type=Path, required=True)
    p.add_argument('--out', type=Path, required=True)
    p.add_argument('--candidate', required=True)
    args = p.parse_args()
    rv, mv = read(args.rawkv, 'analysis-validation.json'), read(args.mvcc, 'analysis-validation.json')
    assert (rv['raw_runs'], rv['rows'], rv['matched_cases']) == (27, 540, 180)
    assert (mv['raw_runs'], mv['rows'], mv['matched_cases']) == (18, 108, 36)
    assert rv['raw_hashes_verified'] and rv['stderr_hashes_verified']
    assert mv['raw_hashes_verified'] and mv['stderr_hashes_verified']
    ri, mi = read(args.rawkv, 'source-identity.json'), read(args.mvcc, 'source-identity.json')
    assert rv['source_tree_sha256'] == mv['source_tree_sha256']
    assert ri['commit'] == mi['commit'] == args.candidate
    assert ri['source_tree_sha256'] == mi['source_tree_sha256'] == rv['source_tree_sha256']
    raw, integrated = read(args.rawkv, 'matched-summary.json'), read(args.mvcc, 'matched-summary.json')
    modes = ('append_only', 'btree', 'cow_btree')
    lookup = {(r['profile'], r['layout'], r['records'], r['operation'], r['mode']): r for r in raw}
    ilookup = {(r['profile'], r['read'], r['iterations'], r['mode']): r for r in integrated}
    lines = [
        '# C2 repaired cached-cut cost diagnostic (#5046)', '',
        'These are bounded integration measurements with three retained repeats. They establish neither statistical significance nor sustained C4 qualification. The C3 MVCC Store fences remain in place. This report must be read with the coordinator cost disposition and final source applicability evidence.', '',
        f"Runtime/harness candidate: `{args.candidate}`. Complete source-manifest identity: `{rv['source_tree_sha256']}`. Both packets use the identical frozen source. Linux 185, Intel Core i5-11400F, six cores/twelve threads, Linux 6.8, Go 1.26.3, GOMAXPROCS 12. GOGC/GOMEMLIMIT overrides were cleared and recorded. Owned validation/collection was serialized; process/load receipts are retained privately with the full runner packets.", '',
        'The published packets retain stdout and stderr separately, receipts, source/binary/environment bindings, matched summaries and all samples. Analyzers require all 45 successful runs, 648 rows, exact cases/iterations, raw-stream hashes, no source drift, complete public-history outputs and zero COW snapshot rotations. The rawKV packet additionally verifies the actual retained cache authority after final Close: every live charge/count is zero and the historical peak is positive. Failed attempts are recorded separately; they are excluded from acceptance and never replaced with success.', '',
        '## Raw KV dirty-cache costs', '',
        'Values below are the median across three fixed-count runs, in **ns/op / ops/s / B/op / allocs/op**. Every timed latency includes the same clock overhead and correctness checks. Fresh capture and explicit-sync writes use 16 operations; owned point hits, forward16 and incremental writes use 128 operations. DirtyCheckpoint is not collected in this current repair packet; earlier checkpoint measurements remain historical. Point misses are not measured.', '',
        'All modes use identical profiles, two shards, N1024/N2048, inline 64 bytes or forced-pointer 4096 bytes, disabled automatic maintenance and a 1 GiB flush threshold. Pointer data is deliberately compressible and uses default compression. Fresh capture reseeds dirty entries outside the timer. Other reads warm one cut outside the timer. Incremental writes replace existing keys without a warm capture; the first explicit sync includes seeded dirty data. Final Close/setup/layout validation are outside timing.', '',
        '| Profile / layout / N / operation | append_only | btree | cow_btree |',
        '| --- | ---: | ---: | ---: |'
    ]
    keys = sorted({(r['profile'], r['layout'], r['records'], r['operation']) for r in raw if r['records'] == 2048 or r['operation'] == 'FreshCapture'})
    for key in keys:
        label = ' / '.join(str(x) for x in key)
        cells = [cell(lookup[(*key, mode)]['metrics']) for mode in modes]
        lines.append('| ' + label + ' | ' + ' | '.join(cells) + ' |')
    lines += ['', 'All N1024/N2048 cases and repeats are in `rawkv/matched-summary.json`; paired COW/comparator and N2048/N1024 ratios are retained without sample filtering. Ratio spread and the small 16/128 sample percentile sets are descriptive, not confidence intervals or production tails.', '',
              '## Actual public MVCC diagnostic', '',
              'This uses public Open→mvcc.New→CommitAt(CommitRelaxed) followed by GetAt or complete exact-key version iteration. Eight logical keys, 128 byte values, eight shards, 16 MiB flush threshold, side stores/background checkpoint off. The write acknowledgement follows the chosen profile; CommitRelaxed is not an explicit-sync promise. The timed boundary includes commit, read/scan, validation and clocks. N128/N256 are growing histories, not steady-state workloads.', '',
              'For a point read output/op is 1. Full history requires every version in timestamp order with visited=retained=output, skipped 0 and no error; average output/op is 8.5 at 128 operations and 16.5 at 256 operations. This prevents an incomplete scan from appearing faster.', '',
              '| Profile / read / operations | append_only | btree | cow_btree |',
              '| --- | ---: | ---: | ---: |']
    for key in sorted({(r['profile'], r['read'], r['iterations']) for r in integrated}):
        cells = [cell(ilookup[(*key, mode)]['metrics']) for mode in modes]
        lines.append('| ' + ' / '.join(str(x) for x in key) + ' | ' + ' | '.join(cells) + ' |')
    lines += ['', '## Memory and cost interpretation', '',
              'B/op and allocs/op describe Go allocations within each benchmark timing boundary. COW engine charges/publication counters are whole-fixture snapshots outside timing, including seeding/reseeds and the final observed cut. They are not per-operation heap use. Peak/retired/pinned charges are bounded engine accounting; process maximum RSS includes fixture setup/teardown and all cases in the child process. Public-MVCC process_alloc includes diagnostic work outside its timer. No charge/RSS equivalence is claimed.', '',
              'The COW mode is opt-in. Capture gains do not erase incremental-write, iterator or pointer-decode costs. Material regressions require source/profile investigation, minimization and explicit coordinator disposition. The final completion packet records that decision; this table alone cannot authorize merge or default promotion.', '',
              '## Retained failures and repaired collection', '',
              'The earlier af59 packet stopped on a reproduced NoWAL final Close EOF during multi-chunk leaf publication. Zero final charges did not prove final seed persistence. A source-bound overwrite→Close→reopen regression and barrier-lifetime repair address that failure. The old collector also overlapped btree/cow_btree filters and merged logger stderr into benchmark stdout; its incomplete/corrupt packet and review erratum remain retained. This packet uses component-anchored filters, separate streams and per-run exact-case validation.', '',
              '## Reproduction and scope', '',
              'Both standalone fixtures are Go package benchmarks, not unified-bench/benchprof profile-dir inputs. See cmd/unified_bench/README.md and cmd/benchprof/README.md for their names and fixed-count commands. The retained collector/analyzer scripts reproduce the full matrix with a new immutable archive/manifest/output directory and exclusive runner handoff. Frozen runtime/harness identity and final artifact-only descendant applicability are part of the packet.', '',
              'Future C4 work must qualify matched sustained retention/read economics, negative lookups, long-lived readers, reclamation, checkpoint/storage growth and relevant tails on the landed harness. These tiny diagnostics do not establish those outcomes.', '']
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text('\n'.join(lines))
    print(json.dumps({'report': str(args.out), 'rawkv_table_rows': len(keys), 'mvcc_table_rows': 12}))


if __name__ == '__main__':
    main()
