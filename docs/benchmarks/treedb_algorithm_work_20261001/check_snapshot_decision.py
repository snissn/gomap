#!/usr/bin/env python3
"""Check the snapshot decision against pinned retained packets; no runtime execution."""
import argparse
import hashlib
import json
from pathlib import Path
import statistics

HEAD = 'e3e912be5501083b9ccb6ae32761bc789cfb21c0'
TREE = 'cce8112f79ac0a765d4612e8382ce68cc2d8ab84'
HASHES = {
    '4916-e3-independent-matched-decision-review.md': '5fbd05c848d07ef8875a316a73d67852899d291941e2409eac6887f4ca43cd17',
    '4916-e3-three-repeat-analysis.json': 'e9d8658b70843133d88e16d25fd2942c6efcd2d6aa067ab55ffc6e4e3c1233cb',
    '4916-e3-matched-pre-timer-plans.json': '1cbbec8c8090dd33a0c4a02d1e0419588440226c0af92c8ce8b7e9b53aae7662',
    '4916-e3-eligibility-plan.json': 'af0d26d95ae77830762bf32f8a41a4e9d073b238f422d5c43fd32601cfe996e1',
    '4916-e3-matched-r1-plan.json': '578a10e57068063e4cb1f651d9d8eb5242b1fbe56374405d5d7b7fd4fa296f4a',
    '4916-e3-matched-r2-plan.json': '92dc022aa697c77f08a947e0cb36f0424bf7d5fcc39e67e02c5d9a13f148fda3',
    '4916-e3-matched-r3-plan.json': '5e35ee6672626f49f4c3db1486749031793ec016414904f650f321b4d9cb6f5f',
    '4916-e3-full-packet/normal-prepared/freeze.json': '67b1a3b58251d24a8f801e90800164f18d98a6d0135f94baebead83a0c103248',
    '4916-e3-full-packet/preparation-anchor.json': 'f9c813e2ceebbd8b2317f4495d598150340b44b1e5247302e1d9abc4e5a3f80d',
    '4916-e3-full-packet/snapshot-rotations-eligibility-v1/validated.json': 'abcbd0eeb488843e7d57c838750a61155ddde034aba4fac0e1870ca0bdf68d6c',
    '4916-e3-full-packet/snapshot-rotations-eligibility-v1/capture/execution.json': 'd8dc40157c1a1227b7b5b7abe351281bdf4e1ff51c7826f1f7e85c4393884e44',
    '4916-e3-full-packet/snapshot-rotations-eligibility-v1/capture/stdout.log': '6b7ba4a6c1377d9449d3f208a73cc293b3e28587c443a978b288c5ae88c4d32d',
    '4916-e3-full-packet/snapshot-rotations-eligibility-v1/capture/stderr.log': 'ec421cd9209c6e5ba29ccc62fbaa72358b53b901176f44fdd5b66986f98ad3d3',
    '4916-e3-matched-packet/snapshot-rotations-matched-r1/validated.json': '3f076ffe81924207d982ee1a3f54fa8cd2dd9d9f678aacf8f355837d036c8fc3',
    '4916-e3-matched-packet/snapshot-rotations-matched-r1/capture/execution.json': '7875c6bfe2a9da9045d49e90f5a0233cda4b5082b9a25becd2c112661e068a6f',
    '4916-e3-matched-packet/snapshot-rotations-matched-r1/capture/stdout.log': '312b7fb80a0706c970d67ad17f06cf7f0577ee417f737d40aa972f0a3bd7adaa',
    '4916-e3-matched-packet/snapshot-rotations-matched-r1/capture/stderr.log': '410bf3325d1f9a0dcaa9e571c9438abdc1427fe9db784cb8b7c7aab65878c6b5',
    '4916-e3-matched-packet/snapshot-rotations-matched-r2/validated.json': 'f77228d064aea128869d5f1d0613745755ac3f257cdf774d58494fb14e77bf25',
    '4916-e3-matched-packet/snapshot-rotations-matched-r2/capture/execution.json': '3ffe24529a4bddd61f8c47b059f1568d6b92b2668d66be0abf72137bb7f33be9',
    '4916-e3-matched-packet/snapshot-rotations-matched-r2/capture/stdout.log': 'a986e74124c9493f2a2f3c81d90ad2531f84e48e54ca4e3246dd30fb8e6fd74e',
    '4916-e3-matched-packet/snapshot-rotations-matched-r2/capture/stderr.log': '2c9693a4637419ad6b32998ae83cba86fdd4ce2019d0b877e9c85213d07dad4f',
    '4916-e3-matched-packet/snapshot-rotations-matched-r3/validated.json': '069a45fc85d55464e4bc0bf40077d0697b45156caadd931fffc324476b8f98ba',
    '4916-e3-matched-packet/snapshot-rotations-matched-r3/capture/execution.json': 'f245b93df178ab5fcb9fafb4ccea151a9ce280be5226adb85fd69d272df515c9',
    '4916-e3-matched-packet/snapshot-rotations-matched-r3/capture/stdout.log': '0cd28877caa1febe847ff559f18d6f331d2c1013f6e9b346a65b282b9cd3fac2',
    '4916-e3-matched-packet/snapshot-rotations-matched-r3/capture/stderr.log': '9b79a8ae216ae2bfa501bf75e2ee38f0b1f4f3e92bdb9a93086af6a0caa1f5b1',
}
PHASES = ['4916-e3-full-packet/snapshot-rotations-eligibility-v1', '4916-e3-matched-packet/snapshot-rotations-matched-r1', '4916-e3-matched-packet/snapshot-rotations-matched-r2', '4916-e3-matched-packet/snapshot-rotations-matched-r3']
PREFIX = 'treedb.cache.flush_backlog_coalescing.'
METRICS = [('interval_ns','Interval ms',1e6),('ack_ns','Ack ms',1e6),
    ('checkpoint_ns','Checkpoint ms',1e6),('snapshot_ns','Snapshot ms',1e6),
    ('allocated_bytes','Allocated MiB',2**20),('allocations','Allocations',1),
    ('post_reopen_gc_heap_bytes','Reopen GC heap MiB',2**20),
    ('read_p99_ns','Read p99 µs',1e3),('read_p999_ns','Read p999 µs',1e3),
    ('read_max_ns','Read max ms',1e6),('read_count','Reads',1),('read_samples','Samples',1)]
PHYSICAL = [('treedb.cache.flush_merge.applied_ops_total','Applied ops'),
    ('treedb.flush_apply.old_leaf_read_decode.node_loads_total','Old node loads'),
    ('treedb.flush_apply.old_leaf_read_decode.bytes_total','Old node bytes'),
    ('treedb.flush_apply.merge_build.leaf_pages_written_total','Leaf pages'),
    ('treedb.flush_apply.merge_build.leaf_page_bytes_written_total','Leaf bytes'),
    ('treedb.flush_apply.merge_build.internal_page_bytes_written_total','Internal bytes')]

def sha(path):
    digest=hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()

def table(headers, rows):
    return '\n'.join(['| '+' | '.join(headers)+' |', '| '+' | '.join(['---']*len(headers))+' |']+
        ['| '+' | '.join(map(str,row))+' |' for row in rows])

def cell(p):
    return f"pointer={p['pointer_threshold']}/checkpoints={p['checkpoints']}/wide={str(p['coalescing_wide']).lower()}"

def delta(p,key):
    return int(p['after'][key])-int(p['before'][key])

def render(packets, analysis):
    eligibility=[]
    for p in sorted(packets[0],key=cell):
        total=delta(p,PREFIX+'admitted_runs_total'); checkpoint=delta(p,PREFIX+'checkpoint.admitted_runs_total')
        eligibility.append([cell(p),total,checkpoint,total-checkpoint,
            delta(p,PREFIX+'admitted_extra_memtables_total'),delta(p,PREFIX+'admitted_extra_ops_total'),
            p['after'][PREFIX+'queued_memtables_max']])
    costs=[]
    for pair in sorted(analysis['pairs']):
        for key,label,scale in METRICS:
            c=[p[key] for ps in packets[1:] for p in ps if cell(p)==pair+'/wide=false']
            w=[p[key] for ps in packets[1:] for p in ps if cell(p)==pair+'/wide=true']
            cm,wm=statistics.median(c),statistics.median(w)
            paired=[(y/x-1)*100 for x,y in zip(c,w)]
            stat=analysis['pairs'][pair][key]
            assert stat['control']['values']==c and stat['wide']['values']==w, (pair,key)
            costs.append([pair,label,f'{cm/scale:.3f}',f'{wm/scale:.3f}',f'{(wm/cm-1)*100:+.3f}',
                '/'.join(f'{x:+.2f}' for x in paired),
                f'{(max(c)-min(c))/abs(cm)*100:.2f}/{(max(w)-min(w))/abs(wm)*100:.2f}'])
    physical=[]
    for key,label in PHYSICAL:
        c=[delta(p,key) for ps in packets[1:] for p in ps if cell(p)=='pointer=16384/checkpoints=1/wide=false']
        w=[delta(p,key) for ps in packets[1:] for p in ps if cell(p)=='pointer=16384/checkpoints=1/wide=true']
        assert len(set(c))==len(set(w))==1, label
        physical.append([label,c[0],w[0],f'{(w[0]/c[0]-1)*100:+.3f}'])
    return '\n\n'.join([table(['Eligibility cell','Admitted runs Δ','Checkpoint Δ','Background Δ','Extra tables Δ','Extra ops Δ','Cumulative queue max'],eligibility),
        table(['Matched cohort','Metric','Default median','Wide median','Change %','Paired r1/r2/r3 %','Spread default/wide %'],costs),
        table(['Inline/CP1 physical work (each of three pairs)','Default','Wide','Change %'],physical)])

def main():
    if not __debug__:
        raise RuntimeError('validation requires Python assertions; run without -O')
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence-root',type=Path,required=True)
    parser.add_argument('--print-tables',action='store_true')
    args=parser.parse_args(); root=args.evidence_root
    doc=Path(__file__).with_name('SNAPSHOT_DECISION.md'); text=doc.read_text(encoding='utf-8')
    for name,digest in HASHES.items():
        assert sha(root/name)==digest, name
        row=f'| `{name}` | `{digest}` |'
        assert text.splitlines().count(row)==1, f'artifact row differs or repeats: {name}'
        assert sum(line.startswith(f'| `{name}` |') for line in text.splitlines())==1, f'duplicate artifact: {name}'
    freeze=json.loads((root/'4916-e3-full-packet/normal-prepared/freeze.json').read_text(encoding='utf-8'))
    assert freeze['runtime_head']==HEAD and freeze['runtime_tree']==TREE and freeze['complete'] is True
    assert len(freeze['inputs'])==2402 and len(freeze['modules'])==17
    assert sha(root/'4916-e3-full-packet/normal-prepared/algorithm-work.test')==freeze['binary_sha256'], 'retained binary differs'
    plans=json.loads((root/'4916-e3-matched-pre-timer-plans.json').read_text(encoding='utf-8'))
    assert plans['eligibility_excluded_from_matched_three'] is True and len(plans['plans'])==3
    packets=[]
    for i,phase in enumerate(PHASES):
        validated=json.loads((root/phase/'validated.json').read_text(encoding='utf-8'))
        execution=json.loads((root/phase/'capture/execution.json').read_text(encoding='utf-8'))
        assert validated['execution_sha256']==sha(root/phase/'capture/execution.json')
        assert validated['freeze_sha256']==HASHES['4916-e3-full-packet/normal-prepared/freeze.json']
        assert validated['host_quiet'] is False and validated['native_timing_concurrency']==1
        assert execution['complete'] is True and execution['returncode']==0
        assert execution['family']=='snapshot-rotations' and execution['pilot'] is False and execution['small_flush'] is False
        assert execution['binary_sha256']==freeze['binary_sha256']
        for stream in ('stdout','stderr'):
            assert execution[stream+'_sha256']==sha(root/phase/'capture'/f'{stream}.log')
        if i:
            assert plans['persisted_unix_ns'] < execution['started_unix']*1e9
            assert validated['plan_sha256']==HASHES[f'4916-e3-matched-r{i}-plan.json']
        for side in ('source_before','source_after'):
            assert execution[side]==validated[side]
            for key in execution[side]:
                assert execution[side][key]==freeze[key], (side,key)
        ps=validated['parsed']['write_packets']; assert len(ps)==8 and len({cell(p) for p in ps})==8
        for p in ps:
            assert (p['keys'],p['updates'],p['key_bytes'],p['value_bytes'],p['ack_batch_ops'],p['flush_threshold'])==(250000,40000,32,256,1000,64<<20)
            assert p['snapshot_acquisitions']==p['snapshot_closes']==p['commits']==40
            assert p['snapshot_stride']==1000 and p['final_close_checked'] is True and p['validated_all_values_and_misses'] is True
            assert p['schema']=='algorithm-work-snapshot-rotations-v1'
            assert delta(p,PREFIX+'admitted_runs_total')==delta(p,PREFIX+'checkpoint.admitted_runs_total')
            expected=32 if not p['coalescing_wide'] else 56
            extra=expected if p['pointer_threshold']==16384 and p['checkpoints']==1 else 0
            assert delta(p,PREFIX+'admitted_extra_memtables_total')==extra
            assert delta(p,PREFIX+'admitted_extra_ops_total')==extra*250
        packets.append(ps)
    analysis=json.loads((root/'4916-e3-three-repeat-analysis.json').read_text(encoding='utf-8'))
    expected=render(packets,analysis)
    if args.print_tables:
        print(expected); return
    actual=text.split('<!-- canonical-tables:start -->\n',1)[1].split('\n<!-- canonical-tables:end -->',1)[0]
    assert actual==expected, 'canonical table differs'
    repo=Path(__file__).resolve().parents[3]
    assert sha(repo/'TreeDB/algorithm_work_bench_test.go')==freeze['harness_sha256']
    assert sha(Path(__file__).with_name('capture.py'))==freeze['capture_sha256']
    assert '](SNAPSHOT_DECISION.md)' in doc.with_name('README.md').read_text(encoding='utf-8')
    print('PASS: pinned review/freeze/plans/raw streams; eight eligibility cells; three matched rounds; arithmetic, source identity and decision tables')

if __name__=='__main__':
    main()
