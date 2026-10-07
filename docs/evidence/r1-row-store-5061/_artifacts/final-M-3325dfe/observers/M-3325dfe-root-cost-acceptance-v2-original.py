#!/usr/bin/env python3
"""Coordinator's explicit finite A/C cost decision; no capture or validation rerun."""
import hashlib
import json
import os
import subprocess
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

O = Path('/home/mikers/gomap-r1-final-observers-20261006')
R = Path('/home/mikers/gomap-r1-final-results-20261006/M-3325dfe')
P = Path('/home/mikers/gomap-r1-final-receipts-20261006')
M = '3325dfe77940fec8587d8b61b1ac4e0b2f72caca'
assert os.environ['R1_RECEIPT_LOCKED'] == 'yes'
source = Path('/home/mikers/gomap-r1-final-source-20261006')
assert subprocess.check_output(['git','rev-parse','HEAD'], cwd=source, text=True).strip() == M
assert not subprocess.check_output(['git','status','--porcelain','--untracked-files=all'], cwd=source)
pins = {
    R/'A/comparison-v2.json': 'a7c55ffc991c8630b31dcf7fcfb08e9c63c867f635615325978dfad62f343fde',
    R/'A/root-approved-A-semantics-and-provenance-certificate-v2.json': '812b6daa88f7e692957aeeac2d1627bb6911cdd767b8a067fa38c4be381926f5',
    R/'A/root-provenance-verification-original.json': '457950e386e4a19f183931f2a79b3cd77deed45c0bf7a5828f22bb1f2610456b',
    R/'A/descriptive-report.md': 'd60eba481ed6fa57ce6b725cbf9ddbed4172e963615442d9299194c2c5f676af',
    R/'C/root-provenance-verification-original.json': 'bb1ec7409e6f5833ff9b7a9ec52deb5e267027c788508013e3c0de8c0ab61a7f',
    R/'C/summary.json': '17f2ae7f7fbda5c12a6e249090ca3f1a5a083b75598fd6f5d41995184247dbe0',
    R/'C/descriptive-report.md': '3d932ac5c6bfbe9ab1dead0578c702cc2b8237d5301edc30b5258ce118b489b9',
    P/'898a-current-ci-normal2-original/artifact/treedb-test.jsonl': '44ae16595353f1995c0f65c2a8428f17cbfa3f12a0e50af3481369320c6294f6',
    P/'898a-current-ci-race4-original/artifact/treedb-race.jsonl': '96be728e5d25f47ebd7984e4dea5b1648a3ac95bbc180094aba1db5fc9aa3aed',
    source/'TreeDB/collections/typed_metadata_test.go': '862aa5fa8c7dbc9b4fd62872e63b8b1ebf5ce59ee5ae63f7569d726a566e3a13',
}
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
for p, expected in pins.items():
    assert p.is_file() and not p.is_symlink() and sha(p) == expected, str(p)
comparison = json.loads((R/'A/comparison-v2.json').read_text())
groups = comparison['comparisons']
counts = Counter(x['comparison'] for x in groups)
assert counts == {'descriptive_matched':20, 'inconclusive':72, 'newly_enabled':1}, counts
stable = [x for x in groups if x['comparison']=='descriptive_matched']
assert min(x['throughput_ratio'] for x in stable) >= .85
checkpoint = next(x for x in groups if x['engine']=='typed-row' and x['phase']=='checkpoint')
assert checkpoint['before']['bytes_per_op_median']==1110120 and checkpoint['after']['bytes_per_op_median']==3829760
metadata = {}
for p in list(pins)[-3:-1]:
    events=[]
    for line in p.read_text().splitlines():
        try: d=json.loads(line)
        except json.JSONDecodeError: continue
        if d.get('Action')=='pass' and d.get('Test') in ('TestTypedMetadataDimensionIndependentPayload4769','TestTypedMetadataUpdateNoVectorRewrite4769'):
            events.append({k:d[k] for k in ('Action','Package','Test')})
    assert len(events)==2, (str(p),events)
    metadata[str(p)]=events
common = {
    'schema':'gomap-r1-root-finite-cost-acceptance-v1', 'owner':'/root R1 coordinator',
    'accepted':True, 'utc':datetime.now(timezone.utc).isoformat(), 'actual_main_commit':M,
    'runtime_sha256':'eab40aaf77ed307d92a7a38adf0dd80be07a7e12bdda8a2ec3118baf9cad33ff',
    'harness_sha256':'706b71f11508bf9c0075b0dd9c5463cd741a49cba83996feaf753330ace77822',
    'binary_sha256':'df995f26aa80ce7e9bb99075537b9595fc850cfc2708c8036edaf2d4fad3bdc2',
    'original_evidence_sha256':{str(p):h for p,h in pins.items()},
    'decision_url':'https://github.com/snissn/gomap/issues/5056#issuecomment-6007281596',
    'unchanged_guard':'15% matched-cell timing noise and median throughput-loss guard; all samples and inconclusive labels retained',
    'original_failed_A_comparison_preserved':True, 'remeasurement':False, 'CI_exception_used':False,
    'separate_pending_gates':['original D retained lifecycle/capacity/cost acceptance','anonymous public replay','reviewed E PR and required current-head CI/merge','remaining issue/parent acceptance'],
}
a = dict(common, lane='A', packet_sha256='e5b83232a75ecb0a30b2859dd64e0ee2e1cba2a31cb922c05d1b9384e048a17b',
    groups=dict(counts), minimum_stable_throughput_ratio=min(x['throughput_ratio'] for x in stable),
    accepted_scope='Full frozen 4096-row/32-row-batch/1000-call/5-fresh-repetition six-engine read/mutation profile; stable old-path guard and enabled ordinary typed full-row range cost.',
    checkpoint_observation=checkpoint,
    explicit_maintenance_cost_decision='Coordinator explicitly accepts the observed one-checkpoint whole-process allocation median increase 1,110,120 to 3,829,760B (+2,719,640B) and +685 allocations within this finite profile. Baseline already has a 3,935,272B high sample, exceeding all M samples; five samples differ in mode frequency. Causal attribution remains UNRESOLVED. No new deterministic allocation mode or stable throughput loss is established. This accepts the visible cost; it does not erase it, call it harmless noise, or establish equal checkpoint cost.',
    additional_setup_observation='BSON read-view setup whole-process median1264 to3512B, allocations -1; timing inconclusive; finite setup cost explicitly retained and accepted.',
    limits=['72 timing groups remain inconclusive; no ratios from those groups accepted as wins or stable regressions','No universal SQLite dominance, checkpoint optimization, background-work attribution, whole-DB capacity or duration-unbounded memory claim','Allocation sampling is whole-process; heap-after is sampled, not peak/RSS','New typed public range is enabled cost, without an enabled baseline speedup ratio'],
    stable_delivered_benefit='Typed GetInto349.538 to59.897us (5.83567x), B/call+2.9%; prepared batch/complete prepared range remain within guard.')
c = dict(common, lane='C', packet_sha256='5be838ef1e24f8fec1793471531b1b6da086cdf522aa35a3cabf1f76bf531e07',
    accepted_scope='Selected #5059 criterion6: bio96/4096 bytes x actual request rows1/32 x absent/email+city indexes x bio/email+city UpdateBatch changes;4096live rows,100serial requests,5freshDB reps,16groups80cells.',
    oracles='All80 original Flush/reopen full-row/current and historical-posting oracles pass.',
    actual_work='Everycell100UpdateBatch calls,100WAL appends,100logical sync and100actual file-sync calls;100or3200items. All13 Flush counter deltas actually zero because ACK already publishes; zero indexed_flush does not mean zero index work.',
    numeric_disposition='Accept enabled finite cost characterization including0.835-7.685MB/request whole-process allocation and full-row WAL width costs; no comparison to stale mutation timings or claim allocations necessary/native partial setter.12ACKgroupswithin15%;4wide32rowACKgroups and15/16Flush groups remain inconclusive.',
    metadata_reference_applicability={'source':str(source/'TreeDB/collections/typed_metadata_test.go'),'sha256':pins[source/'TreeDB/collections/typed_metadata_test.go'],'current_normal_race_pass_events':metadata,'scope':'Existing eligible meta.* reference/no-vector-rewrite and dimension-independent metadata WAL tests pass on reviewed exact current-head CI; separate from generic C full-row mutation sweep; no metadata performance claim.'},
    retained_deferral='Larger population/concurrency profiles remain deferred by the accepted #5057 and live #5059 decision, requiring a separately frozen comparable workload.',
    limits=['No native partial setter or meta.* mutation is measured in C','No causal index timing or per-stage allocation attribution; publication/sync scopes overlap','Narrow prepare timer covers prepareInsertDocuments only; do not add or subtract phase medians','Heap-after is a sample, not peak/RSS; no duration-unbounded whole-DB bound'])
for lane, decision in [('A',a),('C',c)]:
    out=R/lane/'root-cost-acceptance-original.json'
    with out.open('x') as f: json.dump(decision,f,indent=2);f.write('\n')
    print(lane, str(out), sha(out))
for p, expected in pins.items(): assert sha(p)==expected, str(p)
