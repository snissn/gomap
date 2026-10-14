from pathlib import Path
import hashlib, json, shutil, subprocess

base = Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013')
repo = base/'native-4878-traversal'
repair = base/'evidence/ordinary-5111/empty-checkpoint-repair-20261014'
perf = base/'evidence/successor-4876-pointer-tail-20261014'
prefix = 'artifacts/4871/20261014-checkpoint-pointer-tail'
parent = 'ec94c1c9f71dbf65de86666ac1969a4c58893446'
foreign = ['TreeDB/internal/valuelog/native_zstd_rank_census_test.go', 'TreeDB/internal/valuelog/native_zstd_leaf_fixture_export_test.go']

def sha(b): return hashlib.sha256(b).hexdigest()
def git(*args): return subprocess.check_output(['git','-C',str(repo),*args],text=True).strip()
def status(): return subprocess.check_output(['git','-C',str(repo),'status','--porcelain=v1','-uall'],text=True).splitlines()

assert git('rev-parse','HEAD') == parent
assert git('symbolic-ref','HEAD') == 'refs/heads/codex/4878-native-traversal-20261014'
assert not git('diff','--cached','--name-only')
before = status()
assert set(x[3:] for x in before) == set(foreign)
foreign_sha = {x:sha((repo/x).read_bytes()) for x in foreign}
assert sha((repair/'receipt.json').read_bytes()) == '4520a3a5f2eb6e5e3d7ad139c29664fe9020776468789b990bba7b14085aa767'
r = json.loads((repair/'receipt.json').read_text())
assert {k:v['exit'] for k,v in r['runs'].items()} == {'red':1,'green-normal':0,'green-race':0}
assert r['source_unchanged_after_checks'] and r['quiescent']
assert sha((perf/'adopted-public.go').read_bytes()) == '77f871c4a65f95648b9d2af85dbf9cc9ba50dc5fdca174eb36d2cc576ba846dc'
rows = json.loads((perf/'measurement-rows.json').read_text())
assert len(rows) == 8 and {x['shape'] for x in rows} == {'forced_pointer'}
for x in rows:
    for key in ['ack_routing_before_ok','ack_routing_after_ok','close_ok','point_calls/op','cow_capture_calls_total/op','cow_publications_total/op']:
        assert x['metrics'][key] == 1, (x,key)
packet = repo/prefix
assert not packet.exists()
packet.mkdir(parents=True)
retained = []

def copy_members(folder, label, members):
    for item in members:
        rel = Path(item['path'])
        assert not rel.is_absolute() and '..' not in rel.parts
        p = folder/rel
        assert p.is_file() and not p.is_symlink(), p
        data = p.read_bytes()
        assert len(data) == item['bytes'] and sha(data) == item['sha256'], p
        if rel.name.endswith('.test'):
            retained.append({'evidence':label, **item})
            continue
        out = packet/label/rel
        out.parent.mkdir(parents=True,exist_ok=True)
        out.write_bytes(data)
    shutil.copyfile(folder/'manifest.json',packet/label/'manifest.json')

copy_members(repair,'checkpoint-repair',json.loads((repair/'manifest.json').read_text())['members'])
assert sha((perf/'manifest.json').read_bytes()) == '01344d9639beedc8e07887ef2c49fa1ea67456f99925ecc102eb2396035985a7'
assert sha((perf/'report.md').read_bytes()) == '36dcc8bc5f6b1b60753626b7d39c4e197532ee779e5e12e4cdfc506b8dcfad58'
pm = json.loads((perf/'manifest.json').read_text())
copy_members(perf,'pointer-tail',pm['publishable_artifact_allowlist'])
assert not retained
retained = pm['retained_local_binaries']
assert len(retained) == 2
(packet/'README.md').write_text('''Public checkpoint handoff repair and bounded pointer diagnostic — 2026-10-14

The checkpoint repair uses the existing COW backend-maintenance handoff around public command-WAL fallback refresh. Actual public durable/relaxed × inline/pointer cases reproduce the stale successor basis and checked Close failure before the change, then pass after it. Eight focused normal and ten race controls pass, including unchanged strict stale-basis rejection. Source, tests, docs, RED/GREEN receipts and exact census are preserved under checkpoint-repair/.

Both pointer comparison variants adopt the IDENTICAL checkpoint repair. Only the six frozen captured-successor production deltas distinguish baseline and prototype. The common harness adds post-timer sum/mean/first/max/index statistics from existing arrays; timed operations, seeding and warmup behavior are unchanged. All eight authorized calls passed. No further collection was run.

Per public CommitAt20+GetAt15 unit, two balanced64x pairs per size:

| Forced pointer fixture | Before→prototype ns/op | B/op | allocs/op | reader p50 ns |
|---|---:|---:|---:|---:|
| small |114426.5→126576.5|76556→29365.5|186→174|15920→13275|
| multilevel |159746→113898.5|87062→39845|191→179|19855→14835|

Small total elapsed is worse, despite improved read latency and allocation. Every small writer maximum occurs at index42; every multilevel maximum at index19, approximately3.6–7.8ms. First writer calls are much shorter. This proves a midstream writer outlier, not its cause. Zero timed fsync, directory-sync, segment-creation or checkpoint deltas were observed. Raw total elapsed remains the acceptance measure; maximum removal, arbitrary warmup and median-only acceptance are not allowed.

Production M5 remains NO-GO. This artifact commit preserves source-bound component evidence; it is not the compiled native runtime, a production-code merge, final5111/4878 acceptance or full M0 qualification. Next: one bounded source-only writer-trigger diagnosis, then choose the smallest consequential inquiry or coherent production integration. Original graph/per-PR/final gates remain binding. Parent4871 is INCOMPLETE.

Unique worktrees, binaries and evidence remain retained; no cleanup release is granted. Two exact binaries are intentionally omitted from Git and identified in the manifest. Root owns integration and final acceptance; standing skill owner has received findings and confirmed the generalized public-entry/activation/proportional-lifecycle test requirements cover these cases.
''')
(packet/'scripts').mkdir()
shutil.copyfile(base/'publish-checkpoint-pointer-tail-20261014.py',packet/'scripts/publish-checkpoint-pointer-tail-20261014.py')
files = []
for p in sorted(packet.rglob('*')):
    if p.is_file():
        data=p.read_bytes();files.append({'path':str(p.relative_to(packet)),'bytes':len(data),'sha256':sha(data)})
out = {'schema':'mvcc-public-checkpoint-pointer-diagnostic-v1','parent':parent,'production_activation':'NO_GO','repair_acceptance':'focused component only; original full5111 gates open','measured_calls':8,'retained_binaries':retained,'cleanup_release':[],'files':files,'bytes':sum(x['bytes'] for x in files)}
(packet/'artifact-manifest.json').write_text(json.dumps(out,indent=1)+'\n')
git('add','-f','--',prefix)
expected = {prefix+'/'+x['path'] for x in files}|{prefix+'/artifact-manifest.json'}
assert set(git('diff','--cached','--name-only').splitlines()) == expected
git('commit','-m','Preserve public checkpoint regression repair and bounded pointer tails')
commit=git('rev-parse','HEAD')
assert git('rev-parse',commit+'^') == parent
assert set(git('diff-tree','--no-commit-id','--name-only','-r',parent,commit).splitlines()) == expected
for item in files:
    data=subprocess.check_output(['git','-C',str(repo),'show',commit+':'+prefix+'/'+item['path']])
    assert len(data)==item['bytes'] and sha(data)==item['sha256']
assert subprocess.check_output(['git','-C',str(repo),'show',commit+':'+prefix+'/artifact-manifest.json']) == (packet/'artifact-manifest.json').read_bytes()
assert status()==before and {x:sha((repo/x).read_bytes()) for x in foreign} == foreign_sha
ref=git('symbolic-ref','HEAD')
git('push','origin',ref+':'+ref)
assert git('ls-remote','origin',ref).split()[0] == commit
result={'commit':commit,'parent':parent,'ref':ref,'url':f'https://github.com/snissn/gomap/tree/{commit}/{prefix}','members':len(files)+1,'bytes':out['bytes'],'manifest_sha256':sha((packet/'artifact-manifest.json').read_bytes()),'readback_all_members':True,'foreign_preserved':True,'disposition':out['production_activation'],'released_paths':[]}
(perf/'publication-receipt.json').write_text(json.dumps(result,indent=1)+'\n')
print(json.dumps(result),flush=True)
