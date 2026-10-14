from pathlib import Path
import hashlib, json, shutil, statistics, subprocess

base = Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013')
repo, ordinary = base/'native-4878-traversal', base/'ordinary-5111'
evidence = base/'evidence/root-cow-point-lane-affinity-20261014'
parent = '0bb0dd5b26db071ef5777614ce62463c75bd98f1'
prefix = 'artifacts/4878/20261014-cow-point-lane-affinity'
paths = ['TreeDB/caching/db.go', 'TreeDB/caching/cow_point_lane_affinity_test.go', 'TreeDB/mvcc/cow_point_lane_affinity_test.go', 'TreeDB/docs/spec/cow-cache-publication.md']
foreign = ['TreeDB/internal/valuelog/native_zstd_rank_census_test.go', 'TreeDB/internal/valuelog/native_zstd_leaf_fixture_export_test.go']
expected = ['42e30dacfcaac6c0699d7bf0468a5210c238425b80ad0cc490d73c2c9562ff6b', '9f4e487c2d81714a2e239b9776c7eed7a26afc41ff22049870e412721162289d', '9863a0628474d0f18147dcafed667a81c2d8041cbc50d226e3885a82a6840f58']
def sha(b): return hashlib.sha256(b).hexdigest()
def git(*args): return subprocess.check_output(['git','-C',str(repo),*args], text=True).strip()
def status(): return [x[3:] for x in subprocess.check_output(['git','-C',str(repo),'status','--porcelain=v1','-uall'], text=True).splitlines()]
def census():
    return {str(p.relative_to(repo)):sha(p.read_bytes()) for p in sorted(repo.rglob('*')) if p.is_file() and '.git' not in p.relative_to(repo).parts and not str(p.relative_to(repo)).startswith(prefix+'/')}
assert git('rev-parse','HEAD') == parent
assert not git('diff','--cached','--name-only')
assert set(status()) == set(foreign)
assert sha((repo/paths[0]).read_bytes()) == '9d38862a7a8eae58196b73b6816d39fa6b3bad657000bd72d897c376925bd1be'
for x,h in zip(paths[:3],expected): assert sha((ordinary/x).read_bytes()) == h
assert all(not (repo/x).exists() for x in paths[1:3])
old_doc = (repo/paths[3]).read_bytes()
assert sha(old_doc) == '55cac005b4bff235afac319e9ec02e36f423e7500e58c2536a7e75dd01208d74'
assert (ordinary/paths[3]).read_bytes() == old_doc
assert not (repo/prefix).exists()
before = census()
for x in paths[:3]: shutil.copyfile(ordinary/x,repo/x)
doc = old_doc.decode()
anchor = 'a durable acknowledgement. Read owners use the existing physical identity pin\n'
assert doc.count(anchor) == 1
doc = doc.replace(anchor, '''a durable acknowledgement. A single ordinary unsynced COW point Put with an
external command WAL uses the existing shard-affine value-log lane when pointer
placement is required. This applies to both no-flush placement and the relaxed
profile's buffered visibility flush. It avoids opening unrelated cold lanes;
multi-entry commands and durable sync reservations retain ordinary lane selection.
Canonical preparation, registered pointer visibility and publication remain the
same. A busy affinity lane uses the existing lane admission and Close protocol.
Read owners use the existing physical identity pin
''')
for r in [ordinary,repo]: (r/paths[3]).write_text(doc)
after = census()
assert {x:h for x,h in before.items() if x not in paths} == {x:h for x,h in after.items() if x not in paths}

out = evidence/'v2/causal-comparison'
d = json.loads((out/'measurement-receipts.json').read_text())
assert len(d['receipts']) == 16 and all(r['exit']==0 for r in d['receipts'])
groups, samples = {}, []
for r in d['receipts']:
    row = r['rows'][0].split()
    metrics = {row[i+1]:float(row[i]) for i in range(2,len(row)-1,2)}
    probes = {(x['phase'],x['key']):x['value'] for x in r['lane_probes']}
    def pair(key): return [int(probes[phase,key]) for phase in ['before','after']]
    syncs = pair('treedb.cache.value_log.directory_sync.calls_total')
    segments = pair('treedb.cache.vlog_shape.segments_total')
    target = r['profile']=='command_wal_relaxed' and r['placement']=='forced_pointer' and r['operation']=='point'
    expected_counters = [0,0] if r['placement']=='inline' else [1,1] if target and r['label']=='after' else [1,3]
    assert syncs == segments == expected_counters, (r,syncs,segments)
    assert metrics['wal_appends/op']==1 and metrics['public_checkpoints/op']==0 and metrics['vlog_syncs/op']==0
    assert metrics['wal_syncs/op']==(1 if r['profile']=='command_wal_durable' else 0)
    assert metrics['ack_routing_before_ok']==metrics['ack_routing_after_ok']==1
    key = '/'.join([r['profile'],r['placement'],r['operation']])
    groups.setdefault(key,{}).setdefault(r['label'],[]).append(metrics)
    samples.append({'cell':key,'label':r['label'],'sample':r['sample'],'directory_syncs':syncs,'segments':segments,'directory_ns':pair('treedb.cache.value_log.directory_sync.ns_total'),'metrics':metrics})
summary = {'diagnostic_only':True,'invocations':16,'wall_seconds':d['wall_seconds'],'target_path_proved':True,'excluded_classes_preserved':True,'samples':samples,'medians':{}}
for k,g in groups.items():
    med = {label:{n:statistics.median(r[n] for r in rows) for n in ['ns/op','B/op','allocs/op','writer_p50_ns']} for label,rows in g.items()}
    med['time_change_percent'] = 100*(med['after']['ns/op']/med['before']['ns/op']-1)
    summary['medians'][k]=med
(out/'summary.json').write_text(json.dumps(summary,indent=1)+'\n')
(out/'README.md').write_text('''# Public COW point lane routing: V2 causal diagnostic

Two balanced pairs, four public profile/placement/operation cells, fixed64x, quiet111 CPU0-3/GOMAXPROCS4, identical Go1.26.3 and harness. Only caching/db.go differs in the compared production census. All16 invocations passed output, actual profile/selector/ACK, persistent physical pointer layout, WAL and checkpoint checks. This packet is diagnostic, not final sustained or matched Dgraph qualification.

The real RELAXED forced-pointer target now keeps persistent lane/segment count and directory-sync count1->1 rather than1->3. Inline remains0->0; multientry and DURABLE remain1->3. Target median237363->75601ns/op(-68.15%),89162->17644B/op,182->172allocs/op. Two avoided cold lane writers account for much of this short-run gain: warm writerp50 is16785->15235ns, so do not extrapolate68% to steady-state Dgraph. Timing of excluded cells is not attributed to this predicate; allocations and routing preserve their unchanged classes.

V1's helper-only RED/GREEN did not establish production activation: the real command-WAL relaxed profile requests journalDurabilityFlush for pointer visibility, not journalDurabilityNone. The first16 public runs retained two extra cold lanes in both builds. V2 retains those negative raw results, adds actual public Store.CommitAt same-test RED/GREEN and race/durability controls, and admits both None and visibility Flush under the same unsynced single-point eligibility. Visibility remains mandatory, no fsync or durability contract is weakened. Generalized skill repair now requires public-entry failing regression and observable branch selection with proportional target/exclusion coverage.

Next measured seam: populated retained PRIMARY still constructs a full owned projection iterator per successor read. The existing exact scratch lookup cannot supply successor semantics and current-backend recapture would violate the retained cut. A bounded populated-backend fixture must select the next structural change before exhaustive qualification. All original all19, lifecycle, platform, raw-performance and final BOTH-mode >10% matched Badger-ratio gates remain open.
''')
packet = repo/prefix
shutil.copytree(evidence,packet/'evidence')
for name in ['cow-point-lane-causal-comparison-20261014.py','cow-point-lane-causal-comparison-v2-20261014.py','publish-cow-point-lane-affinity-20261014.py']:
    p = packet/'scripts'/name; p.parent.mkdir(exist_ok=True); shutil.copyfile(base/name,p)
shutil.copyfile(out/'README.md',packet/'README.md')
files = []
for p in sorted(packet.rglob('*')):
    if p.is_file():
        b=p.read_bytes(); files.append({'path':str(p.relative_to(packet)),'bytes':len(b),'sha256':sha(b)})
manifest = {'schema':'cow-point-lane-affinity-v2','parent':parent,'production_projection':'Actual coherent ordinary tree; census binds compared production files, not the complete native branch','source_paths':[{'path':x,'sha256':sha((repo/x).read_bytes())} for x in paths],'files':files,'bytes':sum(x['bytes'] for x in files)}
(packet/'artifact-manifest.json').write_text(json.dumps(manifest,indent=1)+'\n')
git('diff','--check','--',*paths)
git('add','-f','--',*paths,prefix)
expected_staged = set(paths+[prefix+'/'+x['path'] for x in files]+[prefix+'/artifact-manifest.json'])
assert set(git('diff','--cached','--name-only').splitlines()) == expected_staged
git('diff','--cached','--check','--',*paths)
git('commit','-m','Use existing shard-affine lane for relaxed public COW point pointers')
commit = git('rev-parse','HEAD')
assert git('rev-parse',commit+'^') == parent
assert set(git('diff-tree','--no-commit-id','--name-only','-r',parent,commit).splitlines()) == expected_staged
for x in paths: assert subprocess.check_output(['git','-C',str(repo),'show',commit+':'+x])==(repo/x).read_bytes()
for x in files:
    b = subprocess.check_output(['git','-C',str(repo),'show',commit+':'+prefix+'/'+x['path']]); assert sha(b)==x['sha256'] and len(b)==x['bytes']
assert {x:sha((repo/x).read_bytes()) for x in foreign} == {x:before[x] for x in foreign}
assert set(status())==set(foreign)
ref = git('symbolic-ref','HEAD'); assert ref=='refs/heads/codex/4878-native-traversal-20261014'
git('push','origin',ref+':'+ref)
assert git('ls-remote','origin',ref).split()[0] == commit
receipt = {'commit':commit,'parent':parent,'ref':ref,'members':len(files)+1,'bytes':manifest['bytes'],'manifest_sha256':sha((packet/'artifact-manifest.json').read_bytes()),'all_sources_and_manifest_members_readback_verified':True,'outside_import_preserved':True,'foreign_preserved':foreign,'url':f'https://github.com/snissn/gomap/tree/{commit}/{prefix}','qualification':'Focused public-path repair and diagnostic only; parent and full graph gates OPEN'}
(evidence/'publication-receipt.json').write_text(json.dumps(receipt,indent=1)+'\n')
print(json.dumps(receipt),flush=True)
