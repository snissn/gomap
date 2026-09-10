"""Validate and attribute the completed one-run diagnostic; never opens its DB."""
from pathlib import Path
import argparse,hashlib,json,socket,statistics,subprocess
import minima_treedb_runner as tr
from minima_treedb_probe import initial_batches, validate_build_capture
def h(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def validate_run_inputs(result, packet, manifest):
    authorization = json.loads((packet / 'launch-authorization.json').read_text())
    assert authorization['authorized'] is True
    ledger = authorization['sha256']
    for path, digest in ledger.items():
        assert h(path) == digest, path
    binding_path = packet / 'source-binding.json'
    binding = json.loads(binding_path.read_text())
    probe = Path(__file__).resolve().with_name('minima_treedb_probe.py')
    binary, serving = (Path(binding[key]).resolve() for key in ('binary', 'serving'))
    for path in (binding_path.resolve(), manifest.resolve(), binary, serving, probe, Path(__file__).resolve()):
        assert ledger[str(path)] == h(path), str(path)
    assert result['source_commit'] == binding['source_commit']
    assert result['binding_sha256'] == h(binding_path)
    assert result['script_sha256'] == h(probe)
    assert result['manifest_sha256'] == binding['manifest_sha256'] == h(manifest)
    assert result['binary_sha256'] == binding['binary_sha256'] == h(binary)
    assert result['serving_sha256'] == binding['serving_sha256'] == h(serving)
    assert result['build_profile'] is binding['build_profile']


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--packet', type=Path, required=True)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--source', type=Path, required=True)
    args=parser.parse_args()
    p=args.packet.resolve();d=p/'run'
    manifest=tr.common.load_manifest(args.manifest)
    expected=initial_batches(manifest)
    expected_count=len(expected); expected_rows=sum(x[2] for x in expected)
    j=json.loads((d/'result.json').read_text());e=json.loads((p/'execution.json').read_text())
    assert e['exit_code']==0 and e['status']=='exited'
    assert j['completed'] and not j['failures']
    assert j['manifest_sha256']==h(args.manifest)
    assert j['source_commit']==subprocess.check_output(['git','rev-parse','HEAD'],cwd=args.source,text=True).strip()
    assert j['completed_batches']==j['expected_batches']==j['batch_stats_records']==expected_count
    assert j['acknowledged_rows']==j['expected_rows']==j['public_count']==expected_rows
    assert h(d/'batch-stats.jsonl')==j['batch_stats_sha256']
    assert h(p/'command.log')==e['command_log_sha256']
    validate_run_inputs(j,p,args.manifest)
    assert not subprocess.check_output(['git','status','--porcelain'],cwd=args.source,text=True)
    records=[json.loads(line) for line in (d/'batch-stats.jsonl').read_text().splitlines()]
    req=[x for x in j['requests'] if x.get('operation_name')=='initial_batch_insert']
    assert len(records)==len(req)==expected_count
    origin=j['initial_work']['work']['origin_unix_nano'];pid=j['pid'];acked=0
    zero=lambda x:all(zero(v) for v in x.values()) if isinstance(x,dict) else x==0
    for i,(r,q,want) in enumerate(zip(records,req,expected)):
     assert r['index']==i and r['request']==q and r['preexisting_rows']==acked
     assert (r['scenario'],q['batch_start'],r['rows'])==want
     assert q['transport']=='native' and q['outcome']=='success' and q['result_count']==q['requested_count']==r['rows']
     assert q['started_monotonic_ns']<q['ended_monotonic_ns']<=r['observer_started_monotonic_ns']<=r['observer_ended_monotonic_ns']
     if i+1<len(req):assert r['observer_ended_monotonic_ns']<=req[i+1]['started_monotonic_ns']
     s=r['stats']['snapshot'];w=s['work'];a=s['last_opened_index'];stats=a['insert']
     assert r['stats']['status']=='captured' and w['pid']==pid and w['origin_unix_nano']==origin
     assert a['name']==j['effective_collection']['name'] and a['generation']==j['effective_collection']['generation']
     assert stats['Documents']==r['rows'] and stats['ColumnPublishRows']>=r['rows']
     assert all(stats[k]>0 for k in ['SourceReplacementPlan','Publish','ColumnPublishCommit','ColumnPublishManifestBytes'])
     assert all(zero(w[k]) for k in ['indexed_json','runtime','graph','fold'])
     dep=stats['ColumnPublishFinalizeCandidateDependencyBytes'];owned=stats['ColumnPublishFinalizeCandidateOwnedBytes'];pending=stats['ColumnPublishFinalizeAdmissionPendingBytes'];commits=stats['ColumnPublishFinalizeAdmissionPendingCommits'];hard=stats['ColumnPublishFinalizeHardAdmissionCount']
     assert 0<dep<=owned<=pending and 0<commits and hard in (0,1) and bool(hard)==(pending>256<<20 or commits>65536)
     acked+=r['rows']
    assert acked==expected_rows
    full_indices=[i for i,(_,_,rows) in enumerate(expected) if rows==256]
    assert [x['name'] for x in j['windows']]==['early','late']
    for window,wanted in zip(j['windows'],[full_indices[:32],full_indices[-32:]]):
     assert window['batch_indices_zero_based']==wanted
     indices=window['batch_indices_zero_based'];assert len(indices)==32
     assert window['requests']==[req[i] for i in indices]
     assert window['active_profile_probe']['status']==500
     assert window['cpu']['started_monotonic_ns']<window['active_profile_probe']['observed_monotonic_ns']<window['write_started_monotonic_ns']<window['write_ended_monotonic_ns']<window['cpu']['ended_monotonic_ns']
     for name in ['cpu','alloc_before','alloc_after']:assert h(window[name]['path'])==window[name]['sha256']
    final=j['final_work']['work'];assert final['pid']==pid and final['origin_unix_nano']==origin
    assert all(zero(final[k]) for k in ['indexed_json','runtime','graph','fold'])
    binding=json.loads((p/'source-binding.json').read_text())
    assert j['build_profile'] is binding['build_profile']
    builds=[q for q in j['requests'] if q.get('operation_name')=='column_graph_initial_build']
    if j['build_profile']:
     b=j['build'];q,=builds
     assert b['requests']==builds
     assert q['started_monotonic_ns']>req[-1]['ended_monotonic_ns']
     validate_build_capture(b,pid,origin,expected_rows)
     for name in ['cpu','alloc_before','alloc_after']:assert h(b[name]['path'])==b[name]['sha256']
    else:
     assert not builds and 'build' not in j
    assert len(j['lifetimes'])==1
    life=j['lifetimes'][0];assert life['exit']['exit_code']==0 and life['pid']==pid and life['exit']['pid']==pid
    assert life['linux_process_identity']==life['exit']['linux_process_identity']==j['linux_process_identity']
    assert not Path('/proc',str(pid)).exists() and not Path('/proc',str(e['pid'])).exists()
    for port in [17220,17221,17222]:
     with socket.socket() as check:check.bind(('127.0.0.1',port))
    S=[r['stats']['snapshot']['last_opened_index']['insert'] for r in records]
    gauge_fields={'ColumnPublishFinalizeAdmissionPendingBytes','ColumnPublishFinalizeAdmissionPendingCommits','ColumnPublishFinalizeCandidateDependencyBytes','ColumnPublishFinalizeHardAdmissionCount','ColumnPublishFinalizeCandidateOwnedBytes'}
    sums={k:sum(s[k] for s in S) for k,v in S[0].items() if isinstance(v,int) and k not in gauge_fields}
    request_ns=sum(q['ended_monotonic_ns']-q['started_monotonic_ns'] for q in req)
    observer_ns=sum(r['observer_ended_monotonic_ns']-r['observer_started_monotonic_ns'] for r in records)
    full=[i for i,r in enumerate(records) if r['rows']==256]
    windows={}
    fields=['SourceReplacementPlan','Publish','ColumnPublishBuildColumnDelta','ColumnPublishFinalize','ColumnPublishFinalizeCandidateBuild','ColumnPublishFinalizeCandidateInheritedFilter','ColumnPublishFinalizeCandidateVisibleClone','ColumnPublishFinalizeCandidateClosureAssemble','ColumnPublishFinalizeCandidateCOWPrepare','ColumnPublishFinalizeAdmissionWait','ColumnPublishFinalizeDurabilityWait','ColumnPublishAssetPreparation','ColumnPublishManifestEncode','ColumnPublishManifestBytes','ColumnPublishManifestMutationRecords']
    for name,indices in [('early',full[:32]),('late',full[-32:])]:
     sample=[S[i] for i in indices]
     windows[name]={'batch_indices':indices,'preexisting_rows':records[indices[0]]['preexisting_rows'],'mean_stats':{k:statistics.mean(s[k] for s in sample) for k in fields},'mean_resource_work':{k:statistics.mean(s['ColumnPublishFinalizeCandidateResourceWork'][k] for s in sample) for k in sample[0]['ColumnPublishFinalizeCandidateResourceWork']}}
    initial_db=j['initial_stats']['snapshot']['database'];last_db=records[-1]['stats']['snapshot']['database']
    db_delta={k:int(v)-int(initial_db[k]) for k,v in last_db.items() if k in initial_db and v.isdigit() and initial_db[k].isdigit() and k.startswith('treedb.durable_root.manifest_build.')}
    verification={'status':'PASS','source_commit':j['source_commit'],'result_sha256':h(d/'result.json'),'batch_stats_sha256':h(d/'batch-stats.jsonl'),'execution_sha256':h(p/'execution.json'),'analyzer_sha256':h(__file__),'batches':expected_count,'rows':expected_rows,'fixture':manifest['fixture'],'build_profile':j['build_profile'],'source_clean':True,'all_bound_hashes_match':True,'zero_indexed_json_runtime_graph_fold_during_ingest':True,'cleanup':'owned service and wrapper PIDs absent, zero service exit, ports free','owned_exit_peak_bytes':life['exit']['peak_rss_bytes'],'scope':'nonqualifying ingest and optional Build attribution; owned peak includes every enabled phase'}
    (p/'run-verification.json').write_text(json.dumps(verification,indent=2)+'\n')
    analysis={'qualifying':False,'request_seconds':request_ns/1e9,'observer_seconds':observer_ns/1e9,'source_plan_seconds':sums['SourceReplacementPlan']/1e9,'publication_seconds':sums['Publish']/1e9,'other_request_seconds':(request_ns-sums['SourceReplacementPlan']-sums['Publish'])/1e9,'stats_sums':sums,'windows':windows,'database_counter_deltas':db_delta,'database_initial':initial_db,'database_last':last_db,'verification_sha256':h(p/'run-verification.json'),'notes':['Timings are nested; do not sum parents with children.','Observer changes scheduling and CPU profiles; request duration excludes observer, no acceptance comparison.','Early/late scenarios differ; monotonic N is also ordinal.','Database current/last gauges are retained; their arithmetic deltas are not cumulative work.']}
    if j['build_profile']:
     b=j['build'];q,=b['requests']
     analysis['build']={'request_nanos':q['ended_monotonic_ns']-q['started_monotonic_ns'],'response':b['response'],'allocation_delta':b['allocation_delta'],'notes':b['notes']}
    (p/'run-attribution.json').write_text(json.dumps(analysis,indent=2)+'\n')
    print(f'PASS diagnostic: {expected_count} batches/{expected_rows} rows; exact provenance, stats, profiles and cleanup.')
    for k in ['request_seconds','observer_seconds','source_plan_seconds','publication_seconds','other_request_seconds']:print(k,analysis[k])
    for k in ['ColumnPublishCommit','ColumnPublishBuildColumnDelta','ColumnPublishBuildSystemDelta','ColumnPublishCommandWALAppend','ColumnPublishOrderedRootApply','ColumnPublishSystemRootApply','ColumnPublishFinalize','ColumnPublishFinalizeCandidateBuild','ColumnPublishFinalizeCandidateClosureAssemble','ColumnPublishFinalizeCandidateCOWPrepare','ColumnPublishFinalizeAdmissionWait','ColumnPublishFinalizeDurabilityWait','ColumnPublishAssetPreparation','ColumnPublishAssetAppendFileSync','ColumnPublishManifestEncode']: print(k,sums[k]/1e9)
    for name,w in windows.items():print(name,'manifest_bytes',w['mean_stats']['ColumnPublishManifestBytes'],'resource entries',w['mean_resource_work']['SourceEntriesInspected'],'root shares',w['mean_resource_work']['PhysicalRootShares'])


if __name__ == '__main__':
    main()
