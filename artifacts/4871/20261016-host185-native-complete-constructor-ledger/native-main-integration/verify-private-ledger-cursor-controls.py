import datetime, hashlib, json, re
from pathlib import Path
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015')
B=D/'native-main-integration/private-ledger-cursor-repair'; C=B/'controls'
def h(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def j(p): return json.loads(p.read_text())
m=j(B/'source-frozen.json'); f=j(B/'freeze-receipt.json'); plan=j(B/'proposed-commands.json')
source_receipt=D/'native-main-integration/private-ledger-cursor-source-root-verification.json'
assert j(source_receipt)['source_manifest_sha256']==h(B/'source-frozen.json')==f['source_manifest_sha256']=='bc2ebe15cd6e1dff56574993b83ffd84319822e2d214078a7218996cbfea0d18'
assert j(source_receipt)['full_source_archive_live_delta_index_verified']
i=j(C/'identity.json'); index=j(C/'index-binding.json'); after=j(C/'index-after.json'); summary=j(C/'summary.json')
assert i['runner_sha256']==h(B/'run-controls.py')
assert i['proposal_json_sha256']==h(B/'proposed-commands.json')=='13f11e804b38b2707c443157acdc1bad39b27a7a0e80788d62c71f9e1904396a'
assert i['grant_md_sha256']==h(D/'native-decision/root-private-ledger-cursor-repair.md')==h(C/'grant-original.md')
assert i['source_manifest_sha256']==f['source_manifest_sha256'] and i['source_archive_sha256']==f['source_archive_sha256']
assert i['archive_verified_files']==len(m)==10389
assert i['go_sha256']==h(Path(i['go_binary']))=='d68b7abbc40d0844f673f6cf06ae3cded225c50437c6454fa37ef178d079fe65'
assert i['compiler_sha256']==h(D/'toolchains/go1.26.3/pkg/tool/linux_amd64/compile')=='3bb421d581f60b9ab48c2c73adf5c018557e89af32ad018d9c4417afa4bf90c1'
assert i['subreaper_verified'] and not Path('/proc',str(i['runner_pid'])).exists()
assert index['source_delta_is_unstaged'] and index['root_recorded_baseline_tree']=='d63bc97e893e518c87687831a6e0a01a357268f2'
assert after['unchanged'] and after['index_sha256_after']==index['index_sha256_before']==h(Path(index['index_path']))
assert summary['commands_executed']==2 and summary['commands_not_executed']==[] and summary['all_authorized_pass']
assert summary['source_still_frozen'] and summary['all_groups_empty'] and not summary['actual33_executed'] and not summary['native_publication_executed']
assert len(plan['commands'])==2 and [c['race'] for c in plan['commands']]==[False,True]
fixed={'CGO_ENABLED':'1','GODEBUG':'','GOENV':'off','GOFLAGS':'','GOGC':'100','GOMAXPROCS':'2','GOMEMLIMIT':'8GiB','GOPROXY':'off','GOSUMDB':'off','GOTOOLCHAIN':'local','GOWORK':'off','GOROOT':str(D/'toolchains/go1.26.3'),'GOCACHE':str(D/'native-build/cache'),'GOMODCACHE':str(D/'ordinary-work/modcache')}
rows=[]; pgids=[]
for expected in plan['commands']:
    V=C/expected['id']; command=j(V/'command.json'); result=j(V/'result.json'); proc=j(V/'process.json')
    for key,value in expected.items():
        assert command[key]==(True if key=='executed' else value),(key,command[key],value)
    assert command['source_manifest_sha256']==f['source_manifest_sha256'] and command['source_archive_sha256']==f['source_archive_sha256']
    assert command['grant_md_sha256']==i['grant_md_sha256'] and command['proposal_json_sha256']==i['proposal_json_sha256']
    assert all(command['env'][k]==v for k,v in fixed.items())
    assert command['cwd']==str(D/'native-integrated') and command['outer_timeout_seconds']==240
    tops=expected['top_level_tests']; selector='^('+'|'.join(tops)+')$'
    assert expected['argv']==[str(D/'toolchains/go1.26.3/bin/go'),'test','-p=1']+(['-race'] if expected['race'] else [])+['./TreeDB/mvcc','-run',selector,'-count=1','-v','-timeout=3m']
    assert len(tops)==3
    children=[parent+'/'+child for parent,names in plan['required_child_tests'].items() for child in names]
    assert len(children)==36
    assert j(V/'source-before.json')==j(V/'source-after.json')==m
    preflight=j(V/'tmpdir-preflight.json')
    assert preflight['path']==command['env']['TMPDIR'] and preflight['exists_writable'] and preflight['write_read_unlink_verified'] and preflight['immediately_before_launch']
    assert j(V/'environment.json')['explicit_overrides']==command['env']
    raw=(V/'run.log').read_text()
    for label,pattern in [('RUN',r'^=== RUN\s+(\S+)'),('PASS',r'^\s*--- PASS: (\S+)'),('FAIL',r'^\s*--- FAIL: (\S+)'),('SKIP',r'^\s*--- SKIP: (\S+)')]:
        assert re.findall(pattern,raw,re.M)==result['test_'+label+'_names']
    assert set(result['test_RUN_names'])==set(result['test_PASS_names'])==set(tops+children)
    assert len(result['test_RUN_names'])==len(result['test_PASS_names'])==39
    assert result['test_FAIL_names']==result['test_SKIP_names']==[] and result['top_level_pass_count']==3
    assert result['exit_code']==0 and result['status']=='PASS' and result['stop_reasons']==[]
    assert not any(s in raw for s in ['WARNING: DATA RACE','panic:','goleak:'])
    assert result['raw_log_sha256']==h(V/'run.log')
    assert command['actual_pid']==command['actual_pgid']==result['pid']==result['pgid']==proc['pid']==proc['pgid']
    assert proc['runner_pid']==i['runner_pid'] and result['leader_joined'] and result['group_and_children_empty']
    assert not result['timeout'] and not result['signals'] and not result['remaining_group_members'] and not result['remaining_direct_or_adopted_children']
    assert result['residual_before_cleanup']=={'children':[],'group':[]} and result['source_before_after_frozen']
    pgids.append(result['pgid'])
    rows.append({'name':expected['name'],'top_PASS':3,'child_PASS':36,'elapsed_seconds':result['elapsed_seconds'],'pid':result['pid'],'pgid':result['pgid'],'raw_sha256':h(V/'run.log'),'result_sha256':h(V/'result.json')})
for p in Path('/proc').iterdir():
    if not p.name.isdigit(): continue
    try:
        stat=(p/'stat').read_text(); fields=stat[stat.rfind(')')+2:].split()
    except OSError: continue
    assert int(fields[2]) not in pgids and int(fields[1])!=i['runner_pid']
receipt={'verified_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'verifier_sha256':h(Path(__file__)),'source_verification_sha256':h(source_receipt),'source_manifest_sha256':f['source_manifest_sha256'],'source_archive_sha256':f['source_archive_sha256'],'full_command_source_tool_environment_process_verified':True,'index_bound_as_unchanged_prior_staged_baseline':True,'result':'PASS','commands_executed':2,'rows':rows,'actual33_executed':False,'native_publication_executed':False,'public_status':'Q32 RED/ACK0; selected P remains unobserved'}
(D/'native-main-integration/private-ledger-cursor-controls-root-verification.json').write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps(receipt,indent=2))
