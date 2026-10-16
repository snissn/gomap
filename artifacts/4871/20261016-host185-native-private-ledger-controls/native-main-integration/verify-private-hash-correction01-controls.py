import datetime,hashlib,json,re
from pathlib import Path
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');B=D/'native-main-integration/private-hash-capacity/source-correction01';C=B/'controls';V=C/'normal-freelist-private-capacity'
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest();j=lambda p:json.loads(p.read_text())
m=j(B/'source-frozen.json');assert sha(B/'source-frozen.json')=='6c525298d214717016b20b93eafd04c1b19ef0044b6ee32eab443ba376ebe7fd'
assert j(V/'source-before.json')==j(V/'source-after.json')==m
p=j(B/'proposed-commands.json');commands=p['commands'];assert len(commands)==12
assert all(m[n]==h for n,h in p['selected_test_files'].items())
command=j(V/'command.json');expected=commands[0]
for k,v in expected.items():
 if k not in ['executed']:assert command[k]==v,(k,command[k],v)
assert command['executed'] and command['grant_sha256']==sha(D/'native-decision/root-private-ledger-correction-and-controls.md')
args=command['argv'];assert args[0]==str(D/'toolchains/go1.26.3/bin/go') and args[1:4]==['test','-p=1','./TreeDB/freelist'] and args[-3:]==['-count=1','-v','-timeout=3m'] and command['outer_timeout_seconds']==240
assert args[args.index('-run')+1]=='^('+'|'.join(command['required_top_level_tests'])+')$'
env=command['env'];fixed={'CGO_ENABLED':'1','GODEBUG':'','GOENV':'off','GOFLAGS':'','GOGC':'100','GOMAXPROCS':'2','GOMEMLIMIT':'8GiB','GOPROXY':'off','GOSUMDB':'off','GOTOOLCHAIN':'local','GOWORK':'off'}
assert all(env[k]==v for k,v in fixed.items());assert env['GOROOT']==str(D/'toolchains/go1.26.3');assert all(str(D) in env[k] for k in ['GOCACHE','GOMODCACHE','TMPDIR'])
i=j(C/'identity.json');assert i['go_binary_sha256']==sha(Path(args[0]))=='d68b7abbc40d0844f673f6cf06ae3cded225c50437c6454fa37ef178d079fe65';assert i['compile_binary_sha256']==sha(D/'toolchains/go1.26.3/pkg/tool/linux_amd64/compile')=='3bb421d581f60b9ab48c2c73adf5c018557e89af32ad018d9c4417afa4bf90c1';assert i['runner_sha256']==sha(B/'run-controls.py') and i['proposed_commands_sha256']==sha(B/'proposed-commands.json') and i['subreaper_verified']
r=j(V/'result.json');proc=j(V/'process.json');raw=(V/'run.log').read_text();assert sha(V/'run.log')==r['raw_log_sha256'];runs=re.findall(r'^=== RUN\s+(\S+)',raw,re.M);passes=re.findall(r'^\s*--- PASS: (\S+)',raw,re.M);fails=re.findall(r'^\s*--- FAIL: (\S+)',raw,re.M);skips=re.findall(r'^\s*--- SKIP: (\S+)',raw,re.M)
assert runs==r['test_RUN_names'] and passes==r['test_PASS_names'] and fails==r['test_FAIL_names'] and skips==r['test_SKIP_names']==[];tops=[n for n in runs if '/' not in n];assert set(tops)==set(command['required_top_level_tests']) and len(tops)==18
failedtops=[n for n in fails if '/' not in n];assert failedtops==['TestPrivateCapacitySnapshotIndependentPlanAndPeakV6','TestPrivateCapacityPartialCloneAdmissionCancelV6','TestPrivateCapacitySinkDuplicateAndCommittedResumeV6'];assert len([n for n in passes if '/' not in n])==15 and len([n for n in runs if '/' in n])==79
assert r['exit_code']==1 and r['status']=='STOP' and not r['timeout'] and not r['signals'] and not r['remaining_group_members'] and not r['remaining_direct_or_adopted_children'] and not r['residual_observed_before_cleanup']
assert r['leader_joined'] and r['group_and_children_empty'] and r['source_before_after_equal'] and proc['pid']==proc['pgid']==r['pid']==r['pgid'] and proc['runner_pid']==i['runner_pid'];assert not Path('/proc/'+str(r['pid'])).exists() and not Path('/proc/'+str(i['runner_pid'])).exists()
summary=j(C/'summary.json');assert summary['commands_executed']==1 and summary['commands_not_executed']==[x['name'] for x in commands[1:]] and summary['stopped_at_first_failure'] and not summary['all_authorized_pass'] and not summary['actual33_executed'] and not summary['native_publication_runtime']
out={'verified_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'source_manifest_sha256':sha(B/'source-frozen.json'),'runner_sha256':sha(B/'run-controls.py'),'raw_sha256':sha(V/'run.log'),'command_sha256':sha(V/'command.json'),'result_sha256':sha(V/'result.json'),'actual_commands':1,'unexecuted_commands':11,'top_tests':18,'top_pass':15,'top_fail':failedtops,'child_RUN':79,'elapsed_seconds':r['elapsed_seconds'],'pid':r['pid'],'pgid':r['pgid'],'runner_pid':i['runner_pid'],'joined_empty_no_intervention':True,'all_before_after_source_and_tool_bindings_verified':True,'result':'FAIL preserved; no retry or production acceptance','index_not_launch_bound':True}
(D/'native-main-integration/private-hash-correction01-controls-root-verification.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(out,indent=2))
