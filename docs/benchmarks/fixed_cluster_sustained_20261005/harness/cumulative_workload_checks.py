"""Exact retained48/49/6s campaign controls; every authority-shaped input is synthetic."""
if not __debug__: raise RuntimeError('ordinary Python required; assertions must run')
assert len(checks)==347
# Reconstruct the actual collector fixture independently of later proof fixtures.
campaign_ns={'c':c,'base64':base64}
campaign_source=(R/'check.py').read_text()
start=campaign_source.index('vecs=')
stop=campaign_source.index('c.sustained_shape(report);')
exec(compile(campaign_source[start:stop],'actual-48-campaign-report-construction','exec'),campaign_ns)
campaign_report=campaign_ns['report']
c.sustained_shape(campaign_report)
checks.append('cumulative48_actual_collector48_prefix49_interval6_positive')
assert m['workload']=={'Originals':48,'Prefixes':49,'DurationSeconds':300,'MinSpacingSeconds':6,'RPCTimeoutSeconds':3,'OverallTimeoutSeconds':420,'CollectorOuterSeconds':480,'Warmup':64,'MaxAttempts':65536,'OutputBytes':134217728,'Concurrency':1,'InitialRows':10005,'FinalRows':10002}
assert (48-1)*max(6,2*3)+2*3==288<300 and (48-1)*6==282
checks.append('cumulative48_exact_campaign_caps_budget288_last_start282')
for key,value in [('Originals',58),('PaceInterval',5000000000)]:
 z=copy.deepcopy(campaign_report);z[key]=value
 bad('cumulative48_actual_collector_rejects_stale_'+key,lambda z=z:c.sustained_shape(z))
z=copy.deepcopy(campaign_report);z['Writes'][-1]['IntendedOffsetNS']=47*5000000000
bad('cumulative48_actual_collector_rejects_repinned_five_second_offset',lambda:c.sustained_shape(z))
z=copy.deepcopy(campaign_report);z['Prefixes']+=copy.deepcopy(z['Prefixes'][-10:])
bad('cumulative48_actual_collector_rejects_repinned59_prefixes',lambda:c.sustained_shape(z))
z=copy.deepcopy(campaign_report)
for i in range(48,58):
 w=copy.deepcopy(z['Writes'][i%2]);w['Ordinal']=i;w['IntendedOffsetNS']=i*6000000000
 w['Replace']['IdempotencyKey']=base64.b64encode(('trial24-key-%d'%i).encode()).decode()
 z['Writes'].append(w)
for i in range(49,59):
 p=copy.deepcopy(z['Prefixes'][-1]);p['Prefix']=i;z['Prefixes'].append(p)
z['Originals']=58;z['ReadPrefixes'][0]['CompatibleMask']=1<<58
bad('cumulative48_actual_collector_rejects_consistent_stale58_59_shape',lambda:c.sustained_shape(z))
# Native capture's real pending-packet expression, with no remote capture.
native_text=(R/m['roles']['native_runner']['output']['path']).read_text()
native_site=next(n for n in ast.walk(ast.parse(native_text)) if isinstance(n,ast.Expr) and isinstance(n.value,ast.Call) and len(n.value.args)>1 and isinstance(n.value.args[1],ast.Constant) and n.value.args[1].value=='pending native packet shape/source')
pending={'state':'NATIVE_CANONICAL_PREFIX_ORACLES_GENERATED_PENDING_ROOT_VALIDATION','source_head':d['source_head'],'source_tree':d['source_tree'],'RunID':'rf4trial24mixedchangingc1mixedc1v1','Profile':'changing-top10','Prefixes':[{}]*49,'OriginalRequests':[{}]*48}
def capture_shape(p):
 exec(compile(ast.Module(body=[native_site],type_ignores=[]),'actual-native-capture-campaign-shape','exec'),{'need':nr.need,'pending':p,'HEAD':d['source_head'],'TREE':d['source_tree']})
capture_shape(pending);checks.append('cumulative48_actual_native_capture48_49_positive')
for key,size in [('Prefixes',59),('OriginalRequests',58)]:
 p=copy.deepcopy(pending);p[key]=[{}]*size
 bad('cumulative48_actual_native_capture_rejects_stale_'+key,lambda p=p:capture_shape(p))
# Exercise the actual adapted verifier's fixed-field guard before any read/data work.
adapted_ast=ast.parse(ra.adapted)
verifier=next(n for n in adapted_ast.body if isinstance(n,ast.FunctionDef))
fixed_site=next(n for n in verifier.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='fixed' for t in n.targets))
ns=dict(ra.rd.__dict__);exec(compile(ast.Module(body=[fixed_site],type_ignores=[]),'actual-campaign-read-fixed-fields','exec'),ns)
assert ns['fixed']['PaceInterval']==6000000000 and "'PaceInterval':5_000_000_000" in ra.original
checks.append('cumulative48_actual_adapted_read_interval6_generic5_controls_preserved')
fixed_index=verifier.body.index(fixed_site)
fixed_loop=verifier.body[fixed_index+1]
def campaign_read_shape(interval):
 fields=copy.deepcopy(ns['fixed']);fields['PaceInterval']=interval
 gate_ns=dict(ra.rd.__dict__,p=fields,r=copy.deepcopy(fields))
 exec(compile(ast.Module(body=[fixed_site,fixed_loop],type_ignores=[]),'actual-campaign-read-fixed-admission','exec'),gate_ns)
campaign_read_shape(6000000000)
bad('cumulative48_actual_adapted_read_rejects_repinned_interval5',lambda:campaign_read_shape(5000000000))

# The helper is inspected as actual source; native Go is never executed here.
helper_text=(R/m['roles']['oracle_helper']['output']['path']).read_text()
go_source=next(n.value.value for n in ast.parse(helper_text).body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='GO_SOURCE' for t in n.targets))
assert 'Originals:48' in go_source and 'PaceInterval:6*time.Second' in go_source and 'Originals:58' not in go_source
assert 'len(r.Writes)!=48||len(r.Prefixes)!=49' in go_source
checks.append('cumulative48_actual_native_helper_source48_49_interval6')
# Repin each stale JSON honestly; rejection must occur before output creation.
campaign_receipts=[]
for label,counts,interval in [('stale58_59',(58,59),6),('inconsistent48_59',(48,59),6),('inconsistent58_49',(58,49),6),('stale_interval5',(48,49),5)]:
 row=copy.deepcopy(d);row['workload']=copy.deepcopy(m['workload'])
 row['workload'].update(Originals=counts[0],Prefixes=counts[1],MinSpacingSeconds=interval)
 out=fixtures/('trial24-cumulative48-rejected-'+label);row['output_root']=str(out)
 binding=fixture('cumulative48-repinned-declaration-'+label,row)
 bad('cumulative48_declaration_rejects_'+label,lambda row=row:x.declaration(row,m))
 command=[sys.executable,'-B',str(R/'instantiate.py'),'--declaration',binding['path'],'--declaration-sha256',binding['sha256']]
 result=subprocess.run(command,capture_output=True,text=True,timeout=30)
 (fixtures/('cumulative48-'+label+'.stdout')).write_text(result.stdout)
 (fixtures/('cumulative48-'+label+'.stderr')).write_text(result.stderr)
 (fixtures/('cumulative48-'+label+'.exit')).write_text(str(result.returncode)+'\n')
 assert result.returncode!=0 and not out.exists() and not out.is_symlink()
 checks.append('cumulative48_actual_repinned_instantiation_'+label+'_rejects_before_output')
 campaign_receipts.append({'case':label,'command':command,'declaration':binding,'exit_code':result.returncode,'output_absent':True})
row=copy.deepcopy(d);out=fixtures/'trial24-cumulative48-positive-source-only';row['output_root']=str(out)
binding=fixture('cumulative48-positive-declaration',row)
command=[sys.executable,'-B',str(R/'instantiate.py'),'--declaration',binding['path'],'--declaration-sha256',binding['sha256']]
result=subprocess.run(command,capture_output=True,text=True,timeout=30)
(fixtures/'cumulative48-positive.stdout').write_text(result.stdout)
(fixtures/'cumulative48-positive.stderr').write_text(result.stderr)
(fixtures/'cumulative48-positive.exit').write_text(str(result.returncode)+'\n')
assert result.returncode==0,(result.stdout,result.stderr)
emitted=json.loads((out/'instantiation.json').read_bytes())
assert emitted['workload']==m['workload'] and emitted['runtime_started'] is False and emitted['admission_flags_granted'] is False
for role,item in emitted['roles'].items():
 raw=Path(item['path']).read_bytes();assert sha(raw)==item['sha256'];ast.parse(raw)
checks.append('cumulative48_actual_full_portable_instantiation_positive_all15_AST_pins')
campaign_receipts.append({'case':'positive48_49_6','command':command,'declaration':binding,'exit_code':result.returncode,'output':str(out)})
(fixtures/'cumulative-workload-integration.json').write_text(json.dumps({'state':'SYNTHETIC_SOURCE_ONLY_CUMULATIVE48_CONTROLS_NOT_ACCEPTANCE','workload':m['workload'],'budget_seconds':288,'headroom_seconds':12,'ideal_last_start_seconds':282,'cases':campaign_receipts,'added_controls':checks[347:],'runtime_started':False,'Go_started':False,'network_calls':0,'limitations':['No final product landing or final source acceptance envelope is asserted.','Actual Python source construction and AST-extracted native shape guard only; native Go and cluster workload are unexecuted.']},indent=2)+'\n')
