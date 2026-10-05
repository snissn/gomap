#!/usr/bin/env python3
"""Opt-in aggregate memory lifecycle evidence. Fresh processes, no timing claims.

Builds one tagged test binary, runs N/2N x prune/control/cancel x reader-unpinned/
pinned in distinct processes, retains exact source identity and raw failures.
Control has identical input shape and no discard floor; it retains all history.
This separate contract does not qualify the native-prune benchmark schema.
"""
import argparse, hashlib, json, os, pathlib, shutil, subprocess, sys, tempfile, time

CONTRACT = 'gomap-native-memory-v2'
MAINTENANCE_RSS_CUTS = frozenset(('prepared', 'partial_private_output', 'accepted_relaxed', 'cancel_requested', 'cancel_drained', 'finish', 'cursor_close', 'db_owned_cleanup'))
BUILD_KEYS = ('CGO_ENABLED','GOFLAGS','GOWORK','GOTOOLCHAIN','GOMAXPROCS','GOROOT','PATH','GOGC','GOMEMLIMIT','GODEBUG')

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def require(condition, message):
    if not condition:
        raise ValueError(message)

def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()

def bindings(root):
    result = {}
    for p in sorted(root.rglob('*')):
        if p.is_file() and '.git' not in p.parts and (p.suffix in ('.go', '.mod', '.sum') or p.name == 'AGENTS.md' or p == root/'scripts/native_prune_memory.py'):
            result[str(p.relative_to(root))] = hashlib.sha256(p.read_bytes()).hexdigest()
    require(bool(result), 'empty source bindings')
    return result

def same_source(expected, actual):
    require(expected == actual, 'source bindings drift')

def validate_state(a):
    require(isinstance(a,dict),'missing custody sample')
    p=a.get('Private');require(isinstance(p,dict) and isinstance(p.get('Native'),dict),'missing private sample')
    z=p['Native']
    for owner, bools, nums in ((a,('Native','Accepted','EOF'),('Phase','InputCount','Chunk')),(p,('Build',),('AllocatedPages','Dependencies','FlatRetiredLen','FlatRetiredCap','RetirementCells')),(z,(),('Window','Frames','FlatRetiredLen','FlatRetiredCap','DecodedLeafBytes','ObservedOutputPages','ObservedOutputBufferBytes'))):
        require(all(k in owner and type(owner[k]) is bool for k in bools),'missing/malformed owner booleans')
        require(all(k in owner and type(owner[k]) is int and owner[k]>=0 for k in nums),'missing/malformed owner counts')
    require(p.get('FlatRetiredCap')==0 and z.get('FlatRetiredCap')==0 and 0 <= z.get('Window',-1) <= 32 and 0 <= a.get('InputCount',-1)<=32,'sample descriptor failure')
    require(z['ObservedOutputPages'] in (0,1) and (z['ObservedOutputPages']==0)==(z['ObservedOutputBufferBytes']==0),'invalid scalar output observation')
    require(z['DecodedLeafBytes'] in (0,4096),'invalid retained decoded leaf size')
    return p,z

def validate_case(x, n, mode, pinned):
    require(isinstance(x, dict), 'result must be object')
    required = ('schema','n','mode','pinned','pid','Calls','Records','Bytes','Pruned','MaxRecords','MaxBytes','MaxRetirementCells','MaxSourceRetirementCells','MaxWindow','MaxFrames','MaxFlatRetiredCap','SampledMaintenanceRSSPeak','RSSPeriodicPeak','RSSPeriodicSamples','PhysicalBefore','PhysicalAfter','FixedSurvivors','PointerOracle','ReaderOracle','ReopenOracle','CleanupOracle','CursorCloseOracle','PartialOutput','RelaxedCustody','ChargedCancel','RetirementPeak','SourceRetirementPeak','cuts')
    require(all(k in x for k in required), 'missing result field')
    require((x['schema'],x['n'],x['mode'],x['pinned']) == (CONTRACT,n,mode,pinned), 'case identity mismatch')
    for k in required:
        if k not in ('schema','mode','pinned','cuts','RetirementPeak','SourceRetirementPeak','RSSPeriodicPeak') and not k.endswith('Oracle') and k not in ('PartialOutput','RelaxedCustody','ChargedCancel'):
            require(type(x[k]) is int and x[k] >= 0, 'invalid numeric field '+k)
    for k in ('pinned','PointerOracle','ReaderOracle','ReopenOracle','CleanupOracle','CursorCloseOracle','PartialOutput','RelaxedCustody','ChargedCancel'):
        require(type(x[k]) is bool, 'invalid boolean field '+k)
    require(x['pid'] > 0 and x['Calls'] > 0, 'missing process/work witness')
    require(x['MaxRecords'] <= 32 and x['MaxBytes'] <= 1<<20 and x['MaxWindow'] <= 32 and x['MaxFrames'] <= 64 and x['MaxFlatRetiredCap'] == 0, 'bounded-work/descriptor failure')
    require(all(x[k] for k in ('PointerOracle','ReaderOracle','ReopenOracle','CleanupOracle','CursorCloseOracle')), 'missing lifecycle oracle')
    require(x['PhysicalBefore'] == n+2 and x['FixedSurvivors'] == 3, 'fixture shape mismatch')
    require(x['PhysicalAfter'] == (3 if mode == 'prune' else n+2) and x['Pruned'] == (n-1 if mode == 'prune' else 0), 'physical/ACK mismatch')
    if mode != 'control':
        require(x['PartialOutput'] and x['MaxSourceRetirementCells'] > 0, 'missing actual physical-page/output witness')
    require(mode != 'prune' or x['RelaxedCustody'], 'missing accepted RELAXED custody')
    require(mode != 'cancel' or x['ChargedCancel'], 'missing charged public cancellation')
    cuts=x['cuts'];require(isinstance(cuts,list) and 8 <= len(cuts) <= 16,'invalid bounded samples')
    names=[s.get('name') for s in cuts if isinstance(s,dict)]; require(len(names)==len(cuts) and len(set(names))==len(names),'invalid/duplicate cuts')
    expected=['process_start','fixture_baseline']
    if mode!='control': expected+=['prepared','partial_private_output']
    if mode=='prune':expected+=['accepted_relaxed','finish']
    elif mode=='cancel':expected+=['cancel_requested','cancel_drained']
    else:expected+=['finish']
    expected+=['cursor_close','db_owned_cleanup','reader_released','db_close','reopen','final_db_close','directory_cleanup']
    require(names==expected,'missing/out-of-order lifecycle cut')
    previous_alloc=previous_malloc=0
    for s in cuts:
        for k in ('HeapAlloc','HeapInuse','HeapObjects','TotalAlloc','Mallocs','RSS','ProcessHWM','OutputPages','OutputBufferBytes'):
            require(k in s and type(s[k]) is int and s[k]>=0,'invalid/missing memory sample '+k)
        require(s['RSS']>0 and s['ProcessHWM']>0,'missing Linux RSS/HWM observation')
        require(s['TotalAlloc']>=previous_alloc and s['Mallocs']>=previous_malloc,'allocation counter regression')
        previous_alloc,previous_malloc=s['TotalAlloc'],s['Mallocs']
        a=s.get('State');require(isinstance(a,dict),'missing custody sample')
        p,z=validate_state(a)
        require(s['OutputPages']==z['ObservedOutputPages'] and s['OutputBufferBytes']==z['ObservedOutputBufferBytes'],'contradictory output sample')
        require(z['ObservedOutputPages'] in (0,1) and (z['ObservedOutputPages']==0)==(z['ObservedOutputBufferBytes']==0),'invalid scalar output observation')
        require(z['DecodedLeafBytes'] in (0,4096),'invalid retained decoded leaf size')
        require(z['Frames']<=x['MaxFrames'] and max(z['Window'],a['InputCount'])<=x['MaxWindow'] and p['RetirementCells']<=x['MaxRetirementCells'],'sample exceeds reported maximum')
        if not a['EOF'] and not a['Accepted']:require(p['RetirementCells']<=x['MaxSourceRetirementCells'],'source retirement sample exceeds reported maximum')
        if s['name']=='partial_private_output':
            require(a['Native'] and p['Build'] and not a['EOF'] and not a['Accepted'] and p['AllocatedPages']>0 and p['RetirementCells']>0 and z['ObservedOutputPages']==1 and z['ObservedOutputBufferBytes']>0,'missing actual allocated-page/source-retirement/partial-output witness')
        if s['name']=='accepted_relaxed':require(a.get('Native') and a.get('Accepted') and p.get('Build'),'missing real DB custody')
        if s['name'] in ('finish','cancel_drained','cursor_close','db_owned_cleanup','reader_released','db_close','reopen','final_db_close','directory_cleanup'):
            require(not a['Native'] and not a['Accepted'] and not p['Build'] and p['AllocatedPages']==0 and p['Dependencies']==0 and p['RetirementCells']==0 and all(z[k]==0 for k in ('Window','Frames','FlatRetiredLen','FlatRetiredCap','DecodedLeafBytes','ObservedOutputPages','ObservedOutputBufferBytes')),'terminal owner retained')
    periodic=x['RSSPeriodicPeak']
    require(isinstance(periodic,dict) and all(type(periodic.get(k)) is int and periodic[k]>=0 for k in ('Call','RSS')),'missing/malformed periodic RSS witness')
    require(x['RSSPeriodicSamples']==x['Calls']//128,'missing periodic RSS schedule coverage')
    if x['RSSPeriodicSamples']==0:
        require(periodic['Call']==0 and periodic['RSS']==0,'nonzero empty periodic RSS witness')
    else:
        require(periodic['RSS']>0 and 128<=periodic['Call']<=x['Calls'] and periodic['Call']%128==0,'invalid periodic RSS witness call/sample')
    observed_rss=max([periodic['RSS']]+[s['RSS'] for s in cuts if s['name'] in MAINTENANCE_RSS_CUTS])
    require(x['SampledMaintenanceRSSPeak']==observed_rss,'unwitnessed sampled maintenance RSS maximum')
    require(x['MaxSourceRetirementCells']<=x['MaxRetirementCells'],'source peak exceeds overall peak')
    for field, maximum, source_only in (('RetirementPeak','MaxRetirementCells',False),('SourceRetirementPeak','MaxSourceRetirementCells',True)):
        w=x[field];require(isinstance(w,dict) and type(w.get('Call')) is int,'missing/malformed peak witness')
        a=w.get('State');p,z=validate_state(a)
        require(p['RetirementCells']==x[maximum],'unwitnessed retirement maximum')
        if not a['EOF'] and not a['Accepted']:require(p['RetirementCells']<=x['MaxSourceRetirementCells'],'source retirement peak exceeds reported maximum')
        if x[maximum]==0:
            require(w['Call']==0 and all(v is False if type(v) is bool else v==0 for owner in (a,p,z) for k,v in owner.items() if k not in ('Private','Native') or type(v) is bool),'nonzero empty peak witness')
        else:
            require(1<=w['Call']<=x['Calls'] and a['Native'] and p['Build'],'invalid observed peak call/custody')
            require(not source_only or (not a['EOF'] and not a['Accepted']),'source peak after EOF/acceptance')
        require(z['Frames']<=x['MaxFrames'] and max(z['Window'],a['InputCount'])<=x['MaxWindow'],'peak descriptor exceeds reported maximum')
    require(cuts[-1]['name']=='directory_cleanup','final DB Close/cleanup allocations excluded')
    return x

def summarize(receipt, results):
    summary=[]
    for c,x in zip(receipt['cases'],results):
        cuts={s['name']:s for s in x['cuts']};start=cuts['process_start'];baseline=cuts['fixture_baseline'];last=cuts['directory_cleanup'];terminal=cuts.get('finish',cuts.get('cancel_drained'))
        summary.append({'n':c['n'],'mode':c['mode'],'pinned':c['pinned'],'calls':x['Calls'],'source_retirement_cells_pre_eof':x['MaxSourceRetirementCells'],'producer_retirement_logical_payload_lower_bound_bytes':8*x['MaxRetirementCells'],'max_window':x['MaxWindow'],'max_flat_retirement_capacity':x['MaxFlatRetiredCap'],'aggregate_forced_gc_heap_alloc_cut_max':max(s['HeapAlloc'] for s in x['cuts']),'sampled_maintenance_rss_peak':x['SampledMaintenanceRSSPeak'],'whole_process_hwm_includes_fixture':max(s['ProcessHWM'] for s in x['cuts']),'process_total_alloc_through_directory_cleanup':last['TotalAlloc']-start['TotalAlloc'],'fixture_total_alloc':baseline['TotalAlloc']-start['TotalAlloc'],'maintenance_through_final_cleanup_total_alloc':last['TotalAlloc']-baseline['TotalAlloc'],'maintenance_through_finish_or_cancel_total_alloc':terminal['TotalAlloc']-baseline['TotalAlloc'],'close_reopen_final_cleanup_total_alloc':last['TotalAlloc']-terminal['TotalAlloc'],'process_mallocs_through_directory_cleanup':last['Mallocs']-start['Mallocs']})
    return {'labels':receipt['labels'],'cases':summary}

def validate_packet(out, root=None):
    receipt=json.loads((out/'receipt.json').read_text())
    source=json.loads((out/'source-bindings.json').read_text())
    require(type(receipt.get('source_count')) is int and receipt['source_count']==len(source) and len(source)>0,'missing source manifest')
    require(type(receipt.get('smoke')) is bool and type(receipt.get('race')) is bool and type(receipt.get('n')) is int and receipt['n']>=64,'invalid run metadata')
    labels=receipt.get('labels');require(isinstance(labels,dict) and all(isinstance(labels.get(k),str) and labels[k] for k in ('heap','rss','hwm','retirement_bytes','allocations','reader','output','control')),'missing measurement scope labels')
    require(receipt['contract']==CONTRACT and receipt['source_digest']==digest(source) and receipt['source_stable'] is True,'invalid source receipt')
    if root is not None:same_source(source,bindings(root))
    capture=pathlib.Path(receipt['capture_out']);require(capture.is_absolute(),'capture directory')
    for file,key in [('native-memory.test','binary_sha256'),('build-command.json','build_command_sha256'),('build.log','build_log_sha256')]:
        require(sha(out/file)==receipt[key],'missing/drifted build artifact '+file)
    build=json.loads((out/'build-command.json').read_text());go=receipt['go'];env=receipt['build_env']
    require(build['go']==go and type(go['path']) is str and pathlib.Path(go['path']).is_absolute() and type(go['version']) is str and go['version'].startswith('go version ') and type(go['sha256']) is str and len(go['sha256'])==64 and all(c in '0123456789abcdef' for c in go['sha256']),'Go identity')
    require(build['env']==env and all(k in env for k in BUILD_KEYS) and all(env[k]==v for k,v in {'CGO_ENABLED':'1','GOFLAGS':'-p=2','GOWORK':'off','GOTOOLCHAIN':'local','GOROOT':None,'GOGC':None,'GOMEMLIMIT':None,'GODEBUG':None}.items()),'controlled build environment')
    require(build['cwd']==receipt['source_root'] and build['argv']==[go['path'],'test','-c']+(['-race'] if receipt['race'] else [])+['-tags','treedb_test,mvcc_native_memory','-o',str(capture/'native-memory.test'),'./TreeDB/mvcc'],'build invocation')
    require(receipt.get('errors') == [],'receipt errors')
    cases=receipt['cases'];require(isinstance(cases,list) and cases,'missing cases')
    n=receipt['n']
    expected={(n,'prune',True),(2*n,'prune',True),(n,'cancel',True),(n,'control',True)} if receipt['smoke'] else {(size,mode,pin) for size in (n,2*n) for mode in ('prune','control','cancel') for pin in (False,True)}
    for c in cases:
        require(type(c['n']) is int and type(c['mode']) is str and type(c['pinned']) is bool and type(c['exit_code']) is int,'invalid case identity/process types')
    require(len(cases)==len(expected) and {(c['n'],c['mode'],c['pinned']) for c in cases}==expected,'missing/duplicate lifecycle cases')
    results=[]
    for c in cases:
        require(pathlib.Path(c['raw']).name==c['raw'] and pathlib.Path(c['result']).name==c['result'],'invalid artifact path')
        raw=out/c['raw'];require(raw.is_file() and hashlib.sha256(raw.read_bytes()).hexdigest()==c['raw_sha256'],'missing/drifted raw log')
        result=out/c['result'];require(result.is_file() and hashlib.sha256(result.read_bytes()).hexdigest()==c['result_sha256'],'missing/drifted result')
        require(c['exit_code']==0 and c['source_digest_before']==c['source_digest_after']==receipt['source_digest'],'failed/drifted process')
        require(type(c['command']) is str and pathlib.Path(c['command']).name==c['command'] and sha(out/c['command'])==c['command_sha256'],'missing/drifted case command')
        require(c['binary_before_sha256']==c['binary_after_sha256']==receipt['binary_sha256'],'case binary stability')
        command=json.loads((out/c['command']).read_text())
        require(command['cwd']==build['cwd'] and command['argv']==[str(capture/'native-memory.test'),'-test.run','^TestNativePruneMemoryLifecycle$','-test.count=1','-test.timeout=120s','-test.v'],'case invocation')
        require(command['env']==dict(env,MVCC_MEMORY_RESULT=str(capture/c['result']),MVCC_MEMORY_N=str(c['n']),MVCC_MEMORY_MODE=c['mode'],MVCC_MEMORY_PINNED=str(int(c['pinned']))),'case environment')
        results.append(validate_case(json.loads(result.read_text()),c['n'],c['mode'],c['pinned']))
    require(len({c['result'] for c in cases})==len(cases),'case overwritten')
    summary=json.loads((out/'summary.json').read_text())
    require(digest(summary)==digest(summarize(receipt,results)),'missing/drifted derived summary')
    return receipt

def self_test(out):
    r=validate_packet(out);c=r['cases'][0];x=json.loads((out/c['result']).read_text())
    validate_case(x,c['n'],c['mode'],c['pinned'])
    witness_case=next((c for c in r['cases'] if c['mode']!='control'),None)
    require(witness_case is not None,'self-test needs genuine partial-output evidence')
    witness=json.loads((out/witness_case['result']).read_text())
    validate_case(witness,witness_case['n'],witness_case['mode'],witness_case['pinned'])
    witness_faults=[('allocator-zero',('Private','AllocatedPages'),0),('retirement-zero',('Private','RetirementCells'),0),('native-pages-zero',('Private','Native','ObservedOutputPages'),0),('native-buffer-zero',('Private','Native','ObservedOutputBufferBytes'),0)]
    for name,path,value in witness_faults:
        y=json.loads(json.dumps(witness));cut=next(s for s in y['cuts'] if s['name']=='partial_private_output');target=cut['State']
        for key in path[:-1]:target=target[key]
        target[path[-1]]=value
        try:validate_case(y,witness_case['n'],witness_case['mode'],witness_case['pinned'])
        except (ValueError,KeyError,TypeError):pass
        else:raise ValueError('inconsistent genuine witness accepted: '+name)
    bad=[]
    y=dict(x);del y['cuts'];bad.append(y)
    y=dict(x);y['MaxRecords']='32';bad.append(y)
    y=dict(x);y['MaxRecords']=33;bad.append(y)
    y=dict(x);y['schema']='native-prune-schema1';bad.append(y)
    for y in bad:
        try:validate_case(y,c['n'],c['mode'],c['pinned'])
        except (ValueError,KeyError,TypeError):pass
        else:raise ValueError('malformed evidence accepted')
    try:same_source({'real':'hash'},{'real':'changed'})
    except ValueError:pass
    else:raise ValueError('source drift accepted')
    try:json.loads('{')
    except json.JSONDecodeError:pass
    else:raise ValueError('malformed JSON accepted')
    # Parser fixtures copy the complete real packet; measurements stay untouched.
    with tempfile.TemporaryDirectory(prefix='native-memory-validator-') as tmp:
        tmp=pathlib.Path(tmp)
        for name in {'source-bindings.json','native-memory.test','build-command.json','build.log','summary.json'} | {c[k] for c in r['cases'] for k in ('raw','result','command')}:
            shutil.copyfile(out/name,tmp/name)
        (tmp/'receipt.json').write_text(json.dumps(r));validate_packet(tmp)
        witness_index=r['cases'].index(witness_case)
        for fault in ('missing','malformed','raw-drift','source-drift','allocator-zero','retirement-zero','native-pages-zero','native-buffer-zero','missing-case','duplicate-case','replacement-duplicate','bool-exit','bool-n','invalid-pinned','invalid-mode','receipt-errors','binary-drift','build-command-drift','build-log-drift','case-command-drift','binary-stability','build-argv','case-argv','case-env','inflated-overall-peak','inflated-source-peak','peak-call','source-peak-phase','overall-peak-source-phase','schema-v1','rss-peak-inflated','rss-witness-missing','rss-witness-type','rss-witness-zero','rss-witness-call','rss-witness-alignment','rss-schedule-missing','rss-schedule-type','summary-missing','summary-malformed','summary-value','summary-case','summary-label','summary-type','summary-extra'):
            shutil.copyfile(out/'summary.json',tmp/'summary.json')
            original=(out/witness_case['result']).read_bytes();(tmp/witness_case['result']).write_bytes(original)
            shutil.copyfile(out/witness_case['raw'],tmp/witness_case['raw']);shutil.copyfile(out/'source-bindings.json',tmp/'source-bindings.json')
            shutil.copyfile(out/witness_case['command'],tmp/witness_case['command']);shutil.copyfile(out/'build-command.json',tmp/'build-command.json')
            test_receipt=json.loads(json.dumps(r));case=test_receipt['cases'][witness_index]
            if fault=='missing':(tmp/case['result']).unlink()
            elif fault=='malformed':
                (tmp/case['result']).write_text('{');case['result_sha256']=hashlib.sha256(b'{').hexdigest()
            elif fault=='raw-drift':(tmp/case['raw']).write_bytes(b'changed raw witness')
            elif fault=='source-drift':(tmp/'source-bindings.json').write_text('{}')
            elif fault.startswith('summary-'):
                if fault=='summary-missing':(tmp/'summary.json').unlink()
                elif fault=='summary-malformed':(tmp/'summary.json').write_text('{')
                else:
                    altered=json.loads((out/'summary.json').read_text())
                    if fault=='summary-value':altered['cases'][0]['sampled_maintenance_rss_peak']+=1
                    elif fault=='summary-case':altered['cases'].pop()
                    elif fault=='summary-label':altered['labels']['rss']='changed scope'
                    elif fault=='summary-type':altered['cases'][0]['calls']=float(altered['cases'][0]['calls'])
                    else:altered['unbound']='extra'
                    (tmp/'summary.json').write_text(json.dumps(altered))
            elif fault=='missing-case':test_receipt['cases'].pop()
            elif fault=='duplicate-case':test_receipt['cases'].append(dict(case))
            elif fault=='replacement-duplicate':test_receipt['cases'][-1]=dict(test_receipt['cases'][0])
            elif fault=='bool-exit':case['exit_code']=False
            elif fault=='bool-n':case['n']=True
            elif fault=='invalid-pinned':case['pinned']=1
            elif fault=='invalid-mode':case['mode']=None
            elif fault in ('inflated-overall-peak','inflated-source-peak','peak-call','source-peak-phase','overall-peak-source-phase','schema-v1','rss-peak-inflated','rss-witness-missing','rss-witness-type','rss-witness-zero','rss-witness-call','rss-witness-alignment','rss-schedule-missing','rss-schedule-type'):
                altered=json.loads(original)
                if fault=='inflated-overall-peak':altered['MaxRetirementCells']+=1
                elif fault=='inflated-source-peak':altered['MaxSourceRetirementCells']+=1
                elif fault=='peak-call':altered['RetirementPeak']['Call']=altered['Calls']+1
                elif fault=='source-peak-phase':altered['SourceRetirementPeak']['State']['Accepted']=True
                elif fault=='overall-peak-source-phase':
                    altered['MaxRetirementCells']=max(altered['MaxRetirementCells'],altered['MaxSourceRetirementCells']+1)
                    peak=altered['RetirementPeak']['State'];peak['Private']['RetirementCells']=altered['MaxRetirementCells'];peak['EOF']=False;peak['Accepted']=False
                elif fault=='rss-peak-inflated':altered['SampledMaintenanceRSSPeak']+=1
                elif fault=='rss-witness-missing':del altered['RSSPeriodicPeak']
                elif fault=='rss-witness-type':altered['RSSPeriodicPeak']['RSS']=True
                elif fault=='rss-witness-zero':altered['RSSPeriodicPeak']={'Call':0,'RSS':0}
                elif fault=='rss-witness-call':altered['RSSPeriodicPeak']['Call']=altered['Calls']+128
                elif fault=='rss-witness-alignment':altered['RSSPeriodicPeak']['Call']=129
                elif fault=='rss-schedule-missing':altered['RSSPeriodicSamples']+=1
                elif fault=='rss-schedule-type':altered['RSSPeriodicSamples']=True
                else:altered['schema']='gomap-native-memory-v1'
                (tmp/case['result']).write_text(json.dumps(altered));case['result_sha256']=sha(tmp/case['result'])
            elif fault=='receipt-errors':test_receipt['errors']=['rejected']
            elif fault=='binary-drift':test_receipt['binary_sha256']='0'*64
            elif fault=='build-command-drift':test_receipt['build_command_sha256']='0'*64
            elif fault=='build-log-drift':test_receipt['build_log_sha256']='0'*64
            elif fault=='case-command-drift':case['command_sha256']='0'*64
            elif fault=='binary-stability':case['binary_after_sha256']='0'*64
            elif fault=='build-argv':
                altered=json.loads((tmp/'build-command.json').read_text());altered['argv'].append('-invalid')
                (tmp/'build-command.json').write_text(json.dumps(altered));test_receipt['build_command_sha256']=sha(tmp/'build-command.json')
            elif fault in ('case-argv','case-env'):
                altered=json.loads((tmp/case['command']).read_text())
                if fault=='case-argv':altered['argv'].append('-invalid')
                else:altered['env']['MVCC_MEMORY_PINNED']='0' if case['pinned'] else '1'
                (tmp/case['command']).write_text(json.dumps(altered));case['command_sha256']=sha(tmp/case['command'])
            else:
                # Truthful fixture checksums force refusal at the custody check.
                mutated=json.loads(original);cut=next(s for s in mutated['cuts'] if s['name']=='partial_private_output');target=cut['State']
                path=next(path for name,path,value in witness_faults if name==fault)
                for key in path[:-1]:target=target[key]
                target[path[-1]]=0
                content=(json.dumps(mutated)+'\n').encode();(tmp/case['result']).write_bytes(content)
                case['result_sha256']=hashlib.sha256(content).hexdigest()
            (tmp/'receipt.json').write_text(json.dumps(test_receipt))
            try:validate_packet(tmp)
            except (ValueError,KeyError,TypeError,OSError,json.JSONDecodeError):pass
            else:raise ValueError('invalid packet accepted: '+fault)
    print('failclosed genuine allocator/retirement/native-output consistency and malformed/missing/bounds/schema/source-drift checks PASS')

def run(args):
    root=args.root.resolve();out=args.out.resolve();out.mkdir(parents=True,exist_ok=False)
    before=bindings(root);source_digest=digest(before)
    (out/'source-bindings.json').write_text(json.dumps(before,indent=2)+'\n')
    env={k:v for k,v in os.environ.items() if not k.startswith('MVCC_MEMORY_')};env.pop('GOROOT',None);env.update(CGO_ENABLED='1',GOWORK='off',GOTOOLCHAIN='local',GOFLAGS='-p=2')
    for key in ('GOGC','GOMEMLIMIT','GODEBUG'):env.pop(key,None)
    go_path=pathlib.Path(shutil.which(args.go,path=env.get('PATH')) or args.go).resolve();require(go_path.is_file(),'Go executable')
    env['PATH']=str(go_path.parent)+os.pathsep+env.get('PATH','')
    version=subprocess.run([str(go_path),'version'],cwd=root,env=env,capture_output=True,text=True,check=True).stdout.strip()
    go={'path':str(go_path),'version':version,'sha256':sha(go_path)};build_env={k:env.get(k) for k in BUILD_KEYS}
    binary=out/'native-memory.test'
    build=[str(go_path),'test','-c']+(['-race'] if args.race else [])+['-tags','treedb_test,mvcc_native_memory','-o',str(binary),'./TreeDB/mvcc']
    (out/'build-command.json').write_text(json.dumps({'cwd':str(root),'argv':build,'go':go,'env':build_env},indent=2)+'\n')
    p=subprocess.run(build,cwd=root,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=180)
    (out/'build.log').write_bytes(p.stdout);require(p.returncode==0,'harness build failed');same_source(before,bindings(root));binary_sha=sha(binary)
    cases=[];errors=[]
    matrix=[(args.n,'prune',True),(2*args.n,'prune',True),(args.n,'cancel',True),(args.n,'control',True)] if args.smoke else [(n,mode,pin) for n in (args.n,2*args.n) for mode in ('prune','control','cancel') for pin in (False,True)]
    for n,mode,pin in matrix:
        try:stable=before==bindings(root) and sha(binary)==binary_sha and sha(go_path)==go['sha256']
        except OSError:stable=False
        if not stable:
            errors.append(f'source/executable drift before {mode}-{n}-pinned{int(pin)}');break
        name=f'{mode}-{n}-pinned{int(pin)}';result=out/(name+'.json');raw=out/(name+'.log')
        case_env=dict(env);case_env.update(MVCC_MEMORY_RESULT=str(result),MVCC_MEMORY_N=str(n),MVCC_MEMORY_MODE=mode,MVCC_MEMORY_PINNED=str(int(pin)))
        cmd=[str(binary),'-test.run','^TestNativePruneMemoryLifecycle$','-test.count=1','-test.timeout=120s','-test.v']
        command=out/(name+'-command.json');command.write_text(json.dumps({'cwd':str(root),'argv':cmd,'env':dict(build_env,**{k:v for k,v in case_env.items() if k.startswith('MVCC_MEMORY_')})},indent=2)+'\n')
        start=time.monotonic();process_error=''
        try:p=subprocess.run(cmd,cwd=root,env=case_env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=150)
        except (subprocess.TimeoutExpired,OSError) as e:
            process_error=str(e);p=subprocess.CompletedProcess(cmd,-1,(getattr(e,'output',None) or b'')+('\nDRIVER ERROR: '+process_error+'\n').encode())
        raw.write_bytes(p.stdout)
        after=bindings(root)
        if before!=after:(out/'drifted-source-bindings.json').write_text(json.dumps(after,indent=2)+'\n')
        try:stable=before==after and sha(binary)==binary_sha and sha(go_path)==go['sha256']
        except OSError:stable=False
        cases.append({'command':command.name,'command_sha256':sha(command),'binary_before_sha256':binary_sha,'binary_after_sha256':sha(binary) if binary.is_file() else None,'n':n,'mode':mode,'pinned':pin,'process_error':process_error,'raw':raw.name,'raw_sha256':hashlib.sha256(raw.read_bytes()).hexdigest(),'result':result.name,'result_sha256':hashlib.sha256(result.read_bytes()).hexdigest() if result.is_file() else None,'exit_code':p.returncode,'elapsed_seconds':time.monotonic()-start,'source_digest_before':source_digest,'source_digest_after':digest(after)})
        try:
            require(stable,'source/executable drift after '+name);require(p.returncode==0,'case failed: '+name);require(result.is_file(),'case missing result: '+name)
            validate_case(json.loads(result.read_text()),n,mode,pin)
        except (ValueError,KeyError,TypeError,OSError) as e:
            errors.append(str(e));break
        print(name+' PASS',flush=True)
    receipt={'contract':CONTRACT,'source_root':str(root),'capture_out':str(out),'go':go,'build_env':build_env,'binary_sha256':binary_sha,'build_command_sha256':sha(out/'build-command.json'),'build_log_sha256':sha(out/'build.log'),'source_digest':source_digest,'source_count':len(before),'source_stable':before==bindings(root),'errors':errors,'n':args.n,'smoke':args.smoke,'race':args.race,'cases':cases,'labels':{'heap':'aggregate forced-GC Go runtime, instrumentation included','rss':'aggregate sampled process RSS; maintenance peak sampled at cuts/every128 quanta','hwm':'whole-process VmHWM includes fixture','retirement_bytes':'RetirementCells*8 logical cell payload lower bound; exclusive native tree heap GAP','allocations':'process_start through directory_cleanup includes discovery, ACK, finish, cancel, cursor Close, DB Close/reopen/final Close; fixture scope separable at fixture_baseline','reader':'pinned cases read an actually deleted old physical version before release; unpinned cases close their real baseline reader','output':'fixed three surviving records; actual noncoalesced page storage may scale. OutputBufferBytes observes one actual held buffer, not total private output memory; allocator Count is exact actual private page-owner count','control':'same source shape, no discard floor; retains input history'}}
    (out/'receipt.json').write_text(json.dumps(receipt,indent=2)+'\n')
    results=[json.loads((out/c['result']).read_text()) for c in cases]
    (out/'summary.json').write_text(json.dumps(summarize(receipt,results),indent=2)+'\n')
    validate_packet(out,root);self_test(out)

if __name__=='__main__':
    a=argparse.ArgumentParser(description=__doc__);a.add_argument('--root',type=pathlib.Path,default=pathlib.Path(__file__).resolve().parents[1]);a.add_argument('--out',type=pathlib.Path,required=True);a.add_argument('--n',type=int,default=512);a.add_argument('--go',default='go');a.add_argument('--smoke',action='store_true');a.add_argument('--race',action='store_true');a.add_argument('--validate',action='store_true');a.add_argument('--self-test',action='store_true');args=a.parse_args()
    try:
        require(args.n>=64,'N must be >=64')
        if args.self_test:self_test(args.out)
        elif args.validate:validate_packet(args.out,args.root.resolve());print('source-bound packet PASS')
        else:run(args)
    except (ValueError,KeyError,TypeError,OSError,json.JSONDecodeError,subprocess.SubprocessError) as e:
        print('FAIL CLOSED: '+str(e),file=sys.stderr);sys.exit(1)
