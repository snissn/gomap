#!/usr/bin/env python3
"""Opt-in aggregate memory lifecycle evidence. Fresh processes, no timing claims.

Builds one tagged test binary, runs N/2N x prune/control/cancel x reader-unpinned/
pinned in distinct processes, retains exact source identity and raw failures.
Control has identical input shape and no discard floor; it retains all history.
This separate contract does not qualify the native-prune benchmark schema.
"""
import argparse, hashlib, json, os, pathlib, shutil, subprocess, sys, tempfile, time

CONTRACT = 'gomap-native-memory-v1'

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

def validate_case(x, n, mode, pinned):
    require(isinstance(x, dict), 'result must be object')
    required = ('schema','n','mode','pinned','pid','Calls','Records','Bytes','Pruned','MaxRecords','MaxBytes','MaxRetirementCells','MaxSourceRetirementCells','MaxWindow','MaxFrames','MaxFlatRetiredCap','SampledMaintenanceRSSPeak','PhysicalBefore','PhysicalAfter','FixedSurvivors','PointerOracle','ReaderOracle','ReopenOracle','CleanupOracle','CursorCloseOracle','PartialOutput','RelaxedCustody','ChargedCancel','cuts')
    require(all(k in x for k in required), 'missing result field')
    require((x['schema'],x['n'],x['mode'],x['pinned']) == (CONTRACT,n,mode,pinned), 'case identity mismatch')
    for k in required:
        if k not in ('schema','mode','pinned','cuts') and not k.endswith('Oracle') and k not in ('PartialOutput','RelaxedCustody','ChargedCancel'):
            require(type(x[k]) is int and x[k] >= 0, 'invalid numeric field '+k)
    for k in ('pinned','PointerOracle','ReaderOracle','ReopenOracle','CleanupOracle','CursorCloseOracle','PartialOutput','RelaxedCustody','ChargedCancel'):
        require(type(x[k]) is bool, 'invalid boolean field '+k)
    require(x['pid'] > 0 and x['Calls'] > 0, 'missing process/work witness')
    require(x['MaxRecords'] <= 32 and x['MaxBytes'] <= 1<<20 and x['MaxWindow'] <= 32 and x['MaxFrames'] <= 64 and x['MaxFlatRetiredCap'] == 0, 'bounded-work/descriptor failure')
    require(all(x[k] for k in ('PointerOracle','ReaderOracle','ReopenOracle','CleanupOracle','CursorCloseOracle')), 'missing lifecycle oracle')
    require(x['PhysicalBefore'] == n+2 and x['FixedSurvivors'] == 3, 'fixture shape mismatch')
    require(x['PhysicalAfter'] == (3 if mode == 'prune' else n+2) and x['Pruned'] == (n-1 if mode == 'prune' else 0), 'physical/ACK mismatch')
    if mode != 'control':
        require(x['PartialOutput'] and x['MaxSourceRetirementCells'] >= n//8, 'missing actual physical-page/output witness')
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
        p=a.get('Private');require(isinstance(p,dict) and isinstance(p.get('Native'),dict),'missing private sample')
        z=p['Native']
        for owner, bools, nums in ((a,('Native','Accepted','EOF'),('Phase','InputCount','Chunk')),(p,('Build',),('AllocatedPages','Dependencies','FlatRetiredLen','FlatRetiredCap','RetirementCells')),(z,(),('Window','Frames','FlatRetiredLen','FlatRetiredCap','DecodedLeafBytes','ObservedOutputPages','ObservedOutputBufferBytes'))):
            require(all(k in owner and type(owner[k]) is bool for k in bools),'missing/malformed owner booleans')
            require(all(k in owner and type(owner[k]) is int and owner[k]>=0 for k in nums),'missing/malformed owner counts')
        require(p.get('FlatRetiredCap')==0 and z.get('FlatRetiredCap')==0 and 0 <= z.get('Window',-1) <= 32 and 0 <= a.get('InputCount',-1)<=32,'sample descriptor failure')
        require(s['OutputPages']==z['ObservedOutputPages'] and s['OutputBufferBytes']==z['ObservedOutputBufferBytes'],'contradictory output sample')
        require(z['ObservedOutputPages'] in (0,1) and (z['ObservedOutputPages']==0)==(z['ObservedOutputBufferBytes']==0),'invalid scalar output observation')
        require(z['DecodedLeafBytes'] in (0,4096),'invalid retained decoded leaf size')
        require(z['Frames']<=x['MaxFrames'] and max(z['Window'],a['InputCount'])<=x['MaxWindow'] and p['RetirementCells']<=x['MaxRetirementCells'],'sample exceeds reported maximum')
        if not a['EOF'] and not a['Accepted']:require(p['RetirementCells']<=x['MaxSourceRetirementCells'],'source retirement sample exceeds reported maximum')
        if s['name']=='partial_private_output':
            require(a['Native'] and p['Build'] and not a['EOF'] and not a['Accepted'] and p['AllocatedPages']>0 and p['RetirementCells']>=n//8 and z['ObservedOutputPages']==1 and z['ObservedOutputBufferBytes']>0,'missing actual allocated-page/source-retirement/partial-output witness')
        if s['name']=='accepted_relaxed':require(a.get('Native') and a.get('Accepted') and p.get('Build'),'missing real DB custody')
        if s['name'] in ('finish','cancel_drained','cursor_close','db_owned_cleanup','reader_released','db_close','reopen','final_db_close','directory_cleanup'):
            require(not a['Native'] and not a['Accepted'] and not p['Build'] and p['AllocatedPages']==0 and p['Dependencies']==0 and p['RetirementCells']==0 and all(z[k]==0 for k in ('Window','Frames','FlatRetiredLen','FlatRetiredCap','DecodedLeafBytes','ObservedOutputPages','ObservedOutputBufferBytes')),'terminal owner retained')
    require(cuts[-1]['name']=='directory_cleanup','final DB Close/cleanup allocations excluded')
    return x

def validate_packet(out, root=None):
    receipt=json.loads((out/'receipt.json').read_text())
    source=json.loads((out/'source-bindings.json').read_text())
    require(type(receipt.get('source_count')) is int and receipt['source_count']==len(source) and len(source)>0,'missing source manifest')
    require(type(receipt.get('smoke')) is bool and type(receipt.get('race')) is bool and type(receipt.get('n')) is int and receipt['n']>=64,'invalid run metadata')
    labels=receipt.get('labels');require(isinstance(labels,dict) and all(isinstance(labels.get(k),str) and labels[k] for k in ('heap','rss','hwm','retirement_bytes','allocations','reader','output','control')),'missing measurement scope labels')
    require(receipt['contract']==CONTRACT and receipt['source_digest']==digest(source) and receipt['source_stable'] is True,'invalid source receipt')
    if root is not None:same_source(source,bindings(root))
    cases=receipt['cases'];require(isinstance(cases,list) and cases,'missing cases')
    for c in cases:
        require(pathlib.Path(c['raw']).name==c['raw'] and pathlib.Path(c['result']).name==c['result'],'invalid artifact path')
        raw=out/c['raw'];require(raw.is_file() and hashlib.sha256(raw.read_bytes()).hexdigest()==c['raw_sha256'],'missing/drifted raw log')
        result=out/c['result'];require(result.is_file() and hashlib.sha256(result.read_bytes()).hexdigest()==c['result_sha256'],'missing/drifted result')
        require(c['exit_code']==0 and c['source_digest_before']==c['source_digest_after']==receipt['source_digest'],'failed/drifted process')
        validate_case(json.loads(result.read_text()),c['n'],c['mode'],c['pinned'])
    require(len({c['result'] for c in cases})==len(cases),'case overwritten')
    if not receipt['smoke']:
        n=receipt['n']; require({(c['n'],c['mode'],c['pinned']) for c in cases}=={(size,mode,pin) for size in (n,2*n) for mode in ('prune','control','cancel') for pin in (False,True)},'missing paired lifecycle cases')
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
    # Parser fixtures use a copy of one real successful case; they are never
    # published as lifecycle measurements or substituted for actual evidence.
    with tempfile.TemporaryDirectory(prefix='native-memory-validator-') as tmp:
        tmp=pathlib.Path(tmp)
        for name in ('source-bindings.json',c['raw'],c['result']):shutil.copyfile(out/name,tmp/name)
        rr=dict(r);rr['cases']=[dict(c)];rr['smoke']=True
        (tmp/'receipt.json').write_text(json.dumps(rr));validate_packet(tmp)
        for fault in ('missing','malformed','raw-drift','source-drift','allocator-zero','retirement-zero','native-pages-zero','native-buffer-zero'):
            original=(out/c['result']).read_bytes();(tmp/c['result']).write_bytes(original)
            shutil.copyfile(out/c['raw'],tmp/c['raw']);shutil.copyfile(out/'source-bindings.json',tmp/'source-bindings.json')
            test_receipt=json.loads(json.dumps(rr))
            if fault=='missing':(tmp/c['result']).unlink()
            elif fault=='malformed':
                (tmp/c['result']).write_text('{')
                test_receipt['cases'][0]['result_sha256']=hashlib.sha256(b'{').hexdigest()
            elif fault=='raw-drift':(tmp/c['raw']).write_bytes(b'changed raw witness')
            elif fault=='source-drift':(tmp/'source-bindings.json').write_text('{}')
            else:
                # Keep every checksum and case identity truthful to the mutated
                # parser fixture, so refusal must come from custody semantics.
                require(c['mode']!='control','packet self-test needs partial-output first case')
                mutated=json.loads(original);cut=next(s for s in mutated['cuts'] if s['name']=='partial_private_output');target=cut['State']
                path=next(path for name,path,value in witness_faults if name==fault)
                for key in path[:-1]:target=target[key]
                target[path[-1]]=0
                content=(json.dumps(mutated)+'\n').encode();(tmp/c['result']).write_bytes(content)
                test_receipt['cases'][0]['result_sha256']=hashlib.sha256(content).hexdigest()
            (tmp/'receipt.json').write_text(json.dumps(test_receipt))
            try:validate_packet(tmp)
            except (ValueError,KeyError,TypeError,OSError,json.JSONDecodeError):pass
            else:raise ValueError('invalid packet accepted: '+fault)
    print('failclosed genuine allocator/retirement/native-output consistency and malformed/missing/bounds/schema/source-drift checks PASS')

def run(args):
    root=args.root.resolve();out=args.out.resolve();out.mkdir(parents=True,exist_ok=False)
    before=bindings(root);source_digest=digest(before)
    (out/'source-bindings.json').write_text(json.dumps(before,indent=2)+'\n')
    env=dict(os.environ);env.pop('GOROOT',None);env.update(GOWORK='off',GOTOOLCHAIN='local',GOFLAGS='-p=2')
    binary=out/'native-memory.test'
    build=[args.go,'test','-c']+(['-race'] if args.race else [])+['-tags','treedb_test,mvcc_native_memory','-o',str(binary),'./TreeDB/mvcc']
    (out/'build-command.json').write_text(json.dumps({'cwd':str(root),'argv':build},indent=2)+'\n')
    p=subprocess.run(build,cwd=root,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=180)
    (out/'build.log').write_bytes(p.stdout);require(p.returncode==0,'harness build failed');same_source(before,bindings(root))
    cases=[]
    matrix=[(args.n,'prune',True),(2*args.n,'prune',True),(args.n,'cancel',True),(args.n,'control',True)] if args.smoke else [(n,mode,pin) for n in (args.n,2*args.n) for mode in ('prune','control','cancel') for pin in (False,True)]
    for n,mode,pin in matrix:
        same_source(before,bindings(root));name=f'{mode}-{n}-pinned{int(pin)}';result=out/(name+'.json');raw=out/(name+'.log')
        case_env=dict(env);case_env.update(MVCC_MEMORY_RESULT=str(result),MVCC_MEMORY_N=str(n),MVCC_MEMORY_MODE=mode,MVCC_MEMORY_PINNED=str(int(pin)))
        cmd=[str(binary),'-test.run','^TestNativePruneMemoryLifecycle$','-test.count=1','-test.timeout=120s','-test.v']
        (out/(name+'-command.json')).write_text(json.dumps({'cwd':str(root),'argv':cmd,'case_env':{k:v for k,v in case_env.items() if k.startswith('MVCC_MEMORY_')}},indent=2)+'\n')
        start=time.monotonic();p=subprocess.run(cmd,cwd=root,env=case_env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=150);raw.write_bytes(p.stdout)
        after=bindings(root)
        if before!=after:(out/'drifted-source-bindings.json').write_text(json.dumps(after,indent=2)+'\n')
        same_source(before,after);require(p.returncode==0,'case failed: '+name);require(result.is_file(),'case missing result: '+name)
        validate_case(json.loads(result.read_text()),n,mode,pin)
        cases.append({'n':n,'mode':mode,'pinned':pin,'raw':raw.name,'raw_sha256':hashlib.sha256(raw.read_bytes()).hexdigest(),'result':result.name,'result_sha256':hashlib.sha256(result.read_bytes()).hexdigest(),'exit_code':p.returncode,'elapsed_seconds':time.monotonic()-start,'source_digest_before':source_digest,'source_digest_after':digest(after)})
        print(name+' PASS',flush=True)
    receipt={'contract':CONTRACT,'source_root':str(root),'source_digest':source_digest,'source_count':len(before),'source_stable':True,'n':args.n,'smoke':args.smoke,'race':args.race,'cases':cases,'labels':{'heap':'aggregate forced-GC Go runtime, instrumentation included','rss':'aggregate sampled process RSS; maintenance peak sampled at cuts/every128 quanta','hwm':'whole-process VmHWM includes fixture','retirement_bytes':'RetirementCells*8 logical cell payload lower bound; exclusive native tree heap GAP','allocations':'process_start through directory_cleanup includes discovery, ACK, finish, cancel, cursor Close, DB Close/reopen/final Close; fixture scope separable at fixture_baseline','reader':'pinned cases read an actually deleted old physical version before release; unpinned cases close their real baseline reader','output':'fixed three surviving records; actual noncoalesced page storage may scale. OutputBufferBytes observes one actual held buffer, not total private output memory; allocator Count is exact actual private page-owner count','control':'same source shape, no discard floor; retains input history'}}
    (out/'receipt.json').write_text(json.dumps(receipt,indent=2)+'\n');validate_packet(out,root);self_test(out)
    summary=[]
    for c in cases:
        x=json.loads((out/c['result']).read_text());cuts={s['name']:s for s in x['cuts']};start=cuts['process_start'];baseline=cuts['fixture_baseline'];last=cuts['directory_cleanup'];terminal=cuts.get('finish',cuts.get('cancel_drained'))
        summary.append({'n':c['n'],'mode':c['mode'],'pinned':c['pinned'],'calls':x['Calls'],'source_retirement_cells_pre_eof':x['MaxSourceRetirementCells'],'producer_retirement_logical_payload_lower_bound_bytes':8*x['MaxRetirementCells'],'max_window':x['MaxWindow'],'max_flat_retirement_capacity':x['MaxFlatRetiredCap'],'aggregate_forced_gc_heap_alloc_cut_max':max(s['HeapAlloc'] for s in x['cuts']),'sampled_maintenance_rss_peak':x['SampledMaintenanceRSSPeak'],'whole_process_hwm_includes_fixture':max(s['ProcessHWM'] for s in x['cuts']),'process_total_alloc_through_directory_cleanup':last['TotalAlloc']-start['TotalAlloc'],'fixture_total_alloc':baseline['TotalAlloc']-start['TotalAlloc'],'maintenance_through_final_cleanup_total_alloc':last['TotalAlloc']-baseline['TotalAlloc'],'maintenance_through_finish_or_cancel_total_alloc':terminal['TotalAlloc']-baseline['TotalAlloc'],'close_reopen_final_cleanup_total_alloc':last['TotalAlloc']-terminal['TotalAlloc'],'process_mallocs_through_directory_cleanup':last['Mallocs']-start['Mallocs']})
    (out/'summary.json').write_text(json.dumps({'labels':receipt['labels'],'cases':summary},indent=2)+'\n')

if __name__=='__main__':
    a=argparse.ArgumentParser(description=__doc__);a.add_argument('--root',type=pathlib.Path,default=pathlib.Path(__file__).resolve().parents[1]);a.add_argument('--out',type=pathlib.Path,required=True);a.add_argument('--n',type=int,default=512);a.add_argument('--go',default='go');a.add_argument('--smoke',action='store_true');a.add_argument('--race',action='store_true');a.add_argument('--validate',action='store_true');a.add_argument('--self-test',action='store_true');args=a.parse_args()
    try:
        require(args.n>=64,'N must be >=64')
        if args.self_test:self_test(args.out)
        elif args.validate:validate_packet(args.out,args.root.resolve());print('source-bound packet PASS')
        else:run(args)
    except (ValueError,KeyError,TypeError,OSError,json.JSONDecodeError,subprocess.SubprocessError) as e:
        print('FAIL CLOSED: '+str(e),file=sys.stderr);sys.exit(1)
