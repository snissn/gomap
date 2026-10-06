#!/usr/bin/env python3
"""Opt-in aggregate memory lifecycle evidence. Fresh processes, no timing claims.

Builds one tagged test binary, runs N/2N x prune/control/cancel x reader-unpinned/
pinned in distinct processes, retains exact source identity and raw failures.
Control has identical input shape and no discard floor; it retains all history.
This separate contract does not qualify the native-prune benchmark schema.
"""
import argparse, hashlib, json, os, pathlib, shutil, subprocess, sys, tempfile, time

CONTRACT = 'gomap-native-memory-v3'
MEASUREMENT_LABELS = {'heap': 'aggregate forced-GC Go runtime, instrumentation included', 'rss': 'aggregate sampled approximate Linux process RSS; maintenance peak sampled at cuts/every128 quanta; periodic peak paired with same-observation VmHWM', 'hwm': 'maximum observed approximate whole-process VmHWM includes fixture; independent snapshots are not monotone or later bounds', 'retirement_bytes': 'RetirementCells*8 logical cell payload lower bound; exclusive native tree heap GAP', 'allocations': 'process_start through directory_cleanup includes discovery, ACK, finish, cancel, cursor Close, DB Close/reopen/final Close; fixture scope separable at fixture_baseline', 'reader': 'pinned prune cases read an actually deleted old physical version before release; pinned control/cancel cases read a version retained in the current tree; unpinned cases close their real baseline reader', 'output': 'prune cases have fixed three surviving records; control/cancel retain input history. Actual noncoalesced page storage may scale. OutputBufferBytes observes one actual held buffer, not total private output memory; allocator Count is exact actual private page-owner count', 'control': 'same source shape, no discard floor; retains input history'}
# The captured runner is linux/amd64: Go int is signed 64-bit; counters
# emitted as uint64 and the maintenance Phase uint8 keep their own widths.
GO_INT_LIMIT = 1 << 63
GO_UINT_LIMIT = 1 << 64
RESULT_INT_FIELDS = frozenset(('n','pid','MaxWindow','MaxFrames','MaxFlatRetiredCap','PhysicalBefore','PhysicalAfter','FixedSurvivors'))
STATE_LIMITS = ({'Phase': 1 << 8, 'InputCount': GO_INT_LIMIT, 'Chunk': GO_UINT_LIMIT},
                {k: GO_UINT_LIMIT if k == 'RetirementCells' else GO_INT_LIMIT for k in ('AllocatedPages','Dependencies','FlatRetiredLen','FlatRetiredCap','RetirementCells')},
                {k: GO_UINT_LIMIT if k in ('DecodedLeafBytes','ObservedOutputBufferBytes') else GO_INT_LIMIT for k in ('Window','Frames','FlatRetiredLen','FlatRetiredCap','DecodedLeafBytes','ObservedOutputPages','ObservedOutputBufferBytes')})
CUT_LIMITS = {k: GO_INT_LIMIT if k == 'OutputPages' else GO_UINT_LIMIT for k in ('HeapAlloc','HeapInuse','HeapObjects','TotalAlloc','Mallocs','RSS','ProcessHWM','OutputPages','OutputBufferBytes')}

def go_count(value, limit):
    return type(value) is int and 0 <= value < limit

MAINTENANCE_RSS_CUTS = frozenset(('prepared', 'partial_private_output', 'accepted_relaxed', 'cancel_requested', 'cancel_drained', 'finish', 'cursor_close', 'db_owned_cleanup'))
BUILD_KEYS = ('CGO_ENABLED', 'GOFLAGS', 'GOWORK', 'GOTOOLCHAIN', 'GOENV', 'HOME', 'PATH')
FIXED_ENV = {'CGO_ENABLED': '1', 'GOFLAGS': '-p=2', 'GOWORK': 'off', 'GOTOOLCHAIN': 'local', 'GOENV': 'off'}

def validate_environment(env, go_path):
    require(type(env) is dict and set(env) == set(BUILD_KEYS) | ({'GOMAXPROCS'} if 'GOMAXPROCS' in env else set()) and all(type(v) is str and v for v in env.values()), 'controlled build environment')
    require(all(env[k] == v for k, v in FIXED_ENV.items()), 'controlled build environment')
    require(pathlib.Path(env['HOME']).is_absolute() and env['PATH'] == str(pathlib.Path(go_path).parent) + os.pathsep + os.defpath, 'controlled build environment')
    if 'GOMAXPROCS' in env:
        value = env['GOMAXPROCS']
        require(value.isascii() and value.isdecimal() and 0 < int(value) <= 2147483647 and str(int(value)) == value, 'controlled build environment')

def controlled_environment(go_path):
    env = dict(FIXED_ENV, HOME=str(pathlib.Path.home()), PATH=str(go_path.parent) + os.pathsep + os.defpath)
    if 'GOMAXPROCS' in os.environ:
        env['GOMAXPROCS'] = os.environ['GOMAXPROCS']
    validate_environment(env, str(go_path))
    return env

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
    for index, (owner, bools, nums) in enumerate(((a,('Native','Accepted','EOF'),('Phase','InputCount','Chunk')),(p,('Build',),('AllocatedPages','Dependencies','FlatRetiredLen','FlatRetiredCap','RetirementCells')),(z,(),('Window','Frames','FlatRetiredLen','FlatRetiredCap','DecodedLeafBytes','ObservedOutputPages','ObservedOutputBufferBytes')))):
        require(all(k in owner and type(owner[k]) is bool for k in bools),'missing/malformed owner booleans')
        require(all(k in owner and go_count(owner[k],STATE_LIMITS[index][k]) for k in nums),'missing/malformed owner counts')
    require(p.get('FlatRetiredCap')==0 and z.get('FlatRetiredCap')==0 and 0 <= z.get('Window',-1) <= 32 and 0 <= a.get('InputCount',-1)<=32,'sample descriptor failure')
    require(z['ObservedOutputPages'] in (0,1) and (z['ObservedOutputPages']==0)==(z['ObservedOutputBufferBytes']==0),'invalid scalar output observation')
    require(z['DecodedLeafBytes'] in (0,4096),'invalid retained decoded leaf size')
    require(p['FlatRetiredLen']<=p['FlatRetiredCap'] and z['FlatRetiredLen']<=z['FlatRetiredCap'],'flat retirement length exceeds capacity')
    private_empty=all(p[k]==0 for k in ('AllocatedPages','Dependencies','FlatRetiredLen','FlatRetiredCap','RetirementCells')) and all(z[k]==0 for k in ('Window','Frames','FlatRetiredLen','FlatRetiredCap','DecodedLeafBytes','ObservedOutputPages','ObservedOutputBufferBytes'))
    require(p['Build'] or private_empty,'private counts without build custody')
    require(a['Native'] or (not a['Accepted'] and not a['EOF'] and all(a[k]==0 for k in ('Phase','InputCount','Chunk')) and not p['Build'] and private_empty),'counts without native custody')
    return p,z

def validate_state_bounds(a, x):
    p,z=validate_state(a)
    require(z['Frames']<=x['MaxFrames'] and max(z['Window'],a['InputCount'])<=x['MaxWindow'] and p['RetirementCells']<=x['MaxRetirementCells'],'state exceeds reported maximum')
    if not a['EOF'] and not a['Accepted']:
        require(p['RetirementCells']<=x['MaxSourceRetirementCells'],'source retirement state exceeds reported maximum')
    return p,z

def validate_case(x, n, mode, pinned):
    require(isinstance(x, dict), 'result must be object')
    required = ('schema','n','mode','pinned','pid','Calls','Records','Bytes','Pruned','MaxRecords','MaxBytes','MaxRetirementCells','MaxSourceRetirementCells','MaxWindow','MaxFrames','MaxFlatRetiredCap','SampledMaintenanceRSSPeak','RSSPeriodicPeak','RSSPeriodicSamples','PhysicalBefore','PhysicalAfter','FixedSurvivors','PointerOracle','ReaderOracle','ReopenOracle','CleanupOracle','CursorCloseOracle','PartialOutput','RelaxedCustody','ChargedCancel','RetirementPeak','SourceRetirementPeak','WindowPeak','FramePeak','cuts')
    require(all(k in x for k in required), 'missing result field')
    require((x['schema'],x['n'],x['mode'],x['pinned']) == (CONTRACT,n,mode,pinned), 'case identity mismatch')
    for k in required:
        if k not in ('schema','mode','pinned','cuts','RetirementPeak','SourceRetirementPeak','WindowPeak','FramePeak','RSSPeriodicPeak') and not k.endswith('Oracle') and k not in ('PartialOutput','RelaxedCustody','ChargedCancel'):
            require(go_count(x[k],GO_INT_LIMIT if k in RESULT_INT_FIELDS else GO_UINT_LIMIT), 'invalid numeric field '+k)
    for k in ('pinned','PointerOracle','ReaderOracle','ReopenOracle','CleanupOracle','CursorCloseOracle','PartialOutput','RelaxedCustody','ChargedCancel'):
        require(type(x[k]) is bool, 'invalid boolean field '+k)
    require(x['pid'] > 0 and x['Calls'] > 0, 'missing process/work witness')
    require(x['MaxRecords'] <= x['Records'] <= x['Calls']*x['MaxRecords'] and x['MaxBytes'] <= x['Bytes'] <= x['Calls']*x['MaxBytes'], 'work total/max/call accounting')
    if x['Pruned'] or x['PartialOutput'] or x['ChargedCancel']:
        require(all(x[k] > 0 for k in ('Records','Bytes','MaxRecords','MaxBytes')), 'missing charged physical work')
    require(x['MaxRecords'] <= 32 and x['MaxBytes'] <= 1<<20 and x['MaxWindow'] <= 32 and x['MaxFrames'] <= 64 and x['MaxFlatRetiredCap'] == 0, 'bounded-work/descriptor failure')
    require(all(x[k] for k in ('PointerOracle','ReaderOracle','ReopenOracle','CleanupOracle','CursorCloseOracle')), 'missing lifecycle oracle')
    require(x['PhysicalBefore'] == n+2 and x['FixedSurvivors'] == 3, 'fixture shape mismatch')
    require(x['PhysicalAfter'] == (3 if mode == 'prune' else n+2) and x['Pruned'] == (n-1 if mode == 'prune' else 0), 'physical/ACK mismatch')
    if mode != 'control':
        require(x['PartialOutput'] and x['MaxSourceRetirementCells'] > 0, 'missing actual physical-page/output witness')
    require(x['PartialOutput'] == (mode != 'control') and x['RelaxedCustody'] == (mode == 'prune') and x['ChargedCancel'] == (mode == 'cancel'), 'wrong-mode lifecycle witness')
    if mode == 'control':
        require(all(x[k] == 0 for k in ('MaxRetirementCells','MaxSourceRetirementCells','MaxWindow','MaxFrames')), 'control native descriptor ownership')
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
            require(k in s and go_count(s[k],CUT_LIMITS[k]),'invalid/missing memory sample '+k)
        require(s['RSS']>0 and s['ProcessHWM']>0,'missing Linux RSS/HWM observation')
        require(s['HeapAlloc']<=s['HeapInuse'],'heap allocation exceeds in-use spans')
        require(s['HeapAlloc']<=s['TotalAlloc'],'heap allocation exceeds cumulative allocation')
        require(s['HeapObjects']<=s['Mallocs'],'heap objects exceed cumulative allocations')
        require(s['RSS']<=s['ProcessHWM'],'RSS exceeds process high-water mark')
        require(s['TotalAlloc']>=previous_alloc and s['Mallocs']>=previous_malloc,'allocation counter regression')
        previous_alloc,previous_malloc=s['TotalAlloc'],s['Mallocs']
        a=s.get('State');require(isinstance(a,dict),'missing custody sample')
        p,z=validate_state_bounds(a,x)
        if mode == 'control': require(not a['Native'], 'control native custody')
        if s['name']=='prepared': require(a['Native'] and p['Build'], 'missing prepared private custody')
        if s['name']=='cancel_requested': require(a['Native'] and p['Build'] and not a['Accepted'], 'missing requested cancel custody')
        require(s['OutputPages']==z['ObservedOutputPages'] and s['OutputBufferBytes']==z['ObservedOutputBufferBytes'],'contradictory output sample')
        require(z['ObservedOutputPages'] in (0,1) and (z['ObservedOutputPages']==0)==(z['ObservedOutputBufferBytes']==0),'invalid scalar output observation')
        require(z['DecodedLeafBytes'] in (0,4096),'invalid retained decoded leaf size')
        if s['name']=='partial_private_output':
            require(a['Native'] and p['Build'] and not a['EOF'] and not a['Accepted'] and p['AllocatedPages']>0 and p['RetirementCells']>0 and z['ObservedOutputPages']==1 and z['ObservedOutputBufferBytes']>0,'missing actual allocated-page/source-retirement/partial-output witness')
        if s['name']=='accepted_relaxed':require(a.get('Native') and a.get('Accepted') and p.get('Build'),'missing real DB custody')
        if s['name'] in ('finish','cancel_drained','cursor_close','db_owned_cleanup','reader_released','db_close','reopen','final_db_close','directory_cleanup'):
            require(not a['Native'] and not a['Accepted'] and not p['Build'] and p['AllocatedPages']==0 and p['Dependencies']==0 and p['RetirementCells']==0 and all(z[k]==0 for k in ('Window','Frames','FlatRetiredLen','FlatRetiredCap','DecodedLeafBytes','ObservedOutputPages','ObservedOutputBufferBytes')),'terminal owner retained')
    periodic=x['RSSPeriodicPeak']
    require(type(periodic) is dict and set(periodic)=={'Call','RSS','ProcessHWM'} and all(go_count(periodic[k],GO_UINT_LIMIT) for k in periodic),'missing/malformed periodic RSS/HWM witness')
    require(x['RSSPeriodicSamples']==x['Calls']//128,'missing periodic RSS schedule coverage')
    if x['RSSPeriodicSamples']==0:
        require(all(periodic[k]==0 for k in ('Call','RSS','ProcessHWM')),'nonzero empty periodic RSS/HWM witness')
    else:
        require(periodic['RSS']>0 and 128<=periodic['Call']<=x['Calls'] and periodic['Call']%128==0,'invalid periodic RSS witness call/sample')
    require(periodic['RSS']<=periodic['ProcessHWM'],'periodic RSS exceeds paired process high-water mark')
    observed_rss=max([periodic['RSS']]+[s['RSS'] for s in cuts if s['name'] in MAINTENANCE_RSS_CUTS])
    require(x['SampledMaintenanceRSSPeak']==observed_rss,'unwitnessed sampled maintenance RSS maximum')
    require(x['MaxSourceRetirementCells']<=x['MaxRetirementCells'],'source peak exceeds overall peak')
    for field, maximum in (('RetirementPeak','MaxRetirementCells'),('SourceRetirementPeak','MaxSourceRetirementCells'),('WindowPeak','MaxWindow'),('FramePeak','MaxFrames')):
        w=x[field];require(isinstance(w,dict) and go_count(w.get('Call'),GO_UINT_LIMIT),'missing/malformed peak witness')
        a=w.get('State');p,z=validate_state_bounds(a,x)
        observed=max(a['InputCount'],z['Window']) if field=='WindowPeak' else z['Frames'] if field=='FramePeak' else p['RetirementCells']
        require(observed==x[maximum],'unwitnessed maximum '+maximum)
        if x[maximum]==0:
            require(w['Call']==0 and not a['Native'],'nonzero empty peak witness')
        else:
            require(1<=w['Call']<=x['Calls'] and a['Native'],'invalid observed peak call/custody')
            # InputCount can precede private build creation; other positive
            # observations belong to a real private owner.
            require(field=='WindowPeak' or p['Build'],'peak without private build custody')
            require(field!='SourceRetirementPeak' or (not a['EOF'] and not a['Accepted']),'source peak after EOF/acceptance')
    require(cuts[-1]['name']=='directory_cleanup','final DB Close/cleanup allocations excluded')
    return x

def summarize(receipt, results):
    summary=[]
    for c,x in zip(receipt['cases'],results):
        cuts={s['name']:s for s in x['cuts']};start=cuts['process_start'];baseline=cuts['fixture_baseline'];last=cuts['directory_cleanup'];terminal=cuts.get('finish',cuts.get('cancel_drained'))
        summary.append({'n':c['n'],'mode':c['mode'],'pinned':c['pinned'],'calls':x['Calls'],'source_retirement_cells_pre_eof':x['MaxSourceRetirementCells'],'producer_retirement_logical_payload_lower_bound_bytes':8*x['MaxRetirementCells'],'max_window':x['MaxWindow'],'max_flat_retirement_capacity':x['MaxFlatRetiredCap'],'aggregate_forced_gc_heap_alloc_cut_max':max(s['HeapAlloc'] for s in x['cuts']),'sampled_maintenance_rss_peak':x['SampledMaintenanceRSSPeak'],'whole_process_hwm_includes_fixture':max(s['ProcessHWM'] for s in x['cuts']),'process_total_alloc_through_directory_cleanup':last['TotalAlloc']-start['TotalAlloc'],'fixture_total_alloc':baseline['TotalAlloc']-start['TotalAlloc'],'maintenance_through_final_cleanup_total_alloc':last['TotalAlloc']-baseline['TotalAlloc'],'maintenance_through_finish_or_cancel_total_alloc':terminal['TotalAlloc']-baseline['TotalAlloc'],'close_reopen_final_cleanup_total_alloc':last['TotalAlloc']-terminal['TotalAlloc'],'process_mallocs_through_directory_cleanup':last['Mallocs']-start['Mallocs']})
    return {'labels':receipt['labels'],'cases':summary}

def validate_version(out, receipt):
    for name, key in [('version-command.json', 'version_command_sha256'), ('version.log', 'version_log_sha256')]:
        value = receipt.get(key)
        require(type(value) is str and len(value) == 64 and all(c in '0123456789abcdef' for c in value) and (out / name).is_file() and sha(out / name) == value, 'version artifact binding ' + name)
    command = json.loads((out / 'version-command.json').read_text())
    go = receipt['go']
    require(type(go['path']) is str and pathlib.Path(go['path']).is_absolute() and type(go['sha256']) is str and len(go['sha256']) == 64 and all(c in '0123456789abcdef' for c in go['sha256']), 'version Go identity')
    require(command == {'cwd': receipt['source_root'], 'argv': [go['path'], 'version'], 'env': receipt['build_env'], 'go_path': go['path'], 'go_sha256': go['sha256']}, 'version invocation binding')
    require(receipt.get('version_go_before_sha256') == receipt.get('version_go_after_sha256') == go['sha256'], 'version executable stability')
    output = go.get('version_output')
    require(type(output) is str and (out / 'version.log').read_bytes() == output.encode() and type(go['version']) is str and go['version'].startswith('go version ') and output == go['version'] + '\n' and '\n' not in go['version'], 'version output binding')


def capture_version(go_path, root, env, out, errors):
    validate_environment(env, str(go_path))
    build_env = dict(env)
    before = artifact_sha(go_path)
    command = {'cwd': str(root), 'argv': [str(go_path), 'version'], 'env': build_env, 'go_path': str(go_path), 'go_sha256': before}
    (out / 'version-command.json').write_text(json.dumps(command, indent=2) + '\n')
    command_before = artifact_sha(out / 'version-command.json')
    raw = None
    go = {'path': str(go_path), 'version': None, 'version_output': None, 'sha256': before}
    if before is None:
        errors.append('Go executable unreadable before version capture')
    else:
        version, process_error = run_logged(command['argv'], root, env, out / 'version.log', 30)
        if version.returncode != 0:
            errors.append('Go version failed: ' + (process_error or str(version.returncode)))
        else:
            raw = (version.stdout or b'') + (version.stderr or b'')
            output = raw.decode(errors='replace')
            go.update(version=output.strip(), version_output=output)
            if output.encode() != raw or not go['version'].startswith('go version ') or output != go['version'] + '\n' or '\n' in go['version']:
                errors.append('invalid Go version output')
    after = artifact_sha(go_path)
    stable = before is not None and before == after
    if not stable:
        errors.append('Go executable drift during version capture')
    metadata = {'version_command_sha256': artifact_sha(out / 'version-command.json'), 'version_log_sha256': artifact_sha(out / 'version.log'), 'version_go_before_sha256': before, 'version_go_after_sha256': after}
    if command_before is None or metadata['version_command_sha256'] != command_before:
        errors.append('version command drift during capture')
    if raw is not None and metadata['version_log_sha256'] != hashlib.sha256(raw).hexdigest():
        errors.append('version output drift during capture')
    return go, build_env, metadata, stable


def validate_packet(out, root=None):
    receipt=json.loads((out/'receipt.json').read_text())
    source=json.loads((out/'source-bindings.json').read_text())
    require(type(receipt.get('source_count')) is int and receipt['source_count']==len(source) and len(source)>0,'missing source manifest')
    require(type(receipt.get('smoke')) is bool and type(receipt.get('race')) is bool and type(receipt.get('n')) is int and receipt['n']>=64,'invalid run metadata')
    require(receipt.get('labels')==MEASUREMENT_LABELS,'noncanonical measurement scope labels')
    require(receipt.get('errors') == [],'receipt errors')
    require(receipt['contract']==CONTRACT and receipt['source_digest']==digest(source) and receipt['source_stable'] is True,'invalid source receipt')
    if root is not None:same_source(source,bindings(root))
    capture=pathlib.Path(receipt['capture_out']);require(capture.is_absolute(),'capture directory')
    for file,key in [('native-memory.test','binary_sha256'),('build-command.json','build_command_sha256'),('build.log','build_log_sha256')]:
        require(sha(out/file)==receipt[key],'missing/drifted build artifact '+file)
    build=json.loads((out/'build-command.json').read_text());go=receipt['go'];env=receipt['build_env']
    require(build['go']==go and type(go['path']) is str and pathlib.Path(go['path']).is_absolute() and type(go['version']) is str and go['version'].startswith('go version ') and type(go['sha256']) is str and len(go['sha256'])==64 and all(c in '0123456789abcdef' for c in go['sha256']),'Go identity')
    require(build['env']==env,'build environment binding')
    validate_environment(env, go['path'])
    require(build['cwd']==receipt['source_root'] and build['argv']==[go['path'],'test','-c']+(['-race'] if receipt['race'] else [])+['-tags','treedb_test,mvcc_native_memory','-o',str(capture/'native-memory.test'),'./TreeDB/mvcc'],'build invocation')
    validate_version(out, receipt)
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
    r=validate_packet(out)
    environment_self_test(out, r)
    periodic_hwm_self_test(out, r)
    integer_self_test(out, r)
    c=r['cases'][0];x=json.loads((out/c['result']).read_text())
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
        for name in {'source-bindings.json','native-memory.test','build-command.json','build.log','version-command.json','version.log','summary.json'} | {c[k] for c in r['cases'] for k in ('raw','result','command')}:
            shutil.copyfile(out/name,tmp/name)
        (tmp/'receipt.json').write_text(json.dumps(r));validate_packet(tmp)
        witness_index=r['cases'].index(witness_case)
        for fault in ('missing','malformed','raw-drift','source-drift','allocator-zero','retirement-zero','native-pages-zero','native-buffer-zero','missing-case','duplicate-case','replacement-duplicate','bool-exit','bool-n','invalid-pinned','invalid-mode','receipt-errors','binary-drift','build-command-drift','build-log-drift','case-command-drift','binary-stability','build-argv','case-argv','case-env','version-command-env','version-output','heap-inuse','heap-total','heap-objects','rss-hwm','inflated-overall-peak','inflated-source-peak','peak-call','source-peak-phase','overall-peak-source-phase','schema-v1','rss-peak-inflated','rss-witness-missing','rss-witness-type','rss-witness-zero','rss-witness-call','rss-witness-alignment','rss-schedule-missing','rss-schedule-type','summary-missing','summary-malformed','summary-value','summary-case','summary-label','summary-type','summary-extra'):
            shutil.copyfile(out/'summary.json',tmp/'summary.json')
            # Each version fault starts from both original artifacts.
            shutil.copyfile(out/'version-command.json',tmp/'version-command.json')
            shutil.copyfile(out/'version.log',tmp/'version.log')
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
                elif fault=='rss-witness-zero':altered['RSSPeriodicPeak']={'Call':0,'RSS':0,'ProcessHWM':0}
                elif fault=='rss-witness-call':altered['RSSPeriodicPeak']['Call']=altered['Calls']+128
                elif fault=='rss-witness-alignment':altered['RSSPeriodicPeak']['Call']=129
                elif fault=='rss-schedule-missing':altered['RSSPeriodicSamples']+=1
                elif fault=='rss-schedule-type':altered['RSSPeriodicSamples']=True
                else:altered['schema']='gomap-native-memory-v1'
                (tmp/case['result']).write_text(json.dumps(altered));case['result_sha256']=sha(tmp/case['result'])
            elif fault in ('heap-inuse','heap-total','heap-objects','rss-hwm'):
                altered=json.loads(original);cut=altered['cuts'][0]
                if fault=='heap-inuse':cut['HeapInuse']=max(0,cut['HeapAlloc']-1)
                elif fault=='heap-total':cut['HeapAlloc']=cut['TotalAlloc']+1;cut['HeapInuse']=max(cut['HeapInuse'],cut['HeapAlloc'])
                elif fault=='heap-objects':cut['HeapObjects']=cut['Mallocs']+1
                else:cut['RSS']=cut['ProcessHWM']+1
                (tmp/case['result']).write_text(json.dumps(altered));case['result_sha256']=sha(tmp/case['result'])
            elif fault=='version-command-env':
                altered=json.loads((tmp/'version-command.json').read_text());altered['env']['GOFLAGS']='changed'
                (tmp/'version-command.json').write_text(json.dumps(altered));test_receipt['version_command_sha256']=sha(tmp/'version-command.json')
            elif fault=='version-output':
                (tmp/'version.log').write_bytes((tmp/'version.log').read_bytes()+b'changed')
                test_receipt['version_log_sha256']=sha(tmp/'version.log')
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
            # Refresh the derived summary for changed result fixtures so an
            # unrelated old-summary mismatch cannot hide a case validator gap.
            result=tmp/case['result']
            if result.is_file() and result.read_bytes()!=original:
                try:derived=summarize(test_receipt,[json.loads((tmp/c['result']).read_text()) for c in test_receipt['cases']])
                except (KeyError,TypeError,json.JSONDecodeError):pass
                else:(tmp/'summary.json').write_text(json.dumps(derived))
            try:validate_packet(tmp)
            except (ValueError,KeyError,TypeError,OSError,json.JSONDecodeError) as e:
                if fault in ('version-command-env','version-output'):
                    require(str(e)==('version invocation binding' if fault=='version-command-env' else 'version output binding'),'version refusal at wrong boundary: '+fault)
            else:raise ValueError('invalid packet accepted: '+fault)
    measurement_self_test(out,r,witness_case)
    print('failclosed genuine allocator/retirement/native-output consistency and malformed/missing/bounds/schema/source-drift checks PASS')

def measurement_self_test(out, receipt, witness_case):
    """Coupled parser fixtures; never lifecycle measurements or retained owners."""
    original=json.loads((out/witness_case['result']).read_text())
    require(original['MaxWindow']>0 and original['MaxFrames']>0,'self-test needs real positive descriptor witnesses')
    zero={'Native':False,'Accepted':False,'EOF':False,'Phase':0,'InputCount':0,'Chunk':0,
          'Private':{'Build':False,'AllocatedPages':0,'Dependencies':0,'FlatRetiredLen':0,'FlatRetiredCap':0,'RetirementCells':0,
                     'Native':{'Window':0,'Frames':0,'FlatRetiredLen':0,'FlatRetiredCap':0,'DecodedLeafBytes':0,'ObservedOutputPages':0,'ObservedOutputBufferBytes':0}}}
    peaks=('RetirementPeak','SourceRetirementPeak','WindowPeak','FramePeak')
    def clone(x):return json.loads(json.dumps(x))
    def put(x,path,value):
        for key in path[:-1]:x=x[key]
        x[path[-1]]=value
    def states(x):return [s['State'] for s in x['cuts']]+[x[k]['State'] for k in peaks]
    # Make room for an inflation inside the hard cap; all retained scalar
    # observations change consistently. This is a valid parser fixture only.
    window_fixture=clone(original)
    if window_fixture['MaxWindow']==32:
        window_fixture['MaxWindow']=31
        for a in states(window_fixture):
            a['InputCount']=min(a['InputCount'],31)
            a['Private']['Native']['Window']=min(a['Private']['Native']['Window'],31)
    frame_fixture=clone(original)
    if frame_fixture['MaxFrames']==64:
        frame_fixture['MaxFrames']=63
        for a in states(frame_fixture):a['Private']['Native']['Frames']=min(a['Private']['Native']['Frames'],63)
    prep=clone(original);a=clone(zero);a.update(Native=True,Phase=1,InputCount=prep['MaxWindow'])
    prep['WindowPeak']={'Call':1,'State':a}
    empty_frame=clone(original);empty_frame['MaxFrames']=0;empty_frame['FramePeak']={'Call':0,'State':clone(zero)}
    for a in states(empty_frame):a['Private']['Native']['Frames']=0
    empty_window=clone(original);empty_window['MaxWindow']=0;empty_window['WindowPeak']={'Call':0,'State':clone(zero)}
    for a in states(empty_window):a['InputCount']=0;a['Private']['Native']['Window']=0
    fixtures=[('window inflation base',window_fixture),('frame inflation base',frame_fixture),('InputCount before private build',prep),('zero frame peak',empty_frame),('zero window peak',empty_window)]
    negatives=[]
    def add(name,base,path,value):
        x=clone(base);put(x,path,value);negatives.append((name,x))
    add('window inflated within cap',window_fixture,('MaxWindow',),window_fixture['MaxWindow']+1)
    add('frame inflated within cap',frame_fixture,('MaxFrames',),frame_fixture['MaxFrames']+1)
    for field,maximum in (('WindowPeak','MaxWindow'),('FramePeak','MaxFrames')):
        for label,value in [('bool',True),('float',1.0),('string','1'),('negative',-1),('zero',0),('after calls',original['Calls']+1)]:
            add(field+' call '+label,original,(field,'Call'),value)
        x=clone(original);del x[field];negatives.append((field+' missing',x))
        add(field+' null',original,(field,),None)
        add(field+' malformed state',original,(field,'State'),None)
        add(maximum+' bool',original,(maximum,),True)
        add(maximum+' deflated',original,(maximum,),original[maximum]-1)
        add(field+' no native custody',original,(field,'State','Native'),False)
    add('private frame without build',original,('FramePeak','State','Private','Build'),False)
    x=clone(original);a=x['WindowPeak']['State'];a['Private']['Build']=False;a['Private']['Native']['Window']=x['MaxWindow']
    negatives.append(('window private counts without build',x))
    add('zero frame invented call',empty_frame,('FramePeak','Call'),1)
    add('zero window invented call',empty_window,('WindowPeak','Call'),1)
    add('zero frame invented owner',empty_frame,('FramePeak','State','Native'),True)
    add('zero window invented owner',empty_window,('WindowPeak','State','Native'),True)
    add('descriptor witness retirement exceeds max',original,('WindowPeak','State','Private','RetirementCells'),original['MaxRetirementCells']+1)
    x=clone(original);a=x['FramePeak']['State'];a['EOF']=False;a['Accepted']=False
    x['MaxRetirementCells']=x['MaxSourceRetirementCells']+1
    x['RetirementPeak']['State']['Private']['RetirementCells']=x['MaxRetirementCells'];x['RetirementPeak']['State']['EOF']=True
    a['Private']['RetirementCells']=x['MaxRetirementCells'];negatives.append(('descriptor witness source exceeds max',x))
    add('retirement witness frame exceeds max',frame_fixture,('RetirementPeak','State','Private','Native','Frames'),frame_fixture['MaxFrames']+1)
    add('frame witness window exceeds max',window_fixture,('FramePeak','State','InputCount'),window_fixture['MaxWindow']+1)
    for owner in ('Private','Native'):
        path=('WindowPeak','State','Private')+(('Native',) if owner=='Native' else ())+('FlatRetiredLen',)
        add(owner+' flat len without cap',original,path,1)
    add('terminal top-level count',original,('cuts',len(original['cuts'])-1,'State','InputCount'),1)
    add('terminal EOF',original,('cuts',len(original['cuts'])-1,'State','EOF'),True)
    with tempfile.TemporaryDirectory(prefix='native-memory-measurement-contract-') as tmp:
        tmp=pathlib.Path(tmp)
        for p in out.iterdir():
            if p.is_file():shutil.copyfile(p,tmp/p.name)
        def write(x):
            r=clone(receipt);p=tmp/witness_case['result'];p.write_text(json.dumps(x)+'\n')
            c=next(c for c in r['cases'] if c['result']==p.name);c['result_sha256']=sha(p)
            (tmp/'receipt.json').write_text(json.dumps(r))
            results=[json.loads((tmp/c['result']).read_text()) for c in r['cases']]
            (tmp/'summary.json').write_text(json.dumps(summarize(r,results)))
        for name,x in fixtures:
            write(x);validate_packet(tmp)
        for name,x in negatives:
            write(x)
            # Prove the case validator refuses before summary comparison.
            try:validate_case(x,witness_case['n'],witness_case['mode'],witness_case['pinned'])
            except (ValueError,KeyError,TypeError):pass
            else:raise ValueError('measurement case fixture accepted: '+name)
            try:validate_packet(tmp)
            except (ValueError,KeyError,TypeError,OSError,json.JSONDecodeError):pass
            else:raise ValueError('checksum/summary-refreshed fixture accepted: '+name)
        write(original)
        for key in MEASUREMENT_LABELS:
            r=clone(receipt);r['labels'][key]='exclusive owner heap'
            results=[json.loads((tmp/c['result']).read_text()) for c in r['cases']]
            (tmp/'receipt.json').write_text(json.dumps(r));(tmp/'summary.json').write_text(json.dumps(summarize(r,results)))
            try:validate_packet(tmp)
            except ValueError as e:require(str(e)=='noncanonical measurement scope labels','scope refusal at wrong boundary')
            else:raise ValueError('coupled scope rewrite accepted: '+key)
        for labels in (None,[],{},dict(MEASUREMENT_LABELS,extra='extra'),dict(MEASUREMENT_LABELS,heap=True)):
            r=clone(receipt);r['labels']=labels;(tmp/'receipt.json').write_text(json.dumps(r))
            try:validate_packet(tmp)
            except ValueError as e:require(str(e)=='noncanonical measurement scope labels','malformed scope refusal at wrong boundary')
            else:raise ValueError('malformed scope labels accepted')
    print('measurement contract: '+str(len(fixtures))+' positive parser fixtures, '+str(len(negatives))+' coupled case refusals, 13 canonical-label refusals PASS')


def integer_self_test(out, receipt):
    """Producer-width checks with refreshed checksums and derived summaries."""
    clone=lambda x:json.loads(json.dumps(x))
    case=receipt['cases'][0];original=json.loads((out/case['result']).read_text())
    faults=[]
    for k,v in original.items():
        if type(v) is int:
            limit=GO_INT_LIMIT if k in RESULT_INT_FIELDS else GO_UINT_LIMIT
            for value in (limit,-1,True,1.0):
                # Identity remains the first check for n; exercise its width
                # with the same caller value to reach the numeric boundary.
                faults.append(('result '+k,lambda y,k=k,value=value:y.update({k:value}),'invalid numeric field '+k,k=='n'))
    for ci,cut in enumerate(original['cuts']):
        for k,limit in CUT_LIMITS.items():
            faults.append(('cut '+k,lambda y,ci=ci,k=k,limit=limit:y['cuts'][ci].update({k:limit}),'invalid/missing memory sample '+k,False))
    containers=[(('cuts',i,'State'),cut['State']) for i,cut in enumerate(original['cuts'])]
    containers += [((k,'State'),original[k]['State']) for k in ('RetirementPeak','SourceRetirementPeak','WindowPeak','FramePeak')]
    def at(y,path):
        for key in path:y=y[key]
        return y
    for path,state in containers:
        for suffix,limits in zip(((),('Private',),('Private','Native')),STATE_LIMITS):
            for k,limit in limits.items():
                faults.append(('state '+k,lambda y,path=path,suffix=suffix,k=k,limit=limit:at(y,path+suffix).update({k:limit}),'missing/malformed owner counts',False))
    for k in ('RetirementPeak','SourceRetirementPeak','WindowPeak','FramePeak'):
        faults.append(('witness call',lambda y,k=k:y[k].update(Call=GO_UINT_LIMIT),'missing/malformed peak witness',False))
    for k in ('Call','RSS','ProcessHWM'):
        faults.append(('periodic width',lambda y,k=k:y['RSSPeriodicPeak'].update({k:GO_UINT_LIMIT}),'missing/malformed periodic RSS/HWM witness',False))
    # Width helper positive boundaries distinguish int/byte/uint, including 0;
    # semantic bounds still independently constrain complete case values.
    for limit in (1<<8,GO_INT_LIMIT,GO_UINT_LIMIT):
        require(go_count(0,limit) and go_count(limit-1,limit),'producer width boundary rejected')
        require(not any(go_count(v,limit) for v in (limit,-1,True,1.0,None)),'producer width boundary accepted')
    with tempfile.TemporaryDirectory(prefix='native-memory-integer-contract-') as folder:
        archive=pathlib.Path(folder)
        for p in out.iterdir():
            if p.is_file():shutil.copyfile(p,archive/p.name)
        for name,mutate,reason,identity in faults:
            x=clone(original);mutate(x)
            try:validate_case(x,x['n'] if identity else case['n'],case['mode'],case['pinned'])
            except ValueError as error:require(str(error)==reason,'integer refusal at wrong case boundary: '+name+' '+str(error))
            else:raise ValueError('producer integer fault accepted: '+name)
            if identity:continue  # impossible run n is checked separately by packet metadata
            r=clone(receipt);target=archive/case['result'];target.write_text(json.dumps(x)+'\n')
            r['cases'][0]['result_sha256']=sha(target)
            (archive/'receipt.json').write_text(json.dumps(r)+'\n')
            results=[json.loads((archive/c['result']).read_text()) for c in r['cases']]
            (archive/'summary.json').write_text(json.dumps(summarize(r,results))+'\n')
            try:validate_packet(archive)
            except ValueError as error:require(str(error)==reason,'integer refusal at wrong packet boundary: '+name+' '+str(error))
            else:raise ValueError('checksum/summary-refreshed producer integer fault accepted: '+name)
    print(str(len(faults))+' producer-width refusals and zero/max boundaries PASS')


def periodic_hwm_self_test(out, receipt):
    """Paired-observation parser fixtures; originals remain runtime evidence."""
    clone=lambda x:json.loads(json.dumps(x))
    periodic_case=next((c for c in receipt['cases'] if json.loads((out/c['result']).read_text())['RSSPeriodicSamples']>0),None)
    zero_case=next((c for c in receipt['cases'] if json.loads((out/c['result']).read_text())['RSSPeriodicSamples']==0),None)
    require(periodic_case is not None and zero_case is not None,'self-test needs genuine periodic and zero-periodic cases')
    original=json.loads((out/periodic_case['result']).read_text())
    zero=json.loads((out/zero_case['result']).read_text())
    pair=original['RSSPeriodicPeak']
    equal=clone(original);equal['RSSPeriodicPeak']['ProcessHWM']=pair['RSS']
    independent=clone(equal)
    # A named-cut HWM may decrease without changing same-observation bounds.
    independent['cuts'][0]['ProcessHWM']=max(s['ProcessHWM'] for s in independent['cuts'])+1
    later=clone(equal)
    # Synthetic parser-only proof that the final snapshot is NOT an envelope.
    peak=max(pair['RSS'],later['cuts'][-1]['RSS']+1)
    later['RSSPeriodicPeak']['RSS']=peak;later['RSSPeriodicPeak']['ProcessHWM']=peak
    later['cuts'][-1]['ProcessHWM']=peak-1
    later['SampledMaintenanceRSSPeak']=max([peak]+[s['RSS'] for s in later['cuts'] if s['name'] in MAINTENANCE_RSS_CUTS])
    fixtures=[('paired equality',periodic_case,equal,None),('independent decreasing HWM',periodic_case,independent,None),
              ('periodic RSS above later HWM',periodic_case,later,None),('genuine empty pair',zero_case,zero,None)]
    bad=clone(equal);bad['RSSPeriodicPeak']['RSS']+=1
    bad['SampledMaintenanceRSSPeak']=max([bad['RSSPeriodicPeak']['RSS']]+[s['RSS'] for s in bad['cuts'] if s['name'] in MAINTENANCE_RSS_CUTS])
    fixtures.append(('RSS above its paired HWM',periodic_case,bad,'periodic RSS exceeds paired process high-water mark'))
    for key in ('Call','RSS','ProcessHWM'):
        bad=clone(original);del bad['RSSPeriodicPeak'][key]
        fixtures.append(('missing paired '+key,periodic_case,bad,'missing/malformed periodic RSS/HWM witness'))
        for value in (True,1.0,'1',None,-1):
            bad=clone(original);bad['RSSPeriodicPeak'][key]=value
            fixtures.append(('typed paired '+key+' '+repr(value),periodic_case,bad,'missing/malformed periodic RSS/HWM witness'))
        bad=clone(zero);bad['RSSPeriodicPeak'][key]=1
        fixtures.append(('nonzero empty '+key,zero_case,bad,'nonzero empty periodic RSS/HWM witness'))
    bad=clone(original);bad['RSSPeriodicPeak']['extra']=0
    fixtures.append(('extra paired field',periodic_case,bad,'missing/malformed periodic RSS/HWM witness'))
    bad=clone(original);bad['schema']='gomap-native-memory-v2'
    fixtures.append(('old result schema',periodic_case,bad,'case identity mismatch'))
    with tempfile.TemporaryDirectory(prefix='native-memory-paired-hwm-contract-') as folder:
        archive=pathlib.Path(folder)
        for p in out.iterdir():
            if p.is_file():shutil.copyfile(p,archive/p.name)
        for name,case,x,reason in fixtures:
            # Reset BOTH result cases independently before every mutation.
            for c in receipt['cases']:shutil.copyfile(out/c['result'],archive/c['result'])
            r=clone(receipt);p=archive/case['result'];p.write_text(json.dumps(x)+'\n')
            next(c for c in r['cases'] if c['result']==p.name)['result_sha256']=sha(p)
            (archive/'receipt.json').write_text(json.dumps(r)+'\n')
            results=[json.loads((archive/c['result']).read_text()) for c in r['cases']]
            (archive/'summary.json').write_text(json.dumps(summarize(r,results))+'\n')
            if reason is None:validate_packet(archive)
            else:
                try:validate_packet(archive)
                except ValueError as error:require(str(error)==reason,'paired HWM refusal at wrong boundary: '+name)
                else:raise ValueError('checksum/summary-refreshed paired HWM fault accepted: '+name)
        r=clone(receipt);r['contract']='gomap-native-memory-v2'
        (archive/'receipt.json').write_text(json.dumps(r)+'\n')
        try:validate_packet(archive)
        except ValueError as error:require(str(error)=='invalid source receipt','old receipt refusal at wrong boundary')
        else:raise ValueError('old receipt contract accepted')
    print('paired HWM contract: 4 positive parser fixtures and '+str(len(fixtures)-4+1)+' exact coupled refusals PASS')


def environment_self_test(out, receipt):
    # Refresh every command binding together so only the environment contract
    # rejects these probes. Original packets and measurement bytes stay intact.
    import tempfile
    faults = [(key, 'missing', None) for key in BUILD_KEYS]
    faults += [(key, 'set', 'unrecorded') for key in ('GOEXPERIMENT', 'GOAMD64', 'CC', 'CGO_CFLAGS', 'LD_PRELOAD', 'GOROOT', 'GOGC', 'GOMEMLIMIT', 'GODEBUG')]
    faults += [('GOENV', 'set', '/tmp/unbound-goenv'), ('HOME', 'set', 'relative'), ('PATH', 'set', receipt['build_env']['PATH'] + os.pathsep + '/tmp/unrecorded-tool-path')]
    faults += [('GOMAXPROCS', 'set', v) for v in (None, 0, '', '0', '01', '2147483648', '٢')]
    with tempfile.TemporaryDirectory(prefix='native-environment-contract-') as folder:
        archive = pathlib.Path(folder)
        for file in out.iterdir():
            if file.is_file():
                shutil.copyfile(file, archive / file.name)
        for key, action, value in faults:
            altered = json.loads(json.dumps(receipt))
            env = altered['build_env']
            if action == 'missing':
                env.pop(key)
            else:
                env[key] = value
            for name, binding in [('version-command.json', 'version_command_sha256'), ('build-command.json', 'build_command_sha256')]:
                command = json.loads((out / name).read_text())
                command['env'] = dict(env)
                target = archive / name
                target.write_text(json.dumps(command) + '\n')
                altered[binding] = sha(target)
            for case in altered['cases']:
                command = json.loads((out / case['command']).read_text())
                command['env'] = dict(env, **{k: v for k, v in command['env'].items() if k.startswith('MVCC_')})
                target = archive / case['command']
                target.write_text(json.dumps(command) + '\n')
                case['command_sha256'] = sha(target)
            (archive / 'receipt.json').write_text(json.dumps(altered) + '\n')
            try:
                validate_packet(archive)
            except ValueError as error:
                if str(error) != 'controlled build environment':
                    raise
            else:
                raise ValueError('coupled uncontrolled environment accepted: ' + key)
    print(str(len(faults)) + ' coupled environment contract refusals PASS; original packets unchanged')

def artifact_sha(path):
    try:
        return sha(path)
    except OSError:
        return None

def run_logged(cmd, root, env, log, timeout):
    process_error = ''
    try:
        p = subprocess.run(cmd, cwd=root, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout)
    except (subprocess.TimeoutExpired, OSError) as e:
        process_error = str(e)
        p = subprocess.CompletedProcess(cmd, -1, getattr(e, 'stdout', None) or b'', getattr(e, 'stderr', None) or b'')
    log.write_bytes((p.stdout or b'') + (p.stderr or b'') + (('\nDRIVER ERROR: ' + process_error + '\n').encode() if process_error else b''))
    return p, process_error

def final_bindings(root, errors):
    try:
        return bindings(root)
    except OSError as e:
        errors.append('source bindings unreadable: ' + str(e))
        return None

def run(args):
    root=args.root.resolve();out=args.out.resolve();out.mkdir(parents=True,exist_ok=False)
    before=bindings(root);source_digest=digest(before)
    (out/'source-bindings.json').write_text(json.dumps(before,indent=2)+'\n')
    go_path=pathlib.Path(shutil.which(args.go) or args.go).resolve()
    env=controlled_environment(go_path)
    cases=[];errors=[];binary_sha=None;source_stable=True
    go,build_env,version_metadata,source_stable=capture_version(go_path,root,env,out,errors)
    binary=out/'native-memory.test'
    build=[str(go_path),'test','-c']+(['-race'] if args.race else [])+['-tags','treedb_test,mvcc_native_memory','-o',str(binary),'./TreeDB/mvcc']
    (out/'build-command.json').write_text(json.dumps({'cwd':str(root),'argv':build,'go':go,'env':build_env},indent=2)+'\n')
    if not errors:
        p,process_error=run_logged(build,root,env,out/'build.log',180)
        if p.returncode != 0: errors.append('harness build failed: '+(process_error or str(p.returncode)))
        else:
            binary_sha=artifact_sha(binary)
            after=final_bindings(root,errors)
            if before!=after or binary_sha is None or artifact_sha(go_path)!=go['sha256']:
                source_stable=False;errors.append('source/executable drift after build')
    matrix=[(args.n,'prune',True),(2*args.n,'prune',True),(args.n,'cancel',True),(args.n,'control',True)] if args.smoke else [(n,mode,pin) for n in (args.n,2*args.n) for mode in ('prune','control','cancel') for pin in (False,True)]
    for n,mode,pin in (matrix if not errors else []):
        try:stable=before==bindings(root) and sha(binary)==binary_sha and sha(go_path)==go['sha256']
        except OSError:stable=False
        if not stable:
            source_stable=False
            errors.append(f'source/executable drift before {mode}-{n}-pinned{int(pin)}');break
        name=f'{mode}-{n}-pinned{int(pin)}';result=out/(name+'.json');raw=out/(name+'.log')
        case_env=dict(env);case_env.update(MVCC_MEMORY_RESULT=str(result),MVCC_MEMORY_N=str(n),MVCC_MEMORY_MODE=mode,MVCC_MEMORY_PINNED=str(int(pin)))
        cmd=[str(binary),'-test.run','^TestNativePruneMemoryLifecycle$','-test.count=1','-test.timeout=120s','-test.v']
        command=out/(name+'-command.json');command.write_text(json.dumps({'cwd':str(root),'argv':cmd,'env':dict(case_env)},indent=2)+'\n')
        start=time.monotonic()
        p,process_error=run_logged(cmd,root,case_env,raw,150)
        if p.returncode != 0: errors.append('case failed: '+name+': '+(process_error or str(p.returncode)))
        after=final_bindings(root,errors)
        if after is not None and before!=after:(out/'drifted-source-bindings.json').write_text(json.dumps(after,indent=2)+'\n')
        try:stable=before==after and sha(binary)==binary_sha and sha(go_path)==go['sha256']
        except OSError:stable=False
        if not stable: source_stable=False
        cases.append({'command':command.name,'command_sha256':sha(command),'binary_before_sha256':binary_sha,'binary_after_sha256':artifact_sha(binary),'n':n,'mode':mode,'pinned':pin,'process_error':process_error,'raw':raw.name,'raw_sha256':hashlib.sha256(raw.read_bytes()).hexdigest(),'result':result.name,'result_sha256':artifact_sha(result),'exit_code':p.returncode,'elapsed_seconds':time.monotonic()-start,'source_digest_before':source_digest,'source_digest_after':digest(after) if after is not None else None})
        try:
            require(stable,'source/executable drift after '+name);require(p.returncode==0,'case failed: '+name);require(result.is_file(),'case missing result: '+name)
            validate_case(json.loads(result.read_text()),n,mode,pin)
        except (ValueError,KeyError,TypeError,OSError) as e:
            errors.append(str(e));break
        print(name+' PASS',flush=True)
    after=final_bindings(root,errors)
    if before!=after: source_stable=False;errors.append('source drift after collection')
    receipt={'contract':CONTRACT,'source_root':str(root),'capture_out':str(out),'go':go,'build_env':build_env,'binary_sha256':binary_sha,'build_command_sha256':artifact_sha(out/'build-command.json'),'build_log_sha256':artifact_sha(out/'build.log'),'source_digest':source_digest,'source_count':len(before),'source_stable':source_stable,'errors':errors,'n':args.n,'smoke':args.smoke,'race':args.race,'cases':cases,'labels':dict(MEASUREMENT_LABELS),**version_metadata}
    if not errors:
        try: validate_version(out, receipt)
        except (OSError,ValueError,KeyError,TypeError) as error: errors.append('version capture invalid: '+str(error))
    (out/'receipt.json').write_text(json.dumps(receipt,indent=2)+'\n')
    require(not errors,'capture failures '+repr(errors))
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
