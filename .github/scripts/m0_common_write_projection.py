"""One original M0 worker linkage/projection. Pure parsed data, no clock fitting."""
import bisect
import hashlib
import json
import m0_trace_consumer as c

LOOP='github.com/snissn/gomap/TreeDB/db.(*DB).commitCombinerLoop'
SEND='github.com/snissn/gomap/TreeDB/db.(*DB).finishCombined'
FILE='TreeDB/db/commit_combiner.go'


def sections(tail):
    # The unchanged parser checks the complete block, including both sections.
    target=c.transition_stack(tail)
    event=None
    if 'Stack=' in tail:
        event=c.transition_stack(['TransitionStack=']+tail[tail.index('Stack=')+1:])
    return target,event


def matches(frames,function,line=None):
    return bool(frames) and any(f['function']==function and
        f['file'].replace('\\','/').endswith('/'+FILE) and
        (line is None or f['line']==line) for f in frames)


def validate_service(owner,service):
    records=c.validate_owner(owner)
    for key,value in {'Schema':'m0-combiner-service-observer-v1',
        'Status':'SERVICE_ROWS_COMPLETE_TRACE_ARTIFACT_VALIDATION_PENDING','Reason':'',
        'DiagnosticOnly':True,'OriginalWriterJoined':True,'OriginalDoneObserved':True,
        'OriginalCloseReturned':True,'OriginalCloseError':'NIL'}.items():
        c.require(type(service.get(key)) is type(value) and service[key]==value,'service lifecycle: '+key)
    rows=service.get('Rows');c.require(type(rows) is list and len(rows)==4096,'service exact population')
    for i,(row,record) in enumerate(zip(rows,records)):
        for k,v in {'Iteration':0,'Ordinal':i,'Kind':record['Kind'],'OriginalRegion':record['Region'],
                    'OriginalDurationNS':record['DurationNS'],
                    'Status':'APPLIED_RESULT_RECEIVED' if i%2==0 else 'NON_COMBINER'}.items():
            c.require(type(row.get(k)) is type(v) and row[k]==v,'service identity/status')
        if i%2==0:
            for k,v in {'BatchSerial':i//2+1,'BatchIndex':0,'BatchSize':1,'Selected':1,
                'Dispatch':'APPLIED','WriteKind':'WRITE','WriteError':'NIL','CloseError':'NIL',
                'IgnoredCloseError':False,'Terminal':True,'Invalid':False}.items():
                c.require(type(row['Worker'].get(k)) is type(v) and row['Worker'][k]==v,'original singleton worker')
            c.require(row['Caller']['Branch']=='RESULT' and row['Caller']['Error']=='NIL'
                      and row['Caller']['Entered'] is True and row['Caller']['Invalid'] is False,'original result caller')
    return records


def evidence(h,offset,block,target,event):
    return {'time_ns':int(h[5]),'parsed_byte_offset':offset,'executing_goroutine':int(h[3]),
        'raw_header_sha256':hashlib.sha256(h[0].encode()).hexdigest(),'raw_block_sha256':block,
        'target_transition_stack':target,'event_stack':event}


def link_worker(alignment,lines,emit):
    aligned=alignment['alignment'];caller=alignment['goroutine']
    starts=[r['begin_trace_ns'] for r in aligned]
    initial={};loop={};links=[];workers=set();outside=[];seen_stacks=set()
    for h,tail,offset,block in c.event_blocks(lines):
        if h[4]!='StateTransition':continue
        t=c.TRANSITION.fullmatch(h[6])
        c.require(t is not None or c.PROC_TRANSITION.fullmatch(h[6]) is not None,'unknown transition grammar')
        if t is None:continue
        target=int(t[1]);old,new=t[2],t[3];ts=int(h[5])
        target_stack,event_stack=sections(tail)
        row=None
        if old in ('NotExist','Undetermined'):
            c.require(len(initial)<4096 or target in initial,'initial GoID evidence bound')
            c.require(target not in initial,'multiple initial target events')
            initial[target]=evidence(h,offset,block,target_stack,event_stack)
        if matches(target_stack,LOOP):
            if target not in loop:loop[target]=evidence(h,offset,block,target_stack,event_stack)
        if target==caller and old=='Waiting' and new=='Runnable' and matches(event_stack,SEND,227):
            index=bisect.bisect_right(starts,ts)-1
            valid=0<=index<len(aligned) and aligned[index]['kind']=='set' and ts<aligned[index]['end_trace_ns']
            row=evidence(h,offset,block,target_stack,event_stack)
            row.update(target_goroutine=caller,worker_goroutine=int(h[3]),ordinal=index if valid else None,
                       correct_set_envelope=valid,link_basis='ORIGINAL_FINISH_COMBINED_LINE227_EVENT_STACK_AND_HEADER_WAKER')
            c.require(len(links)<4096,'result-wake evidence bound')
            for field in ('target_transition_stack','event_stack'):
                frames=row.pop(field)
                sha=None
                if frames:
                    raw=json.dumps(frames,sort_keys=True,separators=(',',':'),ensure_ascii=True).encode('ascii')
                    sha=hashlib.sha256(raw).hexdigest()
                    if sha not in seen_stacks:
                        emit('stacks',{'sha256':sha,'frames':frames,'kind':'link-'+field});seen_stacks.add(sha)
                row[field+'_sha256']=sha
            links.append(row);emit('links',row)
            if not valid:outside.append(row['parsed_byte_offset'])
            else:workers.add(int(h[3]))
    reason=None;worker=None
    if outside:reason='RESULT_WAKE_OUTSIDE_SET_ENVELOPE'
    elif not workers:reason='ORIGINAL_RESULT_WAKE_LINK_MISSING'
    elif len(workers)!=1:reason='ORIGINAL_RESULT_WAKE_WORKER_AMBIGUOUS'
    else:
        worker=next(iter(workers))
        if worker<=0 or worker==caller:reason='INVALID_ORIGINAL_WORKER_LINK'
        elif worker not in initial:reason='ORIGINAL_WORKER_INITIAL_EVENT_MISSING'
        elif initial[worker]['time_ns']>aligned[0]['begin_trace_ns']:reason='ORIGINAL_WORKER_INITIAL_AFTER_FIRST_SET'
        elif worker not in loop:reason='ORIGINAL_WORKER_TARGET_LOOP_STACK_MISSING'
    result={'status':'POSITIVELY_LINKED_ORIGINAL_WORKER' if reason is None else 'INCONCLUSIVE_STOP',
        'reason':reason,'worker_goroutine':worker if reason is None else None,'caller_goroutine':caller,
        'result_wake_links':len(links),'distinct_linked_workers':sorted(workers),
        'initial_event':initial.get(worker),'target_loop_event':loop.get(worker),
        'link_does_not_identify_all2048_service_boundaries':True,'service_clock_used':False}
    emit('links',{'summary':result});return result


class CoverageGap(ValueError):pass


def known(ok,reason):
    if not ok:raise CoverageGap(reason)


def project_worker(alignment,worker,lines,emit):
    """Separate worker state in Set CALLER envelopes, never exclusive Write."""
    aligned=[r for r in alignment['alignment'] if r['kind']=='set']
    starts=[r['begin_trace_ns'] for r in aligned];ends=[r['end_trace_ns'] for r in aligned]
    rows=[dict(ordinal=r['ordinal'],kind='set',begin_trace_ns=starts[i],end_trace_ns=ends[i],
               owner_duration_ns=r['owner_duration_ns'],state_ns=dict.fromkeys(c.STATES,0),segments=0,
               stack_unavailable_ns=0)for i,r in enumerate(aligned)]
    state=None;last=None;entry=None;stack=None;seen=set();endpoints=0;segments=0
    def span(stop):
        nonlocal last,segments
        if last is None:return
        known(stop>=last,'WORKER_TARGET_TIME_REGRESSION')
        left=max(last,starts[0]);right=min(stop,ends[-1]);i=bisect.bisect_right(ends,left)
        while i<len(rows) and starts[i]<right:
            a=max(left,starts[i]);b=min(right,ends[i])
            if a<b:
                known(state in c.STATES,'WORKER_STATE_UNAVAILABLE_IN_SET_ENVELOPE')
                row=rows[i];row['state_ns'][state]+=b-a;row['segments']+=1
                missing=state in ('Waiting','Syscall') and stack is None
                if missing:row['stack_unavailable_ns']+=b-a
                emit('segments',{'population':'worker-in-set-caller-envelope','ordinal':row['ordinal'],
                    'start_trace_ns':a,'end_trace_ns':b,'state':state,'entry':entry,
                    'transition_stack_sha256':stack,'stack_status':'UNAVAILABLE' if missing else 'AVAILABLE' if stack else 'NOT_RECORDED'})
                segments+=1
            i+=1
        last=stop
    try:
        for h,tail,offset,block in c.event_blocks(lines):
            ts=int(h[5])
            if h[4]=='StateTransition':
                t=c.TRANSITION.fullmatch(h[6])
                c.require(t is not None or c.PROC_TRANSITION.fullmatch(h[6]) is not None,'unknown transition grammar')
                if t is None or int(t[1])!=worker:continue
                old,new=t[2],t[3];frames,_=sections(tail)
                if state is None:
                    known(old in ('NotExist','Undetermined') and ts<=starts[0],'WORKER_INITIAL_STATE_COVERAGE_MISSING')
                    last=ts
                else:
                    known(old==state,'WORKER_STATE_CHAIN_GAP');span(ts)
                if old==new:continue
                state=new;last=ts;stack=None
                if frames:
                    raw=json.dumps(frames,sort_keys=True,separators=(',',':'),ensure_ascii=True).encode('ascii')
                    stack=hashlib.sha256(raw).hexdigest()
                    if stack not in seen:emit('stacks',{'sha256':stack,'frames':frames});seen.add(stack)
                entry=evidence(h,offset,block,None,None)
                entry.update(target_goroutine=worker,old=old,new=new,reason=t[4])
            elif h[4] in ('RegionBegin','RegionEnd'):
                m=c.REGION.fullmatch(h[0])
                if m is None:continue
                name=json.loads(m[7])
                if not name.startswith(c.PREFIX):continue
                # consume() reproduced all4096 identities BEFORE this pass.
                index=bisect.bisect_left(starts,ts) if h[4]=='RegionBegin' else bisect.bisect_left(ends,ts)
                values=starts if h[4]=='RegionBegin' else ends
                if index<len(values) and values[index]==ts and name==c.identity(aligned[index]['ordinal']):
                    known(state is not None,'WORKER_STATE_UNKNOWN_AT_CALLER_ENDPOINT');span(ts)
                    known(state in c.STATES,'WORKER_NONEXISTENT_AT_CALLER_ENDPOINT');endpoints+=1
        known(endpoints==4096,'WORKER_CALLER_ENDPOINT_COVERAGE_MISSING')
        for row in rows:
            known(sum(row['state_ns'].values())==row['end_trace_ns']-row['begin_trace_ns'],
                  'WORKER_SET_ENVELOPE_PARTITION_GAP')
    except CoverageGap as error:
        return unknown_worker(alignment,str(error),emit,partial_segments=segments)
    total=dict.fromkeys(c.STATES,0)
    top=set(sorted((r['ordinal']for r in alignment['alignment']),
                   key=lambda i:(-alignment['alignment'][i]['owner_duration_ns'],i))[:41])
    for row in rows:
        row.update(population='worker-in-set-caller-envelope',coverage='COMPLETE',
                   at_original_top41=row['ordinal'] in top,exclusive_write=False)
        emit('operations',row)
        for s in c.STATES:total[s]+=row['state_ns'][s]
    return {'status':'COMPLETE_WORKER_STATE_IN_SET_CALLER_ENVELOPES_ONLY','records':2048,
        'worker_goroutine':worker,'state_ns':total,'segments':segments,'overlaps_caller_wall':True,
        'exclusive_write':False,'running_is_cpu':False,'clock_alignment_to_service':False}


def unknown_worker(alignment,reason,emit,partial_segments=0):
    for row in alignment['alignment']:
        if row['kind']=='set':
            emit('operations',{'population':'worker-in-set-caller-envelope','ordinal':row['ordinal'],
                'begin_trace_ns':row['begin_trace_ns'],'end_trace_ns':row['end_trace_ns'],
                'coverage':'UNKNOWN','reason':reason,'state_ns':None,'exclusive_write':False})
    return {'status':'INCONCLUSIVE_STOP','reason':reason,'records':2048,'state_ns':None,
        'partial_segments_not_complete_coverage':partial_segments,'no_successor_authorized':True}
