#!/usr/bin/env python3
"""Fixed Go1.26.10 parsed-trace/owner-receipt bijection; diagnostic only."""
import json
import re

BOUND=4096
PREFIX='m0/production/bytes_1x/iteration/0000/operation/'
REGION=re.compile(r'M=(-?\d+) P=(-?\d+) G=(-?\d+) (RegionBegin|RegionEnd) Time=(\d+) Task=(\d+) Type=("(?:[^"\\]|\\.)*")\Z')


def require(ok, message):
    if not ok:
        raise ValueError(message)


def identity(ordinal):
    kind='set' if ordinal%2==0 else 'delete-range'
    return PREFIX+f'{ordinal:04d}/'+kind


def checked_write(stream, data):
    require(stream.write(data)==len(data),'retained stream short write')


def validate_owner(owner):
    require(type(owner) is dict,'owner receipt object')
    for key,value in {'Schema':'m0-ordinal-observer-v1',
                      'Status':'ORDINALS_COMPLETE_TRACE_ARTIFACT_VALIDATION_PENDING',
                      'Reason':'','DiagnosticOnly':True,'Iterations':1,'Bound':BOUND,'Written':BOUND,
                      'Lost':False,'Joined':True,'TimerStopped':True,'StatsObserved':True,
                      'ExposureChecked':True,'CheckedClose':True,'TraceEnabledAtStart':True,
                      'TraceEnabledAtEnd':True,'PhaseHookCount':1,'Points':2048,'Ranges':2048,
                      'ExposureMisses':0,'ForegroundError':False,'VacuumError':False,
                      'OriginalDurationsMatch':True}.items():
        require(type(owner.get(key)) is type(value) and owner[key]==value,'owner refusal: '+key)
    require(type(owner.get('Stats')) is dict and
            type(owner['Stats'].get('ConcurrentMutationAborts')) is int and
            owner['Stats']['ConcurrentMutationAborts']==0,'original concurrent mutation abort')
    records=owner.get('Records')
    require(type(records) is list and len(records)==BOUND,'exact owner record population')
    for ordinal,row in enumerate(records):
        require(type(row) is dict,'record object')
        for key,value in {'Iteration':0,'Ordinal':ordinal,'Kind':'set' if ordinal%2==0 else 'delete-range',
                          'Error':False,'Status':'success','Region':identity(ordinal),'Recorded':True}.items():
            require(type(row.get(key)) is type(value) and row[key]==value,'record identity/lifecycle: '+str(ordinal))
        require(type(row.get('DurationNS')) is int and row['DurationNS']>=0,'original duration invalid')
    # This flag is the original in-process equality check. Its other latency
    # array is not serialized; do not invent an independent comparison here.
    return records


def consume(owner, lines, decoder):
    records=validate_owner(owner)
    for field in ('normal_return','stdout_eof','stderr_eof','retained_writes_closed'):
        require(decoder.get(field) is True,'decoder I/O incomplete: '+field)
    require(type(decoder.get('exit')) is int and decoder['exit']==0,'decoder failed')
    current=None;next_ordinal=0;worker=None;task=None;last_end=None;selected=0;aligned=[]
    for raw in lines:
        require(type(raw) is bytes and len(raw)<=1024**2 and raw.endswith(b'\n'),
                'truncated or excessive parsed line')
        line=raw[:-1].decode('utf-8',errors='strict')
        # Standard Event.String is pinned debug text, not a stable public API.
        region_event=(' RegionBegin ' in line or ' RegionEnd ' in line)
        match=REGION.fullmatch(line) if region_event else None
        if match is None:
            require('m0/' not in line,'unknown selected trace grammar/event')
            continue
        name=json.loads(match[7])
        require(type(name) is str,'region name type')
        if not name.startswith('m0/'):
            continue
        selected+=1
        goroutine=int(match[3]);event=match[4];timestamp=int(match[5]);observed_task=int(match[6])
        require(next_ordinal<BOUND,'extra selected region')
        require(name==identity(next_ordinal),'missing/duplicate/out-of-order selected identity')
        require(goroutine>0,'selected goroutine unavailable')
        if worker is None:worker,task=goroutine,observed_task
        require((goroutine,observed_task)==(worker,task),'selected worker/task mismatch')
        if event=='RegionBegin':
            require(current is None,'overlapping/duplicate begin')
            require(last_end is None or timestamp>=last_end,'selected time order regression')
            current=(name,timestamp)
        else:
            require(current is not None and current[0]==name and timestamp>=current[1],
                    'missing begin/reversed region time')
            aligned.append({'iteration':0,'ordinal':next_ordinal,'kind':records[next_ordinal]['Kind'],
                            'goroutine':goroutine,'task':observed_task,'begin_trace_ns':current[1],
                            'end_trace_ns':timestamp,'owner_duration_ns':records[next_ordinal]['DurationNS']})
            current=None;last_end=timestamp;next_ordinal+=1
    require(current is None and next_ordinal==BOUND and selected==BOUND*2,
            'incomplete selected trace bijection')
    return {'status':'DIAGNOSTIC_SAME_ITERATION_TRACE_BIJECTION_ONLY','records':len(records),
            'regions':next_ordinal,'selected_events':selected,'goroutine':worker,'task':task,'alignment':aligned,
            'OriginalDurationsMatch':'authenticated producer in-process comparison, not independent array validation',
            'region_duration_is_original_duration':False,'M0_CV_acceptance':False,
            'historical_alignment_or_cause':False}

# Fixed data-only projection of the SAME already decoded Go1.26.10 text.
# No trace decoder, clock sampling, product execution or cross-goroutine join.
HEADER=re.compile(r'M=(-?\d+) P=(-?\d+) G=(-?\d+) (\w+) Time=(\d+)(.*)\Z')
TRANSITION=re.compile(r' GoID=(\d+) (Undetermined|NotExist|Runnable|Running|Waiting|Syscall)->(Undetermined|NotExist|Runnable|Running|Waiting|Syscall) Reason="([\x20-\x21\x23-\x5b\x5d-\x7e]*)"\Z')
PROC_TRANSITION=re.compile(r' ProcID=(-?\d+) (\w+)->(\w+) Reason="([\x20-\x21\x23-\x5b\x5d-\x7e]*)"\Z')
FRAME=re.compile(r'\t(.+) @ 0x([0-9a-f]+)\Z')
LOCATION=re.compile(r'\t\t(.+):(\d+)\Z')
STATES=('Running','Runnable','Waiting','Syscall')


def event_blocks(lines):
    """Preserve original debug event order and bounded selected block bytes."""
    import hashlib
    header=None;tail=[];offset=0;start=0;length=0;skip=False;block_hash=None
    for raw in lines:
        require(type(raw) is bytes and raw.endswith(b'\n') and len(raw)<=1024**2,
                'truncated or excessive parsed line')
        text=raw[:-1].decode('utf-8',errors='strict')
        if text.startswith('M='):
            if header is not None:
                yield header,tail,start,block_hash.hexdigest() if block_hash is not None else None
            header=HEADER.fullmatch(text)
            require(header is not None,'unknown parsed event header')
            tail=[];start=offset;length=len(raw)
            # Only StateTransition blocks need their stack payload retained.
            skip=header[4]!='StateTransition'
            block_hash=None if skip else hashlib.sha256(raw)
        else:
            require(header is not None,'continuation before first parsed event')
            if not skip:
                length+=len(raw)
                require(length<=65536,'state event block exceeds64KiB')
                tail.append(text)
                block_hash.update(raw)
        offset+=len(raw)
    if header is not None:
        yield header,tail,start,block_hash.hexdigest() if block_hash is not None else None


def transition_stack(tail):
    sections={};active=None;pending=None;frames=0
    for text in tail:
        if not text:
            require(pending is None,'unpaired stack function')
            continue
        if text in ('TransitionStack=','Stack='):
            require(pending is None and text not in sections,'duplicate or incomplete stack section')
            active=text;sections[active]=[]
            continue
        require(active is not None,'unknown state continuation')
        if pending is None:
            match=FRAME.fullmatch(text)
            require(match is not None,'unknown stack function grammar')
            pending=(match[1],match[2])
        else:
            match=LOCATION.fullmatch(text)
            require(match is not None,'unknown stack location grammar')
            sections[active].append({'function':pending[0],'pc_hex':pending[1],
                                     'file':match[1],'line':int(match[2])})
            pending=None;frames+=1
            require(frames<=256,'state stack frame bound')
    require(pending is None,'truncated state stack')
    # Event.Stack may belong to the waker/creator. Never substitute it.
    return sections.get('TransitionStack=')


def project_states(owner, alignment, lines, emit):
    """emit(name, JSON row) is checked/capped by the fixed hosted reader.

    Caller must first reproduce consume() and require exact alignment equality.
    All4096 rows are retained. Trace state wall is NOT original latency or CPU.
    """
    import bisect
    import hashlib
    records=validate_owner(owner)
    require(type(alignment) is dict and len(alignment.get('alignment',[]))==BOUND,
            'fixed complete alignment required')
    aligned=alignment['alignment'];worker=alignment['goroutine']
    require(type(worker) is int and worker>0,'target worker unavailable')
    begin=[r['begin_trace_ns'] for r in aligned];end=[r['end_trace_ns'] for r in aligned]
    rows=[]
    for i,r in enumerate(aligned):
        require(r['ordinal']==i and r['goroutine']==worker and r['task']==alignment['task']
                and r['owner_duration_ns']==records[i]['DurationNS'] and r['kind']==records[i]['Kind'],
                'alignment record mismatch')
        require(type(begin[i]) is int and type(end[i]) is int and begin[i]<=end[i]
                and (i==0 or begin[i]>=end[i-1]),'alignment interval order')
        rows.append({'ordinal':i,'kind':r['kind'],'owner_duration_ns':r['owner_duration_ns'],
                     'begin_trace_ns':begin[i],'end_trace_ns':end[i],
                     'state_ns':dict.fromkeys(STATES,0),'segments':0,'stack_unavailable_ns':0})
    state=None;last=None;entry=None;stack=None;seen_stacks=set();last_event=None
    region_index=0;opened=False;segments=0;gap_ns=0;between_ns=0;status_events=0
    original_durations=sorted(r['DurationNS'] for r in records)
    slow_threshold=original_durations[(BOUND*99+99)//100-1]
    slow_contributions={};stack_frames={}

    def span(stop):
        nonlocal last,segments,gap_ns,between_ns
        if last is None:
            return
        require(stop>=last,'target time regression')
        left=max(last,begin[0]);right=min(stop,end[-1])
        if left>=right:
            last=stop
            return
        require(state in STATES,'unknown/nonexistent state intersects operation envelope')
        between_ns+=right-left;overlap=0
        index=bisect.bisect_right(end,left)
        while index<BOUND and begin[index]<right:
            a=max(left,begin[index]);b=min(right,end[index])
            if b>a:
                row=rows[index];row['state_ns'][state]+=b-a;row['segments']+=1
                unavailable=state in ('Waiting','Syscall') and stack is None
                if unavailable:row['stack_unavailable_ns']+=b-a
                emit('segments',{'ordinal':index,'start_trace_ns':a,'end_trace_ns':b,'state':state,
                     'entry':entry,'transition_stack_sha256':stack,
                     'stack_status':'UNAVAILABLE' if unavailable else 'AVAILABLE' if stack else 'NOT_RECORDED',
                     'clipped_entry':entry['time_ns']<a})
                if row['owner_duration_ns']>=slow_threshold:
                    key=(state,entry['reason'],stack)
                    bucket=slow_contributions.setdefault(key,{'ns':0,'segments':0,'ordinals':set()})
                    bucket['ns']+=b-a;bucket['segments']+=1;bucket['ordinals'].add(index)
                overlap+=b-a;segments+=1
            index+=1
        gap_ns+=right-left-overlap;last=stop

    for h,tail,offset,raw_block_sha256 in event_blocks(lines):
        kind=h[4];timestamp=int(h[5])
        if kind=='StateTransition':
            transition=TRANSITION.fullmatch(h[6])
            require(transition is not None or PROC_TRANSITION.fullmatch(h[6]) is not None,
                    'unknown state transition grammar/reason')
            if transition is None or int(transition[1])!=worker:
                continue
            require(last_event is None or timestamp>=last_event,'target event time regression')
            last_event=timestamp
            old,new,reason=transition[2],transition[3],transition[4]
            target_frames=transition_stack(tail)
            if state is None:
                require(old in ('Undetermined','NotExist'),'missing initial target state')
                require(timestamp<=begin[0],'initial state after first region')
                last=timestamp
            else:
                require(old==state,'target state chain mismatch')
                span(timestamp)
            if old==new:
                require(state is not None,'unknown same-state initialization')
                status_events+=1
                continue
            state=new;last=timestamp
            stack=None
            if target_frames:
                raw=json.dumps(target_frames,sort_keys=True,separators=(',',':'),ensure_ascii=True).encode('ascii')
                stack=hashlib.sha256(raw).hexdigest()
                if stack not in seen_stacks:
                    emit('stacks',{'sha256':stack,'frames':target_frames});seen_stacks.add(stack)
                    stack_frames[stack]=target_frames
            entry={'time_ns':timestamp,'parsed_byte_offset':offset,'executing_goroutine':int(h[3]),
                   'target_goroutine':worker,'old':old,'new':new,'reason':reason,
                   'raw_header_sha256':hashlib.sha256(h[0].encode('utf-8')).hexdigest(),
                   'raw_block_sha256':raw_block_sha256}
        elif kind in ('RegionBegin','RegionEnd'):
            match=REGION.fullmatch(h[0])
            if match is None:
                require('m0/' not in h[0],'unknown selected region grammar')
                continue
            name=json.loads(match[7])
            if not name.startswith('m0/'):
                continue
            require(region_index<BOUND and name==identity(region_index),'selected region identity')
            require(int(match[3])==worker and int(match[6])==alignment['task'],'selected region owner')
            require(last_event is None or timestamp>=last_event,'region/target event order regression')
            last_event=timestamp
            span(timestamp)
            require(state=='Running','region endpoint not in known Running state')
            if kind=='RegionBegin':
                require(not opened and timestamp==begin[region_index],'region begin mismatch')
                opened=True
            else:
                require(opened and timestamp==end[region_index],'region end mismatch')
                opened=False;region_index+=1
    require(not opened and region_index==BOUND,'incomplete region/state stream')
    require(between_ns==end[-1]-begin[0],'operation envelope state coverage gap')
    descending=sorted(range(BOUND),key=lambda i:(-rows[i]['owner_duration_ns'],i))
    ranks={ordinal:rank+1 for rank,ordinal in enumerate(descending)}
    ascending=sorted(range(BOUND),key=lambda i:(rows[i]['owner_duration_ns'],i))
    ascending_indices={ordinal:index for index,ordinal in enumerate(ascending)}
    durations=sorted(r['owner_duration_ns'] for r in rows)
    p99=durations[(BOUND*99+99)//100-1];p95=durations[(BOUND*95+99)//100-1]
    totals=dict.fromkeys(STATES,0);kind_totals={k:dict.fromkeys(STATES,0)for k in ('set','delete-range')}
    high=dict.fromkeys(STATES,0);missing=0;region_sum=0
    for row in rows:
        width=row['end_trace_ns']-row['begin_trace_ns']
        require(sum(row['state_ns'].values())==width,'per-operation coverage gap/overlap')
        row.update(rank_descending=ranks[row['ordinal']],index_ascending=ascending_indices[row['ordinal']],
                   at_or_above_p99=row['owner_duration_ns']>=p99,
                   trace_wall_ns=width,trace_minus_owner_ns=width-row['owner_duration_ns'],coverage='COMPLETE')
        for s in STATES:
            value=row['state_ns'][s];totals[s]+=value;kind_totals[row['kind']][s]+=value
            if row['at_or_above_p99']:high[s]+=value
        missing+=row['stack_unavailable_ns'];region_sum+=width
        emit('operations',row)
    require(region_sum+gap_ns==between_ns,'envelope/operation/gap reconciliation')
    return {'status':'COMPLETE_STATE_WITH_STACK_GAPS' if missing else 'COMPLETE_STATE_WALL_ONLY',
            'records':BOUND,'goroutine':worker,'task':alignment['task'],'segments':segments,
            'same_state_status_events':status_events,'target_stack_count':len(seen_stacks),
            'state_ns':totals,'by_kind_state_ns':kind_totals,'at_or_above_p99_state_ns':high,
            'p99_ns':p99,'p95_ns':p95,'owner_duration_sum_ns':sum(durations),
            'at_or_above_p99_operations':sum(r['owner_duration_ns']>=p99 for r in rows),
            'at_or_above_p99_contributions':[{'state':k[0],'reason':k[1],
                'transition_stack_sha256':k[2],'transition_stack':stack_frames.get(k[2]),
                'ns':v['ns'],'segments':v['segments'],'operations':len(v['ordinals'])}
                for k,v in sorted(slow_contributions.items(),key=lambda item:(item[0][0],item[0][1],item[0][2] or ''))],
            'trace_region_sum_ns':region_sum,'between_regions_ns':gap_ns,
            'envelope_ns':between_ns,'stack_unavailable_ns':missing,
            'running_is_cpu':False,'select_is_queue_service_split':False,'phase_assignment':False,
            'M0_CV_acceptance':False,'historical_cause':False}
