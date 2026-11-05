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


def consume_service(owner, service):
    """Validate the closed private bridge receipt; inclusive wall brackets only."""
    original=validate_owner(owner)
    require(type(service) is dict,'service receipt object')
    for key,value in {'Schema':'m0-combiner-service-observer-v1',
        'Status':'SERVICE_ROWS_COMPLETE_TRACE_ARTIFACT_VALIDATION_PENDING','Reason':'',
        'DiagnosticOnly':True,'OriginalWriterJoined':True,'OriginalDoneObserved':True,
        'OriginalCloseReturned':True,'OriginalCloseError':'NIL'}.items():
        require(type(service.get(key)) is type(value) and service[key]==value,'service lifecycle: '+key)
    rows=service.get('Rows')
    require(type(rows) is list and len(rows)==BOUND,'service population')
    caller_stamps=('Entry','EnqueueBefore','EnqueueAfter','SelectBefore','SelectAfter','Return')
    worker_stamps=('Dequeue','ApplyBefore','ApplyAfter','WriteBefore','WriteAfter',
                   'CloseBefore','CloseAfter','SendBefore','SendAfter')
    caller_keys=set(caller_stamps)|{'EnqueueStatus','Branch','Error','Entered','Invalid'}
    worker_keys=set(worker_stamps)|{'BatchSerial','BatchIndex','BatchSize','Selected','Dispatch',
        'WriteKind','WriteError','CloseError','IgnoredCloseError','Terminal','Invalid'}
    def integer(value):return type(value) is int and 0<=value<=2**63-1
    def stamps(obj, names):
        for name in names:
            stamp=obj[name]
            require(type(stamp) is dict and set(stamp)=={'NS','Valid'} and
                    integer(stamp['NS']) and type(stamp['Valid']) is bool and
                    (stamp['Valid'] or stamp['NS']==0),'service timestamp shape: '+name)
    def order(obj, names):
        selected=[obj[n] for n in names]
        require(all(s['Valid'] for s in selected) and
                all(a['NS']<=b['NS'] for a,b in zip(selected,selected[1:])),
                'same-owner timestamp order')
    def blank(obj):
        return all(v=={'NS':0,'Valid':False} if type(v) is dict else
                   v is False if type(v) is bool else v==0 if type(v) is int else v==''
                   for v in obj.values())
    groups={};output=[];populations={}
    for n,row in enumerate(rows):
        require(type(row) is dict,'service row object')
        for key,value in {'Iteration':0,'Ordinal':n,'Kind':original[n]['Kind'],
            'OriginalRegion':original[n]['Region'],'OriginalDurationNS':original[n]['DurationNS']}.items():
            require(type(row.get(key)) is type(value) and row[key]==value,'service original ordinal binding')
        c,w=row.get('Caller'),row.get('Worker')
        require(type(c) is dict and set(c)==caller_keys and type(w) is dict and
                set(w)==worker_keys,'fixed caller/worker schema')
        stamps(c,caller_stamps);stamps(w,worker_stamps)
        for obj,names in ((c,('Entered','Invalid')),(w,('IgnoredCloseError','Terminal','Invalid'))):
            require(all(type(obj[name]) is bool for name in names),'service boolean type')
        require(all(integer(w[k]) for k in ('BatchSerial','BatchIndex','BatchSize','Selected')),
                'service batch scalar type')
        require(all(type(c[k]) is str for k in ('EnqueueStatus','Branch','Error')) and
                all(type(w[k]) is str for k in ('Dispatch','WriteKind','WriteError','CloseError')),
                'service category type')
        status=row.get('Status');require(type(status) is str,'service status type')
        result={'ordinal':n,'kind':row['Kind'],'owner_duration_ns':row['OriginalDurationNS'],'status':status}
        if n%2:
            require(status=='NON_COMBINER' and blank(c) and blank(w),'odd noncombiner service fields')
        else:
            require(c['Entered'] and not c['Invalid'] and not w['Invalid'] and c['Error']=='NIL',
                    'successful original caller invalid/error')
            order(c,('Entry','Return'))
            result['caller_entry_return_ns']=c['Return']['NS']-c['Entry']['NS']
            if c['EnqueueStatus']=='ENQUEUED':
                require(status=='APPLIED_RESULT_RECEIVED' and c['Branch']=='RESULT' and
                        w['Dispatch']=='APPLIED' and w['Terminal'],'actual applied result required')
                order(c,caller_stamps)
                order(w,('Dequeue','ApplyBefore','WriteBefore','WriteAfter','CloseBefore',
                         'CloseAfter','ApplyAfter','SendBefore','SendAfter'))
                require(w['WriteKind'] in ('WRITE','WRITE_SYNC') and not w['IgnoredCloseError'] and
                        w['WriteError']==w['CloseError']=='NIL','successful original write/Close')
                require(w['BatchSerial']>0 and 1<=w['BatchSize']<=64 and
                        0<=w['BatchIndex']<w['BatchSize'] and 1<=w['Selected']<=w['BatchSize'],
                        'batch bounds')
                shared={k:w[k] for k in ('BatchSize','Selected','ApplyBefore','ApplyAfter','WriteBefore',
                    'WriteAfter','CloseBefore','CloseAfter','WriteKind','WriteError','CloseError','IgnoredCloseError')}
                group=groups.setdefault(w['BatchSerial'],{'shared':shared,'positions':[]})
                require(group['shared']==shared and w['BatchIndex'] not in group['positions'],
                        'conflicting shared batch/duplicate position')
                group['positions'].append(w['BatchIndex'])
                result.update(batch_serial=w['BatchSerial'],batch_index=w['BatchIndex'],
                    caller_select_ns=c['SelectAfter']['NS']-c['SelectBefore']['NS'],
                    apply_wall_ns=w['ApplyAfter']['NS']-w['ApplyBefore']['NS'],
                    write_wall_ns=w['WriteAfter']['NS']-w['WriteBefore']['NS'],
                    close_wall_ns=w['CloseAfter']['NS']-w['CloseBefore']['NS'])
                # No cross-owner inequality: enqueue-after can exceed dequeue,
                # and receive can precede send-after. Zero valid times are legal.
            else:
                require(status=='DIRECT_FALLBACK' and c['Branch']=='DIRECT_FALLBACK' and blank(w) and
                        not c['SelectBefore']['Valid'] and not c['SelectAfter']['Valid'] and
                        c['EnqueueStatus'] in ('NO_CHANNEL','SATURATED_FALLBACK','SEND_FALLBACK'),
                        'successful direct fallback branch')
                if c['EnqueueStatus']=='SEND_FALLBACK':
                    order(c,('Entry','EnqueueBefore','EnqueueAfter','Return'))
                else:
                    require(not c['EnqueueBefore']['Valid'] and not c['EnqueueAfter']['Valid'],
                            'non-send fallback enqueue fields')
        populations[status]=populations.get(status,0)+1
        output.append(result)
    batches=[]
    for serial,group in sorted(groups.items()):
        shared=group['shared']
        require(len(group['positions'])==shared['Selected'],'selected batch members missing')
        batches.append({'batch_serial':serial,'positions':sorted(group['positions']),**shared})
    require(populations.get('NON_COMBINER')==2048 and
            populations.get('APPLIED_RESULT_RECEIVED',0)+populations.get('DIRECT_FALLBACK',0)==2048,
            'complete even/odd population')
    return {'status':'DIAGNOSTIC_SAME_REQUEST_SERVICE_BRACKETS_ONLY','records':BOUND,
        'populations':populations,'rows':output,'unique_batches':batches,
        'interpretation':'One monotonic origin; caller-select and worker apply/write/Close are inclusive wall brackets, overlap and scheduling included. Shared batch rows are not exclusive per-request service; Write may include meta sync but does not isolate it. No positive queue-residence lower bound, CPU, lock-owner, phase cause or historical-CV acceptance.',
        'OriginalDurationsMatch':'Producer in-process check; no independent original latency array serialized',
        'panic_Goexit':'MODEL_ONLY','nonnil_batch_Close_fault':'UNAVAILABLE',
        'M0_CV_acceptance':False,'causal_repair':False}
