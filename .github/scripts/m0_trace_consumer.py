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
