"""Strict public Go JSON grammar, reused from reviewed parser rectification R2.

Build diagnostics are retained; they never count as runtime package/tests.
"""
import json
from protocol import need

def functional_events(raw, expected_tests, package):
    import datetime, math, re
    def object_pairs(pairs):
        out={}
        for key,value in pairs:
            need(key not in out, 'duplicate functional JSON key')
            out[key]=value
        return out
    events=[json.loads(line,object_pairs_hook=object_pairs,
        parse_constant=lambda value: (_ for _ in ()).throw(ValueError('nonfinite functional JSON')))
        for line in raw.splitlines() if line.strip()]
    need(events and all(type(e) is dict for e in events), 'empty functional stream')
    need(expected_tests and all(type(x) is str and x and '/' not in x for x in expected_tests)
         and len(set(expected_tests))==len(expected_tests), 'expected functional test census')
    runtime=[]; builds=[]
    for e in events:
        action=e.get('Action')
        need(action not in ('fail','skip','build-fail'), 'failed/skipped functional check')
        if action=='build-output':
            # go help buildjson: ImportPath is a build ID, not TestEvent.Package.
            need(set(e)=={'ImportPath','Action','Output'} and type(e['ImportPath']) is str
                 and type(e['Output']) is str, 'malformed functional build-output')
            builds.append(e)
            continue
        need(action in ('start','run','pause','cont','output','pass'), 'unknown functional runtime action')
        need({'Time','Action','Package'} <= set(e) <= {'Time','Action','Package','Test','Output','Elapsed'}, 'malformed functional runtime schema')
        need(type(e['Time']) is str and e.get('Package')==package, 'functional package drift')
        # Validate Go RFC3339Nano without relying on Python 3.11 ISO extensions.
        # Only the temporary calendar-check string is normalized; raw Time is retained.
        match=re.fullmatch(r'([0-9]{4}-[0-9]{2}-[0-9]{2}T(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9])(?:\.([0-9]{1,9}))?(Z|[+-](?:[01][0-9]|2[0-3]):[0-5][0-9])',e['Time'])
        need(match is not None, 'functional RFC3339Nano timestamp grammar')
        base,fraction,zone=match.groups()
        normalized=base+('.'+fraction[:6].ljust(6,'0') if fraction else '')+('+00:00' if zone=='Z' else zone)
        stamp=datetime.datetime.fromisoformat(normalized)
        need(stamp.tzinfo is not None, 'functional timestamp timezone')
        if 'Elapsed' in e:
            need(type(e['Elapsed']) in (int,float) and math.isfinite(e['Elapsed']) and e['Elapsed']>=0, 'functional elapsed grammar')
        if 'Output' in e:need(type(e['Output']) is str and action=='output','functional output grammar')
        if action=='output':need('Output' in e,'functional output missing')
        if 'Test' in e:
            need(type(e['Test']) is str and e['Test'] and e['Test'].split('/')[0] in expected_tests, 'unexpected functional test')
        if action in ('run','pause','cont'):need('Test' in e,'functional test identity missing')
        if action=='start':need('Test' not in e,'functional package start grammar')
        runtime.append(e)
    need(runtime and runtime[0]['Action']=='start' and sum(e['Action']=='start' for e in runtime)==1,'functional package start census')
    need(runtime[-1]['Action']=='pass' and 'Test' not in runtime[-1], 'missing final package PASS')
    need(sum(e['Action']=='pass' and 'Test' not in e for e in runtime)==1,'missing/duplicate package PASS')
    runs=[e['Test'] for e in runtime if e['Action']=='run']
    passed=[e['Test'] for e in runtime if e['Action']=='pass' and 'Test' in e]
    need(len(set(runs))==len(runs) and len(set(passed))==len(passed) and sorted(runs)==sorted(passed),'missing/duplicate functional test lifecycle')
    active=set()
    for e in runtime:
        if e['Action']=='run':active.add(e['Test'])
        elif e['Action'] in ('pass','pause','cont') and 'Test' in e:
            need(e['Test'] in active,'functional test event before run/after pass')
            if e['Action']=='pass':active.remove(e['Test'])
    top=[name for name in passed if '/' not in name]
    need(sorted(top)==sorted(expected_tests),'missing/duplicate/unexpected characterization')
    return {'top_level_tests':len(top),'tests':sorted(top),'package':package,
            'runtime_events':len(runtime),'build_output_events':len(builds)}
