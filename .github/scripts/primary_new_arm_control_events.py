#!/usr/bin/env python3
"""Fixed four-top B8e/Cb24 PRIMARY source-control inventory; never a product parser."""
import json
from pathlib import Path

PREFIX = 'github.com/snissn/gomap/TreeDB/'
TOPS = {
    PREFIX+'caching': {'TestPrimaryAllocationObserverOwnershipAndSeam',
                       'TestPrimaryAllocationObserverLateFinishDoesNotChargeSuccessor'},
    PREFIX+'mvcc': {'TestPrimaryAllocationRawDeltaAndRefusals',
                    'TestPrimaryAllocationOutputIsExclusiveAndBounded'},
}
CHILDREN = {
    'TestPrimaryAllocationRawDeltaAndRefusals': ('0','1','2','3'),

}
REQUIRED = {(p,n) for p,names in TOPS.items() for n in names}
REQUIRED |= {(p,n+'/'+c) for p,names in TOPS.items() for n in names for c in CHILDREN.get(n,())}
SELECTOR = '^('+'|'.join(sorted(n for names in TOPS.values() for n in names))+')$'


def validate(path):
    path = Path(path)
    if path.stat().st_size > 32*1024**2:
        raise ValueError('control event stream exceeds32MiB')
    ran, passed, packages = set(), set(), set()
    compiler = {p+s for p in TOPS for s in (' ['+p+'.test]', '_test ['+p+'.test]', '.test')}
    with path.open(encoding='utf-8') as stream:
        for line in stream:
            e = json.loads(line)
            if not isinstance(e,dict) or not isinstance(e.get('Action'),str):
                raise ValueError('malformed event')
            action = e['Action']
            if action == 'build-output':
                if e.get('ImportPath') not in compiler or 'Package' in e or not isinstance(e.get('Output'),str):
                    raise ValueError('foreign compiler transport')
                continue
            pkg, name = e.get('Package'), e.get('Test')
            if pkg not in TOPS or action not in ('start','run','output','pause','cont','pass'):
                raise ValueError('failed/skipped/foreign event')
            if not isinstance(e.get('Output',''),str) or 'WARNING: DATA RACE' in e.get('Output',''):
                raise ValueError('malformed output or race')
            if name is None:
                if action == 'pass':
                    if pkg in packages:
                        raise ValueError('duplicate package pass')
                    packages.add(pkg)
                elif action not in ('start','output'):
                    raise ValueError('foreign package action')
                continue
            if not isinstance(name,str) or (pkg,name) not in REQUIRED or pkg in packages:
                raise ValueError('unexpected or late test event')
            key = (pkg,name)
            if action == 'run':
                if key in ran:
                    raise ValueError('duplicate RUN')
                ran.add(key)
            elif action == 'pass':
                if key not in ran or key in passed:
                    raise ValueError('unentered/duplicate PASS')
                passed.add(key)
            elif action not in ('output','pause','cont'):
                raise ValueError('foreign test action')
    if ran != REQUIRED or passed != REQUIRED or packages != set(TOPS):
        raise ValueError('missing/unfinished exact control inventory')
    return {'status':'EXACT_SOURCE_CONTROL_INVENTORY_PASS','tops':4,'outcomes':8,'leaves':7,
            'product_capture':False,'product_performance_qualified':False}
