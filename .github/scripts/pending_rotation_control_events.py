#!/usr/bin/env python3
"""Exact eight-top fixture controls, retaining explicit platform limitations."""
import json
from pathlib import Path

PKG = 'github.com/snissn/gomap/TreeDB/db'
CHILDREN = {
    'TestCommandWALCleanupZeroFrontierSafety': ('missing_replay','duplicate_replay','corrupt_reserve','pinned',
        'stale_root','stale_journal','pending_rotation','replaced_identity'),
    'TestCommandWALCleanupZeroFrontierNamespaceSync': ('no_op','sync_failure_and_retry'),
    'TestCommandWALCleanupFreshFixedActiveEntryControls': ('fixed_entry_false','fixed_entry_true'),
    'TestCommandWALCleanupFreshFixedActiveRefusesExisting': ('empty','populated','live','fixed'),
    'TestCommandWALCleanupStreamAppendWithoutPendingRotation': (),
    'TestCommandWALCleanupRejectsPendingJournalOwnershipAfterScan': (),
    'TestCommandWALCleanupConvergesAfterActiveAppend': (),
    'TestCommandWALCleanupEmptyFixedFirstFrameRotation': ('small_initial_reserve','oversized_first_atomic_frame'),
}
REQUIRED = set(CHILDREN) | {n+'/'+c for n,children in CHILDREN.items() for c in children}
SELECTOR = '^('+'|'.join(sorted(CHILDREN))+')$'
FIXED = {n+'/'+c for n in ('TestCommandWALCleanupZeroFrontierSafety',
    'TestCommandWALCleanupFreshFixedActiveEntryControls','TestCommandWALCleanupEmptyFixedFirstFrameRotation')
    for c in CHILDREN[n]}
FIXED |= {'TestCommandWALCleanupZeroFrontierNamespaceSync/sync_failure_and_retry',
          'TestCommandWALCleanupFreshFixedActiveRefusesExisting/fixed'}


def validate(path):
    path = Path(path)
    if path.stat().st_size > 32*1024**2:
        raise ValueError('fixture event stream exceeds32MiB')
    ran, passed, skipped, output = set(), set(), set(), {}
    package_pass = False
    compiler = {PKG+s for s in (' ['+PKG+'.test]', '_test ['+PKG+'.test]', '.test')}
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
            name = e.get('Test')
            if e.get('Package') != PKG or action not in ('start','run','output','pause','cont','pass','skip'):
                raise ValueError('failed/foreign event')
            text = e.get('Output','')
            if not isinstance(text,str) or 'WARNING: DATA RACE' in text:
                raise ValueError('malformed output or race')
            if name is None:
                if action == 'pass':
                    if package_pass:
                        raise ValueError('duplicate package pass')
                    package_pass = True
                elif action not in ('start','output'):
                    raise ValueError('foreign package action')
                continue
            if name not in REQUIRED or package_pass:
                raise ValueError('unexpected or late test event')
            if action == 'run':
                if name in ran:
                    raise ValueError('duplicate RUN')
                ran.add(name)
            elif action in ('pass','skip'):
                if name not in ran or name in passed or name in skipped:
                    raise ValueError('unentered/duplicate terminal')
                (passed if action=='pass' else skipped).add(name)
            elif action == 'output':
                output[name] = output.get(name,'')+text
            elif action not in ('pause','cont'):
                raise ValueError('foreign test action')
    if ran != REQUIRED or passed|skipped != REQUIRED or not package_pass:
        raise ValueError('missing/unfinished exact fixture inventory')
    for name in skipped:
        fixed = name in FIXED and 'requires fixed-container creation' in output.get(name,'')
        windows = (name=='TestCommandWALCleanupZeroFrontierSafety/replaced_identity' and
                   'Windows cannot rebind the retained open scan handle' in output.get(name,''))
        if not (fixed or windows):
            raise ValueError('unrecognized coverage skip')
    return {'status':'FIXTURE_PLATFORM_COVERAGE_INCOMPLETE' if skipped else 'EXACT_FIXTURE_INVENTORY_PASS',
            'tops':8,'outcomes':26,'leaves':21,'passed':len(passed),'skipped':sorted(skipped),
            'package_pass':True,'product_capture':False,'product_qualified':False}
