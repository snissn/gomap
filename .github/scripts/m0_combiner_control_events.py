#!/usr/bin/env python3
"""Exact M0 bridge, original owning and ordinal controls; no skipped coverage."""
import json
from pathlib import Path

PKG = 'github.com/snissn/gomap/TreeDB/db'
CHILDREN = {'TestM0CombinerOriginalProducerBranches': ('no_channel', 'full', 'unbuffered', 'stopped', 'saturated_stop', 'read_only', 'missing_db'), 'TestM0CombinerObservedSetCheckedLifecycle': (), 'TestM0CombinerActualCollectApplyFinishDrain': ('0', '1', '63', '64', '129'), 'TestM0CombinerStopReturnSlotLifetime': (), 'TestM0CombinerOverlapAndUnavailableControls': ('caller_invalid', 'worker_invalid', 'missing_terminal', 'absent_clock', 'clock_regression', 'serial_overflow', 'batch_bound', 'unknown_dispatch', 'unknown_write', 'unknown_enqueue', 'unknown_error', 'ordinal_bound', 'odd_binding', 'duplicate_binding'), 'TestM0CombinerDoneClosureIsNotSuccess': ('goexit_false', 'goexit_true'), 'TestM0CombinerPopulationAndSharedReceiptControls': ('duplicate_position', 'conflicting_shared_time', 'missing_member', 'odd_service', 'mixed_owner', 'identity', 'original_kind'), 'TestM0CombinerZeroValidClockAndDoneBorrowControls': (), 'TestM0CombinerObservedOriginalSyncFailure': (), 'TestCommitCombinerCollectAlreadyQueued': ('0', '1', '63', '64', '129'), 'TestCommitCombinerCollectedStopAppliesAndReleases': (), 'TestCommitCombinerBacklogFIFOAndBound': (), 'TestCommitCombinerSyncFailureFansOutAndPoisons': (), 'TestCommitCombinerConcurrentQueuedCopies': (), 'TestCommitCombinerFallback_CombinerStopped_UsesDirectWritePath': (), 'TestCommitCombinerFallback_QueueFullFastProbe': (), 'TestWriteViaCommitCombiner_UnbufferedQueue_DefaultFallback': (), 'TestWriteViaCommitCombiner_StopClosedBeforeEnqueue': (), 'TestWriteViaCommitCombiner_StopClosedDuringFastProbe': (), 'TestWriteViaCommitCombiner_StopClosedWhileWaitingResult': (), 'TestCommitCombinerFallback_PointerCommitDurableAfterReopen': (), 'TestCommitCombinerQueuedStopOwnsQualifiedInput': ('qualified', 'mismatched'), 'TestM0OrdinalObserverCompletenessControls': ('missing', 'duplicate', 'overflow', 'ordinal', 'region', 'kind', 'duration', 'error', 'join', 'timer', 'stats', 'exposure', 'trace', 'counts', 'hook', 'mismatch'), 'TestM0OrdinalObserverJoinKeepsOriginalDurations': ()}
REQUIRED = set(CHILDREN) | {n+'/'+c for n,children in CHILDREN.items() for c in children}
SELECTOR = '^('+'|'.join(sorted(CHILDREN))+')$'


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
    if skipped:
        raise ValueError('selected M0 source defines no qualifying platform skip; coverage incomplete')
    return {'status':'FIXTURE_PLATFORM_COVERAGE_INCOMPLETE' if skipped else 'EXACT_FIXTURE_INVENTORY_PASS',
            'tops':24,'outcomes':82,'leaves':74,'passed':len(passed),'skipped':sorted(skipped),
            'package_pass':True,'product_capture':False,'product_qualified':False}
