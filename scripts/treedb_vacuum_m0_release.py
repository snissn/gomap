#!/usr/bin/env python3
"""External, post-listener release observation; never a performance verdict.

Run from an independently reviewed copy on the same host after the one-job
listener exits and its administrator wrapper releases the host lease. Publish
this separate receipt with the two immutable Actions artifacts. Missing receipts
or a failed check leave the custody chain incomplete. This cannot establish that
the lock was held continuously between earlier observations.
"""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import sys

import treedb_vacuum_m0_environment as environment


def observe(packet, proc=Path('/proc')):
    receipts = {}
    hashes = {}
    for phase in ('admission', 'post-upload'):
        raw = (packet / f'environment-{phase}.json').read_bytes()
        hashes[phase] = hashlib.sha256(raw).hexdigest()
        receipts[phase] = json.loads(raw)
    raw = (packet / 'ci-budget.json').read_bytes()
    hashes['ci-budget'] = hashlib.sha256(raw).hexdigest()
    capture = json.loads(raw)
    if not isinstance(capture, dict):
        raise ValueError('controlled supervisor outcome receipt is missing or malformed')
    if capture.get('qualification_profile') != 'controlled-performance' or type(capture.get('exit_status')) is not int or type(capture.get('completed')) is not bool:
        raise ValueError('controlled supervisor outcome receipt is missing or malformed')
    initial, final = receipts['admission'], receipts['post-upload']
    for phase, receipt in receipts.items():
        if receipt.get('phase') != phase or receipt.get('admitted') is not True:
            raise ValueError(f'{phase} admission did not succeed')
        if receipt['configuration'].get('M0_CONTROLLED') != '1':
            raise ValueError('controlled custody receipts are required')
    if initial['lease'] != final['lease']:
        raise ValueError('custody identity changed')
    if not re.fullmatch(r'[0-9a-f]{40}', initial.get('source_sha') or '') or initial['source_sha'] != final.get('source_sha'):
        raise ValueError('exact source identity is missing or changed')
    identity = {key: initial['runner'].get(key) for key in ('GITHUB_RUN_ID', 'GITHUB_RUN_ATTEMPT')}
    if any(not re.fullmatch(r'[1-9]\d*', value or '') for value in identity.values()) or any(final['runner'].get(key) != value for key, value in identity.items()):
        raise ValueError('run identity is missing or changed')
    if capture.get('source_sha') != initial['source_sha'] or capture.get('run') != identity:
        raise ValueError('supervisor source/run/attempt identity is missing or changed')
    reason = capture.get('termination_reason')
    status = capture['exit_status']
    interrupted = capture.get('interrupted_signal')
    if (not 0 <= status <= 255 or
            reason not in ('capture-exit', 'timeout', 'launch-failure', 'cleanup-failure', 'interrupted') or
            capture['completed'] != (status == 0 and reason == 'capture-exit') or
            interrupted is not None and (type(interrupted) is not int or interrupted not in (1, 2, 15)) or
            reason == 'capture-exit' and interrupted is not None or
            reason == 'interrupted' and (interrupted is None or status != 128 + interrupted) or
            reason in ('timeout', 'launch-failure', 'cleanup-failure') and
            status != {'timeout': 124, 'launch-failure': 127, 'cleanup-failure': 125}[reason]):
        raise ValueError('supervisor outcome is inconsistent')
    boot = (proc / 'sys/kernel/random/boot_id').read_text()
    if initial.get('boot_id') != {'value': boot} or final.get('boot_id') != {'value': boot}:
        raise ValueError('release must be observed on the original boot')
    namespace = os.readlink(proc / 'self/ns/pid')
    if initial.get('pid_namespace') != namespace or final.get('pid_namespace') != namespace:
        raise ValueError('release must be observed in the original PID namespace')
    custody = initial['lease']
    path = Path(custody['path'])
    if not path.is_absolute() or path.is_symlink() or not path.is_file():
        raise ValueError('original lease file is unavailable')
    stat = path.stat()
    observed = [os.major(stat.st_dev), os.minor(stat.st_dev), stat.st_ino]
    if observed != custody['device_inode']:
        raise ValueError('lease file identity changed')
    listener = custody['listener']
    try:
        current = environment.process(listener['pid'], proc)
    except FileNotFoundError:
        current = None
    if current is not None and current['start_ticks'] == listener['start_ticks']:
        raise ValueError('original Runner.Listener has not exited')
    for line in (proc / 'locks').read_text().splitlines():
        fields = line.split()
        # Include blocked lock requests too: release observation must be clear.
        if '->' in fields:
            fields.remove('->')
        if len(fields) < 8:
            raise ValueError('unparseable kernel lock table')
        device = fields[5].split(':')
        if len(device) != 3:
            raise ValueError('unparseable kernel lock identity')
        if [int(device[0], 16), int(device[1], 16), int(device[2])] == observed:
            raise ValueError('lease file is still locked or contended')
    return {'source_sha': initial['source_sha'], 'run': identity,
            'lease': custody, 'receipt_sha256': hashes, 'capture_outcome': capture,
            'boot_id': boot.strip(), 'listener_exited_observed': True,
            'lease_unlocked_observed': True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--packet', type=Path, required=True,
                        help='original packet with post-upload artifact restored beside admission receipt')
    parser.add_argument('--output', type=Path, required=True,
                        help='new separate receipt path; never inside original packet')
    args = parser.parse_args()
    if args.output.resolve().is_relative_to(args.packet.resolve()):
        raise ValueError('release receipt must be separate from the original packet')
    receipt = {'phase': 'post-release',
               'utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
               'release_observation_complete': False, 'performance_acceptance': False,
               'continuous_custody_claim': False}
    status = 2
    try:
        receipt.update(observe(args.packet))
        receipt['release_observation_complete'] = True
        status = 0
    except (OSError, ValueError, KeyError, TypeError) as error:
        receipt['error'] = str(error)
    # Exclusive creation preserves both failed and successful observations.
    with args.output.open('x') as output:
        output.write(json.dumps(receipt, indent=2, sort_keys=True) + '\n')
        output.flush()
        os.fsync(output.fileno())
    return status


if __name__ == '__main__':
    sys.exit(main())
