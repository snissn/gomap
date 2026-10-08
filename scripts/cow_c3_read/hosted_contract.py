"""Hosted admission and root handoff. No authority is inferred from green CI."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import time

from protocol import digest, need, sha

BASELINE = '2d6b07f58902537cfe8d2b3e3a8c3c9ba0f3c07c'
CANDIDATE = '8d8806b495422e44f7802a96f5737f536a3fb2f0'
GO_ARCHIVE = 'go1.26.8.linux-amd64.tar.gz'
GO_SHA256 = 'd0f743b33e8d8945e6b1f432edd15785c70507121d6e2a723b21285eddf8b57b'
GO_BYTES = 66897291
CANONICAL = ('protocol.py', 'collect.py', 'build.py', 'prepare_config.py',
             'analyze.py', 'host_isolation_test.py', 'load_readiness_test.py')
POLICY = dict(cells=54, processes=756, warmups=108, measured=648, cycles=3,
              order=['baseline', 'candidate', 'candidate', 'baseline'],
              warmup_iterations=128, measured_iterations=1024, leaf_seconds=300,
              readiness_poll_seconds=5, readiness_total_seconds=600,
              readiness_load=4.5, host_load=5.0, noise=.30, adverse=.05,
              benefit=.10, min_free_bytes=20 << 30, gomaxprocs=4,
              construction_remaining_seconds=10200, matched_remaining_seconds=9600,
              admission_deadline_seconds=330 * 60, job_seconds=360 * 60,
              exclusions=[])
TEST_REGEX = '^(TestC3|TestGetAt|TestCommitAtGetAtGoldenHistories|TestMVCCOwnedOutputsSurviveMutationFlushAndClose)'
TESTS = [
    'TestC3COWPruneRefusesBeforeFloorWALEffects',
    'TestC3COWStoreReadersProgressDuringPreparedGroup',
    'TestC3CapturedCutMaterializesOutsideFloorAdmission',
    'TestC3GroupAdmissionCapabilityAndACK', 'TestC3ReadCutCapacityRefusalHasNoFallback',
    'TestC3ReadCutFailureAndClose', 'TestCommitAtGetAtGoldenHistories',
    'TestGetAtConcurrentReaders', 'TestGetAtIteratorCopiesBorrowedPayloadBeforeClose',
    'TestGetAtMalformedAndStorageErrors', 'TestGetAtPointSuccessorDiscardFloorAndPruneBoundary',
    'TestGetAtPointSuccessorValueLogCheckpointAndReopen', 'TestGetAtRandomizedOracle',
    'TestGetAtSuccessorTransfersOwnedPayload', 'TestGetAtUsesPointSuccessorWithoutIteratorRotation',
    'TestMVCCOwnedOutputsSurviveMutationFlushAndClose']


def utc(value):
    need(type(value) is str and re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?[+]00:00', value),
         'aware UTC required')
    return datetime.datetime.fromisoformat(value)


def strict_json(raw):
    def pairs(values):
        result = {}
        for key, value in values:
            need(key not in result, 'duplicate authority JSON key')
            result[key] = value
        return result
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=lambda x: need(False, 'nonfinite JSON'))


def capacity(value):
    need(value['system'] == 'Linux' and value['machine'] == 'x86_64', 'Linux amd64 required')
    need(type(value['cpu_count']) is int and value['cpu_count'] >= 4, 'four CPUs required')
    mask = value['affinity']
    need(type(mask) is list and len(mask) >= 4 and mask == sorted(set(mask))
         and all(type(x) is int and x >= 0 for x in mask), 'four runnable CPUs required')
    need(type(value['free_bytes']) is int and value['free_bytes'] >= POLICY['min_free_bytes'],
         '20 GiB actual free space required; no cleanup fallback')


def admission(state, remaining=0, clock=time.monotonic):
    need(state['policy'] == POLICY, 'hosted numeric policy drift')
    need(state['baseline'] == BASELINE and state['candidate'] == CANDIDATE, 'product drift')
    need(state['admission_deadline_monotonic'] - clock() >= remaining,
         'cooperative admission deadline reached; stop before next child')
    now = datetime.datetime.now(datetime.timezone.utc)
    need(utc(state['window_start_utc']) <= now <= utc(state['window_end_utc']), 'inactive run window')
    need((utc(state['window_end_utc']) - now).total_seconds() >= remaining, 'remaining run window too short')


def inventory(root):
    """Complete regular-file modes/bytes, including symlink-directory refusal."""
    root = Path(root)
    need(root.is_dir() and root.resolve() == root and not root.is_symlink(), 'physical inventory root required')
    rows = []
    for current, dirs, names in os.walk(root, followlinks=False):
        for name in sorted(dirs + names):
            path = Path(current) / name
            mode = path.lstat().st_mode
            need(not path.is_symlink(), 'symlink inventory member')
            if stat.S_ISDIR(mode):
                continue
            need(stat.S_ISREG(mode), 'nonregular inventory member')
            rows.append(dict(path=path.relative_to(root).as_posix(), sha256=sha(path),
                             bytes=path.stat().st_size, mode=stat.S_IMODE(mode)))
    need(rows, 'empty full inventory')
    return sorted(rows, key=lambda x: x['path'])


def observe_files(paths):
    """Observe regular identities without treating expected modes as admission."""
    rows, errors = {}, []
    for requested in paths:
        path = Path(requested); kind = 'UNOBSERVED'
        try:
            need(path.is_absolute() and '..' not in path.parts and str(path) == str(requested),
                 'physical absolute compiler path required')
            for parent in reversed(path.parents):
                need(stat.S_ISDIR(parent.lstat().st_mode), 'nonphysical compiler parent')
            value = path.lstat()
            kind = ('REGULAR' if stat.S_ISREG(value.st_mode) else
                    'SYMLINK' if stat.S_ISLNK(value.st_mode) else 'NONREGULAR')
            need(kind == 'REGULAR', 'compiler observation refuses symlink/special member')
            # Never follow a replaced leaf symlink or block on a replaced FIFO.
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            with os.fdopen(fd, 'rb') as source:
                opened = os.fstat(source.fileno())
                need(stat.S_ISREG(opened.st_mode) and
                     (opened.st_dev, opened.st_ino) == (value.st_dev, value.st_ino),
                     'compiler member changed during observation')
                identity = hashlib.sha256(); size = 0
                for block in iter(lambda: source.read(1 << 20), b''):
                    identity.update(block); size += len(block)
                final = os.fstat(source.fileno())
                need((opened.st_size,opened.st_mtime_ns,opened.st_ctime_ns) ==
                     (final.st_size,final.st_mtime_ns,final.st_ctime_ns) and size == opened.st_size,
                     'compiler bytes/mode changed during observation')
            rows[str(path)] = dict(sha256=identity.hexdigest(), bytes=size,
                                   mode=stat.S_IMODE(opened.st_mode))
        except (OSError, ValueError) as exc:
            errors.append(dict(requested_path=str(requested), type='MISSING' if isinstance(exc,FileNotFoundError) else kind,
                               error_type=type(exc).__name__, error=str(exc)))
    return rows, errors


def snapshot_files(paths):
    rows, errors = observe_files(paths)
    need(not errors and len(rows) == 3 and all(x['mode'] == 0o755 for x in rows.values()),
         'exactly three physical declared compiler executables 0755 required')
    return rows


def verify_snapshot(actual, expected, label):
    need(digest(actual) == digest(expected), label + ' inventory drift')


def released(commands, expected):
    need(type(commands) is list and len(commands) == len(expected)
         and [x['label'] for x in commands] == expected, 'missing/duplicate/wrong command census')
    need(all(x['started_child'] is True and x['child_joined'] is True
             and x['custody_released'] is True and type(x['wait_status']) is int
             and x['exit_code'] == 0 and x['timed_out'] is False and not x['signals']
             for x in commands), 'failed/unjoined construction')


def root_acceptance(comment, state, construction, now=None):
    """Only a fresh immutable root-authored API comment admits this same VM."""
    need(type(comment) is dict and type(comment.get('user', {}).get('id')) is int
         and comment['user']['id'] == state['root_actor_id'],
         'receipt author is not configured root account')
    need(comment['created_at'] == comment['updated_at'], 'edited receipt refused')
    value = strict_json(comment['body'])
    expected = dict(repository=state['repository'], workflow_sha=state['workflow_sha'],
                    run_id=state['run_id'], run_attempt=state['run_attempt'],
                    dispatch_actor_id=state['dispatch_actor_id'],
                    attempt=state['attempt'], construction_manifest_sha256=construction['manifest_sha256'],
                    config_sha256=construction['config_sha256'], policy_sha256=digest(POLICY),
                    baseline=BASELINE, candidate=CANDIDATE)
    need(set(value) == set(expected) | {'verdict', 'findings', 'reader_release',
         'all_source_and_artifact_readers_released', 'accepted_utc', 'end_utc'}, 'root receipt fields')
    need(all(type(value[k]) is type(v) and value[k] == v for k, v in expected.items()), 'wrong run/attempt/source/config receipt')
    need(value['verdict'] == 'ACCEPT_ACTUAL_HOSTED_C3_CONSTRUCTION' and value['findings'] == []
         and value['reader_release'] == 'RELEASED'
         and value['all_source_and_artifact_readers_released'] is True, 'construction not independently accepted/released')
    accepted, end = utc(value['accepted_utc']), utc(value['end_utc'])
    current = now or datetime.datetime.now(datetime.timezone.utc)
    need(utc(construction['closed_utc']) <= accepted <= current <= end
         and end <= utc(state['window_end_utc']), 'expired/predated/expanded acceptance window')
    comment_time = datetime.datetime.strptime(comment['created_at'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=datetime.timezone.utc)
    need(utc(construction['closed_utc']) <= comment_time <= current
         and abs((comment_time-accepted).total_seconds()) <= 60, 'stale root API timestamp')
    need((end - current).total_seconds() >= 9600, 'matched root window shorter than 9600 seconds')
    return value



def accepted_window(state, receipt):
    """The root may shorten the window; it may never expand the job admission."""
    result = dict(state)
    end = min(utc(receipt['end_utc']), utc(state['window_end_utc']))
    result['window_end_utc'] = end.isoformat()
    start_monotonic = state['admission_deadline_monotonic'] - POLICY['admission_deadline_seconds']
    result['admission_deadline_monotonic'] = min(state['admission_deadline_monotonic'],
        start_monotonic + (end - utc(state['window_start_utc'])).total_seconds())
    return result


def verify_seal(directory, manifest):
    """Recheck each sealed original; later separately named evidence is allowed."""
    need(manifest['schema'] == 'gomap-hosted-sealed-files-v1', 'sealed schema drift')
    files = manifest['files']
    need(type(files) is list and files and len({x['path'] for x in files}) == len(files),
         'missing/duplicate sealed files')
    root = Path(directory)
    for item in files:
        name = item['path']; path = root / name
        need(type(name) is str and name and not Path(name).is_absolute()
             and '..' not in Path(name).parts and str(Path(name)) == name
             and path.resolve() == path and path.is_file() and not path.is_symlink(),
             'unsafe/missing sealed member')
        need(set(item) == {'path','sha256','bytes','mode'} and sha(path) == item['sha256']
             and path.stat().st_size == item['bytes']
             and stat.S_IMODE(path.stat().st_mode) == item['mode'], 'sealed original drift: '+name)


def seal(directory, destination):
    """No source/cache/toolchain copy: seal only attributable generated evidence."""
    rows = inventory(directory)
    value = {'schema': 'gomap-hosted-sealed-files-v1', 'files': rows}
    destination = Path(destination)
    with destination.open('x') as output:
        json.dump(value, output, indent=2, sort_keys=True)
        output.write('\n')
        output.flush()
        os.fsync(output.fileno())
    return sha(destination)
