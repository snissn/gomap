#!/usr/bin/env python3
"""Bounded prospective context; never interprets a gate or attributes system I/O."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import time

LIMIT = 65536
COUNTERS = ('stat', 'diskstats', 'vmstat', 'pressure/cpu', 'pressure/io',
            'pressure/memory', 'self/cgroup', 'self/mountinfo')
CGROUP_FILES = ('cpu.stat', 'cpu.pressure', 'io.stat', 'io.pressure',
                'memory.events', 'memory.pressure', 'cpuacct.usage',
                'blkio.throttle.io_service_bytes', 'blkio.throttle.io_serviced')


def read_counter(path):
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
        with os.fdopen(fd, 'rb') as source:
            if not stat.S_ISREG(os.fstat(source.fileno()).st_mode):
                return {'status': 'unavailable', 'reason': 'nonregular'}
            data = source.read(LIMIT + 1)
        return {'status': 'truncated' if len(data) > LIMIT else 'available',
                'text': data[:LIMIT].decode('utf-8', errors='replace')}
    except OSError as error:
        return {'status': 'unavailable', 'errno': error.errno}


def unescape(value):
    return re.sub(r'\\([0-7]{3})', lambda m: chr(int(m[1], 8)), value)


def mounts(proc):
    record = read_counter(proc / 'self/mountinfo')
    result = []
    if record['status'] != 'available':
        return {'status': record['status'], 'entries': []}
    for line in record.get('text', '').splitlines():
        fields = line.split()
        try:
            separator = fields.index('-')
            result.append({'root': unescape(fields[3]), 'mount': unescape(fields[4]),
                           'device': fields[2], 'filesystem': fields[separator + 1],
                           'source': unescape(fields[separator + 2])})
        except (ValueError, IndexError):
            continue
    return {'status': 'available', 'entries': result}


def fixture_identity(proc, fixture_root):
    path = Path(fixture_root).resolve()
    result = {'inherited_temp_root': str(path), 'actual_fixture': 'unavailable',
              'scope': 'Go b.TempDir parent only; actual child path and device not observed'}
    try:
        metadata = path.stat()
        result['parent_device'] = f'{os.major(metadata.st_dev)}:{os.minor(metadata.st_dev)}'
        mount_record = mounts(proc)
        result['mountinfo_status'] = mount_record['status']
        eligible = [m for m in mount_record['entries'] if path.is_relative_to(m['mount'])]
        result['parent_mount'] = (max(eligible, key=lambda m: len(m['mount'])) if eligible
                                  else {'status': 'unavailable', 'mountinfo_status': mount_record['status']})
    except OSError as error:
        result['status'] = 'unavailable'
        result['errno'] = error.errno
    return result


def snapshot(proc):
    result = {name: read_counter(proc / name) for name in COUNTERS}
    groups = result['self/cgroup'].get('text', '').splitlines()[:32]
    cg = {}
    for line in groups:
        fields = line.split(':', 2)
        if len(fields) != 3:
            continue
        _, controllers, relative = fields
        for mount in mounts(proc)['entries']:
            if mount['filesystem'] != 'cgroup2':
                continue
            # v1 layouts differ by controller: explicitly unavailable below.
            if controllers or not relative.startswith('/') or '..' in Path(relative).parts:
                continue
            root = Path(mount['root'])
            group = Path(relative)
            if not group.is_relative_to(root):
                continue
            base = Path(mount['mount']) / group.relative_to(root)
            for name in CGROUP_FILES:
                cg[name] = read_counter(base / name)
    result['cgroup_counters'] = cg or {'status': 'unavailable', 'reason': 'no resolved cgroup v2 mount; v1 unsupported'}
    return result


def collect(output, binary, source_sha, source_tree, command, proc=Path('/proc')):
    # Exclusive creation rejects existing artifacts and symlinks before executing.
    with open(output, 'x', encoding='utf-8') as destination:
        digest = hashlib.sha256()
        with open(binary, 'rb') as executable:
            for chunk in iter(lambda: executable.read(1024 * 1024), b''):
                digest.update(chunk)
        digest = digest.hexdigest()
        record = {'schema': 1, 'source_sha': source_sha, 'source_tree': source_tree,
                  'executable': str(Path(binary).resolve()), 'sha256': digest,
                  'fixture': fixture_identity(proc, os.environ.get('TMPDIR') or '/tmp'),
                  'scope': 'system/container counters; no request attribution or physical-host isolation',
                  'tracing': {'status': 'not_requested', 'loss': 'unavailable'},
                  'timing_scope': 'monotonic process envelope, includes startup/setup/teardown; not timed benchmark phase',
                  'before': snapshot(proc)}
        record['start_monotonic_ns'] = time.monotonic_ns()
        try:
            status = subprocess.call(command)
            record['child_status'] = status
        except OSError as error:
            status = 127
            record['child_status'] = status
            record['launch_errno'] = error.errno
        record['end_monotonic_ns'] = time.monotonic_ns()
        record['after'] = snapshot(proc)
        try:
            json.dump(record, destination, indent=2)
            destination.write('\n')
            destination.flush()
        except OSError as error:
            print(f'raw-sync context write unavailable: errno={error.errno}; child status preserved', file=sys.stderr)
    return status if status >= 0 else 128 - status


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('output', 'binary', 'source-sha', 'source-tree'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    if not command:
        parser.error('command required')
    return collect(args.output, args.binary, args.source_sha, args.source_tree, command)


if __name__ == '__main__':
    sys.exit(main())
