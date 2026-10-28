#!/usr/bin/env python3
"""Optional read-only storage snapshot. Never executes work or qualifies a gate."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import time

LIMIT = 65536
MAX_PATHS = 16
MAX_OUTPUT = 262144
MAX_DIGEST_BYTES = 512 * 1024 * 1024
STAGES = ('before-build', 'after-build', 'after-lifecycle', 'quiet-start',
          'quiet-end', 'collection-start', 'collection-end')
# Bounded O_NOFOLLOW counter reads follow mvcc_raw_sync_context.py at #5282
# af8a87f45. No import or dependency on that unmerged collector is required.


def read_counter(path):
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
        with os.fdopen(fd, 'rb') as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                return {'status': 'unavailable', 'reason': 'nonregular'}
            data = stream.read(LIMIT + 1)
        if len(data) > LIMIT:
            return {'status': 'unavailable', 'reason': 'truncated'}
        return {'status': 'available', 'text': data.decode('utf-8', errors='strict')}
    except (OSError, UnicodeError):
        return {'status': 'unavailable', 'reason': 'read_failed'}


def unescape(value):
    return re.sub(r'\\([0-7]{3})', lambda m: chr(int(m[1], 8)), value)


def mount_table(proc):
    record = read_counter(proc / 'self/mountinfo')
    if record['status'] != 'available':
        return None
    rows = []
    try:
        for line in record['text'].splitlines():
            f = line.split()
            sep = f.index('-')
            if sep < 6 or len(f) < sep + 4 or not re.fullmatch(r'\d+:\d+', f[2]):
                return None
            rows.append({'id': int(f[0]), 'device': f[2],
                         'mount': unescape(f[4]), 'filesystem': f[sep + 1]})
    except (ValueError, IndexError):
        return None
    return rows or None


def inventory(path, mounts, sysroot):
    # Do not serialize arbitrary caller paths, mount sources/options, credentials,
    # environment, command lines, or unrestricted proc text.
    try:
        resolved = Path(path).resolve(strict=True)
        dev = resolved.stat().st_dev
        device = f'{os.major(dev)}:{os.minor(dev)}'
        eligible = [m for m in mounts or [] if resolved.is_relative_to(m['mount'])]
        if not eligible:
            return {'status': 'unavailable', 'reason': 'mount_unresolved'}
        if len({m['mount'] for m in eligible}) != len(eligible):
            return {'status': 'unavailable', 'reason': 'ambiguous_mount'}
        longest = max(len(m['mount']) for m in eligible)
        matches = [m for m in eligible if len(m['mount']) == longest]
        if len(matches) != 1:
            return {'status': 'unavailable', 'reason': 'ambiguous_mount'}
        mount = matches[0]
        if device != mount['device']:
            return {'status': 'unavailable', 'reason': 'device_mismatch'}
        result = {'status': 'available', 'mount_id': mount['id'],
                  'device': device, 'filesystem': mount['filesystem'],
                  'actual_fixture_child': 'unavailable',
                  'physical_isolation': 'unavailable', 'ownership': 'unavailable'}
        # Device identity alone does not establish physical separation. Deliberately
        # do not infer parent/slave topology or ownership from symlink names.
        disk = read_counter(sysroot / 'dev/block' / device / 'stat')
        if disk['status'] == 'available':
            try:
                values = [int(v) for v in disk['text'].split()]
                if len(values) < 11 or any(v < 0 for v in values):
                    raise ValueError()
                result['device_stat'] = {'status': 'available', 'raw_fields': values,
                    'scope': 'block device cumulative counters; no dirty-page or owner attribution'}
            except ValueError:
                result['device_stat'] = {'status': 'unavailable', 'reason': 'schema'}
        else:
            result['device_stat'] = disk
        return result
    except (OSError, RuntimeError, ValueError):
        return {'status': 'unavailable', 'reason': 'path_unresolved'}


def memory(proc):
    record = read_counter(proc / 'meminfo')
    if record['status'] != 'available':
        return record
    fields = {}
    for line in record['text'].splitlines():
        match = re.fullmatch(r'(Dirty|Writeback):\s+(\d+) kB', line)
        if match:
            if match[1] in fields:
                return {'status': 'unavailable', 'reason': 'duplicate'}
            fields[match[1]] = int(match[2])
    if set(fields) != {'Dirty', 'Writeback'}:
        return {'status': 'unavailable', 'reason': 'schema'}
    return {'status': 'available', 'scope': 'machinewide; no device or ownership attribution',
            'unit': 'KiB', **fields}


def file_digest(path):
    if path is None:
        return {'status': 'unavailable', 'reason': 'not_provided'}
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
        with os.fdopen(fd, 'rb') as stream:
            before = os.fstat(stream.fileno())
            if not stat.S_ISREG(before.st_mode):
                return {'status': 'unavailable', 'reason': 'nonregular'}
            if before.st_size > MAX_DIGEST_BYTES:
                return {'status': 'unavailable', 'reason': 'size_limit'}
            digest = hashlib.sha256()
            count = 0
            while True:
                chunk = stream.read(min(LIMIT, MAX_DIGEST_BYTES + 1 - count))
                if not chunk:
                    break
                count += len(chunk)
                if count > MAX_DIGEST_BYTES:
                    return {'status': 'unavailable', 'reason': 'size_limit'}
                digest.update(chunk)
            after = os.fstat(stream.fileno())
            identity = lambda st: (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns)
            if identity(before) != identity(after) or count != before.st_size:
                return {'status': 'unavailable', 'reason': 'changed_during_read'}
            return {'status': 'available', 'bytes': count, 'sha256': digest.hexdigest()}
    except OSError:
        return {'status': 'unavailable', 'reason': 'read_failed'}


def collect(paths, *, stage, source_sha=None, binary=None, harness=None,
            proc=Path('/proc'), sysroot=Path('/sys')):
    if stage not in STAGES:
        raise ValueError('known stage required')
    if source_sha is not None and not re.fullmatch(r'[0-9a-f]{40}', source_sha):
        raise ValueError('full lowercase source SHA required')
    if not 1 <= len(paths) <= MAX_PATHS:
        raise ValueError('one to sixteen paths required')
    mounts = mount_table(proc)
    return {'schema': 2, 'qualification': 'unavailable', 'diagnostic_only': True,
            'stage': stage, 'stage_origin': 'operator label; execution phase not verified',
            'identity': {'source': ({'status': 'available', 'sha': source_sha,
                                     'scope': 'operator assertion; checkout not verified'}
                                    if source_sha else {'status': 'unavailable', 'reason': 'not_provided'}),
                         'binary': file_digest(binary), 'harness': file_digest(harness)},
            'scope': 'single read-only snapshot; no history, quiet verdict, or causal attribution',
            'monotonic_ns': time.monotonic_ns(), 'machinewide_memory': memory(proc),
            'paths': [inventory(p, mounts, sysroot) for p in paths]}


def write_fresh(output, record):
    payload = (json.dumps(record, sort_keys=True, indent=2) + '\n').encode()
    if len(payload) > MAX_OUTPUT:
        raise ValueError('output bound exceeded')
    # Require an existing parent with trusted ancestors: O_NOFOLLOW protects
    # only the final directory component. Open it then use dir_fd to avoid
    # final-component symlink substitution; O_EXCL never overwrites evidence.
    parent = Path(output).parent
    fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        out = os.open(Path(output).name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                      0o600, dir_fd=fd)
        with os.fdopen(out, 'wb') as stream:
            stream.write(payload)
    finally:
        os.close(fd)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True)
    parser.add_argument('--path', action='append', required=True)
    parser.add_argument('--stage', choices=STAGES, required=True)
    parser.add_argument('--source-sha')
    parser.add_argument('--binary')
    parser.add_argument('--harness')
    args = parser.parse_args()
    try:
        write_fresh(args.output, collect(args.path, stage=args.stage, source_sha=args.source_sha,
                                         binary=args.binary, harness=args.harness))
    except (OSError, ValueError):
        parser.exit(2, 'storage context unavailable; no qualification result\n')


if __name__ == '__main__':
    main()
