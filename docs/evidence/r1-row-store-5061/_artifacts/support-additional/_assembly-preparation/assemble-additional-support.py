#!/usr/bin/env python3
"""Append frozen adopted groups10/11 as private text support; never qualify evidence."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat

PLAN_SHA256 = '9ffdb5e269b17435e5e97624c9aaf35f2cd546e188fa0e3b856491214a9dd394'


def fail(message):
    raise RuntimeError(message)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def relative(value):
    path = PurePosixPath(value)
    if not value or path.is_absolute() or '..' in path.parts or '\\' in value or str(path) != value:
        fail('unsafe relative path: ' + value)
    return path


def canonical_dir(value):
    path = Path(value)
    if not path.is_absolute() or '..' in path.parts or str(path) != value:
        fail('canonical absolute directory required: ' + value)
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current /= part
        info = current.lstat()
        if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode):
            fail('directory symlink or non-directory: ' + str(current))
    return path


def read_regular(base, name, expected_mode=None, text=True):
    path = base
    parts = relative(name).parts
    for i, part in enumerate(parts):
        path /= part
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode):
            fail('symlink: ' + str(path))
        if i != len(parts) - 1 and not stat.S_ISDIR(info.st_mode):
            fail('non-directory parent: ' + str(path))
    if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o111:
        fail('nonregular or executable: ' + str(path))
    if expected_mode is not None and stat.S_IMODE(info.st_mode) != expected_mode:
        fail('mode differs: ' + str(path))
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), 'rb') as f:
        before = os.fstat(f.fileno())
        data = f.read()
        after = os.fstat(f.fileno())
    identity = lambda s: (s.st_dev, s.st_ino, s.st_mode, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
    if identity(info) != identity(before) or identity(before) != identity(after) or identity(after) != identity(path.lstat()):
        fail('input changed while reading: ' + str(path))
    if len(data) != after.st_size:
        fail('input size changed: ' + str(path))
    if text:
        if b'\0' in data:
            fail('NUL/nontext input: ' + str(path))
        data.decode('utf8')
    return data


def pinned(base, name, sha, size=None, mode=None, text=True):
    data = read_regular(base, name, mode, text)
    if digest(data) != sha or (size is not None and len(data) != size):
        fail('hash/size mismatch: ' + name)
    return data


def ledger(data):
    result = {}
    for line in data.decode('utf8').splitlines():
        match = re.fullmatch(r'([0-9a-f]{64})  (.+)', line)
        if not match:
            fail('malformed SHA256 ledger')
        sha, name = match.groups()
        relative(name)
        if name in result:
            fail('duplicate ledger path: ' + name)
        result[name] = sha
    if not result:
        fail('empty SHA256 ledger')
    return result


def listed_files(root):
    result = set()
    for directory, dirs, files in os.walk(root, followlinks=False):
        for name in dirs:
            path = Path(directory, name)
            if path.is_symlink():
                fail('symlink directory: ' + str(path))
        for name in files:
            path = Path(directory, name)
            rel = path.relative_to(root).as_posix()
            read_regular(root, rel)
            result.add(rel)
    return result


def validate_groups(plan, source):
    if plan['destination'] != 'support-additional' or {g['number'] for g in plan['groups']} != {10, 11}:
        fail('only groups10 and11 in support-additional are permitted')
    if len(plan['groups']) != 2:
        fail('duplicate group contract')
    for group in plan['groups']:
        relative(group['name'])
        relative(group['adoption_relative'])
        root = source / group['name']
        canonical_dir(str(root))
        sums_bytes = pinned(root, 'SUPPORT_SHA256SUMS', group['support_ledger_sha256'])
        sums = ledger(sums_bytes)
        if len(sums) != group['ledger_members'] or listed_files(root) != set(sums) | {'SUPPORT_SHA256SUMS'}:
            fail('support ledger does not exactly cover frozen group')
        for name, sha in sums.items():
            pinned(root, name, sha)
        inv = json.loads(pinned(root, 'inventory.json', group['inventory_sha256']))
        pinned(root, 'handoff.json', group['handoff_sha256'])
        adoption = json.loads(pinned(source, group['adoption_relative'], group['adoption_sha256'], group['adoption_bytes']))
        adoption_pins = adoption.get('pins', {
            'inventory.json': adoption.get('inventory_sha256'),
            'handoff.json': adoption.get('handoff_sha256'),
            'SUPPORT_SHA256SUMS': adoption.get('support_ledger_sha256')})
        expected = {'inventory.json': group['inventory_sha256'], 'handoff.json': group['handoff_sha256'],
                    'SUPPORT_SHA256SUMS': group['support_ledger_sha256']}
        if adoption_pins != expected or not adoption.get('root_adopted'):
            fail('root adoption does not bind exact inventory/handoff/ledger')
        if adoption['original_count'] != group['original_count'] or adoption['original_bytes'] != group['original_bytes'] or adoption['ledger_members'] != group['ledger_members']:
            fail('root adoption count contract differs')
        rows = inv['files']
        if len(rows) != group['original_count'] or sum(row['original_bytes'] for row in rows) != group['original_bytes']:
            fail('inventory original count/size contract differs')
        if len({row['copy'] for row in rows}) != len(rows):
            fail('inventory copy collision')
        for row in rows:
            if row['original_sha256'] != row['copy_sha256'] or row['original_bytes'] != row['copy_bytes']:
                fail('inventory original/copy pair differs')
            original = pinned(source, row['original_relative'], row['original_sha256'], row['original_bytes'], int(row['original_mode'], 8))
            copy = pinned(root, row['copy'], row['copy_sha256'], row['copy_bytes'], int(row['copy_mode'], 8))
            if original != copy:
                fail('actual original/copy bytes differ')


def write_new(path, data):
    path.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
    canonical_dir(str(path.parent))
    with path.open('xb') as f:
        f.write(data)
    os.chmod(path, 0o644)


def assemble(plan, plan_bytes, helper_bytes, source, stage, expected_core_sha256):
    if not re.fullmatch('[0-9a-f]{64}', expected_core_sha256):
        fail('independent exact core SHA256 required')
    if source == stage:
        fail('stage cannot equal source')
    for group in plan['groups']:
        root = source / group['name']
        if stage.is_relative_to(root) or root.is_relative_to(stage):
            fail('stage overlaps frozen group')
    core_before = pinned(stage, 'SHA256SUMS', expected_core_sha256)
    for name, sha in ledger(core_before).items():
        if relative(name).parts[0] == 'support-additional':
            fail('core ledger includes supplemental destination')
        pinned(stage, name, sha, text=False)
    pub_path = stage / 'PUBLICATION_SHA256SUMS'
    publication_before = read_regular(stage, 'PUBLICATION_SHA256SUMS') if os.path.lexists(pub_path) else None
    supplement = stage / 'support-additional'
    if os.path.lexists(supplement):
        fail('exclusive support-additional already exists')
    validate_groups(plan, source)
    files = plan['files']
    if len(files) != plan['counts']['copied_frozen_and_adoption_files'] or sum(f['bytes'] for f in files) != plan['counts']['copied_frozen_and_adoption_bytes']:
        fail('frozen file-count/size contract differs')
    if len({f['destination_relative'] for f in files}) != len(files):
        fail('destination collision')
    payloads = []
    by_number = {g['number']: g for g in plan['groups']}
    group_sources = {g['number']: {g['adoption_relative']} | {
        g['name'] + '/' + name for name in listed_files(source / g['name'])}
        for g in plan['groups']}
    for entry in files:
        group = by_number[entry['group']]
        rel = relative(entry['destination_relative'])
        if rel.parts[0] != group['destination'] or group['destination'] != 'group' + str(group['number']):
            fail('wrong group destination namespace')
        if entry['source_relative'] not in group_sources[entry['group']]:
            fail('source is outside adopted group')
        data = pinned(source, entry['source_relative'], entry['sha256'], entry['bytes'], entry['mode'])
        payloads.append((entry, data))
    for group in plan['groups']:
        if {f['source_relative'] for f in files if f['group'] == group['number']} != group_sources[group['number']]:
            fail('plan omits or duplicates frozen group member')
    # Everything is verified before exclusive stage creation. No original writes.
    if read_regular(stage, 'SHA256SUMS') != core_before:
        fail('core changed before assembly')
    supplement.mkdir(mode=0o755)
    for entry, data in payloads:
        write_new(supplement / entry['destination_relative'], data)
    evidence = {'_assembly-preparation/assemble-additional-support.py': helper_bytes,
                '_assembly-preparation/assembly-plan.json': plan_bytes}
    for name, data in evidence.items():
        write_new(supplement / name, data)
    for entry, data in payloads:
        if read_regular(supplement, entry['destination_relative'], 0o644) != data or read_regular(source, entry['source_relative'], entry['mode']) != data:
            fail('copied/frozen bytes changed during assembly')
    validate_groups(plan, source)
    record = {'schema_version': 1, 'utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'status': 'UNPROMOTED_ADDITIONAL_TEXT_SUPPORT_ASSEMBLY_ONLY',
        'groups': plan['groups'], 'source_base_actual': str(source), 'stage_actual': str(stage),
        'assembly_plan_sha256': digest(plan_bytes), 'executed_helper_sha256': digest(helper_bytes),
        'core_SHA256SUMS_sha256_before_and_after': expected_core_sha256,
        'core_SHA256SUMS_unchanged': True, 'PUBLICATION_SHA256SUMS_unchanged': True,
        'PUBLICATION_SHA256SUMS_preexisting_sha256': None if publication_before is None else digest(publication_before),
        'files': files, 'counts': plan['counts'], 'copied_mode': '0644', 'limits': plan['limits']}
    record_bytes = (json.dumps(record, indent=2, sort_keys=True) + '\n').encode()
    write_new(supplement / 'assembly-record.json', record_bytes)
    sums = {entry['destination_relative']: entry['sha256'] for entry in files}
    sums.update({name: digest(data) for name, data in evidence.items()})
    sums['assembly-record.json'] = digest(record_bytes)
    sum_bytes = ''.join(sha + '  ' + name + '\n' for name, sha in sorted(sums.items())).encode()
    write_new(supplement / 'SUPPORT_SHA256SUMS', sum_bytes)
    for name, sha in sums.items():
        pinned(supplement, name, sha, mode=0o644)
    if read_regular(stage, 'SHA256SUMS') != core_before:
        fail('core SHA256SUMS changed')
    publication_after = read_regular(stage, 'PUBLICATION_SHA256SUMS') if os.path.lexists(pub_path) else None
    if publication_after != publication_before:
        fail('PUBLICATION_SHA256SUMS changed')
    return {'status': record['status'], 'directory': str(supplement), 'frozen_and_adoption_files': len(files),
            'frozen_and_adoption_bytes': plan['counts']['copied_frozen_and_adoption_bytes'],
            'supplemental_ledger_members': len(sums), 'SUPPORT_SHA256SUMS_sha256': digest(sum_bytes),
            'core_SHA256SUMS_unchanged': True, 'PUBLICATION_SHA256SUMS_unchanged': True}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--stage', required=True, help='Root-selected existing actual stage, canonical absolute path')
    p.add_argument('--expected-core-sha256', required=True, help='Independent SHA256 of selected stage/core SHA256SUMS')
    p.add_argument('--expected-helper-sha256', required=True, help='Independent approved SHA256 of this helper')
    p.add_argument('--source-base', default='/private/tmp/gomap-r1-execution-20261005')
    a = p.parse_args()
    here = canonical_dir(str(Path(__file__).absolute().parent))
    helper = pinned(here, Path(__file__).name, a.expected_helper_sha256)
    plan_bytes = pinned(here, 'assembly-plan.json', PLAN_SHA256)
    source = canonical_dir(a.source_base)
    stage = canonical_dir(a.stage)
    if here.is_relative_to(stage) or stage.is_relative_to(here):
        fail('stage overlaps helper preparation')
    print(json.dumps(assemble(json.loads(plan_bytes), plan_bytes, helper, source, stage, a.expected_core_sha256)))


if __name__ == '__main__':
    main()
