#!/usr/bin/env python3
"""Root-invoked pinned text-byte copier; no evidence or numeric acceptance."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import sys

STAGE = Path('/home/mikers/gomap-r1-publication-stage-M-3325dfe-20261006')
NAMESPACE = 'final-M-3325dfe'
LEDGER = 'SUPPORT_SHA256SUMS'

def require(condition, message):
    if not condition:
        raise ValueError(message)

def digest(data):
    return hashlib.sha256(data).hexdigest()

def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def hex_sha(value):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{64}', value) is not None

def safe_absolute(value):
    require(isinstance(value, str) and value and not any(c in value for c in ('\0', '\n', '\r', '\\')), 'unsafe absolute path')
    p = PurePosixPath(value)
    require(p.is_absolute() and not value.startswith('//') and '..' not in p.parts and str(p) == value, 'absolute path must be canonical without traversal')
    return Path(value)

def nonsymlink_parents(path, include_self=False):
    checked = [*reversed(path.parents), path] if include_self else list(reversed(path.parents))
    for parent in checked:
        mode = parent.lstat().st_mode
        require(stat.S_ISDIR(mode) and not stat.S_ISLNK(mode), 'non-directory or symlink path ancestor: ' + str(parent))

def read_regular(path):
    nonsymlink_parents(path)
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        before = os.fstat(descriptor)
        require(stat.S_ISREG(before.st_mode), 'source is not a regular file: ' + str(path))
        with os.fdopen(descriptor, 'rb', closefd=False) as source:
            data = source.read()
        after = os.fstat(descriptor)
        identity = lambda s: (s.st_dev, s.st_ino, s.st_mode, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
        require(identity(before) == identity(after) and len(data) == after.st_size, 'source changed while reading: ' + str(path))
        require(b'\0' not in data, 'NUL bytes are forbidden: ' + str(path))
        data.decode('utf-8', errors='strict')
        return data, {'sha256': digest(data), 'bytes': len(data), 'device': after.st_dev, 'inode': after.st_ino}
    finally:
        os.close(descriptor)

def pairs(entries):
    obj = {}
    for key, value in entries:
        require(key not in obj, 'duplicate JSON key: ' + key)
        obj[key] = value
    return obj

def parse_json(data):
    def invalid(value):
        raise ValueError('nonfinite JSON value: ' + value)
    return json.loads(data.decode('utf-8'), object_pairs_hook=pairs, parse_constant=invalid)

def destination(value):
    require(isinstance(value, str) and value and not any(c in value for c in ('\0', '\n', '\r', '\\')), 'unsafe relative destination')
    p = PurePosixPath(value)
    require(not p.is_absolute() and '..' not in p.parts and str(p) == value and value != '.', 'destination must be canonical relative to the new namespace')
    require(value != LEDGER and p.parts[0] != NAMESPACE, 'destination names reserved ledger or repeats namespace')
    return value

def expected_bytes(path, expected_hash, expected_size=None):
    data, observed = read_regular(path)
    require(observed['sha256'] == expected_hash, 'source SHA256 differs from external plan: ' + str(path))
    require(expected_size is None or observed['bytes'] == expected_size, 'source size differs from external plan: ' + str(path))
    return data, observed

def write_new(path, data):
    nonsymlink_parents(path)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, 'wb', closefd=False) as output:
            output.write(data)
            output.flush()
        os.fchmod(descriptor, 0o644)
    finally:
        os.close(descriptor)

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--plan', required=True)
    p.add_argument('--expected-plan-sha256', required=True)
    p.add_argument('--core-ledger', required=True)
    p.add_argument('--expected-core-sha256', required=True)
    p.add_argument('--receipt-new', required=True)
    args = p.parse_args()
    started = now()
    program_path = safe_absolute(str(Path(__file__).absolute()))
    program_before, program_identity_before = read_regular(program_path)
    require(hex_sha(args.expected_plan_sha256) and hex_sha(args.expected_core_sha256), 'both independent CLI hashes must be lowercase SHA256')
    plan_path, core, receipt = (safe_absolute(v) for v in (args.plan, args.core_ledger, args.receipt_new))
    target = STAGE / NAMESPACE
    nonsymlink_parents(STAGE, include_self=True)
    require(core != STAGE and STAGE in core.parents and target not in core.parents, 'core ledger must be an existing file inside stage and outside the new namespace')
    require(receipt != STAGE and STAGE not in receipt.parents, 'assembly receipt must be outside the entire publication stage')
    require(plan_path != target and target not in plan_path.parents, 'plan must exist outside the new namespace')
    nonsymlink_parents(receipt)
    require(not receipt.exists() and not receipt.is_symlink(), 'receipt-new already exists')
    require(not target.exists() and not target.is_symlink(), 'final namespace already exists; no overwrite/retry')
    plan_data, plan_identity = expected_bytes(plan_path, args.expected_plan_sha256)
    core_data, core_identity = expected_bytes(core, args.expected_core_sha256)
    plan = parse_json(plan_data)
    require(isinstance(plan, list) and bool(plan), 'plan must be a nonempty explicit JSON list')
    entries, destinations, sources = [], set(), set()
    for row in plan:
        require(isinstance(row, dict) and set(row) == {'source', 'destination', 'sha256', 'bytes'}, 'each plan row must have exactly source/destination/sha256/bytes')
        require(hex_sha(row['sha256']) and type(row['bytes']) is int and row['bytes'] >= 0, 'invalid pinned source SHA256/size')
        source, relative = safe_absolute(row['source']), destination(row['destination'])
        require(source != receipt and source != target and target not in source.parents, 'source aliases a new output')
        require(relative not in destinations, 'duplicate physical destination')
        destinations.add(relative)
        sources.add(str(source))
        _, observed = expected_bytes(source, row['sha256'], row['bytes'])
        entries.append({**row, 'preflight': observed})
    for relative in destinations:
        require(not any(str(parent) in destinations for parent in PurePosixPath(relative).parents if str(parent) != '.'), 'file/directory destination collision')
    require(str(receipt) not in sources and receipt != plan_path and receipt != core, 'receipt would rewrite an original input')
    # Every source has passed preflight before any namespace or stage output exists.
    expected_bytes(plan_path, args.expected_plan_sha256)
    expected_bytes(core, args.expected_core_sha256)
    target.mkdir(mode=0o755, exist_ok=False)
    copied = []
    for row in entries:
        source = Path(row['source'])
        data, before = expected_bytes(source, row['sha256'], row['bytes'])
        output = target / row['destination']
        output.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
        write_new(output, data)
        _, actual = expected_bytes(output, row['sha256'], row['bytes'])
        require(stat.S_IMODE(output.lstat().st_mode) == 0o644, 'copied physical file mode differs')
        copied.append({'source': str(source), 'destination': str(output), 'stage_relative': NAMESPACE + '/' + row['destination'],
                       'sha256': actual['sha256'], 'bytes': actual['bytes'], 'mode': '0644', 'source_preflight': row['preflight'], 'source_immediately_before_copy': before})
    # Hash every copied physical file, exactly once. The ledger excludes itself.
    ledger_data = ''.join(row['sha256'] + '  ' + str(Path(row['destination']).relative_to(target)) + '\n' for row in sorted(copied, key=lambda v: v['stage_relative'])).encode('utf-8')
    ledger = target / LEDGER
    write_new(ledger, ledger_data)
    for row in copied:
        _, source_after = expected_bytes(Path(row['source']), row['sha256'], row['bytes'])
        row['source_after_copy'] = source_after
        expected_bytes(Path(row['destination']), row['sha256'], row['bytes'])
        require(stat.S_IMODE(Path(row['destination']).lstat().st_mode) == 0o644, 'copied file permissions changed')
    _, plan_after = expected_bytes(plan_path, args.expected_plan_sha256)
    _, core_after = expected_bytes(core, args.expected_core_sha256)
    require(core_data == read_regular(core)[0], 'core ledger bytes changed')
    require(read_regular(ledger)[0] == ledger_data and stat.S_IMODE(ledger.lstat().st_mode) == 0o644, 'support ledger changed')
    program_after, program_identity_after = read_regular(program_path)
    require(program_before == program_after, 'helper source bytes changed during assembly')
    result = {'state': 'PINNED_BYTES_COPIED_ONLY_NO_EVIDENCE_OR_NUMERIC_ACCEPTANCE',
              'started_utc': started, 'completed_utc': now(), 'actual_argv': sys.argv,
              'stage': str(STAGE), 'namespace': str(target),
              'helper': {'path': str(program_path), 'before': program_identity_before, 'after': program_identity_after, 'bytes_unchanged': True},
              'plan': {'path': str(plan_path), 'independent_cli_sha256': args.expected_plan_sha256, 'before': plan_identity, 'after': plan_after},
              'core_ledger': {'path': str(core), 'independent_cli_sha256': args.expected_core_sha256, 'before': core_identity, 'after': core_after, 'bytes_unchanged': True},
              'actual_copies': copied, 'support_ledger': {'path': str(ledger), 'sha256': digest(ledger_data), 'bytes': len(ledger_data), 'entries': len(copied), 'excludes_itself': True, 'paths_relative_to_namespace': True},
              'limits': ['Explicit file plan only; no directory traversal or recursive copying.', 'UTF8/noNUL regular nonsymlink files only; outputs have mode0644.', 'No acceptance records required or inferred; requested acceptance records may only be copied as bytes.', 'Core ledger byte hash is checked; physical core evidence and the complete final stage require separate root inventory.', 'No validation helpers outside namespace or FULL PUBLICATION ledger are changed.', 'Failures preserve partial namespace/files; no retry, cleanup, replacement or automatic acceptance.']}
    data = (json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + '\n').encode('utf-8')
    write_new(receipt, data)
    require(read_regular(receipt)[0] == data, 'new private assembly receipt differs')
    print(json.dumps({'state': result['state'], 'namespace': str(target), 'files': len(copied), 'support_ledger_sha256': digest(ledger_data), 'receipt': str(receipt), 'receipt_sha256': digest(data)}))

if __name__ == '__main__':
    try:
        main()
    except (ValueError, KeyError, OSError, UnicodeError) as error:
        print('final support byte copier refused: ' + str(error), file=sys.stderr)
        sys.exit(1)
