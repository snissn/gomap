"""Publish complete closed guard outputs; no Node or collector rerun."""
import base64
import hashlib
import json
import os
from pathlib import Path
import stat
import sys

LIMIT = 2097152


def read_closed(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_size > LIMIT:
            raise RuntimeError('publication_input')
        data = bytearray()
        while len(data) < before.st_size:
            block = os.read(fd, min(65536, before.st_size - len(data)))
            if not block:
                raise RuntimeError('publication_short_read')
            data.extend(block)
        if os.read(fd, 1):
            raise RuntimeError('publication_grew')
        after = os.fstat(fd)
        fields = ('st_dev', 'st_ino', 'st_nlink', 'st_mode', 'st_size', 'st_mtime_ns', 'st_ctime_ns')
        if any(getattr(before, n) != getattr(after, n) for n in fields):
            raise RuntimeError('publication_changed')
        return dict(bytes=len(data), sha256=hashlib.sha256(data).hexdigest(),
                    base64=base64.b64encode(data).decode('ascii'))
    finally:
        os.close(fd)


def main():
    root = Path(os.environ['RUNNER_TEMP']) / 'primary-construction-metadata'
    # Guard has returned and joined before any of these fixed files is read.
    stdout = read_closed(root / 'guard' / 'stdout.log')
    stderr = read_closed(root / 'guard' / 'stderr.log')
    receipt = read_closed(root / 'guard' / 'result.json')
    guard = json.loads(base64.b64decode(receipt['base64'], validate=True))
    exit_code = int(sys.argv[1])
    value = dict(schema='primary-metadata-joined-publication-v2', guard=guard,
                 guard_receipt=receipt, guard_exit=exit_code, guard_signal=None,
                 collector_stdout=stdout, collector_stderr=stderr)
    encoded = json.dumps(value, separators=(',', ':'), ensure_ascii=True).encode('ascii') + b'\n'
    if len(encoded) > LIMIT:
        raise RuntimeError('publication_complete_bound')
    if sys.stdout.buffer.write(encoded) != len(encoded):
        raise RuntimeError('publication_short_write')
    sys.stdout.buffer.flush()
    return 0 if (exit_code == 0 and guard['exit_code'] == 0 and not guard['errors'] and
                 not guard['cleanup_signals'] and not guard['pending_cleanup_signals'] and
                 guard['status'] == 'guard-completed-pending-independent-artifact-review') else 1


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except BaseException as error:
        print(json.dumps(dict(status='COMPLETE_PUBLICATION_FAILED', error_class=type(error).__name__)),
              flush=True)
        raise SystemExit(1)
