"""Finite owned-child custody. Cancellation is sticky through durable receipts.

An owner's separate-session descendants require its retained release proof.
Reaping/killing the owner alone never proves those descendants were reaped.
"""
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

CANCEL_SIGNALS = (signal.SIGINT, signal.SIGTERM, signal.SIGQUIT)

class StickySignals:
    def __init__(self):
        self.signals = []
        self.previous = None

    def __enter__(self):
        assert self.previous is None
        self.previous = {s: signal.signal(s, self.remember) for s in CANCEL_SIGNALS}
        return self

    def remember(self, signum, frame):
        self.signals.append({'signal': signum, 'at': time.time()})

    def __exit__(self, kind, error, traceback):
        # Callers leave this scope only after their final receipt is durable.
        for signum, handler in self.previous.items():
            signal.signal(signum, handler)
        self.previous = None


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1 << 20), b''):
            h.update(block)
    return h.hexdigest()


def durable(path, value):
    with Path(path).open('x') as stream:
        stream.write(json.dumps(value, indent=2, sort_keys=True) + '\n')
        stream.flush()
        os.fsync(stream.fileno())


def group_exists(pgid):
    try:
        os.killpg(pgid, 0)
        return True
    except ProcessLookupError:
        return False


def send(pgid, signum):
    try:
        os.killpg(pgid, signum)
    except ProcessLookupError:
        pass


def read_release(path):
    try:
        value = json.loads(Path(path).read_text())
        return value.get('custody_released') is True
    except (OSError, ValueError):
        return False


def monitored_release(directory):
    """Current pinned collect.run_child persists wait4 for every owned leaf."""
    directory = Path(directory)
    if not any((directory / name).is_file() for name in ('completion.json', 'failure.json', 'result.json')):
        return False
    try:
        for path in directory.glob('*-monitor.json'):
            value = json.loads(path.read_text())
            if (value['waited_pid'] != value['owner']['pid'] or
                type(value['wait_status']) is not int or value['reaped_monotonic_ns'] is None):
                return False
            if group_exists(value['owner']['pgid']):
                return False
        for path in directory.glob('*-initialization-failure-join.json'):
            value = json.loads(path.read_text())
            if value['child_pid'] != value['waited_pid'] or type(value['wait_status']) is not int:
                return False
            if group_exists(value['child_pid']):
                return False
        return True
    except (OSError, ValueError, KeyError):
        return False


def supervise(label, argv, cwd, environment, timeout, directory, sticky,
              owner_proof=None, cleanup=False, grace=15.0, input_data=None, receipt_fields=None,
              kill_join_timeout=20.0):
    """Borrow an already installed sticky scope through spawn/wait/receipt.

    owner_proof(row) is mandatory for owners with separate-session children.
    cleanup ignores further cancellation, while keeping it recorded, so an
    already-authorized mandatory closer can finish. Every stage is bounded.
    """
    assert sticky.previous is not None, 'sticky custody must precede spawn'
    assert timeout > 0 and grace > 0 and kill_join_timeout > 0
    directory = Path(directory); directory.mkdir(exist_ok=True)
    output = directory / (label + '.stdout'); errors = directory / (label + '.stderr')
    receipt = directory / (label + '.json')
    assert not receipt.exists()
    started = time.time(); deadline = time.monotonic() + timeout
    child = None; status = None; usage = None; failure = None
    timed_out = False; cancel_sent = False; escalated = False
    cancel_deadline = None; kill_deadline = None; input_error = None; offset = 0
    try:
        with output.open('xb') as stdout, errors.open('xb') as stderr:
            if sticky.signals and not cleanup:
                failure = {'type': 'CancelledBeforeSpawn', 'error': 'sticky cancellation; no child launched'}
            else:
                child = subprocess.Popen(argv, cwd=cwd, env=environment, stdout=stdout,
                    stderr=stderr, stdin=subprocess.PIPE if input_data is not None else None,
                    start_new_session=True)
                if child.stdin is not None:
                    os.set_blocking(child.stdin.fileno(), False)
                while status is None:
                    if child.stdin is not None and not child.stdin.closed:
                        try:
                            if offset < len(input_data):
                                offset += os.write(child.stdin.fileno(), input_data[offset:offset + 65536])
                            if offset == len(input_data):
                                child.stdin.close()
                        except BlockingIOError:
                            pass
                        except OSError as error:
                            input_error = repr(error); child.stdin.close()
                    try:
                        waited, value, resource = os.wait4(child.pid, os.WNOHANG)
                    except InterruptedError:
                        waited = 0  # Deadlines still advance after interrupted waits.
                    if waited:
                        status, usage = value, resource
                        break
                    expired = time.monotonic() >= deadline
                    cancelled = bool(sticky.signals) and not cleanup
                    if not cancel_sent and (expired or cancelled):
                        timed_out = expired
                        send(child.pid, signal.SIGINT)
                        cancel_sent = True; cancel_deadline = time.monotonic() + grace
                    if cancel_sent and time.monotonic() >= cancel_deadline and not escalated:
                        send(child.pid, signal.SIGKILL)
                        escalated = True
                        kill_deadline = time.monotonic() + kill_join_timeout
                    if escalated and time.monotonic() >= kill_deadline:
                        break  # Unreapable child remains HELD in the durable receipt.
                    time.sleep(.02)
    except BaseException as error:
        failure = {'type': type(error).__name__, 'error': str(error)}
    finally:
        # No raising handlers exist here. Repeated INT/TERM/QUIT cannot cut off
        # stop/reap, stream closure, proof validation or receipt fsync.
        if child is not None:
            if child.stdin is not None and not child.stdin.closed:
                child.stdin.close()
            if status is None or group_exists(child.pid):
                if not escalated:
                    send(child.pid, signal.SIGINT)
                    until = time.monotonic() + grace
                    while time.monotonic() < until and (status is None or group_exists(child.pid)):
                        if status is None:
                            try:
                                waited, value, resource = os.wait4(child.pid, os.WNOHANG)
                                if waited: status, usage = value, resource
                            except InterruptedError:
                                continue
                        time.sleep(.02)
                if status is None or group_exists(child.pid):
                    if not escalated:
                        escalated = True; send(child.pid, signal.SIGKILL)
                        kill_deadline = time.monotonic() + kill_join_timeout
                if status is None:
                    # SIGKILL is followed by the actual wait4, never poll-only.
                    until=kill_deadline
                    while status is None and time.monotonic()<until:
                        try:
                            waited,value,resource=os.wait4(child.pid,os.WNOHANG)
                            if waited:status,usage=value,resource
                        except InterruptedError:
                            continue
                        time.sleep(.02)
                child.returncode = os.waitstatus_to_exitcode(status) if status is not None else None
            else:
                child.returncode = os.waitstatus_to_exitcode(status) if status is not None else None
        for path in (output, errors):
            if path.exists():
                with path.open('rb') as stream:os.fsync(stream.fileno())
        closed = child is None or not group_exists(child.pid)
        joined = child is None or status is not None
        row = {'label': label, 'argv': argv, 'cwd': str(cwd), 'environment': environment,
            'started': started, 'completed': time.time(), 'elapsed_seconds': time.time() - started,
            'owned_pid': child.pid if child else None, 'owned_process_group': child.pid if child else None,
            'started_child': child is not None, 'joined': joined, 'child_joined': joined,
            'owned_process_group_closed': closed, 'wait_status': status,
            'exit_code': child.returncode if child else None, 'timed_out': timed_out,
            'exception': failure, 'signals': list(sticky.signals), 'cancel_sent': cancel_sent,
            'owner_escalated': escalated, 'stdin_error': input_error,
            'user_seconds': usage.ru_utime if usage else None,
            'system_seconds': usage.ru_stime if usage else None,
            'maxrss_native': usage.ru_maxrss if usage else None,
            'maxrss_native_unit': 'bytes' if sys.platform == 'darwin' else 'KiB',
            'stdout_sha256': sha(output), 'stderr_sha256': sha(errors)}
        proof = owner_proof is None
        if child is not None and owner_proof is not None:
            try:
                proof = owner_proof(row) is True
            except Exception as error:
                row['owner_proof_error'] = repr(error); proof = False
        if receipt_fields is not None:
            for key,value in receipt_fields.items():
                assert key not in row or row[key]==value, 'action/receipt field mismatch '+key
                row[key]=value
        row['descendant_release_proven'] = proof
        row['custody_released'] = joined and closed and (child is None or proof and (owner_proof is None or not escalated))
        row['outer_driver_completion_proven']=child is not None and status is not None and row['custody_released']
        row['custody'] = 'RELEASED' if row['custody_released'] else 'HELD'
        durable(receipt, row)
    return row
