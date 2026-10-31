#!/usr/bin/env python3
"""Source prototype: one guarded child; no performance/correctness acceptance."""
import argparse
import ctypes
import errno
import importlib
import math
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import time

GiB = 1024 ** 3


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def stat(pid):
    fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
    return dict(pid=pid, start_ticks=int(fields[19]), ppid=int(fields[1]),
                pgrp=int(fields[2]), session=int(fields[3]), state=fields[0],
                rss_bytes=int(fields[21]) * os.sysconf('SC_PAGE_SIZE'))


def gone(error):
    return isinstance(error, OSError) and error.errno in (errno.ENOENT, errno.ESRCH)


def exited(fd):
    return bool(select.select([fd], [], [], 0)[0])


def direct_children():
    return {int(pid) for pid in Path(f'/proc/self/task/{os.getpid()}/children').read_text().split()}


class Owned:
    """Pins identity; a reusable PID or PGID is never a signaling authority."""
    def __init__(self):
        self.rows = {}
        self.retired = []
        self.guard_pid = os.getpid()

    def pin(self, pid, parent=None):
        fd = os.pidfd_open(pid)
        try:
            row = stat(pid)
            if parent is not None:
                require(row['ppid'] == parent, 'parent changed before ownership pin')
                # Reject recycled parent IDs and defer orphan discovery to adoption.
                if parent != self.guard_pid:
                    require(not exited(self.rows[parent]['fd']), 'parent exited during pin')
            final = stat(pid)
            require(final['start_ticks'] == row['start_ticks'] and final['ppid'] == row['ppid'], 'start token/parent changed')
            # A descendant can be reaped/replaced by its original parent between
            # pidfd_open(A) and proc(B). Bind the final proc snapshot to a still
            # live fd(A); reject/close and rediscover B instead of installing it.
            # Direct/adopted children are ours and remain unreaped, including
            # quick-exit zombies, so their PID cannot recycle during this pin.
            if parent != self.guard_pid:
                require(not exited(fd), 'child pidfd exited during identity pin')
            self.rows[pid] = dict(row, fd=fd)
        except BaseException:
            os.close(fd)
            raise

    def sample(self):
        # Discover even children that call setsid; subreaper adoption closes the
        # missed-intermediate-parent window. Short-lived RSS peaks remain sampled.
        candidates = {}
        for entry in Path('/proc').iterdir():
            if entry.name.isdecimal():
                try:
                    candidates[int(entry.name)] = stat(int(entry.name))
                except OSError as error:
                    if not gone(error):
                        raise
        for _ in range(len(candidates) + 1):
            added = False
            for pid, row in candidates.items():
                old = self.rows.get(pid)
                if old and row['start_ticks'] != old['start_ticks']:
                    require(exited(old['fd']), 'live pidfd start token changed')
                    self.retired.append({k: v for k, v in old.items() if k != 'fd'})
                    os.close(old['fd'])
                    del self.rows[pid]
                if pid in self.rows or pid == self.guard_pid:
                    continue
                parent = row['ppid']
                if parent != self.guard_pid and parent not in self.rows:
                    continue
                if parent != self.guard_pid and exited(self.rows[parent]['fd']):
                    continue
                try:
                    self.pin(pid, parent)
                    added = True
                except OSError as error:
                    if not gone(error):
                        raise
                except RuntimeError:
                    # A relation changed during pin: scan again/adopt, never signal
                    # the ambiguous process based on a stale parent snapshot.
                    continue
            if not added:
                break
        rss, live = 0, []
        for pid, pinned in self.rows.items():
            if exited(pinned['fd']):
                continue
            try:
                row = stat(pid)
            except OSError as error:
                if gone(error) and exited(pinned['fd']):
                    continue
                raise
            require(row['start_ticks'] == pinned['start_ticks'], 'owned PID identity mismatch')
            rss += row['rss_bytes']
            live.append({k: row[k] for k in ('pid', 'start_ticks', 'pgrp', 'session', 'rss_bytes')})
        return rss, live

    def send(self, pid, signum):
        record = self.rows[pid]
        if exited(record['fd']):
            return False
        # pidfd selects the exact task even after exec or reparenting; starttime
        # is retained for independent receipt inspection, not a numeric kill.
        try:
            signal.pidfd_send_signal(record['fd'], signum)
            return True
        except ProcessLookupError:
            require(exited(record['fd']), 'pidfd signal vanished without exit')
            return False

    def reap_adopted(self, root_pid):
        for pid, row in self.rows.items():
            if pid == root_pid or not exited(row['fd']):
                continue
            try:
                os.waitid(os.P_PIDFD, row['fd'], os.WEXITED | os.WNOHANG)
            except ChildProcessError:
                pass  # Original parent reaped it; pidfd still identifies that task.

    def close(self):
        for row in self.rows.values():
            os.close(row['fd'])


def subreaper():
    libc = ctypes.CDLL(None, use_errno=True)
    previous = ctypes.c_int()
    require(libc.prctl(37, ctypes.byref(previous), 0, 0, 0) == 0, 'GET_CHILD_SUBREAPER failed')
    require(previous.value == 0, 'dedicated supervisor must not already be subreaper')
    require(not Path(f'/proc/self/task/{os.getpid()}/children').read_text().strip(), 'supervisor already has children')
    require(libc.prctl(36, 1, 0, 0, 0) == 0, 'SET_CHILD_SUBREAPER failed')


def check_pidfd_waitid():
    # Self is not our child: ECHILD proves this kernel recognizes P_PIDFD.
    # EINVAL/ENOSYS or absent Python support fail before launching any child.
    fd = os.pidfd_open(os.getpid())
    try:
        try:
            os.waitid(os.P_PIDFD, fd, os.WEXITED | os.WNOHANG)
        except ChildProcessError:
            return
        raise RuntimeError('unexpected self waitid result')
    finally:
        os.close(fd)


def run(args, helper):
    out = args.out.resolve(strict=False)
    out.mkdir(mode=0o700)  # Exclusive namespace; never overwrite a failed run.
    child, owned, root_fd = None, Owned(), None
    result = dict(schema_version=1, status='failed', scope='sampled outer resource/ownership guard only; pending independent campaign review',
                  command=args.command, cwd=str(args.cwd), limits=dict(rss_bytes=args.rss_cap_bytes, free_bytes=args.free_floor_bytes,
                  deadline_seconds=args.deadline, sample_seconds=args.interval, cleanup_seconds=args.cleanup_seconds),
                  start_utc=helper.now(), max_rss_bytes=0, min_free_bytes=None, cleanup_signals=[], errors=[],
                  whole_interval_verified=False, unperturbed_timing_verified=False)
    helper.write_new(out/'execution-start.json', result)
    samples = (out/'samples.jsonl').open('x', buffering=1)
    started = time.monotonic()

    def sample(enforce=True):
        fs = os.statvfs(args.filesystem)
        free = fs.f_bavail * fs.f_frsize  # Account-available, not privileged reserve.
        rss, live = owned.sample()
        result['max_rss_bytes'] = max(rss, result['max_rss_bytes'])
        result['min_free_bytes'] = free if result['min_free_bytes'] is None else min(free, result['min_free_bytes'])
        import json
        samples.write(json.dumps(dict(elapsed_seconds=time.monotonic()-started, free_bytes=free, rss_bytes=rss, live=live), allow_nan=False)+'\n')
        if enforce:
            require(free >= args.free_floor_bytes, 'account-available free floor crossed')
            require(rss <= args.rss_cap_bytes, 'owned descendant RSS cap crossed')
            require(time.monotonic()-started < args.deadline, 'outer campaign deadline crossed')
        return live

    def cleanup():
        def unhandled_children():
            root = {child.pid} if child is not None and child.returncode is None else set()
            return direct_children() - root

        def cleanup_live():
            try:
                return owned.sample()[1]
            except BaseException as error:
                # Resource/log I/O failure must not prevent known-owned cleanup.
                result['errors'].append(f'cleanup discovery: {error}')
                return [row for row in owned.rows.values() if not exited(row['fd'])]
        # First give the original shell trap time for its checked Close/reopen
        # cleanup. Forced intervention is always a failed campaign, never green.
        if child is not None and root_fd is not None and not exited(root_fd):
            if owned.send(child.pid, signal.SIGTERM):
                result['cleanup_signals'].append(dict(pid=child.pid, start_ticks=owned.rows[child.pid]['start_ticks'], signal='TERM', role='original-shell'))
        deadline = time.monotonic() + args.cleanup_seconds
        while time.monotonic() < deadline:
            live = cleanup_live()
            owned.reap_adopted(child.pid if child else None)
            if not live:
                if not unhandled_children():
                    return
            time.sleep(min(args.interval, max(0, deadline-time.monotonic())))
        # Discover/pin new orphans between passes. Signal every owned task via its
        # retained pidfd, including separate groups/sessions; never call killpg.
        for signum in (signal.SIGTERM, signal.SIGKILL):
            deadline = time.monotonic() + args.cleanup_seconds
            while time.monotonic() < deadline:
                live = cleanup_live()
                if not live:
                    owned.reap_adopted(child.pid if child else None)
                    if not unhandled_children():
                        return
                for row in live:
                    if owned.send(row['pid'], signum):
                        result['cleanup_signals'].append(dict(pid=row['pid'], start_ticks=row['start_ticks'], signal=signum.name))
                owned.reap_adopted(child.pid if child else None)
                time.sleep(min(args.interval, max(0, deadline-time.monotonic())))
        require(not cleanup_live(), 'owned descendants survived bounded cleanup')
        owned.reap_adopted(child.pid if child else None)
        require(not unhandled_children(), 'unreaped/unobserved adopted children remain')

    def interrupted(signum, _):
        raise InterruptedError(f'supervisor interrupted by signal {signum}')

    for signum in helper.CANCELLATION_SIGNALS:
        signal.signal(signum, interrupted)
    try:
        require('GOMAXPROCS' not in os.environ, 'GOMAXPROCS must be absent')
        require(os.stat(out).st_dev == os.stat(args.filesystem).st_dev, 'guard output outside guarded filesystem')
        if args.validation_json:
            require(not args.validation_json.exists(), 'validation input already exists')
        check_pidfd_waitid()
        subreaper()
        sample()
        with (out/'stdout.log').open('x') as stdout, (out/'stderr.log').open('x') as stderr:
            with helper.defer_startup_cancellation() as pending:
                child = subprocess.Popen(args.command, cwd=args.cwd, stdout=stdout, stderr=stderr, start_new_session=True)
                # Direct unreaped child cannot recycle before its first pin.
                owned.pin(child.pid, os.getpid())
                root_fd = owned.rows[child.pid]['fd']
                result['root_identity'] = {k: v for k, v in owned.rows[child.pid].items() if k != 'fd'}
                helper.write_new(out/'root.identity.json', dict(schema_version=1, observed_utc=helper.now(),
                                 process=result['root_identity'], scope='direct unreaped Popen child, pidfd pinned before any wait/poll; executable bound externally'))
            if pending:
                raise InterruptedError(f'cancelled during child startup: {pending}')
            while True:
                live = sample()
                if exited(root_fd):
                    # Root is deliberately unreaped here. One more discovery
                    # includes children adopted after root exit; leaks veto green.
                    live = sample()
                    require(not live, 'child exited with live owned descendants')
                    owned.reap_adopted(child.pid)
                    require(direct_children() == {child.pid}, 'unobserved/unreaped adopted child at root exit')
                    result['exit_code'] = child.wait(timeout=args.cleanup_seconds)
                    require(result['exit_code'] == 0, 'wrapped command exited nonzero')
                    require(not direct_children(), 'adopted child after root wait')
                    break
                select.select([root_fd], [], [], args.interval)
        if args.validation_json:
            accepted = helper.read(args.validation_json)
            require(accepted.get('status') == 'validated-consumer-evidence' and accepted.get('errors') == [], 'maintained campaign validation failed/missing')
            result['validation_sha256'] = helper.digest(args.validation_json)
        owned.reap_adopted(child.pid)
        result['status'] = 'guard-completed-pending-independent-artifact-review'
    except BaseException as error:
        result['errors'].append(f'{type(error).__name__}: {error}')
        mask = signal.pthread_sigmask(signal.SIG_BLOCK, helper.CANCELLATION_SIGNALS)
        try:
            if child is not None and root_fd is None:
                # Maintained fallback only for the still-unreaped direct child.
                result['exit_code'] = helper.terminate_unpinned_child(child, args.cleanup_seconds)
            cleanup()
            if child is not None and child.returncode is None:
                result['exit_code'] = child.wait(timeout=args.cleanup_seconds)
        except BaseException as cleanup_error:
            result['errors'].append(f'cleanup: {type(cleanup_error).__name__}: {cleanup_error}')
        finally:
            result['pending_cleanup_signals'] = sorted(signal.sigpending() & helper.CANCELLATION_SIGNALS)
            result['retired_processes'] = owned.retired
            # Receipt is retained before deferred cancellation can be delivered.
            result['end_utc'] = helper.now()
            helper.write_new(out/'result.json', result)
            samples.close()
            owned.close()
            signal.pthread_sigmask(signal.SIG_SETMASK, mask)
        return 1
    result['end_utc'] = helper.now()
    result['retired_processes'] = owned.retired
    helper.write_new(out/'result.json', result)
    samples.close()
    owned.close()
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scripts-dir', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--cwd', type=Path, required=True)
    parser.add_argument('--filesystem', type=Path, required=True)
    parser.add_argument('--validation-json', type=Path)
    parser.add_argument('--deadline', type=float, default=2400)
    parser.add_argument('--cleanup-seconds', type=float, default=10)
    parser.add_argument('--interval', type=float, default=.2)
    parser.add_argument('--rss-cap-bytes', type=int, default=24*GiB)
    parser.add_argument('--free-floor-bytes', type=int, default=10*GiB)
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    args.command = args.command[1:] if args.command[:1] == ['--'] else args.command
    require(bool(args.command), 'missing wrapped argv')
    require(all(path.is_absolute() for path in (args.scripts_dir, args.out, args.cwd, args.filesystem)), 'absolute paths required')
    require(args.validation_json is None or args.validation_json.is_absolute(), 'absolute validation path required')
    require(all(math.isfinite(x) and x > 0 for x in (args.deadline, args.cleanup_seconds, args.interval)), 'finite positive durations required')
    require(args.interval <= .2 and args.cleanup_seconds <= 60 and args.deadline <= 5400, 'duration bounds exceeded')
    require(args.rss_cap_bytes > 0 and args.free_floor_bytes > 0, 'positive resource bounds required')
    require(hasattr(os, 'pidfd_open') and hasattr(signal, 'pidfd_send_signal') and hasattr(os, 'waitid') and hasattr(os, 'P_PIDFD'), 'Linux pidfd and P_PIDFD waitid support required')
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(args.scripts_dir))
    helper = importlib.import_module('ycsb_compare_validate')
    with helper.outer_deadline(args.deadline):
        return run(args, helper)


if __name__ == '__main__':
    sys.exit(main())
