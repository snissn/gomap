#!/usr/bin/env python3
"""Unchanged source-extracted hosted Attempt API; no old payload or main.

Origin revision07 SHA5c3189c13d3937ca547aa25e9d440248707d193828c2e9d68f89a20788ab5b10.
Selected declarations are AST-identical; caller must set started before __init__.
close() joins only the monitor; maintained outer Linux guard proves drain.
"""
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import stat
import subprocess
import threading
import time

GIB = 1024**3
ROOT_CAP = GIB
LOG_CAP = 32*1024**2

def need(condition, message):
    if not condition:
        raise RuntimeError(message)


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1 << 20), b''):
            h.update(block)
    return h.hexdigest()


def write(path, value):
    with Path(path).open('x', encoding='utf-8') as stream:
        json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write('\n')


def census_category(relative):
    parts = relative.parts
    if parts[0] == 'source':
        return 'git' if len(parts) > 1 and parts[1] == '.git' else 'source'
    if parts[0] in ('modcache', 'gocache', 'tmp'):
        return {'modcache': 'modules', 'gocache': 'go_cache', 'tmp': 'tmp'}[parts[0]]
    if str(relative) == 'nativewire.test.exe':
        return 'binary'
    return 'output' if parts[0] in ('output', 'guard') else 'rest'


def census(root, enforce=True):
    total = logs = 0
    categories = dict.fromkeys(('source', 'git', 'modules', 'go_cache', 'tmp', 'binary', 'output', 'rest'), 0)
    for directory, _, files in os.walk(root, followlinks=False):
        for name in files:
            path = Path(directory)/name
            try:
                s = path.lstat()
            except FileNotFoundError:
                continue  # Concurrent compiler temporary deletion.
            need(not stat.S_ISLNK(s.st_mode), 'unaccounted symlink: '+str(path))
            need(stat.S_ISREG(s.st_mode), 'nonregular task file: '+str(path))
            total += s.st_size
            relative = path.relative_to(root)
            categories[census_category(relative)] += s.st_size
            # Input files are counted in whole bytes, not generated logs.
            if relative.parts[0] in ('output', 'guard') or (len(relative.parts) == 1 and path.suffix in ('.log', '.json', '.jsonl', '.txt', '.trace')):
                logs += s.st_size
    free = shutil.disk_usage(root).free
    errors = []
    if total > ROOT_CAP:
        errors.append('whole root exceeds 1GiB')
    if logs > LOG_CAP:
        errors.append('aggregate log/trace/JSON exceeds 32MiB')
    if free < 10*GIB:
        errors.append('free floor below 10GiB')
    row = {'whole_bytes': total, 'log_trace_json_bytes': logs, 'categories': categories,
           'free_bytes': free, 'errors': errors, 'sampled_monotonic': time.monotonic(),
           'scope': 'single sampled walk; first observed breach is not peak or atomic snapshot'}
    if enforce:
        need(not errors, '; '.join(errors))
    return row


class Attempt:
    # A single fixed stage runner, not a reusable workload/controller API.
    def __init__(self, root):
        self.root = root
        self.out = root/'output'
        self.out.mkdir()
        self.rows = []
        self.child = None
        self.stop = threading.Event()
        self.failure = None
        self.first_failure = None
        self.stage = 'setup'
        self.state_lock = threading.Lock()
        self.maximum = {'whole_bytes': 0, 'log_trace_json_bytes': 0}
        self.thread = threading.Thread(target=self.watch, daemon=True)
        self.thread.start()

    def fail(self, message, row=None):
        # First failure is immutable; later cleanup/emit exceptions cannot replace it.
        try:
            with self.state_lock:
                if self.failure is None:
                    self.failure = message
                    self.first_failure = {'error': message, 'stage': self.stage, 'census': row,
                                          'first_observed_not_peak': True}
                    write(self.out/'resource-first-failure.json', self.first_failure)
        finally:
            self.stop.set()  # Even receipt I/O failure must stop the original attempt.

    def sample(self):
        row = census(self.root, enforce=False)
        row['stage'] = self.stage
        with self.state_lock:
            for k in self.maximum:
                self.maximum[k] = max(self.maximum[k], row[k])
        if row['errors']:
            self.fail('; '.join(row['errors']), row)  # Persist before raising.
            raise RuntimeError(self.failure)
        return row

    def watch(self):
        try:
            while not self.stop.is_set():
                self.sample()
                if time.monotonic()-self.started > 600:
                    raise RuntimeError('600s payload deadline')
                self.stop.wait(.2)
        except BaseException as error:
            self.fail(repr(error))

    def pulse(self):
        # Cheap within already precharged direct writes; no tree walk per chunk/member.
        need(self.failure is None, 'resource failure during materialization: '+str(self.failure))
        remaining = 600-(time.monotonic()-self.started)
        need(remaining > 0, '600s whole deadline during materialization')
        return remaining

    def check(self, additional_bytes=0):
        remaining = self.pulse()
        row = self.sample()
        if row['whole_bytes']+additional_bytes > ROOT_CAP:
            self.fail('prospective write exceeds whole root', dict(row, prospective_additional_bytes=additional_bytes))
            raise RuntimeError(self.failure)
        return remaining

    def run(self, label, argv, seconds, cwd=None, env=None):
        self.stage = label
        self.check()
        before = self.sample()
        write(self.out/(label+'.before-census.json'), before)
        if os.name == 'nt' and argv[0] == 'go':
            # Windows current-directory executable search must not defeat SDK identity.
            argv = [r'C:\hostedtoolcache\windows\go\1.26.10\x64\bin\go.exe', *argv[1:]]
        started = time.monotonic()
        row = {'label': label, 'argv': argv, 'timeout_seconds': seconds, 'started_epoch': time.time()}
        self.rows.append(row)
        with (self.out/(label+'.stdout.log')).open('xb') as stdout, (self.out/(label+'.stderr.log')).open('xb') as stderr:
            self.child = subprocess.Popen(argv, cwd=cwd, env=env, stdout=stdout, stderr=stderr,
                                          start_new_session=os.name != 'nt')
            row['owned_pid'] = self.child.pid
            try:
                while self.child.poll() is None:
                    if self.failure or time.monotonic()-started > seconds:
                        row['intervention'] = self.failure or 'stage deadline'
                        if os.name == 'nt':
                            # Exact still-live owned Popen handle, not a discovered PID.
                            killed = subprocess.run(['taskkill', '/PID', str(self.child.pid), '/T', '/F'],
                                           stdout=stderr, stderr=stderr, timeout=10, check=False)
                            row['taskkill_exit'] = killed.returncode
                            row['cleanup_status'] = 'HOLD_DESCENDANT_DRAIN_UNPROVED'
                            self.fail('Windows intervention: descendant drain unproved')
                            # Receipt below preserves nonzero taskkill; never claim tree drain.
                        else:
                            os.killpg(self.child.pid, signal.SIGTERM)
                            try:
                                self.child.wait(timeout=5)
                            except subprocess.TimeoutExpired:
                                os.killpg(self.child.pid, signal.SIGKILL)
                        break
                    time.sleep(.1)
                row['exit_code'] = self.child.wait(timeout=10)
            finally:
                joined = self.child.poll() is not None
                row['elapsed_seconds'] = time.monotonic()-started
                row['ended_epoch'] = time.time()
                row['original_handle_joined'] = joined
                row.setdefault('cleanup_status', 'ORIGINAL_HANDLE_JOINED_NORMAL_RETURN' if joined else 'HOLD_ORIGINAL_HANDLE_NOT_JOINED')
                if joined:
                    self.child = None
                write(self.out/(label+'.receipt.json'), row)
                after = census(self.root, enforce=False)
                after['stage'] = label
                write(self.out/(label+'.after-census.json'), after)
                if after['errors']:
                    self.fail('; '.join(after['errors']), after)
        need(row['original_handle_joined'], 'owned process not joined; cleanup HOLD')
        need(row.get('taskkill_exit', 0) == 0, 'taskkill failed; cleanup HOLD')
        need('intervention' not in row, 'stage intervention; diagnostic HOLD')
        return row['exit_code']

    def close(self):
        self.stop.set()
        self.thread.join()
        write(self.out/'resource-summary.json', {'maximum': self.maximum, 'error': self.failure, 'first_failure': self.first_failure,
              'scope': 'sampled complete root after workflow bootstrap; Windows Go memory target is soft; no hard Windows RSS guarantee'})
