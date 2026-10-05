"""Optional Linux RSS observation for a caller-validated native command.

No CLI or provenance authority: callers attest the exact executable themselves.
Samples cover only this Popen owner, or its direct /usr/bin/time -v child whose
executable inode/device and parent identity match. Sample maxima are not HWM.
The observer runs in the waiting caller; it creates no sampler thread/process.
"""
import json
import os
import pathlib
import subprocess
import time


def _identity(pid):
    raw = pathlib.Path(f'/proc/{pid}/stat').read_text()
    fields = raw[raw.rfind(')')+2:].split()
    return int(fields[1]), int(fields[19])  # PPID, starttime (protect PID reuse).


def _sample(pid, owner, expected_identity, started):
    before = time.time_ns()
    parent, start = _identity(pid)
    if parent != owner or (started is not None and start != started):
        raise ValueError('owned process identity changed')
    executable = os.stat(f'/proc/{pid}/exe')
    if (executable.st_dev, executable.st_ino) != expected_identity:
        raise ValueError('owned process executable does not match expected binary')
    fields = {}
    for line in pathlib.Path(f'/proc/{pid}/status').read_text().splitlines():
        name, _, value = line.partition(':')
        if name in ('VmRSS', 'RssAnon', 'RssFile', 'RssShmem'):
            amount, unit = value.split()
            if unit != 'kB':
                raise ValueError('unexpected RSS unit')
            fields[name] = int(amount)*1024
    if set(fields) != {'VmRSS', 'RssAnon', 'RssFile', 'RssShmem'}:
        stat = pathlib.Path(f'/proc/{pid}/stat').read_text()
        if stat[stat.rfind(')')+2:].split()[0] == 'Z':
            raise ProcessLookupError('owned process exited during RSS sample')
        raise ValueError('incomplete RSS fields')
    if _identity(pid) != (parent, start):
        raise ValueError('owned process changed during RSS sample')
    return dict(pid=pid, process_start_ticks=start, started_unix_nano=before,
                finished_unix_nano=time.time_ns(), rss_bytes=fields['VmRSS'],
                anonymous_bytes=fields['RssAnon'], file_bytes=fields['RssFile'],
                shared_memory_bytes=fields['RssShmem']), start


def run_with_rss(command, expected_executable, sample_path, interval_ms=100, cancel_event=None, **kwargs):
    """Run an attested binary directly or under /usr/bin/time -v; always reap it.

    Return (CompletedProcess, sampling summary). Missing samples, observation
    errors or cancellation set complete=false. JSONL and summary preserve gaps.
    stdout/stderr should be caller-owned files. shell execution is rejected.
    """
    if type(interval_ms) is not int or not 1 <= interval_ms <= 60000:
        raise ValueError('RSS interval must be integer 1..60000 ms')
    if not isinstance(command, (list, tuple)) or not command or not all(isinstance(arg,str) for arg in command):
        raise ValueError('RSS sampling requires a nonempty native argv list')
    if kwargs.get('shell'):
        raise ValueError('RSS sampling requires a direct native command')
    expected = pathlib.Path(expected_executable).resolve(strict=True)
    with expected.open('rb') as stream:
        if stream.read(4) != b'\x7fELF':
            raise ValueError('RSS sampling requires an ELF executable')
    timed = list(command[:2]) == ['/usr/bin/time', '-v']
    target = command[2] if timed and len(command) > 2 else command[0]
    if pathlib.Path(target).resolve(strict=True) != expected:
        raise ValueError('command does not launch expected executable')
    identity = expected.stat()
    expected_identity = identity.st_dev, identity.st_ino
    summary = dict(interval_ms=interval_ms, samples=0, errors=[], complete=False,
                   cancelled=False, sample_attempts=0, terminal_exit_races=0, started_unix_nano=time.time_ns(),
                   expected_executable=str(expected), scope='owned native process only; sample maxima are not process HWM')
    process, target_pid, start_ticks = None, None, None
    with pathlib.Path(sample_path).open('x') as stream:
        try:
            kwargs["start_new_session"] = True
            process = subprocess.Popen(command, **kwargs)
            summary['launcher_pid'] = process.pid
            while process.poll() is None:
                if cancel_event is not None and cancel_event.is_set():
                    summary['cancelled'] = True
                    break
                try:
                    if timed and target_pid is None:
                        children = pathlib.Path(f'/proc/{process.pid}/task/{process.pid}/children').read_text().split()
                        if len(children) > 1:
                            raise ValueError('time launcher has multiple children')
                        if children:
                            target_pid = int(children[0])
                            _, start_ticks = _identity(target_pid)
                    elif not timed:
                        target_pid = process.pid
                    if target_pid is not None:
                        summary['sample_attempts'] += 1
                        # time may expose its child just before exec. Wait for the
                        # attested executable; never record wrapper/other bytes.
                        exe = os.stat(f'/proc/{target_pid}/exe')
                        if timed and (exe.st_dev, exe.st_ino) != expected_identity:
                            try:
                                process.wait(timeout=interval_ms/1000)
                            except subprocess.TimeoutExpired:
                                pass
                            continue
                        row, start_ticks = _sample(target_pid, process.pid if timed else os.getpid(), expected_identity, start_ticks)
                        stream.write(json.dumps(row)+'\n')
                        stream.flush()
                        summary['samples'] += 1
                        summary['observed_pid'] = target_pid
                        summary.setdefault('first_sample_unix_nano', row['started_unix_nano'])
                        summary['last_sample_unix_nano'] = row['finished_unix_nano']
                except (FileNotFoundError, ProcessLookupError):
                    # Exit between liveness, executable and status reads is a
                    # normal terminal race, not a sample of another process.
                    summary['terminal_exit_races'] += 1
                except (OSError, ValueError) as error:
                    summary['errors'].append(dict(unix_nano=time.time_ns(), error=str(error)))
                    break
                try:
                    process.wait(timeout=interval_ms/1000)
                except subprocess.TimeoutExpired:
                    pass
        except BaseException as error:
            summary['errors'].append(dict(unix_nano=time.time_ns(), error=str(error)))
            raise
        finally:
            if process is not None and process.poll() is None:
                # The new process group contains only this owned launch. Stop
                # wrapper and child together, including cancellation before the
                # child was observed; never leave an unobserved native orphan.
                try:
                    os.killpg(process.pid, 15)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(process.pid, 9)
                    except ProcessLookupError:
                        pass
                    process.wait()
            summary['finished_unix_nano'] = time.time_ns()
            summary['complete'] = bool(summary['samples']) and not summary['errors'] and not summary['cancelled']
            pathlib.Path(str(sample_path)+'.summary.json').write_text(json.dumps(summary, indent=2)+'\n')
    return subprocess.CompletedProcess(command, process.returncode), summary
