#!/usr/bin/env python3
"""Opt-in public COW JSON load regression; see cow_json_load_regression.md."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time

CLIENT_REV = 'c195a9bc777b6aeb1e2f5550db96d872338f2f40'
CLIENT_ORIGINAL_SHA = '051f211d72ca0ba194ba1fcf4d50fe6bac9e6964a6e83ab933e0ac071e1a44d2'
CLIENT_OVERLAY_SHA = 'b09334ad0f53ea49ca553953b123a2617625c04a766dbed624e4058c2399f787'
PROFILES = ('command_wal_durable', 'command_wal_relaxed', 'no_wal_fast')
ROOT = Path(__file__).resolve().parent.parent


class RunnerInterrupted(BaseException):
    pass


class CleanupFailure(BaseException):
    pass


def interrupt(signum, _frame):
    raise RunnerInterrupted(f'signal {signum}')


def sha(data):
    return hashlib.sha256(data).hexdigest()


def git(repo, *args):
    return subprocess.check_output(['git', '-C', str(repo), *args]).decode().strip()


def require_checkout(repo, revision):
    head = git(repo, 'rev-parse', 'HEAD')
    if head != revision or git(repo, 'status', '--porcelain', '--untracked-files=all'):
        raise RuntimeError(f'checkout {repo} must be clean at {revision}; head={head}')


def client_overlay(source):
    if sha(source) != CLIENT_ORIGINAL_SHA:
        raise ValueError('unexpected pinned client file')
    original = b'wireProtocolMinorV0'
    if source.count(original) != 4:
        raise ValueError('unexpected protocol selector count')
    changed = source.replace(original, b'wireProtocolMinorV1')
    changed = changed.replace(b'wireProtocolMinorV1  = uint16(0)', b'wireProtocolMinorV1  = uint16(1)')
    if sha(changed) != CLIENT_OVERLAY_SHA:
        raise ValueError('unexpected client overlay')
    return changed


def load_counts(output):
    # Ignore progress reports; only the final summary after Run finished counts.
    if output.count('Run finished, takes ') != 1:
        raise ValueError('missing or duplicate terminal summary')
    final = output.split('Run finished, takes ', 1)[1]
    counts = {}
    for line in final.splitlines():
        if not re.match(r'^[A-Z_]+\s+- Takes\(s\):', line):
            continue
        match = re.fullmatch(r'([A-Z_]+)\s+- Takes\(s\): [^\n]*?Count: ([0-9]+),.*', line)
        if not match:
            raise ValueError('malformed final operation summary: ' + line)
        kind, count = match.groups()
        if kind in counts:
            raise ValueError('duplicate final operation summary: ' + kind)
        counts[kind] = int(count)
    if counts.get('INSERT') != 100000 or counts.get('TOTAL') != 100000 or counts.get('INSERT_ERROR', 0) != 0:
        raise ValueError('incorrect load totals: ' + repr(counts))
    if set(counts) - {'INSERT', 'INSERT_ERROR', 'TOTAL'}:
        raise ValueError('unexpected operation totals: ' + repr(counts))
    return counts


def write_json(path, value):
    with path.open('x', encoding='utf-8') as f:
        json.dump(value, f, indent=2, sort_keys=True)
        f.write('\n')


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def env_without_override():
    env = os.environ.copy()
    env.pop('GOMAXPROCS', None)
    env.pop('GOROOT', None)
    env['GOWORK'] = 'off'
    return env


def observe_child(proc):
    # Linux-only: assert the environment of the actual process, not its parent.
    raw = Path(f'/proc/{proc.pid}/environ').read_bytes().split(b'\0')
    if any(x.startswith(b'GOMAXPROCS=') for x in raw):
        raise RuntimeError('actual server child has a GOMAXPROCS override')
    return {'pid': proc.pid, 'exe': os.readlink(f'/proc/{proc.pid}/exe'),
            'stat': Path(f'/proc/{proc.pid}/stat').read_text(), 'gomaxprocs_present': False,
            'affinity': sorted(os.sched_getaffinity(proc.pid))}


def run_observed(argv, *, env, stdout, stderr, timeout):
    proc = subprocess.Popen(argv, env=env, stdout=stdout, stderr=stderr, start_new_session=True)
    try:
        observation = observe_child(proc)
        if os.path.realpath(observation['exe']) != str(Path(argv[0]).resolve()):
            raise RuntimeError('unexpected child executable')
        code = proc.wait(timeout=timeout)
        return code, observation
    except BaseException:
        if proc.poll() is None:
            proc.kill()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired as exc:
            raise CleanupFailure(f'child {proc.pid} did not exit after kill') from exc
        raise


def run_profile(out, profile, server_bin, client_bin, verifier_bin, env):
    run = out / profile
    run.mkdir(mode=0o700)
    db = run / 'db'
    db.mkdir()
    tmp = run / 'tmp'
    tmp.mkdir()
    child_env = dict(env, TMPDIR=str(tmp), TREEDB_YCSB_LOG_ERRORS='1')
    receipt = {'profile': profile, 'started_utc': now(), 'errors': [], 'db': str(db)}
    server = None
    interrupted = None
    try:
        server_argv = [str(server_bin), '-dir', str(db), '-profile', profile, '-addr', '127.0.0.1:0']
        receipt['server_argv'] = server_argv
        with (run / 'server.out').open('xb') as stdout, (run / 'server.err').open('xb') as stderr:
            server = subprocess.Popen(server_argv, env=child_env, stdout=stdout, stderr=stderr, start_new_session=True)
            receipt['server_observation'] = observe_child(server)
            if os.path.realpath(receipt['server_observation']['exe']) != str(server_bin.resolve()):
                raise RuntimeError('unexpected server executable')
            address = None
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if server.poll() is not None:
                    raise RuntimeError('server exited during startup')
                text = (run / 'server.out').read_text(errors='replace')
                match = re.search(r'^TreeDB Native Server listening on (127\.0\.0\.1:\d+)$', text, re.M)
                if match and 'TreeDB storage: shared_cow' in text:
                    address = match.group(1)
                    break
                time.sleep(.1)
            if not address:
                raise RuntimeError('no COW readiness within 30 seconds')
            receipt['actual_storage'] = 'shared_cow'
            args = [str(client_bin), 'load', 'treedb-native', '-p', f'treedb.addr={address}',
                    '-p', 'treedb.document_format=json', '-p', 'treedb.ack=visible',
                    '-p', 'treedb.flush_on_close=true', '-p', 'recordcount=100000',
                    '-p', 'operationcount=10000', '-p', 'threadcount=16', '-p', 'fieldcount=10',
                    '-p', 'fieldlength=100', '-p', 'batchsize=1', '-p', 'table=usertable', '-p', 'silence=false']
            receipt['client_argv'] = args
            with (run / 'load.out').open('xb') as load, (run / 'load.err').open('xb') as err:
                code, obs = run_observed(args, env=child_env, stdout=load, stderr=err, timeout=240)
            receipt['client_exit'], receipt['client_observation'] = code, obs
            if code:
                raise RuntimeError(f'client exit {code}')
            load_output = (run / 'load.out').read_text(errors='replace')
            receipt['counts'] = load_counts(load_output)
            if 'operation err:' in load_output or 'operation err:' in (run / 'load.err').read_text(errors='replace'):
                raise RuntimeError('client reported an operation error')
    except (RunnerInterrupted, CleanupFailure) as exc:
        interrupted = exc
        receipt['errors'].append(f'RunnerInterrupted: {exc}')
    except Exception as exc:
        receipt['errors'].append(f'{type(exc).__name__}: {exc}')
    finally:
        if server is not None:
            if server.poll() is None:
                try:
                    server.send_signal(signal.SIGTERM)
                except ProcessLookupError:
                    pass  # It exited between poll and signal; wait below records its status.
            try:
                receipt['server_exit'] = server.wait(timeout=15)
            except subprocess.TimeoutExpired:
                server.kill()
                receipt['errors'].append('server failed to close within 15 seconds')
                try:
                    receipt['server_exit'] = server.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    interrupted = CleanupFailure(f'server {server.pid} did not exit after kill')
                    receipt['errors'].append(str(interrupted))
            if receipt.get('server_exit') not in (None, 0):
                receipt['errors'].append(f"checked server/owner close exit {receipt['server_exit']}")
    # This separate process opens the same DB, scans all keys and fields, explicitly
    # checkpoints, and propagates client/server/manager/owner Close errors.
    if interrupted is None and server is not None and receipt.get('server_exit') == 0:
        verify_argv = [str(verifier_bin), '-dir', str(db), '-profile', profile]
        receipt['verifier_argv'] = verify_argv
        try:
            with (run / 'readback.json').open('xb') as stdout, (run / 'readback.err').open('xb') as stderr:
                code, obs = run_observed(verify_argv, env=child_env, stdout=stdout, stderr=stderr, timeout=180)
            receipt['verifier_exit'], receipt['verifier_observation'] = code, obs
            if code:
                raise RuntimeError(f'verifier exit {code}')
            data = json.loads((run / 'readback.json').read_text())
            if (data.get('rows') != 100000 or data.get('profile') != profile or
                    data.get('actual_mode') != 'cow_btree' or data.get('native') is not True or
                    data.get('expected_hashed_keys_verified') is not True or
                    data.get('verification_max_scan_documents') != 100001 or
                    data.get('verification_max_cursor_retained_bytes') != 256 << 20):
                raise RuntimeError('invalid reopen verifier result')
        except (RunnerInterrupted, CleanupFailure) as exc:
            interrupted = exc
            receipt['errors'].append(f'RunnerInterrupted: {exc}')
        except Exception as exc:
            receipt['errors'].append(f'{type(exc).__name__}: {exc}')
    elif interrupted is None:
        receipt['errors'].append('reopen verifier skipped after server/owner close failure')
    receipt['finished_utc'] = now()
    receipt['passed'] = not receipt['errors']
    write_json(run / 'terminal.json', receipt)
    if interrupted is not None:
        raise interrupted
    return receipt['passed']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--client-repo', type=Path, required=True)
    parser.add_argument('--gomap-repo', type=Path, default=ROOT, help='clean checkout to build, including an earlier revision')
    parser.add_argument('--gomap-revision', required=True, help='exact clean Gomap commit to test')
    parser.add_argument('--out', type=Path, required=True, help='new output directory; never reused')
    args = parser.parse_args()
    if sys.platform != 'linux':
        parser.error('actual child environment observation requires Linux /proc')
    client_repo = args.client_repo.resolve()
    gomap_repo = args.gomap_repo.resolve()
    require_checkout(gomap_repo, args.gomap_revision)
    require_checkout(client_repo, CLIENT_REV)
    out = args.out.resolve()
    out.mkdir(mode=0o700, parents=False, exist_ok=False)
    env = env_without_override()
    env['TMPDIR'] = str(out)
    overlay_source = subprocess.check_output(['git', '-C', str(client_repo), 'show', CLIENT_REV + ':db/treedbnative/nativewire.go'])
    overlay = out / 'nativewire-v1.go'
    overlay.write_bytes(client_overlay(overlay_source))
    overlay_json = out / 'overlay.json'
    write_json(overlay_json, {'Replace': {str(client_repo / 'db/treedbnative/nativewire.go'): str(overlay)}})
    server, client, verifier = (out / 'native-server', out / 'go-ycsb', out / 'readback')
    verifier_source = ROOT / 'scripts/cow_json_load_readback/main.go'
    builds = [([ 'go', 'build', '-o', str(server), './cmd/treedb-native-server'], gomap_repo),
              ([ 'go', 'build', '-o', str(verifier), str(verifier_source)], gomap_repo),
              ([ 'go', 'build', '-tags=libsqlite3', '-mod=readonly', '-overlay', str(overlay_json), '-o', str(client), './cmd/go-ycsb'], client_repo)]
    manifest = {'gomap_revision': args.gomap_revision, 'gomap_repo': str(gomap_repo), 'client_revision': CLIENT_REV,
                'client_original_sha256': CLIENT_ORIGINAL_SHA, 'client_overlay_sha256': CLIENT_OVERLAY_SHA,
                'runner_sha256': sha(Path(__file__).read_bytes()), 'verifier_source_sha256': sha(verifier_source.read_bytes()),
                'builds': [{'argv': a, 'cwd': str(cwd), 'go_version': subprocess.check_output(
                    ['go', 'version'], cwd=cwd, env=env, text=True).strip()} for a, cwd in builds],
                'created_utc': now(), 'profiles': PROFILES, 'historical_client_close': 'original client discards Close return',
                'scope': 'nondeterministic correctness regression; no expected random values or performance claim'}
    write_json(out / 'manifest.json', manifest)
    for index, (argv, cwd) in enumerate(builds):
        with (out / f'build-{index}.out').open('xb') as log:
            subprocess.run(argv, cwd=cwd, env=env, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=600)
    write_json(out / 'binaries.json', {p.name: sha(p.read_bytes()) for p in (server, client, verifier)})
    write_json(out / 'binary-build-info.json', {p.name: subprocess.check_output(
        ['go', 'version', '-m', str(p)], cwd=cwd, env=env, text=True)
        for p, (_, cwd) in zip((server, verifier, client), builds)})
    results = {}
    try:
        for profile in PROFILES:
            results[profile] = run_profile(out, profile, server, client, verifier, env)
    except (RunnerInterrupted, CleanupFailure):
        write_json(out / 'terminal.json', {'finished_utc': now(), 'profiles': results, 'passed': False, 'interrupted': True})
        raise
    write_json(out / 'terminal.json', {'finished_utc': now(), 'profiles': results, 'passed': all(results.values())})
    return 0 if all(results.values()) else 1


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, interrupt)
    signal.signal(signal.SIGINT, interrupt)
    try:
        sys.exit(main())
    except (RunnerInterrupted, CleanupFailure) as exc:
        print(f'cow JSON regression interrupted: {exc}', file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f'cow JSON regression failed: {type(exc).__name__}: {exc}', file=sys.stderr)
        sys.exit(1)
