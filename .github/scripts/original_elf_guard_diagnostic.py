#!/usr/bin/env python3
"""One fixed hosted diagnostic; never an acceptance gate or benchmark rebuild.

The workflow authenticates six fixed bootstrap files. This program owns fixed
Windows source Git acquisition and tool provisioning within its counted root,
and samples new bytes through the selected payload. Windows has no claimed hard descendant RSS cap.
"""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import stat
import subprocess
import sys
import threading
import tarfile
import time
import urllib.request
import urllib.error
import zipfile

BASE = '8e1133085b4052e344a73063f28c2ab142485c73'
HEAD = 'bed11597d3cae29f9555cfb614cca973ddfada99'
OBSERVER = '0b20264570092ba55a51bf0be047c41c877c7258'
ARTIFACT = 13302414006
ZIP_BYTES = 229311766
ZIP_SHA = 'cf86e9161d337fbce526ac82be52f077cb0ae8106e904fcc8aa8e3fdf436611c'
ELFS = {'B': '8a8148fe78f8aa65a5f9cde38cc58b68a46773e2e83a32fb031ff5d9120c27ac',
        'C': '4d52442766f330fb217fd36ed392e34a07fa4880c4acae2e6d64a4c3055b3b96'}
GUARD_TEST = 'TestFixedPeerTCPNativeCollectionCatalogGuardInterpositionV1'
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


def census(root):
    total = logs = 0
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
            # Authenticated source/SDK/module bytes stay in whole-root accounting.
            # Generated outputs have only these fixed namespaces; inputs are not logs.
            if relative.parts[0] in ('output', 'guard') or (len(relative.parts) == 1 and path.suffix in ('.log', '.json', '.jsonl', '.txt', '.trace')):
                logs += s.st_size
    need(total <= ROOT_CAP, 'whole root exceeds 1GiB')
    need(logs <= LOG_CAP, 'aggregate log/trace/JSON exceeds 32MiB')
    need(shutil.disk_usage(root).free >= 10*GIB, 'free floor below 10GiB')
    return {'whole_bytes': total, 'log_trace_json_bytes': logs}


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
        self.maximum = {'whole_bytes': 0, 'log_trace_json_bytes': 0}
        self.thread = threading.Thread(target=self.watch, daemon=True)
        self.thread.start()

    def watch(self):
        try:
            while not self.stop.is_set():
                row = census(self.root)
                for k, v in row.items():
                    self.maximum[k] = max(self.maximum[k], v)
                if time.monotonic()-self.started > 600:
                    raise RuntimeError('600s payload deadline')
                self.stop.wait(.2)
        except BaseException as error:
            self.failure = repr(error)
            self.stop.set()

    def pulse(self):
        # Cheap within already precharged direct writes; no tree walk per chunk/member.
        need(self.failure is None, 'resource failure during materialization: '+str(self.failure))
        remaining = 600-(time.monotonic()-self.started)
        need(remaining > 0, '600s whole deadline during materialization')
        return remaining

    def check(self, additional_bytes=0):
        remaining = self.pulse()
        row = census(self.root)
        need(row['whole_bytes']+additional_bytes <= ROOT_CAP, 'prospective write exceeds whole root')
        return remaining

    def run(self, label, argv, seconds, cwd=None, env=None):
        self.check()
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
                            self.failure = self.failure or 'Windows intervention: descendant drain unproved'
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
        need(row['original_handle_joined'], 'owned process not joined; cleanup HOLD')
        need(row.get('taskkill_exit', 0) == 0, 'taskkill failed; cleanup HOLD')
        need('intervention' not in row, 'stage intervention; diagnostic HOLD')
        return row['exit_code']

    def close(self):
        self.stop.set()
        self.thread.join()
        write(self.out/'resource-summary.json', {'maximum': self.maximum, 'error': self.failure,
              'scope': 'sampled complete root after workflow bootstrap; Windows Go memory target is soft; no hard Windows RSS guarantee'})


def provision_go(attempt, root):
    system = platform.system()
    archive_name, expected_size, expected_sha = {
        'Linux': ('go1.26.10.linux-amd64.tar.gz', 66936659, 'ccfe7ae822d2c2f41f4fe7c956199967a4dac22d0ee0d2b2bfdb440aab3b20c5'),
        'Windows': ('go1.26.10.windows-amd64.zip', 75004774, '7ecd2d7260e82c59bcbd7f1077a07cee2dea607295b858df67195a1e4e09629b'),
    }[system]
    archive = root/archive_name
    with urllib.request.urlopen('https://go.dev/dl/'+archive_name, timeout=min(120, attempt.check(expected_size))) as source, archive.open('xb') as stream:
        size = 0
        while True:
            attempt.pulse()
            if source.isclosed():
                break
            source.fp.raw._sock.settimeout(min(10, attempt.pulse()))
            block = source.read(1 << 20)
            if not block:
                break
            attempt.pulse()
            size += len(block)
            need(size <= expected_size, 'official Go archive oversize')
            stream.write(block)
    attempt.check()
    need(size == expected_size and sha(archive) == expected_sha, 'pinned official tool archive identity')
    attempt.check()
    # Header pass before any extraction: all expanded bytes must fit alongside
    # archive, checkout and receipts. No link, traversal or device extraction.
    if system == 'Linux':
        with tarfile.open(archive, 'r:gz') as package:
            members = package.getmembers()
            need(all(m.isdir() or m.isfile() for m in members), 'tool archive nonregular member')
            names = [m.name for m in members]
            total = sum(m.size for m in members)
    else:
        with zipfile.ZipFile(archive) as package:
            members = package.infolist()
            need(all(not stat.S_ISLNK(m.external_attr >> 16) for m in members), 'tool archive symlink')
            names = [m.filename for m in members]
            total = sum(m.file_size for m in members)
    need(len(names) == len(set(names)) and all((n.startswith('go/') or (system == 'Linux' and n == 'go' and m.isdir() and m.size == 0)) and '..' not in Path(n).parts and '\\' not in n for n, m in zip(names, members)), 'unsafe tool archive')
    before = census(root)
    # Fixed receipt also fits within the precharge; archive is already counted.
    attempt.check(total+1024)
    write(root/'tool-archive.json', {'name': archive_name, 'sha256': expected_sha, 'bytes': size,
          'expanded_bytes': total, 'receipt_reservation_bytes':1024,
          'peak_upper_bound': before['whole_bytes']+total+1024})
    need((root/'tool-archive.json').stat().st_size <= 1024, 'tool receipt exceeds precharge')
    expanded = 0
    if system == 'Linux':
        with tarfile.open(archive, 'r:gz') as package:
            for member in package:
                attempt.pulse()
                output = root/member.name
                if member.isdir():
                    output.mkdir(parents=True, exist_ok=True)
                    continue
                output.parent.mkdir(parents=True, exist_ok=True)
                member_bytes = 0
                with package.extractfile(member) as source, output.open('xb') as stream:
                    while True:
                        attempt.pulse()
                        block = source.read(1 << 20)
                        if not block:
                            break
                        attempt.pulse()
                        member_bytes += len(block)
                        expanded += len(block)
                        declared = member.size if system == 'Linux' else member.file_size
                        need(member_bytes <= declared and expanded <= total, 'tool decoded bytes exceed precharge')
                        stream.write(block)
                need(output.stat().st_size == member.size, 'tool member size')
                output.chmod(member.mode & 0o777)
    else:
        with zipfile.ZipFile(archive) as package:
            for member in package.infolist():
                attempt.pulse()
                output = root/member.filename
                if member.is_dir():
                    output.mkdir(parents=True, exist_ok=True)
                    continue
                output.parent.mkdir(parents=True, exist_ok=True)
                member_bytes = 0
                with package.open(member) as source, output.open('xb') as stream:
                    while True:
                        attempt.pulse()
                        block = source.read(1 << 20)
                        if not block:
                            break
                        attempt.pulse()
                        member_bytes += len(block)
                        expanded += len(block)
                        declared = member.size if system == 'Linux' else member.file_size
                        need(member_bytes <= declared and expanded <= total, 'tool decoded bytes exceed precharge')
                        stream.write(block)
                need(output.stat().st_size == member.file_size, 'tool member size')
    need(expanded == total, 'tool expanded byte total differs from headers')
    attempt.check()
    # Remove only this authenticated owned staging archive, never preexisting inputs.
    archive.unlink()
    os.environ['GOROOT'] = str(root/'go')
    os.environ['PATH'] = str(root/'go/bin')+os.pathsep+os.environ['PATH']
    executable = root/'go/bin'/('go.exe' if system == 'Windows' else 'go')
    write(root/'tool-input.json', {'go_path': str(executable), 'sha256': sha(executable),
          'archive_staging_removed_after_authentication': True})


def retrieve_original(attempt, root, token):
    archive = root/'original.zip'
    need(census(root)['whole_bytes']+ZIP_BYTES <= ROOT_CAP, 'original archive staging peak')
    request = urllib.request.Request('https://api.github.com/repos/snissn/gomap/actions/artifacts/'+str(ARTIFACT)+'/zip',
              headers={'Authorization': 'Bearer '+token, 'Accept': 'application/vnd.github+json',
                       'X-GitHub-Api-Version': '2022-11-28'})
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):
            return None
    try:
        with urllib.request.build_opener(NoRedirect).open(request, timeout=min(30, attempt.pulse())):
            raise RuntimeError('expected original archive redirect')
    except urllib.error.HTTPError as redirect:
        need(redirect.code == 302, 'artifact download authorization/availability')
        location = redirect.headers['Location']
        need(location.startswith('https://'), 'non-HTTPS archive location')
        redirect.close()
    # Deliberately remove the GitHub credential before following signed storage URL.
    with urllib.request.urlopen(location, timeout=min(180, attempt.check(ZIP_BYTES))) as response, archive.open('xb') as stream:
        count = 0
        while True:
            attempt.pulse()
            if response.isclosed():
                break
            response.fp.raw._sock.settimeout(min(10, attempt.pulse()))
            block = response.read(1 << 20)
            if not block:
                break
            attempt.pulse()
            count += len(block)
            need(count <= ZIP_BYTES, 'artifact larger than original')
            stream.write(block)
    attempt.check()
    need(count == ZIP_BYTES and sha(archive) == ZIP_SHA, 'original ZIP identity')
    attempt.check()
    wanted = {'measured-binaries.json', 'environment.txt', 'binary-sha256.txt', 'summary.json',
              'measured-binaries/baseline-db.test', 'measured-binaries/candidate-db.test', 'baseline.txt', 'candidate.txt'}
    destination = root/'original'
    destination.mkdir()
    with zipfile.ZipFile(archive) as z:
        selected_total = sum(m.file_size for m in z.infolist() if m.filename in wanted)
        attempt.check(selected_total+4096)  # Fixed eight-row selected-members receipt.
        selected_written = 0
        names = set()
        rows = []
        for member in z.infolist():
            attempt.pulse()
            name = member.filename
            need(name not in names and not name.startswith('/') and '\\' not in name and '..' not in Path(name).parts,
                 'unsafe/duplicate ZIP member')
            names.add(name)
            need(not stat.S_ISLNK(member.external_attr >> 16), 'ZIP symlink')
            if name not in wanted:
                continue
            need(member.file_size <= 160*1024**2, 'selected member too large')
            output = destination/name
            output.parent.mkdir(parents=True, exist_ok=True)
            member_bytes = 0
            with z.open(member) as source, output.open('xb') as stream:
                while True:
                    attempt.pulse()
                    block = source.read(1 << 20)
                    if not block:
                        break
                    attempt.pulse()
                    member_bytes += len(block)
                    selected_written += len(block)
                    need(member_bytes <= member.file_size and selected_written <= selected_total, 'selected decoded bytes exceed precharge')
                    stream.write(block)  # zipfile checks selected-member CRC at EOF.
            need(output.stat().st_size == member.file_size, 'selected member size')
            rows.append({'path': name, 'bytes': member.file_size, 'crc32': member.CRC, 'sha256': sha(output)})
        need(wanted <= names, 'original measured receipts/members missing')
        need(selected_written == selected_total, 'selected decoded total differs from headers')
    need(len(json.dumps(rows, indent=2, sort_keys=True, allow_nan=False).encode())+1 <= 4096, 'selected receipt exceeds precharge')
    write(destination/'selected-members.json', rows)
    attempt.check()
    manifest = json.loads((destination/'measured-binaries.json').read_text())
    need(manifest['status'] == 'RETAINED_ACTUAL_MEASURED_EXECUTABLES', 'original producer status')
    for key, filename in (('environment_sha256', 'environment.txt'), ('initial_digests_sha256', 'binary-sha256.txt'),
                          ('checker_summary_sha256', 'summary.json')):
        need(manifest[key] == sha(destination/filename), 'original receipt chain '+key)
    summary = json.loads((destination/'summary.json').read_text())
    for arm, revision, source in [('B', 'baseline', BASE), ('C', 'candidate', HEAD)]:
        name = 'measured-binaries/'+revision+'-db.test'
        entries = [row for row in manifest['files'] if row['path'] == name]
        need(len(entries) == 1, 'measured executable inventory')
        row = entries[0]
        need(row['sha256'] == ELFS[arm] == sha(destination/name) and row['source_sha'] == source,
             'measured db identity')
        need(row['source_tree'] == {'B': '6b0fc6bd9f6a22621c85526342727509fc3bcafe', 'C': '8f6db4a61b295eca5606f3182f4d4f7ea2b6fa0b'}[arm], 'original source tree')
        need(row['bytes'] == (destination/name).stat().st_size, 'original binary size')
        need(row['build_argv'][:5] == ['go', 'test', '-c', '-trimpath', '-buildvcs=false'] and row['build_argv'][-1] == './TreeDB/db', 'original build command')
        need(row['package'] == './TreeDB/db' and row['build_environment'] == {'GOWORK': 'off'}, 'original build context')
        need(summary['binary_digests']['db'][revision] == ELFS[arm], 'original checker ELF binding')
        (destination/name).chmod(0o700)
    return destination


def execution_identity(attempt, root):
    names = ['git'] + (['taskset','timeout','readelf','ldd'] if platform.system() == 'Linux' else [])
    files = {'python': Path(sys.executable).resolve(strict=True), 'go': Path(shutil.which('go')).resolve(strict=True)}
    for name in names:
        located = shutil.which(name)
        need(located is not None, 'required standard metadata/execution utility missing: '+name)
        files[name] = Path(located).resolve(strict=True)
    if platform.system() == 'Windows':
        for name in ('kernel32.dll','ntdll.dll','ucrtbase.dll'):
            files[name] = Path(os.environ['SystemRoot'])/'System32'/name
        for path in Path(sys.base_prefix).glob('python*.dll'):
            files[path.name] = path
    result = {name: {'path':str(path),'bytes':path.stat().st_size,'sha256':sha(path)} for name,path in files.items()}
    result['context'] = {'platform':platform.platform(),'kernel':platform.release(),
        'affinity': sorted(os.sched_getaffinity(0)) if hasattr(os,'sched_getaffinity') else None,
        'image': {k:os.environ.get(k) for k in ('ImageOS','ImageVersion','RUNNER_OS','RUNNER_ARCH')},
        'original_runtime_equivalence':False}
    if platform.system() == 'Linux':
        need(0 in result['context']['affinity'], 'CPU0 unavailable; no substitute CPU')
    write(attempt.out/'execution-input-identities.json',result)


def linux(attempt, root, token):
    original = retrieve_original(attempt, root, token)
    env = dict(os.environ, GOMAXPROCS='1', PPROF_TMPDIR=str(root/'tmp'))
    env.pop('GH_TOKEN', None)
    binary = {arm: str(original/'measured-binaries'/name) for arm, name in
              [('B', 'baseline-db.test'), ('C', 'candidate-db.test')]}
    dependencies = {}
    metadata = {}
    for arm in ('B','C'):
        need(attempt.run(arm+'-readelf', ['readelf','-l','-d',binary[arm]], 10, env=env) == 0, 'standard original ELF metadata')
        headers = (attempt.out/(arm+'-readelf.stdout.log')).read_text()
        interpreter = re.findall(r'Requesting program interpreter: ([^\]]+)', headers)
        dynamic = 'NEEDED' in headers or 'INTERP' in headers
        ldd_code = attempt.run(arm+'-ldd', ['ldd',binary[arm]], 10, env=env)
        raw = (attempt.out/(arm+'-ldd.stdout.log')).read_text()+(attempt.out/(arm+'-ldd.stderr.log')).read_text()
        if not dynamic:
            need(not interpreter and 'There is no dynamic section in this file.' in headers,
                 'static ELF must prove absent interpreter and dynamic section')
            need(ldd_code in (0,1) and any(value in raw for value in ('not a dynamic executable','statically linked')),
                 'unsupported static ldd signature')
            metadata[arm] = {'linkage':'static','interpreter':None,'dependencies':[],'ldd_exit':ldd_code}
        else:
            need(ldd_code == 0 and len(interpreter) == 1 and 'not found' not in raw, 'unresolved dynamic ELF dependency')
            paths = set(re.findall(r'(/[^\s()]+)', raw)) | set(interpreter)
            need(bool(paths), 'dynamic ELF loader/dependency unavailable')
            for path in paths:
                actual = Path(path).resolve(strict=True)
                dependencies[str(actual)] = {'sha256':sha(actual), 'bytes':actual.stat().st_size}
            metadata[arm] = {'linkage':'dynamic','interpreter':interpreter[0],'dependencies':sorted(paths),'ldd_exit':ldd_code}
        need(attempt.run(arm+'-original-buildinfo', ['go','version','-m',binary[arm]], 10, env=env) == 0, 'original ELF buildinfo')
    write(attempt.out/'original-new-loader-identities.json', {'elfs':metadata,'resolved_files':dependencies})
    for label, arm in [('B1','B'), ('C1','C'), ('C2','C'), ('B2','B')]:
        profile = str(attempt.out/(label+'.cpu.pprof'))
        argv = ['taskset','-c','0','timeout','--kill-after=5s','30s',binary[arm], '-test.run=^$',
                '-test.bench=^BenchmarkGetVersioned$', '-test.benchmem', '-test.benchtime=20s',
                '-test.count=1','-test.cpuprofile='+profile]
        code = attempt.run(label, argv, 36, env=env)
        need(code == 0, 'original benchmark failed; stop without replacement')
        need(Path(profile).is_file() and Path(profile).stat().st_size > 0, 'missing CPU profile')
    need(attempt.run('pprof-path', ['go','tool','-n','pprof'], 120, env=env) == 0, 'standard pprof materialization')
    tool = Path((attempt.out/'pprof-path.stdout.log').read_text().strip()).resolve(strict=True)
    need(tool.is_relative_to(root), 'fresh pprof tool outside counted root')
    write(attempt.out/'pprof-identity.json', {'path': str(tool), 'sha256': sha(tool), 'bytes': tool.stat().st_size})
    need(attempt.run('pprof-buildinfo', ['go','version','-m',str(tool)], 10, env=env) == 0, 'standard pprof buildinfo')
    # Standard noninteractive pprof only. No online symbols, source listing or alternate decoder.
    for label, arm in [('B1','B'), ('C1','C'), ('C2','C'), ('B2','B')]:
        for name, options in [('top',['-top']), ('cum',['-top','-cum']), ('traces',['-traces'])]:
            code = attempt.run(label+'-'+name, [str(tool),'-symbolize=none',*options,
                               binary[arm],str(attempt.out/(label+'.cpu.pprof'))], 30, env=env)
            need(code == 0, 'standard text decode failed')


def acquire_windows_source(attempt, root, token):
    # Fixed exact-source acquisition; no action/REST/archive fallback.
    source = root/'source'
    template = root/'git-template'
    template.mkdir()
    source.mkdir()
    env = {k:v for k,v in os.environ.items() if not k.startswith('GIT_') and k not in
           ('GH_TOKEN','GITHUB_TOKEN','GCM_TRACE','GIT_CURL_VERBOSE')}
    env.update(GIT_CONFIG_NOSYSTEM='1', GIT_CONFIG_GLOBAL=os.devnull,
               GIT_TERMINAL_PROMPT='0', GCM_INTERACTIVE='Never')
    git = Path(shutil.which('git')).resolve(strict=True)
    write(attempt.out/'source-git-input.json', {'path':str(git),'sha256':sha(git),'bytes':git.stat().st_size,
          'snapshot_is_git_worktree':True,'harness_is_git_worktree':False})
    commands = [('source-init',[str(git),'init','--template='+str(template),str(source)]),
                ('source-sparse',[str(git),'-C',str(source),'config','--local','core.sparseCheckout','true']),
                ('source-line-endings',[str(git),'-C',str(source),'config','--local','core.autocrlf','false']),
                ('source-origin',[str(git),'-C',str(source),'remote','add','origin','https://github.com/snissn/gomap.git'])]
    for label, argv in commands:
        need(attempt.run(label, argv, 10, env=env) == 0, 'fixed source setup failed')
    attempt.check()
    (source/'.git/info').mkdir(parents=True, exist_ok=True)
    (source/'.git/info/sparse-checkout').write_text('/*\n!/docs/evidence/\n!/docs/benchmarks/\n!/TreeDB/docs/evidence/\n!/TreeDB/docs/benchmarks/\n', encoding='utf-8')
    fetch_env = dict(env)
    if token:
        fetch_env.update(GIT_CONFIG_COUNT='1', GIT_CONFIG_KEY_0='http.https://github.com/.extraheader',
            GIT_CONFIG_VALUE_0='Authorization: Basic '+base64.b64encode(('x-access-token:'+token).encode()).decode())
    need(attempt.run('source-fetch',[str(git),'-C',str(source),'-c','protocol.version=2','fetch',
        '--depth=1','--filter=blob:none','--no-tags','--no-recurse-submodules','origin',OBSERVER],120,env=fetch_env)==0,
        'fixed source fetch failed; no fallback')
    fetch_env.clear()  # Authentication is never passed to checkout/build/test/tool commands.
    fetch_log=(attempt.out/'source-fetch.stderr.log').read_text().lower()
    need(not any(value in fetch_log for value in ('filtering not recognized','does not support filter','filter is not supported')),
         'server does not support selected partial fetch; stop')
    # Lazy checkout blob fetch needs the same ephemeral authorization, only while
    # this owned source command runs. Nothing is stored in .git/config.
    checkout_env = dict(env)
    if token:
        checkout_env.update(GIT_CONFIG_COUNT='1', GIT_CONFIG_KEY_0='http.https://github.com/.extraheader',
            GIT_CONFIG_VALUE_0='Authorization: Basic '+base64.b64encode(('x-access-token:'+token).encode()).decode())
    need(attempt.run('source-checkout',[str(git),'-C',str(source),'checkout','--detach',OBSERVER],120,env=checkout_env)==0,
         'fixed source checkout failed; no fallback')
    checkout_env.clear()
    need(attempt.run('source-tree',[str(git),'-C',str(source),'rev-parse','HEAD^{tree}'],10,env=env)==0,'source tree unavailable')
    need((attempt.out/'source-tree.stdout.log').read_text().strip()=='456878d8106f2225cb0593f8200b7ff2f34984a6','exact observer tree required')
    need(attempt.run('source-clean',[str(git),'-C',str(source),'diff','--exit-code','HEAD','--'],30,env=env)==0,'source worktree differs')
    source_bytes=0
    attempt.check()
    for directory, dirs, files in os.walk(source):
        attempt.pulse()
        if Path(directory)==source:
            dirs[:]=[name for name in dirs if name!='.git']
        for name in files:
            path=Path(directory)/name
            need(path.is_file() and not path.is_symlink(),'nonregular observer source')
            source_bytes+=path.stat().st_size
    need(source_bytes==122385372,'exact selected source size mismatch')
    attempt.check()
    write(attempt.out/'observer-source-receipt.json', {'head':OBSERVER,'tree':'456878d8106f2225cb0593f8200b7ff2f34984a6',
        'selected_bytes':source_bytes,'excluded':['docs/evidence','docs/benchmarks','TreeDB/docs/evidence','TreeDB/docs/benchmarks'],
        'source_is_git_worktree':True,'harness_is_git_worktree':False,'archive_fallback':False})


def windows(attempt, root):
    source = root/'source'
    need(attempt.run('observer-head', ['git','rev-parse','HEAD'], 10, cwd=source) == 0, 'observer source metadata')
    observed = (attempt.out/'observer-head.stdout.log').read_text().strip()
    need(observed == OBSERVER, 'exact observer source required')
    env = dict(os.environ, GOMAXPROCS='2', GOMEMLIMIT='4GiB', GOWORK='off')
    need(attempt.run('go-environment', ['go','env','-json','CGO_ENABLED','CC','CXX','GOOS','GOARCH','GOVERSION','GOROOT','GOTOOLDIR'], 10, env=env) == 0, 'actual Windows compiler/runtime defaults')
    compiler_env = json.loads((attempt.out/'go-environment.stdout.log').read_text())
    if compiler_env['CGO_ENABLED'] == '1':
        compiler = shutil.which(compiler_env['CC'])
        need(compiler is not None, 'actual maintained CGO compiler unavailable')
        write(attempt.out/'cgo-compiler-identity.json', {'path':compiler,'sha256':sha(compiler),'bytes':Path(compiler).stat().st_size})
    binary = root/'nativewire.test.exe'
    need(attempt.run('build', ['go','test','-c','-p','1','-mod=readonly','-o',str(binary),'./TreeDB/nativewire'],
                     240, cwd=source, env=env) == 0, 'diagnostic Windows build')
    trace = attempt.out/'guard.trace'
    # Existing twenty-second per-subtest contexts and two-second request timers are untouched.
    code = attempt.run('guard', [str(binary),'-test.v','-test.run=^'+GUARD_TEST+'$',
                       '-test.count=1','-test.timeout=120s','-test.trace='+str(trace)], 130, env=env)
    need(trace.is_file() and trace.stat().st_size > 0, 'missing original trace')
    # Retain original test failure separately; never convert failure into test PASS.
    need(attempt.run('pprof-path', ['go','tool','-n','pprof'], 120, env=env) == 0, 'standard Windows pprof materialization')
    tool = Path((attempt.out/'pprof-path.stdout.log').read_text().strip()).resolve(strict=True)
    need(tool.is_relative_to(root), 'fresh Windows pprof outside counted root')
    need(attempt.run('pprof-buildinfo', ['go','version','-m',str(tool)], 10, env=env) == 0, 'pprof buildinfo')
    write(attempt.out/'pprof-identity.json', {'path': str(tool), 'sha256': sha(tool), 'bytes': tool.stat().st_size})
    need(attempt.run('trace-tool-path', ['go','tool','-n','trace'], 120, env=env) == 0, 'standard trace materialization')
    trace_tool = Path((attempt.out/'trace-tool-path.stdout.log').read_text().strip()).resolve(strict=True)
    need(trace_tool.is_relative_to(root), 'fresh trace tool outside counted root')
    write(attempt.out/'trace-tool-identity.json', {'path':str(trace_tool),'sha256':sha(trace_tool),'bytes':trace_tool.stat().st_size})
    need(attempt.run('trace-tool-buildinfo', ['go','version','-m',str(trace_tool)], 10, env=env) == 0, 'trace buildinfo')
    for view in ('net','sync','syscall','sched'):
        need(attempt.run('trace-'+view, [str(trace_tool),'-pprof='+view,str(trace)], 30, env=env) == 0,
             'standard trace view failed')
        raw_profile = attempt.out/('trace-'+view+'.pprof')
        (attempt.out/('trace-'+view+'.stdout.log')).rename(raw_profile)
        need(attempt.run('trace-'+view+'-top', [str(tool),'-symbolize=none','-top',str(raw_profile)], 30, env=env) == 0, 'standard trace text view')
    write(attempt.out/'guard-observation.json', {'raw_test_exit': code, 'test_pass': code == 0,
          'scope': 'same-process original parent/json/bson only; trace setup/other goroutines included; no server commit inference'})
    output = (attempt.out/'guard.stdout.log').read_text(encoding='utf-8')
    expected = {GUARD_TEST, GUARD_TEST+'/json', GUARD_TEST+'/bson'}
    runs = re.findall(r'^=== RUN   (\S+)', output, re.M)
    passes = re.findall(r'^\s*--- PASS: (\S+)', output, re.M)
    need(code == 0 and len(runs) == 3 and set(runs) == expected and len(passes) == 3 and set(passes) == expected
         and '--- FAIL:' not in output and '--- SKIP:' not in output,
         'original guard failure/incomplete inventory retained; diagnostic does not clear main RED')


def emit_receipts(root, mode):
    # Fixed plain output, never raw binary profiles, credential environment or URLs.
    files = {}
    for path in sorted((root/'output').iterdir()):
        if path.is_file():
            files[path.name] = {'bytes': path.stat().st_size, 'sha256': sha(path)}
    write(root/'output/final-file-manifest.json', files)
    text = ['FIXED_DIAGNOSTIC_FILE_MANIFEST '+json.dumps(files, sort_keys=True)]
    for path in [root/'tool-archive.json',root/'tool-input.json',root/'harness/.github/scripts/original_elf_guard_inputs.json']:
        if path.is_file():
            text.append('AUTHENTICATED_INPUT_RECEIPT '+path.name+' '+path.read_text())
    for path in (root/'output').glob('*identit*.json'):
        text.append('EXECUTION_INPUT_IDENTITY '+path.name+' '+path.read_text())
    for path in sorted((root/'output').glob('*.receipt.json')):
        text.append('STAGE_RECEIPT '+path.read_text())
    if mode == 'linux':
        for name in ('baseline.txt','candidate.txt'):
            path = root/'original'/name
            if path.exists():
                text.append('ORIGINAL_ALL_EIGHT_PAIRS '+name+'\n'+path.read_text())
        selected = [root/'output'/(label+'-'+view+'.stdout.log') for label in ('B1','C1','C2','B2') for view in ('top','cum','traces')]
        selected += [root/'output'/(label+'.stdout.log') for label in ('B1','C1','C2','B2')]
    else:
        selected = [root/'output'/('trace-'+view+'-top.stdout.log') for view in ('net','sync','syscall','sched')]
    for path in selected:
        if path.exists():
            data = path.read_bytes()
            text.append('STANDARD_DIAGNOSTIC_TEXT '+path.name+'\n'+data.decode('utf-8', errors='strict'))
    if mode == 'windows' and (root/'output/guard.stdout.log').exists():
        for line in (root/'output/guard.stdout.log').read_text().splitlines():
            if 'guard-create-client-observation-v1:' in line or line.lstrip().startswith(('=== RUN', '--- PASS:', '--- FAIL:', '--- SKIP:')):
                text.append('ORIGINAL_GUARD_EVENT '+line)
    # Console duplicates are charged to the same aggregate log acceptance cap.
    console_bytes = sum(len(part.encode('utf-8'))+1 for part in text)
    need(console_bytes <= 8*1024**2, 'bounded hosted console budget exceeded; no truncation')
    retained = census(root)
    need(retained['log_trace_json_bytes']+(2 if mode == 'linux' else 1)*console_bytes <= LOG_CAP, 'retained plus duplicated console logs exceed32MiB')

    print('\n'.join(text), flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=('linux','windows','census'))
    parser.add_argument('--root', type=Path, required=True)
    args = parser.parse_args()
    need(os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch' and os.environ.get('GITHUB_REPOSITORY') == 'snissn/gomap'
         and os.environ.get('GITHUB_ACTOR_ID') == '1981537' and os.environ.get('GITHUB_RUN_ATTEMPT') == '1'
         and os.environ.get('GITHUB_REF') == 'refs/heads/diagnostic/original-elf-and-guard-trace-01', 'fixed dispatch authority')
    expected = os.environ.get('REVIEWED_HEAD', '')
    need(re.fullmatch('[0-9a-f]{40}', expected) is not None and expected == os.environ.get('GITHUB_SHA') == os.environ.get('WORKFLOW_COMMIT'), 'immutable reviewed head')
    root = args.root.resolve(strict=True)
    if args.mode == 'census':
        print(json.dumps(census(root), sort_keys=True))
        return
    need(platform.system() == ('Linux' if args.mode == 'linux' else 'Windows'), 'platform mismatch')
    for name in ('tmp','gocache','modcache'):
        (root/name).mkdir(exist_ok=True)
    os.environ.update(TMPDIR=str(root/'tmp'), TMP=str(root/'tmp'), TEMP=str(root/'tmp'),
                      GOTMPDIR=str(root/'tmp'), GOCACHE=str(root/'gocache'), GOMODCACHE=str(root/'modcache'),
                      GOWORK='off', GOTOOLCHAIN='local', PYTHONDONTWRITEBYTECODE='1',
                      PPROF_TMPDIR=str(root/'tmp'), XDG_CACHE_HOME=str(root/'xdg-cache'), XDG_CONFIG_HOME=str(root/'xdg-config'))
    if args.mode == 'windows':
        for name in ('appdata', 'localappdata'):
            (root/name).mkdir(exist_ok=True)
        os.environ.update(APPDATA=str(root/'appdata'), LOCALAPPDATA=str(root/'localappdata'))
    token = os.environ.pop('GH_TOKEN', None)
    os.environ.pop('GITHUB_TOKEN', None)
    need(args.mode != 'linux' or bool(token), 'original artifact read credential missing')
    attempt = Attempt.__new__(Attempt)
    attempt.started = float(os.environ['DIAGNOSTIC_STARTED_MONOTONIC'])
    need(0 <= time.monotonic()-attempt.started < 600, 'first-step whole deadline unavailable/expired')
    Attempt.__init__(attempt, root)
    try:
        write(attempt.out/'new-runtime.json', {'platform': platform.platform(), 'python': sys.version,
              'environment': {k: os.environ.get(k) for k in ('GOMAXPROCS','GOMEMLIMIT','GOCACHE','GOMODCACHE','GOTMPDIR')},
              'scope': 'new hosted diagnostic context, not original runtime equivalence'})
        if args.mode == 'windows':
            acquire_windows_source(attempt, root, token)
        provision_go(attempt, root)
        need(attempt.run('go-version', ['go','version'], 10) == 0, 'tool metadata')
        need((attempt.out/'go-version.stdout.log').read_text().startswith('go version go1.26.10 '), 'pinned Go tool required')
        execution_identity(attempt, root)
        if args.mode == 'linux':
            linux(attempt, root, token)
        else:
            windows(attempt, root)
    finally:
        attempt.close()
        write(attempt.out/'completion.json', {'stages': attempt.rows, 'resource_error': attempt.failure,
              'acceptance': False, 'original_required_failures_preserved': True})
        emit_receipts(root, args.mode)
    need(attempt.failure is None, 'resource failure retained')


if __name__ == '__main__':
    main()
