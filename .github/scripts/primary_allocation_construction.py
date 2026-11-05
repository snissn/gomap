#!/usr/bin/env python3
"""One fixed Linux PRIMARY source/build prerequisite. No benchmark/capture route."""
import argparse
import base64
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import signal
import stat
import subprocess
import sys
import threading
import time

GIB = 1024**3
WHOLE = 840
BRANCH = 'refs/heads/diagnostic/primary-allocation-construction-01'
SDK = Path('/opt/hostedtoolcache/go/1.26.10/x64')
HERE = Path(__file__).resolve().parent
HARNESS = HERE.parent.parent
DATA = HARNESS/'.github/primary-construction'
ARMS = ('B', 'C')
CONTEXT = ('GOVERSION', 'GOOS', 'GOARCH', 'GOAMD64', 'CGO_ENABLED', 'CC', 'CXX',
           'CGO_CFLAGS', 'CGO_CPPFLAGS', 'CGO_CXXFLAGS', 'CGO_LDFLAGS',
           'GOFLAGS', 'GOEXPERIMENT', 'GOROOT')


def need(ok, reason):
    if not ok:
        raise RuntimeError(reason)


def sha(path):
    result = hashlib.sha256()
    with Path(path).open('rb') as source:
        for block in iter(lambda: source.read(1 << 20), b''):
            result.update(block)
    return result.hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True).encode('ascii')


def read(path):
    return json.loads(Path(path).read_text())


def write(path, value):
    encoded = canonical(value)+b'\n'
    need(len(encoded) <= 32*1024**2, 'receipt size limit')
    with Path(path).open('xb') as target:
        target.write(encoded)


def authority():
    e = os.environ
    need(e.get('GITHUB_ACTIONS') == 'true' and e.get('RUNNER_ENVIRONMENT') == 'github-hosted'
         and e.get('RUNNER_OS') == 'Linux' and e.get('GITHUB_EVENT_NAME') == 'workflow_dispatch'
         and e.get('GITHUB_REPOSITORY') == 'snissn/gomap' and e.get('GITHUB_REF') == BRANCH
         and e.get('GITHUB_ACTOR_ID') == '1981537' and e.get('GITHUB_RUN_ATTEMPT') == '1',
         'fixed hosted authority required')
    head = e.get('REVIEWED_HEAD', '')
    need(re.fullmatch('[0-9a-f]{40}', head) is not None
         and head == e.get('GITHUB_SHA') == e.get('WORKFLOW_COMMIT'), 'exact harness head required')
    encoded = e.get('ROOT_ADMISSION_B64', '')
    need(0 < len(encoded) <= 32768, 'bounded ROOT admission required')
    a = json.loads(base64.b64decode(encoded, validate=True))
    need(a.get('schema') == 'primary-construction-admission-v1'
         and a.get('status') == 'ADMITTED_CONSTRUCTION_ONLY' and a.get('authority') == 'ROOT'
         and a.get('actor_id') == '1981537' and a.get('head') == head
         and a.get('run_attempt') == '1' and a.get('allow_capture') is False,
         'fixed ROOT construction-only admission')
    for field in ('independent_source_review_sha256', 'owner_manifest_sha256'):
        need(re.fullmatch('[0-9a-f]{64}', a.get(field, '')) is not None, 'missing review/owner binding')
    c = a.get('controls', {})
    need(c.get('status') == 'ROOT_ADMITTED_FOCUSED_OBSERVER_CONTROLS'
         and c.get('owner_manifest_sha256') == a['owner_manifest_sha256']
         and re.fullmatch('[0-9a-f]{64}', c.get('review_sha256', '')) is not None
         and type(c.get('run_id')) is int and c['run_id'] > 0
         and type(c.get('job_id')) is int and c['job_id'] > 0, 'actual owner controls admission required')
    for mode in ('normal', 'race'):
        need(c.get(mode) == dict(tops=6, outcomes=26, leaves=24, exit=0, skips=0),
             'complete normal/race owner controls required')
    return a


def authenticate(a):
    manifest = HARNESS/'.github/primary-construction-inputs.json'
    need(sha(manifest) == os.environ.get('INPUT_MANIFEST_SHA256'), 'manifest literal differs')
    m = read(manifest)
    for name, row in m['files'].items():
        need(not Path(name).is_absolute() and '..' not in Path(name).parts, 'invalid fixed input path')
        path = HARNESS/name
        s = path.lstat()
        need(stat.S_ISREG(s.st_mode) and s.st_nlink == 1 and not path.is_symlink()
             and s.st_size == row['bytes'] and sha(path) == row['sha256'], 'fixed input drift')
    need(sha(DATA/'owner/manifest.json') == a['owner_manifest_sha256'], 'owner manifest differs')
    return m


def root_path():
    parent = Path(os.environ['RUNNER_TEMP']).resolve(strict=True)
    root = parent/'primary-allocation-construction'
    need(root.resolve(strict=True) == root and root.stat().st_dev == parent.stat().st_dev,
         'fixed root filesystem/identity')
    return root


def accounting_root(root):
    # Hosted job workroot includes _temp, workspace and provider action copies.
    common = root.parent.parent
    need(root.parent.name == '_temp' and common == Path('/home/runner/work')
         and Path(os.environ['GITHUB_WORKSPACE']).resolve().is_relative_to(common),
         'fixed common hosted workroot required')
    return common


def census(root):
    scope = accounting_root(root)
    categories = dict.fromkeys(('source', 'git', 'modules', 'cache', 'tmp', 'bin', 'evidence', 'other'), 0)
    files = count = total = 0
    oversized = False
    for directory, dirs, names in os.walk(scope, followlinks=False):
        for name in dirs:
            need(not (Path(directory)/name).is_symlink(), 'task directory symlink refused')
        for name in names:
            p = Path(directory)/name
            try:
                s = p.lstat()
            except FileNotFoundError:
                continue  # Compiler temp deletion; not an atomic peak measurement.
            need(stat.S_ISREG(s.st_mode) and not p.is_symlink(), 'task nonregular file refused')
            outer = p.relative_to(scope)
            rel = p.relative_to(root) if p.is_relative_to(root) else outer
            top = rel.parts[0]
            category = ('git' if '.git' in rel.parts else 'source') if top.startswith('source-') else {
                'modcache': 'modules', 'gocache': 'cache', 'tmp': 'tmp', 'bin': 'bin',
                'output': 'evidence', 'guard': 'evidence'}.get(top, 'other')
            categories[category] += s.st_size
            total += s.st_size
            count += 1
            # Authenticated source/SDK metadata is input, not a generated log.
            if top in ('output', 'guard'):
                files += s.st_size
                oversized = oversized or s.st_size > 32*1024**2
    free = shutil.disk_usage(root).free
    errors = []
    if total > GIB:
        errors.append('whole root exceeds 1GiB')
    if oversized:
        errors.append('individual evidence/log/profile exceeds 32MiB')
    if free < 10*GIB:
        errors.append('free floor below 10GiB')
    return dict(whole_bytes=total, regular_files=count, evidence_bytes=files, categories=categories,
                free_bytes=free, errors=errors, monotonic=time.monotonic(),
                accounting_root=str(scope), task_root=str(root),
                scope='entire common job workroot from first census; sampled logical bytes, not platform-startup supervision or instantaneous quota')


def load_runner():
    spec = importlib.util.spec_from_file_location('fixed_primary_maintained_runner', DATA/'maintained-runner.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def whole_runner(base, root, started):
    class WholeRootRunner(base.Runner):
        def __init__(self):
            super().__init__(root)
            self.out = root  # WholeRootRunner scope; command logs moved below after initialization.
            self.deadline = started+WHOLE
            self.disk_floor = 10*GIB
            self.first_failure = None
            self.lock = threading.Lock()
            self.stop = threading.Event()
            self.maximum = 0
            self.thread = threading.Thread(target=self.watch, daemon=True)
            self.thread.start()

        def fail(self, reason, row=None):
            try:
                with self.lock:
                    if self.first_failure is None:
                        self.first_failure = dict(reason=reason, census=row, first_observed_not_peak=True)
                        write(root/'output/resource-first-failure.json', self.first_failure)
            finally:
                self.stop.set()

        def storage(self):
            row = census(root)
            self.maximum = max(self.maximum, row['whole_bytes'])
            if row['errors']:
                self.fail('sampled storage/free limit', row)
            need(self.first_failure is None, 'sticky construction failure')
            return row

        def guard(self):
            need(self.first_failure is None, 'sticky construction failure')
            try:
                # Original resource/process/deadline logic retained. Whole-root
                # storage below removes all excluded_database behavior.
                super().guard()
                need(time.monotonic() < self.deadline, 'construction deadline')
                return self.storage()
            except BaseException:
                self.fail('construction guard failure')
                raise

        def watch(self):
            try:
                while not self.stop.is_set():
                    self.storage()
                    need(time.monotonic() < self.deadline, 'construction deadline')
                    self.stop.wait(.2)
            except BaseException:
                self.fail('construction watcher failure')

        def close(self):
            self.stop.set()
            self.thread.join()

    return WholeRootRunner()


def sdk_identity(r, expected):
    need(expected.get('path') == str(SDK) and SDK.resolve(strict=True) == SDK, 'fixed existing SDK required')
    rows = []
    total = 0
    for directory, dirs, names in os.walk(SDK, followlinks=False):
        r.guard()
        for name in dirs:
            need(not (Path(directory)/name).is_symlink(), 'SDK symlink directory refused')
        for name in names:
            p = Path(directory)/name
            s = p.lstat()
            need(stat.S_ISREG(s.st_mode) and not p.is_symlink(), 'SDK regular files required')
            total += s.st_size
            rows.append(dict(path=p.relative_to(SDK).as_posix(), size=s.st_size, sha256=sha(p)))
            need(len(rows) <= 50000 and total <= GIB, 'bounded SDK identity inspection')
    rows.sort(key=lambda row: row['path'])
    observed = dict(path=str(SDK), files=len(rows), bytes=total,
                    sha256=hashlib.sha256(canonical(rows)).hexdigest())
    need(observed == expected, 'full existing SDK identity differs')
    need((SDK/'VERSION').read_text().splitlines()[0] == 'go1.26.10', 'SDK VERSION differs')
    return observed


def tool_identity(path, expected):
    actual = Path(path).resolve(strict=True)
    s = actual.lstat()
    observed = dict(path=str(actual), bytes=s.st_size, sha256=sha(actual))
    need(stat.S_ISREG(s.st_mode) and observed == expected, 'fixed tool identity differs')
    return str(actual)


def source_identity(source, spec):
    expected = {row['path']: row for row in spec['files']}
    observed = set()
    for directory, dirs, names in os.walk(source, followlinks=False):
        if Path(directory) == source:
            dirs[:] = [name for name in dirs if name != '.git']
        for name in dirs:
            need(not (Path(directory)/name).is_symlink(), 'source directory symlink')
        for name in names:
            p = Path(directory)/name
            rel = p.relative_to(source).as_posix()
            need(rel in expected and not p.is_symlink() and p.is_file(), 'unexpected source file')
            s = p.stat()
            row = expected[rel]
            need(s.st_size == row['bytes'], 'source size differs')
            digest = hashlib.sha1(('blob '+str(s.st_size)+'\0').encode())
            with p.open('rb') as stream:
                for block in iter(lambda: stream.read(1 << 20), b''):
                    digest.update(block)
            need(digest.hexdigest() == row['git_blob'], 'source blob differs')
            need(bool(s.st_mode & 0o111) == (row['mode'] == '100755'), 'source mode differs')
            observed.add(rel)
    need(observed == set(expected), 'incomplete sparse source')


def worker(root, a, started):
    # This point is reached only through the authenticated portable outer guard.
    for name in ('output', 'tmp', 'gocache', 'modcache', 'gopath', 'bin', 'xdg-cache', 'xdg-config'):
        (root/name).mkdir()
    module = load_runner()
    r = whole_runner(module, root, started)
    # Runner.command writes logs at r.out; WholeRootRunner storage always scans root.
    r.out = root/'output'
    error = None
    builds = {}
    token = os.environ.pop('GH_TOKEN', None)
    os.environ.pop('ROOT_ADMISSION_B64', None)
    env = {k: v for k, v in os.environ.items() if k in ('HOME', 'LANG', 'LC_ALL')}
    env['PATH'] = str(SDK/'bin')+':/usr/bin:/bin'
    env.update(GOROOT=str(SDK), GOTOOLCHAIN='local', GOENV='off', GOWORK='off', GOTELEMETRY='off',
               GOCACHE=str(root/'gocache'), GOMODCACHE=str(root/'modcache'), GOPATH=str(root/'gopath'),
               TMPDIR=str(root/'tmp'), GOTMPDIR=str(root/'tmp'), TMP=str(root/'tmp'), TEMP=str(root/'tmp'),
               XDG_CACHE_HOME=str(root/'xdg-cache'), XDG_CONFIG_HOME=str(root/'xdg-config'),
               GIT_CONFIG_NOSYSTEM='1', GIT_CONFIG_GLOBAL=os.devnull, GIT_TERMINAL_PROMPT='0',
               GCM_INTERACTIVE='Never', PYTHONDONTWRITEBYTECODE='1',
               GOMAXPROCS='2', GOMEMLIMIT='4GiB')
    # Preserve actual selected inherited compiler flags, then require ROOT's
    # exact recorded context; unsupported flags are refused rather than erased.
    for key in ('CGO_ENABLED', 'CC', 'CXX', 'GOAMD64', 'CGO_CFLAGS', 'CGO_CPPFLAGS',
                'CGO_CXXFLAGS', 'CGO_LDFLAGS', 'GOFLAGS', 'GOEXPERIMENT'):
        if key in os.environ:
            env[key] = os.environ[key]
    r.env = env
    r.base = root

    def run(name, argv, seconds=30, cwd=None, selected_env=None):
        remaining = r.deadline-time.monotonic()
        need(remaining > 0, 'construction aggregate deadline')
        raw = r.command(name, argv, cwd=cwd or root, env=selected_env or env,
                        timeout=min(seconds, remaining))
        r.guard()  # Normal exit0 does not override an observed sticky failure.
        return raw

    try:
        write(root/'output/admission.json', a)
        write(root/'output/sdk-identity.json', sdk_identity(r, a['sdk']))
        tools = {name: tool_identity(path, a['tools'][name]) for name, path in
                 (('git', '/usr/bin/git'), ('gcc', '/usr/bin/gcc'), ('g++', '/usr/bin/g++'))}
        readonly = a['readonly_runtime_inputs']
        need(isinstance(readonly, list) and 0 < len(readonly) <= 4096,
             'bounded original runtime/tool dependency binding required')
        for row in readonly:
            need(Path(row['path']).is_absolute(), 'absolute read-only runtime input')
            tool_identity(row['path'], row)
        write(root/'output/readonly-runtime-inputs.json', readonly)
        write(root/'output/tools.json', tools)
        go = str(SDK/'bin/go')
        context = json.loads(run('go-context', [go, 'env', '-json', *CONTEXT]))
        write(root/'output/build-context.json', context)
        need(context == a['context'] and context['GOVERSION'] == 'go1.26.10'
             and context['GOOS'] == 'linux' and context['GOARCH'] == 'amd64'
             and context['GOFLAGS'] == context['GOEXPERIMENT'] == ''
             and context['CC'] == 'gcc' and context['CXX'] == 'g++', 'unsupported normal build context')
        git = tools['git']
        descriptions = read(DATA/'owner/overlays.json')
        for arm in ARMS:
            source = root/('source-'+arm)
            spec = read(DATA/(arm+'-source.json'))
            template = root/('git-template-'+arm)
            template.mkdir()
            run(arm+'-init', [git, 'init', '--template='+str(template), str(source)])
            run(arm+'-sparse', [git, '-C', str(source), 'config', 'core.sparseCheckout', 'true'])
            run(arm+'-lf', [git, '-C', str(source), 'config', 'core.autocrlf', 'false'])
            (source/'.git/info').mkdir(parents=True, exist_ok=True)
            (source/'.git/info/sparse-checkout').write_text('/*\n!/docs/evidence/\n!/docs/benchmarks/\n!/TreeDB/docs/evidence/\n!/TreeDB/docs/benchmarks/\n')
            run(arm+'-origin', [git, '-C', str(source), 'remote', 'add', 'origin', 'https://github.com/snissn/gomap.git'])
            authenv = dict(env)
            if token:
                authenv.update(GIT_CONFIG_COUNT='1', GIT_CONFIG_KEY_0='http.https://github.com/.extraheader',
                    GIT_CONFIG_VALUE_0='Authorization: Basic '+base64.b64encode(('x-access-token:'+token).encode()).decode())
            try:
                run(arm+'-fetch', [git, '-C', str(source), '-c', 'protocol.version=2', 'fetch', '--depth=1',
                    '--filter=blob:none', '--no-tags', '--no-recurse-submodules', 'origin', spec['commit']],
                    120, selected_env=authenv)
                log = (r.out/(f'{r.counter:02d}-'+arm+'-fetch.log')).read_text().lower()
                need(not any(s in log for s in ('filtering not recognized', 'does not support filter', 'filter is not supported')),
                     'filter unsupported; no fallback')
                run(arm+'-checkout', [git, '-C', str(source), 'checkout', '--detach', spec['commit']],
                    120, selected_env=authenv)
            finally:
                authenv.clear()
            identity = run(arm+'-identity', [git, '-C', str(source), 'show', '-s', '--format=%H%n%T', 'HEAD']).splitlines()
            need(identity == [spec['commit'], spec['tree']], 'source commit/tree differs')
            source_identity(source, spec)
            mapping = descriptions['primary'][arm]
            need(mapping['source'] == spec['commit'], 'arm mapping source mismatch')
            for name in mapping['Replace']:
                original = DATA/'preimages'/arm/name
                if original.exists():
                    need(sha(source/name) == sha(original), 'overlay preimage differs')
                else:
                    need(not (source/name).exists(), 'new overlay path already exists')
            overlay = root/'output'/('overlay-'+arm+'.json')
            write(overlay, {'Replace': {str(source/k): str(DATA/'owner'/v) for k, v in mapping['Replace'].items()}})
            binary = root/'bin'/(arm+'-primary.test')
            run(arm+'-build', [go, 'test', '-tags=treedb_test', '-p=1', '-mod=readonly', '-c',
                '-overlay='+str(overlay), '-o='+str(binary), './TreeDB/mvcc'], 360, cwd=source)
            s = binary.lstat()
            need(stat.S_ISREG(s.st_mode) and s.st_nlink == 1 and not binary.is_symlink(), 'normal ELF type')
            buildinfo = run(arm+'-buildinfo', [go, 'version', '-m', str(binary)], 30)
            need('go1.26.10' in buildinfo and '-race=true' not in buildinfo, 'normal buildinfo differs')
            source_identity(source, spec)
            builds[arm] = dict(path=str(binary), bytes=s.st_size, sha256=sha(binary), source=spec['commit'],
                               tree=spec['tree'], overlay_sha256=sha(overlay))
        token = None
        r.guard()
    except BaseException as caught:
        error = type(caught).__name__  # Never print credential-bearing exception repr/argv/env.
    finally:
        token = None
        r.close()
        final = census(root)
        if final['errors'] or r.first_failure:
            error = error or 'ResourceFailure'
        terminal = dict(status='CONSTRUCTION_FAILED_NO_CAPTURE' if error else 'TWO_NORMAL_BUILDS_PENDING_OUTER_REVIEW',
                        error_class=error, builds=builds, commands=r.commands, final_census=final,
                        first_failure=r.first_failure, max_sampled_whole_bytes=r.maximum,
                        capture=False, m0=False, release='ONLY_OUTER_GUARD_CAN_PROVE_DESCENDANT_DRAIN')
        write(root/'output/worker-terminal.json', terminal)
        # Final receipt bytes count too; this failure cannot be relabeled success.
        if census(root)['errors']:
            error = error or 'FinalReceiptResourceFailure'
    return 1 if error else 0


def main():
    need(sys.flags.isolated and sys.dont_write_bytecode, 'python3 -I -B required')
    p = argparse.ArgumentParser()
    p.add_argument('mode', choices=('launch', 'worker'))
    args = p.parse_args()
    a = authority()
    authenticate(a)  # Before runner/helper import or any source/build acquisition.
    tool_identity(sys.executable, a['tools']['python'])
    # Explicit ROOT-bound read-only runtime/tool closure authenticates before
    # the first downloaded helper import, not merely before Go execution.
    readonly = a['readonly_runtime_inputs']
    need(isinstance(readonly, list) and 0 < len(readonly) <= 4096, 'bounded runtime input closure')
    for row in readonly:
        need(Path(row['path']).is_absolute(), 'absolute runtime input path')
        tool_identity(row['path'], row)
    for name, path in (('git', '/usr/bin/git'), ('gcc', '/usr/bin/gcc'), ('g++', '/usr/bin/g++')):
        tool_identity(path, a['tools'][name])
    root = root_path()
    started = read(root/'bootstrap-start.json')['monotonic']
    need(time.monotonic()-started < WHOLE, 'construction clock exhausted')
    if args.mode == 'worker':
        return worker(root, a, started)
    need('GOMAXPROCS' not in os.environ, 'outer guard requires absent GOMAXPROCS')
    scripts = HARNESS/'scripts'
    remaining = WHOLE-(time.monotonic()-started)
    cmd = [sys.executable, '-I', '-B', str(scripts/'ycsb_campaign_guard.py'), '--scripts-dir', str(scripts),
           '--out', str(root/'guard'), '--cwd', str(root), '--filesystem', str(accounting_root(root)), '--synthetic',
           '--deadline', str(remaining), '--cleanup-seconds', '10', '--interval', '.2',
           '--rss-cap-bytes', str(24*GIB), '--free-floor-bytes', str(10*GIB), '--',
           sys.executable, '-I', '-B', str(Path(__file__).resolve()), 'worker']
    child = subprocess.Popen(cmd)
    try:
        code = child.wait(timeout=max(1, 900-(time.monotonic()-started)))
    except BaseException:
        child.send_signal(signal.SIGTERM)
        try:
            child.wait(timeout=40)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait(timeout=10)
        raise RuntimeError('outer abnormal exit; physical drain requires independent review')
    result = read(root/'guard/result.json')
    terminal = read(root/'output/worker-terminal.json')
    final = census(root)
    passed = (code == 0 and result['errors'] == [] and result['cleanup_signals'] == []
              and result['status'] == 'guard-completed-pending-independent-artifact-review'
              and terminal['status'] == 'TWO_NORMAL_BUILDS_PENDING_OUTER_REVIEW'
              and not final['errors'])
    write(root/'construction-terminal.json', dict(status='CONSTRUCTION_CLOSED_PENDING_ARTIFACT_REVIEW' if passed else 'CONSTRUCTION_FAILED_NO_CAPTURE',
          guard_exit=code, guard_sha256=sha(root/'guard/result.json'), worker_sha256=sha(root/'output/worker-terminal.json'),
          final_census=final, capture=False, m0=False, whole_interval_verified=False, unperturbed_timing_verified=False))
    passed = passed and not census(root)['errors']
    print(json.dumps(dict(status='PENDING_REVIEW' if passed else 'FAILED', capture=False,
                         terminal_sha256=sha(root/'construction-terminal.json'))), flush=True)
    return 0 if passed else 1


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as error:
        print(json.dumps({'status': 'FAILED_NO_CAPTURE', 'error_class': type(error).__name__}), flush=True)
        sys.exit(1)
