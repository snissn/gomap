#!/usr/bin/env python3
"""Bootstrap the fixed, separately reviewed fourteen-call hosted diagnostic."""
from pathlib import Path
import datetime
import contextlib
import hashlib
import importlib.util
import json
import os
import runpy
import signal
import subprocess
import sys
import time
import traceback

BASELINE = '5bca87ffe6e1bfba2d693264d2dc82936adc234f'
CANDIDATE = '5027dc559f83dee04317910169c48d277fcce72f'
COMPILER_SHA256 = 'd68b7abbc40d0844f673f6cf06ae3cded225c50437c6454fa37ef178d079fe65'
BRANCH = 'refs/heads/codex/4871-maintenance-diagnostic-185-20261015'


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate_context(env):
    if (env.get('GITHUB_ACTIONS') != 'true' or env.get('GITHUB_EVENT_NAME') != 'workflow_dispatch'
            or env.get('GITHUB_REF') != BRANCH or env.get('GITHUB_REPOSITORY') != 'snissn/gomap'):
        raise ValueError('only the separately reviewed manual diagnostic branch may execute')
    if env.get('RUNNER_OS') != 'Linux' or env.get('RUNNER_ARCH') != 'X64':
        raise ValueError('Linux amd64 runner required')
    if not env.get('RUNNER_TEMP') or not env.get('GITHUB_WORKSPACE'):
        raise ValueError('ephemeral runner paths missing')


def main():
    validate_context(os.environ)
    harness = Path(os.environ['GITHUB_WORKSPACE']).resolve()
    root = Path(os.environ['RUNNER_TEMP']).resolve() / 'mvcc-maintenance-5027d'
    root.mkdir()  # A repeated attempt must never overwrite prior evidence.
    (root / 'tmp').mkdir()
    commands = []
    bootstrap_env = dict(os.environ, TMPDIR=str(root / 'tmp'), GOTOOLCHAIN='local', GOENV='off', GOWORK='off')

    def save(name, value):
        (root / name).write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')

    def command(label, argv, cwd=harness, env=None, timeout=120):
        path = root / (label + '.log')
        started = time.monotonic()
        record = {'label': label, 'argv': [str(x) for x in argv], 'cwd': str(cwd),
                  'timeout_s': timeout, 'log': path.name, 'started_utc': datetime.datetime.now(datetime.timezone.utc).isoformat()}
        commands.append(record)
        save('bootstrap-commands.json', commands)
        interrupted = None
        with path.open('wb') as stream:
            process = subprocess.Popen(record['argv'], cwd=cwd, env=bootstrap_env if env is None else env,
                                       stdout=stream, stderr=subprocess.STDOUT, start_new_session=True)
            try:
                code = process.wait(timeout=timeout)
            except BaseException as error:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait()
                code = 124 if isinstance(error, subprocess.TimeoutExpired) else 125
                interrupted = repr(error)
        record.update(exit=code, elapsed_s=time.monotonic() - started,
                      log_sha256=digest(path), interrupted=interrupted)
        save('bootstrap-commands.json', commands)
        if code:
            raise RuntimeError(f'{label} failed: exit {code}; see {path.name}')
        return path.read_text()

    def actual_sources(name):
        available = {}
        for label, directory in [('baseline', 'coordinator'), ('candidate', 'ordinary-repair')]:
            source = root / directory
            if not (source / '.git').is_file():
                continue
            def git(*args):
                return subprocess.check_output(['git', *args], cwd=source, text=True).strip()
            files = git('ls-files', '--cached', '--others', '--exclude-standard').splitlines()
            available[label] = {'root': str(source), 'head': git('rev-parse', 'HEAD'),
                'tree': git('rev-parse', 'HEAD^{tree}'), 'status': git('status', '--porcelain'),
                'files': {f: digest(source / f) for f in files if (source / f).is_file()}}
        save(name, available)

    phase = 'bootstrap'
    try:
        save('infrastructure.json', {
            'scope': 'separately granted bounded premerge diagnostic; no qualification claim',
            'runner': {k: os.environ.get(k) for k in (
                'GITHUB_RUN_ID', 'GITHUB_RUN_ATTEMPT', 'GITHUB_WORKFLOW_REF', 'GITHUB_WORKFLOW_SHA',
                'GITHUB_SHA', 'GITHUB_REF', 'GITHUB_EVENT_NAME', 'GITHUB_REPOSITORY',
                'RUNNER_NAME', 'RUNNER_OS', 'RUNNER_ARCH', 'ImageOS', 'ImageVersion')},
            'persistent_runner_available': False, 'persistent_task_cache_available': False,
            'actions_cache_used': False, 'fresh_task_build_and_module_caches': True,
            'toolchain_origin': 'pinned setup-go action/tool cache on ephemeral hosted image; exact executable hash enforced',
            'baseline': BASELINE, 'candidate': CANDIDATE,
            'runner_temp': str(root), 'affinity_required': [0, 1], 'python': sys.version,
            'local_host185_collection': 'REFUSED before any benchmark; external processes exceeded2%; executed0',
            'review_and_root_grant': 'external prerequisites to manual dispatch; this receipt does not assert review acceptance'})
        head = command('harness-head', ['git', 'rev-parse', 'HEAD']).strip()
        assert head == os.environ['GITHUB_SHA']
        assert not command('harness-clean', ['git', 'status', '--porcelain']).strip()
        paths = [Path('.github/workflows/treedb-vacuum-m0.yml')]
        paths += [p.relative_to(harness) for p in sorted((harness / '.github/scripts').glob('mvcc_maintenance_*')) if p.is_file()]
        saved = {}
        for path in paths:
            dest = root / 'harness' / path
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes((harness / path).read_bytes())
            saved[str(path)] = digest(dest)
        save('harness-binding.json', {'head': head, 'files': saved})
        go = Path(command('go-path', ['which', 'go']).strip()).resolve()
        assert digest(go) == COMPILER_SHA256
        goroot = Path(command('go-root', [go, 'env', 'GOROOT']).strip()).resolve()
        assert go == (goroot / 'bin/go').resolve()
        (root / 'toolchains').mkdir()
        (root / 'toolchains/go1.26.3').symlink_to(goroot, target_is_directory=True)
        (root / 'compiler').mkdir()
        for relative in ['bin/go', 'pkg/tool/linux_amd64/compile', 'pkg/tool/linux_amd64/link']:
            original = goroot / relative
            retained = root / 'compiler' / Path(relative).name
            retained.write_bytes(original.read_bytes())
        save('compiler.json', {'goroot': str(goroot), 'go_sha256': digest(go),
             'retained': {p.name: digest(p) for p in (root / 'compiler').iterdir()}})
        command('go-version', [go, 'version'])
        command('go-environment', [go, 'env', '-json'])
        command('c-compiler', ['gcc', '--version'])
        command('system-linker', ['ld', '--version'])
        command('toolchain-archive', ['tar', '-czf', root / 'go1.26.3-linux-amd64.tar.gz', '-C', goroot, '.'], timeout=300)
        save('toolchain-archive.json', {'sha256': digest(root / 'go1.26.3-linux-amd64.tar.gz')})
        command('host', ['uname', '-a'])
        command('cpu', ['lscpu'])
        command('disk', ['df', '-PT', root])
        command('memory', ['free', '-b'])
        command('setup-processes', ['ps', '-eo', 'pid,ppid,etime,pcpu,args'])
        (root / 'ordinary-work/cache').mkdir(parents=True)
        (root / 'ordinary-work/modcache').mkdir()
        (root / 'performance-185').mkdir()
        deps_env = dict(os.environ, GOROOT=str(goroot), GOTOOLCHAIN='local', GOENV='off',
                        GOWORK='off', GOFLAGS='', GOAMD64='v1', CGO_ENABLED='1',
                        GOMAXPROCS='2', GOGC='100', GODEBUG='', GOMEMLIMIT='8GiB',
                        GOCACHE=str(root / 'ordinary-work/cache'),
                        GOMODCACHE=str(root / 'ordinary-work/modcache'), TMPDIR=str(root / 'tmp'),
                        GOPROXY='https://proxy.golang.org,direct', GOSUMDB='sum.golang.org')
        save('dependency-environment.json', {k: deps_env[k] for k in (
            'GOROOT', 'GOTOOLCHAIN', 'GOENV', 'GOWORK', 'GOFLAGS', 'GOAMD64', 'CGO_ENABLED',
            'GOMAXPROCS', 'GOGC', 'GODEBUG', 'GOMEMLIMIT', 'GOCACHE', 'GOMODCACHE', 'TMPDIR', 'GOPROXY', 'GOSUMDB')})
        phase = 'dependency-preparation'
        for label, directory, sha in [('baseline', 'coordinator', BASELINE), ('candidate', 'ordinary-repair', CANDIDATE)]:
            source = root / directory
            command(label + '-worktree', ['git', 'worktree', 'add', '--detach', source, sha])
            assert command(label + '-head', ['git', 'rev-parse', 'HEAD'], cwd=source).strip() == sha
            assert not command(label + '-clean-before-deps', ['git', 'status', '--porcelain'], cwd=source).strip()
        actual_sources('sources-before-dependencies.json')
        for label, directory in [('baseline', 'coordinator'), ('candidate', 'ordinary-repair')]:
            source = root / directory
            command(label + '-download', [go, 'mod', 'download'], cwd=source, env=deps_env, timeout=600)
            # Inventory the actual compiled packages and their test dependencies.
            # A complete module graph also traverses unrelated dependency tools
            # whose local replacement directives do not apply in this module.
            packages = [go, 'list', '-mod=readonly', '-deps', '-test', '-json', './TreeDB', './TreeDB/db']
            command(label + '-packages-prefill', packages,
                    cwd=source, env=deps_env, timeout=600)
            offline = dict(deps_env, GOPROXY='off', GOSUMDB='off')
            command(label + '-verify', [go, 'mod', 'verify'], cwd=source, env=offline)
            command(label + '-packages-offline', packages, cwd=source, env=offline, timeout=600)
            assert not command(label + '-clean-after-deps', ['git', 'status', '--porcelain'], cwd=source).strip()
        actual_sources('sources-after-dependencies.json')
        scripts = harness / '.github/scripts'
        collect = scripts / 'mvcc_maintenance_collect.py'
        spec = importlib.util.spec_from_file_location('fixed_maintenance_collector', collect)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        save('authorization.json', {'runner_sha256': digest(collect), 'schedule': module.schedule(),
             'runtime_authorized': True, 'grant_source': 'manual workflow_dispatch of reviewed diagnostic branch',
             'maximum_benchmark_calls': 14, 'maximum_benchmark_child_seconds': 14 * 150,
             'run_id': os.environ['GITHUB_RUN_ID'], 'run_attempt': os.environ['GITHUB_RUN_ATTEMPT'],
             'harness_head': head, 'note': 'not an independent review verdict or permission to retry'})
        # Run the fixed reviewed drivers in this process: their original child
        # caps own each Go process group, without a second supervisor killing a
        # driver before it can terminate/join its separately grouped child.
        phase_receipts = []
        def driver(label, script, operation):
            record = {'phase': label, 'script': str(script), 'sha256': digest(script)}
            phase_receipts.append(record)
            save('driver-commands.json', phase_receipts)
            start = time.monotonic()
            log = root / (label + '.log')
            try:
                with log.open('w') as stream, contextlib.redirect_stdout(stream), contextlib.redirect_stderr(stream):
                    code = operation()
                record['exit'] = 0 if code is None else code
            except BaseException as error:
                record.update(exit=1, interrupted=repr(error))
                raise
            finally:
                record.update(elapsed_s=time.monotonic() - start, log=log.name, log_sha256=digest(log))
                save('driver-commands.json', phase_receipts)
            if record['exit']:
                raise RuntimeError(f"{label} refused or failed: {record['exit']}")
        phase = 'compile-only'
        prepare = scripts / 'mvcc_maintenance_prepare.py'
        driver('prepare', prepare, lambda: (runpy.run_path(str(prepare), run_name='__main__'), None)[1])
        phase = 'bounded-collection'
        driver('collect', collect, module.main)
        save('hosted-result.json', {'status': 'COMPLETE_BOUNDED_DIAGNOSTIC', 'timings_qualify': False,
             'result': 'performance-185/maintenance-bounded-02/result.json'})
        return 0
    except BaseException as error:
        save('hosted-result.json', {'status': 'FAILED_OR_REFUSED', 'phase': phase,
             'error': repr(error), 'traceback': traceback.format_exc(), 'timings_qualify': False,
             'available_artifacts_retained': True})
        raise
    finally:
        try:
            actual_sources('sources-final-available.json')
        except BaseException as error:
            save('final-source-inventory-error.json', {'error': repr(error)})


if __name__ == '__main__':
    assert not sys.argv[1:]
    sys.dont_write_bytecode = True
    main()
