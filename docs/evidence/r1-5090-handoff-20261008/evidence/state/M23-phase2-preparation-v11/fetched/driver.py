"""Prepared-only R1 functional runner; executable only with a frozen single-use grant."""
from pathlib import Path
import collections, ctypes, datetime, fcntl, hashlib, json, os, re
import shutil, signal, subprocess, sys, time

if sys.flags.optimize:
    raise RuntimeError('Python optimization disables required guards')

R = Path('/home/mikers/dev/codex-r1-5090-functional-linux111-20261008')
W = R / 'source/M23-phase2-v11'
D = R / 'state/M23-phase2-functional-v11'
G = R / 'toolchain/go1.26.3'
sha = lambda p: hashlib.sha256(Path(p).read_bytes()).hexdigest()
now = lambda: datetime.datetime.now(datetime.timezone.utc).isoformat()
read = lambda p: json.loads(Path(p).read_text())

def save(path, obj):
    path = Path(path)
    tmp = path.with_name(path.name + '.tmp-' + str(os.getpid()))
    with tmp.open('x') as f:
        f.write(json.dumps(obj, indent=2, sort_keys=True) + '\n')
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)

def emit(name, obj):
    save(D / name, obj)

def source_snapshot():
    assert W.is_dir() and not (W / '.git').exists()
    assert not any(p.is_symlink() for p in W.rglob('*')), 'source symlink unsupported'
    return {str(p.relative_to(W)): sha(p) for p in sorted(W.rglob('*')) if p.is_file()}

def interrupt(signum, frame):
    raise InterruptedError('driver signal ' + str(signum))

signal.signal(signal.SIGTERM, interrupt)
libc = ctypes.CDLL(None, use_errno=True)
assert os.getppid() != 1 and libc.prctl(36, 1, 0, 0, 0) == 0
assert libc.prctl(1, signal.SIGTERM, 0, 0, 0) == 0 and os.getppid() != 1
lock = (R / 'state/reservation.lock').open('a')
fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
auth = read(D / 'authorization.json')
auth_hash = sha(D / 'authorization.json')
spec = read(D / 'run-spec.json')
source = read(D / 'source.json')
assert auth['authorized'] is True and sha(__file__) == auth['driver_sha256']
assert sha(D / 'run-spec.json') == auth['spec_sha256']
assert sha(D / 'source.json') == auth['source_manifest_sha256']
assert auth['accepted_toolchain'] == 'exact pinned go1.26.3 linux/amd64; functional only'
assert datetime.datetime.now(datetime.timezone.utc) < datetime.datetime.fromisoformat(auth['expires_utc'])
assert source_snapshot() == source
pinned_toolchain_source = read(D / 'pinned-toolchain-source.json')
assert not any(p.is_symlink() for p in G.rglob('*'))
assert {str(p.relative_to(G)): sha(p) for p in sorted(G.rglob('*')) if p.is_file()} == pinned_toolchain_source
assert spec['platform'] == 'linux/amd64' and spec['allowed_skips'] == []
assert spec['finite'] is False and spec['performance'] is False
assert 0 < auth['total_seconds'] <= 2400
immutable = auth['immutable_bindings']
assert immutable and all(sha(p) == h for p, h in immutable.items())
reservation_before = read(R / 'state/reservation.json')
assert reservation_before['owner'] == 'R1 #5090 ROOT'
assert reservation_before['status'] == 'granted_single_use_v2'
assert reservation_before['token'] == auth['token']
assert reservation_before['authorization_sha256'] == auth_hash
assert reservation_before['prior_custody'] == auth['prior_custody']
assert auth['prior_custody']['all_processes_joined'] is True
assert auth['prior_custody']['ROOT_audited'] is True
assert auth['prior_custody']['runtime_never_started'] is True or auth['prior_custody'].get('prior_runtime_receipt_sha256')
used = R / 'state' / ('used-grant-' + auth['token'] + '.json')
assert re.fullmatch('[a-zA-Z0-9_-]+', auth['token'])
with used.open('x') as f:
    f.write(json.dumps(dict(at=now(), authorization_sha256=auth_hash, driver_pid=os.getpid())) + '\n')
    f.flush()
    os.fsync(f.fileno())
save(R / 'state/reservation.json', dict(owner='R1 #5090 ROOT', status='active_v2', token=auth['token'], authorization_sha256=auth_hash, driver_pid=os.getpid(), prior_custody=auth['prior_custody']))

# Deliberately do not inherit host Go/compiler flags, target overrides, or proxies.
env = dict(PATH=str(G / 'bin') + ':/usr/bin:/bin', HOME='/home/mikers', USER='mikers', LOGNAME='mikers', LANG='C.UTF-8', LC_ALL='C.UTF-8',
           GOROOT=str(G), GOTOOLCHAIN='local', GOWORK='off', GOENV='off', GOMAXPROCS='4', GOFLAGS='-mod=readonly',
           GOOS='linux', GOARCH='amd64', GOAMD64='v1', GOEXPERIMENT='', GODEBUG='', CGO_ENABLED='1',
           CC='/usr/bin/gcc', CXX='/usr/bin/g++', CGO_CFLAGS='-O2 -g', CGO_CPPFLAGS='', CGO_CXXFLAGS='-O2 -g', CGO_FFLAGS='-O2 -g', CGO_LDFLAGS='-O2 -g',
           GOCACHE=str(R / 'cache/go-build'), GOMODCACHE=str(R / 'cache/go-mod'), GOPATH=str(R / 'cache/go-path'), TMPDIR=str(R / 'tmp'),
           GOPROXY='https://proxy.golang.org,direct', GOSUMDB='sum.golang.org', GOPRIVATE='', GONOPROXY='', GONOSUMDB='', GOINSECURE='',
           GIT_TERMINAL_PROMPT='0')
assert env == spec['environment'], 'frozen environment mismatch'
tools = {str(p): sha(p) for p in [G / 'bin/go', G / 'bin/gofmt', *sorted((G / 'pkg/tool/linux_amd64').iterdir())] if p.is_file()}
tools.update(spec['system_tools'])
assert tools[str(G / 'bin/go')] == 'd68b7abbc40d0844f673f6cf06ae3cded225c50437c6454fa37ef178d079fe65'
assert all(Path(p).is_file() and sha(p) == h for p, h in tools.items())
assert all(str(Path(p).resolve()) == q for p, q in spec['symlink_bindings'].items())
test2json = D / 'tools/test2json'
emit('environment.json', env)
emit('actual-tools.json', dict(files=tools, symlinks=spec['symlink_bindings'], toolchain_acceptance=auth['accepted_toolchain'], system_header_closure_certified=False))
commands, inputs, binaries, inventories = [], {}, {}, {}
active, joined, failure = None, True, None
started = time.monotonic()
global_end = started + auth['total_seconds']

def custody():
    emit('custody.json', dict(at=now(), driver_pid=os.getpid(), active=active, all_processes_joined=joined, phases=commands))

def resource_check():
    assert shutil.disk_usage(R).free >= auth['reserve_bytes'], 'functional host free-space floor'
    used_bytes = sum(p.stat().st_size for p in R.rglob('*') if p.is_file())
    assert used_bytes <= auth['sampled_logical_budget_bytes'], 'sampled attributable logical budget'
    return used_bytes

def check():
    assert source_snapshot() == source, 'source drift'
    assert sha(__file__) == auth['driver_sha256'] and sha(D / 'authorization.json') == auth_hash
    assert sha(D / 'run-spec.json') == auth['spec_sha256'] and sha(D / 'source.json') == auth['source_manifest_sha256']
    assert all(sha(p) == h for p, h in immutable.items()), 'immutable custody/format/stage/preparation proof drift'
    assert all(sha(p) == h for p, h in tools.items()), 'tool drift'
    assert all(str(Path(p).resolve()) == q for p, q in spec['symlink_bindings'].items()), 'tool link drift'
    assert all(sha(p) == h for p, h in inputs.items()), 'selected compiler input drift'
    assert all(sha(p) == h for p, h in binaries.items()), 'preserved executable drift'
    resource_check()

def run(name, argv, seconds, cwd=W):
    global active, joined
    check()
    estimated_growth = auth['estimated_new_phase_growth_bytes']
    assert shutil.disk_usage(R).free >= auth['reserve_bytes'] + estimated_growth, 'estimated phase growth below host reserve'
    assert resource_check() + estimated_growth <= auth['sampled_logical_budget_bytes'], 'estimated phase growth exceeds own budget'
    assert time.monotonic() < global_end, 'total campaign deadline'
    joined = False
    active = dict(name=name, pid=None, pgid=None, start=now(), launch_custody='unresolved')
    custody()
    log = D / (name + '.log')
    adopted, error, timed_out, survived, group_absent, adopted_joined = [], None, False, False, False, False
    highwater = resource_check()
    stderr_log = D / (name + '.stderr.log')
    with log.open('xb') as out, stderr_log.open('xb') as errout:
        p = subprocess.Popen(list(map(str, argv)), cwd=cwd, env=env, stdout=out, stderr=errout, start_new_session=True)
        active.update(pid=p.pid, pgid=p.pid, launch_custody='owned')
        custody()
        print('START', name, p.pid, flush=True)
        end = min(global_end, time.monotonic() + seconds)
        next_resource = time.monotonic() + 2
        try:
            while p.poll() is None:
                if time.monotonic() >= end:
                    timed_out = True
                    raise TimeoutError(name)
                if time.monotonic() >= next_resource:
                    highwater = max(highwater, resource_check())
                    next_resource = time.monotonic() + 2
                time.sleep(.05)
            p.wait()
        except BaseException as e:
            error = repr(e)
        finally:
            if p.poll() is None:
                try:
                    os.killpg(p.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    p.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(p.pid, signal.SIGKILL)
                    p.wait()
            else:
                p.wait()
            try:
                os.killpg(p.pid, 0)
                survived = True
            except ProcessLookupError:
                pass
            if survived:
                try:
                    os.killpg(p.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            limit = time.monotonic() + 15
            while True:
                try:
                    child_pid, status = os.waitpid(-1, os.WNOHANG)
                except ChildProcessError:
                    adopted_joined = True
                    break
                if child_pid:
                    adopted.append(dict(pid=child_pid, wait_status=status, actual_waitpid=True))
                    continue
                if time.monotonic() >= limit:
                    error = (error or '') + '; adopted descendants remain unjoined; custody retained'
                    break
                time.sleep(.05)
            try:
                os.killpg(p.pid, 0)
            except ProcessLookupError:
                group_absent = True
            joined = group_absent and adopted_joined
            rec = dict(**active, end=now(), cwd=str(cwd), argv=list(map(str, argv)), exit=p.returncode, actual_wait=True,
                       group_absent=group_absent, unexpected_group_survived_parent=survived, adopted_descendants=adopted,
                       adopted_actual_wait_complete=adopted_joined, exception=error, timeout=timed_out,
                       attributable_sampled_highwater=highwater, raw_sha256=sha(log), stderr_sha256=sha(stderr_log), separate_stdout_stderr=True)
            commands.append(rec)
            active = None if joined else active
            emit('commands.json', commands)
            custody()
            print('END', name, p.returncode, flush=True)
    assert p.returncode == 0 and not error and not timed_out and not survived and joined and all(x['wait_status'] == 0 for x in adopted), rec
    check()
    return log

def decode_objects(path):
    text, off, objects = path.read_text(), 0, []
    decoder = json.JSONDecoder()
    while off < len(text):
        while off < len(text) and text[off].isspace():
            off += 1
        if off == len(text):
            break
        obj, off = decoder.raw_decode(text, off)
        objects.append(obj)
    return objects

def capture_inputs(raw, flavor):
    files, expected, selected_dirs, special = {}, [], {}, []
    pkg_paths = {p['import_path'] for p in spec['packages']}
    regular_fields = ['GoFiles', 'CompiledGoFiles', 'CgoFiles', 'CFiles', 'CXXFiles', 'MFiles', 'HFiles', 'FFiles', 'SFiles', 'SwigFiles', 'SwigCXXFiles', 'SysoFiles', 'EmbedFiles']
    test_fields = ['TestGoFiles', 'XTestGoFiles', 'TestEmbedFiles', 'XTestEmbedFiles']
    for obj in decode_objects(raw):
        base = Path(obj.get('Dir', ''))
        selected = obj.get('ImportPath') in pkg_paths
        for field in regular_fields + (test_fields if selected else []):
            for name in obj.get(field, []):
                p = Path(name)
                p = p if p.is_absolute() else base / p
                assert p.is_file(), (p, field)
                files[str(p)] = sha(p)
        for module in [obj.get('Module'), (obj.get('Module') or {}).get('Replace')]:
            if module and module.get('GoMod'):
                p = Path(module['GoMod'])
                files[str(p)] = sha(p)
                if p.with_name('go.sum').is_file():
                    files[str(p.with_name('go.sum'))] = sha(p.with_name('go.sum'))
        if selected:
            selected_dirs[obj['ImportPath']] = str(base)
            for field in ['TestGoFiles', 'XTestGoFiles']:
                for name in obj.get(field, []):
                    p = base / name
                    for test in re.findall(r'(?m)^func\s+((?:Test|Fuzz)\w+)\s*\(', p.read_text()):
                        if test == 'TestMain' or test.startswith('Fuzz'):
                            special.append(dict(package=obj['ImportPath'], name=test, source=str(p), sha256=sha(p)))
                        elif re.fullmatch(spec['regex'], test):
                            expected.append((obj['ImportPath'], test))
    assert len(expected) == len(set(expected)), 'duplicate source test declarations'
    assert len(expected) >= spec['minimum_expected']
    assert sorted(expected) == [tuple(x) for x in spec['expected_tests']], 'frozen package-qualified expected inventory mismatch'
    assert sorted(special, key=lambda x: (x['package'], x['name'], x['source'])) == spec['special_test_inventory'], 'TestMain/Fuzz audit mismatch'
    assert set(selected_dirs) == pkg_paths
    assert all(selected_dirs[p['import_path']] == str(W / p['relative_dir']) for p in spec['packages'])
    emit(flavor + '-inputs.json', files)
    emit(flavor + '-expected.json', dict(expected=sorted(expected), special=sorted(special, key=lambda x: (x['package'], x['name'], x['source'])), selected_dirs=selected_dirs,
                                       unselected_dependency_test_fields_excluded=True, compiler_generated_inputs_and_stdlib_included=True))
    inputs.update(files)
    return set(expected)

def capture_tool_inputs(raw):
    files = {}
    fields = ['GoFiles', 'CompiledGoFiles', 'CgoFiles', 'CFiles', 'CXXFiles', 'MFiles', 'HFiles', 'FFiles', 'SFiles', 'SwigFiles', 'SwigCXXFiles', 'SysoFiles', 'EmbedFiles']
    objects = decode_objects(raw)
    assert any(obj.get('ImportPath') == 'cmd/test2json' for obj in objects)
    for obj in objects:
        base = Path(obj.get('Dir', ''))
        for field in fields:
            for name in obj.get(field, []):
                p = Path(name)
                p = p if p.is_absolute() else base / p
                assert p.is_file(), (p, field)
                files[str(p)] = sha(p)
        for module in [obj.get('Module'), (obj.get('Module') or {}).get('Replace')]:
            if module and module.get('GoMod'):
                p = Path(module['GoMod'])
                files[str(p)] = sha(p)
                if p.with_name('go.sum').is_file():
                    files[str(p.with_name('go.sum'))] = sha(p.with_name('go.sum'))
    assert files
    emit('test2json-compiler-inputs.json', files)
    inputs.update(files)

def events(raw):
    result = []
    for line in raw.read_text().splitlines():
        assert line.strip(), 'blank non-JSON event'
        event = json.loads(line)
        assert isinstance(event, dict) and event.get('Action'), 'invalid test2json event'
        result.append(event)
    return result

def validate_test_events(raw, pkg, expected):
    data = events(raw)
    assert all(e.get('Package') == pkg for e in data), 'foreign test package'
    terminal = [e for e in data if e.get('Action') in ('pass', 'skip', 'fail')]
    top = [e for e in terminal if e.get('Test') and '/' not in e['Test']]
    top_keys = [(e['Package'], e['Test']) for e in top]
    assert set(top_keys) == expected and len(top_keys) == len(expected), 'missing or duplicate terminal outcome'
    starts = collections.Counter((e['Package'], e['Test']) for e in data if e.get('Action') == 'run' and e.get('Test') and '/' not in e['Test'])
    assert starts == collections.Counter({key: 1 for key in expected}), 'missing or duplicate top run'
    named_terminals = collections.Counter((e['Package'], e['Test']) for e in terminal if e.get('Test'))
    assert all(n == 1 for n in named_terminals.values()), 'duplicate named terminal outcome'
    named_runs = collections.Counter((e['Package'], e['Test']) for e in data if e.get('Action') == 'run' and e.get('Test'))
    assert named_runs == named_terminals, 'missing or duplicate named run/terminal event'
    package_terminal = [e for e in terminal if not e.get('Test')]
    assert len(package_terminal) == 1 and package_terminal[0]['Action'] == 'pass'
    assert not any(e['Action'] in ('skip', 'fail') for e in data)
    assert not any(e.get('Test', '').startswith('Fuzz') or e.get('Test') == 'TestMain' for e in data)
    return dict(package=pkg, top_pass=len(top), named_pass=len(named_terminals), top_tests=sorted(top_keys), skips=0, fails=0, duplicate_outcomes=0)

try:
    custody()
    resource_check()
    raw = run('version', [G / 'bin/go', 'version'], 30)
    assert raw.read_text().strip() == 'go version go1.26.3 linux/amd64'
    (D / 'tools').mkdir(exist_ok=False)
    raw = run('test2json-inputs', [G / 'bin/go', 'list', '-compiled', '-deps', '-json', 'cmd/test2json'], 300)
    capture_tool_inputs(raw)
    run('test2json-compile', [G / 'bin/go', 'build', '-x', '-o', test2json, 'cmd/test2json'], 300)
    with test2json.open('rb') as f:
        assert f.read(4) == b'\x7fELF'
    binaries[str(test2json)] = sha(test2json)
    tools[str(test2json)] = sha(test2json)
    emit('preserved-binaries.json', binaries)
    emit('actual-tools.json', dict(files=tools, symlinks=spec['symlink_bindings'], toolchain_acceptance=auth['accepted_toolchain'], system_header_closure_certified=False, test2json_explicitly_built_and_preserved=True))
    run('test2json-buildinfo', [G / 'bin/go', 'version', '-m', test2json], 30)
    for flavor in ['normal', 'race']:
        flags = ['-race'] if flavor == 'race' else []
        args = [p['go_argument'] for p in spec['packages']]
        raw = run(flavor + '-inputs', [G / 'bin/go', 'list', '-compiled', '-deps', '-test', *flags, '-json', *args], 300)
        expected = capture_inputs(raw, flavor)
        package_results = []
        bindir = D / 'binaries' / flavor
        bindir.mkdir(parents=True, exist_ok=False)
        for pkg in spec['packages']:
            tag = flavor + '-' + pkg['tag']
            binary = bindir / (pkg['tag'] + '.test')
            run(tag + '-compile', [G / 'bin/go', 'test', '-c', '-x', *flags, '-o', binary, pkg['go_argument']], 300)
            assert binary.is_file() and binary.read_bytes()[:4] == b'\x7fELF'
            binaries[str(binary)] = sha(binary)
            emit('preserved-binaries.json', binaries)
            run(tag + '-buildinfo', [G / 'bin/go', 'version', '-m', binary], 30)
            raw = run(tag + '-list', [test2json, '-p', pkg['import_path'], binary, '-test.list=' + spec['regex']], 30, W / pkg['relative_dir'])
            listed = [e['Output'].strip() for e in events(raw) if e.get('Action') == 'output' and re.fullmatch(spec['regex'], e.get('Output', '').strip())]
            pkg_expected = {x for x in expected if x[0] == pkg['import_path']}
            assert collections.Counter(listed) == collections.Counter({x[1]: 1 for x in pkg_expected}), 'binary/source list mismatch'
            raw = run(tag + '-functional', [test2json, '-p', pkg['import_path'], binary, '-test.v=test2json', '-test.run=' + spec['regex'], '-test.count=1', '-test.timeout=600s'], 720, W / pkg['relative_dir'])
            package_results.append(validate_test_events(raw, pkg['import_path'], pkg_expected))
        passed = {tuple(t) for p in package_results for t in p['top_tests']}
        assert passed == expected and all(tuple(risk) in passed for risk in spec['mandatory_risk_tests'])
        inventories[flavor] = dict(packages=package_results, top_pass=sum(p['top_pass'] for p in package_results), named_pass=sum(p['named_pass'] for p in package_results), skips=0, fails=0, duplicate_outcomes=0)
        emit(flavor + '-inventory.json', inventories[flavor])
        check()
except BaseException as exc:
    failure = repr(exc)
    print('FAILURE', failure, flush=True)
finally:
    audit_error = None
    try:
        check()
    except BaseException as exc:
        audit_error = repr(exc)
    success = failure is None and audit_error is None and joined and set(inventories) == {'normal', 'race'}
    receipt = dict(at=now(), status='PASS_SCOPED_LINUX111_M23_FUNCTIONAL_NORMAL_RACE' if success else 'FAILED_OR_INCOMPLETE', failure=failure,
                   final_audit_error=audit_error, all_processes_joined=joined, active=active, commands=commands, inventories=inventories,
                   source_paths=len(source), source_manifest_sha256=auth['source_manifest_sha256'], authorization_sha256=auth_hash,
                   driver_sha256=sha(__file__), preserved_binaries=binaries, actual_compiler_inputs=len(inputs), toolchain_acceptance=auth['accepted_toolchain'],
                   protected_native_paths_selected=False, private_toolchain_and_caches=True, global_untouched_proof=False, system_header_closure_certified=False,
                   resource_policy='sampled logical byte budget and host free floor; not kernel quota or peak certificate', finite=False, performance=False,
                   outer_SSH_and_driver_join_requires_ROOT_audit=True)
    emit('receipt.json', receipt)
    custody()
    if joined:
        save(R / 'state/reservation.json', dict(owner='R1 #5090 ROOT', status='joined_pending_ROOT_audit_v2', token=auth['token'],
             authorization_sha256=auth_hash, receipt_sha256=sha(D / 'receipt.json'), all_remote_phases_joined=True, outer_join_audited=False))
    print('RECEIPT', sha(D / 'receipt.json'), receipt['status'], flush=True)
sys.exit(0 if success else 1)
