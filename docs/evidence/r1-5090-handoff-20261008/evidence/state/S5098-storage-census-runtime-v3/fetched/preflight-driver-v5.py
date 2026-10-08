"""Prepared-only R1 functional runner; executable only with a frozen single-use grant."""
from pathlib import Path
import collections, ctypes, datetime, fcntl, hashlib, json, os, re
import shutil, signal, subprocess, sys, time

if sys.flags.optimize:
    raise RuntimeError('Python optimization disables required guards')

R = Path('/home/mikers/dev/codex-r1-5090-functional-linux111-20261008')
NS = R / 'S5098-preflight-v2'
W = NS / 'source-census-v3'
D = NS / 'run-census-v3'
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
assert 0 < auth['total_seconds'] <= 9000
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
           GOCACHE=str(NS / 'cache/go-build'), GOMODCACHE=str(NS / 'cache/go-mod'), GOPATH=str(NS / 'cache/go-path'), TMPDIR=str(NS / 'tmp'),
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
control_binaries, preflight_result, database_result = {}, None, None
active, joined, failure = None, True, None
started = time.monotonic()
global_end = started + auth['total_seconds']

def custody():
    emit('custody.json', dict(at=now(), driver_pid=os.getpid(), active=active, all_processes_joined=joined, phases=commands))

resource = read(D / 'resource-plan.json')['resource']
assert resource['unrelated_free_floor_bytes'] == 160*1024**3
assert resource['S_namespace_allocated_and_apparent_limit_bytes'] == 12*1024**3
assert resource['total_attributable_R_logical_limit_bytes'] == 40*1024**3
assert resource['S_fixture_DB_all_files_including_WAL_limit_bytes'] == 6*1024**3
assert resource['S_source_tools_caches_logs_combined_limit_bytes'] == 4*1024**3
assert resource['shutdown_and_unmeasured_growth_slack_bytes'] == 2*1024**3
resource_samples = []

def byte_census(root):
    apparent = allocated = 0
    for p in root.rglob('*'):
        assert not p.is_symlink(), ('owned namespace symlink', str(p))
        if p.is_file():
            s = p.stat()
            apparent += s.st_size
            allocated += s.st_blocks*512
    return dict(apparent=apparent, allocated=allocated)

def resource_check():
    free = shutil.disk_usage(R).free
    global_census, owned = byte_census(R), byte_census(NS)
    db = dict(apparent=0, allocated=0)
    for p in (NS/'tmp').glob('gomap-r1-tree-*'):
        assert p.is_dir() and not p.is_symlink()
        c = byte_census(p)
        db = {k: db[k]+c[k] for k in db}
    other = {k: owned[k]-db[k] for k in owned}
    sample = dict(at=now(), free=free, global_census=global_census, owned=owned, fixture=db, other=other)
    resource_samples.append(sample)
    emit('resource-samples.json', resource_samples)
    assert free >= resource['unrelated_free_floor_bytes'], 'sampled host free floor'
    assert global_census['apparent'] <= resource['total_attributable_R_logical_limit_bytes'], 'sampled whole R logical limit'
    assert all(v <= resource['S_namespace_allocated_and_apparent_limit_bytes']-resource['shutdown_and_unmeasured_growth_slack_bytes'] for v in owned.values()), 'sampled S namespace shutdown slack'
    assert all(v <= resource['S_fixture_DB_all_files_including_WAL_limit_bytes'] for v in db.values()), 'sampled fixture all-file limit'
    assert all(v <= resource['S_source_tools_caches_logs_combined_limit_bytes'] for v in other.values()), 'sampled source/cache/output limit'
    return global_census['apparent']

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
    raw = run('test2json-inputs', [G / 'bin/go', 'list', '-compiled', '-deps', '-json', 'cmd/test2json'], 240)
    capture_tool_inputs(raw)
    run('test2json-compile', [G / 'bin/go', 'build', '-x', '-o', test2json, 'cmd/test2json'], 240)
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
        raw = run(flavor + '-inputs', [G / 'bin/go', 'list', '-compiled', '-deps', '-test', *flags, '-json', *args], 240)
        expected = capture_inputs(raw, flavor)
        package_results = []
        control_binaries[flavor] = {}
        bindir = D / 'binaries' / flavor
        bindir.mkdir(parents=True, exist_ok=False)
        for pkg in spec['packages']:
            tag = flavor + '-' + pkg['tag']
            binary = bindir / (pkg['tag'] + '.test')
            control_binaries[flavor][pkg['tag']] = binary
            run(tag + '-compile', [G / 'bin/go', 'test', '-c', '-x', *flags, '-o', binary, pkg['go_argument']], 240)
            assert binary.is_file() and binary.read_bytes()[:4] == b'\x7fELF'
            binaries[str(binary)] = sha(binary)
            emit('preserved-binaries.json', binaries)
            run(tag + '-buildinfo', [G / 'bin/go', 'version', '-m', binary], 30)
            raw = run(tag + '-list', [test2json, '-p', pkg['import_path'], binary, '-test.list=' + spec['regex']], 30, W / pkg['relative_dir'])
            listed = [e['Output'].strip() for e in events(raw) if e.get('Action') == 'output' and re.fullmatch(spec['regex'], e.get('Output', '').strip())]
            pkg_expected = {x for x in expected if x[0] == pkg['import_path']}
            assert collections.Counter(listed) == collections.Counter({x[1]: 1 for x in pkg_expected}), 'binary/source list mismatch'
            if pkg['tag'] == 'harness':
                workflow_regex = '^(?:TestR1LeafDefault(RolloverDiscovery|FixedEpochs)5098|TestR1SDefaultStoragePreflight5098)$'
                workflow_raw = run(tag+'-workflow-list', [test2json, '-p', pkg['import_path'], binary, '-test.list='+workflow_regex], 30, W/pkg['relative_dir'])
                declarations = [e['Output'].strip() for e in events(workflow_raw) if e.get('Action') == 'output' and re.fullmatch(workflow_regex, e.get('Output', '').strip())]
                assert collections.Counter(declarations) == collections.Counter({'TestR1LeafDefaultRolloverDiscovery5098':1, 'TestR1LeafDefaultFixedEpochs5098':1, 'TestR1SDefaultStoragePreflight5098':1}), 'missing or duplicate compiled workflow declaration'
                emit(flavor+'-workflow-declarations.json', dict(compiled_declarations=declarations, workload_executed=False, fixed_arms_granted=False))
            raw = run(tag + '-functional', [test2json, '-p', pkg['import_path'], binary, '-test.v=test2json', '-test.run=' + spec['regex'], '-test.count=1', '-test.timeout=600s'], 720, W / pkg['relative_dir'])
            package_results.append(validate_test_events(raw, pkg['import_path'], pkg_expected))
        passed = {tuple(t) for p in package_results for t in p['top_tests']}
        assert passed == expected and all(tuple(risk) in passed for risk in spec['mandatory_risk_tests'])
        inventories[flavor] = dict(packages=package_results, top_pass=sum(p['top_pass'] for p in package_results), named_pass=sum(p['named_pass'] for p in package_results), skips=0, fails=0, duplicate_outcomes=0)
        emit(flavor + '-inventory.json', inventories[flavor])
        check()
    # Fixed short diagnostic after source-identical normal/race controls.
    # This selects neither discovery N nor a rollover/latency/finite claim.
    assert shutil.disk_usage(R).free >= resource['require_before_discovery_free_bytes']
    env.update(GOMAP_R1_S_PREFLIGHT_OUT=str(D/'preflight'))
    pkg = next(p for p in spec['packages'] if p['tag'] == 'harness')
    test = 'TestR1SDefaultStoragePreflight5098'
    raw = run('normal-storage-preflight', [test2json, '-p', pkg['import_path'], control_binaries['normal']['harness'], '-test.v=test2json', '-test.run=^'+test+'$', '-test.count=1', '-test.timeout=600s'], 720, W/pkg['relative_dir'])
    preflight_terminal = validate_test_events(raw, pkg['import_path'], {(pkg['import_path'],test)})
    preflight_result = dict(arms={}, qualification=False, N_selected=False, fixed_arms_granted=False)
    database_result = dict(paths=[], absent_after_actual_test_join=True)
    db_paths=set()
    for arm in ['original','exposure']:
        path=D/'preflight'/(arm+'-stages.jsonl')
        stages=[json.loads(x) for x in path.read_text().splitlines()]
        assert stages and all(s['arm']==arm for s in stages)
        assert stages[0]['stage']=='producer' and stages[-1]['stage']=='preflight_complete'
        assert len([s for s in stages if s['stage']=='producer'])==1
        producer=stages[0]['data']
        assert producer['population']==4096 and producer['hot']==64 and producer['batch']==32 and producer['calls']==256
        assert producer['phase_order']==['hot','broad'] and producer['cuts']==[64,128,192,256]
        assert producer['original_fixture_sha256']=='fd5d377dceaa2e53dd37bb3c2b56504f202e411988e17843ed3be1f257ee205c'
        stats=producer['cached_stats']
        assert stats['treedb.cache.vlog_generation.leaf.segment_target_bytes']=='33554432' and stats['treedb.cache.vlog_generation.hot.segment_target_bytes']=='268435456'
        assert stats['treedb.cache.physical_vlog_writers.configured']
        arm_inputs=[s['data'] for s in stages if s['stage']=='input']
        assert len(arm_inputs)==256 and [s['completed_calls'] for s in arm_inputs]==list(range(1,257))
        for n,s in enumerate(arm_inputs):
            assert s['phase']==('hot' if n<128 else 'broad')
            assert s['helper_call']==(2*n if n<128 else 2*(n-128)+1)
            assert len(s['ids'])==len(s['indices'])==32 and len(set(s['ids']))==32
        required_oracles=['loaded_oracle','drained_oracle','reopened_oracle']+[f'cut_{n:03d}_{phase}_oracle' for n in [64,128,192,256] for phase in ['before_flush','after_flush','after_checkpoint']]
        for name in required_oracles:
            matches=[s['data'] for s in stages if s['stage']==name]
            assert len(matches)==1 and matches[0]['complete_current'] and matches[0]['whole_email_history_and_city_postings']
            assert matches[0]['complete_held']==(name not in ['drained_oracle','reopened_oracle'])
        required_census=['loaded','drained','reopened']+[f'cut_{n:03d}_{phase}' for n in [64,128,192,256] for phase in ['before_flush','after_flush','after_checkpoint']]
        for name in required_census:
            matches=[s['data'] for s in stages if s['stage']==name]
            assert len(matches)==1
            c=matches[0];assert c['all_files'] and c['limit_bytes']==6*1024**3
            assert c['all_file_apparent_bytes']<=c['limit_bytes'] and c['all_file_allocated_bytes']<=c['limit_bytes']
            meta=bytes.fromhex(c['two_meta_pages_hex']);assert len(meta)==8192 and hashlib.sha256(meta).hexdigest()==c['two_meta_pages_sha256']
            if name in ['loaded','drained','reopened'] or name.endswith('_after_checkpoint'):assert c['recoverable_roots']
        complete=stages[-1]['data']
        assert complete['calls']==256 and complete['hot_calls']==complete['broad_calls']==128 and complete['qualification'] is False
        db_dir=Path(producer['db_dir']);assert db_dir.parent==NS/'tmp' and db_dir.name.startswith('gomap-r1-tree-')
        assert str(db_dir) not in db_paths and not db_dir.exists() and not db_dir.is_symlink()
        db_paths.add(str(db_dir));database_result['paths'].append(str(db_dir))
        preflight_result['arms'][arm]=dict(stages_sha256=sha(path),stage_count=len(stages),completion=complete,maximum_observed_apparent_bytes=max(s['data']['all_file_apparent_bytes'] for s in stages if s['stage'] in required_census),maximum_observed_allocated_bytes=max(s['data']['all_file_allocated_bytes'] for s in stages if s['stage'] in required_census))
    emit('preflight-result.json', preflight_result)
    emit('preflight-terminal.json', preflight_terminal)
    emit('database-result.json', database_result)
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
    success = failure is None and audit_error is None and joined and set(inventories) == {'normal', 'race'} and preflight_result is not None and database_result is not None
    receipt = dict(at=now(), status='PASS_SCOPED_LINUX111_S5098_SHORT_STORAGE_PREFLIGHT_PENDING_ROOT_AUDIT' if success else 'FAILED_OR_INCOMPLETE', failure=failure,
                   final_audit_error=audit_error, all_processes_joined=joined, active=active, commands=commands, inventories=inventories,
                   source_paths=len(source), source_manifest_sha256=auth['source_manifest_sha256'], authorization_sha256=auth_hash,
                   driver_sha256=sha(__file__), preserved_binaries=binaries, actual_compiler_inputs=len(inputs), toolchain_acceptance=auth['accepted_toolchain'],
                   protected_native_paths_selected=False, private_toolchain_and_caches=True, global_untouched_proof=False, system_header_closure_certified=False,
                   resource_policy='sampled2sec apparent/allocated S limits and total R logical cap; not kernel quota or peak certificate', preflight_result=preflight_result, database_result=database_result, N_accepted=False, fixed_arms_granted=False, finite=False, performance=False,
                   outer_SSH_and_driver_join_requires_ROOT_audit=True)
    receipt['remaining_fixture_DBs'] = [str(p) for p in (NS/'tmp').glob('gomap-r1-tree-*') if p.exists()]
    receipt['failed_DB_policy'] = 'retain all remaining failed DBs, including Close failure; short preflight removes only successful arms'
    emit('receipt.json', receipt)
    custody()
    if joined:
        save(R / 'state/reservation.json', dict(owner='R1 #5090 ROOT', status='joined_pending_ROOT_audit_v2', token=auth['token'],
             authorization_sha256=auth_hash, receipt_sha256=sha(D / 'receipt.json'), all_remote_phases_joined=True, outer_join_audited=False))
    print('RECEIPT', sha(D / 'receipt.json'), receipt['status'], flush=True)
sys.exit(0 if success else 1)
