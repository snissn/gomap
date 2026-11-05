#!/usr/bin/env python3
"""Fixed B8e/Cb24 PRIMARY controls only. No benchmark/capture route."""
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
ROOT_THREAD = '01a18e7c-8c23-7fd2-8d55-b0486d938cd3'
RESOLVER_POLICY_BINDING = {'path': '.github/primary-construction/resolver-input-policy.json', 'bytes': 656, 'sha256': 'd087f191d9a1d3073bdb0b0cdc7bad11aedca7aafa03695d68bccf3b9bda0da0', 'decision_sha256': '152945251f6dce775b43111aa20711d2603d15998f9417fab93c1a1e0bddecc4'}
RESOLVER_EXPECTED = {'bytes': 972, 'path': '/run/systemd/resolve/stub-resolv.conf', 'sha256': '892c76027c36524dad6a845b53e62a3fd6f7bf4d1fb4bc641217994ed2e87d13'}
RESOLVER_LIMIT = 65536
OWNER_MANIFEST_SHA256 = '3ca00c97842855ec1c859d08371d17f13d89aa55fc1007be26fbec648684b618'
OWNER_REVIEW_SHA256 = 'ffd74ae5d95fd1d4647e2524b778e854f814707d1775ad46a7f585b678d498e6'
BRANCH = 'refs/heads/diagnostic/primary-8e-b24-resolver-role-01'
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
    need(a.get('schema') == 'primary-new-arm-controls-admission-v1'
         and a.get('status') == 'ADMITTED_NEW_ARM_CONTROLS_ONLY' and a.get('authority') == 'ROOT'
         and a.get('actor_id') == '1981537' and a.get('authority_thread') == ROOT_THREAD
         and a.get('head') == head
         and a.get('run_attempt') == '1' and a.get('allow_capture') is False,
         'fixed ROOT construction-only admission')
    for field in ('independent_source_review_sha256', 'owner_manifest_sha256'):
        need(re.fullmatch('[0-9a-f]{64}', a.get(field, '')) is not None, 'missing review/owner binding')
    need(a.get('resolver_configuration_policy') == RESOLVER_POLICY_BINDING,
         'explicit ROOT resolver role admission required')
    need(a.get('owner_manifest_sha256') == OWNER_MANIFEST_SHA256
         and a.get('owner_source_review_sha256') == OWNER_REVIEW_SHA256
         and a.get('arms') == {'B': '8e1133085b4052e344a73063f28c2ab142485c73',
                              'C': 'b24dea5e339cf94d419346ac9b38a8c90d6d3f02'}
         and a.get('controls') == dict(tops=4, outcomes=8, leaves=7, modes=['normal', 'race'])
         and a.get('prior_controls_credit') is False,
         'fixed new-arm controls production scope required')
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
    need(sha(DATA/'owner-independent-review.md') == OWNER_REVIEW_SHA256,
         'independent owner source review differs')
    return m


def runtime_inputs(a):
    # Exact fixed source manifest transports all rows, not a universal closure.
    binding = a.get('readonly_runtime_manifest', {})
    need('readonly_runtime_inputs' not in a and isinstance(binding, dict)
         and set(binding) == {'path', 'bytes', 'sha256', 'rows'}
         and binding['path'] == '.github/primary-construction/readonly-runtime-inputs.json'
         and type(binding['bytes']) is int and 0 < binding['bytes'] <= 1024**2
         and type(binding['rows']) is int and 0 < binding['rows'] <= 4096
         and re.fullmatch('[0-9a-f]{64}', binding['sha256']) is not None,
         'exact runtime manifest admission required')
    path = HARNESS/binding['path']
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        before = os.fstat(fd)
        need(stat.S_ISREG(before.st_mode) and before.st_nlink == 1
             and before.st_size == binding['bytes'], 'regular bounded runtime manifest')
        with os.fdopen(fd, 'rb', closefd=False) as source:
            raw = source.read(binding['bytes']+1)
        after = os.fstat(fd)
        need(len(raw) == binding['bytes']
             and all(getattr(before, k) == getattr(after, k) for k in
                     ('st_dev', 'st_ino', 'st_mode', 'st_nlink', 'st_size', 'st_mtime_ns', 'st_ctime_ns'))
             and hashlib.sha256(raw).hexdigest() == binding['sha256'], 'runtime manifest identity differs')
    finally:
        os.close(fd)
    rows = json.loads(raw)
    need(isinstance(rows, list) and len(rows) == binding['rows']
         and raw == canonical(rows)+b'\n', 'complete canonical runtime rows required')
    return rows


def resolver_policy(a):
    # Immutable source policy and ROOT decision, not a self-declared exemption.
    binding = RESOLVER_POLICY_BINDING
    need(a.get('resolver_configuration_policy') == binding
         and sha(HARNESS/binding['path']) == binding['sha256']
         and (HARNESS/binding['path']).stat().st_size == binding['bytes']
         and sha(DATA/'root-input-role-decision.json') == binding['decision_sha256'],
         'immutable resolver role policy differs')
    policy = read(HARNESS/binding['path'])
    need(policy['authority'] == 'ROOT' and policy['authority_thread'] == ROOT_THREAD
         and policy['actor_id'] == '1981537' and policy['index'] == 11
         and policy['logical_path'] == '/etc/resolv.conf'
         and policy['resolved_target'] == RESOLVER_EXPECTED['path']
         and policy['max_bytes'] == RESOLVER_LIMIT and policy['rows'] == 268,
         'fixed resolver role policy required')

def resolver_record():
    # The logical symlink is allowed only to this one approved existing target.
    logical = Path('/etc/resolv.conf')
    target = Path(RESOLVER_EXPECTED['path'])
    need(logical.resolve(strict=True) == target, 'fixed resolver target differs')
    fd = os.open(target, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(fd)
        need(stat.S_ISREG(before.st_mode) and before.st_nlink == 1
             and before.st_mode & 0o111 == 0 and 0 <= before.st_size <= RESOLVER_LIMIT,
             'bounded non-executable resolver data required')
        with os.fdopen(fd, 'rb', closefd=False) as source:
            raw = source.read(RESOLVER_LIMIT+1)
        after = os.fstat(fd)
        current = target.lstat()
        keys = ('st_dev', 'st_ino', 'st_mode', 'st_nlink', 'st_size', 'st_mtime_ns', 'st_ctime_ns')
        need(len(raw) == before.st_size and len(raw) <= RESOLVER_LIMIT
             and all(getattr(before, k) == getattr(after, k) == getattr(current, k) for k in keys)
             and logical.resolve(strict=True) == target, 'resolver data changed during read')
        return dict(path=str(target), bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())
    finally:
        os.close(fd)

def validate_qualified_rows(original, qualified):
    need(len(original) == 268 and original[11] == RESOLVER_EXPECTED
         and len({row['path'] for row in original}) == 268
         and isinstance(qualified, list) and len(qualified) == 268
         and all(row == original[index] for index, row in enumerate(qualified) if index != 11),
         'all original runtime roles required')
    row = qualified[11]
    need(isinstance(row, dict) and set(row) == {'path', 'bytes', 'sha256'}
         and row['path'] == RESOLVER_EXPECTED['path']
         and type(row['bytes']) is int and 0 <= row['bytes'] <= RESOLVER_LIMIT
         and isinstance(row['sha256'], str) and re.fullmatch('[0-9a-f]{64}', row['sha256']) is not None,
         'bounded qualified resolver record required')

def qualified_runtime_inputs(root, a, original, mode, started):
    resolver_policy(a)
    validate_qualified_rows(original, original)
    # Every other record retains the original equality predicate, before imports.
    for index, row in enumerate(original):
        if index != 11:
            need(Path(row['path']).is_absolute(), 'absolute runtime input path')
            tool_identity(row['path'], row)
    need(time.monotonic()-started < WHOLE, 'construction clock exhausted')
    path = root/'bootstrap-runtime-role.json'
    job = {key: os.environ[key] for key in ('GITHUB_SHA', 'GITHUB_RUN_ID', 'GITHUB_RUN_ATTEMPT', 'GITHUB_JOB')}
    if mode == 'launch':
        qualified = list(original)
        qualified[11] = resolver_record()
        validate_qualified_rows(original, qualified)
        inventory = canonical(qualified)+b'\n'
        receipt = dict(schema='primary-qualified-runtime-inputs-v1', job=job,
                       policy=RESOLVER_POLICY_BINDING, original_manifest=a['readonly_runtime_manifest'],
                       rows=qualified, inventory=dict(bytes=len(inventory), rows=268,
                           sha256=hashlib.sha256(inventory).hexdigest()),
                       original_resolver=original[11],
                       scope='same-job configuration observation after bootstrap; not DNS/whole-interval equivalence')
        encoded = canonical(receipt)+b'\n'
        snapshot = census(root)
        need(len(encoded) <= 1024**2 and not snapshot['errors']
             and snapshot['whole_bytes']+len(encoded) <= GIB,
             'qualified runtime receipt reservation failed')
        write(path, receipt)  # Exclusive; generated bytes count under the same root.
    else:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            before = os.fstat(fd)
            need(stat.S_ISREG(before.st_mode) and before.st_nlink == 1
                 and 0 < before.st_size <= 1024**2, 'bounded qualified runtime receipt required')
            with os.fdopen(fd, 'rb', closefd=False) as source:
                encoded = source.read(1024**2+1)
            after = os.fstat(fd)
            need(len(encoded) == before.st_size
                 and all(getattr(before, k) == getattr(after, k) for k in
                         ('st_dev', 'st_ino', 'st_mode', 'st_nlink', 'st_size', 'st_mtime_ns', 'st_ctime_ns')),
                 'qualified runtime receipt changed')
        finally:
            os.close(fd)
        receipt = json.loads(encoded)
        need(encoded == canonical(receipt)+b'\n', 'canonical qualified runtime receipt required')
        qualified = receipt['rows']
    validate_qualified_rows(original, qualified)
    inventory = canonical(qualified)+b'\n'
    need(receipt['schema'] == 'primary-qualified-runtime-inputs-v1' and receipt['job'] == job
         and receipt['policy'] == RESOLVER_POLICY_BINDING
         and receipt['original_manifest'] == a['readonly_runtime_manifest']
         and receipt['original_resolver'] == original[11]
         and receipt['inventory'] == dict(bytes=len(inventory), rows=268,
             sha256=hashlib.sha256(inventory).hexdigest()), 'qualified runtime binding differs')
    # Launcher after writing and worker before first helper import both re-read
    # the actual configuration. Never recapture/refresh a changed worker record.
    need(resolver_record() == qualified[11], 'qualified resolver identity differs')
    need(time.monotonic()-started < WHOLE and not census(root)['errors'],
         'qualified runtime boundary failed')
    return qualified


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
            if top in ('output', 'guard', 'bootstrap-start.json', 'bootstrap-receipt.json',
                       'construction-terminal.json', 'bootstrap-runtime-role.json'):
                files += s.st_size
                oversized = oversized or s.st_size > 32*1024**2
    free = shutil.disk_usage(root).free
    errors = []
    if total > GIB:
        errors.append('whole root exceeds 1GiB')
    if files > 32*1024**2:
        errors.append('aggregate generated evidence/logs exceeds 32MiB')
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


def control_events():
    path = HERE/'primary_new_arm_control_events.py'
    spec = importlib.util.spec_from_file_location('fixed_primary_control_events', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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



def upload_outputs(text, repository, run_id):
    # Fixed @actions/core OUTPUT protocol for precisely this unchanged upload entry.
    need(len(text.encode('utf-8')) <= 4096, 'bounded upload outputs')
    lines = text.splitlines()
    result = {}
    while lines:
        key, marker = lines.pop(0).split('<<', 1)
        need(key in ('artifact-id', 'artifact-digest', 'artifact-url') and key not in result
             and re.fullmatch('ghadelimiter_[0-9a-f-]{36}', marker) is not None,
             'unexpected upload output')
        need(len(lines) >= 2 and lines[1] == marker, 'malformed upload output')
        result[key] = lines.pop(0)
        lines.pop(0)
    need(set(result) == {'artifact-id', 'artifact-digest', 'artifact-url'}
         and re.fullmatch('[1-9][0-9]*', result['artifact-id']) is not None
         and re.fullmatch('[0-9a-f]{64}', result['artifact-digest']) is not None,
         'missing or invalid upload identity')
    expected = 'https://github.com/'+repository+'/actions/runs/'+run_id+'/artifacts/'+result['artifact-id']
    need(result['artifact-url'] == expected, 'foreign artifact output')
    return result


def publish(root, a, r, run, env, runtime):
    receipt = dict(status='PUBLICATION_FAILED', uploader_commit='ea165f8d65b6e75b540449e92b4886f43607fa02')
    try:
        r.guard()
        node = tool_identity(os.environ['PRIMARY_NODE_PATH'], a['tools']['node'])
        bundle = DATA/'upload-artifact/index.js'
        write(root/'output/prepublication-commands.json', r.commands)
        # Existing command files/logs are closed. Never include the live guard,
        # the forthcoming upload log, or later terminal/custody receipts.
        paths = sorted([*root.glob('bootstrap-*.json'), *list((root/'bin').iterdir()),
                        *list((root/'output').iterdir())])
        need(0 < len(paths) <= 4096, 'bounded immutable publication members')
        rows = []
        for path in paths:
            item = path.lstat()
            need(stat.S_ISREG(item.st_mode) and item.st_nlink == 1 and not path.is_symlink(),
                 'immutable member must be regular single-link')
            rows.append(dict(path=str(path.relative_to(root)), bytes=item.st_size, sha256=sha(path)))
        manifest = root/'output/prepublication-manifest.json'
        write(manifest, dict(schema='primary-prepublication-members-v1', files=rows,
                             excludes='active upload/guard logs and all post-join receipts'))
        receipt.update(member_manifest_sha256=sha(manifest), members=len(rows)+1,
                       uploader_sha256=sha(bundle), node=a['tools']['node'])
        # Only this child receives artifact runtime credentials. Go/Git env is separate.
        output = Path(runtime['GITHUB_OUTPUT'])
        info = output.lstat()
        need(output.resolve(strict=True).is_relative_to(accounting_root(root))
             and stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_size == 0,
             'fresh counted standard action output required before upload')
        selected = dict(env)
        selected.update(runtime)
        selected.update({'INPUT_NAME': 'primary-construction-'+runtime['GITHUB_RUN_ID']+'-'+runtime['GITHUB_RUN_ATTEMPT'],
                         'INPUT_PATH': '\n'.join(str(path) for path in [*paths, manifest]),
                         'INPUT_IF-NO-FILES-FOUND': 'error', 'INPUT_RETENTION-DAYS': '14',
                         'INPUT_COMPRESSION-LEVEL': '6', 'INPUT_OVERWRITE': 'false',
                         'INPUT_INCLUDE-HIDDEN-FILES': 'true'})
        try:
            run('publication', [node, str(bundle)], WHOLE, selected_env=selected)
        finally:
            selected.clear()
        for row in rows:
            path = root/row['path']
            need(path.stat().st_size == row['bytes'] and sha(path) == row['sha256'],
                 'uploaded immutable member changed')
        output = Path(runtime['GITHUB_OUTPUT'])
        info = output.lstat()
        need(output.resolve(strict=True).is_relative_to(accounting_root(root))
             and stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_size <= 4096,
             'counted regular upload output required')
        values = upload_outputs(output.read_text(), runtime['GITHUB_REPOSITORY'], runtime['GITHUB_RUN_ID'])
        receipt.update(status='UPLOAD_JOINED_PENDING_OUTER_GUARD', outputs=values,
                       output_sha256=sha(output), command=r.commands[-1])
        r.guard()
    except BaseException as caught:
        receipt.update(status='PUBLICATION_FAILED', error_class=type(caught).__name__)
        raise
    finally:
        # This receipt intentionally does not belong to the artifact it certifies.
        write(root/'output/publication-receipt.json', receipt)
        r.guard()


def worker(root, a, started, readonly):
    # This point is reached only through the authenticated portable outer guard.
    for name in ('output', 'tmp', 'gocache', 'modcache', 'gopath', 'bin', 'xdg-cache', 'xdg-config'):
        (root/name).mkdir()
    module = load_runner()
    r = whole_runner(module, root, started)
    # Runner.command writes logs at r.out; WholeRootRunner storage always scans root.
    r.out = root/'output'
    error = None
    builds = {}
    inventories = {}
    runtime = {}
    for key in ('ACTIONS_RUNTIME_TOKEN', 'ACTIONS_RUNTIME_URL', 'ACTIONS_RESULTS_URL',
                'GITHUB_OUTPUT', 'GITHUB_REPOSITORY', 'GITHUB_RUN_ID', 'GITHUB_RUN_ATTEMPT',
                'GITHUB_SERVER_URL', 'GITHUB_WORKSPACE'):
        need(os.environ.get(key), 'standard action runtime environment required')
        runtime[key] = os.environ[key]
    for key in ('GITHUB_API_URL', 'GITHUB_GRAPHQL_URL', 'GITHUB_ACTOR', 'GITHUB_REF',
                'GITHUB_SHA', 'GITHUB_RUN_NUMBER', 'GITHUB_WORKFLOW', 'GITHUB_JOB',
                'GITHUB_ACTION', 'GITHUB_RETENTION_DAYS', 'GITHUB_EVENT_NAME', 'GITHUB_EVENT_PATH',
                'RUNNER_DEBUG', 'HTTPS_PROXY', 'HTTP_PROXY', 'NO_PROXY',
                'https_proxy', 'http_proxy', 'no_proxy'):
        if key in os.environ:
            runtime[key] = os.environ[key]
    for key in list(os.environ):
        if key.startswith('ACTIONS_'):
            os.environ.pop(key)
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
        need(resolver_record() == readonly[11], 'qualified resolver identity differs')
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
        events = control_events()  # Authenticated helper, after all runtime/tool checks.
        # Full SDK authentication above also binds the exact formatter executable.
        for arm in ARMS:
            mapping = read(DATA/'owner/overlays.json')['primary'][arm]
            targets = [str(DATA/'owner'/name) for name in mapping['Replace'].values()]
            formatted = run(arm+'-format', [str(SDK/'bin/gofmt'), '-l', *targets], 30)
            need(not formatted.strip(), 'owner overlay source is not formatted; no automatic rewrite')
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
            # Compile-only normal preparation; no test/warmup/benchmark executes here.
            builds[arm] = {}
            for package in ('caching', 'mvcc'):
                binary = root/'bin'/(arm+'-'+package+'.test')
                run(arm+'-'+package+'-build', [go, 'test', '-tags=treedb_test', '-p=1', '-mod=readonly', '-c',
                    '-overlay='+str(overlay), '-o='+str(binary), './TreeDB/'+package], 360, cwd=source)
                s = binary.lstat()
                need(stat.S_ISREG(s.st_mode) and s.st_nlink == 1 and not binary.is_symlink(), 'normal ELF type')
                buildinfo = run(arm+'-'+package+'-buildinfo', [go, 'version', '-m', str(binary)], 30)
                need('go1.26.10' in buildinfo and '-race=true' not in buildinfo, 'normal buildinfo differs')
                builds[arm][package] = dict(path=str(binary), bytes=s.st_size, sha256=sha(binary),
                    source=spec['commit'], tree=spec['tree'], overlay_sha256=sha(overlay))
            inventories[arm] = {}
            for mode in ('normal', 'race'):
                args = [go, 'test', '-json', '-tags=treedb_test', '-p=1', '-mod=readonly',
                        '-overlay='+str(overlay), '-count=1', '-run='+events.SELECTOR,
                        '-timeout='+('150s' if mode == 'normal' else '570s')]
                if mode == 'race':
                    args.append('-race')
                args.extend(('./TreeDB/caching', './TreeDB/mvcc'))
                run(arm+'-'+mode, args, 180 if mode == 'normal' else 600, cwd=source)
                record = r.commands[-1]
                inventory = events.validate(root/'output'/record['log'])
                inventory.update(arm=arm, source=spec['commit'], tree=spec['tree'], mode=mode,
                                 literal_exit=record['exit_code'], event_sha256=sha(root/'output'/record['log']))
                write(root/'output'/(arm+'-'+mode+'-inventory.json'), inventory)
                inventories[arm][mode] = inventory
            source_identity(source, spec)
        token = None
        r.guard()
    except BaseException as caught:
        error = type(caught).__name__  # Never print credential-bearing exception repr/argv/env.
    finally:
        token = None
        # Preserve the original always-retain intent for ordinary build failures,
        # but never start publication after an observed breach or expired clock.
        try:
            if r.first_failure is None and time.monotonic() < r.deadline:
                publish(root, a, r, run, env, runtime)
            else:
                error = error or 'PublicationResourceRefusal'
        except BaseException as caught:
            error = error or type(caught).__name__
        runtime.clear()
        r.close()
        final = census(root)
        if final['errors'] or r.first_failure:
            error = error or 'ResourceFailure'
        terminal = dict(status='CONTROLS_FAILED_NO_CAPTURE' if error else 'NEW_ARM_CONTROLS_PENDING_OUTER_REVIEW',
                        error_class=error, builds=builds, inventories=inventories, commands=r.commands, final_census=final,
                        first_failure=r.first_failure, max_sampled_whole_bytes=r.maximum,
                        publication_sha256=sha(root/'output/publication-receipt.json') if (root/'output/publication-receipt.json').exists() else None,
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
    tool_identity(os.environ['PRIMARY_NODE_PATH'], a['tools']['node'])
    # Explicit ROOT-bound read-only runtime/tool closure authenticates before
    # the first downloaded helper import, not merely before Go execution.
    readonly = runtime_inputs(a)
    need(isinstance(readonly, list) and 0 < len(readonly) <= 4096, 'bounded runtime input closure')
    for name, path in (('git', '/usr/bin/git'), ('gcc', '/usr/bin/gcc'), ('g++', '/usr/bin/g++')):
        tool_identity(path, a['tools'][name])
    root = root_path()
    started = read(root/'bootstrap-start.json')['monotonic']
    need(time.monotonic()-started < WHOLE, 'construction clock exhausted')
    readonly = qualified_runtime_inputs(root, a, readonly, args.mode, started)
    if args.mode == 'worker':
        return worker(root, a, started, readonly)
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
    publication = read(root/'output/publication-receipt.json') if (root/'output/publication-receipt.json').exists() else None
    passed = (publication is not None and publication['status'] == 'UPLOAD_JOINED_PENDING_OUTER_GUARD'
              and code == 0 and result['errors'] == [] and result['cleanup_signals'] == []
              and result['status'] == 'guard-completed-pending-independent-artifact-review'
              and terminal['status'] == 'NEW_ARM_CONTROLS_PENDING_OUTER_REVIEW'
              and not final['errors'])
    write(root/'construction-terminal.json', dict(status='NEW_ARM_CONTROLS_CLOSED_PENDING_ARTIFACT_REVIEW' if passed else 'CONSTRUCTION_FAILED_NO_CAPTURE',
          guard_exit=code, guard_sha256=sha(root/'guard/result.json'), worker_sha256=sha(root/'output/worker-terminal.json'),
          publication_sha256=sha(root/'output/publication-receipt.json') if publication else None,
          final_census=final, capture=False, m0=False, whole_interval_verified=False, unperturbed_timing_verified=False))
    passed = passed and not census(root)['errors']
    # Complete final records are in the original job log, never a second upload.
    closing = dict(schema='primary-post-publication-custody-v1', status='PENDING_REVIEW' if passed else 'FAILED',
                   publication=publication, worker=terminal, guard=result,
                   terminal=read(root/'construction-terminal.json'),
                   terminal_sha256=sha(root/'construction-terminal.json'), capture=False, m0=False)
    encoded = canonical(closing)
    need(len(encoded) <= 8*1024**2, 'bounded complete final log receipt')
    print(encoded.decode('ascii'), flush=True)
    return 0 if passed else 1


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as error:
        print(json.dumps({'status': 'FAILED_NO_CAPTURE', 'error_class': type(error).__name__}), flush=True)
        sys.exit(1)
