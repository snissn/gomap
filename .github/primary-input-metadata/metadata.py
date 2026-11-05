"""Fixed 268-path identity observation; never dependency discovery or admission."""
import errno
import hashlib
import json
import os
from pathlib import Path
import stat
import sys
import time

ROOT_THREAD = '01a18e7c-8c23-7fd2-8d55-b0486d938cd3'
IMAGE = dict(ImageOS='ubuntu24', ImageVersion='20261025.353.1',
             RUNNER_OS='Linux', RUNNER_ARCH='X64')
READ_LIMIT = 1073741824
OUTPUT_LIMIT = 2097152
ROOT = Path(os.environ['RUNNER_TEMP']) / 'primary-construction-metadata'
START = time.monotonic()
read_bytes = 0
first_failure = None
stage = 'authority'


def require(ok, label):
    if not ok:
        raise RuntimeError(label)


def pulse():
    require(time.monotonic() - START < 180, 'query_deadline')


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True).encode('ascii')


def identity(s):
    return (s.st_dev, s.st_ino, s.st_nlink, s.st_mode, s.st_size,
            s.st_mtime_ns, s.st_ctime_ns)


def open_literal(path):
    """No resolution: every literal parent and leaf is opened without following links."""
    parts = path.split('/')
    require(parts[0] == '' and len(parts) > 1 and
            all(p not in ('', '.', '..') and '\\' not in p for p in parts[1:]), 'literal_path')
    fd = os.open('/', os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW)
    leaf = None
    try:
        for part in parts[1:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
                            dir_fd=fd)
            old, fd = fd, child
            # Transfer before checked close: never retry a failed close on old.
            os.close(old)
        leaf = os.open(parts[-1], os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK,
                       dir_fd=fd)
    finally:
        try:
            os.close(fd)
        except BaseException:
            if leaf is not None:
                os.close(leaf)
            raise
    return leaf


def observe(expected):
    global read_bytes
    path = expected['path']
    try:
        fd = open_literal(path)
    except OSError as error:
        kind = ('MISSING' if error.errno == errno.ENOENT else
                'UNSAFE_PATH' if error.errno in (errno.ELOOP, errno.ENOTDIR) else 'UNREADABLE')
        return dict(status=kind, errno=error.errno, equals_original=False)
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode):
            return dict(status='UNSAFE_NONREGULAR', equals_original=False)
        if before.st_size > READ_LIMIT - read_bytes:
            raise RuntimeError('query_read_budget')
        digest = hashlib.sha256()
        count = 0
        while True:
            pulse()
            block = os.read(fd, min(1048576, READ_LIMIT - read_bytes + 1))
            if not block:
                break
            read_bytes += len(block)
            count += len(block)
            require(read_bytes <= READ_LIMIT, 'query_read_budget')
            digest.update(block)
        after = os.fstat(fd)
        if identity(before) != identity(after) or count != before.st_size:
            return dict(status='CHANGED_DURING_READ', equals_original=False)
        actual = dict(path=path, bytes=count, sha256=digest.hexdigest())
        return dict(status='STABLE_REGULAR', actual=actual,
                    descriptor=dict(dev=before.st_dev, ino=before.st_ino,
                                    nlink=before.st_nlink, mode=before.st_mode),
                    equals_original=actual == expected)
    except OSError as error:
        return dict(status='UNREADABLE', errno=error.errno, equals_original=False)
    finally:
        # Close failure is a query failure, never a completed observation.
        os.close(fd)


def census(reserve=0):
    total = 0
    common = Path('/home/runner/work')
    for directory, dirs, names in os.walk(common, followlinks=False):
        require(all(not (Path(directory) / n).is_symlink() for n in dirs), 'root_symlink')
        for name in names:
            p = Path(directory) / name
            try:
                item = p.lstat()
            except FileNotFoundError:
                continue
            require(stat.S_ISREG(item.st_mode), 'root_nonregular')
            total += item.st_size
    require(total + reserve <= 1073741824, 'root_bytes')
    require(os.statvfs(common).f_bavail * os.statvfs(common).f_frsize >= 10737418240, 'free_floor')
    return total


def authority():
    e = os.environ
    require(sys.flags.isolated and sys.dont_write_bytecode, 'isolated_query')
    require(e.get('GITHUB_REPOSITORY') == 'snissn/gomap' and
            e.get('GITHUB_ACTOR_ID') == '1981537' and e.get('GITHUB_RUN_ATTEMPT') == '1' and
            e.get('GITHUB_EVENT_NAME') == 'workflow_dispatch' and
            e.get('GITHUB_REF') == 'refs/heads/diagnostic/primary-row217-metadata-01' and
            e.get('GITHUB_SHA') == e.get('REVIEWED_HEAD') == e.get('WORKFLOW_COMMIT'), 'actor_head')
    require({k: e.get(k) for k in IMAGE} == IMAGE, 'fixed_image')
    admission = json.loads((ROOT / 'dispatch-admission.json').read_text())
    require(admission['authority_thread'] == ROOT_THREAD and admission['authority'] == 'ROOT' and
            admission['actor_id'] == '1981537' and admission['head'] == e['GITHUB_SHA'] and
            admission['status'] == 'ADMITTED_METADATA_ONLY' and admission['allow_build'] is False and
            admission['expected_image'] == IMAGE, 'root_admission')


def main():
    global first_failure, stage
    authority()
    stage = 'initial_census'
    initial = census(4 * OUTPUT_LIMIT)
    stage = 'fixed_manifest'
    source = ROOT / 'harness' / 'original-runtime-inputs.json'
    manifest = json.loads(source.read_text())
    expected = manifest['rows']
    require(len(expected) == 268 and len({r['path'] for r in expected}) == 268, 'fixed_population')
    stage = 'observations'
    rows = []
    for index, original in enumerate(expected):
        if first_failure is not None:
            observation = dict(status='NOT_OBSERVED_AFTER_FAILURE', equals_original=False)
        else:
            try:
                pulse()
                observation = observe(original)
            except BaseException as error:
                label = str(error) if type(error) is RuntimeError else None
                if label not in ('query_deadline', 'query_read_budget', 'literal_path'):
                    label = None
                first_failure = dict(index=index, error_class=type(error).__name__,
                                     fixed_reason=label)
                observation = dict(status='OBSERVATION_FAILED', equals_original=False)
        role = ('runtime_configuration' if original['path'].startswith('/etc/') else
                'sdk_code_or_source' if original['path'].startswith('/opt/hostedtoolcache/go/') else
                'python_code_or_cache' if '/python3' in original['path'] else
                'runtime_code_or_tool')
        rows.append(dict(index=index, role=role, expected=original, observation=observation))
    stage = 'final_census_and_serialization'
    value = dict(schema='primary-fixed-runtime-observations-v1',
                 scope='METADATA_ONLY_NOT_ADMISSION_NOT_CURRENT_CLOSURE',
                 status='QUERY_FAILED' if first_failure else 'ALL_268_OBSERVATIONS_RECORDED',
                 reader_head=os.environ['GITHUB_SHA'], image=IMAGE,
                 kernel=dict(zip(('sysname', 'nodename', 'release', 'version', 'machine'), os.uname())),
                 original_manifest_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
                 original_image=manifest['image'], original_source_head=manifest['source_head'],
                 rows=rows, first_failure=first_failure, read_bytes=read_bytes,
                 elapsed_seconds=time.monotonic() - START,
                 initial_whole_bytes=initial, final_whole_bytes=census(4 * OUTPUT_LIMIT),
                 no_build=True, no_product=True, no_dependency_discovery=True,
                 future_host_identity_observed=False)
    encoded = canonical(value) + b'\n'
    require(len(encoded) <= OUTPUT_LIMIT, 'complete_output_bytes')
    require(sys.stdout.buffer.write(encoded) == len(encoded), 'complete_output_write')
    sys.stdout.buffer.flush()
    return 1 if first_failure else 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except BaseException as error:
        if first_failure is None:
            label = str(error) if type(error) is RuntimeError else None
            if label not in ('root_symlink', 'root_nonregular', 'root_bytes', 'free_floor',
                             'isolated_query', 'actor_head', 'fixed_image', 'root_admission',
                             'fixed_population', 'complete_output_bytes'):
                label = None
            first_failure = dict(stage=stage, error_class=type(error).__name__, fixed_reason=label)
        print(json.dumps(dict(status='QUERY_FAILED_NO_ADMISSION', stage=stage,
                              first_failure=first_failure)), flush=True)
        raise SystemExit(1)
