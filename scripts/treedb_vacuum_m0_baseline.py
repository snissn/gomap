#!/usr/bin/env python3
"""Build the explicitly approved ancestor with the candidate's exact db test harness.

This is qualification setup, never a timing sample. Both revisions are executable
code: the operator must approve the baseline SHA through the existing admission.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import stat


def prepare(repo, run, source):
    if not re.fullmatch(r'[0-9a-f]{40}', source):
        raise ValueError('M0_BASELINE_SHA must be an explicitly approved full commit SHA')
    actual = subprocess.check_output(['git', 'rev-parse', source+'^{commit}'], cwd=repo, text=True).strip()
    if actual != source:
        raise ValueError('baseline commit identity mismatch')
    subprocess.run(['git', 'merge-base', '--is-ancestor', source, 'HEAD'], cwd=repo, check=True)
    if subprocess.check_output(['git', 'status', '--porcelain', '--untracked-files=normal'], cwd=repo):
        raise ValueError('paired qualification requires a clean candidate checkout')
    target = run/'baseline-source'
    target.mkdir()
    archive = run/'baseline-source.tar'
    with archive.open('xb') as output:
        subprocess.run(['git', 'archive', '--format=tar', source], cwd=repo, stdout=output, check=True)
    subprocess.run(['tar', '-xf', str(archive), '-C', str(target)], check=True)
    # Replace rather than append: obsolete baseline test files cannot change the
    # selected harness or prevent the current harness from compiling silently.
    for old in (target/'TreeDB/db').glob('*_test.go'):
        old.unlink()
    harness = {}
    for path in sorted((repo/'TreeDB/db').glob('*_test.go')):
        relative = path.relative_to(repo)
        shutil.copyfile(path, target/relative)
        harness[str(relative)] = hashlib.sha256(path.read_bytes()).hexdigest()
    initial = dict(os.environ, GOWORK='off')
    root = subprocess.check_output(['go', 'env', 'GOROOT'], cwd=repo, env=initial, text=True).strip()
    driver = Path(root)/'bin/go'
    env = dict(initial, GOROOT=root, GOTOOLCHAIN='local')
    record = {'baseline_source_sha': source,
              'candidate_source_sha': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=repo, text=True).strip(),
              'candidate_db_test_harness_sha256': harness,
              'source_archive_sha256': digest(archive),
              'pinned_environment': {'GOROOT': root, 'GOTOOLCHAIN': 'local', 'GOWORK': 'off'},
              'runtime_environment': runtime_environment(env),
              'baseline_trust': 'operator-approved exact ancestor; same candidate db test harness overlay'}
    for arm, cwd in [('baseline', target), ('candidate', repo)]:
        binary = run/(arm+'-db.test')
        binding = build_environment(driver, cwd, env)
        subprocess.run([str(driver), 'test', '-c', '-o', str(binary), './TreeDB/db'], cwd=cwd, env=env, check=True)
        if binding != build_environment(driver, cwd, env):
            raise ValueError('compiler/environment changed during build')
        record[arm+'_binary_sha256'] = digest(binary)
        record[arm+'_build_environment'] = binding
        record[arm+'_build_info'] = subprocess.check_output([str(driver), 'version', '-m', str(binary)], cwd=cwd, env=env, text=True)
    if record['baseline_build_environment'] != record['candidate_build_environment']:
        raise ValueError('baseline/candidate compiler/environment mismatch')
    record['build_command'] = str(driver)+' test -c -o <arm>-db.test ./TreeDB/db'
    (run/'paired-provenance.json').write_text(json.dumps(record, indent=2, sort_keys=True)+'\n')


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def runtime_environment(env):
    # Fixed build/runtime knobs only; never serialize credentials or unrelated env.
    names = {'CPU_SET', 'GOMAXPROCS', 'GOMEMLIMIT', 'GOGC', 'GODEBUG', 'GOTRACEBACK',
             'GOEXPERIMENT', 'TMPDIR', 'LANG', 'LC_ALL', 'TZ', 'LD_PRELOAD', 'LD_LIBRARY_PATH'}
    names.update(k for k in env if k.startswith('TREEDB_') and not k.startswith('TREEDB_VACUUM_M0_'))
    return {k: env.get(k) for k in sorted(names)}


def build_environment(driver, cwd, env):
    keys = ['GOVERSION', 'GOROOT', 'GOTOOLDIR', 'GOOS', 'GOARCH', 'GOAMD64',
            'GOEXPERIMENT', 'GOFLAGS', 'GOTOOLCHAIN', 'CGO_ENABLED', 'CC', 'CXX',
            'CGO_CFLAGS', 'CGO_CPPFLAGS', 'CGO_CXXFLAGS', 'CGO_LDFLAGS', 'GOGCCFLAGS', 'GOWORK']
    resolved = json.loads(subprocess.check_output([str(driver), 'env', '-json', *keys], cwd=cwd, env=env, text=True))
    compiler = Path(resolved['GOTOOLDIR'])/'compile'
    compiler_version = subprocess.check_output([str(driver), 'tool', 'compile', '-V'], cwd=cwd, env=env, text=True).strip()
    if compiler_version != 'compile version '+resolved['GOVERSION']:
        raise ValueError('resolved Go driver/compiler version mismatch')
    return {'go_environment': resolved, 'driver': str(driver), 'driver_sha256': digest(driver),
            'compiler': str(compiler), 'compiler_sha256': digest(compiler),
            'compiler_version': compiler_version, 'runtime_environment': runtime_environment(env)}


def execute(run, arm, flags):
    record = json.loads((run/'paired-provenance.json').read_text())
    binary = run/(arm+'-db.test')
    env = dict(os.environ, **record['pinned_environment'])
    if runtime_environment(env) != record['runtime_environment']:
        raise ValueError('paired runtime environment changed after build')
    binding = record[arm+'_build_environment']
    if (digest(binding['driver']) != binding['driver_sha256'] or
            digest(binding['compiler']) != binding['compiler_sha256']):
        raise ValueError('paired compiler changed after build')
    fd = os.open(binary, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        with os.fdopen(os.dup(fd), 'rb') as content:
            actual = hashlib.sha256(content.read()).hexdigest()
        if not stat.S_ISREG(os.fstat(fd).st_mode) or actual != record[arm+'_binary_sha256']:
            raise ValueError(arm+' executable changed after build')
    except BaseException:
        os.close(fd)
        raise
    # Both arms execute from the same candidate cwd, CPU placement and env.
    try:
        return subprocess.run(['/proc/self/fd/'+str(fd), *flags], env=env, pass_fds=(fd,)).returncode
    finally:
        os.close(fd)


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--repo-root', type=Path)
    p.add_argument('--run-dir', required=True, type=Path)
    p.add_argument('--source')
    p.add_argument('--exec-arm', choices=('baseline', 'candidate'))
    p.add_argument('flags', nargs=argparse.REMAINDER)
    a = p.parse_args()
    if a.exec_arm:
        flags = a.flags[1:] if a.flags[:1] == ['--'] else a.flags
        raise SystemExit(execute(a.run_dir.resolve(), a.exec_arm, flags))
    if a.repo_root is None or a.source is None:
        p.error('--repo-root and --source required for paired build')
    prepare(a.repo_root.resolve(), a.run_dir.resolve(), a.source)
