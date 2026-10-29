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
    binary = run/'baseline-db.test'
    env = dict(os.environ, GOWORK='off')
    subprocess.run(['go', 'test', '-c', '-o', str(binary), './TreeDB/db'], cwd=target, env=env, check=True)
    build = subprocess.check_output(['go', 'version', '-m', str(binary)], cwd=target, env=env, text=True)
    record = {'baseline_source_sha': source,
              'candidate_source_sha': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=repo, text=True).strip(),
              'baseline_binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
              'candidate_db_test_harness_sha256': harness,
              'baseline_build_info': build,
              'source_archive_sha256': hashlib.sha256(archive.read_bytes()).hexdigest(),
              'build_command': 'GOWORK=off go test -c -o baseline-db.test ./TreeDB/db',
              'baseline_trust': 'operator-approved exact ancestor; same candidate db test harness overlay'}
    (run/'paired-provenance.json').write_text(json.dumps(record, indent=2, sort_keys=True)+'\n')


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--repo-root', required=True, type=Path)
    p.add_argument('--run-dir', required=True, type=Path)
    p.add_argument('--source', required=True)
    a = p.parse_args()
    prepare(a.repo_root.resolve(), a.run_dir.resolve(), a.source)
