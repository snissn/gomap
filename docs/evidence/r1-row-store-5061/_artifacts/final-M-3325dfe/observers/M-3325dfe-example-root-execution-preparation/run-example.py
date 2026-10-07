#!/usr/bin/env python3
"""Later root-only exact-M example execution; no numeric acceptance."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

M = '3325dfe77940fec8587d8b61b1ac4e0b2f72caca'
REPO = Path('/home/mikers/gomap-r1-final-source-20261006')
CONFIG = Path('/home/mikers/gomap-r1-final-configs-20261006/M-3325dfe/A-config.json')
CONFIG_SHA = 'f2b95b22d5e1c12a00c5cc77dc5846b199535aa3e30e2cc77ed8b8f8b03257b4'
MANIFEST = CONFIG.parent / 'derived-A-C-source.json'
MANIFEST_SHA = 'd01cabb0159552b9d326977758278b9fd8f5df4f9f1f04b7587e79d95481998f'
OUT = Path('/home/mikers/gomap-r1-final-results-20261006/M-3325dfe/example')
LOCK = '/home/mikers/gomap-r1-evidence-20261005/timed-capture.lock'
RUNTIME = 'eab40aaf77ed307d92a7a38adf0dd80be07a7e12bdda8a2ec3118baf9cad33ff'
EXAMPLES = {'examples/typed_rows/main.go': 'f54a456e2d16bbb78cfc637d80f8b6b48410c563',
            'examples/typed_rows/README.md': 'a268b790e245aecc77712ee82e04be19cc564b27'}
VERIFIED = 'Verified durable reopen: 2 complete rows; current and removed secondary postings; deleted row absent.'
OWN_NEW_OUTPUT = False

def require(value, message):
    if not value:
        raise ValueError(message)

def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def sha_bytes(data):
    return hashlib.sha256(data).hexdigest()

def sha(path):
    return sha_bytes(Path(path).read_bytes())

def read(path):
    def pairs(entries):
        obj = {}
        for k, v in entries:
            require(k not in obj, 'duplicate JSON key')
            obj[k] = v
        return obj
    def invalid(value):
        raise ValueError('nonfinite JSON: ' + value)
    return json.loads(Path(path).read_text(), object_pairs_hook=pairs, parse_constant=invalid)

def freeze(path, value):
    with Path(path).open('x') as output:
        json.dump(value, output, indent=2, sort_keys=True, allow_nan=False)
        output.write('\n')

def git(*args):
    return subprocess.check_output(['git', '-C', str(REPO), *args])

def clean():
    require(git('rev-parse', 'HEAD').decode().strip() == M, 'source is not exact M')
    require(not git('status', '--porcelain', '--untracked-files=normal').strip(), 'source is dirty')

def checked_source(cfg, expected):
    require(sha(CONFIG) == CONFIG_SHA and sha(MANIFEST) == MANIFEST_SHA, 'independent source/config pins changed')
    clean()
    tree = {}
    for entry in git('ls-tree', '-rz', M).split(b'\0'):
        if entry:
            metadata, path = entry.split(b'\t', 1)
            mode, kind, blob = metadata.decode().split()
            tree[path.decode()] = (mode, kind, blob)
    blobs = {p: m[2] for p, m in tree.items() if p in ('go.mod', 'go.sum') or
             (p.startswith(('TreeDB/', 'cmd/internal/treedbstats/')) and Path(p).suffix in ('.go', '.s', '.S', '.c', '.h', '.syso') and not p.endswith('_test.go'))}
    runtime = sha_bytes(b''.join(p.encode() + b'\0' + h.encode() + b'\0' for p, h in sorted(blobs.items())))
    harness_paths = ['cmd/collection_workload_bench/main.go', *sorted(p for p in tree if p.startswith('cmd/collection_workload_bench/r1') and '/' not in p[len('cmd/collection_workload_bench/'):] and p.endswith('.go') and not p.endswith('_test.go')), 'scripts/r1_collection_capture.sh', 'scripts/r1_collection_summary.py', 'scripts/r1_collection_source.py']
    selected = set(blobs) | set(harness_paths) | set(EXAMPLES)
    require(all(tree[p][0] in ('100644', '100755') and tree[p][1] == 'blob' for p in selected), 'unexpected source file kind')
    ids = sorted({tree[p][2] for p in selected})
    raw = subprocess.check_output(['git', '-C', str(REPO), 'cat-file', '--batch'], input=('\n'.join(ids) + '\n').encode())
    bodies, offset = {}, 0
    for identity in ids:
        end = raw.index(b'\n', offset)
        actual, kind, length = raw[offset:end].decode().split()
        length = int(length)
        require(actual == identity and kind == 'blob', 'unexpected Git object')
        body = raw[end + 1:end + 1 + length]
        require(len(body) == length and raw[end + 1 + length:end + 2 + length] == b'\n', 'truncated Git object')
        bodies[identity] = body
        offset = end + 2 + length
    require(offset == len(raw), 'extra Git object bytes')
    require(all((REPO / p).read_bytes() == bodies[tree[p][2]] for p in selected), 'working source differs from actual Git bytes')
    harness = sha_bytes(b''.join(p.encode() + b'\0' + bodies[tree[p][2]] + b'\0' for p in harness_paths))
    actual = {'commit': M, 'clean': True, 'runtime_sha256': runtime, 'runtime_blobs': blobs, 'harness_sha256': harness}
    require(actual == expected and runtime == RUNTIME and cfg['runtime_sha256'] == runtime and cfg['harness_sha256'] == harness, 'actual source inventory differs from independent BEFORE capture manifest')
    example_files = {p: {'blob': tree[p][2], 'sha256': sha_bytes(bodies[tree[p][2]]), 'bytes': len(bodies[tree[p][2]]), 'working_sha256': sha(REPO / p)} for p in EXAMPLES}
    require(all(example_files[p]['blob'] == b for p, b in EXAMPLES.items()), 'actual-M example source differs')
    return {'source': actual, 'tree': git('rev-parse', M + '^{tree}').decode().strip(), 'example_files': example_files}

def observed(command, name, environment):
    freeze(OUT / (name + '-start.json'), {'utc': now(), 'argv': command, 'cwd': str(REPO), 'environment': environment})
    stdout, stderr = OUT / (name + '-stdout.log'), OUT / (name + '-stderr.log')
    with stdout.open('xb') as out, stderr.open('xb') as err:
        status = subprocess.call(command, cwd=REPO, env=environment, stdout=out, stderr=err)
    freeze(OUT / (name + '-exit.json'), {'utc': now(), 'exit': status, 'stdout_sha256': sha(stdout), 'stderr_sha256': sha(stderr), 'stdout_bytes': stdout.stat().st_size, 'stderr_bytes': stderr.stat().st_size, 'source_head_after': git('rev-parse', 'HEAD').decode().strip(), 'source_status_after': git('status', '--porcelain', '--untracked-files=normal').decode()})
    require(status == 0, 'original ' + name + ' command failed; retain outputs, do not retry')
    return stdout.read_text()

def main():
    global OWN_NEW_OUTPUT
    argparse.ArgumentParser(description=__doc__).parse_args()
    require(os.environ.get('R1_RECEIPT_LOCKED') == 'yes', 'root must externally hold the canonical111 flock AFTER D')
    require(not OUT.exists() and not OUT.is_symlink(), 'example output must be NEW; no reuse or replacement')
    require(sha(CONFIG) == CONFIG_SHA and sha(MANIFEST) == MANIFEST_SHA, 'independent config/manifest changed')
    cfg, expected = read(CONFIG), read(MANIFEST)
    require(Path(cfg['repo']).resolve() == REPO and cfg['source_commit'] == M and str(Path(cfg['lock']).resolve()) == LOCK, 'wrong pinned source or canonical lock')
    before = checked_source(cfg, expected)
    program_before = sha(__file__)
    compiler_before = sha(cfg['real_go'])
    OUT.mkdir(parents=True, mode=0o700, exist_ok=False)
    OWN_NEW_OUTPUT = True
    temporary = OUT / 'go-tmp'
    temporary.mkdir(mode=0o700)
    dbdir = OUT / 'db'
    require(dbdir.is_absolute() and not dbdir.exists() and not dbdir.is_symlink(), 'required absolute DB directory must be NEW and unused')
    environment = {k: os.environ[k] for k in ('HOME', 'PATH') if k in os.environ}
    environment.update(GOROOT=cfg['goroot'], GOCACHE=cfg['gocache'], GOMODCACHE=cfg['gomodcache'], GOTOOLCHAIN='local', GOWORK='off', GOENV='off', GOFLAGS='', GODEBUG='', GOMAXPROCS='16', GOGC='100', GOMEMLIMIT='off', PYTHONDONTWRITEBYTECODE='1', LEFTHOOK='0', TMPDIR=str(temporary))
    freeze(OUT / 'root-inputs-original.json', {'utc': now(), 'config': cfg, 'config_sha256': CONFIG_SHA, 'manifest_sha256': MANIFEST_SHA, 'source': before, 'compiler': {'path': cfg['real_go'], 'sha256': compiler_before}, 'environment': environment, 'db_directory': str(dbdir), 'helper_sha256': program_before, 'lock': {'path': LOCK, 'external_flock_required': True, 'marker_is_not_lock_proof': True}, 'ordering': 'Root must invoke after actual D; helper does not infer D acceptance.'})
    version = observed([cfg['real_go'], 'version'], 'go-version', environment).strip()
    require(version == 'go version go1.26.4 linux/amd64', 'actual compiler version differs from frozen Linux toolchain')
    observed([cfg['real_go'], 'vet', './examples/typed_rows'], 'vet', environment)
    require(checked_source(cfg, expected) == before and sha(cfg['real_go']) == compiler_before, 'source/compiler changed during vet')
    require(not dbdir.exists() and not dbdir.is_symlink(), 'DB directory was used before original example run')
    stdout = observed([cfg['real_go'], 'run', './examples/typed_rows', '-dir', str(dbdir)], 'example', environment)
    after = checked_source(cfg, expected)
    freeze(OUT / 'source-after-original.json', {'utc': now(), **after})
    require(after == before and sha(cfg['real_go']) == compiler_before and sha(__file__) == program_before, 'source/compiler/helper changed during original execution')
    require(VERIFIED in stdout.splitlines(), 'meaningful durable-reopen verification line is absent from original stdout')
    require('DB directory: ' + str(dbdir) in stdout.splitlines(), 'original example did not report its required absolute DB directory')
    require(dbdir.is_dir() and not dbdir.is_symlink(), 'original example DB directory is absent')
    originals = [OUT / n for n in ('root-inputs-original.json', 'go-version-start.json', 'go-version-exit.json', 'go-version-stdout.log', 'go-version-stderr.log', 'vet-start.json', 'vet-exit.json', 'vet-stdout.log', 'vet-stderr.log', 'example-start.json', 'example-exit.json', 'example-stdout.log', 'example-stderr.log', 'source-after-original.json')]
    freeze(OUT / 'root-example-verification-original.json', {'utc': now(), 'state': 'ACTUAL_M_EXAMPLE_VET_AND_ORIGINAL_DURABLE_REOPEN_VERIFICATION_PASS_NUMERIC_ACCEPTANCE_SEPARATE', 'source_before': before, 'source_after': after, 'compiler_path': cfg['real_go'], 'compiler_sha256_before_after': compiler_before, 'compiler_version': version, 'config_sha256_before_after': CONFIG_SHA, 'manifest_sha256_before_after': MANIFEST_SHA, 'helper_sha256_before_after': program_before, 'required_verification_line': VERIFIED, 'observed_verification_line_present': True, 'db_directory': str(dbdir), 'originals': {str(p): {'sha256': sha(p), 'bytes': p.stat().st_size} for p in originals}, 'limits': ['Original example correctness only; no numeric capacity, performance or power-loss acceptance.', 'The new private DB remains in output and must never be copied to public support.', 'No retry, replacement, cleanup or success-stdout reconstruction.']})
    print(json.dumps({'output': str(OUT), 'state': 'EXAMPLE_ORIGINAL_VERIFICATION_PASS_NUMERIC_ACCEPTANCE_SEPARATE'}))

if __name__ == '__main__':
    try:
        main()
    except (ValueError, KeyError, OSError, subprocess.CalledProcessError) as error:
        # A new directory created by this attempt keeps every original/partial log.
        # Refusal of an existing output must never add to or alter that old directory.
        if OWN_NEW_OUTPUT:
            try:
                freeze(OUT / 'failure-original.json', {'utc': now(), 'state': 'FAILED_OR_REFUSED_PRESERVE_ORIGINALS_NO_RETRY', 'error': str(error), 'actual_argv': sys.argv})
            except OSError:
                pass
        print('example root execution refused: ' + str(error), file=sys.stderr)
        sys.exit(1)
