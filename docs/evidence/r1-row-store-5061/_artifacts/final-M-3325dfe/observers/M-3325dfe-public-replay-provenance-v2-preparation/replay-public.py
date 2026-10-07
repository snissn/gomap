#!/usr/bin/env python3
"""Root-run public-byte/validator replay. No builds, captures, or acceptance decisions."""
import argparse
import datetime as dt
import fcntl
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import stat
import subprocess
import sys
import time
import urllib.parse
import urllib.request

SCHEMA = 'gomap-r1-public-replay-root-inputs-v2'
REPO = 'https://github.com/snissn/gomap.git'
ARTIFACT = 'docs/evidence/r1-row-store-5061/_artifacts'
M = '3325dfe77940fec8587d8b61b1ac4e0b2f72caca'
MTREE = '0600b47bf0a3a609917d224d7fd06c40737bcd8e'
HELPERS = {
 'restorer': '66f6c4096014322e3407b3efc5959751f534307ebe9a3e272b6148eba2f40ea6',
 'certified_A_compare': '3c4502ebb4620ac010b615ca10bbdd45a4448f66e2c7ed663d57c32413c1c2c2',
 'original_A_compare': '1a54cac528e5c8acc2501ec67b13d9a185b011e1467856741ada320a8bfdcfc8',
 'original_D_validator': 'dcfd45e233bc46a1c1f73ffaf2af840e5d8ab8ba312342ceb61b014e7c199968',
}
HIST_A = [
 'comparator/baseline/0216e2e',
 'comparator/historical-candidate/f95ea1a',
 'comparator/final-typed-scope/00d2c370',
]
HIST_D = ['rejected/' + x for x in (
 'profile-postgc-7adf81c2', 'growth-order-1e7882', 'profile-v3-rejected',
 'manager-red-c4d49', 'revision-red-a21d5', 'source-guard-mismatch',
 'clean-runtime-604', 'frozen-validator-12f3bed', 'original-context',
 'actual-size-b53e3ae-physical-growth')]
OBS = ('root-frozen-inputs.json', 'observer-before.json', 'build-start.json',
 'build-completed.json', 'run-start.json', 'run-exit.json', 'runner-before.json',
 'runner-after.json', 'validation-start.json', 'validation-exit.json',
 'independent-validation.log', 'independent-expected-source.json')

class Refusal(Exception):
    pass

ACTIVE_RECORDER = None

def need(ok, msg):
    if not ok:
        raise Refusal(msg)

def hexpin(value, width=64):
    need(isinstance(value, str) and re.fullmatch('[0-9a-f]{%d}' % width, value),
         'missing/invalid %d-character pin' % width)
    return value

def rel(value):
    need(isinstance(value, str) and value and all(
        re.fullmatch(r'[A-Za-z0-9_.-]+', p) and p not in ('.', '..')
        for p in value.split('/')), 'unsafe/missing relative path: %r' % value)
    return value

def canonical(value, existing=False):
    p = Path(value)
    need(p.is_absolute() and '..' not in p.parts, 'noncanonical absolute path')
    for q in (p, *p.parents):
        need(not q.is_symlink(), 'symlink path: ' + str(q))
    need(not existing or p.exists(), 'missing path: ' + str(p))
    return p

def historical_absolute(value):
    """Original capture/receipt paths are metadata: never touch their filesystem."""
    need(isinstance(value, str) and Path(value).is_absolute() and '..' not in Path(value).parts,
         'missing/noncanonical original absolute-path metadata')

def regular(p):
    canonical(str(p), True)
    need(stat.S_ISREG(p.lstat().st_mode), 'not a regular file: ' + str(p))
    return p

def digest(p):
    regular(p)
    h = hashlib.sha256()
    with p.open('rb') as f:
        for b in iter(lambda: f.read(1048576), b''):
            h.update(b)
    return h.hexdigest()

def pinned(p, expected):
    need(digest(p) == hexpin(expected), 'byte pin mismatch: ' + str(p))
    return p

def pairs(xs):
    d = {}
    for k, v in xs:
        need(k not in d, 'duplicate JSON key: ' + k)
        d[k] = v
    return d

def load(p):
    return json.loads(regular(p).read_text(encoding='utf-8'), object_pairs_hook=pairs,
                      parse_constant=lambda x: (_ for _ in ()).throw(Refusal('nonfinite JSON')))

def write(p, obj):
    with p.open('x', encoding='utf-8') as f:
        json.dump(obj, f, indent=2, sort_keys=True, allow_nan=False)
        f.write('\n')

def files(root):
    out = {}
    for base, dirs, names in os.walk(root, followlinks=False):
        for name in dirs:
            need(not (Path(base) / name).is_symlink(), 'symlink directory')
        for name in names:
            p = regular(Path(base) / name)
            out[p.relative_to(root).as_posix()] = digest(p)
    return out

def ledger(p, expected):
    pinned(p, expected)
    d = {}
    for line in p.read_text(encoding='utf-8').splitlines():
        need(re.fullmatch(r'[0-9a-f]{64}  .+', line), 'malformed ledger')
        h, name = line.split('  ', 1)
        rel(name)
        need(name not in d, 'duplicate ledger member')
        pinned(p.parent / name, h)
        d[name] = h
    need(d, 'empty ledger')
    return d

def pin_fields(row, names):
    for n in names:
        hexpin(row[n], 40 if n in ('commit', 'landed_tooling_commit') else 64)

def validate(c):
    need(c['schema'] == SCHEMA and c['root_approved'] is True, 'complete root-approved v2 inputs required')
    need(c['public_repository_https'] == REPO and c['artifact_repo_relative'] == ARTIFACT,
         'unexpected public repository/artifact path')
    hexpin(c['public_artifact_git_sha'], 40)
    need(c['public_artifact_landing_reachable'] is True and c['selected_actual_M_commit'] == M
         and c['selected_actual_M_tree'] == MTREE, 'missing/wrong public landing or actual M')
    for key in ('core_SHA256SUMS_sha256', 'PUBLICATION_SHA256SUMS_sha256',
                'actual_M_landing_record_sha256'):
        hexpin(c[key])
    rel(c['actual_M_landing_record_stage_relative'])
    need(c['support_ledger_list_complete'] is True, 'support list not independently approved complete')
    need(c['support_ledgers'] and len({x['stage_relative'] for x in c['support_ledgers']})
         == len(c['support_ledgers']), 'missing/duplicate support ledgers')
    for x in c['support_ledgers']:
        rel(x['stage_relative']); hexpin(x['sha256'])
        need(Path(x['stage_relative']).name == 'SUPPORT_SHA256SUMS', 'wrong support ledger leaf')
    o = c['overlay']
    need(o['stage_leaf'] == 'binary-overlay.tar.gz' and type(o['bytes']) is int and o['bytes'] > 0,
         'overlay bytes required')
    hexpin(o['sha256']); hexpin(o['member_index_sha256'])
    u = urllib.parse.urlsplit(o['release_https_url'])
    need(u.scheme == 'https' and u.hostname == 'github.com' and u.username is None
         and u.path.startswith('/snissn/gomap/releases/download/') and not u.fragment,
         'original public HTTPS release URL required')
    f = c['fresh_linux111']
    for key in ('checkout_absolute', 'restored_absolute', 'proof_absolute', 'timed_lock',
                'actual_host_identity_record'):
        canonical(f[key])
    hexpin(f['actual_host_identity_record_sha256'])
    need(f['no_private_capture_source'] is True, 'private capture sources forbidden')
    need(type(c['command_timeout_seconds']) is int and 1 <= c['command_timeout_seconds'] <= 1800,
         'bounded command timeout required')
    need(c['replay_environment_contract'] == {
        'PYTHONDONTWRITEBYTECODE': '1', 'PYTHONOPTIMIZE': 'unset',
        'GOMAXPROCS': '16', 'GOGC': '100', 'GOMEMLIMIT': 'off', 'GODEBUG': ''},
         'replay environment contract changed')
    for key, h in HELPERS.items():
        v = c['frozen_validator_public_paths'][key]
        rel(v['stage_relative']); need(v['sha256'] == h, 'immutable helper pin changed')
    need([x['path'] for x in c['historical_accepted_A']] == HIST_A,
         'historical accepted A set/order changed')
    for row in c['historical_accepted_A']:
        need(row['status'] == 'accepted' and row['kind'] == 'comparator', 'historical A relabeled')
        pin_fields(row, ('binary_sha256', 'packet_sha256', 'independently_verified_source_sha256'))
        pin_fields(row['expected'], ('commit', 'runtime_sha256', 'harness_sha256'))
        rel(row['independently_verified_source_stage_relative'])
    need([x['path'] for x in c['historical_rejected_D']] == HIST_D, 'historical rejected D set changed')
    need(all(x['status'] == 'rejected' and x['fresh_replay_classification'] == 'raw-only'
             for x in c['historical_rejected_D']), 'historical D replay/status changed')
    for label in ('A', 'C', 'D'):
        x = c['actual_lanes'][label]
        need(x['root_accepted'] is True, 'original actual lane not root accepted: ' + label)
        rel(x['capture_restored_relative']); rel(x['public_receipts_stage_relative'])
        pin_fields(x['external_pins'], ('commit', 'landed_tooling_commit', 'runtime_sha256',
                                      'harness_sha256', 'binary_sha256', 'packet_sha256'))
        need(x['external_pins']['commit'] == x['external_pins']['landed_tooling_commit'] == M,
             'actual lane not exact M')
        need(x['original_binary_name'] == ('collections.test' if label == 'D' else 'collection_workload_bench'),
             'wrong retained binary')
        for name in ('public_independent_expected_source', 'trusted_completed_receipt',
                     'original_validation_start', 'original_validation_exit',
                     'original_validation_log', 'root_acceptance'):
            rel(x[name + '_stage_relative'])
            hexpin(x['independent_expected_source_sha256'] if name == 'public_independent_expected_source'
                   else x[name + '_sha256'])
        hexpin(x['source_before_sha256']); hexpin(x['source_after_sha256'])
        need(isinstance(x['original_validation_argv'], list) and x['original_validation_argv']
             and all(isinstance(v, str) and v for v in x['original_validation_argv']),
             'original observer validation argv required')
        historical_absolute(x['original_capture_root']); historical_absolute(x['original_receipt_root'])
    a = c['A_certificate']
    need(a['approved'] is True, 'A certificate not independently approved')
    rel(a['stage_relative']); hexpin(a['independently_root_approved_sha256'])
    for name in ('actual_accepted_freeze', 'approval_record'):
        rel(a[name + '_stage_relative']); hexpin(a[name + '_sha256'])
    need(set(a['selected_observation_sha256']) == set(OBS), 'all 12 original A receipt byte pins required')
    for h in a['selected_observation_sha256'].values(): hexpin(h)
    need(a['selected_harness_blobs'], 'approved selected harness blobs absent')
    for h in a['selected_harness_blobs'].values(): hexpin(h, 40)
    for h in a['reviewed_changed_path_diff_sha256'].values(): hexpin(h)
    s = c['required_final_selection']
    need(s['base13_records_preserved'] is True and s['fresh_actual_A_C_D_records_added'] is True
         and s['record_count'] == 16, 'final original13 plus actual3 selection required')
    rel(s['selection_stage_relative']); hexpin(s['selection_sha256'])
    rel(s['base13_stage_relative']); hexpin(s['base13_sha256'])

class Recorder:
    def __init__(self, proof, env, timeout):
        global ACTIVE_RECORDER
        self.proof, self.env, self.timeout = proof, env, timeout
        self.inputs, self.commands = {}, []
        proof.mkdir(parents=True, exist_ok=False)
        ACTIVE_RECORDER = self
    def add(self, p):
        self.inputs[str(p)] = digest(p)
    def check(self):
        for p, h in self.inputs.items(): pinned(Path(p), h)
    def run(self, label, argv, cwd, stdin=None):
        self.check()
        n = '%02d-%s' % (len(self.commands) + 1, label)
        start = {'argv': list(map(str, argv)), 'cwd': str(cwd), 'environment': self.env,
                 'utc': dt.datetime.now(dt.timezone.utc).isoformat(), 'monotonic_ns': time.monotonic_ns(),
                 'input_before_sha256': dict(self.inputs)}
        if stdin is not None:
            start['stdin_sha256'] = hashlib.sha256(stdin).hexdigest()
            start['stdin_bytes'] = len(stdin)
            (self.proof / (n + '-stdin.raw')).write_bytes(stdin)
        write(self.proof / (n + '-start.json'), start)
        out, err = self.proof / (n + '-stdout.log'), self.proof / (n + '-stderr.log')
        code, failure = None, None
        try:
            with out.open('xb') as stdout, err.open('xb') as stderr:
                code = subprocess.run(start['argv'], cwd=cwd, env=self.env, stdout=stdout,
                                      stderr=stderr, input=stdin, timeout=self.timeout, check=False).returncode
        except Exception as e:
            failure = type(e).__name__ + ': ' + str(e)
        end = {'utc': dt.datetime.now(dt.timezone.utc).isoformat(), 'monotonic_ns': time.monotonic_ns(),
               'exit_code': code, 'execution_error': failure,
               'stdout': {'sha256': digest(out), 'bytes': out.stat().st_size},
               'stderr': {'sha256': digest(err), 'bytes': err.stat().st_size}}
        end['input_after_sha256'] = {}
        for p in self.inputs:
            try: end['input_after_sha256'][p] = digest(Path(p))
            except Exception as e: end['input_after_sha256'][p] = {'error': str(e)}
        try:
            self.check()
        except Exception as e:
            end['input_integrity_error'] = str(e)
        write(self.proof / (n + '-exit.json'), end)
        self.commands.append({'label': label, 'record_prefix': n, 'exit_code': code})
        need(code == 0 and failure is None and 'input_integrity_error' not in end,
             'command failed or input changed: ' + n)
        return out.read_bytes()

def manifest_hash(mapping):
    h = hashlib.sha256()
    for p, identity in sorted(mapping.items()):
        h.update(p.encode() + b'\0' + identity.encode() + b'\0')
    return h.hexdigest()

def verify_public_source(r, repo, source, lifecycle=False):
    """Derive source identity from public Git objects; never run Go/package discovery."""
    commit = hexpin(source['commit'], 40)
    raw = r.run('source-tree-' + commit[:8], ['git', '-C', str(repo), 'ls-tree', '-r', '-z', commit], repo)
    blobs = {}
    for row in raw.split(b'\0'):
        if not row: continue
        metadata, path = row.split(b'\t', 1)
        mode, kind, blob = metadata.split()
        if kind == b'blob': blobs[path.decode()] = blob.decode()
    runtime = {p: b for p, b in blobs.items() if p in ('go.mod', 'go.sum') or (
        p.startswith(('TreeDB/', 'cmd/internal/treedbstats/'))
        and Path(p).suffix in ('.go', '.s', '.S', '.c', '.h', '.syso')
        and not p.endswith('_test.go'))}
    need(source['runtime_blobs'] == runtime and source['runtime_sha256'] == manifest_hash(runtime),
         'independent manifest runtime inventory differs from public Git objects')
    if lifecycle:
        tests = source['compiled_test_files']
        need(tests == sorted(set(tests)) and tests and all(
            p.startswith('TreeDB/collections/') and p.endswith('_test.go') and p in blobs for p in tests),
            'root-pinned D compiled test list invalid')
        paths = sorted(set(tests) | {p for p in blobs if p.startswith('scripts/r1_lifecycle')
                       and Path(p).suffix in ('.py', '.sh')} | {'scripts/r1_collection_source.py'})
        need(set(source['harness_files']) == set(paths), 'D harness file set differs from public M/test receipt')
        wanted = sorted(set(paths) | set(runtime))
    else:
        paths = ['cmd/collection_workload_bench/main.go', *sorted(p for p in blobs
                 if p.startswith('cmd/collection_workload_bench/r1') and p.endswith('.go')
                 and not p.endswith('_test.go')), 'scripts/r1_collection_capture.sh',
                 'scripts/r1_collection_summary.py', 'scripts/r1_collection_source.py']
        wanted = paths
    request = ''.join(blobs[p] + '\n' for p in wanted).encode()
    batch = r.run('source-blobs-' + commit[:8], ['git', '-C', str(repo), 'cat-file', '--batch'], repo, request)
    offset, contents = 0, {}
    for p in wanted:
        newline = batch.find(b'\n', offset)
        need(newline >= offset, 'truncated Git cat-file header')
        blob, kind, count = batch[offset:newline].split()
        size = int(count); offset = newline + 1
        need(blob.decode() == blobs[p] and kind == b'blob' and size >= 0, 'Git blob response mismatch')
        contents[p] = batch[offset:offset + size]
        need(len(contents[p]) == size and batch[offset + size:offset + size + 1] == b'\n', 'truncated Git blob')
        offset += size + 1
    need(offset == len(batch), 'extra Git blob data')
    if lifecycle:
        runtime_files = {p: hashlib.sha256(contents[p]).hexdigest() for p in runtime}
        harness_files = {p: hashlib.sha256(contents[p]).hexdigest() for p in paths}
        need(source['runtime_files'] == runtime_files and source['runtime_working_sha256'] == manifest_hash(runtime_files),
             'D observed runtime bytes differ from public Git objects')
        need(source['harness_files'] == harness_files and source['harness_sha256'] == manifest_hash(harness_files),
             'D observed harness bytes differ from public Git objects')
        need(harness_files['scripts/r1_lifecycle_validate.py'] == HELPERS['original_D_validator'],
             'frozen D parser differs from original actual M source')
    else:
        h = hashlib.sha256()
        for p in paths: h.update(p.encode() + b'\0' + contents[p] + b'\0')
        need(source['harness_sha256'] == h.hexdigest(), 'A/C harness differs from public Git objects')


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--inputs', required=True)
    ap.add_argument('--expected-inputs-sha256', required=True)
    args = ap.parse_args()
    need(__debug__ and not os.environ.get('PYTHONOPTIMIZE'), 'optimized Python forbidden')
    inp = pinned(canonical(args.inputs, True), args.expected_inputs_sha256)
    c = load(inp); validate(c)
    need(platform.system() == 'Linux' and platform.machine() in ('x86_64', 'AMD64'), 'fresh Linux amd64 required')
    f = c['fresh_linux111']
    repo, out, proof = (canonical(f[k]) for k in ('checkout_absolute', 'restored_absolute', 'proof_absolute'))
    for a, b in ((repo, out), (repo, proof), (out, proof)):
        need(a != b and a not in b.parents and b not in a.parents, 'overlapping public/proof paths')
    need(repo.is_dir() and not out.exists() and not proof.exists(), 'public checkout required and outputs must be fresh')
    need(canonical(f['timed_lock'], True).is_file(), 'existing canonical Linux111 timed lock required')
    env = {'PATH': os.environ.get('PATH', '/usr/bin:/bin'), 'HOME': str(proof),
           'LANG': 'C.UTF-8', 'LC_ALL': 'C.UTF-8', 'PYTHONDONTWRITEBYTECODE': '1',
           'GOMAXPROCS': '16', 'GOGC': '100', 'GOMEMLIMIT': 'off', 'GODEBUG': '',
           'GIT_CONFIG_NOSYSTEM': '1', 'GIT_CONFIG_GLOBAL': '/dev/null',
           'GIT_TERMINAL_PROMPT': '0'}
    r = Recorder(proof, env, c['command_timeout_seconds'])
    r.add(inp)
    host = pinned(canonical(f['actual_host_identity_record'], True), f['actual_host_identity_record_sha256'])
    r.add(host)
    write(proof / 'execution-host.json', {'platform': platform.platform(), 'uname': list(platform.uname()),
        'hostname': platform.node(), 'host_identity_input': str(host), 'host_identity_sha256': digest(host),
        'classification': 'actual local identity; no physical-host equivalence claim'})
    with canonical(f['timed_lock'], True).open('r+b') as lock:
        try: fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError: raise Refusal('canonical timed lock busy; no waiting or concurrent capture')
        def git(label, *argv): return r.run(label, ['git', '-C', str(repo), *argv], repo)
        need(git('public-remote', 'remote', 'get-url', 'origin').decode().strip() == REPO,
             'checkout origin not exact public HTTPS repository')
        need(git('public-head', 'rev-parse', 'HEAD').decode().strip() == c['public_artifact_git_sha'],
             'public artifact checkout SHA mismatch')
        commits = list(dict.fromkeys([c['public_artifact_git_sha'], M,
                      c['A_certificate']['baseline']['commit'],
                      *[x['expected']['commit'] for x in c['historical_accepted_A']]]))
        for commit in commits:
            git('fetch-public-' + commit[:8], '-c', 'credential.helper=', 'fetch', '--no-tags', REPO, hexpin(commit, 40))
            need(git('object-' + commit[:8], 'rev-parse', commit + '^{commit}').decode().strip() == commit,
                 'public Git object mismatch')
        need(git('actual-M-tree', 'rev-parse', M + '^{tree}').decode().strip() == MTREE, 'actual M tree mismatch')
        stage = canonical(str(repo / ARTIFACT), True)
        overlay = stage / 'binary-overlay.tar.gz'
        need(not overlay.exists(), 'overlay must be freshly downloaded, never reused/copied from private capture')
        o = c['overlay']; started = dt.datetime.now(dt.timezone.utc).isoformat()
        partial = proof / 'https-overlay-download.partial'
        request = urllib.request.Request(o['release_https_url'], headers={'User-Agent': 'gomap-r1-public-replay'})
        with urllib.request.urlopen(request, timeout=60) as response, partial.open('xb') as dest:
            final_url = response.geturl()
            need(urllib.parse.urlsplit(final_url).scheme == 'https', 'non-HTTPS release redirect')
            while True:
                block = response.read(1048576)
                if not block: break
                dest.write(block)
                need(dest.tell() <= o['bytes'], 'release overlay exceeds approved byte size')
        pinned(partial, o['sha256']); need(partial.stat().st_size == o['bytes'], 'overlay size mismatch')
        with partial.open('rb') as src, overlay.open('xb') as dst:
            for block in iter(lambda: src.read(1048576), b''): dst.write(block)
        write(proof / 'https-overlay-download.json', {'original_https_url': o['release_https_url'],
            'final_https_url': final_url, 'start_utc': started, 'end_utc': dt.datetime.now(dt.timezone.utc).isoformat(),
            'sha256': digest(overlay), 'bytes': overlay.stat().st_size,
            'source': 'actual HTTPS response; no private capture copying'})
        stage_files = files(stage)
        pub = ledger(stage / 'PUBLICATION_SHA256SUMS', c['PUBLICATION_SHA256SUMS_sha256'])
        need(set(pub) == set(stage_files) - {'PUBLICATION_SHA256SUMS'}, 'PUBLICATION ledger incomplete or extra public files')
        core = ledger(stage / 'SHA256SUMS', c['core_SHA256SUMS_sha256'])
        support = {x['stage_relative']: x['sha256'] for x in c['support_ledgers']}
        need(set(support) == {p for p in stage_files if Path(p).name == 'SUPPORT_SHA256SUMS'},
             'independently selected complete SUPPORT ledger set mismatch')
        for p, h in support.items(): ledger(stage / p, h)
        pinned(stage / 'binary-member-index.json', o['member_index_sha256'])
        need(core.get('binary-overlay.tar.gz') == o['sha256'], 'core overlay pin differs')
        tracked = {}
        for row in git('public-artifact-tree', 'ls-tree', '-r', '-z', 'HEAD', '--', ARTIFACT).split(b'\0'):
            if not row: continue
            metadata, path = row.split(b'\t', 1); mode, kind, blob = metadata.split()
            name = path.decode()[len(ARTIFACT) + 1:]
            need(kind == b'blob' and mode == b'100644', 'public stage symlink/executable/submodule')
            tracked[name] = blob.decode()
        need(set(tracked) == set(stage_files) - {'binary-overlay.tar.gz'}, 'public stage does not exactly match public Git tracked tree')
        for name, blob in tracked.items():
            p = stage / name; raw = p.read_bytes()
            need(hashlib.sha1(b'blob ' + str(len(raw)).encode() + b'\0' + raw).hexdigest() == blob,
                 'public file differs from checked-out Git object: ' + name)
        for name in stage_files: r.add(stage / name)
        helper = {}
        for name, row in c['frozen_validator_public_paths'].items():
            helper[name] = pinned(stage / rel(row['stage_relative']), row['sha256'])
        pinned(stage / c['actual_M_landing_record_stage_relative'], c['actual_M_landing_record_sha256'])
        s = c['required_final_selection']
        selection = load(pinned(stage / s['selection_stage_relative'], s['selection_sha256']))
        base = load(pinned(stage / s['base13_stage_relative'], s['base13_sha256']))
        need(len(base) == 13 and len(selection) == 16 and selection[:13] == base,
             'original thirteen selection rows not preserved byte-semantically as prefix')
        need({x['path'] for x in base if x['status'] == 'accepted'} == set(HIST_A)
             and {x['path'] for x in base if x['status'] == 'rejected'} == set(HIST_D), 'historical selection statuses changed')
        need({x['path'] for x in selection[13:]} == {c['actual_lanes'][x]['capture_restored_relative'] for x in ('A', 'C', 'D')}
             and all(x['status'] == 'accepted' for x in selection[13:]), 'actual three selection rows absent')
        manifest = load(stage / 'publication-manifest.json')
        need(manifest['packets_rewritten'] is False and len(manifest['captures']) == 16,
             'original publication manifest capture count/rewrite claim differs')
        for selected, captured in zip(selection, manifest['captures']):
            for key in ('root', 'path', 'kind', 'status'):
                need(captured[key] == selected[key], 'manifest/selected original classification disagreement')
            need(captured['expected'] == selected.get('expected', {})
                 and captured['recorded_validation'] == selected.get('validation'),
                 'manifest/selected original expectations or validator provenance differ')
        r.run('original-restorer', [sys.executable, str(helper['restorer']), '--stage', str(stage), '--out', str(out)], repo)
        for name, h in core.items(): pinned(out / name, h)
        for row in load(stage / 'binary-member-index.json'):
            p = pinned(out / rel(row['path']), row['sha256'])
            need(p.stat().st_size == row['size'], 'restored ELF size mismatch'); r.add(p)
        for p in files(out): r.add(out / p)
        def source(row, capture, source_path, pins, lifecycle=False):
            before = capture / ('source-before.json' if lifecycle else 'source.json')
            after = capture / 'source-after.json'
            expected = load(source_path); packet = load(capture / 'packet.json')
            need(load(before) == load(after) == expected == packet['source_before' if lifecycle else 'source'],
                 'independent expected source/capture before-after/packet disagreement')
            for k in ('commit', 'runtime_sha256', 'harness_sha256'):
                need(expected[k] == pins[k], 'independent external source pin mismatch')
            if lifecycle:
                need(packet['source_after'] == expected and expected['clean'] is True, 'D source after/clean mismatch')
            else:
                need(expected['clean'] is True, 'A/C original source is not clean')
            verify_public_source(r, repo, expected, lifecycle)
            return expected
        for h in c['historical_accepted_A']:
            capture = out / h['path']; binary = pinned(capture / 'collection_workload_bench', h['binary_sha256'])
            need(binary.read_bytes()[:4] == b'\x7fELF', 'historical original executable not ELF')
            pinned(capture / 'packet.json', h['packet_sha256'])
            expected = pinned(stage / h['independently_verified_source_stage_relative'], h['independently_verified_source_sha256'])
            source(h, capture, expected, h['expected'])
            r.run('historical-A-' + Path(h['path']).name,
                  [str(binary), 'r1-validate', '-source-manifest', str(expected), str(capture / 'packet.json')], repo)
        translations = []
        for label in ('A', 'C', 'D'):
            x = c['actual_lanes'][label]; p = x['external_pins']; capture = out / x['capture_restored_relative']
            binary = pinned(capture / x['original_binary_name'], p['binary_sha256'])
            need(binary.read_bytes()[:4] == b'\x7fELF', 'actual retained executable not ELF')
            packet = pinned(capture / 'packet.json', p['packet_sha256'])
            expected = pinned(stage / x['public_independent_expected_source_stage_relative'], x['independent_expected_source_sha256'])
            source(x, capture, expected, p, label == 'D')
            pinned(capture / ('source-before.json' if label == 'D' else 'source.json'), x['source_before_sha256'])
            pinned(capture / 'source-after.json', x['source_after_sha256'])
            for key in ('trusted_completed_receipt', 'root_acceptance', 'original_validation_start',
                        'original_validation_exit', 'original_validation_log'):
                pinned(stage / x[key + '_stage_relative'], x[key + '_sha256'])
            start = load(stage / x['original_validation_start_stage_relative'])
            end = load(stage / x['original_validation_exit_stage_relative'])
            need(start['argv'] == x['original_validation_argv'] and end['exit'] == 0,
                 'original independent validator argv/exit receipt disagreement')
            trusted = load(stage / x['trusted_completed_receipt_stage_relative'])
            need(trusted['exit'] == 0 and trusted['capture_directory'] == x['original_capture_root'],
                 'original completed capture receipt failed/root changed')
            for key in ('commit', 'landed_tooling_commit', 'runtime_sha256', 'harness_sha256',
                        'binary_sha256', 'packet_sha256'):
                need(trusted['source_commit' if key == 'commit' else key] == p[key],
                     'trusted original six-pin receipt differs from independent root inputs')
            if label == 'A':
                need(end['binary_sha256'] == p['binary_sha256'] and end['packet_sha256'] == p['packet_sha256']
                     and end['validation_log_sha256'] == x['original_validation_log_sha256'],
                     'A validation exit does not bind original binary/packet/raw log')
            if label == 'A':
                argv = [str(binary), 'r1-validate', '-source-manifest', str(expected), str(packet)]
            elif label == 'C':
                argv = [str(binary), 'r1-mutation-sweep-validate', '-source-manifest', str(expected)]
            else:
                argv = [sys.executable, str(helper['original_D_validator']), str(packet)]
            if label in ('C', 'D'):
                prefix = '-' if label == 'C' else '--'
                for flag, key in (('expected-commit', 'commit'), ('expected-runtime', 'runtime_sha256'),
                    ('expected-harness', 'harness_sha256'), ('expected-landed-tooling-commit', 'landed_tooling_commit'),
                    ('expected-binary-sha256', 'binary_sha256'), ('expected-packet-sha256', 'packet_sha256')):
                    argv += [prefix + flag, p[key]]
                if label == 'C': argv += [str(packet)]
            translations.append({'lane': label, 'preserved_original_capture_root': x['original_capture_root'],
                'preserved_original_receipt_root': x['original_receipt_root'], 'original_validation_argv': start['argv'],
                'new_replay_argv': argv, 'new_capture_root': str(capture),
                'new_public_receipt_root': str(stage / x['public_receipts_stage_relative'])})
            r.run('actual-' + label + '-original-validator', argv, repo)
        a = c['A_certificate']; receipts = stage / c['actual_lanes']['A']['public_receipts_stage_relative']
        for name, h in a['selected_observation_sha256'].items(): pinned(receipts / name, h)
        cert = pinned(stage / a['stage_relative'], a['independently_root_approved_sha256'])
        certificate = load(cert)
        need(certificate['baseline'] == a['baseline'], 'approved baseline certificate differs from root inputs')
        need(certificate['selected']['commit'] == M
             and certificate['selected']['observation_sha256'] == a['selected_observation_sha256']
             and certificate['selected_harness_blobs'] == a['selected_harness_blobs']
             and certificate['reviewed_changed_path_diff_sha256'] == a['reviewed_changed_path_diff_sha256'],
             'root independently approved certificate fields differ')
        freeze = pinned(stage / a['actual_accepted_freeze_stage_relative'], a['actual_accepted_freeze_sha256'])
        pinned(stage / a['approval_record_stage_relative'], a['approval_record_sha256'])
        r.run('certified-A-descriptive-comparison', [sys.executable, str(helper['certified_A_compare']),
            '--certificate', str(cert), '--expected-certificate-sha256', a['independently_root_approved_sha256'],
            '--repo', str(repo), '--before', str(out / HIST_A[0] / 'packet.json'),
            '--after', str(out / c['actual_lanes']['A']['capture_restored_relative'] / 'packet.json'),
            '--receipts', str(receipts), '--accepted-freeze', str(freeze),
            '--original-compare', str(helper['original_A_compare'])], repo)
        r.check()
        write(proof / 'restored-path-translations.json', translations)
        write(proof / 'public-replay-result.json', {
            'status': 'PUBLIC_BYTES_AND_REQUESTED_ORIGINAL_VALIDATOR_REPLAY_COMPLETED',
            'acceptance': 'NONE; root alone assesses actual original captures and closure criteria',
            'actual_M': M, 'actual_M_tree': MTREE, 'public_artifact_git_sha': c['public_artifact_git_sha'],
            'root_inputs_sha256': digest(inp), 'commands': r.commands,
            'input_final_sha256': r.inputs,
            'historical_A_status': 'three original accepted rows preserved; original ELF validation replayed',
            'historical_D_status': 'ten original rejected rows preserved; raw-byte replay only; no current parser reinterpretation',
            'D_executable': 'hash and ELF magic verified; not executed',
            'new_benchmarks_or_builds': False, 'performance_or_capacity_acceptance': False})

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        if ACTIVE_RECORDER is not None:
            try:
                write(ACTIVE_RECORDER.proof / 'public-replay-failed.json', {
                    'status': 'FAILED_OR_REFUSED', 'error': type(e).__name__ + ': ' + str(e),
                    'utc': dt.datetime.now(dt.timezone.utc).isoformat(),
                    'completed_command_records': ACTIVE_RECORDER.commands,
                    'acceptance': 'NONE', 'historical_statuses_changed': False})
            except Exception:
                pass
        print(type(e).__name__ + ': ' + str(e), file=sys.stderr)
        sys.exit(1)
