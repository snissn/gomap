import argparse
import datetime
import hashlib
import json
import re
import tarfile
from pathlib import Path

D = Path('/mnt/fast4tb/gomap-mvcc-4871-20261015')
P = argparse.ArgumentParser()
P.add_argument('packet')
P.add_argument('manifest_sha256')
P.add_argument('baseline')
P.add_argument('changed_path')
P.add_argument('grant')
P.add_argument('executed', type=int)
P.add_argument('outcome', choices=['PASS', 'STOP'])
a = P.parse_args()
B = D / 'native-main-integration/private-hash-capacity' / a.packet
C = B / 'controls'
base = B.parent / a.baseline

def j(p):
    return json.loads(p.read_text())

def sha(p):
    h = hashlib.sha256()
    with p.open('rb') as f:
        for b in iter(lambda: f.read(1024 * 1024), b''):
            h.update(b)
    return h.hexdigest()

def archive_hashes(p):
    out = {}
    with tarfile.open(p, 'r:gz') as tf:
        for m in tf:
            assert m.isfile(), m.name
            assert m.name not in out and not m.name.startswith('/') and '..' not in Path(m.name).parts
            out[m.name] = hashlib.sha256(tf.extractfile(m).read()).hexdigest()
    return out

m = j(B / 'source-frozen.json')
assert len(m) == 10389 and sha(B / 'source-frozen.json') == a.manifest_sha256
f = j(B / 'freeze-receipt.json')
assert sha(base / 'source-frozen.json') == f['baseline_manifest_sha256']
previous = j(base / 'source-frozen.json')
assert set(previous) == set(m)
changed = [n for n in m if previous[n] != m[n]]
assert changed == [a.changed_path], changed
assert f['changed_paths'] == f['modified_paths'] == 1 and f['added_paths'] == f['deleted_paths'] == 0
assert sha(B / 'source-frozen.tar.gz') == f['source_archive_sha256']
assert archive_hashes(B / 'source-frozen.tar.gz') == m
assert sha(B / 'changed-source.tar.gz') == f['changed_source_archive_sha256']
assert archive_hashes(B / 'changed-source.tar.gz') == {n: m[n] for n in changed}
assert sha(B / 'source-delta.patch') == f['source_delta_sha256']
assert sha(B / 'changed-paths.json') == f['changed_paths_sha256']
for n, h in f['selections'].items():
    assert sha(D / 'native-decision' / n) == h

plan = j(B / 'proposed-commands.json')
commands = plan['commands']
assert all(m[n] == h for n, h in plan['selected_test_files'].items())
i = j(C / 'identity.json')
assert i['archive_sha256'] == f['source_archive_sha256']
assert i['archive_verified_files'] == len(m) and i['subreaper_verified']
assert i['go_binary_sha256'] == sha(D / 'toolchains/go1.26.3/bin/go') == 'd68b7abbc40d0844f673f6cf06ae3cded225c50437c6454fa37ef178d079fe65'
assert i['compile_binary_sha256'] == sha(D / 'toolchains/go1.26.3/pkg/tool/linux_amd64/compile') == '3bb421d581f60b9ab48c2c73adf5c018557e89af32ad018d9c4417afa4bf90c1'
assert i['runner_sha256'] == sha(B / 'run-controls.py')
assert i['proposed_commands_sha256'] == sha(B / 'proposed-commands.json')
assert i['grant_sha256'] == sha(D / 'native-decision' / a.grant)
assert all(m[n] == h for n, h in i['selected_test_files'].items())
assert all(sha(D / 'native-decision' / n) == h for n, h in i['selection_bindings'].items())
assert not Path('/proc/' + str(i['runner_pid'])).exists()
s = j(C / 'summary.json')
assert s['commands_executed'] == a.executed
assert s['commands_not_executed'] == [x['name'] for x in commands[a.executed:]]
assert s['source_manifest_sha256'] == a.manifest_sha256
assert s['all_groups_empty'] and s['source_still_frozen']
assert not s['actual33_executed'] and not s['native_publication_runtime']
aggregate_discrepancy = None
if s['all_authorized_pass'] != (a.outcome == 'PASS'):
    # Preserve the actual runner result. This known summary-only defect arose
    # when one explicitly reused normal call reduced this plan from 12 to 11.
    assert a.packet == 'source-correction03' and a.outcome == 'PASS'
    assert a.executed == len(commands) == 11 and not s['all_authorized_pass']
    assert "'all_authorized_pass':len(results)==12 and all(x['status']=='PASS' for x in results)" in (B / 'run-controls.py').read_text()
    aggregate_discrepancy = 'Raw summary retains false from stale literal 12; all 11 authorized calls are independently verified below.'
if a.outcome == 'STOP':
    assert s['stopped_at_first_failure']
rows = []
for ix, expected in enumerate(commands[:a.executed]):
    V = C / expected['name']
    command = j(V / 'command.json')
    for k, v in expected.items():
        if k != 'executed':
            assert command[k] == v, (k, command[k], v)
    assert command['executed'] and command['grant_sha256'] == i['grant_sha256']
    args = command['argv']
    assert args[0] == str(D / 'toolchains/go1.26.3/bin/go')
    assert args[1:3] == ['test', '-p=1'] and args[-3:] == ['-count=1', '-v', '-timeout=3m']
    assert ('-race' in args) == expected['name'].startswith('race-')
    assert args[args.index('-run') + 1] == '^(' + '|'.join(command['required_top_level_tests']) + ')$'
    assert command['outer_timeout_seconds'] == 240
    assert command['cwd'] == str(D / 'native-integrated')
    env = command['env']
    fixed = {'CGO_ENABLED':'1', 'GODEBUG':'', 'GOENV':'off', 'GOFLAGS':'', 'GOGC':'100', 'GOMAXPROCS':'2', 'GOMEMLIMIT':'8GiB', 'GOPROXY':'off', 'GOSUMDB':'off', 'GOTOOLCHAIN':'local', 'GOWORK':'off'}
    assert all(env[k] == v for k, v in fixed.items())
    assert env['GOROOT'] == str(D / 'toolchains/go1.26.3')
    assert all(str(D) in env[k] for k in ['GOCACHE', 'GOMODCACHE', 'TMPDIR'])
    assert j(V / 'source-before.json') == j(V / 'source-after.json') == m
    r = j(V / 'result.json')
    proc = j(V / 'process.json')
    raw = (V / 'run.log').read_text()
    assert sha(V / 'run.log') == r['raw_log_sha256']
    runs = re.findall(r'^=== RUN\s+(\S+)', raw, re.M)
    passes = re.findall(r'^\s*--- PASS: (\S+)', raw, re.M)
    fails = re.findall(r'^\s*--- FAIL: (\S+)', raw, re.M)
    skips = re.findall(r'^\s*--- SKIP: (\S+)', raw, re.M)
    assert runs == r['test_RUN_names'] and passes == r['test_PASS_names'] and fails == r['test_FAIL_names']
    assert skips == r['test_SKIP_names'] == []
    tops = [n for n in runs if '/' not in n]
    assert set(tops) == set(command['required_top_level_tests']) and len(tops) == len(set(tops))
    is_stop = a.outcome == 'STOP' and ix == a.executed - 1
    assert r['exit_code'] == (1 if is_stop else 0) and r['status'] == ('STOP' if is_stop else 'PASS')
    assert bool(fails) == is_stop
    if not is_stop:
        assert all(n in passes for n in runs) and not r['stop_reasons']
    assert not any(v in raw for v in ['WARNING: DATA RACE', 'panic:', 'goleak:'])
    assert not r['timeout'] and not r['signals'] and not r['remaining_group_members']
    assert not r['remaining_direct_or_adopted_children'] and not r['residual_observed_before_cleanup']
    assert r['leader_joined'] and r['group_and_children_empty'] and r['source_before_after_equal']
    assert proc['pid'] == proc['pgid'] == r['pid'] == r['pgid'] and proc['runner_pid'] == i['runner_pid']
    assert not Path('/proc/' + str(r['pid'])).exists()
    rows.append(dict(name=expected['name'], top_RUN=len(tops), top_PASS=sum('/' not in n for n in passes), top_FAIL=[n for n in fails if '/' not in n], child_RUN=sum('/' in n for n in runs), elapsed_seconds=r['elapsed_seconds'], raw_sha256=sha(V/'run.log'), result_sha256=sha(V/'result.json'), pid=r['pid'], pgid=r['pgid']))
assert [x['name'] for x in s['results']] == [x['name'] for x in rows]
reuse = None
reuse_path = C / 'reused-normal-freelist-applicability.json'
if reuse_path.exists():
    reuse = j(reuse_path)
    original_result = Path(reuse['original_result'])
    original = j(original_result)
    assert changed == ['TreeDB/internal/rootpublication/selected_manifest_ledger_test.go']
    assert reuse['original_source_manifest_sha256'] == sha(base / 'source-frozen.json')
    assert reuse['current_manifest_sha256'] == a.manifest_sha256
    assert reuse['original_result_sha256'] == sha(original_result)
    assert original['status'] == 'PASS' and original['exit_code'] == 0 and not original['stop_reasons']
    assert original['top_level_pass_count'] == 18 and original['child_RUN_count'] == 79
    assert j(original_result.parent/'source-before.json') == j(original_result.parent/'source-after.json') == previous
    assert sha(original_result.parent/'run.log') == original['raw_log_sha256']
    assert original['group_and_children_empty'] and original['leader_joined'] and not original['signals']
    assert not reuse['rerun'] and reuse['all_production_and_freelist_test_files_byte_identical']
out = dict(verified_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(), source_manifest_sha256=a.manifest_sha256, source_archive_sha256=f['source_archive_sha256'], source_files=len(m), changed_paths=changed, source_archive_and_runtime_bindings_verified=True, process_groups_joined_empty_no_intervention=True, index_not_launch_bound=True, result=a.outcome, commands_executed=a.executed, unexecuted_commands=len(commands)-a.executed, raw_summary_all_authorized_pass=s['all_authorized_pass'], summary_discrepancy=aggregate_discrepancy, reused_normal_freelist=bool(reuse), reused_result_sha256=reuse['original_result_sha256'] if reuse else None, rows=rows)
path = D / 'native-main-integration' / ('private-hash-' + a.packet + '-root-verification.json')
path.write_text(json.dumps(out, indent=2) + '\n')
print(json.dumps(out, indent=2))
