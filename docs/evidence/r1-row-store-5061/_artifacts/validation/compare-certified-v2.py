#!/usr/bin/env python3
"""Private preparation: preserve packets and comparator math; require external certificate."""
import argparse
import ast
import hashlib
import json
from pathlib import Path
import re
import subprocess

BASELINE = '0216e2e9ee701dee0eda96c569dc0c8facb37ca9'
UNCHANGED = {
    'cmd/collection_workload_bench/r1.go': 'fec4f76df23af2cfa04522fa3e6d07a6fbb43a68',
    'cmd/collection_workload_bench/r1_sqlite.go': 'b4af34686b2e97b5b2715be01516cf2f6347b2c0',
    'scripts/r1_collection_summary.py': '2e785e8508666fc6c05f5810765c1bf76647d577',
    'scripts/r1_collection_source.py': '16e6bd4bacaf53b0f49f70e2789d0a47b3b42496',
}
CHANGED = {'cmd/collection_workload_bench/main.go',
           'cmd/collection_workload_bench/r1_mutation_sweep.go',
           'scripts/r1_collection_capture.sh'}
OBSERVATIONS = {'root-frozen-inputs.json', 'observer-before.json', 'build-start.json',
                'build-completed.json', 'run-start.json', 'run-exit.json',
                'runner-before.json', 'runner-after.json', 'validation-start.json',
                'validation-exit.json', 'independent-validation.log',
                'independent-expected-source.json'}
SCOPE = 'A workload semantics only; no runtime/generated-code/performance equivalence'

def require(ok, message):
    if not ok: raise ValueError(message)

def load(path): return json.loads(Path(path).read_text())
def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def git(repo, *args): return subprocess.check_output(['git', '-C', str(repo), *args])

def inventory(repo, commit):
    blobs = {}
    for line in git(repo, 'ls-tree', '-r', commit).decode().splitlines():
        metadata, path = line.split('\t', 1)
        blobs[path] = metadata.split()[2]
    paths = ['cmd/collection_workload_bench/main.go',
             *sorted(path for path in blobs if path.startswith('cmd/collection_workload_bench/r1')
                     and path.endswith('.go') and not path.endswith('_test.go')),
             'scripts/r1_collection_capture.sh', 'scripts/r1_collection_summary.py',
             'scripts/r1_collection_source.py']
    harness = hashlib.sha256()
    for path in paths:
        harness.update(path.encode() + b'\0' + git(repo, 'show', commit + ':' + path) + b'\0')
    runtime = {path: blob for path, blob in blobs.items() if path in ('go.mod', 'go.sum') or (
        path.startswith(('TreeDB/', 'cmd/internal/treedbstats/'))
        and Path(path).suffix in ('.go', '.s', '.S', '.c', '.h', '.syso')
        and not path.endswith('_test.go'))}
    digest = hashlib.sha256()
    for path, blob in sorted(runtime.items()): digest.update(path.encode() + b'\0' + blob.encode() + b'\0')
    return {'commit': commit, 'harness_sha256': harness.hexdigest(),
            'runtime_sha256': digest.hexdigest(), 'runtime_blobs': runtime,
            'harness_blobs': {path: blobs[path] for path in paths}}

def verify_certificate(certificate_path, expected_sha, repo, before_path, after_path, receipts, freeze, original):
    require(re.fullmatch('[0-9a-f]{64}', expected_sha) and sha(certificate_path) == expected_sha,
            'certificate must match independently supplied SHA256')
    c = load(certificate_path)
    require(c['schema'] == 'gomap-r1-A-harness-equivalence-v1' and c['approved'] is True,
            'coordinator approval missing')
    approval = c['approval']
    require(approval['scope'] == SCOPE and all(approval.get(key) for key in ('coordinator', 'review_url', 'utc')),
            'exact scoped sign-off missing')
    require(c['original_compare_sha256'] == sha(original), 'original math helper changed')
    require(c['baseline']['commit'] == BASELINE and c['unchanged_A_blobs'] == UNCHANGED,
            'wrong baseline or A blob allowlist')
    require(set(c['reviewed_changed_path_diff_sha256']) == CHANGED, 'changed-path review scope mismatch')
    before, after = load(before_path), load(after_path)
    for label, path, packet in [('baseline', before_path, before), ('selected', after_path, after)]:
        pin = c[label]
        for key, n in [('commit', 40), ('runtime_sha256', 64), ('harness_sha256', 64), ('packet_sha256', 64)]:
            require(isinstance(pin.get(key), str) and re.fullmatch('[0-9a-f]{'+str(n)+'}', pin[key]), 'blank/invalid '+label+' '+key)
        require(sha(path) == pin['packet_sha256'], label+' original packet bytes mismatch')
        observed = inventory(repo, pin['commit'])
        for key in ('commit', 'runtime_sha256', 'harness_sha256', 'runtime_blobs'):
            require(packet['source'][key] == observed[key], label+' committed source inventory mismatch')
        for key in ('commit', 'runtime_sha256', 'harness_sha256'):
            require(packet['source'][key] == pin[key], label+' certificate source mismatch')
        require(packet['source']['clean'] is True, label+' source not clean')
        for path, blob in UNCHANGED.items():
            require(observed['harness_blobs'].get(path) == blob, label+' A blob changed: '+path)
    require(sha(before_path.parent/'capture-environment.json') == c['baseline']['environment_sha256'], 'baseline original environment changed')
    require(sha(before_path.parent/'frozen-source-accepted.json') == c['baseline']['freeze_sha256']
            and sha(before_path.parent/'source.json') == c['baseline']['source_sha256'], 'baseline original freeze/source changed')
    require(sha(before_path.parent/'collection_workload_bench') == c['baseline']['binary_sha256'], 'baseline original executable changed')
    selected = inventory(repo, c['selected']['commit'])
    require(set(selected['harness_blobs']) == set(UNCHANGED)|CHANGED, 'unexpected selected harness input')
    require(selected['harness_blobs'] == c['selected_harness_blobs'], 'selected exact reviewed harness blobs changed')
    for path in CHANGED:
        actual = hashlib.sha256(git(repo, 'diff', '--no-ext-diff', '--no-textconv', '--no-color', BASELINE, c['selected']['commit'], '--', path)).hexdigest()
        require(actual == c['reviewed_changed_path_diff_sha256'][path], 'unreviewed changed-path diff: '+path)
    require(not re.search(rb'\bfunc\s+init\s*\(', git(repo, 'show', c['selected']['commit']+':cmd/collection_workload_bench/r1_mutation_sweep.go')),
            'C init is outside certified unused-dispatch scope')
    observed_hashes = c['selected']['observation_sha256']
    require(set(observed_hashes) == OBSERVATIONS, 'independent observation hash inventory incomplete')
    for name, expected in observed_hashes.items():
        require(isinstance(expected, str) and re.fullmatch('[0-9a-f]{64}', expected)
                and sha(receipts/name) == expected, 'independently frozen observation changed: '+name)
    trusted_path = receipts/'trusted-completed-receipt.json'
    require(sha(trusted_path) == c['selected']['receipt_sha256'] and sha(freeze) == c['selected']['freeze_sha256'],
            'independent receipt/freeze bytes mismatch')
    trusted, accepted = load(trusted_path), load(freeze)
    for key in ('commit', 'runtime_sha256', 'harness_sha256'):
        external = 'source_commit' if key == 'commit' else key
        require(trusted[external] == c['selected'][key] == accepted[key], 'receipt/freeze source mismatch')
    require(accepted['accepted'] is True and trusted['landed_tooling_commit'] == c['selected']['commit'], 'selected actual landed freeze absent')
    require(trusted['exit'] == 0 and trusted['packet_sha256'] == sha(after_path), 'failed or substituted observed packet')
    require(trusted['binary_sha256'] == sha(after_path.parent/'collection_workload_bench'), 'observed executable substituted')
    expected_path = receipts/'independent-expected-source.json'
    require(sha(expected_path) == trusted['expected_source_sha256'] and load(expected_path) == after['source'], 'expected source substitution')
    validation, started = load(receipts/'validation-exit.json'), load(receipts/'validation-start.json')
    config = load(receipts/'root-frozen-inputs.json')
    actual_out = Path(config['out'])
    require(started['argv'] == [str(actual_out/'collection_workload_bench'), 'r1-validate', '-source-manifest', str(Path(config['receipt_dir'])/'independent-expected-source.json'), str(actual_out/'packet.json')],
            'independent validation invocation mismatch')
    require(validation['exit'] == 0 and validation['binary_sha256'] == trusted['binary_sha256']
            and validation['packet_sha256'] == trusted['packet_sha256']
            and validation['validation_log_sha256'] == sha(receipts/'independent-validation.log'), 'independent validation failed/changed')
    return c, before, after, accepted

def derive_environment(receipts, source):
    config = load(receipts/'root-frozen-inputs.json')
    start, build, completed = load(receipts/'run-start.json'), load(receipts/'build-start.json'), load(receipts/'build-completed.json')
    pre, timed, post = load(receipts/'runner-before.json'), completed['runner'], load(receipts/'runner-after.json')
    require(load(receipts/'run-exit.json')['exit'] == 0 and completed['exit'] == 0, 'observed run/build failed')
    require(build['source'] == completed['source'] == load(receipts/'observer-before.json')['source'] == source, 'observed source changed')
    require(config['source_commit'] == config['landed_tooling_commit'] == source['commit'], 'actual selected source is not exact landing')
    require(build['argv'] == [config['real_go'], 'build', '-o', str(Path(config['out'])/'collection_workload_bench'), './cmd/collection_workload_bench'], 'compiler argv mismatch')
    for key in ('cpu_count', 'uname', 'temp_device', 'output_parent_device'):
        require(pre[key] == timed[key] == post[key], 'runner pre/post identity changed: '+key)
    env = start['environment']
    for observed in (pre, timed, post):
        for key in ('GOMAXPROCS','GOGC','GOMEMLIMIT','GODEBUG','TMPDIR'):
            require(observed['environment'][key] == env[key], 'actual runtime environment changed: '+key)
    require(env['R1_MODE'] == 'r1', 'wrong driver mode')
    args = start['argv'][2:]
    required = {'-qualification':'retained','-documents':'4096','-batch-size':'32','-operations':'1000','-repetitions':'5','-durability':'durable','-read-state':'flushed','-engines':'json,template-v1,bson,typed-row,sqlite-json,sqlite-row'}
    require(len(args)==2*len(required) and dict(zip(args[::2],args[1::2])) == required, 'actual full A argv mismatch')
    return {'coordinator_wrapper_sha256':load(receipts/'observer-before.json')['observer_sha256'],
            'worktree':config['repo'],'frozen_source':source['commit'],'output':config['out'],
            'capture_args':args,'utc':pre['utc'],'cpu_count':pre['cpu_count'],'uname':pre['uname'],
            'go_build_environment':build['go_build_environment'],'environment':env,
            'temp_device':pre['temp_device'],'output_parent_device':pre['output_parent_device'],
            'temp_filesystem':pre['temp_filesystem'],'output_filesystem':pre['output_filesystem'],
            'loadavg':pre['loadavg'],'top_processes':pre['top_processes'],
            'actual_post_runner':post,'actual_after_build_runner':timed}

def adapted_math(original, certificate):
    require(__debug__, 'Python optimization disables original comparison guards')
    tree = ast.parse(original.read_text())
    compare = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name=='compare')
    targets=[node for node in compare.body if isinstance(node,ast.Assert) and isinstance(node.msg,ast.Constant) and node.msg.value=='harness mismatch']
    require(len(targets)==1 and ast.unparse(targets[0].test)=="before['source']['harness_sha256'] == after['source']['harness_sha256']", 'original harness assertion shape changed')
    targets[0].test=ast.parse('_certified_harness_pair(before, after)',mode='eval').body
    # Only candidate identity's accepted/environment loader assignments change;
    # every remaining original ownership/environment/buildinfo/SQLite check stays.
    identity=next(node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name=='capture_identity')
    identity.name='candidate_identity';identity.args.args.extend([ast.arg(arg='accepted'),ast.arg(arg='environment')])
    identity.body=[node for node in identity.body if not (isinstance(node,ast.Assign) and isinstance(node.targets[0],ast.Name) and node.targets[0].id in ('accepted','environment'))]
    namespace={'__name__':'frozen_original_math', '_certified_harness_pair':lambda b,a: all(b['source'][key]==certificate['baseline'][key] and a['source'][key]==certificate['selected'][key] for key in ('commit','runtime_sha256','harness_sha256'))}
    exec(compile(ast.fix_missing_locations(tree),str(original),'exec'),namespace)
    return namespace

# Exact original raw buildinfo pair; no dependency or compiler-setting stripping.
BASELINE_BUILDINFO_SHA256 = '08f685dcd28426e51a2b3fac6af304a6adb472f5fa3b082f3ea0a9dab6f03bb2'
SELECTED_BUILDINFO_SHA256 = '1d1e68b983f212119bb070311de3744a40e594195054f373355d5212826d63b2'
BASELINE_BUILDINFO_LINES = ['/home/mikers/gomap-r1-evidence-20261005/r1-baseline-landed-0216e2e/collection_workload_bench: go1.26.4', '\tpath\tgithub.com/snissn/gomap/cmd/collection_workload_bench', '\tmod\tgithub.com/snissn/gomap\t(devel)\t', '\tdep\tgithub.com/buger/jsonparser\tv1.1.2\th1:frqHqw7otoVbk5M8LlE/L7HTnIq2v9RX6EJ48i9AxJk=', '\tdep\tgithub.com/cespare/xxhash/v2\tv2.3.0\th1:UL815xU9SqsFlibzuggzjXhog7bL6oX9BbNZnL2UFvs=', '\tdep\tgithub.com/golang/snappy\tv0.0.4\th1:yAGX7huGHXlcLOEtBnF4w7FQwA26wojNCwOYAEhLjQM=', '\tdep\tgithub.com/klauspost/cpuid/v2\tv2.2.10\th1:tBs3QSyvjDyFTq3uoc/9xFpCuOsJQFNPiAhYdw2skhE=', '\tdep\tgithub.com/mattn/go-sqlite3\tv1.14.42\th1:MigqEP4ZmHw3aIdIT7T+9TLa90Z6smwcthx+Azv4Cgo=', '\tdep\tgithub.com/pierrec/lz4/v4\tv4.1.22\th1:cKFw6uJDK+/gfw5BcDL0JL5aBsAFdsIT18eRtLj7VIU=', '\tdep\tgithub.com/snissn/compress\tv1.18.2-snissn.0.0.20260506201017-87fb149e4721\th1:2XYKYq/WV3oldGbGi3b7M7VQoh38DIKSO0lNteuazJ0=', '\tdep\tgithub.com/snissn/go-crc32-asm\tv0.0.0-20260522204125-08945951423a\th1:uqSq50AAT36ZzDiRApq8GNsbeJASPaneZ6aKfw3KShc=', '\tdep\tgithub.com/tidwall/btree\tv1.8.1\th1:27ehoXvm5AG/g+1VxLS1SD3vRhp/H7LuEfwNvddEdmA=', '\tdep\tgithub.com/tidwall/gjson\tv1.14.3\th1:9jvXn7olKEHU1S9vwoMGliaT8jq1vJ7IH/n9zD9Dnlw=', '\tdep\tgithub.com/tidwall/match\tv1.1.1\th1:+Ho715JplO36QYgwN9PGYNhgZvoUSc9X2c80KVTi+GA=', '\tdep\tgithub.com/tidwall/pretty\tv1.2.0\th1:RWIZEg2iJ8/g6fDDYzMpobmaoGh5OLl4AXtGUGPcqCs=', '\tdep\tgithub.com/tphakala/simd\tv1.0.22\th1:3wHL91t4yvhCB0ycyTznvucTHax+QGpYkvOhqfraTYw=', '\tdep\tgithub.com/zeebo/xxh3\tv1.1.0\th1:s7DLGDK45Dyfg7++yxI0khrfwq9661w9EN78eP/UZVs=', '\tdep\tgo.mongodb.org/mongo-driver/v2\tv2.6.0\th1:b9sJOYrkmt4l8bY43ZenFBcPlhYIjaOfYHLtbB/5qi8=', '\tdep\tgolang.org/x/sys\tv0.41.0\th1:Ivj+2Cp/ylzLiEU89QhWblYnOE9zerudt9Ftecq2C6k=', '\tbuild\t-buildmode=exe', '\tbuild\t-compiler=gc', '\tbuild\tCGO_ENABLED=1', '\tbuild\tCGO_CFLAGS=', '\tbuild\tCGO_CPPFLAGS=', '\tbuild\tCGO_CXXFLAGS=', '\tbuild\tCGO_LDFLAGS=', '\tbuild\tGOARCH=amd64', '\tbuild\tGOOS=linux', '\tbuild\tGOAMD64=v1']
SELECTED_BUILDINFO_LINES = ['/home/mikers/gomap-r1-evidence-20261005/r1-final-A-M-3325dfe/collection_workload_bench: go1.26.4', '\tpath\tgithub.com/snissn/gomap/cmd/collection_workload_bench', '\tmod\tgithub.com/snissn/gomap\tv0.6.2-0.20261006234446-3325dfe77940\t', '\tdep\tgithub.com/buger/jsonparser\tv1.1.2\th1:frqHqw7otoVbk5M8LlE/L7HTnIq2v9RX6EJ48i9AxJk=', '\tdep\tgithub.com/cespare/xxhash/v2\tv2.3.0\th1:UL815xU9SqsFlibzuggzjXhog7bL6oX9BbNZnL2UFvs=', '\tdep\tgithub.com/golang/snappy\tv0.0.4\th1:yAGX7huGHXlcLOEtBnF4w7FQwA26wojNCwOYAEhLjQM=', '\tdep\tgithub.com/klauspost/cpuid/v2\tv2.2.10\th1:tBs3QSyvjDyFTq3uoc/9xFpCuOsJQFNPiAhYdw2skhE=', '\tdep\tgithub.com/mattn/go-sqlite3\tv1.14.42\th1:MigqEP4ZmHw3aIdIT7T+9TLa90Z6smwcthx+Azv4Cgo=', '\tdep\tgithub.com/pierrec/lz4/v4\tv4.1.22\th1:cKFw6uJDK+/gfw5BcDL0JL5aBsAFdsIT18eRtLj7VIU=', '\tdep\tgithub.com/snissn/compress\tv1.18.2-snissn.0.0.20260506201017-87fb149e4721\th1:2XYKYq/WV3oldGbGi3b7M7VQoh38DIKSO0lNteuazJ0=', '\tdep\tgithub.com/snissn/go-crc32-asm\tv0.0.0-20260522204125-08945951423a\th1:uqSq50AAT36ZzDiRApq8GNsbeJASPaneZ6aKfw3KShc=', '\tdep\tgithub.com/tidwall/btree\tv1.8.1\th1:27ehoXvm5AG/g+1VxLS1SD3vRhp/H7LuEfwNvddEdmA=', '\tdep\tgithub.com/tidwall/gjson\tv1.14.3\th1:9jvXn7olKEHU1S9vwoMGliaT8jq1vJ7IH/n9zD9Dnlw=', '\tdep\tgithub.com/tidwall/match\tv1.1.1\th1:+Ho715JplO36QYgwN9PGYNhgZvoUSc9X2c80KVTi+GA=', '\tdep\tgithub.com/tidwall/pretty\tv1.2.0\th1:RWIZEg2iJ8/g6fDDYzMpobmaoGh5OLl4AXtGUGPcqCs=', '\tdep\tgithub.com/tphakala/simd\tv1.0.22\th1:3wHL91t4yvhCB0ycyTznvucTHax+QGpYkvOhqfraTYw=', '\tdep\tgithub.com/zeebo/xxh3\tv1.1.0\th1:s7DLGDK45Dyfg7++yxI0khrfwq9661w9EN78eP/UZVs=', '\tdep\tgo.mongodb.org/mongo-driver/v2\tv2.6.0\th1:b9sJOYrkmt4l8bY43ZenFBcPlhYIjaOfYHLtbB/5qi8=', '\tdep\tgolang.org/x/sys\tv0.41.0\th1:Ivj+2Cp/ylzLiEU89QhWblYnOE9zerudt9Ftecq2C6k=', '\tbuild\t-buildmode=exe', '\tbuild\t-compiler=gc', '\tbuild\tCGO_ENABLED=1', '\tbuild\tCGO_CFLAGS=', '\tbuild\tCGO_CPPFLAGS=', '\tbuild\tCGO_CXXFLAGS=', '\tbuild\tCGO_LDFLAGS=', '\tbuild\tGOARCH=amd64', '\tbuild\tGOOS=linux', '\tbuild\tGOAMD64=v1', '\tbuild\tvcs=git', '\tbuild\tvcs.revision=3325dfe77940fec8587d8b61b1ac4e0b2f72caca', '\tbuild\tvcs.time=2026-10-06T23:44:46Z', '\tbuild\tvcs.modified=false']
BUILDINFO_SCOPE = 'Only exact own-module version and VCS provenance presence; no runtime/generated-code/performance equivalence'
SELECTED_COMMIT = '3325dfe77940fec8587d8b61b1ac4e0b2f72caca'
PARENT_ADAPTER_SHA256 = '5bc6c7eb0c00b8f78860e31769ac4731f1d1aed2f91013e2a9b1ed7633065c75'


def normalize_provenance(certificate, before_path, after_path, before_identity, after_identity):
    extension = certificate['buildinfo_provenance']
    require(extension['schema'] == 'gomap-r1-A-buildinfo-provenance-v1'
            and extension['approved'] is True, 'separate buildinfo provenance approval missing')
    approval = extension['approval']
    require(approval['scope'] == BUILDINFO_SCOPE and all(approval.get(k) for k in ('coordinator', 'review_url', 'utc')),
            'exact buildinfo-only scoped approval missing')
    require(extension['parent_adapter_sha256'] == PARENT_ADAPTER_SHA256, 'wrong reviewed parent adapter')
    require(certificate['selected']['commit'] == SELECTED_COMMIT, 'provenance exception is pinned to exact actual M')
    expected_pair = {
        'baseline': {'buildinfo_sha256': BASELINE_BUILDINFO_SHA256, 'raw_lines': BASELINE_BUILDINFO_LINES},
        'selected': {'buildinfo_sha256': SELECTED_BUILDINFO_SHA256, 'raw_lines': SELECTED_BUILDINFO_LINES},
    }
    require(extension['exact_original_pair'] == expected_pair, 'unreviewed raw buildinfo pair')
    transforms = {'own_module': 'github.com/snissn/gomap',
                  'baseline_version': '(devel)',
                  'selected_version': 'v0.6.2-0.20261006234446-3325dfe77940',
                  'remove_selected_exact_lines': ['\tbuild\tvcs=git', '\tbuild\tvcs.modified=false']}
    require(extension['normalization'] == transforms, 'widened metadata normalization')
    raw_before, raw_after = before_path.parent/'buildinfo.txt', after_path.parent/'buildinfo.txt'
    require(sha(raw_before) == BASELINE_BUILDINFO_SHA256 and sha(raw_after) == SELECTED_BUILDINFO_SHA256,
            'original raw buildinfo bytes changed')
    require(raw_before.read_text().splitlines() == BASELINE_BUILDINFO_LINES
            and raw_after.read_text().splitlines() == SELECTED_BUILDINFO_LINES, 'raw buildinfo lines changed')
    raw_filtered = lambda lines: [line for line in lines[1:] if not any(k in line for k in ('vcs.revision=', 'vcs.time='))]
    require(before_identity['buildinfo'] == raw_filtered(BASELINE_BUILDINFO_LINES)
            and after_identity['buildinfo'] == raw_filtered(SELECTED_BUILDINFO_LINES), 'original identity buildinfo extraction changed')
    require({k:v for k,v in before_identity.items() if k != 'buildinfo'} ==
            {k:v for k,v in after_identity.items() if k != 'buildinfo'},
            'non-buildinfo host/toolchain/compiler/Go-runtime/filesystem/SQLite mismatch')
    baseline_module = '\tmod\tgithub.com/snissn/gomap\t(devel)\t'
    selected_module = '\tmod\tgithub.com/snissn/gomap\tv0.6.2-0.20261006234446-3325dfe77940\t'
    lines = list(after_identity['buildinfo'])
    require(before_identity['buildinfo'].count(baseline_module) == 1 and lines.count(selected_module) == 1,
            'wrong own-module metadata line')
    lines[lines.index(selected_module)] = baseline_module
    for line in transforms['remove_selected_exact_lines']:
        require(lines.count(line) == 1, 'missing/duplicated exact allowed selected VCS line')
        lines.remove(line)
    require(lines == before_identity['buildinfo'], 'remaining buildinfo dependencies/flags/settings mismatch')
    normalized_after = {**after_identity, 'buildinfo': lines}
    require(before_identity == normalized_after, 'remaining identity mismatch')
    return before_identity, normalized_after, {'scope': BUILDINFO_SCOPE,
        'parent_adapter_sha256': PARENT_ADAPTER_SHA256, 'exact_original_pair': expected_pair,
        'normalization': transforms, 'all_other_environment_fields_equal': True,
        'baseline_original_identity': before_identity, 'selected_original_identity': after_identity,
        'numeric_acceptance_pending': True}

def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('certificate','expected-certificate-sha256','repo','before','after','receipts','accepted-freeze','original-compare'):p.add_argument('--'+name,required=True)
    args=p.parse_args();before_path,after_path=Path(args.before),Path(args.after);receipts=Path(args.receipts);original=Path(args.original_compare)
    c,b,a,accepted=verify_certificate(Path(args.certificate),args.expected_certificate_sha256,Path(args.repo),before_path,after_path,receipts,Path(args.accepted_freeze),original)
    env=derive_environment(receipts,a['source'])
    require(a['hostname'] == env['uname'][1] and a['goos'] == env['go_build_environment']['GOOS']
            and a['goarch'] == env['go_build_environment']['GOARCH'] and a['go_version'] == 'go1.26.4',
            'packet host/toolchain differs from actual runner/compiler observations')
    math=adapted_math(original,c)
    # Baseline ownership/acceptance/env checks use the original unmodified function.
    baseline_namespace={'__name__':'original_baseline_math'};exec(compile(original.read_text(),str(original),'exec'),baseline_namespace)
    baseline_identity=baseline_namespace['capture_identity'](before_path,b)
    selected_identity=math['candidate_identity'](after_path,a,accepted,env)
    baseline_identity,selected_identity,provenance=normalize_provenance(c,before_path,after_path,baseline_identity,selected_identity)
    result=math['compare'](b,a,baseline_identity,selected_identity)
    result['buildinfo_provenance']=provenance
    result['harness_equivalence']={'certificate_sha256':args.expected_certificate_sha256,'scope':SCOPE,'original_math_sha256':sha(original),'full_harness_equal':b['source']['harness_sha256']==a['source']['harness_sha256'],'baseline_harness_sha256':b['source']['harness_sha256'],'selected_harness_sha256':a['source']['harness_sha256']}
    result['observed_candidate_environment']=env
    print(json.dumps(result,indent=2,allow_nan=False))

if __name__=='__main__':main()
