import datetime, hashlib, json, re, tarfile
from pathlib import Path

D = Path('/mnt/fast4tb/gomap-mvcc-4871-20261015')
B = D / 'native-main-integration/private-hash-capacity/source-final02'
V = B / 'verification'
sha = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
read = lambda p: json.loads(p.read_text())
receipt = read(B / 'freeze-receipt.json')
manifest = read(B / 'source-frozen.json')
assert sha(B / 'source-frozen.json') == receipt['source_manifest_sha256'] == '15f0548d03b03923cc8785816b21ee957df3a4f114a2578cba0ef3bbbdd142ec'
assert sha(B / 'source-frozen.tar.gz') == receipt['source_archive_sha256'] == '7fd459782bcd9bea535b91956324c424b2a8703a102a71526917202130b6eacc'
assert sha(B / 'source-delta.patch') == receipt['source_delta_sha256']
with tarfile.open(B / 'source-frozen.tar.gz', 'r:gz') as archive:
    members = [m for m in archive if m.isfile()]
    assert len({m.name for m in members}) == len(members) == len(manifest) == 10389
    actual = {m.name: hashlib.sha256(archive.extractfile(m).read()).hexdigest() for m in members}
    assert actual == manifest
baseline_path = D / 'native-main-integration/index-pin-inventory-final/source-final.json'
assert sha(baseline_path) == receipt['baseline_manifest_sha256'] == '23e37a210f156838de8adecbf8532c0cd16cd12f6d6c37fb05529b7974d0869f'
baseline = read(baseline_path)
changed = sorted(p for p in baseline.keys() | manifest.keys() if baseline.get(p) != manifest.get(p))
assert changed == sorted(row['path'] for row in read(B / 'changed-paths.json')) and len(changed) == receipt['changed_paths'] == 30
assert not (baseline.keys() - manifest.keys())
for suffix in ['before', 'after']:
    assert read(V / f'compile-only/source-{suffix}.json') == manifest
command = read(V / 'compile-only/command.json')
assert command['executed'] is True
assert command['cwd'] == str(D / 'native-integrated')
packages = ['./TreeDB/freelist', './TreeDB/db', './TreeDB/internal/rootpublication', './TreeDB/internal/retainedalloc', './TreeDB/caching', './TreeDB/internal/primaryarena', './TreeDB/mvcc', './TreeDB/zipper']
assert command['argv'] == [str(D / 'toolchains/go1.26.3/bin/go'), 'test', '-p=1', *packages, '-run', '^$', '-count=1', '-timeout=3m']
assert command['outer_timeout_seconds'] == 300
expected_env = {'CGO_ENABLED':'1','GOCACHE':str(D/'native-build/cache'),'GODEBUG':'','GOENV':'off','GOFLAGS':'','GOGC':'100','GOMAXPROCS':'2','GOMEMLIMIT':'8GiB','GOMODCACHE':str(D/'ordinary-work/modcache'),'GOPROXY':'off','GOROOT':str(D/'toolchains/go1.26.3'),'GOSUMDB':'off','GOTOOLCHAIN':'local','GOWORK':'off','TMPDIR':str(V/'compile-only/tmp')}
assert command['env'] == expected_env
assert command['grant_sha256'] == sha(D/'native-decision/root-private-hash-capacity-selection.md') == receipt['selections']['root-private-hash-capacity-selection.md']
assert command['grant_addendum_sha256'] == sha(D/'native-decision/root-private-clone-admission-addendum.md') == receipt['selections']['root-private-clone-admission-addendum.md']
assert command['source_archive_sha256'] == receipt['source_archive_sha256']
for name, digest in receipt['selections'].items():
    assert sha(D/'native-decision'/name) == digest
identity = read(V/'identity.json')
assert identity['selection_bindings'] == receipt['selections']
assert identity['go_binary_sha256'] == sha(Path(command['argv'][0])) == 'd68b7abbc40d0844f673f6cf06ae3cded225c50437c6454fa37ef178d079fe65'
assert identity['compile_binary_sha256'] == sha(D/'toolchains/go1.26.3/pkg/tool/linux_amd64/compile') == '3bb421d581f60b9ab48c2c73adf5c018557e89af32ad018d9c4417afa4bf90c1'
assert identity['goroot_VERSION'] == (D/'toolchains/go1.26.3/VERSION').read_text()
assert identity['subreaper_verified'] and identity['archive_verified_files'] == len(manifest)
process = read(V/'compile-only/process.json')
result = read(V/'compile-only/result.json')
assert result['exit_code'] == 0 and not result['timeout'] and not result['signals']
for key in ['leader_joined','subreaper_enabled','group_and_children_empty','source_before_equals_frozen','source_after_equals_frozen','source_before_after_equal']:
    assert result[key] is True
assert not result['remaining_group_members'] and not result['remaining_direct_or_adopted_children'] and not result['residual_observed_before_cleanup']
assert process['pid'] == process['pgid'] == result['pid'] == result['pgid']
assert process['runner_pid'] == identity['runner_pid'] and process['runner_subreaper']
log = (V/'compile-only/run.log').read_text()
assert not re.search(r'=== RUN|--- (?:FAIL|PASS|SKIP):|WARNING: DATA RACE|panic:|FAIL\s', log)
assert result['test_RUN_count'] == result['top_level_pass_count'] == result['top_level_fail_count'] == result['top_level_skip_count'] == 0
for package in packages:
    assert re.search(r'^ok\s+github.com/snissn/gomap/' + re.escape(package[2:]) + r'\s+[\d.]+s \[no tests to run\]$', log, re.M)
summary = read(V/'summary.json')
assert summary['commands_executed'] == 1 and not summary['commands_not_executed']
assert summary['zero_test_RUN_verified'] and summary['all_requested_packages_reported'] and not summary['native_publication_runtime']
assert not Path(f'/proc/{result["pid"]}').exists() and not Path(f'/proc/{identity["runner_pid"]}').exists()
payloads = {str(p.relative_to(B)):sha(p) for p in B.rglob('*') if p.is_file() and 'tmp' not in p.relative_to(B).parts and p.name != 'source-frozen.tar.gz'}
output = {'verified_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'source_manifest_sha256':receipt['source_manifest_sha256'],'archive_sha256':receipt['source_archive_sha256'],'source_files':len(manifest),'changed_paths':changed,'compile_only_pass':True,'test_bodies':0,'elapsed_seconds':result['elapsed_seconds'],'pid':result['pid'],'pgid':result['pgid'],'runner_pid':identity['runner_pid'],'joined_empty_no_intervention':True,'source_before_after_archive_verified':True,'payload_sha256':payloads,'live_source_not_asserted':'This verifies immutable source-final02 compile receipts; source verification is separate.','index_not_launch_bound':True,'numeric_allowance':False,'native_publication':False}
target = D/'native-main-integration/private-hash-compile02-root-verification.json'
target.write_text(json.dumps(output,indent=2)+'\n')
print(json.dumps({k:v for k,v in output.items() if k!='payload_sha256'},indent=2))
