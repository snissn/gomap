import argparse
import ast
import hashlib
import json
from pathlib import Path
import re

D = Path('/mnt/fast4tb/gomap-mvcc-4871-20261015')
W = D / 'native-integrated'
p = argparse.ArgumentParser()
p.add_argument('--packet', type=Path, required=True)
p.add_argument('--core-proposal', type=Path, required=True)
p.add_argument('--core-proposal-sha256', required=True)
a = p.parse_args()
B = a.packet.resolve()
assert B.is_relative_to(D / 'native-main-integration')
assert not (B / 'proposed-commands.json').exists()


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


assert sha(a.core_proposal) == a.core_proposal_sha256
manifest = json.loads((B / 'source-frozen.json').read_text())
core = json.loads(a.core_proposal.read_text())
old = json.loads((D / 'native-main-integration/native-registry-api-preparation/proposed-commands.json').read_text())
inventories = {}
for command in old['commands'] + core['commands']:
    packages = [v for v in command['argv'] if v.startswith('./')]
    assert len(packages) == 1
    package = packages[0]
    assert package in {'./TreeDB/db', './TreeDB/lifecycle'}
    inventories.setdefault(package, set()).update(command['required_top_level_tests'])
inventories['./TreeDB/internal/retainedalloc'] = {
    'TestOriginalRemovalReceiptReuseKeepsExactAccounting',
    'TestOriginalRemovalResetPreservesFailedGoverningShrink',
    'TestOriginalRemovalResetCannotConsumeBoundedReceipt',
    'TestRemovalWorkManyOriginalBindingsAndFutureGrowth',
    'TestRemovalWorkConcurrentOriginalGrowthAndEnrollment',
    'TestNativeRemovalCompletionRetainsExactDeltaOnRefusal',
}
inventories['./TreeDB/caching'] = {
    'TestSnapshotAcquisitionErrorDoesNotRetryCachedFallback',
    'TestPublishedRootSnapshotAcquisitionErrorIsNotAbsence',
}
inventories['./TreeDB'] = {
    'TestPublicSnapshotPropagatesOriginalRegistryAdmissionError',
    'TestPublicSnapshotClosedErrorHasNilInterface',
}
children = {
    'TestReaderRegistryOwnedAdmissionV6': ['header-refusal', 'peak-and-drain', 'closed-binding', 'outside-lock-recheck', 'failed-unused-refund'],
    'TestReaderRegistryPositiveCompletionV6': ['nil-work', 'short-work', 'different-owner'],
    'TestNativeSnapshotMinimumAcquisitionAdmissionV6': ['record-remainder-short', 'byte-remainder-short', 'first-shard', 'last-shard'],
    'TestNativeSnapshotMinimumUnsupportedGeometryV6': ['whole-map', 'removed-fallback-summary'],
    'TestNativeSnapshotMinimumEpochInvalidationV6': ['initial', 'registry', 'terminal', 'completed-reuse', 'counter-before-begin-epoch'],
    'TestNativeDataDependencyReleaseRechecksCurrentGenerationV6': ['current', 'retired-after-eligibility'],
    'TestOriginalDATAOnlySnapshotLateManagerFailureRetainedByDB': ['held-snapshot', 'held-active-read'],
    'TestPrimarySnapshotPhysicalOwnerSurvivesDBCloseAndLastRead': ['ordinary', 'bounded'],
    'TestSnapshotAcquisitionErrorDoesNotRetryCachedFallback': ['backend_fast_path', 'mutable_cached_path'],
    'TestPublicSnapshotPropagatesOriginalRegistryAdmissionError': ['retained_metadata_capacity_exhausted', 'retained_metadata_owner_closed'],
}
for command in core['commands']:
    for name, rows in command['required_child_tests'].items():
        # Worker inventories contain complete Go test paths; this runner's
        # serialized plan stores child suffixes and reconstructs the path.
        assert all(row.startswith(name + '/') for row in rows), (name, rows)
        rows = [row[len(name) + 1:] for row in rows]
        if name in children:
            assert sorted(children[name]) == sorted(rows), (name, 'changed old child inventory')
        children[name] = rows
bindings = {}
test_paths = {}
for package, names in inventories.items():
    found = {}
    for path in sorted((W / package[2:]).glob('*_test.go')):
        matches = set(re.findall(r'^func (Test\w+)\(t \*testing.T\)', path.read_text(), re.M)) & names
        for name in matches:
            assert name not in found, name
            found[name] = path.relative_to(W).as_posix()
    assert set(found) == names, (package, names - set(found))
    test_paths.update(found)
    for name in found.values():
        assert sha(W / name) == manifest[name]
        bindings[name] = manifest[name]
go = D / 'toolchains/go1.26.3/bin/go'
env = {'CGO_ENABLED': '1', 'GOCACHE': str(D / 'native-build/cache'),
    'GOMODCACHE': str(D / 'ordinary-work/modcache'), 'GOROOT': str(go.parent.parent),
    'GOTOOLCHAIN': 'local', 'GOPROXY': 'off', 'GOSUMDB': 'off', 'GOWORK': 'off',
    'GOENV': 'off', 'GOFLAGS': '', 'GODEBUG': '', 'GOGC': '100',
    'GOMAXPROCS': '2', 'GOMEMLIMIT': '8GiB'}
commands = []
order = [('retainedalloc', './TreeDB/internal/retainedalloc'), ('lifecycle', './TreeDB/lifecycle'),
    ('db', './TreeDB/db'), ('caching', './TreeDB/caching'), ('public', './TreeDB')]
for mode in ['normal', 'race']:
    for label, package in order:
        name = mode + '-' + label
        tests = sorted(inventories[package])
        required_children = {test: children[test] for test in tests if test in children}
        commands.append({'name': name, 'selection_class': 'focused_requires_root_runtime_grant',
            'cwd': str(W), 'executed': False,
            'argv': [str(go), 'test', '-mod=readonly', '-p=1', '-parallel=1', '-count=1', '-v', '-timeout=3m']
                + (['-race'] if mode == 'race' else []) + ['-run', '^(' + '|'.join(tests) + ')$', package],
            'env': dict(env, TMPDIR=str(B / 'controls' / name / 'tmp')),
            'outer_timeout_seconds': 600, 'required_top_level_tests': tests,
            'expected_top_level_count': len(tests), 'required_child_tests': required_children,
            'expected_child_count': sum(len(rows) for rows in required_children.values())})
plan = {'scope': 'checked registry API focused controls, including C1 original lifetime correction',
    'executed': False, 'commands': commands, 'test_file_bindings': bindings, 'test_path_by_name': test_paths,
    'input_core_proposal': str(a.core_proposal), 'input_core_proposal_sha256': sha(a.core_proposal),
    'source_manifest_sha256': sha(B / 'source-frozen.json'),
    'actual_native_driver_authorized': False}
(B / 'proposed-commands.json').write_text(json.dumps(plan, indent=2, sort_keys=True) + '\n')
print(json.dumps({'plan_sha256': sha(B / 'proposed-commands.json'),
    'inventories': {package: len(tests) for package, tests in inventories.items()},
    'commands': len(commands), 'executed': False}, indent=2))
