#!/usr/bin/env python3
"""Bounded actual-packet check: python3 [-O] selfcheck.py ABS_EVIDENCE_ROOT."""
import copy
import pathlib
import sys

namespace = {'__name__': 'assembler', '__file__': str(pathlib.Path(__file__).with_name('assemble.py'))}
exec(compile(pathlib.Path(namespace['__file__']).read_bytes(), namespace['__file__'], 'exec', optimize=0), namespace)
root = pathlib.Path(sys.argv[1]).resolve()
repo = pathlib.Path(__file__).resolve().parents[3]
baseline = namespace['read'](root / 'A2-full-baseline-completion.json')
validate, collector = namespace['contract'](repo)
records = namespace['load_bundle'](root / 'baseline-primary-local', repo, namespace['BASE'], baseline, validate, collector, {}, {}, root)
namespace['require'](len(records) == 12 and all(r['status'] == 'PASS_UNPROFILED' for r in records), 'actual accepted baseline parser')
directory = pathlib.Path(records[0]['raw_directory'])
payload = namespace['read'](directory / 'stdout.json')
cell = records[0]['cell']
mutated = copy.deepcopy(payload)
mutated[0]['phases'][2]['requested_absent'] -= 1000000
mutated[0]['phases'][2]['requested_present'] += 1000000
try:
    validate(mutated, cell, directory, {}, {})
except (AssertionError, ValueError):
    pass
else:
    raise ValueError('mutated actual request contract accepted')
mutated = copy.deepcopy(payload)
mutated[0]['config']['workers'] += 1
try:
    validate(mutated, cell, directory, {}, {})
except (AssertionError, ValueError):
    pass
else:
    raise ValueError('mutated executed configuration accepted')
plan = namespace['read'](root / 'baseline-primary-local/plan.json')
build = namespace['read'](root / 'baseline-primary-local/build-receipt.json')
tree_record = next(r for r in records if r['cell']['engine'] == 'treedb' and r['cell']['profile'] == 'durable')
for mutation in ('wrong executable', 'undeclared compression'):
    meta = copy.deepcopy(tree_record['run_metadata'])
    if mutation == 'wrong executable':
        # Same basename elsewhere must still fail absolute process binding.
        meta['command'][0] = '/unrelated/bin/' + pathlib.Path(meta['command'][0]).name
    else:
        meta['command'].append('-treedb-vlog-compression=off')
    try:
        namespace['check_command'](meta, plan, build)
    except ValueError:
        pass
    else:
        raise ValueError('mutated retained command accepted: ' + mutation)
entries = namespace['harness_entries']
check_harness = namespace['check_harness']
original = entries(repo, namespace['BASE'])
repaired = entries(repo, namespace['DOC_REPAIR'])
check_harness(original, original, repaired, repaired)
check_harness(original, repaired, repaired, repaired)
try:
    check_harness(original, repaired, original, repaired)
except ValueError:
    pass
else:
    raise ValueError('reverse README transition accepted')
for path, entry in (
    ('cmd/benchprof/README.md', '100644 blob ' + '0' * 40),
    ('cmd/unified_bench/README.md', repaired['cmd/benchprof/README.md']),
    ('cmd/unified_bench/README.md', repaired['cmd/unified_bench/README.md'].replace('100644', '100755')),
    ('cmd/unified_bench/main.go', '100644 blob ' + '0' * 40),
    ('cmd/benchprof/main_test.go', '100644 blob ' + '0' * 40),
    ('cmd/unified_bench/unreviewed.md', '100644 blob ' + '0' * 40),
    ('scripts/unified_bench_quicksilver_capture.py', '100644 blob ' + '0' * 40),
    ('scripts/test_unified_bench_quicksilver_capture.py', '100644 blob ' + '0' * 40),
    ('cmd/benchprof/README.md', None),
):
    mutated = repaired.copy()
    if entry is None:
        del mutated[path]
    else:
        mutated[path] = entry
    for captured, landed in ((original, mutated), (mutated, repaired)):
        try:
            check_harness(original, captured, landed, repaired)
        except ValueError:
            pass
        else:
            raise ValueError('mutated harness accepted: ' + path)
source = namespace['read'](root / 'o1-o1-composed-durable91-pair1/source-receipt.json')
applicability = namespace['source_applicability'](repo, source['head'], namespace['DOC_REPAIR'], source)
namespace['require'](applicability['captured_head'] == source['head'] and
                     applicability['compiled_project_input_count'] == 2130, 'actual O1 documentation applicability')
try:
    namespace['source_applicability'](repo, source['head'], namespace['BASE'], source)
except ValueError:
    pass
else:
    raise ValueError('changed landed TreeDB accepted')
name = next(iter(source['compiled_project_inputs']))
try:
    namespace['project_inputs'](repo, namespace['DOC_REPAIR'], {name: '0' * 64})
except ValueError:
    pass
else:
    raise ValueError('changed compiled input accepted')
print('PASS: 12 actual baseline packets; original request/configuration/argv guards; exact README transitions and 2130 O1 compiled inputs; README/runtime/test/producer drift rejected; assertions retained under -O')
