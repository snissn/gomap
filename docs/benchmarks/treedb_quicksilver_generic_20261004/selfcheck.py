#!/usr/bin/env python3
"""Bounded actual-packet check: python3 [-O] selfcheck.py ABS_EVIDENCE_ROOT."""
import copy
import json
import pathlib
import shutil
import sys
import tempfile

namespace = {'__name__': 'assembler', '__file__': str(pathlib.Path(__file__).with_name('assemble.py'))}
exec(compile(pathlib.Path(namespace['__file__']).read_bytes(), namespace['__file__'], 'exec', optimize=0), namespace)
root = pathlib.Path(sys.argv[1]).resolve()
repo = pathlib.Path(__file__).resolve().parents[3]
validate, collector = namespace['contract'](repo)
packet = root / 'A2-full-baseline-completion.json'
baseline = namespace['accepted_baseline'](packet, collector)
with tempfile.TemporaryDirectory() as temporary:
    mutated_packet = pathlib.Path(temporary) / packet.name
    mutated_packet.write_bytes(packet.read_bytes() + b'\n')
    for path, identity in ((mutated_packet, collector), (packet, '0' * 64)):
        try:
            namespace['accepted_baseline'](path, identity)
        except ValueError:
            pass
        else:
            raise ValueError('mutated baseline packet/collector identity accepted')
records = namespace['load_bundle'](root / 'baseline-primary-local', repo, namespace['BASE'], baseline, validate, collector, {}, {}, root)
namespace['require'](len(records) == 12 and all(r['status'] == 'PASS_UNPROFILED' for r in records), 'actual accepted baseline parser')
with tempfile.TemporaryDirectory() as temporary:
    bundle = shutil.copytree(root / 'o2-o1-o2-treedb-durable-s24', pathlib.Path(temporary) / 'o2-o1-o2-treedb-durable-s24')
    accepted = namespace['load_bundle'](bundle, repo, '17712b9cfcef2b90516419a34ccf3d464984d473', baseline, validate, collector, {}, {}, root)
    namespace['require'](len(accepted) == 1 and accepted[0]['status'] == 'PASS_UNPROFILED', 'copied accepted final bundle')
    stdout = next(bundle.glob('*/stdout.json'))
    original_stdout = stdout.read_bytes()
    altered = namespace['read'](stdout)
    altered[0]['phases'][0]['seconds'] *= 2
    altered[0]['phases'][0]['ops_per_sec'] /= 2
    extra = bundle / 'unreviewed.txt'
    for mutation in ('consistent timing change', 'missing file', 'extra file'):
        if mutation == 'consistent timing change':
            stdout.write_text(json.dumps(altered))
        elif mutation == 'missing file':
            stdout.unlink()
        else:
            extra.write_text('unreviewed')
        try:
            namespace['load_bundle'](bundle, repo, '17712b9cfcef2b90516419a34ccf3d464984d473', baseline,
                                     lambda *args: namespace['fail']('validator called before raw inventory rejection'),
                                     collector, {}, {}, root)
        except ValueError as error:
            namespace['require']('accepted raw bundle inventory' in str(error), 'raw inventory rejected before validator')
        else:
            raise ValueError('mutated raw bundle accepted: ' + mutation)
        stdout.write_bytes(original_stdout)
        if extra.exists():
            extra.unlink()
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
for bundle, landed in (('baseline-primary-local', namespace['BASE']),
                       ('o1-o1-composed-durable91-pair1', namespace['DOC_REPAIR']),
                       ('final-remaining30', '17712b9cfcef2b90516419a34ccf3d464984d473')):
    accepted = namespace['read'](root / bundle / 'source-receipt.json')
    namespace['source_applicability'](repo, accepted['head'], landed, accepted)
    original_git = namespace['git']
    namespace['git'] = lambda *args: namespace['fail']('Git called before landed identity rejection')
    try:
        namespace['source_applicability'](repo, accepted['head'], accepted['head'], accepted)
    except ValueError as error:
        namespace['require']('accepted final landed identity' in str(error), 'unaccepted landed identity rejected before Git')
    else:
        raise ValueError('unaccepted landed identity accepted: ' + bundle)
    finally:
        namespace['git'] = original_git
    name = next(k for k in accepted['compiled_project_inputs'] if not k.startswith('TreeDB/'))
    for mutation in ('omission', 'addition', 'same-count replacement'):
        mutated = copy.deepcopy(accepted)
        if mutation != 'addition':
            del mutated['compiled_project_inputs'][name]
            del mutated['files'][name]
        if mutation != 'omission':
            mutated['compiled_project_inputs']['unreviewed.go'] = '0' * 64
            mutated['files']['unreviewed.go'] = '0' * 64
        try:
            namespace['source_applicability'](repo, mutated['head'], landed, mutated)
        except ValueError as error:
            namespace['require']('complete frozen project input inventory' in str(error), 'inventory rejected before blob checks')
        else:
            raise ValueError('mutated complete inventory accepted: ' + bundle + ' ' + mutation)
try:
    namespace['source_applicability'](repo, source['head'], namespace['BASE'], source)
except ValueError:
    pass
else:
    raise ValueError('unaccepted O1-to-baseline landed identity accepted')
name = next(iter(source['compiled_project_inputs']))
try:
    namespace['project_inputs'](repo, namespace['DOC_REPAIR'], {name: '0' * 64})
except ValueError:
    pass
else:
    raise ValueError('changed compiled input accepted')
print('PASS: accepted baseline packet/collector identity; 12 actual baseline packets; copied final raw bundle rejects consistent timing change/missing/extra files before validator; request/configuration/argv guards; exact README transitions; complete 2120/2130/2131 frozen inventories reject omission/addition/replacement and unaccepted landed identity before Git; README/runtime/test/producer drift rejected; assertions retained under -O')
