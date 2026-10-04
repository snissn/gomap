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
print('PASS: 12 actual baseline packets; altered requests/configuration rejected; assertions retained under -O')
