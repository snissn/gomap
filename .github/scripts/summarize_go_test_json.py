#!/usr/bin/env python3
import json
import sys
from pathlib import Path

if len(sys.argv) != 2:
    print('usage: summarize_go_test_json.py <jsonl>', file=sys.stderr)
    sys.exit(2)

path = Path(sys.argv[1])
if not path.exists():
    print(f'missing log file: {path}', file=sys.stderr)
    sys.exit(1)

running = {}
finished = []
package_elapsed = {}

with path.open() as f:
    for raw in f:
        raw = raw.strip()
        if not raw:
            continue
        try:
            evt = json.loads(raw)
        except json.JSONDecodeError:
            continue
        pkg = evt.get('Package', '')
        test = evt.get('Test')
        action = evt.get('Action')
        elapsed = evt.get('Elapsed')
        if test and action == 'run':
            running[(pkg, test)] = True
        elif test and action in ('pass', 'fail', 'skip'):
            finished.append((float(elapsed or 0), action, pkg, test))
            running.pop((pkg, test), None)
        elif not test and action in ('pass', 'fail', 'skip'):
            package_elapsed[pkg] = (action, float(elapsed or 0))

finished.sort(reverse=True)
slow = finished[:25]
open_tests = sorted(running.keys())

lines = []
lines.append('## Go Test Timing Summary')
lines.append('')
if slow:
    lines.append('| elapsed_s | result | package | test |')
    lines.append('| ---: | --- | --- | --- |')
    for elapsed, action, pkg, test in slow:
        lines.append(f'| {elapsed:.2f} | {action} | `{pkg}` | `{test}` |')
else:
    lines.append('No completed test events found.')

lines.append('')
lines.append('## Package Results')
lines.append('')
if package_elapsed:
    lines.append('| elapsed_s | result | package |')
    lines.append('| ---: | --- | --- |')
    for pkg, (action, elapsed) in sorted(package_elapsed.items(), key=lambda kv: kv[1][1], reverse=True):
        lines.append(f'| {elapsed:.2f} | {action} | `{pkg}` |')
else:
    lines.append('No package result events found.')

lines.append('')
lines.append('## Tests Still Running At Log End')
lines.append('')
if open_tests:
    for pkg, test in open_tests[:50]:
        lines.append(f'- `{pkg}` :: `{test}`')
    if len(open_tests) > 50:
        lines.append(f'- ... and {len(open_tests)-50} more')
else:
    lines.append('- none')

summary = '\n'.join(lines) + '\n'
print(summary)
summary_path = path.with_suffix(path.suffix + '.summary.md')
summary_path.write_text(summary)
