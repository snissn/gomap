import json
from pathlib import Path
p = Path(__file__).parent
stages = {'initial.json': 2, 'full-normal.json': 3, 'full-race.json': 3, 'full-safe.json': 3}
receipts = {}
for name in ['initial-exit.tsv', 'gate-exits.tsv']:
    for line in (p / name).read_text().splitlines():
        stage, status = line.split('\t')
        assert stage not in receipts
        receipts[stage] = int(status)
assert receipts == {'initial': 0, 'normal': 0, 'race': 0, 'safe': 0, 'vet': 0, 'final-no-extra-proof': 0}, receipts
summary = {}
for name, expected_packages in stages.items():
    events = []
    for line in (p / name).read_text().splitlines():
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict):
            events.append(item)
    failed = [e for e in events if e.get('Action') == 'fail']
    assert not failed, failed
    packages = [e for e in events if e.get('Action') == 'pass' and 'Test' not in e]
    assert len(packages) == expected_packages, packages
    summary[name] = {'test_pass_events': sum(e.get('Action') == 'pass' and 'Test' in e for e in events), 'package_pass_events': len(packages), 'fail_events': 0, 'package_elapsed': {e['Package']: e['Elapsed'] for e in packages}}
identity = json.loads((p / 'source-identity.json').read_text())
proofs = [json.loads((p / n).read_text()) for n in ['initial-source-check.json', 'post-gate-source-check.json']]
for proof in proofs:
    assert proof['commit'] == identity['commit']
    assert proof['file_count'] == identity['file_count']
    assert proof['mismatches'] == []
assert proofs[0]['archive_sha256'] == proofs[1]['archive_sha256']
print(json.dumps({'candidate': identity['commit'], 'source_tree_sha256': identity['source_tree_sha256'], 'stages': summary, 'stage_exits': receipts, 'source_checks': proofs}, indent=2))
