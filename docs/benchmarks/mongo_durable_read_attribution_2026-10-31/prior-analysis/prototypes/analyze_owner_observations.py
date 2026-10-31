"""Validate retained GoMap identities; summarize whole-owner, not timed-run, stats."""
from pathlib import Path
import hashlib
import json

ROOT = Path(__file__).resolve().parent.parent
MANIFEST = json.loads((ROOT / 'context/selected-evidence.json').read_text())
ELF = '35409b3e8a34e9af8cd578ac00ef72cb0d721abbcccfd9437eab67e050df4ee4'
SOURCE = '8bea490ad5a2d89a14f02f428b4283579a6dd864'
KEYS = [
    'treedb.command_wal.file_sync.calls_total',
    'treedb.command_wal.file_sync.ns_total',
    'treedb.command_wal.group_commit.group_size_max',
    'treedb.command_wal.group_commit.groups_total',
    'treedb.command_wal.group_commit.participants_total',
]

def authenticated(path):
    relative = str(path.relative_to(ROOT / 'evidence/selected'))
    expected = MANIFEST['selected'][relative]
    raw = path.read_bytes()
    assert len(raw) == expected['bytes']
    assert hashlib.sha256(raw).hexdigest() == expected['sha256']
    return json.loads(raw)

rows = []
for cell in sorted((ROOT / 'evidence/selected/cells').iterdir()):
    data = cell / 'output/treedb_mongo'
    lifecycle = authenticated(data / 'lifecycle.json')
    assert lifecycle['source_commit'] == SOURCE
    assert lifecycle['process']['exe_sha256'] == ELF
    assert lifecycle['server_exit_code'] == 0
    for stage in ['checkpoint', 'server_close', 'reopen', 'random_state_preservation']:
        assert lifecycle[stage]['attempted'] and lifecycle[stage]['success']
        assert not lifecycle[stage]['error']
    assert lifecycle['preclose_state_sha256'] == lifecycle['reopened_state_sha256']
    observations = {}
    for name in ['open', 'before-close']:
        owner = authenticated(data / f'server.evidence/owner.{name}.json')
        assert owner['process']['exe_sha256'] == ELF
        assert owner['owner_id'] == lifecycle['owner_id']
        assert owner['profile'] == lifecycle['profile'] and owner['backend'] == 'cow'
        assert not owner['capture_error']
        assert owner['observed_gomaxprocs'] == 12
        observations[name] = {key: int(owner['stats'][key]) for key in KEYS}
    # Checkpoint and close are result envelopes, not stats observations.
    checkpoint = authenticated(data / 'server.evidence/owner.checkpoint.json')
    assert checkpoint['owner']['owner_id'] == lifecycle['owner_id']
    assert checkpoint['owner']['process']['exe_sha256'] == ELF
    close = authenticated(data / 'server.evidence/owner.close.json')
    assert close['owner_id'] == lifecycle['owner_id']
    assert close['process']['exe_sha256'] == ELF
    assert close['close']['success'] and not close['close']['error']
    rows.append({'cell': cell.name, 'profile': lifecycle['profile'],
                 'scope': 'whole original owner: setup, load, census, mixed run, and other untimed work; no timed-run delta available',
                 'observations': observations,
                 'observed_state_preserved': True})
assert len(rows) == 6
result = {'source': SOURCE, 'elf_sha256': ELF, 'samples': rows,
          'claim_limit': 'Counters are not per-phase. Sync time may overlap other work and is not additive wall time. Singleton observed groups do not prove that batching is safe, absent, or useful for the timed mixed workload. Reopen evidence is clean observed-state preservation, not crash certification.'}
(ROOT / 'evidence/owner-observations.json').write_text(json.dumps(result, indent=2) + '\n')
for row in rows:
    stats = row['observations']['before-close']
    print(row['cell'], 'sync_calls=', stats[KEYS[0]], 'sync_seconds=', round(stats[KEYS[1]] / 1e9, 3),
          'max_group=', stats[KEYS[2]], 'groups=', stats[KEYS[3]], 'participants=', stats[KEYS[4]])
