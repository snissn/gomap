#!/usr/bin/env python3
"""Validate diagnostic records without summing repeated cache captures."""
import argparse
import json
from collections import Counter
from pathlib import Path


def validate(record):
    st = record['Stats']
    allocated, entries = st['AllocatedSlots'], st['Entries']
    live, all_caps, empty = (record[key] for key in ('LiveK', 'AllSlotOffsetCapacity', 'EmptySlotOffsetCapacity'))
    assert record['Schema'] == 1
    assert sum(live.values()) == entries
    assert sum(all_caps.values()) == allocated
    assert sum(empty.values()) == allocated-entries
    assert all(0 <= int(k) <= 256 and n >= 0 for k, n in all_caps.items())
    assert all(1 <= int(k) <= 255 and n >= 0 for k, n in live.items())
    assert all(n <= all_caps[k] for k, n in empty.items())
    offset_bytes = sum(int(k)*4*n for k, n in all_caps.items())
    assert record['SlotStructBytes'] == allocated*record['SlotSizeBytes']
    if record['Inline']:
        assert all(int(k) == 256 for k in all_caps)
        assert record['InlineOffsetBytes'] == offset_bytes and record['OffsetBackingBytes'] == 0
    else:
        assert record['InlineOffsetBytes'] == 0 and record['OffsetBackingBytes'] == offset_bytes
    assert record['StructuralMetadataBytes'] == record['SlotStructBytes']+record['OffsetBackingBytes']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('jsonl', type=Path)
    args = parser.parse_args()
    caches, versions, count = set(), Counter(), 0
    for line in args.jsonl.read_text().splitlines():
        if not line.strip(): continue
        record = json.loads(line)
        validate(record)
        caches.add((record['PID'], record['Cache']))
        versions['inline' if record['Inline'] else 'slice'] += 1
        count += 1
    assert count > 0, 'empty diagnostic packet'
    print(json.dumps({'result': 'PASS', 'records': count, 'distinct_caches': len(caches), 'representation_records': dict(versions), 'aggregation': 'none; repeated captures are not additive'}))


if __name__ == '__main__':
    main()
