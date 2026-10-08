"""Read-only integrity check for this portable evidence packet (no Go/runtime)."""
import hashlib
import json
from pathlib import Path

root = Path(__file__).resolve().parent
inventory = json.loads((root / 'file-inventory.json').read_text())
for row in inventory['files']:
    path = root / row['path']
    assert path.is_file() and not path.is_symlink(), row['path']
    data = path.read_bytes()
    assert len(data) == row['bytes'], row['path']
    assert hashlib.sha256(data).hexdigest() == row['sha256'], row['path']
    if path.suffix == '.json' and not row.get('historical_non_json_or_empty'):
        json.loads(data)
print(f"Verified {len(inventory['files'])} preserved files / {inventory['bytes']} bytes")
print(f"Retained {len(inventory.get('historical_format_exceptions', []))} inventoried historical empty/non-JSON outputs unchanged")
