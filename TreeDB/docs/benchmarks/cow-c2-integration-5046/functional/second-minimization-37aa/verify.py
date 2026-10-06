import json,hashlib,sys
from pathlib import Path
a=Path(sys.argv[1]);s=Path(sys.argv[2]);m=json.loads((a/'source-identity.json').read_text());actual={str(p.relative_to(s)):hashlib.sha256(p.read_bytes()).hexdigest() for p in s.rglob('*') if p.is_file()};bad=sorted(k for k in set(actual)|set(m['files']) if actual.get(k)!=m['files'].get(k));v=dict(commit=m['commit'],file_count=len(actual),mismatches=bad,archive_sha256=hashlib.sha256((a/'source.tar').read_bytes()).hexdigest());print(json.dumps(v));assert not bad
