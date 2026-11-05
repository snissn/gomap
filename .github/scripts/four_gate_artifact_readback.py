"""Fixed original hosted artifact text readback. No ELF/tool/test execution."""
import hashlib
import json
import os
from pathlib import Path
import stat
import zipfile

EXPECTED_BYTES = 46884
EXPECTED_SHA = "8ce619ae377de88c846e8c0a015e73081c3b7256ad7c4897798ebc0fe1e5e3fa"
NAMES = {'output/stages.json', 'guard/stderr.log', 'output/final-file-manifest.json', 'output/failure.json', 'output/race.stdout.log', 'output/gofmt.stderr.log', 'entry.stderr.log', 'output/race.stderr.log', 'output/completion.json', 'guard/result.json', 'output/normal.stderr.log', 'output/race-inventory.json', 'entry.stdout.log', 'guard/stdout.log', 'output/pure-controls.stdout.log', 'output/compile-context.json', 'output/normal.stdout.log', 'output/pure-controls.stderr.log', 'output/normal-inventory.json', 'output/gofmt.stdout.log'}
root = Path(os.environ["RUNNER_TEMP"]) / "four-gate-readback"
archive = root / "original.zip"
assert archive.stat().st_size == EXPECTED_BYTES, "archive size mismatch"
h = hashlib.sha256()
with archive.open("rb") as f:
    for chunk in iter(lambda: f.read(1048576), b""):
        h.update(chunk)
assert h.hexdigest() == EXPECTED_SHA, "archive digest mismatch"
result = {"source_head": os.environ["GITHUB_SHA"], "artifact": 13374174271,
          "archive_sha256": h.hexdigest(), "scope": "retained original text only; no gate alteration",
          "entries": [], "texts": {}, "missing": []}
with zipfile.ZipFile(archive) as z:
    names = z.namelist()
    assert len(names) == len(set(names)), "duplicate ZIP name"
    for item in z.infolist():
        parts = item.filename.split("/")
        assert not item.filename.startswith("/") and ".." not in parts and "\\" not in item.filename
        assert not stat.S_ISLNK(item.external_attr >> 16), "ZIP link"
        result["entries"].append({"name": item.filename, "size": item.file_size})
    total = 0
    for name in sorted(NAMES):
        if name not in names:
            result["missing"].append(name)
            continue
        info = z.getinfo(name)
        assert info.file_size <= 131072, "selected metadata file too large"
        total += info.file_size
        assert total <= 1048576, "selected metadata aggregate too large"
        data = z.read(name)
        result["texts"][name] = {"size": len(data), "sha256": hashlib.sha256(data).hexdigest(),
                                 "text": data.decode("utf-8")}
(root / "readback.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps(result, indent=2))
