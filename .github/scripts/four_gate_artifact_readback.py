"""Fixed original hosted artifact text readback. No ELF/tool/test execution."""
import hashlib
import json
import os
from pathlib import Path
import stat
import zipfile

EXPECTED_BYTES = 39216183
EXPECTED_SHA = "5de6712dad46d51df5b56a83c89b190116fdb9971ad32c630509bb03152e6cb7"
NAMES = {'db-test-sha256.txt', 'db-bindings.objdump', 'structural-status.txt', 'tree-compiler.exit', 'tree-test-sha256.txt', 'tree-compiler.selected.stderr', 'context.txt', 'db-compiler-stream.json', 'normal-inventory.json', 'artifact-manifest.json', 'db-compiler.exit', 'db-capture.objdump', 'tree-compiler-stream.json', 'tree-selection.exit', 'db-selection.exit', 'race-inventory.json', 'object-tools.exit', 'source.txt', 'tree-bindings.objdump', 'tree-reset.objdump', 'db-compiler.selected.stderr'}
root = Path(os.environ["RUNNER_TEMP"]) / "four-gate-readback"
archive = root / "original.zip"
assert archive.stat().st_size == EXPECTED_BYTES, "archive size mismatch"
h = hashlib.sha256()
with archive.open("rb") as f:
    for chunk in iter(lambda: f.read(1048576), b""):
        h.update(chunk)
assert h.hexdigest() == EXPECTED_SHA, "archive digest mismatch"
result = {"source_head": os.environ["GITHUB_SHA"], "artifact": 13373044724,
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
