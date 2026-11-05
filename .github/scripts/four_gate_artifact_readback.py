"""Fixed original hosted artifact text readback. No ELF/tool/test execution."""
import hashlib
import json
import os
from pathlib import Path
import stat
import zipfile

EXPECTED_BYTES = 2733520
EXPECTED_SHA = "8bc1f60e2762200e52724b73eb1cf06284d59c7ad1c1936e3f06a7fcb2a275cf"
NAMES = {
    'dispatch-admission.json',
    'entry.stdout.log',
    'entry.stderr.log',
    'guard/result.json',
    'output/admission.json',
    'output/controls-pass.json',
    'output/binding-review.json',
    'output/input-bindings.json',
    'output/compile-context.json',
    'output/cgo-tool-identities.json',
    'output/hosted-tools.json',
    'output/runtime.json',
    'output/trace-tool.json',
    'output/normal-ELF.json',
    'output/stages.json',
    'output/completion.json',
    'output/final-file-manifest.json',
    'output/trace-bijection.json',
    'output/combiner-service-validation.json',
    'output/arm/m0-ordinals.json',
    'output/arm/m0-combiner-service.json',
    'output/one-M0.stdout.log',
}
root = Path(os.environ["RUNNER_TEMP"]) / "four-gate-readback"
archive = root / "original.zip"
assert archive.stat().st_size == EXPECTED_BYTES, "archive size mismatch"
h = hashlib.sha256()
with archive.open("rb") as f:
    for chunk in iter(lambda: f.read(1048576), b""):
        h.update(chunk)
assert h.hexdigest() == EXPECTED_SHA, "archive digest mismatch"
result = {"source_head": os.environ["GITHUB_SHA"], "artifact": 13390292576,
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
        assert info.file_size <= 8 * 1024**2, "selected metadata file too large"
        total += info.file_size
        assert total <= 24 * 1024**2, "selected metadata aggregate too large"
        data = z.read(name)
        result["texts"][name] = {"size": len(data), "sha256": hashlib.sha256(data).hexdigest(),
                                 "text": data.decode("utf-8")}
data = (json.dumps(result, indent=2) + "\n").encode("utf-8")
assert len(data) <= 32 * 1024**2, "complete readback JSON too large"
(root / "readback.json").write_bytes(data)
print(data.decode("utf-8"), end="")
