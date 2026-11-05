"""Fixed original hosted artifact text readback. No ELF/tool/test execution."""
import hashlib
import json
import os
from pathlib import Path
import stat
import zipfile

EXPECTED_BYTES = 1854589
EXPECTED_SHA = "f5db8af8fd294fb9bef55344586a7fc68d0520061c7e6d6838d1f9d28ac7669b"
NAMES = {
    'entry.stderr.log',
    'entry.stdout.log',
    'guard/result.json',
    'guard/stderr.log',
    'output/admission.json',
    'output/binding-review.json',
    'output/build-normal.stderr.log',
    'output/build-normal.stdout.log',
    'output/cgo-tool-identities.json',
    'output/compile-context.json',
    'output/completion.json',
    'output/consumer-controls.stderr.log',
    'output/consumer-controls.stdout.log',
    'output/controls-pass.json',
    'output/failure.json',
    'output/final-file-manifest.json',
    'output/go-context.stderr.log',
    'output/go-context.stdout.log',
    'output/go-version.stderr.log',
    'output/go-version.stdout.log',
    'output/gofmt.stderr.log',
    'output/gofmt.stdout.log',
    'output/hosted-tools.json',
    'output/input-bindings.json',
    'output/m0-C.overlay.json',
    'output/normal-ELF.json',
    'output/normal-buildinfo.stderr.log',
    'output/normal-buildinfo.stdout.log',
    'output/one-M0.stderr.log',
    'output/one-M0.stdout.log',
    'output/resource-first-failure.json',
    'output/runtime.json',
    'output/source-C-identity.stderr.log',
    'output/source-C-identity.stdout.log',
    'output/stages.json',
    'output/trace-parsed.stderr.log',
    'output/trace-tool-buildinfo.stderr.log',
    'output/trace-tool-buildinfo.stdout.log',
    'output/trace-tool-path.stderr.log',
    'output/trace-tool-path.stdout.log',
    'output/trace-tool-sources.json',
    'output/trace-tool.json',
    'post-guard-output.json',
}
root = Path(os.environ["RUNNER_TEMP"]) / "four-gate-readback"
archive = root / "original.zip"
assert archive.stat().st_size == EXPECTED_BYTES, "archive size mismatch"
h = hashlib.sha256()
with archive.open("rb") as f:
    for chunk in iter(lambda: f.read(1048576), b""):
        h.update(chunk)
assert h.hexdigest() == EXPECTED_SHA, "archive digest mismatch"
result = {"source_head": os.environ["GITHUB_SHA"], "artifact": 13377421539,
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
