"""Fixed original hosted artifact text readback. No ELF/tool/test execution."""
import hashlib
import json
import os
from pathlib import Path
import stat
import zipfile

EXPECTED_BYTES = 310679
EXPECTED_SHA = "69b9ba87c21f31cfe0beee4b7bb8afe2d55f1f764136ef7ffd5783a56fa86fcc"
NAMES = {'guard/result.json', 'data/vlog_auto_incompressible_auto_sample1.rusage.json', 'data/process-usage-validation.json', 'data/row-exits.tsv', 'data/vlog_auto_incompressible_off_sample4.rusage.json', 'data/vlog_auto_incompressible_off_sample6.rusage.json', 'data/vlog_auto_incompressible_auto_sample3.rusage.json', 'scope.txt', 'data/go-env.json', 'data/runtime-context.json', 'guard/execution-start.json', 'data/vlog_auto_incompressible_off_sample5.rusage.json', 'data/go-version.txt', 'data/vlog_auto_incompressible_off_sample1.rusage.json', 'data/vlog_auto_incompressible_auto_sample6.rusage.json', 'data/pprof-tool-sha256.txt', 'data/vlog_auto_incompressible_auto_sample5.rusage.json', 'data/vlog_auto_incompressible_off_sample3.rusage.json', 'data/process-usage-validation.exit', 'input-hashes.txt', 'guard.exit', 'data/pprof-tool-buildinfo.txt', 'guard/root.identity.json', 'data/vlog_auto_incompressible_auto_sample2.rusage.json', 'data/vlog_auto_incompressible_off_sample2.rusage.json', 'dispatch.txt', 'source.txt', 'data/vlog_auto_incompressible_auto_sample4.rusage.json', 'data/build-tools.json'}
root = Path(os.environ["RUNNER_TEMP"]) / "four-gate-readback"
archive = root / "original.zip"
assert archive.stat().st_size == EXPECTED_BYTES, "archive size mismatch"
h = hashlib.sha256()
with archive.open("rb") as f:
    for chunk in iter(lambda: f.read(1048576), b""):
        h.update(chunk)
assert h.hexdigest() == EXPECTED_SHA, "archive digest mismatch"
result = {"source_head": os.environ["GITHUB_SHA"], "artifact": 13372929344,
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
