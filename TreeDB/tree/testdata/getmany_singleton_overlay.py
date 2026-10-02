#!/usr/bin/env python3
"""Extend the existing checked-loader overlay with untimed planner entries."""
import json
import pathlib
import subprocess
import sys

root = pathlib.Path(sys.argv[1]).resolve()
output = pathlib.Path(sys.argv[2]).resolve()
subprocess.run([sys.executable, str(root / "scripts/treedb_algorithm_work_overlay.py"), str(root), str(output)], check=True)
source = output / "tree.go"
text = source.read_text()
start = text.index("func (t *Tree) planGetManyInterval(")
entry = text.index("{", start) + 1
source.write_text(text[:entry] + "\nalgorithmWorkIntervalEntries++\n" + text[entry:])
helper = output / "singleton.go"
helper.write_text("package tree\nvar algorithmWorkIntervalEntries uint64\n")
manifest = output / "overlay.json"
data = json.loads(manifest.read_text())
data["Replace"][str(root / "TreeDB/tree/getmany_singleton_overlay.go")] = str(helper)
manifest.write_text(json.dumps(data, indent=2) + "\n")
