#!/usr/bin/env python3
"""Freeze an existing main-DB grouped-frame budget into a Go test overlay."""
import argparse
import hashlib
import json
from pathlib import Path
import tempfile

ANCHOR = "\tvm.SetDisableReadChecksum(opts.ValueLog.ReadIntegrity == IntegritySkipChecksums)"
COHORTS = (0, 16, 32, 48, 64)


def amended(source, frame_mib):
    if frame_mib not in COHORTS or source.count(ANCHOR) != 1:
        raise ValueError("unsupported budget or nonunique manager setup anchor")
    # Public Open assigns opts.Dir to the main layout before db.Open. Side
    # stores keep their existing limits; this measures the main-cache split.
    setup = (
        '\tif filepath.Base(opts.Dir) == "maindb" {\n'
        f"\t\tvm.SetGroupedFrameCacheMaxBytes({frame_mib} << 20)\n"
    )
    if frame_mib == 0:
        # MaxBytes(0) alone removes the byte bound; Entries(0) disables cache.
        setup += "\t\tvm.SetGroupedFrameCacheEntries(0)\n"
    setup += "\t}\n"
    return source.replace(ANCHOR, setup + ANCHOR)


def generate(source_root, destination, frame_mib):
    original = (source_root / "TreeDB/db/db.go").resolve()
    data = original.read_bytes()
    edited = amended(data.decode(), frame_mib).encode()
    destination.mkdir(parents=True, exist_ok=False)
    replacement = (destination / "db.go").resolve()
    replacement.write_bytes(edited)
    overlay = destination / "overlay.json"
    overlay.write_text(json.dumps({"Replace": {str(original): str(replacement)}}, indent=2) + "\n")
    identity = {
        "schema": "memory-budget-overlay-v1",
        "main_frame_budget_bytes": frame_mib << 20,
        "main_frame_entries_disabled": frame_mib == 0,
        "side_store_limits_changed": False,
        "original_path": str(original),
        "original_sha256": hashlib.sha256(data).hexdigest(),
        "replacement_sha256": hashlib.sha256(edited).hexdigest(),
        "overlay_sha256": hashlib.sha256(overlay.read_bytes()).hexdigest(),
    }
    (destination / "identity.json").write_text(json.dumps(identity, indent=2) + "\n")
    return identity


def self_check():
    source = "package db\n" + ANCHOR + "\n"
    for budget in COHORTS:
        text = amended(source, budget)
        assert text.count(ANCHOR) == 1
        assert ('filepath.Base(opts.Dir) == "maindb"') in text
        assert ("SetGroupedFrameCacheEntries(0)" in text) == (budget == 0)
    for bad_source, bad_budget in ((source, -1), (source, 1), ("", 64), (source * 2, 64)):
        try:
            amended(bad_source, bad_budget)
        except ValueError:
            pass
        else:
            raise AssertionError("accepted invalid overlay")
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        (root / "TreeDB/db").mkdir(parents=True)
        (root / "TreeDB/db/db.go").write_text(source)
        identity = generate(root, root / "out", 0)
        assert identity["main_frame_entries_disabled"]
        assert identity["original_sha256"] != identity["replacement_sha256"]
    print("memory budget overlay self-check PASS")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--frame-mib", type=int, choices=COHORTS)
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()
    if args.self_check:
        self_check()
    elif args.source_root is None or args.output is None or args.frame_mib is None:
        parser.error("source-root, output and frame-mib are required")
    else:
        print(json.dumps(generate(args.source_root, args.output, args.frame_mib)))
