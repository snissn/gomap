#!/usr/bin/env python3
"""Unit tests for treedb_jsonbench_storage_audit.py."""

from __future__ import annotations

import importlib.util
import struct
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("treedb_jsonbench_storage_audit.py")
spec = importlib.util.spec_from_file_location("treedb_jsonbench_storage_audit", SCRIPT)
audit = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(audit)


def record(body: bytes, grouped: bool) -> bytes:
    header = bytearray(20)
    header[4] = 1
    header[5] = 1 if grouped else 0
    struct.pack_into("<I", header, 16, len(body))
    return bytes(header) + body


def grouped_frame(raw_lengths: list[int], stored_payload: bytes, *, compressed: bool, reserved: int, dict_id: int = 0) -> bytes:
    k = len(raw_lengths)
    frame = bytearray()
    frame.extend(bytes([1, 1 if compressed else 0, k, reserved]))
    frame.extend(struct.pack("<Q", dict_id))
    frame.extend(b"\x00" * (k * 8))
    offset = 0
    offsets = [0]
    for length in raw_lengths:
        offset += length
        offsets.append(offset)
    for value in offsets:
        frame.extend(struct.pack("<I", value))
    frame.extend(stored_payload)
    return bytes(frame)


class StorageAuditTest(unittest.TestCase):
    def test_frame_audit_separates_leaf_and_value_modes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "db" / "maindb"
            (root / "leaf_vlog").mkdir(parents=True)
            (root / "value_vlog").mkdir()
            (root / "column_assets").mkdir()
            (root / "index.db").write_bytes(b"index")
            (root / "leaf_vlog" / "value-l255-000001.log").write_bytes(
                record(grouped_frame([100, 200], b"x" * 80, compressed=True, reserved=2), grouped=True)
            )
            (root / "value_vlog" / "value-l000001.log").write_bytes(
                record(b"raw-json-payload", grouped=False)
                + record(grouped_frame([50], b"z" * 25, compressed=True, reserved=1), grouped=True)
            )

            report = audit.build_report(
                type("Args", (), {"db_dir": str(root.parent), "result_json": None, "gzip_level": 6})()
            )

        leaf = report["vlog_frame_audit"]["leaf_vlog"]
        value = report["vlog_frame_audit"]["value_vlog"]
        self.assertEqual(leaf["modes"]["grouped_block_lz4"]["raw_payload_bytes"], 300)
        self.assertEqual(leaf["raw_mode_payload_bytes"], 0)
        self.assertEqual(value["modes"]["raw_record"]["raw_payload_bytes"], len(b"raw-json-payload"))
        self.assertEqual(value["modes"]["grouped_block_snappy"]["stored_payload_bytes"], 25)
        self.assertGreater(value["raw_mode_payload_fraction"], 0)

    def test_resolve_main_dir_accepts_maindb(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            main = Path(tmp) / "maindb"
            main.mkdir()
            (main / "index.db").write_bytes(b"")
            self.assertEqual(audit.resolve_main_dir(main), main.resolve())


if __name__ == "__main__":
    unittest.main()
