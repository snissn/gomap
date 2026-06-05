#!/usr/bin/env python3
"""Unit tests for treedb_jsonbench_storage_audit.py."""

from __future__ import annotations

import importlib.util
import json
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
                + record(grouped_frame([120, 80], b"y" * 40, compressed=True, reserved=3), grouped=True)
            )
            (root / "value_vlog" / "value-l000001.log").write_bytes(
                record(b"raw-json-payload", grouped=False)
                + record(grouped_frame([50], b"z" * 25, compressed=True, reserved=1), grouped=True)
            )

            report = audit.build_report(
                type(
                    "Args",
                    (),
                    {
                        "db_dir": str(root.parent),
                        "result_json": None,
                        "gzip_level": 6,
                        "skip_retained_payload_audit": True,
                    },
                )()
            )

        self.assertTrue(report["retained_payload_status_audit"]["retained_payload_encoding_status_missing"])
        self.assertTrue(report["retained_payload_status_audit"]["retained_payload_compression_status_missing"])
        leaf = report["vlog_frame_audit"]["leaf_vlog"]
        value = report["vlog_frame_audit"]["value_vlog"]
        self.assertEqual(leaf["modes"]["grouped_block_lz4"]["raw_payload_bytes"], 300)
        self.assertEqual(leaf["modes"]["grouped_block_zstd"]["codec"], "zstd")
        self.assertEqual(leaf["modes"]["grouped_block_zstd"]["records_per_frame"], 2)
        self.assertEqual(leaf["modes"]["grouped_block_zstd"]["raw_payload_bytes_per_frame"], 200)
        self.assertEqual(leaf["raw_mode_payload_bytes"], 0)
        self.assertEqual(value["modes"]["raw_record"]["raw_payload_bytes"], len(b"raw-json-payload"))
        self.assertEqual(value["modes"]["grouped_block_snappy"]["stored_payload_bytes"], 25)
        self.assertGreater(value["raw_mode_payload_fraction"], 0)

    def test_retained_status_summary_detects_recorded_encoding_and_missing_compression(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "result.json"
            result.write_text(
                json.dumps(
                    {
                        "jsonbench_cells": [
                            {
                                "retained_payload_policy": "non-column",
                                "retained_payload_encoding": "json",
                                "retained_payload_encoding_status": "active_json_non_column_retained_payload",
                            }
                        ]
                    }
                )
            )
            status = audit.retained_status_summary(result)
        self.assertEqual(status["jsonbench_cell_count"], 1)
        self.assertFalse(status["retained_payload_encoding_status_missing"])
        self.assertFalse(status["retained_payload_encoding_inactive"])
        self.assertTrue(status["retained_payload_compression_status_missing"])
        self.assertTrue(status["retained_payload_compression_inactive"])
        self.assertEqual(status["retained_payload_encoding_missing_cells"], [])
        self.assertEqual(status["retained_payload_encoding_inactive_cells"], [])
        self.assertEqual(len(status["retained_payload_compression_missing_cells"]), 1)

    def test_retained_status_summary_requires_encoding_per_jsonbench_cell(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "result.json"
            result.write_text(
                json.dumps(
                    {
                        "jsonbench_cells": [
                            {
                                "query": "q1",
                                "retained_payload_encoding": "json",
                                "retained_payload_encoding_status": "active_json_non_column_retained_payload",
                            },
                            {"query": "q2"},
                        ]
                    }
                )
            )
            status = audit.retained_status_summary(result)
        self.assertEqual(status["jsonbench_cell_count"], 2)
        self.assertTrue(status["retained_payload_encoding_status_missing"])
        self.assertTrue(status["retained_payload_encoding_inactive"])
        self.assertEqual(
            status["retained_payload_encoding_missing_cells"],
            [
                {
                    "path": "jsonbench_cells[1]",
                    "missing_value_fields": ["retained_payload_encoding"],
                    "missing_status_fields": ["retained_payload_encoding_status"],
                }
            ],
        )

    def test_retained_status_enrichment_accepts_audit_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "result.json"
            result.write_text(json.dumps({"collection": "events"}))
            status = audit.retained_status_summary(result)
        self.assertTrue(status["retained_payload_encoding_status_missing"])
        self.assertTrue(status["retained_payload_compression_status_missing"])

        enriched = audit.enrich_retained_status_from_audit(
            status,
            {
                "status": "passed",
                "retained_payload_encoding": "template-v1",
                "retained_payload_encoding_status": "active_template_v1_non_column_retained_payload",
                "retained_payload_compression": "value_log_grouped_frame",
                "retained_payload_compression_policy": "default_value_log_auto_storage_first",
                "retained_payload_compression_status": "active_value_log_auto_grouped_frame_non_column_retained_payload",
            },
        )
        self.assertFalse(enriched["retained_payload_encoding_status_missing"])
        self.assertFalse(enriched["retained_payload_encoding_inactive"])
        self.assertFalse(enriched["retained_payload_compression_status_missing"])
        self.assertFalse(enriched["retained_payload_compression_inactive"])
        self.assertEqual(enriched["retained_payload_status_source"], "retained_payload_audit")

    def test_result_compression_summary_accepts_mixed_active_and_intentional_none(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "result.json"
            result.write_text(
                json.dumps(
                    {
                        "storage": {
                            "column_store_physical": {
                                "typed_column_parts": [
                                    {
                                        "image": {
                                            "columns_detail": [
                                                {
                                                    "column": "__treedb_primary_id",
                                                    "requested_compression": "none",
                                                    "actual_compression_mix": {"none": 2},
                                                },
                                                {
                                                    "column": "did",
                                                    "requested_compression": "lz4",
                                                    "actual_compression_mix": {"lz4": 2},
                                                },
                                                {
                                                    "column": "time_us",
                                                    "requested_compression": "lz4",
                                                    "actual_compression_mix": {"none": 2},
                                                    "fallback_reasons": {"not_smaller": 2},
                                                },
                                            ]
                                        }
                                    }
                                ]
                            }
                        },
                        "retained_payload_audit": {
                            "retained_payload_compression": "value_log_grouped_frame",
                            "retained_payload_compression_status": "active_value_log_auto_grouped_frame_non_column_retained_payload",
                        },
                    }
                )
            )

            summary = audit.load_result_compression_summary(result)

        self.assertFalse(summary["silent_none_suspected"])
        self.assertGreater(len(summary["compression_none_fields"]), 0)
        self.assertGreater(len(summary["compression_active_fields"]), 0)

    def test_result_compression_summary_flags_all_none(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "result.json"
            result.write_text(
                json.dumps(
                    {
                        "storage": {
                            "column_store_physical": {
                                "typed_column_parts": [
                                    {
                                        "image": {
                                            "columns_detail": [
                                                {
                                                    "column": "did",
                                                    "requested_compression": "none",
                                                    "actual_compression_mix": {"none": 2},
                                                }
                                            ]
                                        }
                                    }
                                ]
                            }
                        }
                    }
                )
            )

            summary = audit.load_result_compression_summary(result)

        self.assertTrue(summary["silent_none_suspected"])
        self.assertGreater(len(summary["compression_none_fields"]), 0)
        self.assertEqual(summary["compression_active_fields"], [])

    def test_retained_payload_audit_command_wrapper(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            script = Path(tmp) / "fake_retained_audit.py"
            script.write_text(
                "#!/usr/bin/env python3\n"
                "import json\n"
                "print(json.dumps({"
                "'status':'passed',"
                "'collection':'events',"
                "'checked_rows':2,"
                "'retained_payload_bytes':123,"
                "'retained_payload_encoding':'template-v1',"
                "'retained_payload_encoding_status':'active_template_v1_non_column_retained_payload',"
                "'retained_payload_compression':'value_log_grouped_frame',"
                "'retained_payload_compression_policy':'default_value_log_auto_storage_first',"
                "'retained_payload_compression_status':'active_value_log_auto_grouped_frame_non_column_retained_payload'"
                "}))\n"
            )
            script.chmod(0o755)
            args = type(
                "Args",
                (),
                {
                    "db_dir": str(Path(tmp) / "db"),
                    "retained_payload_audit_cmd": str(script),
                    "retained_payload_audit_limit": 0,
                    "skip_retained_payload_audit": False,
                },
            )()
            main = Path(tmp) / "db" / "maindb"
            main.mkdir(parents=True)
            retained = audit.run_retained_payload_audit(args, {"collection": "events"}, {}, main)

        self.assertEqual(retained["status"], "passed")
        self.assertTrue(retained["required_for_final_claim"])
        self.assertEqual(retained["checked_rows"], 2)
        self.assertIn("-paths", retained["command"])
        db_dir_index = retained["command"].index("-db-dir") + 1
        self.assertEqual(retained["command"][db_dir_index], str(main.resolve()))

    def test_retained_payload_audit_runs_without_collection_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            script = Path(tmp) / "fake_retained_audit.py"
            script.write_text(
                "#!/usr/bin/env python3\n"
                "import json\n"
                "print(json.dumps({'status':'passed','collection':'events','checked_rows':1}))\n"
            )
            script.chmod(0o755)
            main = Path(tmp) / "db" / "maindb"
            main.mkdir(parents=True)
            args = type(
                "Args",
                (),
                {
                    "db_dir": str(Path(tmp) / "db"),
                    "retained_payload_audit_cmd": str(script),
                    "retained_payload_audit_limit": 0,
                    "skip_retained_payload_audit": False,
                },
            )()
            retained = audit.run_retained_payload_audit(args, {"rows": 1}, {}, main)

        self.assertEqual(retained["status"], "passed")
        self.assertNotIn("-collection", retained["command"])
        self.assertIn("-paths", retained["command"])

    def test_resolve_main_dir_accepts_maindb(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            main = Path(tmp) / "maindb"
            main.mkdir()
            (main / "index.db").write_bytes(b"")
            self.assertEqual(audit.resolve_main_dir(main), main.resolve())


if __name__ == "__main__":
    unittest.main()
