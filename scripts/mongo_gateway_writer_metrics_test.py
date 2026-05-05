#!/usr/bin/env python3
import csv
import importlib.util
import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location(
    "mongo_gateway_writer_metrics",
    ROOT / "mongo_gateway_writer_metrics.py",
)
writer_metrics = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(writer_metrics)


class WriterMetricsTests(unittest.TestCase):
    def test_missing_and_malformed_json_warn_and_keep_header(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            bad_json = out_dir / "bad.json"
            bad_json.write_text("{not json", encoding="utf-8")
            matrix = out_dir / "matrix.tsv"
            matrix.write_text(
                "target\tconfig\tdocuments\tsecondary_indexes\traw_json\n"
                "treedb\tmissing\t100\t0\tmissing.json\n"
                "treedb\tbad\t100\t0\tbad.json\n",
                encoding="utf-8",
            )
            output = out_dir / "writer_metrics.tsv"

            stderr = io.StringIO()
            with redirect_stderr(stderr):
                writer_metrics.write_writer_metrics(str(out_dir), str(matrix), str(output))

            warnings = stderr.getvalue()
            self.assertIn("missing.json", warnings)
            self.assertIn("bad.json", warnings)
            with output.open(newline="") as out_file:
                rows = list(csv.reader(out_file, delimiter="\t"))
            self.assertEqual(rows, [writer_metrics.COLUMNS])

    def test_schema_invalid_json_warns_and_keeps_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            (out_dir / "array.json").write_text("[]", encoding="utf-8")
            (out_dir / "bad_phases.json").write_text('{"phases":{}}', encoding="utf-8")
            (out_dir / "null_phase.json").write_text('{"target":"treedb","phases":[null]}', encoding="utf-8")
            matrix = out_dir / "matrix.tsv"
            matrix.write_text(
                "target\tconfig\tdocuments\tsecondary_indexes\traw_json\n"
                "treedb\tarray\t100\t0\tarray.json\n"
                "treedb\tbad_phases\t100\t0\tbad_phases.json\n"
                "treedb\tnull_phase\t100\t0\tnull_phase.json\n",
                encoding="utf-8",
            )
            output = out_dir / "writer_metrics.tsv"

            stderr = io.StringIO()
            with redirect_stderr(stderr):
                writer_metrics.write_writer_metrics(str(out_dir), str(matrix), str(output))

            warnings = stderr.getvalue()
            self.assertIn("top-level JSON is not an object", warnings)
            self.assertIn("phases is not a list", warnings)
            self.assertIn("non-object phase", warnings)
            with output.open(newline="") as out_file:
                rows = list(csv.reader(out_file, delimiter="\t"))
            self.assertEqual(rows, [writer_metrics.COLUMNS])

    def test_exact_integer_composites_and_invalid_present_values(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            huge = "900719925474099312345"
            raw = out_dir / "run.json"
            raw.write_text(
                json.dumps({
                    "target": "treedb",
                    "documents": 100,
                    "secondary_indexes": 0,
                    "phases": [{
                        "name": "concurrent_id_update_set_w8",
                        "ops_per_sec": 1,
                        "sampled_ns_per_op": 2,
                        "driver_calls": 3,
                        "treedb_drain_ms": 4.25,
                        "treedb_drain_stats_delta": {
                            "treedb.collections.write_domain.indexed_flush.calls_total": "1",
                            "treedb.collections.write_domain.coalesced_flush_batch.batches_total": "1",
                            "treedb.collections.write_domain.primary_only.drains_total": "0",
                        },
                        "treedb_metrics": {
                            "coalesced_batch_units/batch": 2,
                            "coalesced_batch_docs/batch": 50,
                            "coalesced_batch_bytes/batch": 4096,
                            "read_only_prepare_calls/doc": 0.1,
                            "read_only_prepare_ns/doc": 25,
                            "read_only_prepare_ns/plan": 250,
                            "read_only_prepare_ops/doc": 3,
                            "read_only_prepare_leaf_spans/plan": 6,
                            "read_only_prepare_worker_targets/plan": 4,
                            "read_only_prepare_worker_ranges/plan": 3,
                            "read_only_prepare_worker_max_ops/plan": 512,
                            "raw_root_delta_entries/doc": 6,
                            "raw_primary_root_delta_entries/doc": 2,
                            "raw_primary_root_delta_tombstones/doc": 0.1,
                            "raw_template_root_delta_entries/doc": 0.25,
                            "raw_template_root_delta_bytes/doc": 25,
                            "raw_template_root_delta_tombstones/doc": 0,
                            "raw_index_state_root_delta_entries/doc": 0.5,
                            "raw_index_state_root_delta_bytes/doc": 50,
                            "raw_index_state_root_delta_tombstones/doc": 0,
                            "raw_secondary_root_delta_entries/doc": 4,
                            "raw_secondary_root_delta_tombstones/doc": 0,
                            "final_root_delta_entries/doc": 3,
                            "final_primary_root_delta_entries/doc": 1,
                            "final_primary_root_delta_tombstones/doc": 0,
                            "final_template_root_delta_entries/doc": 0.125,
                            "final_template_root_delta_bytes/doc": 12.5,
                            "final_template_root_delta_tombstones/doc": 0,
                            "final_index_state_root_delta_entries/doc": 0.25,
                            "final_index_state_root_delta_bytes/doc": 25,
                            "final_index_state_root_delta_tombstones/doc": 0,
                            "final_secondary_root_delta_entries/doc": 2,
                            "final_secondary_root_delta_tombstones/doc": 0,
                            "squashed_root_delta_entries/doc": 3,
                            "net_zero_root_batches/doc": 0,
                            "net_zero_root_plans/doc": 0.25,
                            "skipped_secondary_roots/doc": 2.5,
                            "primary_only_duplicate_ids_coalesced/doc": 0.75,
                            "primary_only_drains/doc": 0.125,
                        },
                        "treedb_stats_delta": {
                            "treedb.collections.write_domain.indexed_async_flush.backpressure_sync_total": huge,
                            "treedb.collections.write_domain.collection_root_base_mismatch_total": "bad",
                            "treedb.collections.write_domain.indexed_flush.root_base_mismatch_total": "1",
                            "treedb.collections.write_domain.root_delta_plan.raw_unit.primary.entries_total": huge,
                            "treedb.collections.write_domain.root_delta_plan.raw_unit.secondary.entries_total": "11",
                            "treedb.collections.write_domain.root_delta_plan.final.primary.entries_total": "7",
                            "treedb.collections.write_domain.root_delta_plan.final.secondary.entries_total": "5",
                            "treedb.collections.write_domain.root_delta_plan.squashed_entries_total": "9",
                            "treedb.collections.write_domain.coalesced_flush_batch.net_zero_batches_total": "0",
                            "treedb.collections.write_domain.primary_only.duplicate_ids_coalesced_total": "6",
                            "treedb.collections.write_domain.primary_only.drains_total": "2",
                            "treedb.publish.ordered_root_delta_group.root_apply_readonly_prepare_calls_total": "10",
                            "treedb.publish.ordered_root_delta_group.root_apply_readonly_prepare_worker_targets_total": "40",
                            "treedb.publish.ordered_root_delta_group.root_apply_readonly_prepare_worker_ranges_total": "30",
                        },
                    }],
                }),
                encoding="utf-8",
            )
            matrix = out_dir / "matrix.tsv"
            matrix.write_text(
                "target\tconfig\tdocuments\tsecondary_indexes\traw_json\n"
                "treedb\tcfg\t100\t0\trun.json\n",
                encoding="utf-8",
            )
            output = out_dir / "writer_metrics.tsv"

            stderr = io.StringIO()
            with redirect_stderr(stderr):
                writer_metrics.write_writer_metrics(str(out_dir), str(matrix), str(output))

            with output.open(newline="") as out_file:
                rows = list(csv.DictReader(out_file, delimiter="\t"))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["treedb_drain_ms"], "4.25")
            self.assertEqual(rows[0]["drain_indexed_flush_calls_total"], "1")
            self.assertEqual(rows[0]["drain_coalesced_flush_batches_total"], "1")
            self.assertEqual(rows[0]["drain_primary_only_drains_total"], "0")
            self.assertEqual(rows[0]["coalesced_batch_units_per_batch"], "2")
            self.assertEqual(rows[0]["read_only_prepare_calls_per_doc"], "0.1")
            self.assertEqual(rows[0]["read_only_prepare_ns_per_doc"], "25")
            self.assertEqual(rows[0]["read_only_prepare_ns_per_plan"], "250")
            self.assertEqual(rows[0]["read_only_prepare_worker_targets_per_plan"], "4")
            self.assertEqual(rows[0]["read_only_prepare_worker_ranges_per_plan"], "3")
            self.assertEqual(rows[0]["read_only_prepare_worker_max_ops_per_plan"], "512")
            self.assertEqual(rows[0]["raw_root_delta_entries_per_doc"], "6")
            self.assertEqual(rows[0]["raw_primary_root_delta_entries_per_doc"], "2")
            self.assertEqual(rows[0]["raw_primary_root_delta_tombstones_per_doc"], "0.1")
            self.assertEqual(rows[0]["raw_template_root_delta_entries_per_doc"], "0.25")
            self.assertEqual(rows[0]["raw_index_state_root_delta_bytes_per_doc"], "50")
            self.assertEqual(rows[0]["raw_secondary_root_delta_entries_per_doc"], "4")
            self.assertEqual(rows[0]["raw_secondary_root_delta_tombstones_per_doc"], "0")
            self.assertEqual(rows[0]["final_root_delta_entries_per_doc"], "3")
            self.assertEqual(rows[0]["final_template_root_delta_bytes_per_doc"], "12.5")
            self.assertEqual(rows[0]["final_index_state_root_delta_tombstones_per_doc"], "0")
            self.assertEqual(rows[0]["final_secondary_root_delta_tombstones_per_doc"], "0")
            self.assertEqual(rows[0]["squashed_root_delta_entries_per_doc"], "3")
            self.assertEqual(rows[0]["net_zero_root_batches_per_doc"], "0")
            self.assertEqual(rows[0]["backpressure_sync_total"], huge)
            self.assertEqual(rows[0]["root_mismatch_total"], "")
            self.assertEqual(rows[0]["readonly_prepare_calls_total"], "10")
            self.assertEqual(rows[0]["readonly_prepare_worker_targets_total"], "40")
            self.assertEqual(rows[0]["readonly_prepare_worker_ranges_total"], "30")
            self.assertEqual(rows[0]["root_delta_plan_raw_unit_primary_entries_total"], huge)
            self.assertEqual(rows[0]["root_delta_plan_raw_unit_secondary_entries_total"], "11")
            self.assertEqual(rows[0]["root_delta_plan_final_primary_entries_total"], "7")
            self.assertEqual(rows[0]["root_delta_plan_squashed_entries_total"], "9")
            self.assertEqual(rows[0]["coalesced_flush_net_zero_batches_total"], "0")
            self.assertEqual(rows[0]["primary_only_duplicate_ids_coalesced_total"], "6")
            self.assertEqual(rows[0]["primary_only_drains_total"], "2")
            self.assertIn("treedb_drain_ms", rows[0])
            self.assertIn("root_mismatch_total", stderr.getvalue())


if __name__ == "__main__":
    raise SystemExit(unittest.main())
