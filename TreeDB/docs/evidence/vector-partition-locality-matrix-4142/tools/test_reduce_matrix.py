#!/usr/bin/env python3
from __future__ import annotations

import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

sys.dont_write_bytecode = True

import reduce_matrix as reducer
import preflight_matrix as preflight_contract


IDENTITY = "a" * 64


def topology_contract() -> dict:
    return {
        "schema_version": 1,
        "result_kind": "vector_partition_locality_matrix_topology_contract_v1",
        "graph_sha256": preflight_contract.FROZEN_GRAPH_SHA256,
        "topologies": [
            {"layout": layout, "partitions": partitions, "overlap": overlap, "membership_sha256": f"{index + 1:064x}", "router_sha256": f"{index + 101:064x}"}
            for index, (layout, partitions, overlap) in enumerate(sorted(preflight_contract.TOPOLOGY_COORDINATES))
        ],
    }


def topology_identity(layout: str, partitions: int, overlap: str) -> tuple[str, str]:
    for topology in topology_contract()["topologies"]:
        if (topology["layout"], topology["partitions"], topology["overlap"]) == (layout, partitions, overlap):
            return topology["membership_sha256"], topology["router_sha256"]
    raise AssertionError("missing test topology")


def row(row_id: str) -> dict:
    return {
        "schema_version": 1, "result_kind": "vector_partition_locality_matrix_row_v1", "row_id": row_id, "terminal": True,
        "source_head": preflight_contract.MEASURED_SOURCE_HEAD, "binary_sha256": IDENTITY,
        "dataset_sha256": preflight_contract.FROZEN_DATASET_SHA256, "truth_sha256": preflight_contract.FROZEN_TRUTH_SHA256,
        "graph_sha256": preflight_contract.FROZEN_GRAPH_SHA256, "membership_sha256": topology_identity("entry-first-bfs", 16, "zero")[0], "router_sha256": topology_identity("entry-first-bfs", 16, "zero")[1],
        "query_union_sha256": preflight_contract.FROZEN_QUERY_UNION_SHA256,
        "query_split_sha256": preflight_contract.FROZEN_HOLDOUT_SHA256,
        "layout": "entry-first-bfs", "partitions": 16, "overlap": "zero", "probes": 2, "ef": 96, "split": "holdout",
        "metrics": {"queries": 194, "filler_replicas": 0, "unique_pages_per_query": 1.0},
    }


def complete_rows() -> list[dict]:
    rows = []
    for index, point in enumerate(sorted(reducer.AUTHORIZED_COORDINATES)):
        value = row(f"{index:04d}")
        value["layout"], value["partitions"], value["overlap"], value["probes"], value["ef"], value["split"] = point
        value["membership_sha256"], value["router_sha256"] = topology_identity(value["layout"], value["partitions"], value["overlap"])
        if value["split"] == "train":
            value["query_split_sha256"] = preflight_contract.FROZEN_CALIBRATION_SHA256
            value["metrics"]["queries"] = 806
        if value["overlap"] == "exact-20%":
            value["metrics"]["filler_replicas"] = 1
        rows.append(value)
    return rows


def preflight() -> dict:
    return {
        "schema_version": 1,
        "result_kind": "vector_partition_locality_matrix_preflight_v1",
        "source_head": preflight_contract.MEASURED_SOURCE_HEAD,
        "binary_sha256": IDENTITY,
        "frozen_head": preflight_contract.FROZEN_INPUT_HEAD,
        "campaign_sha256": preflight_contract.FROZEN_CAMPAIGN_SHA256,
        "descriptor_sha256": preflight_contract.FROZEN_DESCRIPTOR_SHA256,
        "descriptor_head": preflight_contract.FROZEN_INPUT_HEAD,
        "dataset_sha256": preflight_contract.FROZEN_DATASET_SHA256,
        "truth_sha256": preflight_contract.FROZEN_TRUTH_SHA256,
        "graph_sha256": preflight_contract.FROZEN_GRAPH_SHA256,
        "calibration_sha256": preflight_contract.FROZEN_CALIBRATION_SHA256,
        "holdout_sha256": preflight_contract.FROZEN_HOLDOUT_SHA256,
        "query_union_sha256": preflight_contract.FROZEN_QUERY_UNION_SHA256,
        "binary_vcs_revision": preflight_contract.MEASURED_SOURCE_HEAD,
        "binary_vcs_modified": "false",
        "topology_contract_sha256": IDENTITY,
        "status": "ready",
    }


def expected_identity() -> dict:
    return {key: preflight()[key] for key in (*reducer.CAMPAIGN_IDENTITIES, "calibration_sha256", "holdout_sha256")}


class ReducerTest(unittest.TestCase):
    def test_query_union_is_complete_and_disjoint(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            paths = [Path(directory) / name for name in ("calibration.json", "holdout.json")]
            for path, ordinals in zip(paths, (list(range(806)), list(range(806, 1000)))):
                path.write_text(json.dumps({"schema": "vector_partition_4105_query_split_v1", "ordinals": ordinals}), encoding="utf-8")
            self.assertEqual(preflight_contract.query_union(*paths), preflight_contract.FROZEN_QUERY_UNION_SHA256)
            value = json.loads(paths[1].read_text(encoding="utf-8"))
            value["ordinals"][0] = 0
            paths[1].write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaisesRegex(SystemExit, "complete disjoint"):
                preflight_contract.query_union(*paths)

    def test_evidence_output_preserves_existing_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "evidence.json"
            path.write_text("retain", encoding="utf-8")
            with self.assertRaises(FileExistsError):
                preflight_contract.write_json_exclusive(path, {})
            self.assertEqual(path.read_text(encoding="utf-8"), "retain")

    def test_row_identity_and_accounting_are_required(self) -> None:
        value = row("a")
        expected = expected_identity()
        reducer.validate_row(value, expected)
        for mutate, message in ((lambda x: x.update(source_head="b" * 40), "mixed identity"), (lambda x: x.update(terminal=False), "nonterminal"), (lambda x: x["metrics"].update(filler_replicas=1), "filler")):
            changed = copy.deepcopy(value)
            mutate(changed)
            with self.assertRaisesRegex(reducer.ContractError, message):
                reducer.validate_row(changed, expected)

    def test_matrix_rejects_duplicate_reordered_mixed_and_incomplete_rows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def write(name: str, value: dict) -> Path:
                path = root / name
                path.write_text(json.dumps(value), encoding="utf-8")
                return path
            preflight_path = write("preflight.json", preflight())
            contract_path = write("topology-contract.json", topology_contract())
            value = preflight()
            value["topology_contract_sha256"] = reducer.sha256(contract_path)
            preflight_path.write_text(json.dumps(value), encoding="utf-8")
            paths = [write(f"{value['row_id']}.json", value) for value in complete_rows()]
            self.assertFalse(preflight_contract.topology_contract_is_approved(value["topology_contract_sha256"]))
            with mock.patch.object(preflight_contract, "APPROVED_TOPOLOGY_CONTRACT_SHA256", value["topology_contract_sha256"]):
                self.assertEqual(reducer.reduce_rows(paths, preflight_path, contract_path)["rows"], len(reducer.AUTHORIZED_COORDINATES))
                summary_path = root / "summary.json"
                with mock.patch.object(sys, "argv", ["reduce_matrix.py", "--preflight", str(preflight_path), "--topology-contract", str(contract_path), "--out", str(summary_path), *map(str, paths)]):
                    reducer.main()
                self.assertEqual(json.loads(summary_path.read_text(encoding="utf-8"))["rows"], len(reducer.AUTHORIZED_COORDINATES))
                changed_contract = topology_contract()
                changed_contract["topologies"][0]["router_sha256"] = "f" * 64
                with self.assertRaisesRegex(reducer.ContractError, "does not match preflight"):
                    reducer.reduce_rows(paths, preflight_path, write("changed-topology-contract.json", changed_contract))
                with self.assertRaisesRegex(reducer.ContractError, "incomplete"):
                    reducer.reduce_rows(paths[:-1], preflight_path, contract_path)
                with self.assertRaisesRegex(reducer.ContractError, "duplicate"):
                    reducer.reduce_rows(paths + [paths[0]], preflight_path, contract_path)
                duplicate_coordinate = copy.deepcopy(complete_rows()[0])
                duplicate_coordinate["row_id"] = "distinct-duplicate"
                with self.assertRaisesRegex(reducer.ContractError, "duplicate coordinate"):
                    reducer.reduce_rows(paths + [write("duplicate-coordinate.json", duplicate_coordinate)], preflight_path, contract_path)
                unauthorized_coordinate = copy.deepcopy(complete_rows()[0])
                with mock.patch.object(reducer, "AUTHORIZED_COORDINATES", reducer.AUTHORIZED_COORDINATES - {reducer.coordinate(unauthorized_coordinate)}):
                    with self.assertRaisesRegex(reducer.ContractError, "unauthorized coordinate"):
                        reducer.reduce_rows(paths, preflight_path, contract_path)
                with self.assertRaisesRegex(reducer.ContractError, "reordered"):
                    reducer.reduce_rows([paths[1], paths[0]] + paths[2:], preflight_path, contract_path)
                mixed = complete_rows()[-1]
                mixed["dataset_sha256"] = "b" * 64
                with self.assertRaisesRegex(reducer.ContractError, "mixed identity"):
                    reducer.reduce_rows(paths[:-1] + [write("mixed.json", mixed)], preflight_path, contract_path)
                mixed_topology = complete_rows()[-1]
                mixed_topology["membership_sha256"] = "b" * 64
                with self.assertRaisesRegex(reducer.ContractError, "pinned contract"):
                    reducer.reduce_rows(paths[:-1] + [write("mixed-topology.json", mixed_topology)], preflight_path, contract_path)
                consistently_mislabeled = complete_rows()
                source = ("source-order", 16, "zero")
                replacement = topology_identity("entry-first-bfs", 16, "zero")
                for value in consistently_mislabeled:
                    if (value["layout"], value["partitions"], value["overlap"]) == source:
                        value["membership_sha256"], value["router_sha256"] = replacement
                mislabeled_paths = [write(f"mislabeled-{value['row_id']}.json", value) for value in consistently_mislabeled]
                with self.assertRaisesRegex(reducer.ContractError, "pinned contract"):
                    reducer.reduce_rows(mislabeled_paths, preflight_path, contract_path)
                blocked = preflight()
                blocked["status"] = "blocked_source_identity"
                with self.assertRaisesRegex(reducer.ContractError, "preflight"):
                    reducer.reduce_rows(paths, write("blocked.json", blocked), contract_path)

    def test_variant_identities_and_exact_baseline_filler_are_permitted(self) -> None:
        value = row("exact")
        value["overlap"] = "exact-20%"
        value["metrics"]["filler_replicas"] = 3
        value["membership_sha256"] = "b" * 64
        value["router_sha256"] = "c" * 64
        expected = expected_identity()
        reducer.validate_row(value, expected)

    def test_rejects_invalid_numeric_metrics(self) -> None:
        for value in (True, -1, float("nan"), float("inf")):
            changed = row("metric")
            changed["metrics"]["unique_pages_per_query"] = value
            with self.assertRaisesRegex(reducer.ContractError, "metric"):
                reducer.validate_row(changed, expected_identity())

    def test_rejects_wrong_split_identity_and_empty_measurement(self) -> None:
        expected = expected_identity()
        wrong_split = row("wrong-split")
        wrong_split["query_split_sha256"] = preflight_contract.FROZEN_CALIBRATION_SHA256
        with self.assertRaisesRegex(reducer.ContractError, "query split"):
            reducer.validate_row(wrong_split, expected)
        empty = row("incomplete")
        empty["metrics"]["queries"] = 1
        with self.assertRaisesRegex(reducer.ContractError, "full frozen split"):
            reducer.validate_row(empty, expected)
        for metric in ("unique_pages_per_query",):
            empty = row("empty")
            empty["metrics"][metric] = 0
            with self.assertRaisesRegex(reducer.ContractError, "no measured pages"):
                reducer.validate_row(empty, expected)


if __name__ == "__main__":
    unittest.main()
