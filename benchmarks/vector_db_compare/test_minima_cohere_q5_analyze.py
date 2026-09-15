import copy
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import minima_cohere_q5_analyze as analyzer


COMMIT = "a" * 40


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode() + b"\n"


class Q5AnalyzeTest(unittest.TestCase):
    @staticmethod
    def _append_construction_calls(events, rows):
        def append(phase, **fields):
            start = len(events) * 10 + 1
            events.append({
                "event": "call", "phase": phase, "start_ns": start,
                "end_ns": start + 5, "duration_ns": 5, "outcome": "completed",
                **fields,
            })

        append("service_start")
        append("schema_ensure")
        for start in range(0, rows, 256):
            append("initial_durable_ingest", first_row=start, rows=min(256, rows - start))
        append("initial_graph_build")

    def test_strict_json_rejects_duplicates_and_nonfinite(self):
        with self.assertRaisesRegex(analyzer.EvidenceError, "duplicate"):
            analyzer.decode_json(b'{"a":1,"a":2}')
        with self.assertRaisesRegex(analyzer.EvidenceError, "non-finite"):
            analyzer.decode_json(b'{"a":NaN}')
        with self.assertRaisesRegex(analyzer.EvidenceError, "non-finite"):
            analyzer.decode_json(b'{"a":1e999}')

    def test_execution_shapes_are_fixed_independently_of_frozen_plan_values(self):
        native_plan = {
            "eligible_counts": analyzer.native.counts(512),
            "efs": [128, 256, 512, 1024, 2048], "overlap_ef": 512,
            "overlap_eligible": 65, "reader_concurrency": 4, "writer_calls": 8,
            "rss_controls": analyzer.native.RSS_CONTROLS,
            "rss_calibration_queries": analyzer.native.RSS_CALIBRATION_QUERIES,
            "rss_revalidation_queries": analyzer.native.RSS_REVALIDATION_QUERIES,
            "rss_recall_target": analyzer.native.RSS_RECALL_TARGET,
            "ef_construction": 32, "construction_decisions": False,
            "construction_calibration_contract":
                analyzer.native.construction_calibration_contract(32),
        }
        self.assertTrue(analyzer._native_diagnostic_shape_valid(native_plan, 512))
        for field, value in (("efs", [128]), ("reader_concurrency", 1),
                             ("writer_calls", 1), ("overlap_eligible", 64)):
            changed = copy.deepcopy(native_plan)
            changed[field] = value
            with self.subTest(native=field):
                self.assertFalse(analyzer._native_diagnostic_shape_valid(changed, 512))

        qdrant_plan = {
            "rows": 500000, "queries": 200, "dimensions": 768,
            "top_k": 10, "batch_size": 256, "controls": analyzer.native.RSS_CONTROLS,
            "dataset_manifest_sha256": "a" * 64,
            "dataset_files_sha256": {
                "documents": "b" * 64, "queries": "c" * 64, "truth": "d" * 64,
            },
            "cpu_affinity": list(range(6)), "platform": "test-linux",
            "comparison_contract": {
                "rows": 500000, "dimensions": 768, "top_k": 10, "batch_size": 256,
                "dataset_manifest_sha256": "a" * 64,
                "dataset_files_sha256": {
                    "documents": "b" * 64, "queries": "c" * 64, "truth": "d" * 64,
                },
                "cpu_affinity": list(range(6)), "platform": "test-linux",
                "ann_controls": {
                    "treedb_ef_search": analyzer.native.RSS_CONTROLS,
                    "qdrant_hnsw_ef": analyzer.native.RSS_CONTROLS,
                },
            },
            "production_hnsw": analyzer.qdrant.existing.PRODUCTION_HNSW_CONFIG,
            "production_optimizers": analyzer.qdrant.existing.PRODUCTION_OPTIMIZERS_CONFIG,
            "initial_upload_hnsw": analyzer.qdrant.existing.INITIAL_UPLOAD_HNSW_CONFIG,
            "initial_upload_optimizers": analyzer.qdrant.existing.INITIAL_UPLOAD_OPTIMIZERS_CONFIG,
        }
        self.assertTrue(analyzer._qdrant_diagnostic_shape_valid(qdrant_plan))
        for field, value in (("batch_size", 1000), ("cpu_affinity", list(range(1, 6))),
                             ("initial_upload_hnsw", {"m": 16})):
            changed = copy.deepcopy(qdrant_plan)
            changed[field] = value
            with self.subTest(qdrant=field):
                self.assertFalse(analyzer._qdrant_diagnostic_shape_valid(changed))

    def test_jsonl_is_streamed_with_line_and_total_caps(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.jsonl"
            path.write_bytes(b'{"event":"one"}\n{"event":"two"}\n')
            self.assertEqual([row["event"] for row in analyzer.read_jsonl(path)], ["one", "two"])
            with mock.patch.object(analyzer, "MAX_JSONL_LINE_BYTES", 8):
                with self.assertRaisesRegex(analyzer.EvidenceError, "line 1"):
                    list(analyzer.read_jsonl(path))
            with mock.patch.object(analyzer, "MAX_JSONL_BYTES", 20):
                with self.assertRaisesRegex(analyzer.EvidenceError, "total byte"):
                    list(analyzer.read_jsonl(path))
            path.write_bytes(b'{"payload":"' + b"x" * 100 + b'"}')
            with mock.patch.object(analyzer, "MAX_JSONL_LINE_BYTES", 16):
                with self.assertRaisesRegex(analyzer.EvidenceError, "line 1"):
                    list(analyzer.read_jsonl(path))

    def test_json_and_packet_reads_are_bounded_before_decode(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "oversized.json"
            path.write_bytes(b"{" + b" " * 100 + b"}")
            with self.assertRaisesRegex(analyzer.EvidenceError, "exceeds 8 bytes"):
                analyzer.read_json(path, "oversized", 8)
            with mock.patch.object(analyzer, "MAX_PACKET_BYTES", 8), \
                    self.assertRaisesRegex(analyzer.EvidenceError, "exceeds 1 MiB"):
                analyzer.load_packet(path, "0" * 64)

    def test_go_inputs_require_reproducible_candidate_build_metadata(self):
        output = "\n".join((
            "/packet/service: go1.26.0",
            "\tpath\tgithub.com/snissn/gomap/cmd/treedb-document-service",
            "\tmod\tgithub.com/snissn/gomap\t(devel)",
            "\tbuild\t-trimpath=true",
            "\tbuild\tvcs=git",
            f"\tbuild\tvcs.revision={COMMIT}",
            "\tbuild\tvcs.modified=false",
        ))
        completed = mock.Mock(returncode=0, stdout=output)
        with mock.patch.object(analyzer.subprocess, "run", return_value=completed):
            build = analyzer._go_binary_build(
                Path("/packet/service"), COMMIT,
                "github.com/snissn/gomap/cmd/treedb-document-service",
            )
        self.assertEqual(build["go_version"], "go1.26.0")
        self.assertEqual(build["build_settings"]["vcs.revision"], COMMIT)
        changed = output.replace("vcs.modified=false", "vcs.modified=true")
        with mock.patch.object(
                analyzer.subprocess, "run", return_value=mock.Mock(returncode=0, stdout=changed)), \
                self.assertRaisesRegex(analyzer.EvidenceError, "reproducible candidate"):
            analyzer._go_binary_build(
                Path("/packet/service"), COMMIT,
                "github.com/snissn/gomap/cmd/treedb-document-service",
            )

    def test_consumer_source_requires_clean_candidate_import_blobs(self):
        analyzer_raw = Path(analyzer.__file__).read_bytes()

        def clean_run(argv, **_kwargs):
            if argv[1] == "show":
                return mock.Mock(returncode=0, stdout=analyzer_raw)
            if argv[1] == "rev-parse":
                return mock.Mock(returncode=0, stdout="b" * 40 + "\n")
            return mock.Mock(returncode=0, stdout="")

        with mock.patch.object(analyzer.sys, "modules", {}), \
                mock.patch.object(analyzer.subprocess, "run", side_effect=clean_run):
            identity = analyzer.validate_consumer_source(COMMIT)
        self.assertEqual(identity["candidate_commit"], COMMIT)
        self.assertEqual(identity["analyzer_sha256"], hashlib.sha256(analyzer_raw).hexdigest())

        def dirty_run(argv, **_kwargs):
            if argv[1] == "status":
                return mock.Mock(returncode=0, stdout=" M analyzer.py\n")
            return mock.Mock(returncode=0, stdout="")

        with mock.patch.object(analyzer.subprocess, "run", side_effect=dirty_run), \
                self.assertRaisesRegex(analyzer.EvidenceError, "dirty"):
            analyzer.validate_consumer_source(COMMIT)

    def test_inventory_has_one_path_and_hash_per_semantic_role(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            arms = {role: "arm_" + role for role in analyzer.ARM_KINDS}
            plans = {role: "plan_" + role for role in analyzer.PLAN_KINDS}
            dataset = {role: "dataset_" + role for role in analyzer.DATASET_KINDS}
            inputs = {role: "input_" + role for role in analyzer.INPUT_KINDS}
            logical = [*arms.values(), *plans.values(), *dataset.values(), *inputs.values()]
            files = {}
            for number, name in enumerate(logical):
                raw = f"{number}:{name}".encode()
                (root / name).write_bytes(raw)
                files[name] = {"path": name, "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}
            packet = {"files": files, "arms": arms, "plans": plans,
                      "dataset": dataset, "inputs": inputs}
            resolved = analyzer.resolve_inventory(root / "packet.json", packet)
            self.assertEqual(set(resolved), set(logical))

            changed = copy.deepcopy(packet)
            second = logical[1]
            changed["files"][second]["path"] = changed["files"][logical[0]]["path"]
            with self.assertRaisesRegex(analyzer.EvidenceError, "same path"):
                analyzer.resolve_inventory(root / "packet.json", changed)
            changed = copy.deepcopy(packet)
            changed["files"][second]["sha256"] = changed["files"][logical[0]]["sha256"]
            with self.assertRaisesRegex(analyzer.EvidenceError, "unique"):
                analyzer.resolve_inventory(root / "packet.json", changed)
            fifo = root / logical[0]
            fifo.unlink()
            os.mkfifo(fifo)
            with self.assertRaisesRegex(analyzer.EvidenceError, "regular file"):
                analyzer.resolve_inventory(root / "packet.json", packet)

    def test_go_validator_keeps_exact_native_and_sq8_plan_separate(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            exact = root / "exact.json"
            sq8 = root / "sq8.json"
            plan = root / "plan.json"
            binary = root / "validator"
            completed = {
                "backends": [{"name": "treedb", "operations": {"manifest_ordered": True}}],
                "failures": [], "raw_evidence": {"treedb": {"final_scroll_state": {"match": True}}},
            }
            exact.write_bytes(canonical({
                **completed, "native_path_proof": {"strategy": "native_runtime"},
            }))
            sq8.write_bytes(canonical({
                **completed,
                "schema": "treedb_rag_application/minima_quantized_diagnostic_v1",
                "backends": [{"name": "treedb", "operations": {"manifest_ordered": True},
                              "configuration": {"vector_strategy": "column_graph"}}],
            }))
            plan.write_bytes(b"plan")
            binary.write_bytes(b"binary")
            packet = {
                "candidate_commit": COMMIT,
                "inputs": {
                    "bounded_validator_binary": "validator",
                    "bounded_sq8_plan": "plan",
                },
                "arms": {"bounded_exact": "exact", "bounded_sq8": "sq8"},
                "files": {"plan": {"sha256": hashlib.sha256(b"plan").hexdigest()}},
            }
            paths = {"validator": binary, "plan": plan, "exact": exact, "sq8": sq8}
            calls = []
            analyzer.validate_bounded_artifacts(packet, paths, calls.append)
            self.assertEqual(len(calls), 2)
            self.assertNotIn("-minima-quantized-plan", calls[0])
            self.assertIn("-minima-quantized-plan", calls[1])
            self.assertEqual(calls[0][-1], COMMIT)
            self.assertEqual(calls[1][-1], COMMIT)

            changed = json.loads(exact.read_text())
            changed["native_path_proof"]["strategy"] = "column_graph"
            exact.write_bytes(canonical(changed))
            with self.assertRaisesRegex(analyzer.EvidenceError, "native_runtime"):
                analyzer.validate_bounded_artifacts(packet, paths, calls.append)

    def test_receipts_bind_dataset_outputs_and_typed_unqualified_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            path = root / "receipts.json"
            arms = {role: "arm_" + role for role in analyzer.ARM_KINDS}
            plans = {role: "plan_" + role for role in analyzer.PLAN_KINDS}
            dataset = {role: "dataset_" + role for role in analyzer.DATASET_KINDS}
            inputs = {role: "input_" + role for role in analyzer.INPUT_KINDS}
            logical = [*arms.values(), *plans.values(), *dataset.values(), *inputs.values()]
            files = {name: {"sha256": f"{number + 10:064x}"}
                     for number, name in enumerate(logical)}
            dataset_dir = root / "dataset"
            paths = {
                dataset["manifest"]: dataset_dir / "manifest.json",
                dataset["documents"]: dataset_dir / "documents.f32",
                dataset["queries"]: dataset_dir / "queries.f32",
                dataset["truth"]: dataset_dir / "truth.json",
                inputs["bounded_validator_binary"]: root / "bin" / "validator",
                inputs["bounded_sq8_plan"]: root / "inputs" / "bounded-plan.json",
                inputs["treedb_service_binary"]: root / "bin" / "service",
                inputs["serving"]: root / "inputs" / "serving.json",
                inputs["qdrant_binary"]: root / "bin" / "qdrant",
            }
            native_roles = analyzer.PLAN_KINDS - {"qdrant_fp32_rss"}
            for role in native_roles:
                filename = ("rss.json" if role in {"treedb_fp32_rss", "treedb_sq8_rss"}
                            else "events.jsonl")
                paths[arms[role]] = root / "runs" / role / filename
            qdrant_run = root / "runs" / "qdrant_fp32_rss"
            paths.update({
                arms["qdrant_fp32_rss"]: qdrant_run / "qdrant-rss.json",
                arms["fp32_comparison"]: qdrant_run / "comparison.json",
                arms["three_arm_comparison"]: qdrant_run / "three-arm-comparison.json",
                arms["bounded_exact"]: root / "runs" / "bounded-exact.json",
                arms["bounded_sq8"]: root / "runs" / "bounded-sq8.json",
                arms["command_receipts"]: path,
            })
            environment = {name: "" for name in analyzer.REQUIRED_RECEIPT_ENV}
            environment.update(
                HOME="/home/test", PATH="/usr/bin", TMPDIR="/tmp/q5", TZ="UTC",
                LANG="C.UTF-8", LC_ALL="C.UTF-8", GOWORK="off", GOMAXPROCS="6",
                PYTHONHASHSEED="0", PYTHONDONTWRITEBYTECODE="1",
                OPENBLAS_NUM_THREADS="1", OMP_NUM_THREADS="1",
            )
            plan_paths = {role: root / "plans" / (plans[role] + ".json")
                          for role in analyzer.PLAN_KINDS}
            for role, plan_path in plan_paths.items():
                plan_path.parent.mkdir(parents=True, exist_ok=True)
                paths[plans[role]] = plan_path
            dataset_hashes = {
                role: files[dataset[role]]["sha256"]
                for role in ("documents", "queries", "truth")
            }
            runtime = {key: environment[key]
                       for key in ("GOMAXPROCS", "GOGC", "GOMEMLIMIT")}
            for role, plan_path in plan_paths.items():
                if role == "qdrant_fp32_rss":
                    plan = {
                        "dataset": str(dataset_dir),
                        "dataset_manifest_sha256": files[dataset["manifest"]]["sha256"],
                        "dataset_files_sha256": dataset_hashes,
                        "treedb_artifact": str(paths[arms["treedb_fp32_rss"]]),
                        "treedb_artifact_sha256": files[arms["treedb_fp32_rss"]]["sha256"],
                        "treedb_sq8_artifact": str(paths[arms["treedb_sq8_rss"]]),
                        "treedb_sq8_artifact_sha256": files[arms["treedb_sq8_rss"]]["sha256"],
                        "qdrant_bin": str(paths[inputs["qdrant_binary"]]),
                        "qdrant_bin_sha256": files[inputs["qdrant_binary"]]["sha256"],
                        "storage_path": str(qdrant_run / "storage"),
                        "url": "http://127.0.0.1:18140", "collection": "minima_q5",
                        "run_dir": str(qdrant_run), "operation_timeout_s": 600,
                        "startup_timeout_s": 120.0, "optimizer_timeout_s": 2700.0,
                        "poll_interval_s": .25,
                        "rows": 500000, "dimensions": 768, "top_k": 10,
                        "batch_size": 256, "controls": analyzer.native.RSS_CONTROLS,
                        "cpu_affinity": list(range(6)), "platform": "test-linux",
                        "comparison_contract": {
                            "treedb_go_runtime": runtime,
                            "rows": 500000, "dimensions": 768, "top_k": 10,
                            "batch_size": 256,
                            "dataset_manifest_sha256": files[dataset["manifest"]]["sha256"],
                            "dataset_files_sha256": dataset_hashes,
                            "cpu_affinity": list(range(6)), "platform": "test-linux",
                            "ann_controls": {
                                "treedb_ef_search": analyzer.native.RSS_CONTROLS,
                                "qdrant_hnsw_ef": analyzer.native.RSS_CONTROLS,
                            },
                        },
                    }
                else:
                    quantized = role in {
                        "smoke_sq8", "treedb_sq8_rss", "full_sq8_events",
                    }
                    rss_only = role in {"treedb_fp32_rss", "treedb_sq8_rss"}
                    plan = {
                        "dataset": str(dataset_dir),
                        "dataset_manifest_sha256": files[dataset["manifest"]]["sha256"],
                        "dataset_files_sha256": dataset_hashes,
                        "service_bin": str(paths[inputs["treedb_service_binary"]]),
                        "service_sha256": files[inputs["treedb_service_binary"]]["sha256"],
                        "product_commit": COMMIT,
                        "serving_path": str(paths[inputs["serving"]]),
                        "serving_sha256": files[inputs["serving"]]["sha256"],
                        "run_dir": str(paths[arms[role]].parent),
                        "rows": 512 if role.startswith("smoke_") else 500000,
                        "rss_only": rss_only,
                        "ef_construction": analyzer.native.DEFAULT_EF_CONSTRUCTION,
                        "construction_decisions": False,
                        "url": f"http://127.0.0.1:{18020 + len(role)}",
                        "native_address": f"127.0.0.1:{18120 + len(role)}",
                        "diagnostics_url": f"http://127.0.0.1:{18220 + len(role)}",
                        "treedb_go_runtime": runtime,
                        "cpu_affinity": list(range(6)),
                    }
                    if quantized:
                        plan.update(
                            query_mode="quantized_rerank",
                            quantized_index_name=analyzer.native.QUANTIZED_PROFILE_NAME,
                        )
                    if role == "full_sq8_events":
                        plan["all_rows_coordinate_lock"] = {
                            "artifact_path": str(paths[arms["treedb_sq8_rss"]]),
                            "artifact_sha256": files[arms["treedb_sq8_rss"]]["sha256"],
                        }
                plan["blas_threads"] = {
                    key: environment[key]
                    for key in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS")
                }
                plan_path.write_bytes(canonical(plan))

            def bounded_command(role):
                argv = [str(paths[inputs["bounded_validator_binary"]]), "-workload=minima",
                        "-validate-minima-artifact", str(paths[arms[role]])]
                if role == "bounded_sq8":
                    argv.extend([
                        "-minima-quantized-plan", str(paths[inputs["bounded_sq8_plan"]]),
                        "-minima-expected-quantized-plan-sha256",
                        files[inputs["bounded_sq8_plan"]]["sha256"],
                    ])
                return [*argv, "-minima-expected-commit", COMMIT]

            def plan_command(role, mode):
                plan = json.loads(plan_paths[role].read_text())
                script = analyzer.qdrant.__file__ if role == "qdrant_fp32_rss" \
                    else analyzer.native.__file__
                argv = [
                    "/usr/bin/taskset", "-c", "0-5",
                    str(Path(analyzer.sys.executable).resolve()), str(Path(script).resolve()),
                ]
                if role == "qdrant_fp32_rss":
                    options = (
                        ("--dataset", plan["dataset"]),
                        ("--treedb-artifact", plan["treedb_artifact"]),
                        ("--treedb-sq8-artifact", plan["treedb_sq8_artifact"]),
                        ("--qdrant-bin", plan["qdrant_bin"]),
                        ("--storage-path", plan["storage_path"]), ("--url", plan["url"]),
                        ("--collection", plan["collection"]), ("--run-dir", plan["run_dir"]),
                        ("--operation-timeout", plan["operation_timeout_s"]),
                        ("--startup-timeout", plan["startup_timeout_s"]),
                        ("--optimizer-timeout", plan["optimizer_timeout_s"]),
                        ("--poll-interval", plan["poll_interval_s"]),
                    )
                else:
                    options = [
                        ("--dataset", plan["dataset"]), ("--service-bin", plan["service_bin"]),
                        ("--product-commit", COMMIT), ("--serving", plan["serving_path"]),
                        ("--run-dir", plan["run_dir"]), ("--rows", plan["rows"]),
                        ("--query-mode", plan.get("query_mode", "exact")),
                        ("--ef-construction", plan["ef_construction"]),
                        ("--url", plan["url"]), ("--native-address", plan["native_address"]),
                        ("--diagnostics-url", plan["diagnostics_url"]),
                    ]
                    if plan["rss_only"]:
                        argv.append("--rss-only")
                    if plan.get("query_mode") == "quantized_rerank":
                        options.append(("--quantized-index-name", plan["quantized_index_name"]))
                    if role == "full_sq8_events":
                        lock = plan["all_rows_coordinate_lock"]
                        options.extend([
                            ("--all-rows-sq8-rss-artifact", lock["artifact_path"]),
                            ("--expected-all-rows-sq8-rss-artifact-sha256",
                             lock["artifact_sha256"]),
                        ])
                for option, value in options:
                    argv.extend([option, str(value)])
                argv.extend([f"--{mode}", str(plan_paths[role])])
                if mode == "run":
                    argv.extend([
                        "--expected-plan-sha256", files[plans[role]]["sha256"],
                    ])
                return argv

            runs = {
                role: {"freeze_argv": (plan_command(role, "freeze")
                                        if role in analyzer.PLAN_KINDS else None),
                       "freeze_exit_code": 0 if role in analyzer.PLAN_KINDS else None,
                       "run_argv": (plan_command(role, "run")
                                    if role in analyzer.PLAN_KINDS else bounded_command(role)),
                       "cwd": str(root), "environment": environment, "umask": "0022",
                       "started_utc": "2026-09-15T00:00:00Z",
                       "ended_utc": "2026-09-15T00:01:00Z",
                       "exit_code": 1 if role == "full_sq8_events" else 0,
                       "plan_sha256": (files[plans[role]]["sha256"]
                                       if role in analyzer.PLAN_KINDS else None),
                       "output_sha256": files[arms[role]]["sha256"]}
                for role in analyzer.RUN_ARM_KINDS
            }
            receipts = {
                "schema": analyzer.RECEIPT_SCHEMA, "candidate_commit": COMMIT,
                "dataset_manifest_sha256": files[dataset["manifest"]]["sha256"],
                "dataset_files_sha256": dataset_hashes,
                "runs": runs,
            }
            path.write_bytes(canonical(receipts))
            packet = {"candidate_commit": COMMIT, "declared_quality_outcome": "miss",
                      "arms": arms, "plans": plans, "dataset": dataset,
                      "inputs": inputs, "files": files}
            analyzer.validate_receipts(
                packet, paths, receipts["dataset_manifest_sha256"], dataset_hashes,
            )
            for mutation in (
                    "exit", "environment", "environment_extra", "environment_value",
                    "environment_path", "plan", "argv", "matching_extra",
                    "missing_input", "duplicate", "affinity"):
                changed = copy.deepcopy(receipts)
                if mutation == "exit":
                    changed["runs"]["full_sq8_events"]["exit_code"] = 0
                elif mutation == "environment":
                    del changed["runs"]["smoke_exact"]["environment"]["TMPDIR"]
                elif mutation == "environment_extra":
                    changed["runs"]["smoke_exact"]["environment"]["UNCONTROLLED"] = "1"
                elif mutation == "environment_value":
                    changed["runs"]["smoke_exact"]["environment"]["PYTHONHASHSEED"] = "random"
                elif mutation == "environment_path":
                    changed["runs"]["smoke_exact"]["environment"]["PATH"] = "bin:/usr/bin"
                elif mutation == "argv":
                    changed["runs"]["bounded_exact"]["run_argv"].append("--unexpected")
                elif mutation == "matching_extra":
                    changed["runs"]["smoke_exact"]["freeze_argv"].extend([
                        "--unexpected", "same",
                    ])
                    changed["runs"]["smoke_exact"]["run_argv"].extend([
                        "--unexpected", "same",
                    ])
                elif mutation == "missing_input":
                    for command in ("freeze_argv", "run_argv"):
                        argv = changed["runs"]["smoke_exact"][command]
                        index = argv.index("--dataset")
                        del argv[index:index + 2]
                elif mutation == "duplicate":
                    changed["runs"]["smoke_exact"]["freeze_argv"].extend([
                        "--rows", "512",
                    ])
                    changed["runs"]["smoke_exact"]["run_argv"].extend([
                        "--rows", "512",
                    ])
                elif mutation == "affinity":
                    changed["runs"]["smoke_exact"]["freeze_argv"][2] = "1-5"
                    changed["runs"]["smoke_exact"]["run_argv"][2] = "1-5"
                else:
                    changed["runs"]["smoke_exact"]["plan_sha256"] = "f" * 64
                path.write_bytes(canonical(changed))
                with self.subTest(mutation=mutation), self.assertRaises(analyzer.EvidenceError):
                    analyzer.validate_receipts(
                        packet, paths, receipts["dataset_manifest_sha256"], dataset_hashes,
                    )

            qdrant_plan = json.loads(plan_paths["qdrant_fp32_rss"].read_text())
            changed_plan = copy.deepcopy(qdrant_plan)
            changed_plan["cpu_affinity"] = list(range(1, 6))
            plan_paths["qdrant_fp32_rss"].write_bytes(canonical(changed_plan))
            changed = copy.deepcopy(receipts)
            for command in ("freeze_argv", "run_argv"):
                changed["runs"]["qdrant_fp32_rss"][command][2] = "1-5"
            path.write_bytes(canonical(changed))
            with self.assertRaisesRegex(analyzer.EvidenceError, "comparison"):
                analyzer.validate_receipts(
                    packet, paths, receipts["dataset_manifest_sha256"], dataset_hashes,
                )
            plan_paths["qdrant_fp32_rss"].write_bytes(canonical(qdrant_plan))

            outside = root / "outside" / "service-copy"
            outside.parent.mkdir()
            paths[inputs["treedb_service_binary"]].parent.mkdir(parents=True, exist_ok=True)
            paths[inputs["treedb_service_binary"]].write_bytes(b"same binary")
            outside.write_bytes(b"same binary")
            smoke_plan = json.loads(plan_paths["smoke_exact"].read_text())
            smoke_plan["service_bin"] = str(outside)
            plan_paths["smoke_exact"].write_bytes(canonical(smoke_plan))
            changed = copy.deepcopy(receipts)
            for command in ("freeze_argv", "run_argv"):
                argv = changed["runs"]["smoke_exact"][command]
                argv[argv.index("--service-bin") + 1] = str(outside)
            path.write_bytes(canonical(changed))
            with self.assertRaisesRegex(analyzer.EvidenceError, "packet inventory"):
                analyzer.validate_receipts(
                    packet, paths, receipts["dataset_manifest_sha256"], dataset_hashes,
                )

    def _full_events(self, include_retune=False, quality_pass=True):
        truth = {"500000": [
            [f"row-{query * 10 + rank:06d}" for rank in range(10)]
            for query in range(200)
        ]}
        plan = {
            "schema": analyzer.native.SCHEMA, "qualification": "not_evaluated",
            "mode": "diagnostic", "rows": 500000, "dimensions": 768, "queries": 200,
            "top_k": 10, "batch_size": 256, "query_mode": "quantized_rerank",
            "rss_only": False, "product_commit": COMMIT,
            "harness_commit": COMMIT, "dataset_manifest_sha256": "0" * 64,
            "dataset_files_sha256": {
                "documents": "1" * 64, "queries": "2" * 64, "truth": "3" * 64,
            },
            "eligible_counts": analyzer.FULL_ELIGIBLE_COUNTS,
            "rss_controls": analyzer.native.RSS_CONTROLS,
            "rss_calibration_queries": list(range(100)),
            "rss_revalidation_queries": list(range(100, 200)),
            "rss_recall_target": .9,
            "ef_construction": 32, "construction_decisions": False,
            "construction_calibration_contract":
                analyzer.native.construction_calibration_contract(32),
            "quantized_index_name": analyzer.native.QUANTIZED_PROFILE_NAME,
            "quantized_coordinate_policy": "ordered_ef_grid_with_requested_rerank_candidates_equal_ef",
            "representation_arm": analyzer.native.quantized_representation_arm(),
            "paired_timing": analyzer.native.paired_timing_plan(200),
            "efs": [128, 256, 512, 1024, 2048], "overlap_ef": 512,
            "overlap_eligible": 4097, "reader_concurrency": 4, "writer_calls": 8,
            "wall_limit_s": 2700, "minimum_free_bytes": 10 * analyzer.native.GIB,
            "maximum_output_bytes": 11 * analyzer.native.GIB,
            "maximum_combined_rss_bytes": 24 * analyzer.native.GIB,
        }
        coordinate = {"ef_search": 32, "rerank_candidates": 32}
        owner = {
            "schema_hash": 1, "schema_generation": 2,
            "base_manifest": {"generation": 3, "format": "tcs1", "version": 1,
                              "checksum": 4},
            "current_manifest": {"generation": 3, "format": "tcs1", "version": 1,
                                 "checksum": 4},
            "base_coverage_lsn": 5, "current_coverage_lsn": 5,
        }
        plan["all_rows_coordinate_lock"] = {
            "schema": "treedb_cohere_sq8_coordinate_lock/v1",
            "artifact_path": "/frozen/sq8.json", "artifact_sha256": "4" * 64,
            "artifact_schema": analyzer.native.QUANTIZED_RSS_ARTIFACT_SCHEMA,
            "source_process_identity": "10:20", "selected_coordinate": coordinate,
        }
        events = [{"event": "plan", "plan": plan}, {
            "event": "resource", "elapsed_s": 1.0,
            "free_bytes": 20 * analyzer.native.GIB, "output_bytes": 1,
            "combined_rss_bytes": 1,
        }]
        self._append_construction_calls(events, 500000)
        events.append({
            "event": "coordinate_lock_consumed", "source_artifact_sha256": "4" * 64,
            "source_process_identity": "10:20", "fresh_process_identity": "11:21",
            "coordinate": coordinate,
        })
        def append_search(event):
            start = len(events) * 10 + 1
            events.extend([{
                "event": "call", "phase": event["phase"], "start_ns": start,
                "end_ns": start + 5, "duration_ns": 5, "outcome": "completed",
                "eligible": event["eligible"], "ef": event["ef"],
                "query": event["query"], "writer_active": False,
                "request_mode": "quantized_rerank",
            }, event])
        for query in range(200):
            ids = (truth["500000"][query] if quality_pass else
                   [f"row-{400000 + rank:06d}" for rank in range(10)])
            append_search({
                "event": "search_result", "phase": "locked_all_rows_revalidation",
                "eligible": 500000, "ef": 32, "rerank_candidates": 32, "query": query,
                "ids": ids, "recall": 1.0 if quality_pass else 0.0,
                "ndcg_at_10": 1.0 if quality_pass else 0.0,
                "score_plane": {"snapshot": copy.deepcopy(owner)},
            })
        if include_retune:
            events.append({
                "event": "search_result", "phase": "quality_selection", "eligible": 500000,
                "ef": 64, "rerank_candidates": 64, "query": 0,
                "ids": truth["500000"][0], "recall": 1.0, "ndcg_at_10": 1.0,
            })
        metric = 1.0 if quality_pass else 0.0
        fixed = lambda queries: {
            "queries": queries, "mean_recall_at_10": metric, "mean_ndcg_at_10": metric,
            "per_query_recall": [metric] * 100, "per_query_ndcg_at_10": [metric] * 100,
        }
        quality = {
            "schema": "treedb_cohere_sq8_locked_all_rows_confirmation/v1",
            "selection_protocol": "prior_sq8_rss_coordinate_revalidated_on_fresh_graph/v1",
            "coordinate": coordinate,
            "fixed_sets": {"queries_0_99": fixed(list(range(100))),
                           "queries_100_199": fixed(list(range(100, 200)))},
            "target_mean_recall_at_10": .9, "passed": quality_pass,
        }
        events.extend([
            {"event": "locked_all_rows_quality", **quality},
            {"event": "quantized_quality", "cohorts": {"500000": quality}},
        ])
        if quality_pass:
            events.extend([
                {"event": "paired_query_timing",
                 "artifact": {"selected_coordinate": copy.deepcopy(coordinate),
                              "warmup": {"common_owner": copy.deepcopy(owner)}}},
                {"event": "full_state_verified", "rows": 500000, "vectors_checked": 500000,
                 "normalized_full_vector_tolerance": 1e-6},
                {"event": "terminal", "lifecycle_complete": True,
                 "qualification": "producer_gates_passed", "error": None,
                 "process_lifetimes": [{"linux_process_identity": "11:21"}],
                 "final_disk_bytes": 1},
            ])
        else:
            error = "QualityUnqualified: quantized fixed-set quality failed for cohorts 500000"
            events.extend([
                {"event": "failure", "error": error},
                {"event": "terminal", "lifecycle_complete": False,
                 "qualification": "valid_unqualified", "error": error,
                 "process_lifetimes": [{"linux_process_identity": "11:21"}],
                 "final_disk_bytes": 1},
            ])
        return events, truth

    def _smoke_sq8_events(self):
        truth = {}
        for eligible in analyzer.native.counts(512):
            ids = []
            for ordinal in range(512):
                if (ordinal * 7919) % 512 < eligible:
                    ids.append(f"row-{ordinal:06d}")
                if len(ids) == 10:
                    break
            truth[str(eligible)] = [list(ids) for _ in range(4)]
        plan = {
            "schema": analyzer.native.SCHEMA, "qualification": "not_evaluated",
            "mode": "smoke", "rows": 512, "dimensions": 768, "queries": 4,
            "top_k": 10, "batch_size": 256, "query_mode": "quantized_rerank",
            "rss_only": False, "product_commit": COMMIT, "harness_commit": COMMIT,
            "dataset_manifest_sha256": "0" * 64,
            "dataset_files_sha256": {
                "documents": "1" * 64, "queries": "2" * 64, "truth": "3" * 64,
            },
            "eligible_counts": analyzer.native.counts(512),
            "rss_controls": analyzer.native.RSS_CONTROLS,
            "rss_calibration_queries": analyzer.native.RSS_CALIBRATION_QUERIES,
            "rss_revalidation_queries": analyzer.native.RSS_REVALIDATION_QUERIES,
            "rss_recall_target": analyzer.native.RSS_RECALL_TARGET,
            "ef_construction": 32, "construction_decisions": False,
            "construction_calibration_contract":
                analyzer.native.construction_calibration_contract(32),
            "paired_timing": analyzer.native.paired_timing_plan(4),
            "efs": [128, 256, 512, 1024, 2048], "overlap_ef": 512,
            "reader_concurrency": 4, "writer_calls": 8,
            "overlap_eligible": 65, "wall_limit_s": 2700,
            "minimum_free_bytes": 10 * analyzer.native.GIB,
            "maximum_output_bytes": 11 * analyzer.native.GIB,
            "maximum_combined_rss_bytes": 24 * analyzer.native.GIB,
        }
        owner = {
            "schema_hash": 1, "schema_generation": 2,
            "base_manifest": {"generation": 3, "format": "tcs1", "version": 1,
                              "checksum": 4},
            "current_manifest": {"generation": 3, "format": "tcs1", "version": 1,
                                 "checksum": 4},
            "base_coverage_lsn": 5, "current_coverage_lsn": 5,
        }
        events = [{"event": "plan", "plan": plan}, {
            "event": "resource", "elapsed_s": 1.0,
            "free_bytes": 20 * analyzer.native.GIB, "output_bytes": 1,
            "combined_rss_bytes": 1,
        }]
        self._append_construction_calls(events, 512)

        def append_search(phase, eligible, query, *, mode="quantized_rerank",
                          writer_active=False, ids=None):
            ids = truth[str(eligible)][query] if ids is None and eligible else (ids or [])
            start = len(events) * 10 + 1
            events.extend([{
                "event": "call", "phase": phase, "start_ns": start,
                "end_ns": start + 5, "duration_ns": 5, "outcome": "completed",
                "eligible": eligible, "ef": 32, "query": query,
                "writer_active": writer_active, "request_mode": mode,
            }, {
                "event": "search_result", "phase": phase, "eligible": eligible,
                "ef": 32, "query": query, "ids": ids, "recall": 1.0,
                "ndcg_at_10": 1.0, "writer_active": writer_active,
                "dense_work": {},
                **({"rerank_candidates": 32,
                    "score_plane": {"snapshot": copy.deepcopy(owner)}}
                   if mode == "quantized_rerank" else {}),
            }])

        def append_call(phase, **fields):
            start = len(events) * 10 + 1
            events.append({
                "event": "call", "phase": phase, "start_ns": start,
                "end_ns": start + 5, "duration_ns": 5, "outcome": "completed",
                **fields,
            })

        for eligible in [512, 64, 65, 128, 256]:
            for query in range(4):
                append_search(
                    "predicate_first_quality_query" if query == 0 else "quality_selection",
                    eligible, query,
                )
        cohorts = analyzer.native.select_quantized_cohorts(
            lambda eligible, _control, query: truth[str(eligible)][query],
            truth, plan["eligible_counts"], 512, analyzer.native.RSS_CONTROLS,
            [0, 1], [2, 3], 1.0,
        )
        events.append({"event": "quantized_quality", "cohorts": cohorts})

        batches = []
        sequence = 1
        for kind, repetition in [("warmup", -1), *[("measured", value) for value in range(5)]]:
            order = analyzer.native.paired_batch_arm_order(kind, repetition)
            arms = {}
            for mode in order:
                phase = analyzer.native.paired_phase(kind, repetition, mode)
                records = []
                for query in range(4):
                    ids = truth["512"][query]
                    append_search(phase, 512, query, mode=mode, ids=ids)
                    record = {
                        "request_sequence": sequence, "phase": phase,
                        "eligible": 512, "query": query, "requested_ef_search": 32,
                        "results": [{"id": identifier} for identifier in ids],
                        "recall": 1.0, "ndcg_at_10": 1.0, "dense_work": {},
                    }
                    if mode == "quantized_rerank":
                        record.update(
                            requested_rerank_candidates=32,
                            score_plane={"snapshot": copy.deepcopy(owner)},
                        )
                    records.append(record)
                    sequence += 1
                arms[mode] = {"requests": records}
            batches.append({
                "batch_kind": kind, "repetition": repetition,
                "arm_order": order, "common_owner": copy.deepcopy(owner), "arms": arms,
            })
        paired = {"warmup": batches[0], "repetitions": batches[1:]}
        events.append({"event": "paired_query_timing", "artifact": paired})
        for eligible in plan["eligible_counts"]:
            for query in range(4):
                append_search("fixed_coordinate_curve", eligible, query)
        for worker in range(4):
            for number in range(64):
                append_search("overlap_search", 65, (number * 4 + worker) % 4)
        for number in range(8):
            append_call("overlap_replace", first_row=(512 - 256 * (number + 1)) % 512,
                        rows=256)
        append_call("explicit_update", first_row=0, rows=256)
        append_call("native_get_many")
        append_call("http_delete_file")
        append_call("native_deleted_visibility")
        append_call("reindex_replacement", first_row=0, rows=256)
        append_call("native_reinsert_visibility")
        append_search("empty_user", 0, 0, ids=[])
        for phase in ("pre_close_fold", "close", "reopen", "idempotent_ensure",
                      "reopen_graph_ensure"):
            append_call(phase)
        for eligible in plan["eligible_counts"]:
            for query in range(4):
                append_search("post_reopen_curve", eligible, query)
        events.append({
            "event": "full_state_verified", "rows": 512, "vectors_checked": 512,
            "normalized_full_vector_tolerance": 1e-6,
        })
        append_call("verification_only_full_scroll")
        events.append({
            "event": "terminal", "lifecycle_complete": True, "error": None,
            "process_lifetimes": [{}, {}], "final_disk_bytes": 1,
        })
        return events, truth

    def test_smoke_sq8_consumes_one_complete_producer_phase_ledger(self):
        packet = {
            "candidate_commit": COMMIT,
            "files": {
                "manifest": {"sha256": "0" * 64},
                "documents": {"sha256": "1" * 64},
                "queries": {"sha256": "2" * 64},
                "truth": {"sha256": "3" * 64},
            },
            "dataset": {"manifest": "manifest", "documents": "documents",
                        "queries": "queries", "truth": "truth"},
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "smoke.jsonl"
            events, truth = self._smoke_sq8_events()
            path.write_bytes(b"".join(canonical(event) for event in events))
            with mock.patch.object(analyzer.native, "validate_shutdowns"), \
                    mock.patch.object(analyzer.native, "paired_query_artifact_valid", return_value=True), \
                    mock.patch.object(analyzer, "_validate_quantized_search_path", return_value=True):
                analyzer.validate_smoke_log(path, "quantized_rerank", packet, truth)
                for label, predicate in (
                        ("quality", lambda event: event.get("event") == "quantized_quality"),
                        ("empty", lambda event: event.get("phase") == "empty_user"
                         and event.get("event") == "search_result"),
                        ("mutation", lambda event: event.get("phase") == "overlap_replace"
                         and event.get("event") == "call"),
                        ("fixed", lambda event: event.get("phase") == "fixed_coordinate_curve"
                         and event.get("event") == "search_result")):
                    changed = copy.deepcopy(events)
                    del changed[next(index for index, event in enumerate(changed) if predicate(event))]
                    path.write_bytes(b"".join(canonical(event) for event in changed))
                    with self.subTest(label=label), self.assertRaises(analyzer.EvidenceError):
                        analyzer.validate_smoke_log(path, "quantized_rerank", packet, truth)
                changed = copy.deepcopy(events)
                fixed = next(event for event in changed
                             if event.get("event") == "search_result"
                             and event.get("phase") == "fixed_coordinate_curve")
                fixed["score_plane"]["snapshot"]["schema_hash"] = 9
                path.write_bytes(b"".join(canonical(event) for event in changed))
                with self.assertRaisesRegex(analyzer.EvidenceError, "pre-mutation graph owner"):
                    analyzer.validate_smoke_log(path, "quantized_rerank", packet, truth)
                changed = copy.deepcopy(events)
                search_index = next(index for index, event in enumerate(changed)
                                    if event.get("event") == "search_result"
                                    and event.get("phase") == "fixed_coordinate_curve")
                changed[search_index + 1:search_index + 1] = copy.deepcopy(
                    changed[search_index - 1:search_index + 1]
                )
                path.write_bytes(b"".join(canonical(event) for event in changed))
                with self.assertRaises(analyzer.EvidenceError):
                    analyzer.validate_smoke_log(path, "quantized_rerank", packet, truth)
                for label, call in (
                        ("unknown", {
                            "event": "call", "phase": "unplanned_retry", "start_ns": 1,
                            "end_ns": 2, "duration_ns": 1, "outcome": "completed",
                        }),
                        ("duplicate-build", copy.deepcopy(next(
                            event for event in events
                            if event.get("phase") == "initial_graph_build"
                        )))):
                    changed = copy.deepcopy(events)
                    changed.insert(-1, call)
                    path.write_bytes(b"".join(canonical(event) for event in changed))
                    with self.subTest(label=label), self.assertRaises(analyzer.EvidenceError):
                        analyzer.validate_smoke_log(path, "quantized_rerank", packet, truth)

    def test_exact_smoke_requires_the_complete_search_and_lifecycle_ledger(self):
        _, truth = self._smoke_sq8_events()
        plan = {
            "rows": 512, "queries": 4, "eligible_counts": analyzer.native.counts(512),
            "efs": [128, 256, 512, 1024, 2048], "overlap_eligible": 65,
            "overlap_ef": 512, "reader_concurrency": 4, "writer_calls": 8,
        }
        events = []
        self._append_construction_calls(events, 512)

        def append_call(phase, **fields):
            start = len(events) * 10 + 1
            events.append({
                "event": "call", "phase": phase, "start_ns": start,
                "end_ns": start + 5, "duration_ns": 5, "outcome": "completed",
                **fields,
            })

        def append_search(phase, eligible, ef, query, writer_active=False):
            append_call(
                phase, eligible=eligible, ef=ef, query=query,
                writer_active=writer_active, request_mode="exact",
            )
            events.append({
                "event": "search_result", "phase": phase, "eligible": eligible,
                "ef": ef, "query": query, "ids": truth[str(eligible)][query],
                "recall": 1.0, "ndcg_at_10": 1.0,
                "writer_active": writer_active, "dense_work": {},
            })

        for eligible in plan["eligible_counts"]:
            append_search("predicate_first_query", eligible, 512, 0)
            for ef in plan["efs"]:
                for query in range(4):
                    append_search("warm_curve", eligible, ef, query)
        for worker in range(4):
            for number in range(64):
                append_search("overlap_search", 65, 512, (number * 4 + worker) % 4)
        for number in range(8):
            append_call("overlap_replace", first_row=(512 - 256 * (number + 1)) % 512,
                        rows=256)
        append_call("explicit_update", first_row=0, rows=256)
        for phase in ("native_get_many", "http_delete_file", "native_deleted_visibility"):
            append_call(phase)
        append_call("reindex_replacement", first_row=0, rows=256)
        append_call("native_reinsert_visibility")
        append_call("empty_user")
        for phase in ("pre_close_fold", "close", "reopen", "idempotent_ensure",
                      "reopen_graph_ensure"):
            append_call(phase)
        for eligible in plan["eligible_counts"]:
            for query in range(4):
                append_search("post_reopen_curve", eligible, 512, query)
        full_state_index = len(events)
        events.append({"event": "full_state_verified"})
        append_call("verification_only_full_scroll")
        events.append({"event": "terminal"})
        analyzer._validate_exact_smoke_searches(events, plan, truth, full_state_index)
        changed = copy.deepcopy(events)
        del changed[next(index for index, event in enumerate(changed)
                         if event.get("phase") == "empty_user")]
        with self.assertRaisesRegex(analyzer.EvidenceError, "empty_user"):
            analyzer._validate_exact_smoke_searches(changed, plan, truth, full_state_index - 1)

    def test_full_log_consumes_lock_once_without_all_row_retune(self):
        packet = {
            "candidate_commit": COMMIT,
            "arms": {"treedb_sq8_rss": "rss"},
            "files": {
                "dataset_manifest": {"sha256": "0" * 64},
                "dataset_documents": {"sha256": "1" * 64},
                "dataset_queries": {"sha256": "2" * 64},
                "dataset_truth": {"sha256": "3" * 64},
                "rss": {"sha256": "4" * 64},
            },
            "dataset": {"manifest": "dataset_manifest", "documents": "dataset_documents", "queries": "dataset_queries",
                        "truth": "dataset_truth"},
        }
        rss = {"quality": {"selected_coordinate": {"ef_search": 32, "rerank_candidates": 32}},
               "rss": {"process_identity": "10:20"}}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.jsonl"
            events, truth = self._full_events()
            path.write_bytes(b"".join(canonical(event) for event in events))
            with mock.patch.object(analyzer.native, "validate_shutdowns"), \
                    mock.patch.object(analyzer.native, "sq8_rss_artifact_reasons", return_value=[]), \
                    mock.patch.object(analyzer.native, "paired_query_artifact_valid", return_value=True), \
                    mock.patch.object(analyzer.native, "sq8_requests_share_initial_owner", return_value=True), \
                    mock.patch.object(analyzer, "_validate_quantized_search_path", return_value=True), \
                    mock.patch.object(analyzer, "_recompute_filtered_quality", return_value=({}, True)), \
                    mock.patch.object(analyzer, "paired_results_match_dataset", return_value=True), \
                    mock.patch.object(analyzer, "_paired_owner_matches_locked_calls", return_value=True), \
                    mock.patch.object(analyzer, "_validate_post_selection_searches", return_value=set()), \
                    mock.patch.object(analyzer, "paired_statistics", return_value={"ok": True}):
                passed, stats, state = analyzer.validate_full_sq8_log(
                    path, packet, truth, rss, None, None,
                )
            self.assertTrue(passed)
            self.assertEqual(stats, {"ok": True})
            self.assertEqual(state["rows"], 500000)

            with mock.patch.object(
                    analyzer.native, "sq8_rss_artifact_reasons",
                    return_value=["construction contract differs"]), \
                    self.assertRaisesRegex(analyzer.EvidenceError, "intrinsically valid"):
                analyzer.validate_full_sq8_log(path, packet, truth, rss, None, None)

            with mock.patch.object(analyzer.native, "validate_shutdowns"), \
                    mock.patch.object(analyzer.native, "sq8_rss_artifact_reasons", return_value=[]), \
                    mock.patch.object(analyzer.native, "paired_query_artifact_valid", return_value=True), \
                    mock.patch.object(analyzer.native, "sq8_requests_share_initial_owner", return_value=True), \
                    mock.patch.object(analyzer, "_validate_quantized_search_path", return_value=True), \
                    mock.patch.object(analyzer, "_recompute_filtered_quality", return_value=({}, True)), \
                    mock.patch.object(analyzer, "_paired_owner_matches_locked_calls", return_value=False), \
                    self.assertRaisesRegex(analyzer.EvidenceError, "locked all-row graph owner"):
                analyzer.validate_full_sq8_log(path, packet, truth, rss, None, None)

            changed = copy.deepcopy(events)
            next(event for event in changed
                 if event.get("event") == "paired_query_timing")["artifact"][
                     "selected_coordinate"] = {"ef_search": 64, "rerank_candidates": 64}
            path.write_bytes(b"".join(canonical(event) for event in changed))
            with mock.patch.object(analyzer.native, "validate_shutdowns"), \
                    mock.patch.object(analyzer.native, "sq8_rss_artifact_reasons", return_value=[]), \
                    mock.patch.object(analyzer.native, "paired_query_artifact_valid", return_value=True), \
                    mock.patch.object(analyzer.native, "sq8_requests_share_initial_owner", return_value=True), \
                    mock.patch.object(analyzer, "_validate_quantized_search_path", return_value=True), \
                    mock.patch.object(analyzer, "_recompute_filtered_quality", return_value=({}, True)), \
                    self.assertRaisesRegex(analyzer.EvidenceError, "locked all-row coordinate"):
                analyzer.validate_full_sq8_log(path, packet, truth, rss, None, None)

            events, truth = self._full_events(quality_pass=False)
            path.write_bytes(b"".join(canonical(event) for event in events))
            with mock.patch.object(analyzer.native, "validate_shutdowns"), \
                    mock.patch.object(analyzer.native, "sq8_rss_artifact_reasons", return_value=[]), \
                    mock.patch.object(analyzer.native, "sq8_requests_share_initial_owner", return_value=True), \
                    mock.patch.object(analyzer, "_validate_quantized_search_path", return_value=True), \
                    mock.patch.object(analyzer, "_recompute_filtered_quality", return_value=({}, True)):
                passed, stats, state = analyzer.validate_full_sq8_log(
                    path, packet, truth, rss, None, None,
                )
            self.assertFalse(passed)
            self.assertIsNone(stats)
            self.assertIsNone(state)
            for cohorts in ("", "4097", "500000,4097"):
                changed = copy.deepcopy(events)
                error = "QualityUnqualified: quantized fixed-set quality failed for cohorts " + cohorts
                next(event for event in changed if event.get("event") == "failure")["error"] = error
                changed[-1]["error"] = error
                path.write_bytes(b"".join(canonical(event) for event in changed))
                with mock.patch.object(analyzer.native, "validate_shutdowns"), \
                        mock.patch.object(analyzer.native, "sq8_rss_artifact_reasons", return_value=[]), \
                        mock.patch.object(analyzer.native, "sq8_requests_share_initial_owner", return_value=True), \
                        mock.patch.object(analyzer, "_validate_quantized_search_path", return_value=True), \
                        mock.patch.object(analyzer, "_recompute_filtered_quality", return_value=({}, True)), \
                        self.subTest(cohorts=cohorts), \
                        self.assertRaisesRegex(analyzer.EvidenceError, "terminal disposition"):
                    analyzer.validate_full_sq8_log(path, packet, truth, rss, None, None)

            changed = copy.deepcopy(events)
            changed.insert(-2, {
                "event": "call", "phase": "post_miss_retry", "start_ns": 1,
                "end_ns": 2, "duration_ns": 1, "outcome": "completed",
            })
            path.write_bytes(b"".join(canonical(event) for event in changed))
            with mock.patch.object(analyzer.native, "validate_shutdowns"), \
                    mock.patch.object(analyzer.native, "sq8_rss_artifact_reasons", return_value=[]), \
                    mock.patch.object(analyzer.native, "sq8_requests_share_initial_owner", return_value=True), \
                    mock.patch.object(analyzer, "_validate_quantized_search_path", return_value=True), \
                    mock.patch.object(analyzer, "_recompute_filtered_quality", return_value=({}, True)), \
                    self.assertRaisesRegex(analyzer.EvidenceError, "terminal disposition"):
                analyzer.validate_full_sq8_log(path, packet, truth, rss, None, None)

            events, truth = self._full_events(include_retune=True)
            path.write_bytes(b"".join(canonical(event) for event in events))
            with mock.patch.object(analyzer.native, "validate_shutdowns"), \
                    mock.patch.object(analyzer.native, "sq8_rss_artifact_reasons", return_value=[]), \
                    mock.patch.object(analyzer, "_validate_quantized_search_path", return_value=True):
                with self.assertRaisesRegex(analyzer.EvidenceError, "retuned"):
                    analyzer.validate_full_sq8_log(path, packet, truth, rss, None, None)

            events, truth = self._full_events()
            next(event for event in events
                 if event.get("event") == "coordinate_lock_consumed")["fresh_process_identity"] = "12:22"
            path.write_bytes(b"".join(canonical(event) for event in events))
            with mock.patch.object(analyzer.native, "validate_shutdowns"), \
                    mock.patch.object(analyzer.native, "sq8_rss_artifact_reasons", return_value=[]), \
                    mock.patch.object(analyzer, "_validate_quantized_search_path", return_value=True):
                with self.assertRaisesRegex(analyzer.EvidenceError, "fresh owner"):
                    analyzer.validate_full_sq8_log(path, packet, truth, rss, None, None)

            events, truth = self._full_events(quality_pass=False)
            events.insert(-1, {"event": "finalization_failure", "errors": ["disk cap"]})
            path.write_bytes(b"".join(canonical(event) for event in events))
            with mock.patch.object(analyzer.native, "sq8_requests_share_initial_owner", return_value=True), \
                    mock.patch.object(analyzer.native, "sq8_rss_artifact_reasons", return_value=[]), \
                    mock.patch.object(analyzer, "_validate_quantized_search_path", return_value=True), \
                    mock.patch.object(analyzer, "_recompute_filtered_quality", return_value=({}, True)):
                with self.assertRaisesRegex(analyzer.EvidenceError, "terminal disposition"):
                    analyzer.validate_full_sq8_log(path, packet, truth, rss, None, None)

    def test_filtered_quality_is_rebuilt_in_exact_producer_order(self):
        truth, rows = {}, []
        for eligible in analyzer.FULL_ELIGIBLE_COUNTS[:-1]:
            expected = [[f"row-{query:06d}"] for query in range(200)]
            truth[str(eligible)] = expected
            for query in range(200):
                event = {
                    "eligible": eligible, "ef": 32, "rerank_candidates": 32,
                    "query": query, "ids": expected[query],
                    "phase": "predicate_first_quality_query" if query == 0 else "quality_selection",
                }
                rows.append((len(rows), event, 1.0, 1.0))
        cohorts, passed = analyzer._recompute_filtered_quality(rows, truth, [32, 64])
        self.assertTrue(passed)
        self.assertEqual(set(cohorts), {"4096", "4097", "5000", "50000"})
        self.assertEqual(cohorts["4096"]["exact_query_matches"], [True] * 200)
        self.assertEqual(cohorts["4097"]["selected_coordinate"], {
            "ef_search": 32, "rerank_candidates": 32,
        })
        failed_rows = copy.deepcopy(rows)
        for index in range(200, 300):
            position, event, _, ndcg = failed_rows[index]
            failed_rows[index] = (position, event, 0.0, ndcg)
        failed_cohorts, passed = analyzer._recompute_filtered_quality(
            failed_rows, truth, [32],
        )
        self.assertFalse(passed)
        self.assertIsNone(failed_cohorts["4097"]["selected_coordinate"])
        self.assertEqual(failed_cohorts["50000"]["selected_coordinate"], {
            "ef_search": 32, "rerank_candidates": 32,
        })
        rows[201], rows[202] = rows[202], rows[201]
        with self.assertRaisesRegex(analyzer.EvidenceError, "ordered frozen grid"):
            analyzer._recompute_filtered_quality(rows, truth, [32, 64])

    def test_paired_statistics_do_not_pool_calls_across_repetitions(self):
        repetitions = []
        for repetition, exact in enumerate(([1, 100], [2, 200], [3, 300], [4, 400], [5, 500])):
            def arm(values):
                requests, clock = [], 10
                for value in values:
                    requests.append({"duration_ns": value, "started_monotonic_ns": clock,
                                     "ended_monotonic_ns": clock + value})
                    clock += value
                return {"requests": requests, "resources": {"delta": {
                    "server_cpu_ns_per_call": 1, "client_harness_cpu_ns_per_call": 2,
                    "total_alloc_bytes_per_call": 3, "mallocs_per_call": 4,
                }}}
            repetitions.append({
                "repetition": repetition,
                "arms": {
                    "exact": arm(exact),
                    "quantized_rerank": arm([value * 2 for value in exact]),
                },
            })
        result = analyzer.paired_statistics({"repetitions": repetitions})
        exact = result["arms"]["exact"]
        self.assertEqual(exact["per_repetition"][0]["p50_ns"], 1)
        self.assertEqual(exact["per_repetition"][0]["p95_ns"], 100)
        self.assertEqual(exact["across_repetitions"]["p95_ns"], {"median": 300, "mad": 100})
        self.assertEqual(exact["raw_repetitions_ns"][0], [1, 100])

    def test_paired_scores_are_recomputed_from_frozen_vectors(self):
        vectors = analyzer.np.zeros((2, 768), dtype=analyzer.np.float32)
        queries = analyzer.np.zeros((1, 768), dtype=analyzer.np.float32)
        vectors[:, 0], queries[:, 0] = 1, 1
        artifact = {"warmup": {"arms": {"exact": {"requests": [{
            "query": 0, "results": [{"id": "row-000001", "score": 1.0}],
            "recall": 1.0, "ndcg_at_10": 1.0,
        }]}}}, "repetitions": []}
        truth = [["row-000001"]]
        self.assertTrue(analyzer.paired_results_match_dataset(
            artifact, vectors, queries, truth,
        ))
        artifact["warmup"]["arms"]["exact"]["requests"][0]["results"][0]["score"] = .5
        self.assertFalse(analyzer.paired_results_match_dataset(
            artifact, vectors, queries, truth,
        ))
        artifact["warmup"]["arms"]["exact"]["requests"][0]["results"][0]["score"] = 1.0
        artifact["warmup"]["arms"]["exact"]["requests"][0]["recall"] = 0.0
        self.assertFalse(analyzer.paired_results_match_dataset(
            artifact, vectors, queries, truth,
        ))
        artifact["warmup"]["arms"]["exact"]["requests"][0]["recall"] = 1.0
        artifact["warmup"]["arms"]["exact"]["requests"][0]["results"][0]["id"] = "row-000000"
        self.assertFalse(analyzer.paired_results_match_dataset(
            artifact, vectors, queries, truth,
        ))
        exact = artifact["warmup"]["arms"]["exact"]["requests"][0]
        exact["results"][0]["id"] = "row-000001"
        artifact["warmup"]["arms"]["quantized_rerank"] = {
            "requests": [copy.deepcopy(exact)],
        }
        self.assertTrue(analyzer.paired_results_match_dataset(
            artifact, vectors, queries, truth, {0: ["row-000001"]},
        ))
        artifact["warmup"]["arms"]["quantized_rerank"]["requests"][0]["results"][0]["id"] = "row-000000"
        self.assertFalse(analyzer.paired_results_match_dataset(
            artifact, vectors, queries, truth, {0: ["row-000001"]},
        ))

    def test_paired_order_uses_recomputed_scores_and_locked_graph_owner(self):
        vectors = analyzer.np.zeros((2, 768), dtype=analyzer.np.float32)
        queries = analyzer.np.zeros((1, 768), dtype=analyzer.np.float32)
        vectors[0, 0] = queries[0, 0] = 1
        vectors[1, :2] = (1, .004)
        record = {
            "query": 0,
            "results": [
                {"id": "row-000001", "score": 1.0},
                {"id": "row-000000", "score": .999999},
            ],
            "recall": 1.0, "ndcg_at_10": 1.0,
        }
        artifact = {"warmup": {"arms": {"exact": {"requests": [record]}}},
                    "repetitions": []}
        self.assertFalse(analyzer.paired_results_match_dataset(
            artifact, vectors, queries, [["row-000000", "row-000001"]],
        ))

        owner = {
            "schema_hash": 1, "schema_generation": 2,
            "base_manifest": {"generation": 3, "format": "tcs1", "version": 1,
                              "checksum": 4},
            "current_manifest": {"generation": 3, "format": "tcs1", "version": 1,
                                 "checksum": 4},
            "base_coverage_lsn": 5, "current_coverage_lsn": 5,
        }
        locked = [(0, {"score_plane": {"snapshot": copy.deepcopy(owner)}})]
        paired = {"warmup": {"common_owner": copy.deepcopy(owner)}}
        self.assertTrue(analyzer._paired_owner_matches_locked_calls(locked, paired))
        paired["warmup"]["common_owner"]["schema_hash"] = 9
        self.assertFalse(analyzer._paired_owner_matches_locked_calls(locked, paired))

    def test_fp32_rss_quality_is_recomputed_from_ordered_id_ledger(self):
        truth = [["row-000000", "row-000001"], ["row-000001", "row-000000"]]
        quality = {
            "calibration": {"queries": [0], "curve": [{
                "control": 32, "mean_recall_at_10": 1.0, "mean_ndcg_at_10": 1.0,
                "per_query": [1.0], "per_query_ndcg_at_10": [1.0],
            }]},
            "revalidation": {"queries": [1], "curve": [{
                "control": 32, "mean_recall_at_10": 1.0, "mean_ndcg_at_10": 1.0,
                "per_query": [1.0], "per_query_ndcg_at_10": [1.0],
            }]},
        }
        observations = {
            "schema": "cohere_500k_768d_quality_observations/v1",
            "control_name": "hnsw_ef",
            "requests": [
                {"request_sequence": 1, "control": 32, "query": 0, "ids": truth[0]},
                {"request_sequence": 2, "control": 32, "query": 1, "ids": truth[1]},
            ],
            "exact_reference": {"query": 0, "ids": truth[0]},
        }
        artifact = {"quality": quality, "observed_quality": observations}
        analyzer._validate_fp32_quality_observations(artifact, truth, "hnsw_ef", 2)
        changed = copy.deepcopy(artifact)
        changed["observed_quality"]["exact_reference"]["ids"].reverse()
        with self.assertRaisesRegex(analyzer.EvidenceError, "truth order"):
            analyzer._validate_fp32_quality_observations(changed, truth, "hnsw_ef", 2)
        changed = copy.deepcopy(artifact)
        changed["quality"]["revalidation"]["curve"][0]["per_query"][0] = 0.5
        with self.assertRaisesRegex(analyzer.EvidenceError, "aggregate"):
            analyzer._validate_fp32_quality_observations(changed, truth, "hnsw_ef", 2)

    def test_frozen_truth_requires_exact_cohorts_ids_and_membership(self):
        truth = {}
        for eligible in analyzer.FULL_ELIGIBLE_COUNTS:
            selected = []
            ordinal = 0
            while len(selected) < 10:
                if (ordinal * 7919) % 500000 < eligible:
                    selected.append(f"row-{ordinal:06d}")
                ordinal += 1
            truth[str(eligible)] = [list(selected) for _ in range(200)]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "truth.json"
            path.write_bytes(canonical(truth))
            self.assertEqual(analyzer._full_truth(path), truth)
            truth["4096"][0][0] = "row-499999"
            path.write_bytes(canonical(truth))
            with self.assertRaisesRegex(analyzer.EvidenceError, "outside"):
                analyzer._full_truth(path)

    def _minimal_packet(self, declared):
        arms = {role: role for role in analyzer.ARM_KINDS}
        plans = {role: "plan_" + role for role in analyzer.PLAN_KINDS}
        dataset = {role: "dataset_" + role for role in analyzer.DATASET_KINDS}
        inputs = {role: "input_" + role for role in analyzer.INPUT_KINDS}
        files = {name: {"sha256": f"{number + 1:064x}"}
                 for number, name in enumerate(dataset.values())}
        return {
            "schema": analyzer.PACKET_SCHEMA, "candidate_commit": COMMIT,
            "files": files, "arms": arms, "plans": plans,
            "dataset": dataset, "inputs": inputs,
            "declared_quality_outcome": declared,
            "tradeoff_review": {
                "schema": analyzer.TRADEOFF_REVIEW_SCHEMA,
                "disposition": ("not_reached_after_quality_miss" if declared == "miss"
                                else "no_material_regression"),
                "rationale": "reviewed test disposition", "authority": None,
            },
        }

    def test_material_regression_requires_explicit_owner_acceptance(self):
        base = {"schema": analyzer.TRADEOFF_REVIEW_SCHEMA,
                "rationale": "reviewed tradeoff", "authority": None}
        self.assertTrue(analyzer.validate_tradeoff_review(
            {**base, "disposition": "no_material_regression"}, True,
        ))
        self.assertFalse(analyzer.validate_tradeoff_review(
            {**base, "disposition": "unaccepted_material_regression"}, True,
        ))
        with self.assertRaisesRegex(analyzer.EvidenceError, "owner acceptance"):
            analyzer.validate_tradeoff_review(
                {**base, "disposition": "owner_accepted_material_regression"}, True,
            )
        self.assertTrue(analyzer.validate_tradeoff_review({
            **base, "disposition": "owner_accepted_material_regression",
            "authority": "https://github.com/snissn/gomap/issues/4688#issuecomment-1",
        }, True))

    def test_states_and_final_state_wording_are_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "packet.json"
            packet = self._minimal_packet("miss")
            raw = canonical(packet)
            path.write_bytes(raw)
            pin = hashlib.sha256(raw).hexdigest()
            paths = {name: Path(directory) / name for name in set(packet["arms"].values())}
            paths.update({name: Path(directory) / name for name in packet["dataset"].values()})
            paths.update({name: Path(directory) / name for name in packet["inputs"].values()})
            dataset_paths = {kind: paths[name] for kind, name in packet["dataset"].items()}
            with mock.patch.object(analyzer, "resolve_inventory", return_value=paths), \
                    mock.patch.object(analyzer, "validate_consumer_source", return_value={}), \
                    mock.patch.object(analyzer, "validate_dataset", return_value=(dataset_paths, {})), \
                    mock.patch.object(analyzer, "validate_go_inputs", return_value={}), \
                    mock.patch.object(analyzer, "validate_receipts"), \
                    mock.patch.object(analyzer, "validate_plans"), \
                    mock.patch.object(analyzer, "validate_bounded_artifacts"), \
                    mock.patch.object(analyzer, "validate_rss_and_comparisons",
                                      return_value=({}, {}, {}, {"state": "accept"}, {"state": "complete"})), \
                    mock.patch.object(analyzer, "_smoke_truth", return_value={}), \
                    mock.patch.object(analyzer, "validate_smoke_log"), \
                    mock.patch.object(analyzer, "_full_truth", return_value={}), \
                    mock.patch.object(analyzer.np, "memmap", return_value=object()), \
                    mock.patch.object(analyzer, "validate_full_sq8_log",
                                      return_value=(False, None, None)):
                result = analyzer.analyze(path, pin, lambda _: None)
            self.assertEqual(result["state"], "valid_unqualified", result)
            assurance = result["final_state_assurance"]
            self.assertFalse(assurance["producer_attested"])
            self.assertFalse(assurance["independently_recomputed"])
            self.assertFalse(assurance["digest_claim"])
            self.assertEqual(assurance["availability"], "not_reached_after_frozen_quality_miss")

            packet["declared_quality_outcome"] = "pass"
            raw = canonical(packet)
            path.write_bytes(raw)
            result = analyzer.analyze(path, hashlib.sha256(raw).hexdigest(), lambda _: None)
            self.assertEqual(result["state"], "invalid")


if __name__ == "__main__":
    unittest.main()
