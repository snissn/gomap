#!/usr/bin/env python3
"""Command-contract tests for bench_vector_db_compare.sh."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("bench_vector_db_compare.sh")
ROOT = SCRIPT.parents[1]


class VectorDBCompareScriptTest(unittest.TestCase):
    def test_external_comparator_defaults_are_exactly_pinned(self) -> None:
        script = SCRIPT.read_text(encoding="utf-8")
        self.assertIn("PSYCOPG_PACKAGE=\"${PSYCOPG_PACKAGE:-psycopg[binary]==3.3.4}\"", script)
        self.assertIn("PYMILVUS_PACKAGE=\"${PYMILVUS_PACKAGE:-pymilvus==2.6.16}\"", script)
        self.assertIn("MILVUS_TOKEN=\"${MILVUS_TOKEN:-root:Milvus}\"", script)
        self.assertIn('--token "$MILVUS_TOKEN"', script)
        self.assertIn('"$MILVUS_STORAGE_DIR_EXPLICIT" == "true"', script)
        self.assertNotIn('|| -d "$MILVUS_STORAGE_DIR"', script)
        self.assertIn("9e0e8187e197ce23d3da3e63c19bc20189782f96bacb97287f8fcee80ba628c3", script)
        self.assertIn("milvusdb/milvus:v2.6.20@sha256:e514fced2aa26cf3b94e7de20986fe9e535159fde08f9934d245d0e1a909c18c", script)
        self.assertIn("pgvector/pgvector:pg16@sha256:84a355869251af1a3379cfc9fa7b4dbf962c03f642a4bb7b339a203925071c43", script)

    def run_with_fake_tools(
        self,
        *,
        validate_queries: str | None,
        backends: str = "treedb",
        extra_env: dict[str, str] | None = None,
    ) -> list[list[str]]:
        with tempfile.TemporaryDirectory(prefix="gomap_vector_db_compare_test_") as tmp:
            tmpdir = Path(tmp)
            fake_bin = tmpdir / "bin"
            fake_bin.mkdir()
            go_log = tmpdir / "go.jsonl"

            fake_python = fake_bin / "python3"
            fake_python.write_text(
                textwrap.dedent(
                    f"""\
                    #!{sys.executable}
                    import pathlib
                    import shutil
                    import sys

                    if sys.argv[1:3] == ["-m", "venv"]:
                        target = pathlib.Path(sys.argv[3]) / "bin" / "python"
                        target.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(sys.argv[0], target)
                        target.chmod(0o755)
                    """
                ),
                encoding="utf-8",
            )
            fake_python.chmod(0o755)

            fake_go = fake_bin / "go"
            fake_go.write_text(
                textwrap.dedent(
                    f"""\
                    #!{sys.executable}
                    import json
                    import os
                    import pathlib
                    import sys

                    with pathlib.Path(os.environ["FAKE_GO_LOG"]).open("a", encoding="utf-8") as log:
                        json.dump(sys.argv[1:], log)
                        log.write("\\n")
                    print("{{}}")
                    """
                ),
                encoding="utf-8",
            )
            fake_go.chmod(0o755)

            env = os.environ.copy()
            for name in (
                "MIN_RECALL",
                "TREEDB_QUANTIZED_MIN_RECALL",
                "TREEDB_QUANTIZED_ONLY_MIN_RECALL",
                "TREEDB_QUANTIZED_RERANK_MIN_RECALL",
                "TREEDB_RABITQ_QUANTIZED_MIN_RECALL",
                "TREEDB_RABITQ_QUANTIZED_ONLY_MIN_RECALL",
                "TREEDB_RABITQ_QUANTIZED_RERANK_MIN_RECALL",
            ):
                env.pop(name, None)
            env.update(
                {
                    "PATH": f"{fake_bin}{os.pathsep}{env['PATH']}",
                    "FAKE_GO_LOG": str(go_log),
                    "RUN_DIR": str(tmpdir / "run"),
                    "VENV": str(tmpdir / "venv"),
                    "PYTHON": str(fake_python),
                    "BACKENDS": backends,
                    "DOCS": "10",
                    "DIMS": "4",
                    "QUERIES": "10",
                    "TOP_K": "2",
                    "SEARCH_CONCURRENCY": "1",
                }
            )
            if validate_queries is None:
                env.pop("VALIDATE_QUERIES", None)
            else:
                env["VALIDATE_QUERIES"] = validate_queries
            if extra_env is not None:
                env.update(extra_env)

            result = subprocess.run(
                [str(SCRIPT)],
                cwd=ROOT,
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
            if result.returncode != 0:
                self.fail(
                    f"script failed with {result.returncode}\n"
                    f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
                )
            return [
                json.loads(line)
                for line in go_log.read_text(encoding="utf-8").splitlines()
            ]

    @staticmethod
    def flag_value(command: list[str], flag: str) -> str:
        return command[command.index(flag) + 1]

    def assert_forwarding(
        self,
        commands: list[list[str]],
        *,
        exporter_truth_queries: str,
        consumer_validate_queries: str,
        consumer_min_recall: str,
    ) -> None:
        exporter = next(
            command
            for command in commands
            if "./cmd/treedb_vector_dataset_export" in command
        )
        consumer = next(
            command
            for command in commands
            if "./cmd/treedb_vector_search_demo" in command
        )
        self.assertEqual(
            self.flag_value(exporter, "-truth-queries"),
            exporter_truth_queries,
        )
        self.assertEqual(
            self.flag_value(consumer, "-validate-queries"),
            consumer_validate_queries,
        )
        self.assertEqual(
            self.flag_value(consumer, "-min-recall"),
            consumer_min_recall,
        )

    def test_default_validation_is_bounded_by_short_query_count(self) -> None:
        self.assert_forwarding(
            self.run_with_fake_tools(validate_queries=None),
            exporter_truth_queries="10",
            consumer_validate_queries="64",
            consumer_min_recall="0.95",
        )

    def test_zero_validation_still_disables_exported_truth(self) -> None:
        self.assert_forwarding(
            self.run_with_fake_tools(validate_queries="0"),
            exporter_truth_queries="0",
            consumer_validate_queries="0",
            consumer_min_recall="0",
        )

    def test_quantized_recall_gate_is_preserved_with_validation(self) -> None:
        self.assert_forwarding(
            self.run_with_fake_tools(
                validate_queries="1",
                backends="treedb_column_graph_quantized_only",
                extra_env={"TREEDB_QUANTIZED_ONLY_MIN_RECALL": "0.8"},
            ),
            exporter_truth_queries="1",
            consumer_validate_queries="1",
            consumer_min_recall="0.8",
        )

    def test_zero_validation_disables_quantized_recall_gate(self) -> None:
        self.assert_forwarding(
            self.run_with_fake_tools(
                validate_queries="0",
                backends="treedb_column_graph_quantized_only",
                extra_env={"TREEDB_QUANTIZED_ONLY_MIN_RECALL": "0.8"},
            ),
            exporter_truth_queries="0",
            consumer_validate_queries="0",
            consumer_min_recall="0",
        )


if __name__ == "__main__":
    unittest.main()
