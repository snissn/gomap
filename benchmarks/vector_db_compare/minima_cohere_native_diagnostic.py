#!/usr/bin/env python3
"""Nonqualifying Minima-style public/native lifecycle using existing Cohere FP32 exports.

Separate from the frozen 8D Minima/M5 schema. No download or protected holdout access.
Freeze this reviewed harness before collection; use --freeze, then --run with its SHA256.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
import hashlib
import inspect
import json
import math
import os
from pathlib import Path
import platform
import shutil
import signal
import subprocess
import threading
import time

import numpy as np

import minima_treedb_runner as existing

SCHEMA = "treedb_minima_cohere_native_diagnostic/v1"
GIB = 1 << 30


def digest(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            result.update(chunk)
    return result.hexdigest()


def canonical(value):
    return (json.dumps(value, sort_keys=True, allow_nan=False, separators=(",", ":")) + "\n").encode()


def counts(rows):
    if rows == 512:
        return [64, 65, 128, 256, 512]
    return sorted({min(rows, n) for n in (4096, 4097, 5000, 50000, rows)})


def predicate(rows, eligible):
    return None if eligible == rows else {"field": "meta.user_id", "operator": "<", "value": f"{eligible:06d}"}


def make_document(vectors, row, total, updated=False):
    return {"id": f"row-{row:06d}", "content": f"minima-cohere:{row}" + (":updated" if updated else ""),
            "embedding": vectors[row].tolist(),
            "meta": {"user_id": f"{(row * 7919) % total:06d}", "fpath": f"/cohere/{row // 256:06d}.txt"}}


def exact_truth(vectors, queries, eligible_counts):
    """Bounded smoke oracle only; the 500K run consumes the existing exhaustive oracle."""
    normalized = np.asarray(vectors, dtype=np.float64)
    normalized /= np.linalg.norm(normalized, axis=1)[:, None]
    normalized_queries = np.asarray(queries, dtype=np.float64)
    normalized_queries /= np.linalg.norm(normalized_queries, axis=1)[:, None]
    ranks = (np.arange(len(vectors)) * 7919) % len(vectors)
    scores = normalized_queries @ normalized.T
    result = {}
    for eligible in eligible_counts:
        rows = np.flatnonzero(ranks < eligible)
        result[str(eligible)] = [[f"row-{int(row):06d}" for row in rows[np.lexsort((rows, -values[rows]))[:10]]]
                                for values in scores]
    return result


def quantiles(values):
    values = sorted(values)
    if not values:
        return {"count": 0}
    return {"count": len(values), "total_ns": sum(values),
            **{name + "_ns": values[math.ceil(len(values) * fraction) - 1]
               for name, fraction in (("p50", .5), ("p95", .95), ("p99", .99), ("max", 1))}}


def prepare(args):
    source = Path(__file__).resolve().parents[2]
    validate_imports(source)
    harness = existing.repository_commit()
    dataset = args.dataset.resolve()
    manifest = json.loads((dataset / "manifest.json").read_text())
    if (manifest.get("dimensions"), manifest.get("top_k"), manifest.get("exact_train_query_overlap")) != (768, 10, 0):
        raise ValueError("expected the existing real 768D top-10 nonoverlapping diagnostic export")
    if args.rows not in (512, 500000) or args.rows > manifest["rows"] or math.gcd(args.rows, 7919) != 1:
        raise ValueError("only real-prefix 512-row smoke or 500000-row diagnostic runs are supported")
    query_count = 4 if args.rows == 512 else 100
    if query_count > manifest["query_count"]:
        raise ValueError("not enough exported queries")
    files = {}
    for name, expected_size in (("documents", manifest["rows"] * 768 * 4),
                                ("queries", manifest["query_count"] * 768 * 4), ("truth", None)):
        path = dataset / (name + (".json" if name == "truth" else ".f32"))
        if expected_size is not None and path.stat().st_size != expected_size:
            raise ValueError(f"{name} size does not match dataset manifest")
        files[name] = digest(path)
        if files[name] != manifest[name + "_sha256"]:
            raise ValueError(f"{name} hash does not match dataset manifest")
    existing.service_binary_build_provenance(args.service_bin, args.product_commit)
    root_tree = lambda path: subprocess.check_output(["git", "rev-parse", "HEAD:" + path], cwd=source, text=True).strip()
    product_tree = lambda path: subprocess.check_output(["git", "rev-parse", args.product_commit + ":" + path], cwd=source, text=True).strip()
    serving = json.loads(args.serving.read_text())
    if not isinstance(serving, dict) or not serving:
        raise ValueError("explicit column_graph serving limits required")
    return {"schema": SCHEMA, "qualification": "not_evaluated", "mode": "smoke" if args.rows == 512 else "diagnostic",
            "harness_commit": harness, "harness_source_sha256": digest(Path(__file__)),
            "harness_trees": {path: root_tree(path) for path in ("benchmarks/vector_db_compare", "clients/python/treedb_client")},
            "product_commit": args.product_commit, "service_bin": str(args.service_bin.resolve()),
            "product_trees": {path: product_tree(path) for path in ("TreeDB", "cmd/treedb-document-service", "internal", "go.mod", "go.sum")},
            "service_sha256": digest(args.service_bin), "dataset": str(dataset),
            "dataset_manifest_sha256": digest(dataset / "manifest.json"), "dataset_files_sha256": files,
            "serving": serving, "serving_sha256": digest(args.serving), "serving_path": str(args.serving.resolve()),
            "run_dir": str(args.run_dir.resolve()), "rows": args.rows, "dimensions": 768, "queries": query_count,
            "top_k": 10, "batch_size": 256, "efs": [128, 256, 512, 1024, 2048], "overlap_ef": 512,
            "overlap_eligible": counts(args.rows)[1],
            "eligible_counts": counts(args.rows), "reader_concurrency": 4, "writer_calls": 8,
            "scalar_shape": "dispersed unique rank=(row*7919)%rows; user_id range, fpath equality for lifecycle delete",
            "writer_shape": "eight 256-row same-ID replacements after full initial ingestion; fixed live row count",
            "url": args.url, "native_address": args.native_address, "diagnostics_url": args.diagnostics_url,
            "operation_timeout_s": 600, "wall_limit_s": 2700, "minimum_free_bytes": 10 * GIB,
            "maximum_output_bytes": 11 * GIB, "maximum_combined_rss_bytes": 24 * GIB,
            "gomaxprocs": os.environ.get("GOMAXPROCS", ""), "cpu_affinity": sorted(os.sched_getaffinity(0)),
            "blas_threads": {key: os.environ.get(key, "") for key in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS")},
            "python": os.sys.version, "numpy": np.__version__, "platform": platform.platform(),
            "query_usage": "previously opened diagnostic/calibration queries, not final holdout",
            "infrastructure": "INFRASTRUCTURE_UNAVAILABLE: runner: shared workstation, serialized quiet window; persistent cache and local artifact storage"}


def validate_plan(plan, expected):
    if plan != expected:
        raise ValueError("frozen plan differs from current source/binary/dataset/configuration/environment")


def validate_imports(source):
    for imported, directory in ((existing, "benchmarks/vector_db_compare"),
                                (existing.common, "benchmarks/vector_db_compare"),
                                (existing.TreeDBClient, "clients/python/treedb_client/src")):
        if not Path(inspect.getfile(imported)).resolve().is_relative_to(source / directory):
            raise ValueError("imported runner/client is outside the frozen source tree")


def validate_shutdowns(lifetimes, expected_count):
    if len(lifetimes) != expected_count:
        raise RuntimeError("owned service lifetime count mismatch")
    for lifetime in lifetimes:
        terminal, exited = lifetime.get("terminal_work", {}), lifetime.get("exit", {})
        if (terminal.get("cleanup_completed") is not True or terminal.get("shutdown_failures") != 0
                or terminal.get("contract_version") != existing.SERVICE_CONTRACT
                or terminal.get("work", {}).get("pid") != lifetime["pid"]
                or exited.get("pid") != lifetime["pid"] or exited.get("exit_code") != 0
                or exited.get("availability") != "measured"
                or exited.get("linux_process_identity") != lifetime["linux_process_identity"]):
            raise RuntimeError("owned service did not complete a clean verified shutdown")


class Run:
    def __init__(self, plan):
        self.plan = plan
        self.output = Path(plan["run_dir"])
        self.output.mkdir(parents=True, exist_ok=False)  # Never reuse or overwrite a prior run.
        self.events = (self.output / "events.jsonl").open("x", buffering=1)
        self.lock, self.cancel = threading.Lock(), threading.Event()
        self.failure = None
        self.controller = existing.ServiceController(Path(plan["service_bin"]), plan["url"], self.output / "db",
            "command_wal_durable", 600, 120, diagnostics_url=plan["diagnostics_url"],
            block_profile_rate=0, mutex_profile_fraction=0, native_address=plan["native_address"], measured=True)
        self.clients = existing.ThreadLocalClients(plan["url"], plan["operation_timeout_s"], self.controller)
        data = Path(plan["dataset"])
        self.vectors = np.memmap(data / "documents.f32", mode="r", dtype="<f4", shape=(plan["rows"], 768))
        self.queries = np.memmap(data / "queries.f32", mode="r", dtype="<f4", shape=(plan["queries"], 768))
        self.truth = (json.loads((data / "truth.json").read_text()) if plan["mode"] == "diagnostic"
                      else exact_truth(self.vectors, self.queries, plan["eligible_counts"]))
        for eligible in plan["eligible_counts"]:
            if len(self.truth[str(eligible)]) != plan["queries"]:
                raise ValueError("oracle query cardinality mismatch")
        self.updated = set()
        self.info = None

    def emit(self, event, **fields):
        with self.lock:
            self.events.write(canonical({"event": event, **fields}).decode())

    def timed(self, phase, call, **fields):
        if self.failure:
            raise RuntimeError(self.failure)
        start = time.monotonic_ns()
        try:
            result = call()
        except BaseException as exc:
            self.emit("call", phase=phase, start_ns=start, duration_ns=time.monotonic_ns() - start,
                      outcome="error", error=f"{type(exc).__name__}: {exc}", **fields)
            raise
        end = time.monotonic_ns()
        self.emit("call", phase=phase, start_ns=start, end_ns=end, duration_ns=end - start, outcome="completed", **fields)
        return result

    def check_resources(self):
        free = shutil.disk_usage(self.output).free
        size = existing.common.disk_bytes(self.output)
        process = self.controller.process
        rss = 0
        for pid in (os.getpid(), process.pid if process else None):
            if pid is not None:
                try:
                    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
                        if line.startswith("VmRSS:"):
                            rss += int(line.split()[1]) * 1024
                except FileNotFoundError:
                    pass
        elapsed = time.monotonic() - self.started
        self.emit("resource", elapsed_s=elapsed, free_bytes=free, output_bytes=size, combined_rss_bytes=rss)
        if (free < self.plan["minimum_free_bytes"] or size > self.plan["maximum_output_bytes"]
                or rss > self.plan["maximum_combined_rss_bytes"] or elapsed > self.plan["wall_limit_s"]):
            raise RuntimeError("frozen disk/RAM/wall budget exceeded")

    def guard(self):
        while not self.cancel.wait(1):
            try:
                self.check_resources()
            except BaseException as exc:
                self.failure = f"resource guard: {exc}"
                self.emit("guard_failed", error=self.failure)
                process = self.controller.process
                if process and existing.common.linux_process_identity(process.pid) == self.controller._owned_identity:
                    os.kill(process.pid, signal.SIGTERM)
                return

    def ensure(self):
        info = self.clients.ensure_index("minima_cohere", 768, "cosine", typed_input=True,
            scalar_fields=[{"field": "meta.user_id", "value_type": "string"}, {"field": "meta.fpath", "value_type": "string"}],
            vector_index_options={"strategy": "column_graph"})
        if (info.dimension != 768 or info.metric != "cosine" or info.vector_strategy != "column_graph"
                or info.extra.get("typed_input") is not True
                or {(f.field, f.value_type) for f in info.scalar_fields} != {("meta.user_id", "string"), ("meta.fpath", "string")}):
            raise RuntimeError("public collection schema differs from frozen typed schema")
        self.info = info
        return info

    def optimize(self, action):
        return self.clients.optimize_index("minima_cohere", column_graph_action=action,
            column_graph_serving=self.plan["serving"] if action in ("build", "ensure") else None,
            expected_generation=self.info.generation)

    def upsert(self, rows, phase, updated=False):
        batch = [make_document(self.vectors, row, self.plan["rows"], updated) for row in rows]
        response = self.timed(phase, lambda: self.clients.native.upsert_documents("minima_cohere", batch, index_info=self.info),
                              first_row=rows[0], rows=len(rows))
        if response.upserted != len(rows) or response.ids != [d["id"] for d in batch]:
            raise RuntimeError("public upsert completion mismatch")
        return response

    def search(self, phase, eligible, ef, query, writer_active=False):
        response = self.timed(phase, lambda: self.clients.native.query_by_embedding("minima_cohere", self.queries[query].tolist(), 10,
            predicate(self.plan["rows"], eligible), route="ann", ef_search=ef, index_info=self.info),
            eligible=eligible, ef=ef, query=query, writer_active=writer_active)
        if (response.native_command_version != 2 or response.index.generation != self.info.generation
                or response.index.vector_strategy != "column_graph" or response.route != "ann"
                or response.exact_fallbacks or response.full_document_scan_fallbacks or response.dense_work is None):
            raise RuntimeError("search left the required producer-backed native typed route")
        if eligible != self.plan["rows"] and (not response.dense_work.graph.filter.completed
                                               or response.dense_work.graph.filter.eligible_rows != eligible):
            raise RuntimeError("producer scalar-filter membership differs from declared population")
        ids = [doc.id for doc in response.documents]
        if len(ids) != min(10, eligible) or len(set(ids)) != len(ids):
            raise RuntimeError("unexpected search result cardinality")
        for doc in response.documents:
            row = int(doc.id.removeprefix("row-"))
            if not 0 <= row < self.plan["rows"] or (row * 7919) % self.plan["rows"] >= eligible:
                raise RuntimeError("cross-filter search result")
            if doc.meta.get("user_id") != f"{(row * 7919) % self.plan['rows']:06d}" or doc.score is None:
                raise RuntimeError("missing scalar/score projection")
            # Correctness-only scoring is outside the public API timer.
            vector = np.asarray(self.vectors[row], dtype=np.float64)
            query_vector = np.asarray(self.queries[query], dtype=np.float64)
            score = np.dot(vector, query_vector) / (np.linalg.norm(vector) * np.linalg.norm(query_vector))
            if not math.isfinite(doc.score) or abs(score - doc.score) > 2e-5:
                raise RuntimeError("returned score differs from independent full-vector cosine")
        truth = self.truth[str(eligible)][query]
        self.emit("search_result", phase=phase, eligible=eligible, ef=ef, query=query, ids=ids,
                  recall=len(set(ids) & set(truth)) / len(truth), writer_active=writer_active,
                  dense_work=asdict(response.dense_work))
        return response

    def check_documents(self, docs, expected_ids):
        if [None if doc is None else doc.id for doc in docs] != expected_ids:
            raise RuntimeError("public materialization ID ordering/missing mismatch")
        for doc in docs:
            if doc is None:
                continue
            row = int(doc.id.removeprefix("row-"))
            expected = make_document(self.vectors, row, self.plan["rows"], row in self.updated)
            if doc.content != expected["content"] or doc.meta != expected["meta"]:
                raise RuntimeError("public materialization payload mismatch")
            actual = np.asarray(doc.embedding, dtype=np.float64)
            want = np.asarray(self.vectors[row], dtype=np.float64)
            if actual.shape != (768,) or not np.isfinite(actual).all():
                raise RuntimeError("public materialization vector shape mismatch")
            actual_norm, want_norm = np.linalg.norm(actual), np.linalg.norm(want)
            if not (math.isfinite(actual_norm) and actual_norm > 0 and math.isfinite(want_norm) and want_norm > 0):
                raise RuntimeError("public materialization zero/nonfinite vector norm")
            actual /= actual_norm
            want /= want_norm
            if np.max(np.abs(actual - want)) > 1e-6:
                raise RuntimeError("public materialization full-vector mismatch")

    def overlap(self):
        barrier = threading.Barrier(5)
        active = threading.Event()
        active.set()
        def reader(worker):
            barrier.wait(timeout=30)
            for n in range(64):
                self.search("overlap_search", self.plan["overlap_eligible"], self.plan["overlap_ef"],
                            (n * 4 + worker) % self.plan["queries"], active.is_set())
        def writer():
            barrier.wait(timeout=30)
            try:
                for n in range(8):
                    start = (self.plan["rows"] - 256 * (n + 1)) % self.plan["rows"]
                    rows = list(range(start, start + 256))
                    self.upsert(rows, "overlap_replace", updated=True)
                    self.updated.update(rows)
            finally:
                active.clear()
        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = [pool.submit(reader, n) for n in range(4)] + [pool.submit(writer)]
            for future in futures:
                future.result()

    def lifecycle(self):
        rows = list(range(256))
        ids = [f"row-{row:06d}" for row in rows]
        self.updated.update(rows)
        self.upsert(rows, "explicit_update", updated=True)
        docs = self.timed("native_get_many", lambda: self.clients.native.get_many("minima_cohere", ids, index_info=self.info))
        self.check_documents(docs, ids)
        deleted = self.timed("http_delete_file", lambda: self.clients.delete_by_filter("minima_cohere",
            {"field": "meta.fpath", "operator": "==", "value": "/cohere/000000.txt"}, expected_generation=self.info.generation))
        if deleted.deleted != len(rows):
            raise RuntimeError("file delete cardinality mismatch")
        docs = self.timed("native_deleted_visibility", lambda: self.clients.native.get_many("minima_cohere", ids, index_info=self.info))
        self.check_documents(docs, [None] * len(ids))
        self.upsert(rows, "reindex_replacement", updated=True)
        docs = self.timed("native_reinsert_visibility", lambda: self.clients.native.get_many("minima_cohere", ids, index_info=self.info))
        self.check_documents(docs, ids)
        response = self.timed("empty_user", lambda: self.clients.native.query_by_embedding("minima_cohere", self.queries[0].tolist(), 10,
            {"field": "meta.user_id", "operator": "==", "value": "missing-user"}, route="ann", ef_search=512, index_info=self.info))
        if response.documents:
            raise RuntimeError("empty-user query returned documents")

    def scroll(self):
        after, seen = None, 0
        while True:
            result = self.clients.filter_documents("minima_cohere", limit=256, return_embedding=True,
                after_id=after, cursor_page=True, expected_generation=self.info.generation)
            expected = [f"row-{row:06d}" for row in range(seen, min(seen + 256, self.plan["rows"]))]
            self.check_documents(result.documents, expected)
            seen += len(result.documents)
            if result.exhausted:
                break
            if not result.next_after_id or result.next_after_id == after or not result.documents:
                raise RuntimeError("public verification cursor did not advance")
            after = result.next_after_id
        if seen != self.plan["rows"]:
            raise RuntimeError("public full-state count mismatch")
        self.emit("full_state_verified", rows=seen, vectors_checked=seen, normalized_full_vector_tolerance=1e-6)

    def execute(self):
        self.started = time.monotonic()
        monitor = threading.Thread(target=self.guard, daemon=True)
        self.emit("plan", plan=self.plan)
        monitor.start()
        failed = None
        try:
            self.timed("service_start", self.controller.start)
            self.timed("schema_ensure", self.ensure)
            for start in range(0, self.plan["rows"], 256):
                self.upsert(list(range(start, min(start + 256, self.plan["rows"]))), "initial_durable_ingest")
            self.timed("initial_graph_build", lambda: self.optimize("build"))
            # Cache-cold means first request for this predicate, not OS/disk-cold.
            for eligible in self.plan["eligible_counts"]:
                self.search("predicate_first_query", eligible, 512, 0)
                for ef in self.plan["efs"]:
                    for query in range(self.plan["queries"]):
                        self.search("warm_curve", eligible, ef, query)
            self.overlap()
            self.lifecycle()
            self.timed("pre_close_fold", lambda: self.optimize("fold"))
            self.timed("close", self.clients.close)
            validate_shutdowns(self.controller.lifetimes, 1)
            self.timed("reopen", self.controller.start)
            self.timed("idempotent_ensure", self.ensure)
            self.timed("reopen_graph_ensure", lambda: self.optimize("ensure"))
            for eligible in self.plan["eligible_counts"]:
                for query in range(self.plan["queries"]):
                    self.search("post_reopen_curve", eligible, 512, query)
            self.timed("verification_only_full_scroll", self.scroll)
            if self.failure:
                raise RuntimeError(self.failure)
        except BaseException as exc:
            failed = f"{type(exc).__name__}: {exc}"
            self.emit("failure", error=failed)
        finally:
            try:
                self.clients.close()
                validate_shutdowns(self.controller.lifetimes, 2)
            except BaseException as exc:
                failed = failed or f"shutdown: {exc}"
            self.cancel.set()
            monitor.join(timeout=5)
            failed = failed or self.failure
            try:
                if monitor.is_alive():
                    raise RuntimeError("resource guard did not stop")
                self.check_resources()
            except BaseException as exc:
                failed = failed or f"final resource guard: {exc}"
            self.emit("terminal", lifecycle_complete=failed is None, qualification="not_evaluated", error=failed,
                      process_lifetimes=self.controller.lifetimes, final_disk_bytes=existing.common.disk_bytes(self.output / "db"))
            self.events.close()
        return int(failed is not None)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--freeze", type=Path)
    mode.add_argument("--run", type=Path)
    parser.add_argument("--expected-plan-sha256")
    parser.add_argument("--dataset", required=True, type=Path)
    parser.add_argument("--service-bin", required=True, type=Path)
    parser.add_argument("--product-commit", required=True)
    parser.add_argument("--serving", required=True, type=Path)
    parser.add_argument("--run-dir", required=True, type=Path)
    parser.add_argument("--rows", type=int, choices=[512, 500000], default=500000)
    parser.add_argument("--url", default="http://127.0.0.1:17420")
    parser.add_argument("--native-address", default="127.0.0.1:17422")
    parser.add_argument("--diagnostics-url", default="http://127.0.0.1:17421")
    args = parser.parse_args()
    plan = prepare(args)
    if args.freeze:
        args.freeze.parent.mkdir(parents=True, exist_ok=True)
        with args.freeze.open("xb") as stream:
            stream.write(canonical(plan))
        print(digest(args.freeze), args.freeze)
        return 0
    if not args.expected_plan_sha256 or digest(args.run) != args.expected_plan_sha256:
        raise ValueError("externally pinned plan hash required")
    frozen = json.loads(args.run.read_text())
    validate_plan(frozen, plan)
    if shutil.disk_usage(args.run_dir.parent).free < plan["minimum_free_bytes"] + GIB:
        raise RuntimeError("insufficient disk headroom before starting service")
    return Run(frozen).execute()


if __name__ == "__main__":
    raise SystemExit(main())
