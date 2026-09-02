#!/usr/bin/env python3
"""Run the frozen #4331 manifest against an external Qdrant 1.19.0 server."""
from __future__ import annotations
import argparse, contextlib, hashlib, importlib.metadata, json, math, os, re, signal, subprocess, time, urllib.request
from collections import Counter
from pathlib import Path
from typing import Any

VERSION = "1.19.0"
SCHEMA = "treedb-rag-application-comparison/v1"
ARTIFACT_SCHEMA = "treedb-rag-qdrant-comparison/v1"
TOKENS = re.compile(r"[a-z0-9]+")

def canonical(value: Any) -> bytes: return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
def file_sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""): digest.update(block)
    return digest.hexdigest()
def server_info(url: str):
    with urllib.request.urlopen(url.rstrip("/") + "/", timeout=5) as response: value = json.load(response)
    if str(value.get("version")) != VERSION: raise RuntimeError(f"Qdrant server must be {VERSION}, got {value.get('version')!r}")
    return value

def sparse_vectors(manifest):
    tokenized = [TOKENS.findall(chunk["content"].lower()) for chunk in manifest["chunks"]]
    vocab = sorted({term for row in tokenized for term in row} | {term for query in manifest["queries"] for term in TOKENS.findall(query["text"].lower())})
    index, df = {term: i for i, term in enumerate(vocab)}, Counter(term for row in tokenized for term in set(row))
    n, avg, k1, b = len(tokenized), sum(map(len, tokenized)) / len(tokenized), manifest["config"]["sparse_bm25_k1"], manifest["config"]["sparse_bm25_b"]
    docs, serial = {}, {"vocabulary": vocab, "documents": {}, "queries": {}}
    for chunk, terms in zip(manifest["chunks"], tokenized, strict=True):
        counts, values = Counter(terms), []
        for term in sorted(counts, key=index.get):
            tf = counts[term]; idf = math.log(1 + (n - df[term] + .5) / (df[term] + .5))
            values.append((index[term], idf * tf * (k1 + 1) / (tf + k1 * (1 - b + b * len(terms) / avg))))
        docs[chunk["id"]] = ([v[0] for v in values], [v[1] for v in values]); serial["documents"][chunk["id"]] = values
    queries = {}
    for query in manifest["queries"]:
        counts = Counter(TOKENS.findall(query["text"].lower())); values = [(index[t], float(counts[t])) for t in sorted(counts, key=index.get)]
        queries[query["id"]] = ([v[0] for v in values], [v[1] for v in values]); serial["queries"][query["id"]] = values
    return docs, queries, hashlib.sha256(canonical(serial)).hexdigest()

@contextlib.contextmanager
def phase_timeout(name, seconds):
    previous = signal.getsignal(signal.SIGALRM)
    def expired(_signum, _frame): raise TimeoutError(f"{name} exceeded {seconds}s phase cap")
    signal.signal(signal.SIGALRM, expired); signal.setitimer(signal.ITIMER_REAL, seconds)
    try: yield
    finally: signal.setitimer(signal.ITIMER_REAL, 0); signal.signal(signal.SIGALRM, previous)
def filter_for(models, spec):
    must = []
    if spec.get("tenant"): must.append(models.FieldCondition(key="tenant", match=models.MatchValue(value=spec["tenant"])))
    if spec.get("workspace"): must.append(models.FieldCondition(key="workspace", match=models.MatchValue(value=spec["workspace"])))
    if spec.get("updated_year_gte"): must.append(models.FieldCondition(key="updated_year", range=models.Range(gte=spec["updated_year_gte"])))
    return models.Filter(must=must) if must else None
def authorized(payload, spec):
    return (not spec.get("tenant") or payload.get("tenant") == spec["tenant"]) and (not spec.get("workspace") or payload.get("workspace") == spec["workspace"]) and (not spec.get("updated_year_gte") or payload.get("updated_year", 0) >= spec["updated_year_gte"])
def percentile(values, q):
    ordered = sorted(values); return ordered[max(0, math.ceil(q * len(ordered)) - 1)]
def quality(ids, parents, chunks, relevant_parents):
    relevant, relevantp = set(chunks), set(relevant_parents)
    precision = lambda k: sum(value in relevant for value in ids[:k]) / k
    recall = lambda k: sum(value in relevant for value in ids[:k]) / len(relevant)
    def ndcg(k):
        actual = sum(1 / math.log2(i + 2) for i, value in enumerate(ids[:k]) if value in relevant)
        ideal = sum(1 / math.log2(i + 2) for i in range(min(k, len(relevant))))
        return actual / ideal
    rank = next((i + 1 for i, value in enumerate(ids[:10]) if value in relevant), 0)
    parent_recall = lambda k: len({value for value in parents[:k] if value in relevantp}) / len(relevantp)
    return {"precision_at_5": precision(5), "precision_at_10": precision(10), "ndcg_at_5": ndcg(5), "ndcg_at_10": ndcg(10), "mrr_at_10": 1 / rank if rank else 0, "hit_rate_at_10": 1 if rank else 0, "chunk_recall_at_5": recall(5), "chunk_recall_at_10": recall(10), "parent_recall_at_5": parent_recall(5), "parent_recall_at_10": parent_recall(10)}
def mean_quality(rows):
    out = {key: sum(row[key] for row in rows) / len(rows) for key in rows[0]}
    out.update({"relevant_chunks_mean": 0, "relevant_parents_mean": 0, "max_achievable_chunk_recall_at_5": 0, "max_achievable_chunk_recall_at_10": 0, "max_achievable_parent_recall_at_5": 0, "max_achievable_parent_recall_at_10": 0, "max_per_parent_results": 0, "collapse_rejections": 0, "collapse_exhaustions": 0, "text_attributed_results": 0, "vector_attributed_results": 0, "text_vector_overlap_results": 0, "attribution_mode": "qdrant_native_route"})
    return out
def directory_bytes(path): return sum(row.stat().st_size for row in path.rglob("*") if row.is_file())
def process_stats(pid):
    if pid <= 0: return {}
    result = subprocess.run(["ps", "-o", "rss=", "-o", "time=", "-p", str(pid)], check=True, capture_output=True, text=True, timeout=5)
    fields = result.stdout.split()
    if len(fields) != 2: raise RuntimeError(f"cannot parse Qdrant process metrics for PID {pid}")
    clock = fields[1]
    days = 0
    if "-" in clock:
        day_text, clock = clock.split("-", 1); days = int(day_text)
    parts = [float(value) for value in clock.split(":")]
    cpu_seconds = sum(value * (60 ** index) for index, value in enumerate(reversed(parts))) + days * 86400
    return {"pid": pid, "rss_bytes": int(fields[0]) * 1024, "cpu_seconds": cpu_seconds, "captured_unix_nanos": time.time_ns()}

class Runner:
    def __init__(self, manifest, manifest_sha, args, factory, models):
        self.manifest, self.manifest_sha, self.args, self.factory, self.models, self.client = manifest, manifest_sha, args, factory, models, factory()
        self.config = manifest["config"]; self.filters = {row["id"]: row for row in manifest["filters"]}
        self.sparse_docs, self.sparse_queries, self.sparse_sha = sparse_vectors(manifest)
        self.ids = {row["id"]: i + 1 for i, row in enumerate(manifest["chunks"])}; self.cells, self.failures, self.build_seconds, self.query_seconds = [], [], 0, 0
        self.reopen = {"attempted": False, "succeeded": False, "version": "", "point_count": 0, "seconds": 0}
        self.process_samples = [process_stats(args.server_pid)]
    def build(self):
        m, c = self.models, self.client
        started = time.monotonic()
        if c.collection_exists(self.args.collection):
            if not self.args.allow_drop: raise RuntimeError("collection exists; --allow-drop required")
            c.delete_collection(self.args.collection, timeout=90)
        c.create_collection(collection_name=self.args.collection, vectors_config={self.config["dense_vector_name"]: m.VectorParams(size=384, distance=m.Distance.COSINE)}, sparse_vectors_config={self.config["sparse_vector_name"]: m.SparseVectorParams(index=m.SparseIndexParams(on_disk=False))}, hnsw_config=m.HnswConfigDiff(m=16, ef_construct=100, full_scan_threshold=0), optimizers_config=m.OptimizersConfigDiff(indexing_threshold=1, max_optimization_threads=1), timeout=90)
        c.create_payload_index(self.args.collection, "tenant", m.PayloadSchemaType.KEYWORD, wait=True); c.create_payload_index(self.args.collection, "workspace", m.PayloadSchemaType.KEYWORD, wait=True); c.create_payload_index(self.args.collection, "updated_year", m.PayloadSchemaType.INTEGER, wait=True)
        points = []
        for chunk in self.manifest["chunks"]:
            sparse = self.sparse_docs[chunk["id"]]
            payload = {key: chunk[key] for key in ("id", "parent_id", "ordinal", "content", "tenant", "workspace", "updated_year")}
            points.append(m.PointStruct(id=self.ids[chunk["id"]], vector={self.config["dense_vector_name"]: chunk["dense_vector"], self.config["sparse_vector_name"]: m.SparseVector(indices=sparse[0], values=sparse[1])}, payload=payload))
        c.upsert(self.args.collection, points=points, wait=True)
        deadline, info = time.monotonic() + 90, None
        while time.monotonic() < deadline:
            info = c.get_collection(self.args.collection)
            if int(getattr(info, "points_count", -1)) == 54 and int(getattr(info, "indexed_vectors_count", -1)) >= 54 and str(getattr(info, "status", "")).lower().endswith("green"): break
            time.sleep(.1)
        self.build_seconds = time.monotonic() - started
        if info is None or int(getattr(info, "points_count", -1)) != 54 or int(getattr(info, "indexed_vectors_count", -1)) < 54 or any(field not in (getattr(info, "payload_schema", {}) or {}) for field in ("tenant", "workspace", "updated_year")): raise RuntimeError("build count/index proof failed")
    def restart(self):
        started = time.monotonic()
        self.reopen["attempted"] = True
        self.process_samples.append(process_stats(self.args.server_pid))
        self.client.close()
        restarted = subprocess.run([str(self.args.restart_hook)], check=True, capture_output=True, text=True, timeout=90)
        try: self.args.server_pid = int(restarted.stdout.splitlines()[-1])
        except (IndexError, ValueError) as exc: raise RuntimeError("standalone restart hook did not return authoritative PID") from exc
        self.args.server_identity += f"|reopened_pid:{self.args.server_pid}"
        self.process_samples.append(process_stats(self.args.server_pid))
        deadline, last = time.monotonic() + 90, None
        while time.monotonic() < deadline:
            try:
                info = server_info(self.args.url); self.client = self.factory(); count = int(getattr(self.client.get_collection(self.args.collection), "points_count", -1))
                if count != 54: raise RuntimeError(f"reopen count={count}")
                self.reopen.update(attempted=True, succeeded=True, version=info["version"], point_count=count, seconds=time.monotonic() - started); return
            except Exception as exc: last = exc; time.sleep(.2)
        raise RuntimeError(f"reopen failed: {last}")
    def query(self, route, query, filter_id):
        m, c, filt = self.models, self.client, filter_for(self.models, self.filters[filter_id]); sparse = self.sparse_queries[query["id"]]
        params, started = m.SearchParams(hnsw_ef=64, exact=False), time.monotonic_ns()
        if route == "lexical": response = c.query_points(self.args.collection, query=m.SparseVector(indices=sparse[0], values=sparse[1]), using=self.config["sparse_vector_name"], query_filter=filt, limit=self.config["top_k"], with_payload=False, with_vectors=False)
        elif route == "dense": response = c.query_points(self.args.collection, query=query["dense_vector"], using=self.config["dense_vector_name"], query_filter=filt, search_params=params, limit=self.config["top_k"], with_payload=False, with_vectors=False)
        else: response = c.query_points(self.args.collection, query=m.FusionQuery(fusion=m.Fusion.RRF), prefetch=[m.Prefetch(query=m.SparseVector(indices=sparse[0], values=sparse[1]), using=self.config["sparse_vector_name"], filter=filt, limit=self.config["candidate_limit"]), m.Prefetch(query=query["dense_vector"], using=self.config["dense_vector_name"], filter=filt, params=params, limit=self.config["candidate_limit"])], query_filter=filt, limit=self.config["top_k"], with_payload=False, with_vectors=False)
        point_ids = [row.id for row in response.points]; search_ms = (time.monotonic_ns() - started) / 1e6; started = time.monotonic_ns()
        fetched = c.retrieve(self.args.collection, ids=point_ids[:self.config["top_k"]], with_payload=True, with_vectors=False); by_id = {row.id: row.payload for row in fetched}; payloads = [by_id[value] for value in point_ids if value in by_id]
        if len(payloads) != len(point_ids) or len(payloads) > self.config["top_k"]: raise RuntimeError("bounded fetch failed")
        ids = [row["id"] for row in payloads]; fetch_ms = (time.monotonic_ns() - started) / 1e6
        return ids, payloads, search_ms, fetch_ms
    def run(self):
        query_started = time.monotonic()
        timing = "total_ms spans query_points, point-ID extraction, bounded retrieve, payload ordering/validation, leakage/accounting, and sample recording; search_ms and fetch_ms are nested subtimers"
        for route in ("lexical", "dense", "hybrid"):
          for filter_id in [row["id"] for row in self.manifest["filters"]]:
            samples, repetitions, last, leakage, fetch_max = [], [], {}, 0, 0
            for ordinal in range(self.config["warmups_per_cell"]): self.query(route, self.manifest["queries"][ordinal % 3], filter_id)
            for repetition in range(self.config["repetitions"]):
              repetition_started = time.monotonic_ns()
              for ordinal in range(self.config["samples_per_cell"]):
                total_started = time.monotonic_ns(); query = self.manifest["queries"][ordinal % 3]
                ids, payloads, search_ms, fetch_ms = self.query(route, query, filter_id)
                leakage += sum(not authorized(row, self.filters[filter_id]) for row in payloads); fetch_max = max(fetch_max, len(payloads)); last[query["id"]] = (ids, [row["parent_id"] for row in payloads])
                sample = {"repetition": repetition, "ordinal": ordinal, "query_id": query["id"], "search_ms": search_ms, "fetch_ms": fetch_ms, "total_ms": 0, "result_ids": ids, "fetched_count": len(payloads), "fetched_bytes": len(canonical(payloads))}
                samples.append(sample); sample["total_ms"] = (time.monotonic_ns() - total_started) / 1e6
              wall = (time.monotonic_ns() - repetition_started) / 1e9
              repetitions.append({"repetition": repetition, "samples": self.config["samples_per_cell"], "wall_seconds": wall, "qps": self.config["samples_per_cell"] / wall})
            metrics = []
            for query in self.manifest["queries"]:
                judgment = next(row for row in query["cases"] if row["filter"] == filter_id); ids, parents = last[query["id"]]; metrics.append(quality(ids, parents, judgment["relevant_chunks"], judgment["relevant_parents"]))
            durations = [row["total_ms"] for row in samples]; vectors = [self.config["sparse_vector_name"]] if route == "lexical" else [self.config["dense_vector_name"]]
            if route == "hybrid": vectors = [self.config["sparse_vector_name"], self.config["dense_vector_name"]]
            self.cells.append({"route": route, "filter": filter_id, "equivalence": "directional", "timing_semantics": timing, "warmups": self.config["warmups_per_cell"], "repetitions": self.config["repetitions"], "samples": samples, "repetition_performance": repetitions, "summary": {"qps": sum(row["qps"] for row in repetitions) / len(repetitions), "latency_ms_p50": percentile(durations, .5), "latency_ms_p95": percentile(durations, .95), "latency_ms_p99": percentile(durations, .99)}, "quality": mean_quality(metrics), "leakage": leakage, "errors": 0, "timeouts": 0, "fetch_max_count": fetch_max, "route_proof": {"api": "qdrant.query_points", "named_vectors": vectors, "fusion": "rrf" if route == "hybrid" else "", "fallbacks": 0, "exhaustive_search": False, "bounded_fetch": True}})
            self.process_samples.append(process_stats(self.args.server_pid))
        self.query_seconds = time.monotonic() - query_started
    def artifact(self):
        observed = [row for row in self.process_samples if row]
        resources = {"host_pid_metrics": "observed_process_samples_across_pre_and_post_restart_segments", "process_samples": observed, "peak_observed_rss_bytes": max((row["rss_bytes"] for row in observed), default=0), "cpu_seconds": sum(max(0, right["cpu_seconds"] - left["cpu_seconds"]) for left, right in zip(observed, observed[1:]) if left["pid"] == right["pid"]), "durable_bytes": directory_bytes(self.args.storage_path)}
        server_binary_sha = file_sha256(self.args.server_binary)
        return {"schema": ARTIFACT_SCHEMA, "backend": "qdrant_server", "harness_revision": self.args.harness_revision, "client_version": VERSION, "manifest_sha256": self.manifest_sha, "fixture_sha256": self.manifest["fixture_sha256"], "semantic_vector_sha256": self.manifest["semantic_vector_sha256"], "config_sha256": self.manifest["config_sha256"], "source_count": 18, "chunk_count": 54, "query_count": 3, "sparse_vector_sha256": self.sparse_sha, "build": {"seconds": self.build_seconds, "points": 54}, "query_seconds": self.query_seconds, "server": {"version": VERSION, "deployment": "standalone", "binary_sha256": server_binary_sha, "identity": self.args.server_identity, "local_mode": False, "config": {"dense": self.config["dense_vector_name"], "sparse": self.config["sparse_vector_name"], "hnsw_m": 16, "full_scan_threshold": 0, "indexing_threshold": 1, "exact": False}}, "resources": resources, "reopen": self.reopen, "cells": self.cells, "failures": self.failures}

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name, kwargs in [("--manifest", {"type": Path, "required": True}), ("--output", {"type": Path, "required": True}), ("--url", {"required": True}), ("--collection", {"required": True}), ("--server-identity", {"required": True}), ("--harness-revision", {"required": True}), ("--storage-path", {"type": Path, "required": True}), ("--restart-hook", {"type": Path, "required": True}), ("--server-binary", {"type": Path, "required": True}), ("--server-pid", {"type": int, "required": True})]: parser.add_argument(name, **kwargs)
    parser.add_argument("--allow-drop", action="store_true"); args = parser.parse_args()
    raw = args.manifest.read_bytes(); manifest = json.loads(raw)
    if manifest.get("schema") != SCHEMA or importlib.metadata.version("qdrant-client") != VERSION: raise RuntimeError("manifest or qdrant-client identity mismatch")
    if not re.fullmatch(r"[0-9a-f]{40}", args.harness_revision): raise RuntimeError("full harness revision required")
    if not args.server_binary.is_file() or file_sha256(args.server_binary) != manifest["config"]["qdrant_binary_sha256"]: raise RuntimeError("Qdrant server binary SHA-256 mismatch")
    if manifest["config"]["qdrant_server_version"] != VERSION: raise RuntimeError("Qdrant server version binding mismatch")
    if not args.storage_path.is_dir() or not os.access(args.restart_hook, os.X_OK): raise RuntimeError("durable path and executable restart hook required")
    if args.server_pid <= 0: raise RuntimeError("standalone Qdrant requires an authoritative positive server PID")
    from qdrant_client import QdrantClient, models
    server_info(args.url); factory = lambda: QdrantClient(url=args.url, timeout=90, prefer_grpc=False); runner = Runner(manifest, hashlib.sha256(raw).hexdigest(), args, factory, models); code = 0
    try:
        with phase_timeout("build", 90): runner.build()
        with phase_timeout("reopen", 90): runner.restart()
        with phase_timeout("query", 90): runner.run()
    except BaseException as exc: runner.failures.append(f"{type(exc).__name__}: {exc}"); code = 1
    finally:
        try: runner.client.close()
        finally: args.output.parent.mkdir(parents=True, exist_ok=True); args.output.write_text(json.dumps(runner.artifact(), indent=2, sort_keys=True, allow_nan=False) + "\n")
    return code
if __name__ == "__main__": raise SystemExit(main())
