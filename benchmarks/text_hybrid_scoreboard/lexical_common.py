#!/usr/bin/env python3
"""Frozen corpus, reference semantics, validation, and consolidation."""

from __future__ import annotations

import hashlib
import json
import math
import re
import unicodedata
from pathlib import Path
from typing import Any, Iterable

MANIFEST_SCHEMA = "treedb_lexical_manifest/v1"
RESULT_SCHEMA = "treedb_lexical_result/v1"
COMPARISON_SCHEMA = "treedb_lexical_comparison/v1"
TOKEN_RE = re.compile(r"[a-z0-9]+")


class ValidationError(ValueError):
    pass


def canonical_json(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def load_manifest(path: Path) -> dict[str, Any]:
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != MANIFEST_SCHEMA:
        raise ValidationError(f"manifest schema must be {MANIFEST_SCHEMA}")
    frozen_analysis = {
        "normalization": "Unicode NFKC then lowercase",
        "tokenization": "contiguous ASCII letters or digits",
        "stopwords": [],
        "fields": [{"name": "title", "weight": 3.0}, {"name": "body", "weight": 1.0}],
        "bm25f": {"k1": 1.2, "b": 0.75, "idf": "ln(1 + (N - df + 0.5) / (df + 0.5))", "field_length": "tf / (1 - b + b * length / average_field_length)", "combination": "sum(field_weight * normalized_field_tf), then one BM25 saturation per term"},
        "external_weighted_field": "title tokens repeated exactly 3 times followed by body tokens; frozen equal field lengths make Lucene weighted-term scoring algebraically identical to pinned BM25F; Lucene phrase eligibility is a non-scoring title/body phrase OR filter over those weighted term scores; Bleve and SQLite retain their separately disclosed native directional scoring",
    }
    if manifest.get("analysis") != frozen_analysis:
        raise ValidationError("manifest analysis must match the adapter-bound frozen scoring contract")
    query_ids = [query.get("id") for query in manifest.get("queries", [])]
    if len(query_ids) != len(set(query_ids)) or not query_ids:
        raise ValidationError("manifest query IDs must be non-empty and unique")
    return manifest


def manifest_sha256(manifest: dict[str, Any]) -> str:
    return sha256_bytes(canonical_json(manifest))


def normalize(text: str) -> str:
    return unicodedata.normalize("NFKC", text).lower()


def tokenize(text: str) -> list[str]:
    return TOKEN_RE.findall(normalize(text))


def frozen_document(index: int) -> tuple[str, str, str, str]:
    length = 12
    title: list[str] = []
    body: list[str] = []
    probes = {
        0: (4, ["common"] * 4, ["common"] * 4),
        1: (4, ["common"] * 4, ["common"]),
        2: (4, ["common"] * 3, ["common"]),
        3: (4, ["common"] * 2, ["common"]),
        4: (4, ["common"], ["common"] * 2),
        5: (4, ["common"], ["common"]),
        6: (4, ["common"], []),
        7: (4, [], ["common"] * 2),
        8: (12, [], ["common"]),
        9: (4, [], ["common"]),
    }
    if index in probes:
        length, title, body = probes[index]
    elif index < 5000:
        body.append("common")
    if 10 <= index < 20:
        body.append("rare")
    if 20 <= index < 30:
        body.extend(("alpha", "beta"))
    elif 30 <= index < 40:
        body.append("alpha")
    elif 40 <= index < 50:
        body.append("beta")
    if 50 <= index < 60:
        body.append("gamma")
    elif 60 <= index < 70:
        body.append("delta")
    elif 70 <= index < 80:
        body.extend(("gamma", "delta"))
    if 80 <= index < 85:
        title = ["titlefill"] * 11 + ["quick"]
        body = ["fox"]
    elif 85 <= index < 90:
        title = ["fox"] + ["titlefill"] * 10 + ["quick"]
    elif 90 <= index < 100:
        body.extend(("quick", "fox"))
    if 100 <= index < 120:
        body.append("tenantterm")
    if len(title) > length or len(body) > length:
        raise ValidationError(f"frozen corpus document {index} exceeds its field length")
    title.extend(["titlefill"] * (length - len(title)))
    body.extend(["bodyfill"] * (length - len(body)))
    tenant = "tenant-rare" if 100 <= index < 110 else "tenant-broad"
    return f"doc-{index:06d}", " ".join(title), " ".join(body), tenant


def corpus_bytes(document_count: int) -> bytes:
    return "".join("\t".join(frozen_document(i)) + "\n" for i in range(document_count)).encode()


def write_frozen_corpus(manifest: dict[str, Any], path: Path) -> str:
    payload = corpus_bytes(int(manifest["corpus"]["document_count"]))
    digest = sha256_bytes(payload)
    expected = manifest["corpus"]["sha256"]
    if digest != expected:
        raise ValidationError(f"frozen corpus drift: generated {digest}, manifest requires {expected}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return digest


def read_corpus(path: Path) -> list[dict[str, str]]:
    documents = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        fields = line.split("\t")
        if len(fields) != 4:
            raise ValidationError(f"corpus line {line_number}: expected four TSV fields")
        documents.append(dict(zip(("id", "title", "body", "tenant"), fields, strict=True)))
    return documents


def result_digest(ids: Iterable[str]) -> str:
    values = list(ids)
    return sha256_bytes(("\n".join(values) + ("\n" if values else "")).encode())


def eligible_result_ids(manifest: dict[str, Any], documents: list[dict[str, str]]) -> dict[str, set[str]]:
    eligible: dict[str, set[str]] = {}
    for query in manifest["queries"]:
        terms = [normalize(term) for term in query["terms"]]
        matches: set[str] = set()
        for document in documents:
            if query.get("filter") and document[query["filter"]["field"]] != query["filter"]["equals"]:
                continue
            fields = {name: tokenize(document[name]) for name in ("title", "body")}
            combined = fields["title"] + fields["body"]
            semantic = query["semantic"]
            if semantic == "phrase":
                width = len(terms)
                matched = any(any(tokens[offset:offset + width] == terms for offset in range(len(tokens) - width + 1)) for tokens in fields.values())
            elif semantic == "and":
                matched = all(term in combined for term in terms)
            else:
                matched = any(term in combined for term in terms)
            if matched:
                matches.add(document["id"])
        eligible[query["id"]] = matches
    return eligible


def reference_results(
    manifest: dict[str, Any],
    documents: list[dict[str, str]],
    *,
    weight_overrides: dict[str, float] | None = None,
    b_override: float | None = None,
    verify_evidence: bool = True,
) -> dict[str, list[str]]:
    weights = {field["name"]: float(field["weight"]) for field in manifest["analysis"]["fields"]}
    if weight_overrides:
        weights.update(weight_overrides)
    bm25f = manifest["analysis"]["bm25f"]
    k1 = float(bm25f["k1"])
    b = float(bm25f["b"]) if b_override is None else b_override
    top_k = int(manifest["execution"]["top_k"])
    tokenized = {doc["id"]: {field: tokenize(doc[field]) for field in weights} for doc in documents}
    average_lengths = {
        field: sum(len(fields[field]) for fields in tokenized.values()) / len(documents)
        for field in weights
    }
    results: dict[str, list[str]] = {}
    corpus_size = len(documents)
    for query in manifest["queries"]:
        terms = [normalize(term) for term in query["terms"]]
        document_frequency = {
            term: sum(any(term in fields[field] for field in weights) for fields in tokenized.values())
            for term in terms
        }
        ranked: list[tuple[float, str]] = []
        for doc in documents:
            if query.get("filter") and doc[query["filter"]["field"]] != query["filter"]["equals"]:
                continue
            fields = tokenized[doc["id"]]
            counts = {term: sum(fields[field].count(term) for field in weights) for term in terms}
            semantic = query["semantic"]
            if semantic in ("term", "term_scalar"):
                matched = counts[terms[0]] > 0
            elif semantic == "and":
                matched = all(counts[term] for term in terms)
            elif semantic == "or":
                matched = any(counts[term] for term in terms)
            elif semantic == "phrase":
                matched = any(
                    any(tokens[i : i + len(terms)] == terms for i in range(len(tokens) - len(terms) + 1))
                    for tokens in fields.values()
                )
            else:
                raise ValidationError(f"unknown query semantic {semantic!r}")
            if not matched:
                continue
            score = 0.0
            for term in terms:
                df = document_frequency[term]
                if not df or not counts[term]:
                    continue
                idf = math.log(1 + (corpus_size - df + 0.5) / (df + 0.5))
                combined_tf = 0.0
                for field, weight in weights.items():
                    field_tf = fields[field].count(term)
                    if not field_tf:
                        continue
                    denominator = 1 - b + b * len(fields[field]) / average_lengths[field]
                    combined_tf += weight * field_tf / denominator
                score += idf * (combined_tf * (k1 + 1)) / (combined_tf + k1)
            ranked.append((-score, doc["id"]))
        ranked.sort()
        ids = [doc_id for _, doc_id in ranked[:top_k]]
        evidence = query.get("ranking_evidence", {}).get("expected_top_k")
        if verify_evidence and evidence is not None and ids != evidence:
            raise ValidationError(f"query {query['id']}: frozen BM25F ranking evidence drift")
        results[query["id"]] = ids
    return results


def unavailable_result(engine: dict[str, str], repetition: int, manifest_digest: str, kind: str, reason: str, setup_command: list[str], stderr: str = "") -> dict[str, Any]:
    return {
        "schema_version": RESULT_SCHEMA,
        "status": "unavailable",
        "engine": engine,
        "repetition": repetition,
        "manifest_sha256": manifest_digest,
        "unavailable": {
            "kind": kind,
            "reason": reason,
            "setup_command": setup_command,
            "stderr": stderr[-4000:],
        },
        "cases": [],
    }


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValidationError(message)


def _validate_resource(resource: Any, name: str, prefix: str) -> None:
    _require(isinstance(resource, dict), f"{prefix}: {name} resource evidence missing")
    status = resource.get("status")
    _require(status in {"ok", "unsupported"}, f"{prefix}: {name} resource status is untyped")
    if status == "ok":
        _require(isinstance(resource.get("value"), int) and resource["value"] >= 0, f"{prefix}: {name} resource value invalid")
        _require(bool(resource.get("unit")), f"{prefix}: {name} resource unit missing")
    else:
        _require(bool(resource.get("reason")), f"{prefix}: {name} unsupported reason missing")


def _validate_environment(environment: Any, manifest: dict[str, Any], prefix: str) -> None:
    _require(isinstance(environment, dict), f"{prefix}: environment evidence missing")
    _require(environment.get("contract") == manifest["environment"], f"{prefix}: environment contract mismatch")
    filesystem = environment.get("filesystem", {})
    _require(filesystem.get("same_filesystem") is True, f"{prefix}: filesystem identity policy not enforced")
    identities = [filesystem.get(key) for key in ("runner_device_id", "corpus_store_id", "index_store_id", "result_store_id")]
    _require(all(isinstance(identity, str) and identity.isdigit() for identity in identities) and len(set(identities)) == 1, f"{prefix}: runner/corpus/index/result POSIX st_dev mismatch")
    memory = environment.get("memory", {})
    _require(bool(memory.get("detection_source")) and memory.get("matches_runner_detected") is True, f"{prefix}: memory-limit detection proof missing")
    _require(memory.get("adapter_changed_limit") is False, f"{prefix}: adapter changed the inherited memory limit")
    _require(isinstance(memory.get("detected_address_space_limit"), str) and (memory["detected_address_space_limit"] == "unlimited" or memory["detected_address_space_limit"].isdigit()), f"{prefix}: detected memory limit missing or not exact")
    execution = environment.get("execution", {})
    _require(execution.get("query_concurrency") == 1, f"{prefix}: query concurrency mismatch")
    _require(execution.get("engine_process_concurrency") == 1, f"{prefix}: engine process concurrency mismatch")
    _require(execution.get("runtime_cpu_parallelism") == 1, f"{prefix}: runtime CPU parallelism mismatch")
def _is_sha256(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def validate_result(artifact: dict[str, Any], manifest: dict[str, Any], expected: dict[str, list[str]], eligible: dict[str, set[str]], corpus_ids: set[str]) -> None:
    _require(artifact.get("schema_version") == RESULT_SCHEMA, "result schema version mismatch")
    _require(type(artifact.get("repetition")) is int and artifact["repetition"] >= 1, "result repetition is invalid")
    _require(_is_sha256(artifact.get("manifest_sha256")), "manifest digest is malformed")
    engine_id = artifact.get("engine", {}).get("id", "<missing>")
    prefix = f"{engine_id} repetition {artifact.get('repetition', '?')}"
    engine = artifact.get("engine", {})
    _require(engine_id != "<missing>" and all(isinstance(engine.get(key), str) and bool(engine[key]) for key in ("family", "name", "version")), f"{prefix}: engine identity/version missing")
    _require(isinstance(artifact.get("repetition"), int) and artifact["repetition"] > 0, f"{prefix}: invalid repetition")
    _require(artifact.get("schema_version") == RESULT_SCHEMA, f"{prefix}: wrong result schema")
    _require(artifact.get("manifest_sha256") == manifest_sha256(manifest), f"{prefix}: manifest drift")
    status = artifact.get("status")
    if status == "unavailable":
        unavailable = artifact.get("unavailable", {})
        _require(unavailable.get("kind") in {"missing_runtime", "dependency_setup_failed", "feature_unavailable"}, f"{prefix}: untyped unavailability")
        _require(bool(unavailable.get("reason")), f"{prefix}: unavailable reason missing")
        _require(isinstance(unavailable.get("setup_command"), list) and bool(unavailable.get("setup_command")), f"{prefix}: setup command missing")
        _require(artifact.get("cases") == [], f"{prefix}: unavailable artifact must not contain cases")
        return
    _require(status == "ok", f"{prefix}: status must be ok or unavailable")
    corpus = artifact.get("corpus", {})
    _require(corpus.get("sha256") == manifest["corpus"]["sha256"], f"{prefix}: corpus content drift")
    _require(corpus.get("document_count") == manifest["corpus"]["document_count"], f"{prefix}: document count drift")
    _require(isinstance(artifact.get("command"), list) and bool(artifact["command"]) and all(isinstance(value, str) for value in artifact["command"]), f"{prefix}: exact command missing or malformed")
    _require(isinstance(artifact.get("versions"), dict) and bool(artifact["versions"]), f"{prefix}: versions missing")
    _require(isinstance(artifact.get("config"), dict) and bool(artifact["config"]), f"{prefix}: configuration missing")
    _require(artifact["config"].get("top_k") == manifest["execution"]["top_k"], f"{prefix}: top-K config mismatch")
    _require(artifact["config"].get("stored_source_fields") == ["id", "title", "body", "tenant"], f"{prefix}: durable footprint omits logical source fields")
    expected_scoring_contract = "pinned_bm25f" if engine_id in {"treedb_text_v2", "lucene"} else "native_directional"
    _require(artifact["config"].get("scoring_contract") == expected_scoring_contract, f"{prefix}: scoring equivalence classification mismatch")
    _require(artifact["config"].get("build_timing_boundary") == manifest["execution"]["build_timing_boundary"], f"{prefix}: build timing boundary mismatch")
    _require(artifact["config"].get("tie_break") == "score,id" or engine_id == "treedb_text_v2", f"{prefix}: tie-break config mismatch")
    if engine_id == "treedb_text_v2":
        _require(artifact["config"].get("weights") == {"title": 3, "body": 1} and artifact["config"].get("bm25f") == {"k1": 1.2, "b": 0.75}, f"{prefix}: TreeDB BM25F config mismatch")
    elif engine_id == "lucene":
        _require(artifact["config"].get("weighted_field_materialization") == "title repeated 3x then body for term scoring, including phrase-qualified rows", f"{prefix}: Lucene weighted-field config mismatch")
        _require(artifact["config"].get("phrase_filter_fields") == ["title", "body"], f"{prefix}: Lucene phrase-filter fields mismatch")
        _require(artifact["config"].get("phrase_scoring") == "weighted terms scored; title/body phrase OR applied as FILTER", f"{prefix}: Lucene phrase scoring/filter contract mismatch")
    elif engine_id == "bleve":
        _require(artifact["config"].get("weighted_field_materialization") == "title repeated 3x then body for non-phrase native scoring", f"{prefix}: Bleve weighted-field config mismatch")
        _require(artifact["config"].get("phrase_fields") == ["title", "body"] and artifact["config"].get("phrase_scoring") == "native TF-IDF title boost 3, body boost 1", f"{prefix}: Bleve phrase scoring disclosure mismatch")
    elif engine_id == "sqlite_fts5":
        _require(artifact["config"].get("weighted_field_materialization") == "title repeated 3x then body for non-phrase native scoring", f"{prefix}: SQLite weighted-field config mismatch")
        _require(artifact["config"].get("phrase_fields") == ["title", "body"] and artifact["config"].get("phrase_scoring") == "native bm25 title weight 3, body weight 1", f"{prefix}: SQLite phrase scoring disclosure mismatch")
    _validate_environment(artifact.get("environment"), manifest, prefix)
    build = artifact.get("build", {})
    _require(build.get("checkpointed") is True, f"{prefix}: build was not checkpointed")
    _require(all(key in build for key in ("elapsed_nanos", "docs_per_second", "cpu", "peak_rss")), f"{prefix}: build/resource fields missing")
    _require(type(build.get("elapsed_nanos")) is int and build["elapsed_nanos"] >= 0 and isinstance(build.get("docs_per_second"), (int, float)) and build["docs_per_second"] >= 0, f"{prefix}: build metrics malformed")
    _validate_resource(build.get("cpu"), "build CPU", prefix)
    _validate_resource(build.get("peak_rss"), "peak RSS", prefix)
    storage = artifact.get("storage", {})
    _require(all(key in storage for key in ("durable_bytes", "wal_bytes", "transient_bytes")), f"{prefix}: storage classes missing")
    _require(all(type(storage[key]) is int and storage[key] >= 0 for key in ("durable_bytes", "wal_bytes", "transient_bytes")), f"{prefix}: storage values malformed")
    reopen = artifact.get("reopen", {})
    _require(reopen.get("performed") is True and reopen.get("verified") is True and _is_sha256(reopen.get("result_digest")), f"{prefix}: reopen proof missing or malformed")
    if engine_id == "sqlite_fts5":
        _require(reopen.get("query_connection_reopened") is True and reopen.get("durability_connection_reopened") is True, f"{prefix}: SQLite query/durability reopen separation missing")
    cases = artifact.get("cases", [])
    _require(isinstance(cases, list) and all(isinstance(case, dict) for case in cases), f"{prefix}: cases must be an object array")
    by_id = {case.get("id"): case for case in cases}
    _require(len(by_id) == len(cases), f"{prefix}: duplicate case IDs")
    _require(set(by_id) == set(expected), f"{prefix}: cases do not match manifest")
    _require(reopen["result_digest"] == result_digest(case.get("reopen_result_digest", "") for case in cases), f"{prefix}: consolidated reopen digest mismatch")
    measured = int(manifest["execution"]["measured_queries_per_case"])
    for query in manifest["queries"]:
        case = by_id[query["id"]]
        case_prefix = f"{prefix} case {query['id']}"
        if case.get("status") == "unsupported":
            _require(bool(case.get("unsupported_reason")), f"{case_prefix}: unsupported reason missing")
            _require(case.get("equivalent") is False, f"{case_prefix}: unsupported row marked equivalent")
            _require(not case.get("result_ids") and not case.get("samples_nanos"), f"{case_prefix}: unsupported row contains measured results")
            continue
        _require(case.get("status") in {"ok", "directional"}, f"{case_prefix}: invalid status")
        directional = case["status"] == "directional"
        _require(case.get("equivalent") is (not directional), f"{case_prefix}: equivalence classification mismatch")
        if directional:
            _require(bool(case.get("non_equivalent_reason")), f"{case_prefix}: non-equivalent reason missing")
        ids = case.get("result_ids")
        _require(isinstance(ids, list), f"{case_prefix}: result IDs missing")
        _require(len(ids) == len(set(ids)), f"{case_prefix}: duplicate result IDs")
        _require(set(ids) <= corpus_ids, f"{case_prefix}: result leakage outside corpus")
        if not directional:
            _require(ids == expected[query["id"]], f"{case_prefix}: reference result/order mismatch")
        _require(case.get("result_digest") == result_digest(ids), f"{case_prefix}: result digest mismatch")
        _require(case.get("reopen_result_ids") == ids, f"{case_prefix}: reopen result mismatch")
        _require(case.get("reopen_result_digest") == case.get("result_digest"), f"{case_prefix}: reopen digest mismatch")
        samples = case.get("samples_nanos")
        _require(isinstance(samples, list) and len(samples) == measured, f"{case_prefix}: raw sample count mismatch")
        _require(all(isinstance(value, int) and value >= 0 for value in samples), f"{case_prefix}: invalid latency sample")
        route = case.get("route", {})
        if directional:
            universe = eligible[query["id"]]
            _require(set(ids) <= universe, f"{case_prefix}: directional row violates query/filter eligibility")
            _require(len(ids) == min(manifest["execution"]["top_k"], len(universe)), f"{case_prefix}: directional row is truncated")
        proof = route.get("proof")
        _require(route.get("intended") is True and bool(route.get("name")) and isinstance(proof, (dict, list)) and bool(proof), f"{case_prefix}: intended-route proof missing")
        if engine_id == "treedb_text_v2":
            _require(isinstance(proof, dict) and type(proof.get("fail_closed")) is int and proof["fail_closed"] == 0 and proof.get("documents_fetched") == 0, f"{case_prefix}: TreeDB text-v2 fail-closed/score-only proof invalid")
            if query["semantic"] == "term_scalar":
                _require(route["name"] == "text_v2_blockmax_scalar_prefilter" and proof.get("scalar_filter_strategy") == "prefilter" and type(proof.get("text_index_epoch")) is int and proof["text_index_epoch"] >= 0 and type(proof.get("text_candidates")) is int and proof["text_candidates"] >= manifest["execution"]["top_k"], f"{case_prefix}: TreeDB scalar-prefilter route proof invalid")
            else:
                _require(proof.get("index_version") == "v2" and isinstance(proof.get("active_roots"), list) and bool(proof["active_roots"]) and type(proof.get("postings_scanned")) is int and proof["postings_scanned"] > 0, f"{case_prefix}: TreeDB text-v2 index route proof invalid")
        elif engine_id == "lucene":
            _require(isinstance(proof, dict) and bool(proof.get("query_class")) and isinstance(proof.get("reader_documents"), int), f"{case_prefix}: Lucene route proof invalid")
        elif engine_id == "bleve":
            _require(isinstance(proof, dict) and bool(proof.get("index_type")) and bool(proof.get("query_type")), f"{case_prefix}: Bleve route proof invalid")
        elif engine_id == "sqlite_fts5":
            _require(isinstance(proof, list), f"{case_prefix}: SQLite FTS5 query-plan proof invalid")
        _require(route.get("fallback") is False, f"{case_prefix}: silent fallback")
        _require(case.get("timed_out") is False, f"{case_prefix}: silent timeout")


def percentile(values: list[int], probability: float) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    return ordered[max(0, math.ceil(probability * len(ordered)) - 1)]


def consolidate(artifacts: list[dict[str, Any]], manifest: dict[str, Any], documents: list[dict[str, str]], repetitions: int, context: dict[str, Any]) -> dict[str, Any]:
    source = context.get("source", {})
    _require(all(source.get(key) for key in ("commit", "tree_oid", "treedb_subtree_oid", "harness_subtree_oid")), "source provenance is incomplete")
    _require(isinstance(source.get("vcs_modified"), bool), "source dirty state is missing")
    _require(source.get("qualification_eligible") is (not source["vcs_modified"]), "source qualification state is inconsistent")
    _require(bool(source.get("tracked_diff_sha256")) and source.get("post_run_reverified") is True, "source end-of-run recheck proof is missing")
    expected = reference_results(manifest, documents)
    untracked_sources = source.get("untracked_sources")
    _require(isinstance(untracked_sources, list) and all(isinstance(item, dict) and bool(item.get("path")) and item.get("kind") in {"file", "symlink"} and isinstance(item.get("sha256"), str) and len(item["sha256"]) == 64 for item in untracked_sources), "untracked source identity is malformed")
    tracked_dirty = source["tracked_diff_sha256"] != sha256_bytes(b"")
    _require(source["vcs_modified"] is (tracked_dirty or bool(untracked_sources)), "source dirty state does not match tracked/untracked identity")
    _require(context.get("environment_contract") == manifest["environment"], "runner environment contract mismatch")
    enforced = context.get("enforced_execution", {})
    _require(enforced.get("query_concurrency") == 1 and enforced.get("engine_process_concurrency") == 1 and enforced.get("runtime_cpu_parallelism") == 1, "runner execution policy mismatch")
    eligible = eligible_result_ids(manifest, documents)
    _require(bool(context.get("detected_address_space_limit")) and bool(context.get("runner_filesystem_device_id")), "runner detected resource policy is incomplete")
    corpus_ids = {doc["id"] for doc in documents}
    for artifact in artifacts:
        validate_result(artifact, manifest, expected, eligible, corpus_ids)
        if artifact.get("status") == "ok":
            environment = artifact["environment"]
            _require(environment["filesystem"]["runner_device_id"] == context["runner_filesystem_device_id"], f"{artifact['engine']['id']}: artifact/runner filesystem identity mismatch")
            _require(environment["memory"]["detected_address_space_limit"] == context["detected_address_space_limit"], f"{artifact['engine']['id']}: artifact/runner memory limit mismatch")
        if artifact.get("status") == "ok" and artifact["engine"]["id"] == "treedb_text_v2":
            _require(artifact["engine"]["version"] == source["commit"], "TreeDB artifact is not bound to the source commit")
    grouped: dict[str, list[dict[str, Any]]] = {}
    for artifact in artifacts:
        grouped.setdefault(artifact["engine"]["id"], []).append(artifact)
    completed: set[str] = set()
    ledger = []
    rows = []
    directional_rows = []
    builds = []
    partial: set[str] = set()
    for engine_id, engine_artifacts in sorted(grouped.items()):
        engine_artifacts.sort(key=lambda item: item["repetition"])
        available = [item for item in engine_artifacts if item["status"] == "ok"]
        unavailable = [item for item in engine_artifacts if item["status"] == "unavailable"]
        if unavailable:
            _require(not available, f"{engine_id}: mixed available and unavailable repetitions")
            _require(len(unavailable) == repetitions, f"{engine_id}: expected {repetitions} unavailable repetition records")
            _require([item["repetition"] for item in unavailable] == list(range(1, repetitions + 1)), f"{engine_id}: unavailable repetition sequence is incomplete")
            first = unavailable[0]
            for item in unavailable[1:]:
                _require(item["engine"] == first["engine"], f"{engine_id}: unavailable engine metadata differs across repetitions")
                _require(item["unavailable"] == first["unavailable"], f"{engine_id}: unavailable classification differs across repetitions")
            ledger.append({"engine": first["engine"], "status": "unavailable", "detail": first["unavailable"]})
            continue
        _require(len(available) == repetitions, f"{engine_id}: expected {repetitions} retained repetitions")
        _require([item["repetition"] for item in available] == list(range(1, repetitions + 1)), f"{engine_id}: retained repetition sequence is incomplete")
        baseline = available[0]
        for item in available[1:]:
            _require(item["engine"] == baseline["engine"], f"{engine_id}: engine metadata differs across repetitions")
            _require(item["versions"] == baseline["versions"], f"{engine_id}: versions differ across repetitions")
            _require(item["config"] == baseline["config"], f"{engine_id}: benchmark config differs across repetitions")
            _require(item["environment"] == baseline["environment"], f"{engine_id}: detected/enforced environment differs across repetitions")
        accepted_cases = [query["id"] for query in manifest["queries"] if all(next(case for case in item["cases"] if case["id"] == query["id"])["status"] == "ok" for item in available)]
        retained_cases = [query["id"] for query in manifest["queries"] if all(next(case for case in item["cases"] if case["id"] == query["id"])["status"] in {"ok", "directional"} for item in available)]
        if len(accepted_cases) == len(manifest["queries"]):
            completed.add(engine_id)
        elif retained_cases:
            partial.add(engine_id)
        engine = available[0]["engine"]
        builds.append({
            "engine": engine,
            "elapsed_nanos": [item["build"]["elapsed_nanos"] for item in available],
            "docs_per_second": [item["build"]["docs_per_second"] for item in available],
            "cpu": [item["build"]["cpu"] for item in available],
            "peak_rss": [item["build"]["peak_rss"] for item in available],
            "durable_bytes": [item["storage"]["durable_bytes"] for item in available],
            "wal_bytes": [item["storage"]["wal_bytes"] for item in available],
            "transient_bytes": [item["storage"]["transient_bytes"] for item in available],
            "commands": [item["command"] for item in available],
            "versions": available[0]["versions"],
            "config": available[0]["config"],
            "environment": available[0]["environment"],
        })
        for query in manifest["queries"]:
            cases = [next(case for case in item["cases"] if case["id"] == query["id"]) for item in available]
            statuses = {case["status"] for case in cases}
            _require(len(statuses) == 1, f"{engine_id} case {query['id']}: status differs across repetitions")
            if statuses == {"unsupported"}:
                reasons = sorted({case.get("unsupported_reason", "unsupported") for case in cases})
                ledger.append({"engine": engine, "case": query["id"], "status": "unsupported", "detail": "; ".join(reasons)})
                continue
            result_ids = cases[0]["result_ids"]
            result_digest_value = cases[0]["result_digest"]
            _require(all(case["result_ids"] == result_ids for case in cases), f"{engine_id} case {query['id']}: result IDs differ across repetitions")
            _require(all(case["result_digest"] == result_digest_value for case in cases), f"{engine_id} case {query['id']}: result digest differs across repetitions")
            samples = [sample for case in cases for sample in case["samples_nanos"]]
            row = {
                "engine": engine,
                "case": query["id"],
                "semantic": query["semantic"],
                "p50_nanos": percentile(samples, 0.50),
                "p95_nanos": percentile(samples, 0.95),
                "p99_nanos": percentile(samples, 0.99),
                "raw_samples_nanos": samples,
                "result_ids": result_ids,
                "result_digest": result_digest_value,
            }
            if statuses == {"directional"}:
                reasons = sorted({case["non_equivalent_reason"] for case in cases})
                row["non_equivalent_reason"] = "; ".join(reasons)
                directional_rows.append(row)
                ledger.append({"engine": engine, "case": query["id"], "status": "directional", "detail": row["non_equivalent_reason"]})
                continue
            _require(statuses == {"ok"}, f"{engine_id} case {query['id']}: invalid retained status")
            row["headline_eligible"] = source["qualification_eligible"]
            rows.append(row)
            ledger.append({"engine": engine, "case": query["id"], "status": "equivalent", "detail": "pinned scoring semantics, reference IDs/order, reopen, and intended route proven"})
    _require("treedb_text_v2" in completed, "consolidation requires a complete accepted TreeDB text-v2 engine")
    external_completed = completed - {"treedb_text_v2"}
    available_external = {build["engine"]["id"] for build in builds} - {"treedb_text_v2"}
    _require(external_completed, "consolidation requires at least one complete semantically equivalent external engine")
    _require(len(available_external) >= 2, "consolidation requires retained rows from at least two external engines")
    return {
        "schema_version": COMPARISON_SCHEMA,
        "manifest_sha256": manifest_sha256(manifest),
        "corpus": manifest["corpus"],
        "execution": manifest["execution"] | {"actual_retained_repetitions": repetitions},
        "context": context,
        "reference_results": expected,
        "engines_completed": sorted(completed),
        "engines_partial": sorted(partial),
        "qualification_eligible": source["qualification_eligible"],
        "builds": builds,
        "headline_rows": rows if source["qualification_eligible"] else [],
        "directional_rows": directional_rows,
        "equivalence_ledger": ledger,
    }


def format_resources(resources: list[dict[str, Any]], divisor: float, suffix: str) -> str:
    values = []
    for resource in resources:
        if resource["status"] == "ok":
            values.append(f"{resource['value'] / divisor:.3f} {suffix}")
        else:
            values.append(f"unsupported: {resource['reason']}")
    return ", ".join(values)


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Same-corpus lexical comparison",
        "",
        f"Schema: `{report['schema_version']}`. Manifest: `{report['manifest_sha256']}`. Corpus: `{report['corpus']['document_count']}` documents, `{report['corpus']['sha256']}`.",
        "",
        f"Source: `{report['context']['source']['commit']}` / tree `{report['context']['source']['tree_oid']}`; retained qualification eligible: **{str(report['qualification_eligible']).upper()}**.",
        "",
        f"Completed engines: `{', '.join(report['engines_completed'])}`; partial engines: `{', '.join(report['engines_partial']) or 'none'}`.",
        "",
        "Only pinned-scoring, validator-accepted rows enter the headline table. Native-scoring rows with different formulas are retained separately as directional context. Times are warm single-query latency on one host; they are not timing assertions.",
        "",
        "## Headline query latency",
        "",
        "| engine | case | p50 | p95 | p99 | result digest |",
        "| --- | --- | ---: | ---: | ---: | --- |",
    ]
    for row in report["headline_rows"]:
        lines.append(f"| {row['engine']['name']} | {row['case']} | {row['p50_nanos'] / 1e6:.3f} ms | {row['p95_nanos'] / 1e6:.3f} ms | {row['p99_nanos'] / 1e6:.3f} ms | `{row['result_digest']}` |")
    lines.extend(["", "## Directional native-scoring latency (not semantically equivalent)", "", "| engine | case | p50 | p95 | p99 | disposition |", "| --- | --- | ---: | ---: | ---: | --- |"])
    for row in report["directional_rows"]:
        reason = row["non_equivalent_reason"].replace("|", "\\|")
        lines.append(f"| {row['engine']['name']} | {row['case']} | {row['p50_nanos'] / 1e6:.3f} ms | {row['p95_nanos'] / 1e6:.3f} ms | {row['p99_nanos'] / 1e6:.3f} ms | {reason} |")
    lines.extend(["", "## Build resources and checkpointed storage", "", "| engine | build repetitions (s) | docs/s | CPU per repetition | peak RSS per repetition | durable bytes per repetition | WAL bytes per repetition | transient bytes per repetition |", "| --- | --- | --- | --- | --- | --- | --- | --- |"])
    for build in report["builds"]:
        elapsed = ", ".join(f"{value / 1e9:.3f}" for value in build["elapsed_nanos"])
        throughput = ", ".join(f"{value:.1f}" for value in build["docs_per_second"])
        cpu = format_resources(build["cpu"], 1e9, "s")
        rss = format_resources(build["peak_rss"], 1024 * 1024, "MiB")
        durable = ", ".join(str(value) for value in build["durable_bytes"])
        wal = ", ".join(str(value) for value in build["wal_bytes"])
        transient = ", ".join(str(value) for value in build["transient_bytes"])
        lines.append(f"| {build['engine']['name']} | {elapsed} | {throughput} | {cpu} | {rss} | {durable} | {wal} | {transient} |")
    lines.extend(["", "## Equivalence and availability ledger", "", "| engine | case | status | detail |", "| --- | --- | --- | --- |"])
    for item in report["equivalence_ledger"]:
        detail = item["detail"]
        if isinstance(detail, dict):
            command = " ".join(detail.get("setup_command", []))
            detail = f"{detail['kind']}: {detail['reason']}; setup: {command}"
        detail = str(detail).replace("|", "\\|").replace("\n", " ")
        lines.append(f"| {item['engine']['name']} | {item.get('case', 'all')} | {item['status']} | {detail} |")
    lines.extend(["", "## Exact commands, versions, and configuration", ""])
    for build in report["builds"]:
        lines.append(f"### {build['engine']['name']}")
        lines.append("")
        lines.append(f"- Versions: `{json.dumps(build['versions'], sort_keys=True)}`")
        lines.append(f"- Configuration: `{json.dumps(build['config'], sort_keys=True)}`")
        lines.append(f"- Environment: `{json.dumps(build['environment'], sort_keys=True)}`")
        for command in build["commands"]:
            lines.append(f"- Command: `{' '.join(command)}`")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"
