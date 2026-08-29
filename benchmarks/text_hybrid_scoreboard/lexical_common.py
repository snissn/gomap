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
    terms: list[str] = []
    if index < 5000:
        terms.append("common")
    if index < 10:
        terms.append("rare")
    if 20 <= index < 30:
        terms.extend(("alpha", "beta"))
    elif 30 <= index < 40:
        terms.append("alpha")
    elif 40 <= index < 50:
        terms.append("beta")
    if 50 <= index < 60:
        terms.append("gamma")
    elif 60 <= index < 70:
        terms.append("delta")
    elif 70 <= index < 80:
        terms.extend(("gamma", "delta"))
    if 80 <= index < 90:
        terms.extend(("quick", "fox"))
    elif 90 <= index < 100:
        terms.extend(("quick", "bridge", "fox"))
    if 100 <= index < 120:
        terms.append("tenantterm")
    terms.extend(["filler"] * (12 - len(terms)))
    tenant = "tenant-rare" if 100 <= index < 110 else "tenant-broad"
    return f"doc-{index:06d}", "catalog entry", " ".join(terms), tenant


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


def reference_results(manifest: dict[str, Any], documents: list[dict[str, str]]) -> dict[str, list[str]]:
    weights = {field["name"]: float(field["weight"]) for field in manifest["analysis"]["fields"]}
    top_k = int(manifest["execution"]["top_k"])
    tokenized = {
        doc["id"]: {field: tokenize(doc[field]) for field in weights}
        for doc in documents
    }
    results: dict[str, list[str]] = {}
    for query in manifest["queries"]:
        terms = [normalize(term) for term in query["terms"]]
        ranked: list[tuple[float, str]] = []
        for doc in documents:
            if query.get("filter") and doc[query["filter"]["field"]] != query["filter"]["equals"]:
                continue
            fields = tokenized[doc["id"]]
            counts = [sum(tokens.count(term) for tokens in fields.values()) for term in terms]
            semantic = query["semantic"]
            if semantic in ("term", "term_scalar"):
                matched = counts[0] > 0
            elif semantic == "and":
                matched = all(counts)
            elif semantic == "or":
                matched = any(counts)
            elif semantic == "phrase":
                matched = any(
                    any(tokens[i : i + len(terms)] == terms for i in range(len(tokens) - len(terms) + 1))
                    for tokens in fields.values()
                )
            else:
                raise ValidationError(f"unknown query semantic {semantic!r}")
            if not matched:
                continue
            score = sum(weights[field] * sum(tokens.count(term) for term in terms) for field, tokens in fields.items())
            ranked.append((-score, doc["id"]))
        ranked.sort()
        results[query["id"]] = [doc_id for _, doc_id in ranked[:top_k]]
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


def validate_result(artifact: dict[str, Any], manifest: dict[str, Any], expected: dict[str, list[str]], corpus_ids: set[str]) -> None:
    engine_id = artifact.get("engine", {}).get("id", "<missing>")
    prefix = f"{engine_id} repetition {artifact.get('repetition', '?')}"
    _require(engine_id != "<missing>" and bool(artifact.get("engine", {}).get("name")) and bool(artifact.get("engine", {}).get("version")), f"{prefix}: engine identity/version missing")
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
    _require(isinstance(artifact.get("command"), list) and bool(artifact["command"]), f"{prefix}: exact command missing")
    _require(isinstance(artifact.get("versions"), dict) and bool(artifact["versions"]), f"{prefix}: versions missing")
    _require(isinstance(artifact.get("config"), dict) and bool(artifact["config"]), f"{prefix}: configuration missing")
    build = artifact.get("build", {})
    _require(build.get("checkpointed") is True, f"{prefix}: build was not checkpointed")
    _require(all(key in build for key in ("elapsed_nanos", "docs_per_second", "cpu_nanos", "peak_rss_bytes")), f"{prefix}: build/resource fields missing")
    storage = artifact.get("storage", {})
    _require(all(key in storage for key in ("durable_bytes", "wal_bytes", "transient_bytes")), f"{prefix}: storage classes missing")
    reopen = artifact.get("reopen", {})
    _require(reopen.get("performed") is True and reopen.get("verified") is True and bool(reopen.get("result_digest")), f"{prefix}: reopen proof missing")
    cases = artifact.get("cases", [])
    by_id = {case.get("id"): case for case in cases}
    _require(len(by_id) == len(cases), f"{prefix}: duplicate case IDs")
    _require(set(by_id) == set(expected), f"{prefix}: cases do not match manifest")
    measured = int(manifest["execution"]["measured_queries_per_case"])
    for query in manifest["queries"]:
        case = by_id[query["id"]]
        case_prefix = f"{prefix} case {query['id']}"
        if case.get("status") == "unsupported":
            _require(bool(case.get("unsupported_reason")), f"{case_prefix}: unsupported reason missing")
            _require(case.get("equivalent") is False, f"{case_prefix}: unsupported row marked equivalent")
            _require(not case.get("result_ids") and not case.get("samples_nanos"), f"{case_prefix}: unsupported row contains measured results")
            continue
        _require(case.get("status") == "ok", f"{case_prefix}: invalid status")
        _require(case.get("equivalent") is True, f"{case_prefix}: equivalent row not marked equivalent")
        ids = case.get("result_ids")
        _require(isinstance(ids, list), f"{case_prefix}: result IDs missing")
        _require(len(ids) == len(set(ids)), f"{case_prefix}: duplicate result IDs")
        _require(set(ids) <= corpus_ids, f"{case_prefix}: result leakage outside corpus")
        _require(ids == expected[query["id"]], f"{case_prefix}: reference result/order mismatch")
        _require(case.get("result_digest") == result_digest(ids), f"{case_prefix}: result digest mismatch")
        _require(case.get("reopen_result_ids") == ids, f"{case_prefix}: reopen result mismatch")
        _require(case.get("reopen_result_digest") == case.get("result_digest"), f"{case_prefix}: reopen digest mismatch")
        samples = case.get("samples_nanos")
        _require(isinstance(samples, list) and len(samples) == measured, f"{case_prefix}: raw sample count mismatch")
        _require(all(isinstance(value, int) and value >= 0 for value in samples), f"{case_prefix}: invalid latency sample")
        route = case.get("route", {})
        _require(route.get("intended") is True and bool(route.get("name")), f"{case_prefix}: intended-route proof missing")
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
    expected = reference_results(manifest, documents)
    corpus_ids = {doc["id"] for doc in documents}
    for artifact in artifacts:
        validate_result(artifact, manifest, expected, corpus_ids)
        if artifact["engine"]["id"] == "treedb_text_v2":
            _require(artifact["engine"]["version"] == source["commit"], "TreeDB artifact is not bound to the source commit")
    grouped: dict[str, list[dict[str, Any]]] = {}
    for artifact in artifacts:
        grouped.setdefault(artifact["engine"]["id"], []).append(artifact)
    completed: set[str] = set()
    ledger = []
    rows = []
    builds = []
    for engine_id, engine_artifacts in sorted(grouped.items()):
        engine_artifacts.sort(key=lambda item: item["repetition"])
        available = [item for item in engine_artifacts if item["status"] == "ok"]
        unavailable = [item for item in engine_artifacts if item["status"] == "unavailable"]
        if unavailable:
            _require(not available, f"{engine_id}: mixed available and unavailable repetitions")
            _require(len(unavailable) == repetitions, f"{engine_id}: expected {repetitions} unavailable repetition records")
            _require([item["repetition"] for item in unavailable] == list(range(1, repetitions + 1)), f"{engine_id}: unavailable repetition sequence is incomplete")
            first = unavailable[0]
            ledger.append({"engine": first["engine"], "status": "unavailable", "detail": first["unavailable"]})
            continue
        _require(len(available) == repetitions, f"{engine_id}: expected {repetitions} retained repetitions")
        _require([item["repetition"] for item in available] == list(range(1, repetitions + 1)), f"{engine_id}: retained repetition sequence is incomplete")
        if any(all(next(case for case in item["cases"] if case["id"] == query["id"])["status"] == "ok" for item in available) for query in manifest["queries"]):
            completed.add(engine_id)
        engine = available[0]["engine"]
        builds.append({
            "engine": engine,
            "elapsed_nanos": [item["build"]["elapsed_nanos"] for item in available],
            "docs_per_second": [item["build"]["docs_per_second"] for item in available],
            "durable_bytes": [item["storage"]["durable_bytes"] for item in available],
            "wal_bytes": [item["storage"]["wal_bytes"] for item in available],
            "transient_bytes": [item["storage"]["transient_bytes"] for item in available],
            "commands": [item["command"] for item in available],
            "versions": available[0]["versions"],
            "config": available[0]["config"],
        })
        for query in manifest["queries"]:
            cases = [next(case for case in item["cases"] if case["id"] == query["id"]) for item in available]
            statuses = {case["status"] for case in cases}
            if statuses != {"ok"}:
                reasons = sorted({case.get("unsupported_reason", "unsupported") for case in cases})
                ledger.append({"engine": engine, "case": query["id"], "status": "unsupported", "detail": "; ".join(reasons)})
                continue
            samples = [sample for case in cases for sample in case["samples_nanos"]]
            rows.append({
                "engine": engine,
                "case": query["id"],
                "semantic": query["semantic"],
                "headline_eligible": source["qualification_eligible"],
                "p50_nanos": percentile(samples, 0.50),
                "p95_nanos": percentile(samples, 0.95),
                "p99_nanos": percentile(samples, 0.99),
                "raw_samples_nanos": samples,
                "result_ids": cases[0]["result_ids"],
                "result_digest": cases[0]["result_digest"],
            })
            ledger.append({"engine": engine, "case": query["id"], "status": "equivalent", "detail": "reference IDs and order match; reopen and intended route proven"})
    _require("treedb_text_v2" in completed, "consolidation requires a complete accepted TreeDB text-v2 engine")
    external = completed - {"treedb_text_v2"}
    _require(len(external) >= 2, "consolidation requires at least two complete accepted external engines")
    return {
        "schema_version": COMPARISON_SCHEMA,
        "manifest_sha256": manifest_sha256(manifest),
        "corpus": manifest["corpus"],
        "execution": manifest["execution"] | {"actual_retained_repetitions": repetitions},
        "context": context,
        "reference_results": expected,
        "engines_completed": sorted(completed),
        "qualification_eligible": source["qualification_eligible"],
        "builds": builds,
        "headline_rows": rows,
        "equivalence_ledger": ledger,
    }


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Same-corpus lexical comparison",
        "",
        f"Schema: `{report['schema_version']}`. Manifest: `{report['manifest_sha256']}`. Corpus: `{report['corpus']['document_count']}` documents, `{report['corpus']['sha256']}`.",
        "",
        f"Source: `{report['context']['source']['commit']}` / tree `{report['context']['source']['tree_oid']}`; retained qualification eligible: **{str(report['qualification_eligible']).upper()}**.",
        "",
        "Only exact, validator-accepted rows enter the headline table. Times are warm single-query latency on one host; they are not timing assertions.",
        "",
        "## Headline query latency",
        "",
        "| engine | case | p50 | p95 | p99 | result digest |",
        "| --- | --- | ---: | ---: | ---: | --- |",
    ]
    for row in report["headline_rows"]:
        lines.append(f"| {row['engine']['name']} | {row['case']} | {row['p50_nanos'] / 1e6:.3f} ms | {row['p95_nanos'] / 1e6:.3f} ms | {row['p99_nanos'] / 1e6:.3f} ms | `{row['result_digest']}` |")
    lines.extend(["", "## Build and checkpointed storage", "", "| engine | build repetitions (s) | docs/s | durable bytes | WAL bytes | transient bytes |", "| --- | --- | --- | --- | --- | --- |"]) 
    for build in report["builds"]:
        elapsed = ", ".join(f"{value / 1e9:.3f}" for value in build["elapsed_nanos"])
        throughput = ", ".join(f"{value:.1f}" for value in build["docs_per_second"])
        lines.append(f"| {build['engine']['name']} | {elapsed} | {throughput} | {build['durable_bytes']} | {build['wal_bytes']} | {build['transient_bytes']} |")
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
        for command in build["commands"]:
            lines.append(f"- Command: `{' '.join(command)}`")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"
