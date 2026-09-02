#!/usr/bin/env python3
"""Regenerate the checked-in #4289 MiniLM vectors; not used at benchmark runtime."""

import argparse
import json
from pathlib import Path

from sentence_transformers import SentenceTransformer

MODEL = "sentence-transformers/all-MiniLM-L6-v2"
REVISION = "1110a243fdf4706b3f48f1d95db1a4f5529b4d41"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--inputs", type=Path, default=Path(__file__).with_name("semantic_inputs.json"))
    parser.add_argument("--output", type=Path, default=Path(__file__).with_name("semantic_vectors.json"))
    args = parser.parse_args()

    manifest = json.loads(args.inputs.read_text())
    model = SentenceTransformer(MODEL, revision=REVISION)
    texts = [item["text"] for item in manifest["inputs"]]
    encoded = model.encode(texts, normalize_embeddings=True, convert_to_numpy=True)
    vectors: dict[str, list[float]] = {}
    queries: dict[str, list[float]] = {}
    for item, vector in zip(manifest["inputs"], encoded, strict=True):
        values = [float(value) for value in vector]
        if item["kind"] == "query":
            queries[item["id"]] = values
        else:
            vectors[item["text"]] = values

    bundle = {
        "schema": "treedb-rag-semantic-vectors/v1",
        "model": MODEL,
        "revision": REVISION,
        "license": "Apache-2.0",
        "preprocessing": "SentenceTransformer.encode(normalize_embeddings=True); model tokenizer; max_seq_length=256",
        "dimensions": 384,
        "corpus_license": "MIT (gomap repository fixture)",
        "generation_command": "python3 TreeDB/cmd/treedb_rag_benchmark/testdata/generate_semantic_vectors.py --inputs TreeDB/cmd/treedb_rag_benchmark/testdata/semantic_inputs.json --output TreeDB/cmd/treedb_rag_benchmark/testdata/semantic_vectors.json (sentence-transformers==5.4.1, transformers==5.8.0, torch==2.11.0)",
        "vectors": vectors,
        "queries": queries,
    }
    args.output.write_text(json.dumps(bundle, sort_keys=True, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    main()
