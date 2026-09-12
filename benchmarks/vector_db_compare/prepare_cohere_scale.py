#!/usr/bin/env python3
"""Export an existing Cohere parquet subset and independent exact cosine truth.

No download, synthetic vectors, dimensional projection, or dataset mutation.
Requires the benchmark environment's existing numpy and pyarrow packages.
"""
import argparse
import hashlib
import json
import math
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq


def export(source, dest, rows, excluded=frozenset()):
    digest = hashlib.sha256()
    written = 0
    with dest.open("xb") as out:
        for batch in pq.ParquetFile(source).iter_batches(batch_size=4096, columns=["emb"]):
            values = np.asarray(batch.column(0).to_pylist(), dtype="<f4")[: rows - written]
            if values.ndim != 2 or values.shape[1] != 768 or not np.isfinite(values).all():
                raise ValueError("expected finite 768D embeddings")
            if np.any(~np.any(values != 0, axis=1)):
                raise ValueError("cosine truth requires nonzero vectors")
            if excluded and any(hashlib.sha256(row.tobytes()).digest() in excluded for row in values):
                raise ValueError("query vector also occurs in train subset; choose another held-out query slice")
            raw = values.tobytes()
            out.write(raw)
            digest.update(raw)
            written += len(values)
            if written == rows:
                break
    if written != rows:
        raise ValueError(f"wanted {rows} vectors, found {written}")
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--train", required=True, type=Path)
    parser.add_argument("--queries", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--rows", type=int, default=500000)
    parser.add_argument("--query-count", type=int, default=100)
    args = parser.parse_args()
    if not 4097 <= args.rows <= 1000000 or not 1 <= args.query_count <= 10000 or math.gcd(args.rows, 7919) != 1:
        parser.error("need 4097..1000000 rows coprime to7919 and 1..10000 queries")
    args.out.mkdir(parents=True, exist_ok=False)
    manifest = dict(rows=args.rows, dimensions=768, top_k=10, query_count=args.query_count,
                    dataset="First rows of caller-supplied 768D train parquet; source origin/split must be attested separately",
                    train_source=str(args.train.resolve()), query_source=str(args.queries.resolve()),
                    filter="rank=(row*7919)%rows; six-digit lexical user range",
                    truth="float64 cosine exhaustive scan; query/train float32 byte overlap rejected")
    manifest["exporter_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    query_hashes = frozenset()
    for name, source, count in [("queries", args.queries, args.query_count), ("documents", args.train, args.rows)]:
        manifest[name + "_sha256"] = export(source, args.out / (name + ".f32"), count, query_hashes)
        if name == "queries":
            q = np.fromfile(args.out / "queries.f32", dtype="<f4").reshape(-1, 768)
            query_hashes = frozenset(hashlib.sha256(row.tobytes()).digest() for row in q)
        print(f"exported {name}: {count} x 768", flush=True)
    manifest["exact_train_query_overlap"] = 0
    vectors = np.memmap(args.out / "documents.f32", dtype="<f4", mode="r", shape=(args.rows, 768))
    queries = np.asarray(np.memmap(args.out / "queries.f32", dtype="<f4", mode="r", shape=(args.query_count, 768)), dtype=np.float64)
    queries /= np.linalg.norm(queries, axis=1)[:, None]
    counts = sorted(set([4096, 4097, max(4097, args.rows // 100), max(4097, args.rows // 10), args.rows]))
    truth = {str(count): [([], []) for _ in queries] for count in counts}
    for start in range(0, args.rows, 4096):
        chunk = np.asarray(vectors[start:start + 4096], dtype=np.float64)
        chunk /= np.linalg.norm(chunk, axis=1)[:, None]
        scores = queries @ chunk.T
        rows = np.arange(start, start + len(chunk))
        ranks = (rows * 7919) % args.rows
        for count in counts:
            mask = ranks < count
            selected = rows[mask]
            if not len(selected):
                continue
            for q, values in enumerate(scores[:, mask]):
                old_rows, old_values = truth[str(count)][q]
                all_rows = np.concatenate((old_rows, selected)).astype(np.int64)
                all_values = np.concatenate((old_values, values))
                best = np.lexsort((all_rows, -all_values))[:10]
                truth[str(count)][q] = (all_rows[best], all_values[best])
    truth = {count: [[f"row-{int(row):06d}" for row in rows] for rows, _ in cases] for count, cases in truth.items()}
    (args.out / "truth.json").write_text(json.dumps(truth, sort_keys=True) + "\n")
    manifest["truth_sha256"] = hashlib.sha256((args.out / "truth.json").read_bytes()).hexdigest()
    (args.out / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(json.dumps(manifest), flush=True)


if __name__ == "__main__":
    main()
