# Vector DB Comparison

This benchmark compares persistent database-tier ANN search:

- TreeDB native persisted HNSW via `cmd/treedb_vector_search_demo`
- SQLite with the Vectorlite loadable extension, backed by hnswlib/HNSW

`sqlite-vec` is intentionally not used for the ANN comparison. Upstream
sqlite-vec `vec0` search is brute-force today, and ANN support is tracked as
future work. Vectorlite is the SQLite-native comparator used here because it
has a persisted HNSW index path.

Run:

```sh
scripts/bench_vector_db_compare.sh
```

Useful overrides:

```sh
RUN_DIR=/tmp/vector_db_compare \
DOCS=10000 \
DIMS=64 \
QUERIES=10000 \
VALIDATE_QUERIES=64 \
SEARCH_CONCURRENCY=2,4,8,16,32,64,128 \
scripts/bench_vector_db_compare.sh
```

The runner writes:

- `dataset/`: TreeDB-owned synthetic vectors and manifest
- `treedb.json`: TreeDB benchmark result
- `vectorlite.json`: SQLite+Vectorlite benchmark result
- `comparison.md`: normalized comparison table

The TreeDB dataset exporter writes row-major little-endian `float32` vector
files plus JSONL convenience files. TreeDB loads documents from the exported
JSONL and query vectors from the exported binary query file; the Python
Vectorlite benchmark consumes the exported binary vector files directly.

Record the generated `README.md`, `treedb.json`, and `vectorlite.json` with any
published result. The script records the git commit and run shape; reruns can
also override `PYTHON`, `VENV`, `NUMPY_PACKAGE`, and `VECTORLITE_PACKAGE` when
exact dependency reproduction requires a different pinned package set.
