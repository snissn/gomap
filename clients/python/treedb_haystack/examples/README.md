# TreeDB Haystack examples

These examples require a running TreeDB document service and the local Python
packages installed in a virtual environment.

From the repository root:

```sh
python3 -m venv .venv-treedb-haystack
. .venv-treedb-haystack/bin/activate
python -m pip install -U pip
python -m pip install -e clients/python/treedb_client
python -m pip install -e clients/python/treedb_haystack

go run ./cmd/treedb-document-service \
  -dir /tmp/treedb-document-service \
  -addr 127.0.0.1:7120 \
  -profile command_wal_durable
```

In another shell with the same virtual environment:

```sh
python clients/python/treedb_haystack/examples/basic_ingest_retrieve.py
python clients/python/treedb_haystack/examples/code_search_metadata.py
```

Both scripts use small deterministic embeddings and exact TreeDB service-backed
dense retrieval. They do not exercise or claim keyword/hybrid search support.
Use `--base-url` and `--index` to point at a different service or isolate runs.
