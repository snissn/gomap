# TreeDB VectorDBBench Artifact Harness

Issues: `snissn/gomap#2599`, `#4181`, `#4193`. Parent tracker: `#2598`.

This harness creates a repeatable TreeDB VectorDBBench artifact root. It starts
`treedb-document-service` with a fresh artifact-owned data directory, captures
service logs and host/version context, runs a focused no-document route-proof
smoke, and can optionally run selected TreeDB VectorDBBench rows from a local
`snissn/vectordbbench` checkout.

The artifact is a reproducibility contract for downstream benchmark issues. A
smoke artifact is **not** public claim-quality throughput evidence.

## Quick smoke

```sh
OUT=/tmp/treedb_vdbbench_artifact_$(date +%Y%m%d_%H%M%S)
python3 scripts/treedb_vectordbbench_artifact.py \
  --out "$OUT" \
  --run-tests off
```

Expected primary files:

- `manifest.json` — artifact schema, commands, gomap/vectordbbench commits when
  available, Python/Go/OS context, service command, health response, skips.
- `route_proof.json` — stable sidecar proving exact/scalar TreeDB routes and
  counters.
- `health.json` — `GET /v1/health` response.
- `service.log` — TreeDB document-service stdout/stderr.
- `commands/*.stdout.txt`, `commands/*.stderr.txt` — command output streams.
- `vdbbench-results/` and `vdbbench.log` — VectorDBBench result JSON/logs when
  `--run-vdbbench` is enabled.
- `vdbbench_load_metrics.json` — checksum-identified canonical VDBBench result
  metrics for each completed load row.

The harness requires `--out` to be new or empty, then creates a fresh
`treedb-data` directory under that artifact root. It does not truncate durable
collections or modify WAL/storage semantics.

## Route-proof sidecar contract

`route_proof.json` uses schema
`treedb-vectordbbench-route-proof/v2` and contains:

- `exact_fp32.route == "exact_hnsw_search_pack_v1"`
- `exact_fp32.documents_fetched == 0`
- `exact_fp32.fallback_reason == "none"`
- for dimensions `>=32`, both rows report optimized score batches greater than
  zero and fallback batches equal to zero
- `scalar_u8_rerank.route == "quantized_rerank"`
- `scalar_u8_rerank.quantized_scorer_active == 1`
- `scalar_u8_rerank.documents_fetched == 0`
- `smoke_top_k <= scalar_u8_rerank.quantized_rerank_exact_score_calls <= scalar_u8_rerank.response.quantized_rerank_candidates`
- `assertions[]` and top-level `passed` for machine-readable gating.

The default remains the historical tiny `4 x 2`, `topK=2` smoke. Set only the
three route-proof shape knobs when validating a campaign shape. The harness
rejects document count, efSearch, or rerank candidates below `topK` before it
starts the service.

For the Cohere 1M campaign shape:

```sh
python3 scripts/treedb_vectordbbench_artifact.py \
  --out "$OUT" \
  --run-tests off \
  --smoke-dimension 768 \
  --smoke-documents 256 \
  --smoke-top-k 100 \
  --ef-search 192 \
  --rerank-candidates 150
```

## VectorDBBench checkout

Use a checkout at or after the merged TreeDB CLI/client stack:

```sh
export VECTORDBBENCH_DIR=/path/to/snissn/vectordbbench
export PYTHONPATH="$VECTORDBBENCH_DIR:$PWD/clients/python/treedb_client/src${PYTHONPATH:+:$PYTHONPATH}"
```

Run the merged TreeDB VectorDBBench tests through the harness. When `uv` is
available, the harness uses `uv run --no-sync --with ... python -m ...` for the
Python dependencies (set `UV_PYTHON` if you need to pin uv's interpreter);
otherwise it falls back to `python -m pytest`.

```sh
python3 scripts/treedb_vectordbbench_artifact.py \
  --out "$OUT" \
  --vectordbbench-dir "$VECTORDBBENCH_DIR" \
  --run-tests required
```

If the checkout or Python dependencies are unavailable, set `--run-tests off`
and record the skip reason in the PR/issue evidence. Do not imply the tests ran.

## Run selected TreeDB VDBBench rows

For a dry-run that captures the exact commands without loading/searching:

```sh
python3 scripts/treedb_vectordbbench_artifact.py \
  --out "$OUT" \
  --vectordbbench-dir "$VECTORDBBENCH_DIR" \
  --run-tests required \
  --run-vdbbench \
  --vdbbench-dry-run
```

For downstream baseline/comparator issues, remove `--vdbbench-dry-run` and keep
case/shape knobs explicit:

```sh
python3 scripts/treedb_vectordbbench_artifact.py \
  --out "$OUT" \
  --vectordbbench-dir "$VECTORDBBENCH_DIR" \
  --run-tests required \
  --run-vdbbench \
  --rows exact,scalar \
  --case-type Performance1536D50K \
  --k 10 \
  --num-concurrency 1,8,32 \
  --concurrency-duration 30 \
  --m 16 \
  --ef-construction 128 \
  --ef-search 128 \
  --rerank-candidates 32
```

The generated VDBBench commands use unique index names derived from
`--index-prefix` (or a timestamped default) and set `RESULTS_LOCAL_DIR` to
`$OUT/vdbbench-results` plus `LOG_FILE` to `$OUT/vdbbench.log`. The exact row is
`treedbcolumngraphexact`; the scalar row is `treedbscalaru8rerank` with
`query_mode="quantized_rerank"`, `quantized_index_name="embedding.scalar_u8.fast"`,
and `quantized_rerank_candidates=32`. TreeDB rows also receive
`NUM_PER_BATCH=1000` by default; override only this harness with
`--num-per-batch` or `TREEDB_VDBBENCH_NUM_PER_BATCH`. The selected value is
recorded in the manifest, README, and each VDBBench row record. CLI and
environment values must be positive integers; zero and negative values are
rejected before service startup.

For a completed load, the harness selects exactly one *new* canonical
`result_*.json` matching that generated index name. It records the result path,
SHA-256, run ID, task configuration, insert duration, offline optimize duration,
total load duration, and `vector_count / insert_duration` in
`vdbbench_load_metrics.json`. It fails closed if selection is ambiguous, a
duration is absent/non-positive, the three durations disagree, or the case type
does not end in a count suffix such as `50K` or `1M`. The profile is deliberately
full-load only: phase-specific pprof would require VDBBench orchestration not
owned by this harness.

For `PerformanceCustomDataset`, the vector count instead comes fail-closed from
the selected result's `task_config.case_config.custom_case.dataset_config.size`.

## Environment variables

Most flags also have environment equivalents:

| variable | meaning | default |
| --- | --- | --- |
| `TREEDB_VDBBENCH_OUT` | artifact root | `/tmp/treedb_vdbbench_artifact_*` |
| `VECTORDBBENCH_DIR` | VectorDBBench checkout | unset |
| `TREEDB_VDBBENCH_HOST` / `TREEDB_VDBBENCH_PORT` | service bind address | `127.0.0.1` / free port |
| `TREEDB_VDBBENCH_PROFILE` | TreeDB service profile | `command_wal_durable` |
| `TREEDB_VDBBENCH_RUN_TESTS` | `off`, `auto`, or `required` | `auto` |
| `TREEDB_VDBBENCH_RUN_VDBBENCH` | run selected VDBBench rows | `false` |
| `TREEDB_VDBBENCH_ROWS` | row list (`exact,scalar`) | `exact,scalar` |
| `TREEDB_VDBBENCH_DRY_RUN` | add VDBBench `--dry-run` and skip stages | `false` |
| `TREEDB_VDBBENCH_CASE_TYPE` | VDBBench case | `Performance1536D50K` |
| `TREEDB_VDBBENCH_K` | topK | `10` |
| `TREEDB_VDBBENCH_NUM_CONCURRENCY` | concurrency list | `1,8,32` |
| `TREEDB_VDBBENCH_CONCURRENCY_DURATION` | seconds per concurrent search | `30` |
| `TREEDB_VDBBENCH_M` | HNSW M | `16` |
| `TREEDB_VDBBENCH_EF_CONSTRUCTION` | HNSW efConstruction | `128` |
| `TREEDB_VDBBENCH_EF_SEARCH` | HNSW efSearch | `128` |
| `TREEDB_VDBBENCH_QUANTIZED_INDEX_NAME` | scalar score-plane name | `embedding.scalar_u8.fast` |
| `TREEDB_VDBBENCH_RERANK_CANDIDATES` | scalar rerank shortlist | `32` |
| `TREEDB_VDBBENCH_NUM_PER_BATCH` | TreeDB VDBBench load batch size | `1000` |
| `TREEDB_VDBBENCH_SMOKE_DIMENSION` | route-proof vector dimensions | `2` |
| `TREEDB_VDBBENCH_SMOKE_DOCUMENTS` | route-proof document count | `4` |
| `TREEDB_VDBBENCH_SMOKE_TOP_K` | route-proof topK | `2` |
| `TREEDB_VDBBENCH_EXTRA_ARGS` | appended to each VDBBench row command | unset |
| `TREEDB_VDBBENCH_USE_UV` | `auto`, `on`, or `off` for VDBBench Python commands | `auto` |
| `TREEDB_VDBBENCH_TEST_CMD` | override VDBBench test command | auto `uv run ... python -m pytest ...` or `python -m pytest ...` |

## Reporting boundaries

When posting artifacts or reports:

- VDBBench TreeDB rows include Python/client/HTTP/service overhead and must not
  be presented as native Go `B/op` or `allocs/op` evidence.
- Haystack/service `/search/vector` remains exact dense document scoring, not
  ANN. ANN/no-document rows use `/search/vector-index`.
- Do not promote RaBitQ v1 as exact-like recall; this harness only owns exact
  FP32 and scalar_u8 rerank32 route proof.
- Do not publish noisy/local smoke numbers as public claim-quality results.
