# TreeDB VectorDBBench #2600 Baseline Attempt (2026-06-11)

Issue: `snissn/gomap#2600`. Parent tracker: `#2598`.
Predecessor harness: `#2599` / PR `#2605`, merged to `main` as
`7f66890467f96e7460ed6eeec29fdee81e61acac`.

## Status

The claim-quality/full `Performance1536D50K` TreeDB-only baseline is **blocked
on this host by load runtime**. The exact FP32 row did not complete VDBBench
load before VDBBench's 3600s case load timeout under the
`command_wal_durable` TreeDB profile. Per coordinator policy, the scalar_u8
rerank32 row was stopped after the exact row timed out.

A smaller bounded custom VDBBench smoke was captured to prove the harness,
TreeDB VDBBench rows, route guards, and artifact shape. The custom-smoke
numbers below are **not public claim-quality throughput evidence** and should
not be compared to native Go `B/op` / `allocs/op` rows.

## Measurement Boundaries

- VDBBench TreeDB rows include Python, VDBBench client, `treedb-client`, HTTP,
  JSON, TreeDB document service, and TreeDB vector-index work.
- Haystack/service `/search/vector` remains exact dense document scoring; ANN
  rows use `/search/vector-index`.
- The scalar row uses `query_mode="quantized_rerank"`, explicit
  `quantized_index_name="embedding.scalar_u8.fast"`, and
  `quantized_rerank_candidates=32`.
- No result in this note is native Go allocation evidence.
- No noisy/local-host number in this note is suitable for a public headline.

## Full `Performance1536D50K` Attempt: Blocked

Artifact root:

- `/tmp/treedb_vdbbench_2600_serial_20260610_224155`
- blocker marker: `/tmp/treedb_vdbbench_2600_serial_20260610_224155/FULL_RUN_BLOCKED.json`
- blocker note: `/tmp/treedb_vdbbench_2600_serial_20260610_224155/FULL_RUN_BLOCKED.md`

Caveat: a later cleanup command removed most of the original log files before it
was stopped. The original VDBBench log path was
`/tmp/treedb_vdbbench_2600_serial_20260610_224155/vdbbench.log`; the observed
blocker lines are preserved in `FULL_RUN_BLOCKED.json`.

Observed blocker:

```text
2026-06-10 23:42:05,397 | WARNING | VectorDB load dataset timeout in 3600
2026-06-10 23:42:05,583 | TreeDB ... load_dur=0.0 qps=0.0 recall=0.0 label=x
Coordinator stopped scalar row after exact Performance1536D50K hit VDBBench 3600s load timeout.
```

Full-run harness command:

```sh
python3 scripts/treedb_vectordbbench_artifact.py \
  --out /tmp/treedb_vdbbench_2600_serial_20260610_224155 \
  --vectordbbench-dir /Users/michaelseiler/dev/snissn/vectordbbench \
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
  --rerank-candidates 32 \
  --client-timeout 600 \
  --vdbbench-timeout 28800 \
  --db-label treedb-vdbbench-2600-serial-20260610_224155
```

Do **not** publish partial throughput, latency, or recall numbers from this full
attempt. It is evidence that the first full 50K durable TreeDB run needs either a
longer/dedicated host run or further load-path optimization before it can produce
claim-quality VDBBench rows.

## Bounded Custom VDBBench Smoke

Artifact root:

- `/tmp/treedb_vdbbench_2600_custom_smoke_20260610_235005`
- manifest: `/tmp/treedb_vdbbench_2600_custom_smoke_20260610_235005/manifest.json`
- route proof sidecar: `/tmp/treedb_vdbbench_2600_custom_smoke_20260610_235005/route_proof.json`
- VDBBench-index route proof: `/tmp/treedb_vdbbench_2600_custom_smoke_20260610_235005/route_proof_vdbbench_indexes.json`
- VDBBench log: `/tmp/treedb_vdbbench_2600_custom_smoke_20260610_235005/vdbbench.log`
- custom dataset manifest: `/tmp/treedb_vdbbench_2600_custom_smoke_20260610_235005/custom_dataset_manifest.json`

Custom dataset:

| field | value |
| --- | --- |
| dataset | `treedb2600smoke/treedb2600_smoke_512x1536_20260610_234950` |
| train rows | 512 |
| test rows | 64 |
| dimension | 1536 |
| metric | cosine |
| ground truth | deterministic top-10 cosine neighbors |
| source path | `/tmp/vectordb_bench/dataset/treedb2600smoke/treedb2600_smoke_512x1536_20260610_234950` |

Smoke harness command:

```sh
python3 scripts/treedb_vectordbbench_artifact.py \
  --out /tmp/treedb_vdbbench_2600_custom_smoke_20260610_235005 \
  --vectordbbench-dir /Users/michaelseiler/dev/snissn/vectordbbench \
  --run-tests required \
  --run-vdbbench \
  --rows exact,scalar \
  --case-type PerformanceCustomDataset \
  --k 10 \
  --num-concurrency 1,8 \
  --concurrency-duration 10 \
  --m 16 \
  --ef-construction 128 \
  --ef-search 128 \
  --rerank-candidates 32 \
  --client-timeout 60 \
  --vdbbench-timeout 1200 \
  --db-label treedb-vdbbench-2600-custom-smoke \
  --vdbbench-extra-args "--custom-case-name TreeDB2600CustomSmoke --custom-case-description 'TreeDB issue 2600 small custom smoke after Performance1536D50K load timeout' --custom-case-load-timeout 300 --custom-case-optimize-timeout 300 --custom-dataset-name treedb2600smoke --custom-dataset-dir treedb2600_smoke_512x1536_20260610_234950 --custom-dataset-size 512 --custom-dataset-dim 1536 --custom-dataset-metric-type COSINE --custom-dataset-file-count 1 --custom-dataset-with-gt"
```

Smoke results, as emitted by VDBBench result JSONs (latencies are seconds in the
artifact result files):

| row | label | load_s | insert_s | optimize_s | recall | NDCG | serial p95_s | serial p99_s | conc | QPS | conc p95_s | conc p99_s |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| exact FP32 | `:)` | 8.0109 | 7.2123 | 0.7986 | 1.0 | 1.0 | 0.0100 | 0.0115 | 1 | 230.0320 | 0.007268 | 0.011794 |
| exact FP32 | `:)` | 8.0109 | 7.2123 | 0.7986 | 1.0 | 1.0 | 0.0100 | 0.0115 | 8 | 714.7322 | 0.031774 | 0.069137 |
| scalar_u8 rerank32 | `:)` | 10.0787 | 9.2183 | 0.8604 | 1.0 | 1.0 | 0.0395 | 0.0564 | 1 | 67.9341 | 0.042236 | 0.087240 |
| scalar_u8 rerank32 | `:)` | 10.0787 | 9.2183 | 0.8604 | 1.0 | 1.0 | 0.0395 | 0.0564 | 8 | 145.4473 | 0.149379 | 0.274755 |

Route proof from actual VDBBench indexes:

| row | route | fallback | documents_fetched | quantized_scorer_active | rerank exact calls |
| --- | --- | --- | ---: | ---: | ---: |
| exact FP32 | `exact_hnsw_search_pack_v1` | `none` | 0 | 0 | 0 |
| scalar_u8 rerank32 | `quantized_rerank` | `none` | 0 | 1 | 32 |

The scalar rerank proof used `quantized_rerank_candidates=32` and observed
`quantized_rerank_exact_score_calls=32`, proving the rerank bound for the custom
smoke query.

## Context

From the custom smoke artifact:

- gomap commit: `ce243af9611c97f1e4aaaf2159c898b2d7dd233b` at run start
  (the docs PR was later rebased onto `origin/main` after #2605 merged as
  `7f66890467f96e7460ed6eeec29fdee81e61acac`).
- vectordbbench commit: `c1c763c0dcd97c489c2b107fd7060374f76d09b8`.
- Host: Apple M3, macOS Darwin arm64.
- Go: `go version go1.26.0 darwin/arm64`.
- Python: `3.14.0`.
- TreeDB profile: `command_wal_durable`.

## Validation

Captured in artifacts or run locally:

```sh
python3 scripts/treedb_vectordbbench_artifact.py --self-test
PYTHONPATH=scripts python3 -m unittest scripts/treedb_vectordbbench_artifact_test.py
go test ./TreeDB/documentservice ./cmd/treedb-document-service
# custom smoke harness above; included VDBBench TreeDB tests: 56 passed
git diff --check
```

## Handoff for #2602

Use the custom smoke only as functional evidence that exact/scalar TreeDB
VDBBench rows and route guards work. Treat the full `Performance1536D50K`
TreeDB baseline as blocked on durable load runtime for this host until rerun on a
quiet/dedicated host or after load-path optimization. Do not fold the smoke
numbers into a public comparison matrix.
