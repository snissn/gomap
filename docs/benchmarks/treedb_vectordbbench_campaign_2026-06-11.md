# TreeDB VectorDBBench Campaign Attempt (2026-06-11)

Issue: `snissn/gomap#2602`. Parent tracker: `#2598`.
Base for this closeout branch: `origin/main` at
`d8c2442adec53711fa2e69c78a4df06dc6512a17`.

## Status: blocked / incomplete, no comparative claims

This campaign did **not** produce a claim-quality expanded VectorDBBench matrix.
The full TreeDB `Performance1536D50K` durable baseline did not complete load on
this host, and the full Docker/pgvector comparator did not produce a valid row.
This report therefore records the attempt, exact artifacts, blockers, and rerun
guidance. It must not be used for README headline performance claims or
TreeDB-vs-pgvector full-matrix comparisons.

The only successful numbers below are from small `512 x 1536` custom smokes for
TreeDB and local PostgreSQL/pgvector. They are labeled as functional
harness/route/setup proof only.

## Issue / PR stack state

| issue | PR | state | artifact(s) | report use |
| --- | --- | --- | --- | --- |
| `#2599` harness / route-proof sidecar | `#2605` merged as `7f66890467f96e7460ed6eeec29fdee81e61acac` | Complete. Harness contract merged. | `/tmp/treedb_vdbbench_2599_final_20260610_221336` | Foundation only: proves artifact capture and route-proof schema. |
| `#2600` TreeDB-only baseline | `#2608` merged as `d8c2442adec53711fa2e69c78a4df06dc6512a17` | Full `Performance1536D50K` blocked by TreeDB durable load runtime; smaller custom smoke completed. | `/tmp/treedb_vdbbench_2600_serial_20260610_224155`; `/tmp/treedb_vdbbench_2600_custom_smoke_20260610_235005` | Blocker evidence plus smoke-only appendix. No full baseline row. |
| `#2601` server comparator rows | `#2607` updated after the blocked report | Docker pgvector setup/dry-run passed; full Docker pgvector load failed with `pg_wal` no-space; local PostgreSQL/pgvector custom smoke completed. | `/tmp/treedb_vdbbench_2601_pgvector_setup_script_20260610_225302`; `/tmp/treedb_vdbbench_2601_pgvector_20260610_223212`; `/tmp/treedb_vdbbench_2601_pgvector_full_20260610_234701`; `/tmp/treedb_vdbbench_2601_pgvector_local_custom_20260611_014219` | Full-row blocker evidence plus custom-smoke comparator row. No full comparator row. |
| `#2602` final/expanded matrix report | `#2609` from `snissn/2602-manager` | Interim closeout for a blocked campaign. | `docs/benchmarks/treedb_vectordbbench_campaign_2026-06-11.md` | Documents incomplete status and rerun plan. |

## Matrix rows requested vs. captured

| lane | requested shape | outcome | publishable comparative row? |
| --- | --- | --- | --- |
| TreeDB exact FP32 | `Performance1536D50K`, COSINE, `k=10`, concurrency `1,8,32`, `30s`, `m=16`, `efConstruction=128`, `efSearch=128`, `command_wal_durable` | Exact row hit VDBBench's `3600s` load timeout. | No. |
| TreeDB scalar_u8 rerank32 | Same case, `quantized_rerank_candidates=32` | Intentionally stopped after the exact row timed out. | No. |
| pgvector HNSW | Same case and HNSW parameters through PostgreSQL/pgvector | Docker setup/dry-run passed; full Docker load failed with `pg_wal/xlogtemp.156`: no space left on device. | No. |
| pgvector HNSW local custom smoke | `PerformanceCustomDataset`, `512 x 1536`, COSINE, `k=10`, concurrency `1,8`, `10s`, local PostgreSQL/pgvector | Completed with result label `:)`. | Smoke-only; not a full comparative row. |
| Qdrant/server comparators | Optional after pgvector | Not run. | No. |
| RaBitQ | Optional experimental TreeDB lane | Not run in this campaign. RaBitQ remains experimental-only. | No. |

## Shared measurement boundaries and caveats

- VectorDBBench rows include Python, VDBBench client, `treedb-client`, HTTP,
  JSON, the TreeDB document service, and engine work. They are **not** native Go
  allocation or `B/op` / `allocs/op` evidence.
- Haystack/service `/search/vector` remains an exact dense-document route and is
  separate from `/search/vector-index` ANN/vector-index lanes.
- USearch/Faiss-style rows are in-memory library comparator lanes and must not
  be mixed with server/database rows.
- pgvector rows are PostgreSQL/server comparator lanes and include database and
  container/service behavior.
- TreeDB exact FP32, scalar_u8 rerank32, and RaBitQ are separate lanes. RaBitQ is
  experimental only and should not be promoted as exact-like evidence.
- No public README or TreeDB README headline number should be added from this
  campaign attempt.

## `#2600` TreeDB `Performance1536D50K` attempt: blocked

Artifact root and markers:

- `/tmp/treedb_vdbbench_2600_serial_20260610_224155`
- `/tmp/treedb_vdbbench_2600_serial_20260610_224155/FULL_RUN_BLOCKED.json`
- `/tmp/treedb_vdbbench_2600_serial_20260610_224155/FULL_RUN_BLOCKED.md`

The marker records schema
`treedb-vectordbbench-blocker-marker/v1`, profile `command_wal_durable`, case
`Performance1536D50K`, `k=10`, concurrency `1,8,32`, `30s`, HNSW `m=16`,
`ef_construction=128`, `ef_search=128`, and scalar rerank candidates `32`.

Observed blocker excerpts preserved in `FULL_RUN_BLOCKED.json`:

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

Do not report partial load, latency, QPS, recall, or NDCG values from this
artifact. A cleanup command removed most original log files; the blocker marker
is the durable summary.

## `#2600` TreeDB custom smoke: functional only, not claim-quality

Artifact root:

- `/tmp/treedb_vdbbench_2600_custom_smoke_20260610_235005`
- manifest: `/tmp/treedb_vdbbench_2600_custom_smoke_20260610_235005/manifest.json`
- custom dataset manifest: `/tmp/treedb_vdbbench_2600_custom_smoke_20260610_235005/custom_dataset_manifest.json`
- route proof: `/tmp/treedb_vdbbench_2600_custom_smoke_20260610_235005/route_proof.json`
- VDBBench-index route proof: `/tmp/treedb_vdbbench_2600_custom_smoke_20260610_235005/route_proof_vdbbench_indexes.json`
- VDBBench log: `/tmp/treedb_vdbbench_2600_custom_smoke_20260610_235005/vdbbench.log`

Sanitized context from the artifact:

| field | value |
| --- | --- |
| gomap commit at run start | `ce243af9611c97f1e4aaaf2159c898b2d7dd233b` (`snissn/2600-manager`, dirty only for `scripts/__pycache__/`) |
| vectordbbench commit | `c1c763c0dcd97c489c2b107fd7060374f76d09b8` |
| host class | Apple M3, macOS/Darwin arm64 |
| Go | `go version go1.26.0 darwin/arm64` |
| Python | `3.14.0` |
| TreeDB service profile | `command_wal_durable` |
| VDBBench TreeDB tests | `56 passed` |

Custom dataset:

| field | value |
| --- | --- |
| dataset | `treedb2600smoke/treedb2600_smoke_512x1536_20260610_234950` |
| train rows | 512 |
| test rows | 64 |
| dimension | 1536 |
| metric | cosine |
| topK | 10 |
| concurrency | `1,8` for `10s` each |
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

Smoke results as emitted by VDBBench result JSONs. Latencies are seconds in the
artifact result files. These rows are **functional smoke only** and must not be
used as public or comparative throughput evidence.

| row | label | load_s | insert_s | optimize_s | recall | NDCG | serial p95_s | serial p99_s | conc | QPS | conc p95_s | conc p99_s |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| exact FP32 | `:)` | 8.0109 | 7.2123 | 0.7986 | 1.0 | 1.0 | 0.0100 | 0.0115 | 1 | 230.0320 | 0.007268 | 0.011794 |
| exact FP32 | `:)` | 8.0109 | 7.2123 | 0.7986 | 1.0 | 1.0 | 0.0100 | 0.0115 | 8 | 714.7322 | 0.031774 | 0.069137 |
| scalar_u8 rerank32 | `:)` | 10.0787 | 9.2183 | 0.8604 | 1.0 | 1.0 | 0.0395 | 0.0564 | 1 | 67.9341 | 0.042236 | 0.087240 |
| scalar_u8 rerank32 | `:)` | 10.0787 | 9.2183 | 0.8604 | 1.0 | 1.0 | 0.0395 | 0.0564 | 8 | 145.4473 | 0.149379 | 0.274755 |

### Custom smoke route/counter proof

`route_proof_vdbbench_indexes.json` passed all assertions against the actual
VDBBench-created indexes:

| row | route | fallback | documents_fetched | quantized_scorer_active | quantized_score_calls | rerank exact calls |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| exact FP32 | `exact_hnsw_search_pack_v1` | `none` | 0 | 0 | 0 | 0 |
| scalar_u8 rerank32 | `quantized_rerank` | `none` | 0 | 1 | 548 | 32 |

The scalar row requested `quantized_rerank_candidates=32` and observed
`quantized_rerank_exact_score_calls=32`, proving the rerank bound for this smoke
query. The independent smoke fixture route proof in `route_proof.json` also
passed, with four fixture documents and `4 <= 32` exact rerank calls.

## `#2601` pgvector comparator lane: setup validated, full row blocked

### Setup smoke artifact

Artifact root:

- `/tmp/treedb_vdbbench_2601_pgvector_setup_script_20260610_225302`
- status manifest: `/tmp/treedb_vdbbench_2601_pgvector_setup_script_20260610_225302/status_manifest.json`
- context: `/tmp/treedb_vdbbench_2601_pgvector_setup_script_20260610_225302/context.txt`
- dry-run command: `/tmp/treedb_vdbbench_2601_pgvector_setup_script_20260610_225302/commands/vdbbench_pgvectorhnsw_dry_run.command.txt`
- VDBBench log: `/tmp/treedb_vdbbench_2601_pgvector_setup_script_20260610_225302/vdbbench_pgvectorhnsw.log`

Sanitized context:

| field | value |
| --- | --- |
| gomap commit | `ce243af9611c97f1e4aaaf2159c898b2d7dd233b` (`snissn/2601-manager`) |
| vectordbbench commit | `c1c763c0dcd97c489c2b107fd7060374f76d09b8` |
| Docker | `Docker version 27.5.1, build 9f9e405` |
| Python / uv | `Python 3.14.0`; `uv 0.9.18` |
| host class | Apple M3, macOS/Darwin arm64 |
| image | `pgvector/pgvector:pg16` |
| PostgreSQL / pgvector | PostgreSQL `16.14`; `vector=0.8.2` |
| VDBBench shape | `Performance1536D50K`, COSINE, `k=10`, concurrency `1,8,32`, `30s`, HNSW `m=16`, `ef_construction=128`, `ef_search=128` |
| result | setup smoke complete; dry-run exit `0`; no full benchmark |

Captured dry-run command, with local password redacted by the artifact harness:

```sh
uv run --no-sync --with click --with pydantic --with pyyaml --with environs --with pandas --with polars --with pyarrow --with psutil --with pytz --with tqdm --with plotly --with ujson --with hdrhistogram --with scikit-learn --with s3fs --with oss2 --with psycopg --with psycopg-binary --with pgvector python -m vectordb_bench.cli.vectordbbench pgvectorhnsw --user-name postgres --password <redacted-artifact-local-password> --host 127.0.0.1 --port 55322 --db-name vectordb --m 16 --ef-construction 128 --ef-search 128 --case-type Performance1536D50K --k 10 --num-concurrency 1\,8\,32 --concurrency-duration 30 --db-label pgvector-hnsw-2601-20260610_225302 --task-label pgvector-hnsw-2601-20260610_225302 --skip-load --skip-search-serial --skip-search-concurrent --dry-run
```

### Aborted/contention-tainted full attempt

Artifact root:

- `/tmp/treedb_vdbbench_2601_pgvector_20260610_223212`
- marker: `/tmp/treedb_vdbbench_2601_pgvector_20260610_223212/ABORTED_CONTENTION_TAINTED.md`
- manifest: `/tmp/treedb_vdbbench_2601_pgvector_20260610_223212/abort_manifest.json`

This attempt was intentionally stopped because it overlapped with the #2600
TreeDB full baseline. Its logs and failed/zero result JSON are contention-tainted
and must not be used for pgvector load, index, latency, QPS, recall, or NDCG.

### No-concurrent full pgvector attempt: failed no-space

Artifact root:

- `/tmp/treedb_vdbbench_2601_pgvector_full_20260610_234701`
- marker: `/tmp/treedb_vdbbench_2601_pgvector_full_20260610_234701/FAILED_NO_SPACE.md`
- failure manifest: `/tmp/treedb_vdbbench_2601_pgvector_full_20260610_234701/failure_manifest.json`
- status manifest: `/tmp/treedb_vdbbench_2601_pgvector_full_20260610_234701/status_manifest.json`
- failed result JSON: `/tmp/treedb_vdbbench_2601_pgvector_full_20260610_234701/vdbbench-results/PgVector/result_20260610_pgvector-hnsw-2601-20260610_234701_pgvector.json`
- VDBBench log: `/tmp/treedb_vdbbench_2601_pgvector_full_20260610_234701/vdbbench_pgvectorhnsw.log`

Sanitized context:

| field | value |
| --- | --- |
| gomap commit | `56359e2951a4697d1256bff0691d7faab5910444` (`snissn/2601-manager`) |
| vectordbbench commit | `c1c763c0dcd97c489c2b107fd7060374f76d09b8` |
| Docker | `Docker version 27.5.1, build 9f9e405` |
| Python / uv | `Python 3.14.0`; `uv 0.9.18` |
| host class | Apple M3, macOS/Darwin arm64 |
| image | `pgvector/pgvector:pg16` |
| PostgreSQL / pgvector | PostgreSQL `16.14`; `vector=0.8.2`; `shared_buffers=128MB`; `maintenance_work_mem=64MB`; `max_parallel_workers=8` |
| requested shape | `Performance1536D50K`, OpenAI-SMALL-50K, 1536 dimensions, COSINE, `k=10`, concurrency `1,8,32`, `30s`, HNSW `m=16`, `ef_construction=128`, `ef_search=128` |
| blocker | load/insert failed with PostgreSQL `could not write to file "pg_wal/xlogtemp.156": No space left on device` |

Captured Docker start command:

```sh
docker run -d --name gomap2601-pgvector-20260610_234701 -e POSTGRES_PASSWORD=<redacted> -e POSTGRES_DB=vectordb -p 127.0.0.1:60856:5432 -v /tmp/treedb_vdbbench_2601_pgvector_full_20260610_234701/pgdata:/var/lib/postgresql/data pgvector/pgvector:pg16
```

Captured full VDBBench command, with local password redacted by the artifact
harness:

```sh
uv run --no-sync --with click --with pydantic --with pyyaml --with environs --with pandas --with polars --with pyarrow --with psutil --with pytz --with tqdm --with plotly --with ujson --with hdrhistogram --with scikit-learn --with s3fs --with oss2 --with psycopg --with psycopg-binary --with pgvector python -m vectordb_bench.cli.vectordbbench pgvectorhnsw --user-name postgres --password <redacted-artifact-local-password> --host 127.0.0.1 --port 60856 --db-name vectordb --m 16 --ef-construction 128 --ef-search 128 --case-type Performance1536D50K --k 10 --num-concurrency 1\,8\,32 --concurrency-duration 30 --db-label pgvector-hnsw-2601-20260610_234701 --task-label pgvector-hnsw-2601-20260610_234701
```

`failure_manifest.json` records that VDBBench process exit was `0` but the case
label was `x` and all metrics were zero after insert retries failed. Those zeros
are invalid as comparator evidence. Valid evidence from this artifact is limited
to startup/version/health logs, VDBBench dry-run/full command shape, and the
exact storage blocker.

### Local pgvector custom smoke: functional only, not claim-quality

Artifact root:

- `/tmp/treedb_vdbbench_2601_pgvector_local_custom_20260611_014219`
- marker: `/tmp/treedb_vdbbench_2601_pgvector_local_custom_20260611_014219/LOCAL_CUSTOM_SMOKE.md`
- result summary: `/tmp/treedb_vdbbench_2601_pgvector_local_custom_20260611_014219/result_summary.json`
- result JSON: `/tmp/treedb_vdbbench_2601_pgvector_local_custom_20260611_014219/vdbbench-results/PgVector/result_20260611_pgvector-hnsw-2601-local-custom_pgvector.json`

This run used local Homebrew PostgreSQL `18.4` plus pgvector `0.8.2` on loopback
instead of Docker, with the same custom `512 x 1536` dataset used by the #2600
TreeDB custom smoke. It completed successfully and is useful as small server
comparator setup proof only.

| row | load_s | insert_s | optimize_s | recall | NDCG | serial p95_s | serial p99_s | conc | QPS | conc p95_s | conc p99_s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| pgvector HNSW local custom | 2.0156 | 0.8104 | 1.2052 | 1.0 | 1.0 | 0.0018 | 0.0051 | 1 | 990.2530 | 0.001065 | 0.001239 |
| pgvector HNSW local custom | 2.0156 | 0.8104 | 1.2052 | 1.0 | 1.0 | 0.0018 | 0.0051 | 8 | 4608.7741 | 0.002742 | 0.005455 |

Do not compare this smoke row against full TreeDB `Performance1536D50K` rows,
because those full TreeDB rows did not complete in #2600. It also does not
replace the requested full `Performance1536D50K` pgvector comparator row.

## What this campaign proves vs. does not prove

Proved:

- The #2599 artifact harness and route-proof sidecar are usable for TreeDB
  VectorDBBench captures.
- TreeDB exact FP32 and scalar_u8 rerank32 VDBBench rows can run on a bounded
  custom dataset and can prove vector-index/no-document routes.
- pgvector HNSW setup can start locally and VDBBench can resolve the intended
  `pgvectorhnsw` command in dry-run mode.
- Local PostgreSQL/pgvector can complete a bounded `512 x 1536` custom
  VDBBench smoke row.

Not proved:

- No full TreeDB `Performance1536D50K` exact or scalar_u8 baseline row exists.
- No full `Performance1536D50K` pgvector comparator row exists.
- No TreeDB-vs-pgvector, TreeDB-vs-Qdrant, TreeDB-vs-USearch, or TreeDB-vs-RaBitQ
  full-matrix comparative performance claim is supported by this campaign.
- No native Go allocation or per-operation benchmark conclusion follows from
  any VDBBench artifact here.

## Recommended next rerun plan

1. Use a dedicated quiet host or an explicitly isolated no-concurrent-benchmark
   window. Record load averages and avoid overlapping TreeDB, pgvector, or other
   VectorDBBench jobs.
2. Fix storage before rerunning pgvector: allocate a Docker/PostgreSQL volume
   with ample free space for data, index build, and `pg_wal`/temporary files;
   verify Docker's own disk image/volume capacity, not only host `/tmp` free
   space.
3. Start with a small fresh end-to-end case after storage cleanup to validate
   the harness, result parsing, route/counter proof, and pgvector insert/index
   path.
4. Rerun the real `Performance1536D50K` matrix for TreeDB exact FP32,
   TreeDB scalar_u8 rerank32, and pgvector HNSW with the same `k=10`,
   concurrency `1,8,32`, `30s`, `m=16`, `efConstruction=128`, and
   `efSearch=128` parameters.
5. Only after valid 50K rows exist, consider the larger optional matrix:
   `Performance1536D500K`, `Performance768D1M`, `efSearch={64,128,256}`, and
   rerank candidates `{16,32,64}`.
6. Keep README updates blocked until the rerun produces quiet-host, full-row,
   same-boundary evidence with clear caveats. If still blocked, file/keep
   follow-ups for TreeDB load-path tuning and comparator environment setup.

## Validation for this report

- Manual docs spelling/link sanity check: local report references are local
  artifact paths or issue/PR identifiers; the local artifact paths listed
  above were checked on this host while drafting.
- `git diff --check` passed.
- `go test ./TreeDB/documentservice ./cmd/treedb-document-service` skipped:
  this report changes benchmark Markdown only and does not modify service docs,
  service API, scripts, or Go code.
