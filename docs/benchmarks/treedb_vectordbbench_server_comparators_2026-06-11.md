# TreeDB VectorDBBench Server Comparator Setup (2026-06-11)

Issue: `snissn/gomap#2601`. Parent tracker: `#2598`. Built on the
merged `snissn/gomap#2605` harness (`7f66890467f96e7460ed6eeec29fdee81e61acac`).

This note is a **setup/runbook, blocker record, and custom-smoke comparator
record**. The successful pgvector row below is a small `512 x 1536` functional
smoke, not a full `Performance1536D50K` row and not public-claim quality.

## Current status

- pgvector HNSW is the first server/database comparator lane.
- A local `pgvector/pgvector:pg16` Docker container starts and passes health
  checks; VDBBench `pgvectorhnsw` dry-run succeeds for the intended
  `Performance1536D50K` shape.
- The first full Docker/pgvector attempt was aborted because it overlapped with
  the #2600 TreeDB-only full baseline. Treat that artifact as
  contention-tainted.
- A later no-concurrent Docker/pgvector full attempt reached VDBBench but failed
  during PostgreSQL insert with `pg_wal/xlogtemp.*: No space left on device`.
- A local Homebrew PostgreSQL + pgvector custom smoke completed successfully on
  the same `512 x 1536` custom dataset used by #2600's TreeDB smoke. This is a
  valid setup/custom-smoke comparator row only; it does not unblock full-matrix
  comparative claims.

## Artifact roots

| artifact | status | use |
| --- | --- | --- |
| `/tmp/treedb_vdbbench_2601_pgvector_20260610_223212` | aborted/contention-tainted | Setup, versions, health, dry-run, and abort logs only. Do not report benchmark metrics. |
| `/tmp/treedb_vdbbench_2601_pgvector_setup_script_20260610_225302` | setup smoke | Script validation: container health + VDBBench dry-run, no full benchmark. |
| `/tmp/treedb_vdbbench_2601_pgvector_full_20260610_234701` | failed/no-space | Attempted full Docker/pgvector row; load failed with PostgreSQL `pg_wal` no-space error. Do not report metrics. |
| `/tmp/treedb_vdbbench_2601_pgvector_local_custom_20260611_014219` | complete local custom smoke | Local PostgreSQL/pgvector custom `512 x 1536` VDBBench row. Functional comparator smoke only, not claim-quality. |

Key markers:

- `/tmp/treedb_vdbbench_2601_pgvector_20260610_223212/ABORTED_CONTENTION_TAINTED.md`
- `/tmp/treedb_vdbbench_2601_pgvector_20260610_223212/abort_manifest.json`
- `/tmp/treedb_vdbbench_2601_pgvector_full_20260610_234701/FAILED_NO_SPACE.md`
- `/tmp/treedb_vdbbench_2601_pgvector_full_20260610_234701/failure_manifest.json`
- `/tmp/treedb_vdbbench_2601_pgvector_local_custom_20260611_014219/LOCAL_CUSTOM_SMOKE.md`
- `/tmp/treedb_vdbbench_2601_pgvector_local_custom_20260611_014219/result_summary.json`

## Docker pgvector setup captured

From the setup-smoke artifact:

- Docker: `Docker version 27.5.1, build 9f9e405`.
- Image: `pgvector/pgvector:pg16`.
- PostgreSQL: `16.14 (Debian 16.14-1.pgdg12+1)`.
- pgvector extension: `vector=0.8.2`.
- VDBBench checkout: `snissn/vectordbbench` at
  `c1c763c0dcd97c489c2b107fd7060374f76d09b8`.
- Command shape: `pgvectorhnsw`, `Performance1536D50K`, COSINE, `k=10`,
  concurrency `1,8,32`, `30s`, `m=16`, `ef_construction=128`, `ef_search=128`.
- Dry-run exit: `0`.

## Local pgvector custom smoke captured

Artifact root:

- `/tmp/treedb_vdbbench_2601_pgvector_local_custom_20260611_014219`

Sanitized context:

| field | value |
| --- | --- |
| gomap commit at run start | `f68ae6813` (`snissn/2601-manager`) |
| vectordbbench commit | `c1c763c0dcd97c489c2b107fd7060374f76d09b8` |
| host class | Apple M3, macOS/Darwin arm64 |
| PostgreSQL | Homebrew PostgreSQL `18.4` |
| pgvector extension | `vector=0.8.2` |
| Python / uv | Python `3.14.0`; `uv 0.9.18` |
| VDBBench row | `pgvectorhnsw` |
| HNSW | `m=16`, `ef_construction=128`, `ef_search=128` |

Dataset and VDBBench shape:

| field | value |
| --- | --- |
| case | `PerformanceCustomDataset` / `TreeDB2601PgvectorLocalSmoke` |
| dataset | `treedb2600smoke/treedb2600_smoke_512x1536_20260610_234950` |
| train rows | 512 |
| test rows | 64 |
| dimension | 1536 |
| metric | COSINE |
| topK | 10 |
| concurrency | `1,8` for `10s` each |
| result label | `:)` |

Smoke results as emitted by VDBBench. Latencies are seconds in the artifact
result file. These rows are **functional smoke only** and must not be used as
public or full-matrix comparative throughput evidence.

| row | load_s | insert_s | optimize_s | recall | NDCG | serial p95_s | serial p99_s | conc | QPS | conc p95_s | conc p99_s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| pgvector HNSW local custom | 2.0156 | 0.8104 | 1.2052 | 1.0 | 1.0 | 0.0018 | 0.0051 | 1 | 990.2530 | 0.001065 | 0.001239 |
| pgvector HNSW local custom | 2.0156 | 0.8104 | 1.2052 | 1.0 | 1.0 | 0.0018 | 0.0051 | 8 | 4608.7741 | 0.002742 | 0.005455 |

Local custom-smoke command outline:

```sh
OUT=/tmp/treedb_vdbbench_2601_pgvector_local_custom_20260611_014219
initdb -D "$OUT/pgdata" -A trust
pg_ctl -D "$OUT/pgdata" -l "$OUT/logs/postgres.log" -w start
createdb -h 127.0.0.1 -p "$PORT" vectordb
psql -h 127.0.0.1 -p "$PORT" -d vectordb -c 'CREATE EXTENSION IF NOT EXISTS vector;'

uv run --no-sync \
  --with click --with pydantic --with pyyaml --with environs \
  --with pandas --with polars --with pyarrow --with psutil --with pytz \
  --with tqdm --with plotly --with ujson --with hdrhistogram \
  --with scikit-learn --with s3fs --with oss2 \
  --with psycopg --with psycopg-binary --with pgvector \
  python -m vectordb_bench.cli.vectordbbench pgvectorhnsw \
  --user-name "$USER" --password '' --host 127.0.0.1 --port "$PORT" \
  --db-name vectordb \
  --m 16 --ef-construction 128 --ef-search 128 \
  --case-type PerformanceCustomDataset --k 10 \
  --num-concurrency 1,8 --concurrency-duration 10 \
  --db-label pgvector-hnsw-2601-local-custom \
  --task-label pgvector-hnsw-2601-local-custom \
  --custom-case-name TreeDB2601PgvectorLocalSmoke \
  --custom-case-description TreeDB2601PgvectorLocalSmoke \
  --custom-case-load-timeout 300 \
  --custom-case-optimize-timeout 300 \
  --custom-dataset-name treedb2600smoke \
  --custom-dataset-dir treedb2600_smoke_512x1536_20260610_234950 \
  --custom-dataset-size 512 \
  --custom-dataset-dim 1536 \
  --custom-dataset-metric-type COSINE \
  --custom-dataset-file-count 1 \
  --custom-dataset-with-gt
```

The complete captured command is in
`/tmp/treedb_vdbbench_2601_pgvector_local_custom_20260611_014219/commands/vdbbench_pgvectorhnsw.command.txt`.

## Runbook: Docker helper

Health + dry-run only (safe while another full benchmark is running):

```sh
OUT=/tmp/treedb_vdbbench_2601_pgvector_setup_$(date +%Y%m%d_%H%M%S) \
VECTORDBBENCH_DIR=/path/to/snissn/vectordbbench \
RUN_FULL=false \
scripts/treedb_vdbbench_pgvector_comparator.sh
```

Full Docker comparator run, only in a no-concurrent-benchmark window:

```sh
OUT=/tmp/treedb_vdbbench_2601_pgvector_full_$(date +%Y%m%d_%H%M%S) \
VECTORDBBENCH_DIR=/path/to/snissn/vectordbbench \
RUN_FULL=true \
ALLOW_CONCURRENT_BENCHMARKS=false \
scripts/treedb_vdbbench_pgvector_comparator.sh
```

Custom-case Docker runs can append VDBBench CLI arguments with
`VDBBENCH_EXTRA_ARGS`, parsed with Python `shlex`, for example:

```sh
VDBBENCH_EXTRA_ARGS="--custom-case-name Smoke --custom-dataset-name treedb2600smoke ..." \
CASE_TYPE=PerformanceCustomDataset \
RUN_FULL=true \
scripts/treedb_vdbbench_pgvector_comparator.sh
```

The script refuses a full run when another `vectordb_bench.cli.vectordbbench`
process is visible unless `ALLOW_CONCURRENT_BENCHMARKS=true` is explicitly set.
It records Docker/image/version context, health checks, VDBBench command output,
results, and container logs under `OUT`.

## Failed/aborted full-attempt caveats

The aborted Docker artifact loaded 50K vectors and began pgvector HNSW index
creation, but the coordinator terminated it to avoid overlap with #2600.
VDBBench then wrote a failed result JSON with zero metrics and `label = x`.
That JSON is retained only as abort evidence.

The later no-concurrent Docker full attempt failed during pgvector load with:

```text
could not write to file "pg_wal/xlogtemp.156": No space left on device
```

It also wrote a failed result JSON with `label = x` and zero metrics. The
artifact is useful for setup/blocker evidence only.

Do not compare either failed artifact against TreeDB, Qdrant, USearch, Faiss,
or any other lane.

## Next comparator/full-row work

The local custom pgvector smoke satisfies setup and small-row comparator proof,
but it does **not** replace the requested full `Performance1536D50K` comparator
row. To produce claim-quality comparator evidence, rerun pgvector on a quiet
host with verified PostgreSQL/Docker storage (or a dedicated local PostgreSQL
cluster) and capture the real `Performance1536D50K`, `k=10`, concurrency
`1,8,32`, `30s` row.

Qdrant HNSW remains the next priority after pgvector once the full pgvector row
has a clean no-contention environment. Keep Qdrant evidence in the same
server/database boundary lane and do not mix it with library-only rows.
