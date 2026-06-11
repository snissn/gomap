# TreeDB VectorDBBench Server Comparator Setup (2026-06-11)

Issue: `snissn/gomap#2601`. Parent tracker: `#2598`. Built on the
merged `snissn/gomap#2605` harness (`7f66890467f96e7460ed6eeec29fdee81e61acac`).

This note is a **setup/runbook and abort record**, not a comparator result
report. No pgvector/Qdrant latency, QPS, recall, load, or index-build number in
these artifacts is public-claim quality.

## Current status

- pgvector HNSW is the first comparator lane.
- A local `pgvector/pgvector:pg16` container starts and passes health checks.
- VDBBench `pgvectorhnsw` dry-run succeeds for the intended TreeDB baseline
  shape.
- The first full pgvector attempt was aborted because it overlapped with the
  #2600 TreeDB-only full baseline. Treat that artifact as contention-tainted.
- Wait for a no-concurrent-benchmark window before running a full comparator.

## Artifact roots

| artifact | status | use |
| --- | --- | --- |
| `/tmp/treedb_vdbbench_2601_pgvector_20260610_223212` | aborted/contention-tainted | Setup, versions, health, dry-run, and abort logs only. Do not report benchmark metrics. |
| `/tmp/treedb_vdbbench_2601_pgvector_setup_script_20260610_225302` | setup smoke | Script validation: container health + VDBBench dry-run, no full benchmark. |

Key aborted-artifact marker:

- `/tmp/treedb_vdbbench_2601_pgvector_20260610_223212/ABORTED_CONTENTION_TAINTED.md`
- `/tmp/treedb_vdbbench_2601_pgvector_20260610_223212/abort_manifest.json`

## pgvector setup captured

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

## Runbook

Health + dry-run only (safe while another full benchmark is running):

```sh
OUT=/tmp/treedb_vdbbench_2601_pgvector_setup_$(date +%Y%m%d_%H%M%S) \
VECTORDBBENCH_DIR=/path/to/snissn/vectordbbench \
RUN_FULL=false \
scripts/treedb_vdbbench_pgvector_comparator.sh
```

Full comparator run, only in a no-concurrent-benchmark window:

```sh
OUT=/tmp/treedb_vdbbench_2601_pgvector_full_$(date +%Y%m%d_%H%M%S) \
VECTORDBBENCH_DIR=/path/to/snissn/vectordbbench \
RUN_FULL=true \
ALLOW_CONCURRENT_BENCHMARKS=false \
scripts/treedb_vdbbench_pgvector_comparator.sh
```

The script refuses a full run when another `vectordb_bench.cli.vectordbbench`
process is visible unless `ALLOW_CONCURRENT_BENCHMARKS=true` is explicitly set.
It records Docker/image/version context, health checks, VDBBench command output,
results, and container logs under `OUT`.

## Aborted full attempt caveat

The aborted artifact loaded 50K vectors and began pgvector HNSW index creation,
but the coordinator terminated it to avoid overlap with #2600. VDBBench then
wrote a failed result JSON with zero metrics and `label = x`. That JSON is
retained only as abort evidence.

Do not compare it against TreeDB, Qdrant, USearch, Faiss, or any other lane.

## Next comparator

Qdrant HNSW remains the next priority after pgvector once the full pgvector row
has a clean no-contention window. Keep Qdrant evidence in the same
server/database boundary lane and do not mix it with library-only rows.
