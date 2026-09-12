# Real 768D native Minima-style diagnostic

This is a separate, nonqualifying workload, not a replacement for the frozen 8D
Minima/M5 manifests. It opens only the already-used Cohere diagnostic export;
no protected final holdout, download, zero-padding or dimensional projection.
The harness must be reviewed and landed before collecting the 500K run.

The public service uses typed Collections `column_graph` with
`command_wal_durable`: native upsert/search/GetMany; explicit HTTP schema,
build/fold/ensure, metadata-filter deletion and full-state cursor verification.
Search timing includes Python request preparation, native transport, public
collection admission/filter/search, result materialization and client decode.
It is not a bare HNSW microbenchmark. The protocol's producer work record is
retained separately after each search timer.

The declared 500K workload uses 100 existing diagnostic queries, an independent
float64 cosine top-10 oracle, EF128/256/512/1024/2048, and eligible populations
4096/4097/5000/50000/500000. Scalar labels are synthetic, dispersed unique ranks
with range predicates, **not** a categorical tenant posting-list benchmark.
First-predicate timings are predicate-cache-cold, not OS-cache-cold. Each curve
follows that predicate's first query. Reopen curves use EF512.

Four readers execute 64 EF512 queries each at 4097 eligible rows while one writer performs eight 256-row
same-ID replacements. This is fixed-live-count replacement throughput, not
concurrent ingestion. Writer-active flags are approximate; event intervals
allow exact observed temporal-overlap analysis. Payload preparation is outside
write-call timers; native packing and transport are inside. Lifecycle probes
update 256 rows, delete their file through a public scalar filter, check missing
GetMany values, reinsert, fold, close/reopen/ensure, then verify every ID, payload
and normalized full vector through bounded 256-row public HTTP pages. Full-state verification
is a separate verification-only phase, not query or point-fetch throughput.

Reuse `/mnt/fast4tb/gomap-768-lanes-20260911/scale-data`; raw vectors are 1.536 GB.
No full-corpus Python float lists or DB copies are created. Estimated incremental
disk 5–8 GiB, hard output cap 11 GiB with 10 GiB free reserve and a 24 GiB combined
runner plus service RSS guard. Estimated runtime 15–30 minutes, hard 45-minute guard, 600-second
per-operation deadline. Budget failure preserves partial events and stops the
owned service; it does not retry or weaken caps. Whole-run guards are approximate
one-second samples, not hard filesystem quotas. A dedicated runner is unavailable:
collection uses a serialized quiet window on the shared workstation.

Freeze with a clean reviewed harness checkout and unmodified product binary:

```sh
python benchmarks/vector_db_compare/minima_cohere_native_diagnostic.py \
  --freeze /mnt/fast4tb/TASK/plan.json --run-dir /mnt/fast4tb/TASK/run \
  --dataset /mnt/fast4tb/gomap-768-lanes-20260911/scale-data \
  --service-bin /mnt/fast4tb/TASK/treedb-document-service \
  --product-commit FULL_PRODUCT_COMMIT --serving /mnt/fast4tb/TASK/serving.json
```

Repeat those same arguments/environment using `--run PLAN` instead of
`--freeze PLAN`, adding `--expected-plan-sha256 HASH`. The plan binds exact
product/harness commits and trees, service binary, dataset/truth/serving bytes,
CPU affinity, GOMAXPROCS and Python/NumPy versions. The run directory must not
already exist. Set `GOWORK=off GOMAXPROCS=6 PYTHONDONTWRITEBYTECODE=1`, an explicit
fastmount `TMPDIR`, and client/harness `PYTHONPATH`; use `taskset -c 0-5` on this
workstation. Set `OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1` to keep independent
oracle checks single-threaded. Existing benchmark Python with NumPy is sufficient.

Use `--rows 512` for the bounded
public lifecycle smoke. It uses the real prefix and independently computes a
four-query oracle with eligible counts 64/65/128/256/512 and overlap population 65;
it never supplies retained 500K performance evidence. Its tiny population cycles
the same two replacement batches, so later overlap writes are no-ops, unlike the
500K run's eight changed batches.

`events.jsonl` preserves successes and failures; `terminal.lifecycle_complete`
is true only after full verification and clean owned-process shutdown. Its
qualification remains `not_evaluated`, regardless of recall or timing. Preserve
all failed runs and producer identities; no silent reruns or reuse of a mutated
DB. The service log and wait4 process-lifetime peaks are retained alongside it.

Tiny source checks (no service or collection):

```sh
PYTHONPATH=benchmarks/vector_db_compare:clients/python/treedb_client/src \
  OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 \
  python -m unittest test_minima_cohere_native_diagnostic
```
