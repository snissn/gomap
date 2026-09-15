# Opt-in 768D diagnostic harness

This harness measures the public Go Collections interface, not native transport,
HTTP, document materialization, or an end-to-end Minima workload. It introduces
no runtime changes and carries no performance improvement claim.

`prepare_cohere_scale.py` exports finite, nonzero 768D vectors from caller-supplied
parquet files using the existing NumPy/PyArrow environment. It computes exhaustive
float64 cosine top-10 truth with row-ID tie-breaking and rejects byte-identical
float32 query/train overlap. Record the external dataset revision and split
provenance separately: filenames and absence of exact overlap do not establish
a genuine Cohere held-out split. The output directory must not already exist.

```sh
python benchmarks/vector_db_compare/prepare_cohere_scale.py \
  --train /data/train.parquet --queries /data/test.parquet \
  --out /mnt/fast4tb/task/cohere-export --rows 500000 --query-count 200
```

Run the wrapper from a clean committed repository root, with output and temporary
files outside the checkout. Each invocation exclusively reserves
`RESULTS/LABEL-PHASE/` before compilation; failed runs remain there and must not
be reused. It records the source commit/tree, complete tracked path/blob inventory,
runtime subtree and harness blob identities, binary SHA256, Go build information,
dataset manifest digest, and command output. The Go fixture validates payload
hashes/shapes and truth cardinality/IDs before measured searches.

```sh
export TMPDIR=/mnt/fast4tb/task/tmp GOMAXPROCS=6
export TREEDB_SCALE_DATA=/mnt/fast4tb/task/cohere-export
export TREEDB_SCALE_DB=/mnt/fast4tb/task/pristine-db
export TREEDB_SCALE_RESULTS=/mnt/fast4tb/task/evidence
export TREEDB_SCALE_LABEL=candidate TREEDB_SCALE_CPUS=0-5
TREEDB_SCALE_PHASE=build benchmarks/vector_db_compare/run_cohere_scale.sh
# After construction, in a separate quiet window:
TREEDB_SCALE_PHASE=measure benchmarks/vector_db_compare/run_cohere_scale.sh
```

The caller creates TMPDIR, confirms capacity, freezes inputs, and excludes other
CPU/I/O-heavy work. Compilation and full dataset hashing precede measured search
intervals. The first unfiltered call is process/application-cache cold, **not**
OS-page-cache cold. A `public_cold_filter` row is an uncached-predicate call in
that already warmed process. For 4,096, 4,097, 5,000, and 50,000 eligible rows,
the measure phase separately reports predicate-membership preparation, optional
navigation construction, one direct EF512 navigation search, and the outer
public uncached-predicate call. The direct construction budget is reported and
does not include the public keeper's additional capacity cap; its timer includes
construction admission wait. A declined navigation build is required only at
the 4,096-row exact-scan cutoff. Direct phase helpers are differently budgeted
diagnostic comparators, not components timed inside the public call: they do not
populate its serving predicate cache, and their times must not be summed into a
public-call decomposition. Warm per-query p50/p95 and recall remain reported at
each EF. EF512 here is diagnostic and does not revise frozen M5 EF2048.

Write phases require `TREEDB_SCALE_WRITE_COPY=1` and a separately copied, cleanly
closed DB fixture. This acknowledgement does not create or prove the copy: retain
the copy command and source lineage, and never point a write phase at the pristine
search fixture. `write` and `write_schema` perform eight 256-ID changed replacements;
the latter first creates unrelated metadata-only schema. They preserve vector and
scalar truth. Same IDs bound the live replacement set, not physical mutation
history: eight batches consume all 4096 publication rows (replacement plus delete).
`write_schema_roots` names an operator-declared prior run via
`TREEDB_SCALE_PRECEDING_RUN`; the label is not verified DB lineage. It creates
unrelated text roots; reuse of that exhausted fixture is a fold-budget negative
diagnostic, not successful write-throughput evidence. All phases fail on write
errors; rejected attempts must not be reported as completed durable writes.

The 768D write diagnostics are separate opt-in synthetic-vector diagnostics:
`GOMAP_WRITE_768_DIAGNOSTIC=1 go test -p=1 ./TreeDB/collections
./TreeDB/documentservice -run
'^Test(TypedWrite768(Diagnostic|ReadWriteInteractionDiagnostic)|DocumentServiceTypedWrite768Diagnostic)$'
-count=1 -v`. They time 16 durable upsert calls of previously absent IDs at
1/64/256/1024 rows and 1/2/4 writers, include two scalar indexes and one text
index, and check acknowledged IDs after clean reopen. The document-service cells
exercise the typed method used by native Minima but exclude wire/client encoding.
Matched collection read controls distinguish a growing exact-scored suffix from
writer interaction. Optional `GOMAP_WRITE_768_PRELOAD` is setup outside timing.
The synthetic diagnostic accepts at most 10,000 preloaded rows; use the separate
500K Minima qualification for large-scale evidence.
This is neither ANN quality evidence nor crash/power-loss certification. Direct
Go runs need their own clean source/binary provenance capture; the scale wrapper
runs only the scale fixture.

## Matched query-ready RSS comparison

`minima_cohere_native_diagnostic.py --rss-only` and
`minima_cohere_qdrant_rss_diagnostic.py` compare fresh server processes on the
same frozen 500K x 768D export. Run them serially on the same CPU set and quiet
host. Both use 256-row durable-and-visible batches, logical `row-*` IDs, FP32
cosine vectors, `content`, nested `meta.user_id`/`meta.fpath`, and both scalar
indexes. Qdrant maps the logical ID to a deterministic UUID only because its
physical point-ID type requires it; the matched logical ID remains in payload.

The v3 protocol uses queries 0..99 as observed calibration and 100..199 as
observed revalidation; neither fixed set is a holdout. Each backend selects the
lowest control in the predeclared 32, 64, 128, 256, 512, 1024, 2048 grid whose
mean recall@10 is at least 0.90 on both sets. TreeDB tunes `ef_search`; Qdrant
tunes `hnsw_ef`.
Qdrant `exact=true` is
run once after the RSS sample as a correctness reference and is never used as
the ANN latency, quality, or RSS boundary.

First freeze and run TreeDB from a clean committed checkout and an exact clean
service binary. `SERVING_JSON` is the already reviewed column-graph serving
configuration. The result is `TREE_RUN/rss.json`.

```sh
taskset -c 0-5 python benchmarks/vector_db_compare/minima_cohere_native_diagnostic.py \
  --freeze "$TREE_PLAN" --dataset "$COHERE_EXPORT" --rows 500000 --rss-only \
  --service-bin "$TREE_SERVICE" --product-commit "$TREE_COMMIT" \
  --serving "$SERVING_JSON" --run-dir "$TREE_RUN"
TREE_PLAN_SHA=$(sha256sum "$TREE_PLAN" | awk '{print $1}')
taskset -c 0-5 python benchmarks/vector_db_compare/minima_cohere_native_diagnostic.py \
  --run "$TREE_PLAN" --expected-plan-sha256 "$TREE_PLAN_SHA" \
  --dataset "$COHERE_EXPORT" --rows 500000 --rss-only \
  --service-bin "$TREE_SERVICE" --product-commit "$TREE_COMMIT" \
  --serving "$SERVING_JSON" --run-dir "$TREE_RUN"
```

Then use the repository's pinned `qdrant-client==1.19.0` Python environment.
The run owns a fresh standalone Qdrant 1.19.0 process and requires both
`QDRANT_STORAGE` and `QDRANT_RUN` not to exist before launch.

```sh
taskset -c 0-5 "$QDRANT_PYTHON" benchmarks/vector_db_compare/minima_cohere_qdrant_rss_diagnostic.py \
  --freeze "$QDRANT_PLAN" --dataset "$COHERE_EXPORT" --treedb-artifact "$TREE_RUN/rss.json" \
  --qdrant-bin "$QDRANT_BIN" --storage-path "$QDRANT_STORAGE" \
  --url "http://127.0.0.1:$QDRANT_PORT" --run-dir "$QDRANT_RUN"
QDRANT_PLAN_SHA=$(sha256sum "$QDRANT_PLAN" | awk '{print $1}')
taskset -c 0-5 "$QDRANT_PYTHON" benchmarks/vector_db_compare/minima_cohere_qdrant_rss_diagnostic.py \
  --run "$QDRANT_PLAN" --expected-plan-sha256 "$QDRANT_PLAN_SHA" \
  --dataset "$COHERE_EXPORT" --treedb-artifact "$TREE_RUN/rss.json" \
  --qdrant-bin "$QDRANT_BIN" --storage-path "$QDRANT_STORAGE" \
  --url "http://127.0.0.1:$QDRANT_PORT" --run-dir "$QDRANT_RUN"
```

`comparison.json` says `accept` when TreeDB server-process `VmHWM` is no greater
than Qdrant's and recommends stopping RSS work for this initial-ready workload.
Otherwise it reports the TreeDB-minus-Qdrant byte delta and ratio as `investigate`. Missing `VmHWM`, PID
drift, nonempty backends, readiness/quality failures, or mismatched frozen
artifacts produce `uncalibrated`. Qdrant readiness requires green/OK optimizer
state, exact and indexed vector counts of 500,000, and both scalar indexes.
This comparison does not replace or retroactively pass the historical 2.5M x 8D
M5 contract; fold and restart RSS remain separately reported there.

### Separate SQ8 representation row

Run TreeDB a second time with a fresh owned directory and process, adding
`--query-mode quantized_rerank --quantized-index-name minima_sq8` to the TreeDB
freeze and run commands. Its `rss.json` has the separate
`treedb_cohere_768_sq8_rss_boundary/v1` schema. The ordered control coordinate is
always `(E, requested R=E)`; both fixed 100-query sets participate in selection.
Build/code preparation, all failed lower coordinates and calibration remain
inside the sampled process-lifetime `VmHWM` boundary.
The SQ8 arm explicitly creates and validates the fixed `M=16` graph shape; it
does not inherit an ambient collection default.

Keep the SQ8 freeze and run identities distinct from the FP32 arm. For example,
using the same immutable dataset, service, product commit, serving JSON and
environment as the earlier TreeDB commands:

```sh
SQ8_TASK=/mnt/fast4tb/cohere-sq8
SQ8_PLAN="$SQ8_TASK/plan.json"
SQ8_RUN="$SQ8_TASK/run"
mkdir -p "$SQ8_TASK"
taskset -c 0-5 python benchmarks/vector_db_compare/minima_cohere_native_diagnostic.py \
  --freeze "$SQ8_PLAN" --run-dir "$SQ8_RUN" \
  --dataset "$COHERE_EXPORT" --service-bin "$TREE_SERVICE" \
  --product-commit "$TREE_COMMIT" --serving "$SERVING_JSON" \
  --rows 500000 --rss-only \
  --query-mode quantized_rerank --quantized-index-name minima_sq8
SQ8_PLAN_SHA=$(sha256sum "$SQ8_PLAN" | awk '{print $1}')
taskset -c 0-5 python benchmarks/vector_db_compare/minima_cohere_native_diagnostic.py \
  --run "$SQ8_PLAN" --expected-plan-sha256 "$SQ8_PLAN_SHA" \
  --run-dir "$SQ8_RUN" --dataset "$COHERE_EXPORT" \
  --service-bin "$TREE_SERVICE" --product-commit "$TREE_COMMIT" \
  --serving "$SERVING_JSON" --rows 500000 --rss-only \
  --query-mode quantized_rerank --quantized-index-name minima_sq8
```

`SQ8_RUN` must not exist before the run. Do not reuse the FP32 plan digest,
run directory, database, or service lifetime.

Pass that artifact as `--treedb-sq8-artifact SQ8_RUN/rss.json` when freezing and
running the Qdrant harness. `comparison.json` remains the original FP32
TreeDB-versus-FP32-Qdrant decision. The additional
`three-arm-comparison.json` nests that decision unchanged and reports SQ8
TreeDB-versus-FP32 Qdrant only as a quality-matched, different-representation
observation. The SQ8 row has no `accept`/`investigate` state and cannot revise the
historical M5 result. The two TreeDB arms and Qdrant must each use a distinct
owned process and backend directory.

## Retained evidence gate

Land the reviewed harness before expensive retained collection. Freeze the exact
measured runtime commit and harness commit/blobs, and identify their dependency
order. A runtime change, harness change, changed dataset digest, or materially
different environment invalidates affected comparisons. Old diagnostic results
remain historical; never relabel them as measurements of a new runtime head.

An artifact-only descendant may carry retained evidence only after verifying its
diff contains exclusively the declared evidence paths, no runtime/harness changes,
and that every recorded implementation/harness blob still matches the frozen
source. Keep the frozen commit/tree and `source-blobs.txt` as the authority, not
the descendant's different tree hash. Explicitly report missing dedicated runner,
persistent cache, or durable artifact storage as
`INFRASTRUCTURE_UNAVAILABLE: <runner|cache|storage>: <reason>` with the actual
fallback. Local focused smoke tests are not retained performance acceptance.
