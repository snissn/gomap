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
  --serving "$SERVING_JSON" --run-dir "$TREE_RUN" --ef-construction 32
TREE_PLAN_SHA=$(sha256sum "$TREE_PLAN" | awk '{print $1}')
taskset -c 0-5 python benchmarks/vector_db_compare/minima_cohere_native_diagnostic.py \
  --run "$TREE_PLAN" --expected-plan-sha256 "$TREE_PLAN_SHA" \
  --dataset "$COHERE_EXPORT" --rows 500000 --rss-only \
  --service-bin "$TREE_SERVICE" --product-commit "$TREE_COMMIT" \
  --serving "$SERVING_JSON" --run-dir "$TREE_RUN" --ef-construction 32
```

Then use the repository's pinned `qdrant-client==1.19.0` Python environment.
The run owns a fresh standalone Qdrant 1.19.0 process and requires both
`QDRANT_STORAGE` and `QDRANT_RUN` not to exist before launch.
`QDRANT_STORAGE` must be a strict descendant of `QDRANT_RUN` (for example,
`QDRANT_RUN/storage`) so one owned root covers the database, logs and evidence.

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

`comparison.json` is a new, current-candidate 500K mirror-bounded FP32/Qdrant
control; it is not a replay or reclassification of #4672. It says `accept` when
TreeDB server-process `VmHWM` is no greater
than Qdrant's and recommends stopping RSS work for this initial-ready workload.
Otherwise it reports the TreeDB-minus-Qdrant byte delta and ratio as `investigate`. Missing `VmHWM`, PID
drift, nonempty backends, readiness/quality failures, or mismatched frozen
artifacts produce `uncalibrated`. Qdrant readiness requires green/OK optimizer
state, exact and indexed vector counts of 500,000, and both scalar indexes.
Its frozen one-second resource guard enforces the 45-minute wall limit, 10 GiB
free reserve, 11 GiB owned-root cap and 24 GiB simultaneous harness-plus-server
RSS cap from harness start through identity-owned cleanup. The final guard
sample is taken after the owned server exits and still enforces wall, free-space,
owned-byte, and surviving-harness RSS limits. Qdrant remains in the harness process group, while internal TERM and
any forced KILL are PID/start-time checked separately. The comparison is not
computed until the guard summary, initial-ready storage size, client closure,
server exit and log closure are final; a forced exit, prior exit, identity drift,
or failed cleanup invalidates the measurement.
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
  --rows 500000 --rss-only --ef-construction 32 \
  --query-mode quantized_rerank --quantized-index-name minima_sq8
SQ8_PLAN_SHA=$(sha256sum "$SQ8_PLAN" | awk '{print $1}')
taskset -c 0-5 python benchmarks/vector_db_compare/minima_cohere_native_diagnostic.py \
  --run "$SQ8_PLAN" --expected-plan-sha256 "$SQ8_PLAN_SHA" \
  --run-dir "$SQ8_RUN" --dataset "$COHERE_EXPORT" \
  --service-bin "$TREE_SERVICE" --product-commit "$TREE_COMMIT" \
  --serving "$SERVING_JSON" --rows 500000 --rss-only --ef-construction 32 \
  --query-mode quantized_rerank --quantized-index-name minima_sq8
```

`SQ8_RUN` must not exist before the run. Do not reuse the FP32 plan digest,
run directory, database, or service lifetime.

Pass that artifact as `--treedb-sq8-artifact SQ8_RUN/rss.json` when freezing and
running the Qdrant harness. `comparison.json` remains the new mirror-bounded FP32
TreeDB-versus-FP32-Qdrant control for these exact current artifacts. The additional
`three-arm-comparison.json` nests that decision unchanged and reports SQ8
TreeDB-versus-FP32 Qdrant only as a quality-matched, different-representation
observation. The SQ8 row has no `accept`/`investigate` state and cannot revise the
historical M5 result. The two TreeDB arms and Qdrant must each use a distinct
owned process and backend directory.

For query-path attribution, freeze a separate full SQ8 plan in its own fresh
directory and pass the prior SQ8 RSS artifact plus its external hash using
`--all-rows-sq8-rss-artifact` and
`--expected-all-rows-sq8-rss-artifact-sha256`. The full plan consumes the prior
all-row decision and starts a graph owner whose PID/start-time differs from the
RSS source. It revalidates that one coordinate on both fixed sets and never
reruns the all-row grid. After that revalidation and before any mutation, the
full diagnostic retains one warm batch plus five order-balanced
repetitions of native-v2 FP32 and native-v3 SQ8 calls against the same
code-declared graph owner. Each arm is a separately bracketed complete batch
over the same queries, and first-arm order alternates by repetition. The paired
packet records raw call timings and work, per-arm server/client CPU,
process-wide Go allocation endpoints/deltas, current heap,
aggregate typed-graph assets and total DB bytes including WAL. It explicitly
marks the SQ8-only physical TVIS length producer-unavailable: internal
`VectorIndexSearchStats` has physical asset counters, but neither the public
dense-work/score-plane transport nor the diagnostics endpoint exposes that
split, so it must
not be inferred from aggregate assets. The paired calls are outside the strict
RSS population and selection ledger and do not alter its boundary.

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

For Q5's bounded ordinary-mutable controls, keep the existing Go validator
boundary intact. Completed exact bounded evidence uses `native_runtime` as an
application-lifecycle control. Completed SQ8 bounded evidence uses the pinned
quantized plan and `column_graph`. Those two bounded rows are not a
representation-matched exact-versus-SQ8 comparison; same-graph query-only
attribution comes from the full diagnostic's paired native-v2/native-v3 packet.
Do not relax the bounded exact validator to admit `column_graph` under the old
schema.

### Q5 packet analysis

Freeze the evidence consumer before collection. A Q5 packet uses schema
`treedb_cohere_q5_packet/v1` and is itself pinned by an external SHA-256. Its
`candidate_commit` is the exact 40-character merged runtime/harness commit and
`declared_quality_outcome` is `pass` or `miss`. `tradeoff_review` uses schema
`treedb_cohere_q5_tradeoff_review/v1` and records `disposition`, nonempty
`rationale`, and nullable `authority`. A passing quality result may declare
`no_material_regression`, `unaccepted_material_regression`, or
`owner_accepted_material_regression`; the last requires an explicit authority.
A quality miss must use `not_reached_after_quality_miss`. The packet directory is an
immutable inventory: every entry in `files` has exactly `path`, `sha256`, and
`bytes`; resolved entries must be regular files, paths are relative descendants;
and each path and digest has exactly
one semantic role.

The `dataset` map has `manifest`, `documents`, `queries`, and `truth`. The
`inputs` map has `bounded_validator_binary`, `bounded_sq8_plan`,
`treedb_service_binary`, `serving`, and `qdrant_binary`; each is a distinct
inventory role. Native plans must bind the exact service and serving hashes,
and the Qdrant plan must bind the exact Qdrant executable hash. The consumer
also resolves the candidate's harness/product Git tree IDs and requires both
Go executables to report Go 1.26, `-trimpath`, the exact candidate VCS revision,
and `vcs.modified=false`; their parsed build settings are retained in the
analysis. The matched RSS contract also freezes the CPU model and sorted
feature flags in addition to affinity, cgroup, memory, platform, and Go runtime. The
consumer refuses a dirty/different harness or Python-client tree and records
the candidate SHA-256 of every local module actually imported by the analysis. The
`plans` map retains the exact frozen plans for `smoke_exact`, `smoke_sq8`,
`treedb_fp32_rss`, `treedb_sq8_rss`, `qdrant_fp32_rss`, and
`full_sq8_events`. The consumer checks native event plans byte-semantically and
binds each RSS artifact back to its plan. The Qdrant plan must be the guarded
three-arm schema, retain both TreeDB input hashes, and copy both hashes into
the Qdrant artifact provenance. It also fixes 500K rows, 200 queries, 768
dimensions, top-10, 256-row ingestion, the complete control grid, and the exact
initial-upload and production HNSW/optimizer configurations; the artifact must
repeat those four configurations byte-semantically. The
`arms` map has exactly:

```text
smoke_exact smoke_sq8 bounded_exact bounded_sq8
treedb_fp32_rss treedb_sq8_rss qdrant_fp32_rss
fp32_comparison three_arm_comparison full_sq8_events command_receipts
```

Build both Go inputs once from the clean candidate checkout into the durable
campaign root, never into the source tree. Set `GO` to the absolute Go 1.26
executable and `Q5_ROOT` to the absolute durable campaign root; use that same
tool for the build and metadata inspection:

```sh
GOWORK=off "$GO" build -trimpath -o "$Q5_ROOT/bin/treedb-document-service" \
  ./cmd/treedb-document-service
GOWORK=off "$GO" build -trimpath -o "$Q5_ROOT/bin/treedb-rag-benchmark" \
  ./TreeDB/cmd/treedb_rag_benchmark
"$GO" version -m "$Q5_ROOT/bin/treedb-document-service"
"$GO" version -m "$Q5_ROOT/bin/treedb-rag-benchmark"
```

Do not collect if either metadata record lacks the candidate revision,
`vcs.modified=false`, or `-trimpath=true`; the packet consumer enforces the
same boundary independently.

`command_receipts` uses schema `treedb_cohere_q5_command_receipts/v1`, repeats
the candidate and dataset hashes, and has one `runs` entry for every arm above
except the two comparison files and the receipt itself. Every entry records
`run_argv`, absolute `cwd`, the complete controlled environment, four-digit
octal `umask`, UTC start/end timestamps, exit code, output SHA-256, and nullable
freeze fields. Plan-backed arms additionally require `freeze_argv`, a zero
freeze exit, and their plan SHA-256; bounded arms set those three fields to
null and must record the exact inventoried Go validator command, artifact path,
candidate commit, and, for SQ8, quantized plan path and external plan hash. A
plan-backed freeze and run use the same closed, role-specific option grammar
outside `--freeze` versus `--run` plus `--expected-plan-sha256`. Both must use
an exact `taskset -c/--cpu-list` prefix matching the plan's observed affinity,
then the recorded Python interpreter and reviewed script; duplicate, unknown,
or inapplicable options are rejected. Each command explicitly names every workload control: dataset, run directory,
row/mode/construction settings, reserved ports, and time controls. Native arms
must name the inventoried service and serving configuration; the full SQ8 arm
must name the inventoried prior SQ8 artifact and its hash; the Qdrant arm must
name the inventoried Qdrant binary and both TreeDB artifacts. The plan paths and
content hashes must resolve back to those unique packet roles, and each plan's
owned output path must be the inventoried result. Qdrant's top-level dataset,
workload, control grid, platform, and CPU-affinity values must exactly equal its
matched-RSS comparison contract. Run argv must consume the plan
with its exact external hash. API-key-bearing or content-identical
outside-inventory substitutions are not valid campaign commands. Use a deliberate
`env -i` wrapper and explicitly record even empty `GOGC`,
`GOMEMLIMIT`, and `GODEBUG`; the consumer requires the runtime, locale,
temporary-directory, hashing, and BLAS controls rather than inheriting an
unknown shell. Set `TZ=UTC`, `LANG=C.UTF-8`, `LC_ALL=C.UTF-8`, `GOWORK=off`,
`PYTHONHASHSEED=0`, `PYTHONDONTWRITEBYTECODE=1`, both BLAS thread variables to
`1`, and a positive explicit `GOMAXPROCS`; each plan must repeat the recorded Go
runtime and BLAS values. All runs exit zero except a declared, structurally valid frozen
quality miss, whose `full_sq8_events` producer exits one. The receipt preserves
reproducibility; the consumer separately validates the output semantics and
never treats the receipt as correctness evidence.

Set `PYTHON` to the absolute interpreter used by the frozen plans. Run the
consumer once into a new output path:

```sh
Q5_PACKET_SHA=$(sha256sum "$Q5_PACKET" | awk '{print $1}')
PATH="$(dirname "$GO"):/usr/bin:/bin" \
PYTHONPATH=benchmarks/vector_db_compare:clients/python/treedb_client/src \
  OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 \
  "$PYTHON" benchmarks/vector_db_compare/minima_cohere_q5_analyze.py \
  --packet "$Q5_PACKET" --expected-packet-sha256 "$Q5_PACKET_SHA" \
  --output "$Q5_ANALYSIS"
```

The consumer returns `qualified`, `valid_unqualified`, or `invalid`. Passing
quality is not sufficient for `qualified`: the reviewed tradeoff must be either
non-material or explicitly accepted by the issue owner. It
independently reconstructs fixed-set quality from retained ordered IDs and the
pinned truth for TreeDB FP32, TreeDB SQ8, and Qdrant FP32; it also requires the
Qdrant exact reference to preserve canonical truth order. It verifies the one
prior SQ8 RSS coordinate was consumed on a fresh
owner whose identity is the first retained service lifetime without an all-row
retune, rebuilds every filtered selection in producer order, revalidates the
pinned RSS artifact against the full plan, binds paired calls to the locked graph
owner, and validates exact smoke/full ledgers for construction, selection, paired
batches, fixed curves, overlap (as a concurrency-order-independent multiset), typed-empty
lifecycle, reopen curves, and final verification. It cross-binds paired log rows
to their embedded artifact, validates paired native evidence with median/MAD
aggregation, recomputes every retained paired-call ID, recall/NDCG, canonical
FP32 score and score/ID order from the frozen vectors, and
recomputes both RSS comparisons. `valid_unqualified` is only the exact typed
quality-miss path before timing/mutation, including the exact ordered failed-cohort
error; cleanup, guard, provenance, schema, an unknown/additional public call, or
evidence defects are `invalid`. Final state is reported only as the reviewed
producer's exhaustive semantic attestation with
`independently_recomputed:false` and `digest_claim:false`.
