# TreeDB VectorDBBench Artifact Harness

Issues: `snissn/gomap#2599`, `#4181`, `#4193`, `#4380`. Parent tracker: `#2598`.

This harness creates a repeatable TreeDB VectorDBBench artifact root. It starts
`treedb-document-service` with a fresh artifact-owned data directory, captures
service logs and host/version context, optionally runs selected TreeDB
VectorDBBench rows from a local `snissn/vectordbbench` checkout on that empty
database, then runs a focused no-document route-proof smoke.

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
  available, Python/Go/OS context, service command and binary SHA-256, health
  response, skips.
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

## Lifecycle completion contract

Issue #4380 extends the existing `treedb-vectordbbench-artifact/v1` manifest;
it does not introduce a second runner. An integrated lifecycle artifact adds a
`manifest.lifecycle` object with schema
`treedb-vectordbbench-lifecycle/v1` and a checksum-bound `lifecycle.jsonl`.
Each JSONL event uses `treedb-vectordbbench-lifecycle-event/v1`, an increasing
sequence and lexical RFC3339 timestamp with timezone, and a state snapshot containing distinct
`client_sent`, `server_accepted`, `server_durable`, and `reopened` row counts.
WAL frontier/total-written counters and the selected cumulative product
counters must also remain monotonic. Every event must repeat the same non-empty
cumulative counter key set. `lifecycle.jsonl` must contain exactly one JSON
object per line and no blank lines.

The strict gate requires these ordered markers:

```text
startup -> reset -> load_start -> load_end -> drain_checkpoint
-> optimize_start -> optimize_end -> cache_prime -> cache_warm
-> graceful_close -> cold_open_ready -> exact_verify -> route_verify -> teardown
```

The integrated runner creates `startup` and a `partial` lifecycle declaration
after the service health gate but before invoking VDBBench. Here `startup`
means the beginning of lifecycle observation, not the operating-system process
launch instant. On a load, optimize, close, reopen, or verification
failure it keeps only structurally complete sidecar boundaries; it never
synthesizes a missing future stage. `graceful_close` is written only after the
first service exits successfully, and terminal `teardown` only after the
cold-reopened service exits successfully. The graceful-close timeout is
configured with `--service-close-timeout` (300 seconds by default for 10M-scale
reuse).

For a completed lifecycle, both `reset` and `load_start` must report zero for
all four row counts. `teardown` must be the final event and must retain the
exact expected row counts. The command-WAL profiles must also report positive
WAL frontier and cumulative bytes at `load_end` after accepted rows and at
`drain_checkpoint` after durable rows; `no_wal_fast` and partial streams are
exempt from this completion proof. Reopened rows remain zero through
`graceful_close` and become populated only after the cold-reopen boundary.
For the currently supported `command_wal_durable` producer, successful insert
responses establish the durable acknowledgement boundary. The diagnostics
snapshot at `load_end` must independently show a positive accepted frontier,
a durable frontier at or beyond it, and positive WAL bytes before the adapter's
`optimize_start`; otherwise the runner fails closed rather than claiming a
checkpoint. Load-end and optimize-start/end stages each use their own sampled
snapshot so offline-build counters cannot be attributed to ingestion.
The canonical sampler runs every five seconds by default and records both the
service snapshot and filesystem WAL bytes/file count from the first pre-load
sample onward. The checksum-bound adapter sidecar retains each equal-size batch
completion; `lifecycle_load_milestones.json` sorts those completions by time and
records cumulative accepted-row time and throughput.

The lifecycle declaration binds the exact clean gomap and VectorDBBench
commits, service-binary SHA-256, effective service/harness configuration,
dataset checksum/dimensions/count, CPU topology, memory, storage, lifecycle
JSONL, raw artifacts, and every profile window. The minimum effective
configuration includes a canonical public service profile, case type,
the exact non-empty service argv using only the document service's defined
flags and with one matching `profile` selector before Go flag parsing
terminates at `--` or a positional argument,
concurrency, batch size, `m`, and `ef_construction`. The argv is part
of the effective-configuration checksum, and its executable must equal the
path of a readable, executable local file whose current bytes match both
recorded service-binary SHA-256 fields. The effective last `dir` flag value
must be non-empty, match `service.data_dir`, and resolve to the artifact-owned
`treedb-data` directory. Standard case names (for
example, `Performance768D1M`) must match the lifecycle dataset dimensions and
vector count. Integer diagnostic flags must also parse within the 64-bit Go
service's signed `flag.Int` range. A nonempty effective `pprof` address must
use an unscoped loopback host and an ASCII-decimal TCP port from 1 through
65535; port zero is excluded because the service does not publish the selected
ephemeral diagnostics port. `PerformanceCustomDataset` additionally binds the
unique canonical result file and task-config checksum, verifies its size and
dimension against the lifecycle declaration, resolves the exact one-file
`train.parquet` or `shuffle_train.parquet` selection, and hashes those actual
dataset bytes. Profile entries
name existing before/after event sequences and use the same checksum as their
raw-artifact entry. Supported profile kinds are `cpu`, `heap`, `allocs`,
`block`, and `mutex` as non-empty gzip-compressed `.pprof` files that the native `go tool pprof`
decoder accepts with matching period and sample metadata. CPU profiles are
distinct and every pprof must contain at least one actual sample; heap and
allocs use the shared Go allocation family but require their
respective sample-type selection (Go 1.26 heap profiles may omit the default
marker, while allocs must mark `alloc_space/bytes`); block and mutex share
indistinguishable Go contention metadata and are validated as that family. Go `trace` is a `.out`
file that the native trace decoder accepts, and Linux `perf` as a `.data` file
with bounded header sections that native `perf script` can decode while walking
samples. Profile validation is an offline correctness gate and invokes the
corresponding native decoder once per profile. The optimized index identity and durable `asset_generation`
must survive close and cold reopen. For a `column_graph`, H2 uses the positive index generation paired
with `column_graph_loaded` and requires the cold-reopened index to report that same generation. For a
`native_runtime` index, H2 uses its positive vector-maintenance root ID. The artifact-owned
database identity and server `commit_seq` must also match across close/reopen,
and route proof must use the same index identity and asset generation without
fallback through either `exact_hnsw_search_pack_v1` or `quantized_rerank`.
`graceful_close.database` is the post-close durable state verified by the first
cold-open diagnostics snapshot, while its event timestamp remains the actual
close-completion time. TreeDB `Close` may advance the commit sequence after the
last live snapshot, so the runner preserves that pre-close snapshot in
`diagnostics.jsonl` and performs no application mutation between cold-open
health and the verifying snapshot.
`T_ready` is reconstructed from `load_start`
through `cold_open_ready`; client, accepted/durable, and reopened counts are
never substituted for one another, and a completed lifecycle requires strictly
positive `T_ready`.

When `graceful_close` or `cold_open_ready` is present, its database snapshot
must contain a non-empty string identity and non-negative integer commit
sequence. A partial stream that has not reached those stages remains
analyzable; a present malformed snapshot does not.
The same fail-closed rule applies to a present index snapshot's object and
identity, asset-generation, and status field types; missing future index
evidence remains a completion error.
Any present route snapshot is likewise structurally validated as an object
with typed route-proof fields, while a route that has not yet been emitted is
only missing completion evidence. Once `route_verify` is emitted, all five
route-proof fields are structurally required.

Lifecycle validation requires the pinned Go toolchain for pprof and trace
profiles, and Linux `perf` for perf-data profiles. A missing native decoder is
a structural validation error rather than an invalid-profile diagnosis.

Validate a completed artifact with:

```sh
python3 scripts/treedb_vectordbbench_artifact.py \
  --validate-lifecycle "$OUT"
```

The command exits nonzero for partial, interrupted, stale, mismatched, or
corrupt artifacts. `--allow-partial` returns success only when a partial or
interrupted artifact is structurally analyzable; its JSON result still has
`complete: false`. Existing smoke-only artifact-v1 output remains unchanged
unless `--lifecycle` is selected.

Canonical timed lifecycle artifacts include a low-overhead heap snapshot.
CPU, allocation, block, mutex, trace, and Linux perf captures are separate
aligned diagnostic companions: each must declare its nearest lifecycle
before/after state window and is not silently included in the canonical wall
measurement.

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

Use `--skip-route-proof` only for measurement repetitions when an independently
captured route-proof artifact already gates the same binary and configuration.
This keeps the timed database limited to the VDBBench collection. The flag is
rejected for dry runs, skipped loads, or an empty row list.

Run one VDBBench row per artifact when comparing empty-database load/build
phases. Multiple requested rows share the artifact-owned service and data
directory, so only the first row starts from an empty database.

The generated VDBBench commands use unique index names derived from
`--index-prefix` (or a timestamped default) and set `RESULTS_LOCAL_DIR` to
`$OUT/vdbbench-results/exact` or `$OUT/vdbbench-results/scalar` plus `LOG_FILE`
to `$OUT/vdbbench.log`. The exact row is
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
SHA-256, run ID, task configuration and its canonical JSON SHA-256, insert
duration, offline optimize duration, total load duration, and
`throughput_vector_count / insert_duration` in
`vdbbench_load_metrics.json`. It fails closed if selection is ambiguous, a
case is unsuccessful, a duration is absent/non-positive, the three durations
disagree, a positive reported `inserted_count` differs from the expected dataset
size, or the case type does not end in a count suffix such as `50K` or `1M`.
The current VDBBench performance result schema emits `inserted_count=0` as a
sentinel after a successful full-dataset load; the artifact records whether its
throughput numerator came from a positive reported count or that explicit
full-dataset contract.
The profile is deliberately full-load only: phase-specific pprof would require
VDBBench orchestration not owned by this harness.

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
| `TREEDB_VDBBENCH_SKIP_ROUTE_PROOF` | omit route proof from measurement-only repetitions | `false` |
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
