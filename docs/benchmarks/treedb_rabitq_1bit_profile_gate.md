# TreeDB `rabitq_1bit` Profile Gate

Use this workflow for Sublane A of the RaBitQ performance lane (#2476 and
successors). It builds on the quantized buffered per-row profiling workflow from
issue #2465 while making the `rabitq_1bit` evidence contract explicit.

This gate is measurement-only. It must not be bundled with scorer/search runtime
optimization, storage format changes, codec identity changes, asset identity
changes, fail-closed behavior changes, or `rabitq_1bit` v1 score/LSB-first bit
order changes.

## What this gate proves

The workflow captures isolated lower-level and collection buffered rows for a
shape-parameterized quantized-search fixture. The default remains the historical
1024 row / 128 dimension / `topK=10` / `efSearch=128` fixture, and
`SHAPE=10k_x_1536` selects the high-dimensional profile contract needed by
#2515. It records:

- same-host baseline and candidate commit identity;
- `ns/op`, `ops/sec`, `B/op`, `allocs/op`, and recall@K;
- route, exact-read, asset-unavailable, fallback, and cache counters;
- `quantized_code_B/search`, `quantized_code_B/vector`, and
  `quantized_asset_B/vector`;
- CPU and allocation profiles for selected c=1/c=8 rows. By default these
  use benchmark-controlled search-loop hooks that start after fixture setup,
  vector-index rebuild, collection prepared-cache warmup, and worker warmup.

Smoke runs validate benchmark shape and counters only. They are not speedup or
promotion evidence.

## Script

```sh
scripts/treedb_rabitq_1bit_profile_gate.sh
```

Default selection:

- `SHAPE=1024x128`: the historical fixed gate shape. Set `SHAPE=10k_x_1536`
  for the #2515 high-dimensional contract. The underlying benchmark also
  accepts `BENCH_ROWS`, `BENCH_DIMS`, `BENCH_M`, `BENCH_TOP_K`,
  `BENCH_EF_SEARCH`, and `BENCH_QUERY_ORDINAL` overrides.
- `ROWS=claim_core`: RaBitQ `quantized_only` c=1/c=8 lower-level and collection
  rows plus scalar_u8 `quantized_only` c=1/c=8 guardrail rows.
- `PROFILE_ROWS=rabitq_collection_quantized_only_c1,rabitq_collection_quantized_only_c8`:
  CPU/alloc profiles for the required RaBitQ collection buffered target rows.
- `BENCHTIME=100000x` for the default shape, `BENCHTIME=1000x` for
  `SHAPE=10k_x_1536`, `TIMING_COUNT=5`, `PROFILE_COUNT=1`.
- `PROFILE_SCOPE=search_loop`: default clean pprof mode. The script sets
  `TREEDB_COLUMN_GRAPH_QUANTIZED_HOT_CPU_PROFILE_PATH`,
  `TREEDB_COLUMN_GRAPH_QUANTIZED_HOT_ALLOCS_PROFILE_PATH`, and
  `TREEDB_COLUMN_GRAPH_QUANTIZED_HOT_ALLOCS_BASE_PROFILE_PATH` for one isolated
  RaBitQ benchmark subrow and does **not** pass Go's test-level `-cpuprofile` /
  `-memprofile` flags. `PROFILE_SCOPE=search_loop` rejects non-RaBitQ profile
  rows up front; use `PROFILE_SCOPE=go_test` only for the legacy compatibility
  mode that includes setup/rebuild attribution or for scalar guardrail profiles.
- `HOT_MEM_PROFILE_RATE=1`: allocation sampling rate used during the timed
  search loop and preserved while `allocs_raw.pprof` is serialized so Go pprof
  scales sampled allocations correctly. Raise it only to reduce allocation
  profiling overhead; the guardrail timing row remains the source of truth for
  `B/op` and `allocs/op`.
- `ALLOC_PROFILE_IGNORE`: optional pprof regexp used only by `PROFILE_SCOPE=search_loop`
  to filter allocation-profile writer frames from the diffed `allocs.pprof`.
- `RECALL_TOLERANCE_PCT=0`: when `BASELINE_DIR` is set, candidate median recall must be at least the matching baseline row's median recall minus this tolerance for the row guardrail to pass.

Useful selectors for `ROWS` and `PROFILE_ROWS` are comma-separated and ORed:

- `claim_core`, `claim_rerank`, `all`;
- `rabitq`, `rabitq_only`, `rabitq_rerank`;
- `scalar_guardrail`, `scalar_only`, `scalar_rerank`;
- `lower`, `collection`, `quantized_only`, `rerank32`, `c1`, `c8`;
- exact row IDs from the table below.

Use `DRY_RUN=true` to write context, matrix, row directories, and exact commands
without running Go benchmarks.

## Row and artifact names

Each selected row gets a directory named by `row_id` under `RUN_DIR`. Use a fresh `RUN_DIR` for claim-quality runs; the generated summary is limited to the current `matrix.tsv` rows, but stale directories from reused run roots should still be treated as archival clutter, not current evidence.

| row id | codec | API boundary | mode | c | benchmark regex |
| --- | --- | --- | --- | ---: | --- |
| `rabitq_lower_quantized_only_c1` | `rabitq_1bit` | `VectorIndexSearcher.SearchWithBuffer` | `quantized_only` | 1 | `^BenchmarkVectorIndexSearcherColumnGraphRabitQQuantizedSearchWithBuffer2451$/^route=quantized_only$/^c=1$` |
| `rabitq_lower_quantized_only_c8` | `rabitq_1bit` | `VectorIndexSearcher.SearchWithBuffer` | `quantized_only` | 8 | `^BenchmarkVectorIndexSearcherColumnGraphRabitQQuantizedSearchWithBuffer2451$/^route=quantized_only$/^c=8$` |
| `rabitq_collection_quantized_only_c1` | `rabitq_1bit` | `Collection.SearchVectorIndexWithBuffer` | `quantized_only` | 1 | `^BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphRabitQQuantized2452$/^route=quantized_only$/^c=1$` |
| `rabitq_collection_quantized_only_c8` | `rabitq_1bit` | `Collection.SearchVectorIndexWithBuffer` | `quantized_only` | 8 | `^BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphRabitQQuantized2452$/^route=quantized_only$/^c=8$` |
| `rabitq_lower_quantized_rerank32_c1` | `rabitq_1bit` | `VectorIndexSearcher.SearchWithBuffer` | `quantized_rerank/candidates=32` | 1 | `^BenchmarkVectorIndexSearcherColumnGraphRabitQQuantizedSearchWithBuffer2451$/^route=quantized_rerank$/^candidates=32$/^c=1$` |
| `rabitq_lower_quantized_rerank32_c8` | `rabitq_1bit` | `VectorIndexSearcher.SearchWithBuffer` | `quantized_rerank/candidates=32` | 8 | `^BenchmarkVectorIndexSearcherColumnGraphRabitQQuantizedSearchWithBuffer2451$/^route=quantized_rerank$/^candidates=32$/^c=8$` |
| `rabitq_collection_quantized_rerank32_c1` | `rabitq_1bit` | `Collection.SearchVectorIndexWithBuffer` | `quantized_rerank/candidates=32` | 1 | `^BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphRabitQQuantized2452$/^route=quantized_rerank$/^candidates=32$/^c=1$` |
| `rabitq_collection_quantized_rerank32_c8` | `rabitq_1bit` | `Collection.SearchVectorIndexWithBuffer` | `quantized_rerank/candidates=32` | 8 | `^BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphRabitQQuantized2452$/^route=quantized_rerank$/^candidates=32$/^c=8$` |
| `scalar_lower_quantized_only_c1` / `c8` | `scalar_u8` | lower-level guardrail | `quantized_only` | 1/8 | `BenchmarkVectorIndexSearcherColumnGraphScalarU8QuantizedSearchWithBuffer2414` subrows |
| `scalar_collection_quantized_only_c1` / `c8` | `scalar_u8` | collection guardrail | `quantized_only` | 1/8 | `BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphScalarU8Quantized2415` subrows |
| `scalar_*_quantized_rerank32_c1` / `c8` | `scalar_u8` | rerank guardrail when rerank is in scope | `quantized_rerank/candidates=32` | 1/8 | scalar_u8 rerank subrows |

Primary artifacts:

- `context.txt`: branch, commit, Go version/env, `GOMAXPROCS`, `GOWORK`, shape,
  fixture knobs, uptime, git status, and visible competing benchmark/test
  processes.
- `matrix.tsv`: selected row IDs, codec, layer, mode, c, rerank candidates,
  profile selection, shape, fixture knobs, and regex.
- `summary.md` / `summary.tsv`: median timing, throughput, allocation, recall,
  byte/counter summaries, and guardrail status.
- `<row>/bench_timing.txt`: unprofiled timing/counter output. Use this for
  tables.
- `<row>/bench_profile.txt`: profiled benchmark output for profiled rows.
- `<row>/cpu.pprof`, `<row>/allocs.pprof`, `<row>/cpu_top.txt`,
  `<row>/allocs_top.txt`: required CPU/allocation artifacts for profiled rows.
  In the default `PROFILE_SCOPE=search_loop`, these exclude setup/rebuild and
  cover only the timed search loop. `allocs.pprof` is generated from
  `<row>/allocs_raw.pprof` minus `<row>/allocs_base.pprof`, then filtered with
  `ALLOC_PROFILE_IGNORE` to remove allocation-profile writer noise such as
  `runtime/pprof` and `compress/gzip`; it is expected to be empty or
  runtime-noise-only when the row remains `0 B/op`, `0 allocs/op`.
- `<row>/allocs_base.pprof`, `<row>/allocs_raw.pprof`,
  `<row>/allocs_diff_raw.pprof`: supporting profiles for `PROFILE_SCOPE=search_loop`
  allocation diffing and filter audits.
- `<row>/block.pprof`, `<row>/mutex.pprof`, top summaries: emitted only by
  `PROFILE_SCOPE=go_test` legacy mode.
- `<row>/pprof_lists/*.txt`: supporting line-level CPU attribution.

Recommended raw directory naming:

```sh
/tmp/gomap_rabitq_1bit_<baseline|candidate>_<branch>_<shortsha>_$(date +%Y%m%d_%H%M%S)
```

## Smoke and dry-run commands

Dry-run script validation:

```sh
DRY_RUN=true ROWS=rabitq_collection_quantized_only_c1 \
  scripts/treedb_rabitq_1bit_profile_gate.sh
```

Tiny smoke for one row:

```sh
RUN_DIR=/tmp/gomap_rabitq_1bit_smoke_$(date +%Y%m%d_%H%M%S) \
  ROWS=rabitq_collection_quantized_only_c1 \
  BENCHTIME=1000x TIMING_COUNT=1 RUN_PROFILES=false \
  GOMAXPROCS=8 GOWORK=off \
  scripts/treedb_rabitq_1bit_profile_gate.sh
```

Issue-level smoke matrix (shape/counters only; no speed claim):

```sh
GOMAXPROCS=8 GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench '^(BenchmarkVectorIndexSearcherColumnGraphRabitQQuantizedSearchWithBuffer2451|BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphRabitQQuantized2452|BenchmarkVectorIndexSearcherColumnGraphScalarU8QuantizedSearchWithBuffer2414|BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphScalarU8Quantized2415|BenchmarkColumnGraphScalarU8QuantizedScorePlanes1926)$' \
  -benchmem -benchtime=100x -count=3
```

#2515 `10k_x_1536` RaBitQ collection profile smoke/contract path:

```sh
RUN_DIR=/tmp/gomap_rabitq_1bit_10k_x_1536_$(date +%Y%m%d_%H%M%S) \
  BENCHMARK_LOCK=/tmp/gomap_2538_benchmark.lock \
  PROFILE_SCOPE=search_loop \
  SHAPE=10k_x_1536 \
  ROWS=rabitq_collection_quantized_only_c1,rabitq_collection_quantized_only_c8,rabitq_collection_quantized_rerank32_c1,rabitq_collection_quantized_rerank32_c8 \
  PROFILE_ROWS=rabitq_collection_quantized_only_c1,rabitq_collection_quantized_only_c8,rabitq_collection_quantized_rerank32_c1,rabitq_collection_quantized_rerank32_c8 \
  TIMING_COUNT=1 PROFILE_COUNT=1 BENCHTIME=1000x \
  GOMAXPROCS=8 GOWORK=off \
  lockf /tmp/gomap_2538_benchmark.lock scripts/treedb_rabitq_1bit_profile_gate.sh
```

On Linux hosts, use `flock /tmp/gomap_2538_benchmark.lock -c '<same env-prefixed command>'`
if `lockf` is unavailable.

## Claim-quality same-host baseline/candidate workflow

Run baseline and candidate on the same host, with the same Go toolchain,
`GOMAXPROCS`, `GOWORK`, row selection, timing/profile counts, fixture, and as
little wall-clock separation as practical. Record both branch and full commit
SHA from each `context.txt`.

Baseline:

```sh
BASE=/tmp/gomap_rabitq_1bit_baseline_main_$(git rev-parse --short HEAD)_$(date +%Y%m%d_%H%M%S)
RUN_DIR="$BASE" \
  ROWS=claim_core \
  PROFILE_ROWS=rabitq_collection_quantized_only_c1,rabitq_collection_quantized_only_c8 \
  BENCHTIME=100000x TIMING_COUNT=5 PROFILE_COUNT=1 \
  GOMAXPROCS=8 GOWORK=off \
  scripts/treedb_rabitq_1bit_profile_gate.sh
```

Candidate:

```sh
CAND=/tmp/gomap_rabitq_1bit_candidate_$(git rev-parse --short HEAD)_$(date +%Y%m%d_%H%M%S)
RUN_DIR="$CAND" BASELINE_DIR="$BASE" \
  ROWS=claim_core \
  PROFILE_ROWS=rabitq_collection_quantized_only_c1,rabitq_collection_quantized_only_c8 \
  BENCHTIME=100000x TIMING_COUNT=5 PROFILE_COUNT=1 \
  GOMAXPROCS=8 GOWORK=off \
  scripts/treedb_rabitq_1bit_profile_gate.sh
```

When rerank scorer/read behavior is in scope, include rerank rows and profiles:

```sh
ROWS=claim_rerank \
PROFILE_ROWS=rabitq_collection_quantized_only_c1,rabitq_collection_quantized_only_c8,rabitq_collection_quantized_rerank32_c1,rabitq_collection_quantized_rerank32_c8 \
  scripts/treedb_rabitq_1bit_profile_gate.sh
```

If lower-level scorer-only evidence is the claim, keep lower-level RaBitQ c=1/c=8
rows in the timing table. Collection rows must still be present so lower-level
wins that regress the collection seam are no-promote.

## Summary table conventions

Use `summary.tsv` or `summary.md` to report medians from unprofiled timing rows.
PR or issue evidence should include at least:

| field | rule |
| --- | --- |
| baseline/candidate identity | branch, full SHA, Go version, OS/arch, `GOMAXPROCS`, fixture |
| timing/allocation | `ns/op`, `ops/sec`, `B/op`, `allocs/op` |
| quality | recall@K versus exact, c=1 and c=8 shown separately |
| bytes | `quantized_code_B/search`, `quantized_code_B/vector` (shape-aware: scalar_u8=`dims`, RaBitQ=`ceil(next_power_of_two(dims)/8)`), `quantized_asset_B/vector`, exact vector/norm bytes for rerank |
| route counters | `search_route_quantized_only/search`, `search_route_quantized_rerank/search`, `search_route_column_graph_prepared/search` |
| failure/fallback counters | document fetches, graph/typed/scratch/float64 fallbacks, quantized asset missing/invalid/stale/closed/unavailable |
| collection seam | cache hits/misses/waits/errors, `open_setup_in_timed_loop=0`, `open_searcher_calls/op=0` |
| profiles | `cpu_top.txt` and `allocs_top.txt` summaries for c=1/c=8 target rows |

## Guardrails

A row is not promotable unless its raw benchmark output and generated summary
show:

- `0 B/op` and `0 allocs/op` for steady-state buffered search;
- `docs_fetched/search=0`, no graph-row fallback, no typed-column fallback, no
  scratch decode, and no float64 scorer fallback;
- selected quantized asset counters do not report missing, invalid, stale,
  closed, or unavailable assets;
- `quantized_only` has zero exact vector/norm bytes, zero exact rerank score
  calls, and zero prepared exact score calls;
- `quantized_rerank/candidates=32` exact vector/norm bytes and exact score calls
  stay bounded by the shortlist;
- c=1 and c=8 both preserve recall and route counters;
- scalar_u8 guardrail rows stay allocation-free and do not materially regress
  when shared code is touched.

### Exact FP32 guardrail guidance for shared code

If a PR touches shared traversal, frontier/top-k, collection prepared cache,
row-ref/result-ID finalization, search buffer shapes, or result materialization,
RaBitQ and scalar_u8 rows are not enough. Add exact FP32 rows from the active
vector-search gate for that boundary (for example the #2445/#2399 HNSW-pack or
Tier-S exact rows), and report them in the same before/after table. Exact rows
must prove default FP32 behavior is unchanged, retain exact route/fallback
counters, avoid new allocations, and keep vector/norm byte reads within the
expected exact-search boundary. Do not hide exact regressions behind RaBitQ-only
wins.

## Quiet-host and no-promote policy

Read `context.txt` before using a run for claims. Reject and rerun if it shows
high load, competing benchmark/test processes, thermal throttling suspicion, or
large unexplained per-row variance. Run baseline and candidate adjacent in time
on the same host.

Promotion is blocked by any material regression in target or guardrail rows:

- c=1 improves but c=8 regresses, or vice versa;
- lower-level improves but collection buffered search regresses;
- RaBitQ improves but scalar_u8 or exact FP32 shared guardrails regress;
- allocations leave `0 B/op` / `0 allocs/op`;
- recall, route, exact-read, fallback, or asset-unavailable counters regress;
- evidence is mixed, noisy, unrepeatable, or current-only.

Mixed/noisy/regressing candidates are **no-promote**. Leave a durable issue/PR
comment with raw artifact paths and do not request AI reviewers for performance
promotion until same-host evidence is coherent.
