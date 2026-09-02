# Issue #1848 Integration Status

Updated: 2026-05-26

## Branch / head

- Branch: `snissn/1848-manager`
- Base: `origin/main` / `1b74b546cb33fc7f94fa74ea08fb62d2e147d9ce`
- Current PR-readiness head before this C5 status update: `95dd067a6`
- C4 benchmark artifact commit: `fbdb4587f` (`docs: add 1848 vector benchmark artifacts`)

## Integrated commits

- C1 capability substrate:
  - `548e5ff16` `typed columns: clarify vector adjacency capabilities`
  - `eb3d21e39` `fix: require dense vector adjacency dimensions`
  - `ef5f3ea3a` `fix: reject zero-row dense layouts without dimensions`
- C2 direct-view validation:
  - `c16391c0b` `treedb: share dense direct-view validation`
- C3 graph prepared/counters/filtering:
  - `a34ee22d7` `feat: add graph prepared counters and filtering`
  - `33160d247` `fix: count graph upper-layer visits`
- C4 benchmarks/profiles/docs:
  - `fbdb4587f` `docs: add 1848 vector benchmark artifacts`
- C5 readiness / status:
  - `7b1696018` `test: update typed storage naming allowlist`
  - `0c4354e72` `test: allow vector graph row-selection seam`
  - `f4055391d` `docs: record 1848 integration status`
  - `95dd067a6` `docs: update 1848 final validation status`
  - this commit: C5 PR-readiness status / PR body source update

## Review status

- C1: PASS after focused manager validation.
  - Original blocker: missing vector dimensions / adjacency degree could become layout-capability eligible.
  - Fixes now fail closed without positive `FixedWidthElements`, including zero-row granule validation.
- C2: PASS.
  - Dense direct-view validation is shared through `TreeDB/internal/typeddecode` and fails closed to scratch/materializing paths when certification, length, alignment, row count, or lifetime checks fail.
- C3: PASS after focused manager validation.
  - Upper-layer graph scoring is counted in `visited_nodes`.
  - Vector demo markdown includes direct/scratch decode counters.
- C4: PASS / integrated.
  - Benchmarks and CPU/allocation profiles are under `artifacts/1848/bench/`.

## Validation run for C5 PR readiness

Focused validation passed:

```sh
go test -count=1 ./TreeDB/internal/columnsemantics ./TreeDB/internal/columnlayout ./TreeDB/internal/typeddecode

go test -count=1 ./TreeDB/collections -run 'Semantic|Capability|Unsupported|Vector|Adjacency|ColumnVectorGraphNativeSearch|VectorIndex|TypedColumnPrepared|SearchVectorIndex|TypedColumnAdapter|TypedStorageLegacyNameAllowlist'

go test -count=1 ./cmd/treedb_vector_demo ./cmd/treedb_column_graph_demo
```

Broad validation passed:

```sh
go test -count=1 ./TreeDB/... ./cmd/treedb_vector_demo ./cmd/treedb_column_graph_demo
```

Latest-head benchmark guardrail passed on `darwin/arm64`, Apple M3:

```sh
go test ./TreeDB/collections -run '^$' -bench 'ColumnVectorGraphNativeSearchCosine(TypedColumn)?V3|TypedColumnAdjacencyDenseDirectViewScan' -benchmem -count=1

go test ./TreeDB/internal/typedcolumn -run '^$' -bench 'Dense.*DirectView|Dense.*Section|DenseFloat32Dot' -benchmem -count=1
```

## Before / after performance evidence

Primary graph-search comparison from latest-head benchmark guardrail:

| Path | Benchmark | ns/op | ops/sec | B/op | allocs/op | vector direct views/search | vector scratch decodes/search | candidates/search | visited nodes/search |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Before / shared substrate off | `BenchmarkColumnVectorGraphNativeSearchCosineV3` | 24,623 | 40,612 | 8,904 | 212 | 0 | 164 | 128 | 164 |
| After / typed-column direct view | `BenchmarkColumnVectorGraphNativeSearchCosineTypedColumnV3` | 24,488 | 40,836 | 8,904 | 212 | 164 | 0 | 128 | 164 |

Delta: typed-column direct view was ~0.5% faster in this local run, with identical `B/op`, `allocs/op`, candidate count, and visited-node count while replacing vector scratch decodes with direct views.

Vector payload direct-view comparison from latest-head benchmark guardrail:

| Path | Benchmark | ns/op | ops/sec | B/op | allocs/op | notes |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| Before / materializing section scan | `BenchmarkTypedColumnVectorDenseSectionScan` | 27,068 | 36,944 | 131,169 | 6 | materializes dense vector section |
| After / direct view scan | `BenchmarkTypedColumnVectorDenseDirectViewScan` | 11,635 | 85,947 | 0 | 0 | `direct_views/op=1`, `scratch_decodes/op=0` |

Delta: direct-view scan was ~57.0% lower `ns/op`, ~132.6% higher ops/sec, and removed per-op allocation.

Adjacency direct-view guardrail from latest-head benchmark: `BenchmarkTypedColumnAdjacencyDenseDirectViewScan` = 35,122 ns/op, 14,927.71 MB/s, `0 B/op`, `0 allocs/op`.

C4 profile artifacts in `artifacts/1848/bench/summary.md` provide the corresponding CPU/memory profile interpretation and the original C4 run details.

## PR readiness notes

- PR scope is limited to vector/adjacency typed-column shared-substrate integration.
- No scalar aggregate/range semantics are introduced for vector or adjacency columns.
- Optimized vector/graph search paths do not materialize full documents unless requested.
- Do not merge until coordinator approves.
