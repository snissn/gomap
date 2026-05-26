# Issue #1848 Integration Status

Updated: 2026-05-25

## Integrated head

- Branch: `snissn/1848-manager`
- Base: `1b74b546cb33fc7f94fa74ea08fb62d2e147d9ce`
- Integrated head for C4: `ef5f3ea3a` (`fix: reject zero-row dense layouts without dimensions`)

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
- Integration/test maintenance:
  - `7b1696018` `test: update typed storage naming allowlist`

## Review status

- C1: PASS after focused re-review.
  - Original review found fail-open behavior for missing vector dimensions / adjacency degree.
  - Manager fixes now reject vector/adjacency capabilities without positive `FixedWidthElements`, including zero-row granule validation.
- C2: PASS.
- C3: PASS after focused re-review.
  - Manager fix counts upper-layer graph scoring in `visited_nodes` and updates public comments/tests.
  - Vector demo markdown now includes direct/scratch decode counters.

## Validation run

Passed:

```sh
go test -count=1 ./TreeDB/internal/columnsemantics ./TreeDB/internal/columnlayout ./TreeDB/internal/typeddecode ./TreeDB/collections ./cmd/treedb_vector_demo ./cmd/treedb_column_graph_demo
```

Additional focused reviewer/manager runs passed for:

```sh
go test -count=1 ./TreeDB/internal/columnsemantics ./TreeDB/internal/columnlayout ./TreeDB/collections -run 'Semantic|Capability|Unsupported|Vector|Adjacency|TypedStorageLegacyNameAllowlist'
go test -count=1 ./TreeDB/collections -run 'ColumnVectorGraphNativeSearch|VectorIndex|TypedColumnPrepared|SearchVectorIndex'
go test -count=1 ./cmd/treedb_vector_demo ./cmd/treedb_column_graph_demo
```

## C4 / PR notes

- C1-C3 are integrated and reviewed; C4 benchmark/docs work is unblocked from head `ef5f3ea3a`.
- When opening/updating the PR, include before/after performance evidence: baseline/head refs, exact benchmark/profile commands, and summarized deltas.
