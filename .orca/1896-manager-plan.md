# #1896 manager plan — certified typed-column-part direct-view readers

## Dependency and start-phase inventory

- Base: `origin/main`/`snissn/1896-manager` at `37d3cb2b1e30a7c2ddb9bce59a05e323d2c69900` (#1895 merge).
- Verified prerequisites:
  - #1893 / PR #1902 merged (`29dd3cb3857baff8b7889c21b8be30957f9e9bc2`).
  - #1737 / PR #1905 merged (`7dc8eb7c895ff198280660348ee9cd6ed0287241`).
  - #1895 / PR #1907 merged (`37d3cb2b1e30a7c2ddb9bce59a05e323d2c69900`).
- Issue/tracker read: #1896 and #1886 bodies/comments. Current scope is typed-column-part fixed-width scalar/vector readers only: raw int64, native raw float32, native raw double/float64, fixed-dim float32_vector. `adjacency_list` and physical row assets are deferred/fallback-only.

## Reader path inventory (start phase)

| Path | Files | Current state / #1896 action |
| --- | --- | --- |
| Shared direct-view validation | `TreeDB/internal/typeddecode/plan.go`, `TreeDB/internal/mappedresource/typed_view.go` | Existing int64/vector/adjacency validation vocabulary and handle checks; add native scalar float plans/views, source counters, stronger tests. |
| Prepared int64 predicate aggregate/reducer | `TreeDB/collections/typed_column_prepared_int64.go`, `typed_column_int64_scan.go`, `typed_column_prepared_state.go` | Existing certified direct-view candidate over raw int64 blocks; add source/reason counters, fallback accounting, stale session tests, absolute-offset test coverage. |
| Generic typed-column adapter materialization/reconstruction | `typed_column_adapter.go`, `typed_column_publication.go`, `document_materializer.go` | Uses typed-column-part decoded values and copies into document values. Keep semantics; add lower-level certified direct-view helper/tests where hot paths can consume section handles safely. |
| Native scalar float reader fixtures | `typed_column_adapter.go`, `typed_column_float_fallback_test.go`, `typeddecode` tests | Writer emits native raw_float32/raw_float64 when `fixed_width_encoding=little_endian`; add certified reader plans/views and raw-bit tests. Raw-int64 logical float carriers remain fallback/compat only. |
| Fixed-dim float32_vector typed-column source | `column_vector_graph_typed_column.go` | Already validates certification and handle before direct-viewing section; keep within #1896 safety helper scope, avoid #1898 graph search counter expansion. Add stale/absolute/fallback tests if needed. |
| Adjacency/list | `typed_column_adapter.go`, `column_vector_graph_block_view.go`, conformance tests | Explicitly deferred to #1901. Ensure plan/refusal reason is stable and tests prove no accidental direct view. |
| Bool/string/dictionary/nullable/default/compressed/delta | `typed_column_direct_view_contract.go`, `typed_column_semantics_test.go`, typeddecode/layout tests | Fallback-only; ensure counters/reasons and direct-view validation fail closed. |

## Subtask chunks

| ID | Scope | Files/packages | Dependencies | Tests/benchmarks/evidence | Executor thinking | Reviewer thinking |
| --- | --- | --- | --- | --- | --- | --- |
| A | Shared typeddecode native scalar float direct-view plans/views, source counters, reason mapping, raw-bit/validation tests. | `TreeDB/internal/typeddecode/*`, `TreeDB/internal/mappedresource/*` | #1737 native scalar LE encodings, #1895 certification fields | `go test ./TreeDB/internal/typeddecode ./TreeDB/internal/mappedresource`; checkptr if feasible | high (unsafe/lifetime) | xhigh (safety/lifetime) |
| B | Collections reader integration/accounting for prepared int64 direct-view path and vector typed-column section helper; add source/reason counters without #1898 search expansion. | `TreeDB/collections/typed_column_prepared_int64.go`, `typed_column_int64_scan.go`, `column_vector_graph_typed_column.go` | A | Focused collections tests; new stale/absolute-offset/counter tests | high | xhigh |
| C | Scalar float/vector/adjacency conformance tests: raw-bit float32/float64 direct views, missing/wrong/corrupt fallback/fail-closed, adjacency deferred. | `TreeDB/collections/typed_column_direct_view_conformance_test.go`, `typed_column_adapter_test.go`, `typed_column_float_fallback_test.go` | A, B | Focused test regex for direct-view/float/fallback | high | high |
| D | Benchmarks/evidence and PR docs: before/after commands, counters table, no-scope-drift review. | benchmark tests under `TreeDB/collections`, PR body | A-C | `go test -bench ... -benchmem`; full `go test ./TreeDB/internal/typeddecode ./TreeDB/collections` if time | medium | xhigh |

## Close-phase evidence

- Implemented typeddecode native scalar float plans/views, stricter fixed-width certification validation, and raw-bit/stale/unaligned tests.
- Implemented prepared int64 source/reason counters, mmap vs heap-copy typed-view accounting, scratch-decode accounting, production absolute-offset fallback and heap-copy typed-view tests.
- Added certified scalar float resource readers and tests; retained adjacency as fallback-only/deferred and fixed typed-vector source close/stale-slice exposure.
- Local validation:
  - `go test ./TreeDB/internal/typeddecode ./TreeDB/internal/mappedresource ./TreeDB/collections`
  - `go test -gcflags=all=-d=checkptr=2 ./TreeDB/internal/typeddecode ./TreeDB/internal/mappedresource ./TreeDB/collections -run 'Test.*DirectView|TestTypedColumnAdapterNativeFixedWidthScalarByteFixtures|TestColumnVectorGraphTypedColumnVectorCloseReleasesHandles1782|TestTypedColumnInt64PreparedHeapCopyTypedViewCounters|TestTypedColumnInt64PreparedAbsoluteOffsetUnalignedFallsBack'`
  - `go test ./...`
  - `TREEDB_TYPED_COLUMN_BENCH_LAYOUTS=raw TREEDB_TYPED_COLUMN_BENCH_ROWS=4096 TREEDB_TYPED_COLUMN_BENCH_SHAPES=selective_range_1pct TREEDB_TYPED_COLUMN_BENCH_DISTS=clustered_monotonic go test ./TreeDB/collections -run '^$' -bench '^BenchmarkTypedColumnInt64PredicateAggregate' -benchtime=100ms -count=1`

## Scope guards

- Do not change on-disk writer layouts beyond tiny test fixtures.
- Do not implement row-asset or adjacency direct views; report adjacency as deferred/fallback (#1901), row assets as #1897 deferred.
- Do not claim raw-int64 float carriers as native scalar float direct views.
- Unsafe typed slices remain internal and tied to mappedresource handles/sessions; released handles/sessions must fail closed.
