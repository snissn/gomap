# Column Vector Dynamic Overlay Benchmarks

This prototype track measures a dynamic graph search path while column-store
persistence work continues. Readers share one immutable `ColumnVectorGraph`
base snapshot. Writers publish copy-on-write overlay generations containing
appended vectors, deleted overlay rows, and sorted base-document tombstones.

Search scope:

- Atomically load one base generation plus one overlay generation.
- Run base `SearchCosine` with caller-owned warmed scratch.
- Exact-scan live overlay vectors without fetching full documents.
- Filter tombstoned base documents and merge top-k results.
- Return result document IDs that alias immutable snapshot storage.

Primary benchmark smoke:

```sh
GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench 'BenchmarkColumnVectorDynamicGraphSearchCosineScale/rows_100k' \
  -benchmem \
  -benchtime=500ms \
  -count=5
```

The benchmark shapes are:

- `rows_100k_dims_128_degree_16/parallel_read_only`
- `rows_100k_dims_128_degree_16/parallel_read_write`
- `rows_1m_dims_128_degree_16/parallel_read_only`
- `rows_1m_dims_128_degree_16/parallel_read_write`

Run with `-short` to skip the 1M shape. Setup, base graph build, overlay seed,
query vector selection, and writer mutation-batch construction are outside the
timed loop.

Concurrency discipline:

- `ColumnVectorDynamicGraph` publishes immutable snapshots through an atomic
  pointer.
- `ColumnVectorGraph` and overlay snapshot buffers are read-only for readers.
- Each benchmark worker owns one warmed
  `ColumnVectorDynamicGraphSearchScratch`.
- The read-only benchmark is expected to stay at `0 B/op` and `0 allocs/op`.
- The read-write benchmark includes writer copy-on-write publish cost in
  `B/op` and `allocs/op`; use `TestColumnVectorDynamicGraphSearchAllocs` as
  the hot read allocation guard.

Reported metrics separate the major costs:

- `base_candidates/search`, `edges/search`, and `edges/node` cover immutable
  base graph traversal.
- `overlay_scanned/search`, `overlay_rows`, `overlay_live_rows`, and
  `overlay_tombstones` cover the exact-scan overlay.
- `merge_candidates/search` and `base_tombstoned/search` cover tombstone and
  merge behavior.
- `read_qps`, `publishes/s`, `mutations/s`, and `publish_ns/op` cover
  concurrent read/write behavior.
- `base_payload_bytes` and `overlay_payload_bytes` approximate the in-memory
  payload footprint.

Local M11 follow-up evidence:

```sh
GOWORK=off go test ./TreeDB/collections \
  -run 'TestColumnVectorDynamicGraph|TestColumnVectorDynamicOverlay|TestColumnVectorGraph' \
  -count=1

GOWORK=off go test -race ./TreeDB/collections \
  -run 'TestColumnVectorDynamicGraphConcurrentReadersAndWriter|TestColumnVectorDynamicGraphSearchTombstonesAndOverlay' \
  -count=1

GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench 'BenchmarkColumnVectorDynamicOverlayPublishCloneAppend' \
  -benchmem \
  -benchtime=300ms \
  -count=5

GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench 'BenchmarkColumnVectorDynamicGraphSearchCosineScale/rows_100k_dims_128_degree_16/parallel_read_(only|write)$' \
  -benchmem \
  -benchtime=300ms \
  -count=3

GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench 'BenchmarkColumnVectorDynamicGraphSearchCosineScale/rows_1m_dims_128_degree_16/parallel_read_(only|write)$' \
  -benchmem \
  -benchtime=100ms \
  -count=1
```

Latest #1615 Apple M3 smoke results:

- 100k read-only: `13030-13194 ns/op`, `75792-76744 read_qps`,
  `1064 total_candidates/search`, `5600 edges/search`, `0 B/op`,
  `0 allocs/op`.
- 100k read-write: `337121-456932 ns/op`, `2189-2966 read_qps`,
  `18306 total_candidates/search`, `708812-760204 publish_ns/op`,
  `1563561-2630119 B/op`, `18-26 allocs/op`.
- Isolated publish clone+append: `15529-16244 ns/op`, `160265-160266 B/op`,
  and `18 allocs/op` for the 256-row overlay shape; `474182-481100 ns/op`,
  `5179013-5179014 B/op`, and `64 allocs/op` for the
  8192-row/4096-tombstone shape.

Earlier 1M scale smoke from the same dynamic-overlay track:

- 1M read-only: `8850 ns/op`, `112996 read_qps`,
  `695 total_candidates/search`, `2688 edges/search`, `0 B/op`,
  `0 allocs/op`.
- 1M read-write: `127760 ns/op`, `7827 read_qps`,
  `8902 total_candidates/search`, `400424 publish_ns/op`, `711480 B/op`,
  `9 allocs/op`.

Interpretation: warmed read-only search stays allocation-free at 100k and 1M
because all mutable state is worker-local scratch. Read-write benchmark
allocations are writer-side copy-on-write publish and overlay growth costs, not
full-document fetches or shared reader scratch. The largest read amplification
comes from exact overlay scans plus base over-fetch to compensate tombstoned
base documents; those counters are the trigger for sealing mini-graphs or
rebuilding a compacted base generation.

Sealing and rebuild model:

- Start with exact-scan overlay while deltas are small.
- Seal larger deltas into immutable mini-graph segments when
  `overlay_scanned/search` becomes comparable to base candidate count or when
  tombstones force a materially larger base `TopK`.
- Rebuild and publish a new base graph when tombstones or sealed segments make
  read amplification dominate writer publish cost.
- Keep rebuild/seal timing separate from query timing; publish the rebuilt base
  by swapping a new immutable snapshot rather than mutating the current base.

Focused validation:

```sh
GOWORK=off go test ./TreeDB/collections \
  -run 'TestColumnVectorDynamicGraph|TestColumnVectorGraph' \
  -count=1

GOWORK=off go test -race ./TreeDB/collections \
  -run 'TestColumnVectorDynamicGraphConcurrentReadersAndWriter|TestColumnVectorDynamicGraphSearchTombstonesAndOverlay' \
  -count=1
```
