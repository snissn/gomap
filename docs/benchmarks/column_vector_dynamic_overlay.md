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
