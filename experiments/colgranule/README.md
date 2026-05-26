# Int64 Column Granule Experiment

This package is a non-durable column-store smoke test. It does not publish
TreeDB roots, use production TreeDB value-log publication, use collection WAL,
or expose public collection APIs.

It currently covers:

- 8192-row default int64 granules;
- raw int64 encoding;
- delta + zigzag varint encoding;
- double-delta-style int64 encoding;
- nullable/default-heavy int64, bool bitpack/RLE, and low-cardinality code paths;
- none, snappy, and lz4 compression;
- min/max metadata and range-scan skip;
- sort-key marks and predicate pruning diagnostics;
- aggregate kernels over encoded granules;
- non-durable in-memory column parts made from row-aligned granules and
  independently split column codec blocks;
- exact serialized in-memory column part images with a manifest, section
  directory, descriptor bytes, marks, locators, dictionaries, aggregate
  metadata, and column payload sections;
- value-log-shaped `TCS1` column part assets that wrap serialized images,
  persist/reopen through `ColumnAssetStore`, and reconstruct scan-capable parts;
- a disk-backed `ColumnWorkspace` that persists workspace manifests, collection
  manifests, and `TCS1` asset refs for reopen/integrity experiments;
- an experiment `ColumnCollectionManifest` and `ColumnPartSetReader` for
  multipart base/delta/tombstone visibility and latest-row locator lookup;
- compaction of visible multipart rows back into a new base column part;
- JSONBench Bluesky fixture loading into int64-derived columns.

## Local JSONBench Data

The JSONBench data directory is expected at:

```text
$JSONBENCH_DATA
```

That path matches the default output from `$JSONBENCH_REPO/download_data.sh`
for the Bluesky data set. The upstream downloader writes larger scales into the
same directory: 1m is `file_0001.json.gz`, 10m is `file_0001.json.gz` through
`file_0010.json.gz`, 100m through `file_0100.json.gz`, and 1000m through
`file_1000.json.gz`.

The repository includes a tiny `testdata/jsonbench_sample.jsonl` fixture for
tests. The 129 MiB compressed 1M-row file is intentionally not vendored.

Run the full local 1M-row column summary:

```sh
go run ./experiments/colgranule/cmd/jsonbench_colgranule \
  -data $JSONBENCH_DATA \
  -limit 1000000 \
  -rows-per-granule 8192
```

The loader derives int64 columns from real JSONBench rows, including `time_us`,
line size, row index, string lengths, low-cardinality dictionary codes, boolean
presence flags, `createdAt` milliseconds, and language counts.

The query-oriented derived columns include the JSONBench paths used by the
ClickHouse setup and queries: `kind`, `commit.operation`, `commit.collection`,
`did`, and `time_us`.

Build the raw ClickHouse comparison data and Markdown summary:

```sh
go run ./experiments/colgranule/cmd/jsonbench_compare \
  -data $JSONBENCH_DATA \
  -limit 1000000 \
  -rows-per-granule 8192 \
  -attempts 5
```

This writes:

- `JSONBENCH_COMPARISON_RAW.json`: raw ClickHouse result imports, column codec
  summaries, query-kernel timing attempts, and the compacted TreeDB JSON/BSON
  remaining-field measurements;
- `JSONBENCH_COMPARISON_REPORT.md`: human-readable timing and storage summary.

By default, the comparison command also builds temporary TreeDB collections
under `artifacts/colgranule_remaining_treedb-*`, flushes and compacts each
database, and adds the disk footprints to the report. It records two
remaining-field shapes: a conservative shape with only top-level `time_us`
removed, and a ClickHouse-aligned shape with the typed JSON paths removed:
`time_us`, `kind`, `did`, `commit.operation`, and `commit.collection`. Disable
this part with `-measure-remaining-treedb=false`. The command also records a raw
TreeDB key/value shape that stores `documentID(row) -> original JSON line bytes`
without collection document encoding.

To iterate on encoded-part build and query kernels without reloading the retained
payload collections, reuse a previous raw comparison file:

```sh
go run ./experiments/colgranule/cmd/jsonbench_compare \
  -data $JSONBENCH_DATA \
  -limit 1000000 \
  -rows-per-granule 8192 \
  -attempts 5 \
  -measure-remaining-treedb=false \
  -retained-payload-from-json experiments/colgranule/JSONBENCH_COMPARISON_RAW.json
```

When retained-payload measurements are available, the Markdown report includes a
full-dataset estimate that adds the current serialized in-memory column part to
the measured TreeDB payload bytes. This is the pre-M2 comparison point for
ClickHouse `total_size`; the encoded part is serialized but still in memory,
while the retained payload is measured from compacted TreeDB files.

## Serialized Part Image Layout

`ColumnPartImage` is the pre-file representation that M2 should persist with
minimal semantic changes. The image starts with a manifest containing a magic
number, image version, part id, row count, manifest byte length, and one section
directory entry per following byte section. Section directory entries record
kind, category, offset, length, row/granule/block counts, encoding,
compression, name, and column name. Accounting exposes the manifest as a
separate `manifest` pseudo-section so descriptor bytes do not hide or double
count manifest overhead.

The current canonical section kinds are:

| Section | Category | Purpose |
|---|---|---|
| `descriptor` | `descriptor` | Part-level metadata, granule descriptors, column descriptors, block descriptors, and per-block granule reader metadata such as null/default counts and min/max. |
| `sort_key_metadata` | `sort_key_metadata` | Sort-key columns, direction, and null ordering, distinct from TreeDB logical primary key. |
| `sort_key_marks` | `marks` | Per-granule sparse mark summaries for sort-key prefixes. |
| `row_locators` | `locators` | `primary_id -> part row, granule, row-in-granule` lookup records. |
| `aggregate_metadata` | `aggregate_metadata` | Admitted exact per-granule aggregate metadata definitions, stats, and entries. |
| `dictionaries` | `dictionaries` | Part-local dictionary payloads for declared low-cardinality columns when available. |
| `column_data` | `declared_columns` | Concatenated encoded/compressed column block payload bytes, split by column and addressed by block descriptors. |

`ParseColumnPartImage` validates the manifest and section bounds.
`ColumnPartFromImage` reconstructs a scan-capable read-only part from serialized
bytes alone.

## TCS1 Column Asset Layout

`TCS1` is the M2 value-log-shaped wrapper for a serialized column part image. It
is a logical column asset payload, not a separate production column-file
lifecycle. The first M2 mode stores one complete `ColumnPartImage` as a single
asset with:

- `TCS1` magic and version;
- payload kind for serialized part images;
- format flags, currently strict-zero;
- payload byte length;
- part id, row count, and image version copied from the image;
- payload checksum.

`ColumnAssetStore` is the experiment seam that models future TreeDB `ValuePtr`
storage without importing `TreeDB/internal` from this package. The memory store
is useful for benchmarks, and the append-segment store proves reopen/readback
through a value-log-shaped `file id + offset + length + checksum` ref. Query
kernels still receive a reconstructed `ColumnPart`, so M2 validates storage
plumbing without changing the encoded-block execution interface.

JSONBench query timings now build the serialized image, wrap/store it as a
`TCS1` asset, read it back, reconstruct the part, and run q1-q5 through the same
encoded kernels. The query hot path uses scan-only reconstruction so it does not
decode the point-lookup row-locator map or aggregate metadata for scans that do
not need them. Full reconstruction with locators and eager metadata remains
available for point lookup, metadata kernels, and integrity tests. Split assets
for individual blocks, dictionaries, marks, locators, or aggregate metadata
should remain benchmark-driven follow-ups; the single-record mode is the
baseline.

## Disk Workspace, Manifests, and Multipart Parts

`ColumnWorkspace` is the M3 disk-backed experiment wrapper. It stores `TCS1`
assets in append segments, writes checksum-protected manifests, and validates
referenced assets on reopen. Column-lane files live under an isolated namespace:
`manifests/` for workspace and collection manifests, `assets/segments/` for
append-segment assets, `assets/indexes/` for future secondary column metadata,
`prepared/` for staged publish state, `quarantine/` for suspect assets, and
`tmp/` for atomic manifest writes. It is still not a TreeDB root publication
path and does not claim durable-at-ack collection semantics.

`ColumnCollectionManifest` is the M4 collection-lane control-plane model used by
the experiment. It records declared columns, logical primary key, sort key,
base parts, delta parts, tombstones, retained-payload byte accounting metadata,
referenced `TCS1` asset records, and compact coverage descriptors for each part
ref. Coverage descriptors carry role, generation, compaction level, source
parts, optional replacement-delta source row/root generation ranges, primary-id
range, sort-key range, row counts, deleted counts, asset refs, and checksums.
Collection manifest envelopes are written as compact JSON and checksum the
canonical compact manifest payload; reopen can still validate earlier
pretty-printed experiment payloads by compacting the raw manifest field before
checksum comparison.
`ColumnPartSetReader` opens those refs from the workspace, builds latest-visible
row state, exposes `LatestLocator`, and runs JSONBench q1-q5 over visible rows.

M6 treats declared JSONBench paths as column-owned state and the retained row
payload as the non-column remainder. `JSONBenchRetainedDocument` removes the
declared paths from row JSON, `JSONBenchDeclaredColumnValuesFromPart` reads
those values back from an image-backed column part, and
`RestoreJSONBenchDeclaredColumns` must reconstruct the original JSON document
under canonical JSON comparison.

Column asset lifecycle planning is manifest/ref based. `ColumnAssetRef`
reachability is computed from active manifests, pending manifests,
snapshot-pinned manifests, superseded manifests, prepared assets, and
quarantined assets without decoding `TCS1` payload bytes or scanning rows. A
typed `ColumnAssetManager` facade owns the experiment refs and models the
production pin, zombie, rewrite-debt, quarantine, and deletion gate: reclaimable
bytes are not ready to delete until reachability, zombie marking, and pin drain
all agree. Compaction reports both pre-publish and post-publish reachability:
before the new manifest is published, old active assets remain protected; after
publish, old assets are directly reclaimable only when their whole segment has
no live refs, otherwise they are rewrite debt. Prepared publish state is recorded
in a checksum-protected registry under `prepared/`, and namespace inventory is a
read-only reconciliation input that joins segment files and prepared refs against
manifest/ref state before reporting orphan candidates. Publish bookkeeping can
also use `PlanColumnAssetRefDelta` when the changed part refs are known, keeping
the accounting path proportional to newly published, superseded, and prepared
refs instead of rows or unchanged manifests.

`PlanColumnPartSetCompaction` is the M6 policy-report seam for future background
selection. It reports selected/skipped parts, live bytes, stale bytes, tombstone
debt, expected reclaim ratio, sparse-visible-row pressure, read amplification,
aggregate-metadata invalidation, and column-asset stale-byte pressure without
publishing a new part. The explicit `CompactColumnPartSet` helper remains the
only experiment path that writes the replacement base asset.

Reopen validation has an explicit lazy boundary. The default experiment mode
still validates full `TCS1` images for integrity tests, while
`ColumnWorkspaceValidateTCS1Header` validates manifest/ref/header metadata
without reading every full image payload. Loading the part still verifies the
payload checksum and section structure before use.

Adaptive mark sizing is explicit and opt-in through `ColumnAdaptiveMarkSizing`.
When enabled, part build estimates uncompressed declared-column bytes per row
and chooses rows per mark from a target byte budget with min/max row clamps.
Fixed `RowsPerGranule` remains the default path for existing gates.

M8A adds a non-durable mutation adapter over the same manifest path. The adapter
publishes bulk base batches, row-aligned insert/update delta parts, and delete
tombstones, then persists a new collection manifest generation. It accepts
complete declared-column vectors produced from collection-shaped mutations; it
does not encode physical column diffs as replay source of truth and still does
not claim command-WAL durable-at-ack semantics. The restart/replay tests compare
logical rows and JSONBench q1-q5 hashes after applying the same mutation
sequence into a fresh workspace.

The M8A locator profile keeps the V1 locator shape as one primary id to latest
visible row locator per manifest generation. It does not use one locator per
column because column parts remain row-aligned across declared columns. The
current reader builds an in-memory side index while opening a manifest, and the
no-side-index comparison scans part-local locator maps. On an Apple M3, the
measured profile was:

| Delta parts | Side-index locator | Part-local scan locator | Side-index point value | Part-local scan point value |
|---:|---:|---:|---:|---:|
| 1 | ~10-11 ns/op, 0 allocs | ~63-65 ns/op, 0 allocs | ~58-66 ns/op, 0 allocs | ~114-145 ns/op, 0 allocs |
| 8 | ~10-12 ns/op, 0 allocs | ~237-262 ns/op, 0 allocs | ~58-69 ns/op, 0 allocs | ~284-317 ns/op, 0 allocs |
| 32 | ~10-11 ns/op, 0 allocs | ~867-987 ns/op, 0 allocs | ~58-75 ns/op, 0 allocs | ~912-1100 ns/op, 0 allocs |
| 128 | ~10-12 ns/op, 0 allocs | ~4.0-4.6 us/op, 0 allocs | ~58-66 ns/op, 0 allocs | ~4.1-4.2 us/op, 0 allocs |

This is enough to defer a root-backed locator for the next production step. The
manifest-load side index gives constant-time, zero-allocation lookup and
scratch-backed point reconstruction, while the scan path only proves why an
index is needed once part counts grow. A production root-backed locator should
be added later only when a real update/delete or point-reconstruction workload
needs persistent locator state across opens, and it should map `primary id ->
latest visible row locator` for a generation rather than `primary id + column`.

The multipart query gate should preserve the M2 encoded-part execution shape:
avoid projected-row materialization for hot q1-q5 kernels, report visible,
superseded, deleted, part, block, decoded-byte, and cache diagnostics, and keep
per-query hot allocation bounded. The current synthetic M4 gate is:

```sh
GOWORK=off go test ./experiments/colgranule -run '^$' \
  -bench 'BenchmarkJSONBenchEncodedPartQueries|BenchmarkJSONBenchColumnPartSetQueriesM4|BenchmarkJSONBenchColumnPartSetQueriesM4PerQuery' \
  -benchmem -count=5
```

To compare the historical in-memory colgranule JSONBench kernels with the
current durable TreeDB collection column-store physical query path, run:

```sh
GOWORK=off go test ./experiments/colgranule -run '^$' \
  -bench '^BenchmarkJSONBenchColumnStoreCompare$' \
  -benchmem -benchtime=3x -count=1
```

`BenchmarkJSONBenchColumnStoreCompare` uses a synthetic, filter-degenerate
JSONBench fixture where every row is `kind=commit`, `operation=create`, and
`collection=app.bsky.feed.post`. This keeps the current TreeDB physical query
API comparable to the older JSONBench Q1/Q2/Q3-hour/Q4/Q5 kernels even though
TreeDB does not yet expose the experiment's separate filter-column predicates in
this query surface. Override row count with `TREEDB_JSONBENCH_COMPARE_ROWS=8192`
for a smoke run.

The current M5 compaction gate measures both full compaction and phase
breakdowns for visible scanning, part rebuild, image serialization, asset
publish, and manifest save:

```sh
GOWORK=off go test ./experiments/colgranule -run '^$' \
  -bench 'BenchmarkColumnPartSetCompactionM5|BenchmarkColumnPartSetCompactionM5Breakdown' \
  -benchmem -count=5
```

The local 1M-row multipart benchmark intentionally skips unless `JSONBENCH_DATA`
points to a real 1M-row fixture; the repository fixture is only for tests:

```sh
JSONBENCH_DATA=/path/to/file_0001.json.gz \
GOWORK=off go test ./experiments/colgranule -run '^$' \
  -bench 'BenchmarkJSONBenchLocalColumnPartSetQueriesM4PerQuery' \
  -benchmem -count=1
```

The known q4 caveat is sort order, not row materialization: M2's single-part
baseline can early-stop on a globally time-ordered part, while the correct M4
multipart path currently scans visible rows. A future k-way sort-key/mark merge
across parts is needed before multipart q4 can early-stop safely. The TreeDB
comparison also includes `q3_group_hour_count`, a production physical reducer
shape for dictionary predicates plus `collection/hour(time_us)` grouping, and
explicit aggregate-metadata Top-K q4/q5 variants that report `topk_limit/op`,
`topk_candidates/op`, and `result_shape_ns/op`; metadata paths use logical
rows/sec with `rows_scanned/op=0` and should not be confused with full data-row
scans or pruning metadata.

## M1D Gates Before Value-Log-Backed M2

Before moving this experiment into durable files, the in-memory representation
must stay aligned with the future file format:

- byte accounting must reconcile to the serialized image byte length, not struct
  estimates;
- serialized images must parse back into read-only column parts without relying
  on builder-owned descriptor or payload structs;
- the report must show section bytes for manifest/descriptors, declared columns,
  dictionaries, marks, sort-key metadata, aggregate metadata, and row locators;
- JSONBench query runners must read column payloads through parsed image-backed
  parts;
- retained JSON must remain separate from declared column image bytes in the
  full-dataset comparison;
- local 1M JSONBench reports must include granule count, codec block count, part
  file count, retained-payload file count, build throughput, allocations, and
  TreeDB/ClickHouse size ratios;
- `BenchmarkJSONBenchPartImageM1D` and the optional 1M
  `BenchmarkJSONBenchLocalPartImageM1D` benchmark must keep measuring
  serialization of an existing part, parse/reconstruct from image bytes, and the
  full build/serialize/parse/reconstruct path;
- M2 should add `TCS1` value-log-shaped persistence, checksums, asset refs,
  recovery, and lifecycle behavior without changing the logical section model.
