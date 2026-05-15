# Int64 Column Granule Experiment

This package is a non-durable column-store smoke test. It does not publish
TreeDB roots, write column files, use collection WAL, or expose public
collection APIs.

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
bytes alone. JSONBench query timings use that parsed image-backed part so M1D
can catch representation mistakes before M2 adds files, checksums, and recovery.

## M1D Gates Before File-Backed M2

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
- M2 should add persistence, checksums, file/container layout, recovery, and
  lifecycle behavior without changing the logical section model.
