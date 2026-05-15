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
full-dataset estimate that adds the current encoded column part to the measured
TreeDB payload bytes. This is the M1C-era comparison point for ClickHouse
`total_size`; the encoded part is still in memory, while the retained payload is
measured from compacted TreeDB files.
