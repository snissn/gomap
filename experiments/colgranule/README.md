# Int64 Column Granule Experiment

This package is a non-durable column-store smoke test. It does not publish
TreeDB roots, write column files, use collection WAL, or expose public
collection APIs.

It currently covers:

- 8192-row default int64 granules;
- raw int64 encoding;
- delta + zigzag varint encoding;
- none, snappy, and lz4 compression;
- min/max metadata and range-scan skip;
- JSONBench Bluesky fixture loading into int64-derived columns.

## Local JSONBench Data

The JSONBench data directory is expected at:

```text
/Users/michaelseiler/data/bluesky
```

That path matches the default output from `/Users/michaelseiler/dev/snissn/JSONBench/download_data.sh`
for the Bluesky data set. The upstream downloader writes larger scales into the
same directory: 1m is `file_0001.json.gz`, 10m is `file_0001.json.gz` through
`file_0010.json.gz`, 100m through `file_0100.json.gz`, and 1000m through
`file_1000.json.gz`.

The repository includes a tiny `testdata/jsonbench_sample.jsonl` fixture for
tests. The 129 MiB compressed 1M-row file is intentionally not vendored.

Run the full local 1M-row column summary:

```sh
go run ./experiments/colgranule/cmd/jsonbench_colgranule \
  -data /Users/michaelseiler/data/bluesky \
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
  -data /Users/michaelseiler/data/bluesky \
  -limit 1000000 \
  -rows-per-granule 8192 \
  -attempts 5
```

This writes:

- `JSONBENCH_COMPARISON_RAW.json`: raw ClickHouse result imports, column codec
  summaries, query-kernel timing attempts, and the compacted TreeDB JSON/BSON
  remaining-field measurements;
- `JSONBENCH_COMPARISON_REPORT.md`: human-readable timing and storage summary.

By default, the comparison command also builds temporary TreeDB collections at
`artifacts/colgranule_remaining_treedb-json` and
`artifacts/colgranule_remaining_treedb-bson`, stores the original JSON rows with
the ClickHouse typed JSON paths removed, flushes and compacts each database, and
adds both disk footprints to the report. Those removed paths are `time_us`,
`kind`, `did`, `commit.operation`, and `commit.collection`. Disable this part with
`-measure-remaining-treedb=false`.
