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

The JSONBench 1M fixture is expected at:

```text
/Users/michaelseiler/data/bluesky/file_0001.json.gz
```

That path matches the default output from `/Users/michaelseiler/dev/snissn/JSONBench/download_data.sh`
for the 1m Bluesky data set.

The repository includes a tiny `testdata/jsonbench_sample.jsonl` fixture for
tests. The 129 MiB compressed 1M-row file is intentionally not vendored.

Run the full local 1M-row column summary:

```sh
go run ./experiments/colgranule/cmd/jsonbench_colgranule \
  -data /Users/michaelseiler/data/bluesky/file_0001.json.gz \
  -limit 1000000 \
  -rows-per-granule 8192
```

The loader derives int64 columns from real JSONBench rows, including `time_us`,
line size, row index, string lengths, low-cardinality dictionary codes, boolean
presence flags, `createdAt` milliseconds, and language counts.

The query-oriented derived columns include the JSONBench paths used by the
ClickHouse setup and queries: `kind`, `commit.operation`, `commit.collection`,
`did`, and `time_us`.
