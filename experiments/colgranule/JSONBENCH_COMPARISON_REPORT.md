# JSONBench ClickHouse Comparison

Generated from `/Users/michaelseiler/data/bluesky` with row limit `1000000`; this local run read `1` file(s) and `1000000` rows. The comparison is a smoke-level column-kernel comparison, not a full database benchmark: TreeDB roots, collection WAL, query planning, persistence, and SQL execution are intentionally out of scope.

## Query Timing

| Query | Column-kernel best | ClickHouse local | ClickHouse AWS best | Notes |
|---|---:|---:|---:|---|
| Q1 | 0.004271s | 0.014000s | 0.004000s | Top event types |
| Q2 | 0.039045s | 0.027000s | 0.022000s | Top event types with unique users |
| Q3 | 0.006773s | 0.014000s | 0.012000s | Event counts by hour |
| Q4 | 0.009645s | 0.013000s | 0.017000s | Top 3 post veterans |
| Q5 | 0.009706s | 0.013000s | 0.019000s | Top 3 users with longest activity |

## Storage Footprint

ClickHouse local 1m size: total `101786238` bytes, data `101761540` bytes, index `24559` bytes.

The table below is one-column-at-a-time storage for the experimental granule codecs. It picks the smallest stored byte count observed for each derived `int64` column across raw, delta-varint, snappy, and lz4 combinations.

| Column | Best codec | Stored bytes | Ratio vs int64 values |
|---|---|---:|---:|
| `cid_bytes` | `delta_varint` + `snappy` | 127657 | 0.015957 |
| `commit_collection_code` | `delta_varint` + `snappy` | 537086 | 0.067136 |
| `commit_operation_code` | `delta_varint` + `snappy` | 148297 | 0.018537 |
| `commit_rev_bytes` | `delta_varint` + `lz4` | 31257 | 0.003907 |
| `commit_rkey_bytes` | `delta_varint` + `lz4` | 86179 | 0.010772 |
| `did_bytes` | `delta_varint` + `lz4` | 6749 | 0.000844 |
| `did_code` | `delta_varint` + `lz4` | 2321001 | 0.290125 |
| `kind_code` | `delta_varint` + `lz4` | 31312 | 0.003914 |
| `line_bytes` | `delta_varint` + `snappy` | 952538 | 0.119067 |
| `record_created_at_unix_ms` | `delta_varint` + `snappy` | 2968163 | 0.371020 |
| `record_has_reply` | `delta_varint` + `snappy` | 144988 | 0.018124 |
| `record_has_subject` | `delta_varint` + `snappy` | 261203 | 0.032650 |
| `record_langs_count` | `delta_varint` + `snappy` | 192851 | 0.024106 |
| `record_subject_string_bytes` | `delta_varint` + `snappy` | 347079 | 0.043385 |
| `record_text_bytes` | `delta_varint` + `snappy` | 372759 | 0.046595 |
| `record_type_code` | `delta_varint` + `snappy` | 571548 | 0.071443 |
| `row_index` | `delta_varint` + `lz4` | 6738 | 0.000842 |
| `time_us` | `delta_varint` + `none` | 2007816 | 0.250977 |

Best-codec total for all derived columns: `11115221` bytes.
Best-codec total for ClickHouse indexed/query paths (`kind`, `commit.operation`, `commit.collection`, `did`, `time_us`): `5045512` bytes.

## Raw Data

Machine-readable raw data is in `experiments/colgranule/JSONBENCH_COMPARISON_RAW.json`.
