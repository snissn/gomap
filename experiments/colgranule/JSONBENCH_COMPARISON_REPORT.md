# JSONBench ClickHouse Comparison

Generated from `/Users/michaelseiler/data/bluesky` with row limit `1000000`; this local run read `1` file(s) and `1000000` rows. The comparison is a smoke-level column-kernel comparison, not a full database benchmark: TreeDB roots, collection WAL, query planning, persistence, and SQL execution are intentionally out of scope.

The ClickHouse numbers below use the local JSONBench result `ClickHouse` `26.4.3.1` on `Darwin 25.2.0 arm64`, so query timings and disk bytes are local-machine comparisons.

## Query Timing

| Query | Column-kernel best | ClickHouse local | Kernel / ClickHouse | Notes |
|---|---:|---:|---:|---|
| Q1 | 0.004263s | 0.014000s | 0.30x | Top event types |
| Q2 | 0.038711s | 0.027000s | 1.43x | Top event types with unique users |
| Q3 | 0.006609s | 0.014000s | 0.47x | Event counts by hour |
| Q4 | 0.009618s | 0.013000s | 0.74x | Top 3 post veterans |
| Q5 | 0.010000s | 0.013000s | 0.77x | Top 3 users with longest activity |

## Storage Footprint

| Item | Bytes | MiB | Notes |
|---|---:|---:|---|
| JSONBench compressed input files | 135176827 | 128.91 | Local `.json.gz` source bytes read by this run. |
| ClickHouse local total | 101786238 | 97.07 | `total_size` from local ClickHouse JSONBench result. |
| ClickHouse local data | 101761540 | 97.05 | `data_size` from local ClickHouse JSONBench result. |
| ClickHouse local index | 24559 | 0.02 | `index_size` from local ClickHouse JSONBench result. |
| Granule best-codec all derived columns | 11115221 | 10.60 | 10.92% of ClickHouse local total. |
| Granule best-codec query/index paths | 5045512 | 4.81 | 4.96% of ClickHouse local total. |
| TreeDB BSON remaining fields after compaction + value-log rewrite | 54685705 | 52.15 | Stores original JSON minus ClickHouse typed paths as BSON in a compressed no-index collection. |
| Granules all derived columns + TreeDB BSON remaining fields | 65800926 | 62.75 | 64.65% of ClickHouse local total. |
| Granules query/index paths + TreeDB BSON remaining fields | 59731217 | 56.96 | 58.68% of ClickHouse local total. |
| TreeDB JSON remaining fields after compaction + value-log rewrite | 52099928 | 49.69 | Stores original JSON minus ClickHouse typed paths as JSON in a compressed no-index collection. |
| Granules all derived columns + TreeDB JSON remaining fields | 63215149 | 60.29 | 62.11% of ClickHouse local total. |
| Granules query/index paths + TreeDB JSON remaining fields | 57145440 | 54.50 | 56.14% of ClickHouse local total. |
| TreeDB Template-v1 remaining fields after compaction + value-log rewrite | 49859640 | 47.55 | Stores original JSON minus ClickHouse typed paths as Template-v1 in a compressed no-index collection. |
| Granules all derived columns + TreeDB Template-v1 remaining fields | 60974861 | 58.15 | 59.90% of ClickHouse local total. |
| Granules query/index paths + TreeDB Template-v1 remaining fields | 54905152 | 52.36 | 53.94% of ClickHouse local total. |

The remaining-fields TreeDB collection stores each original JSON row after deleting the same explicitly typed JSON paths used by the local ClickHouse JSONBench schema: `time_us`, `kind`, `did`, `commit.operation`, and `commit.collection`. The removed paths are represented by granule columns in this experiment. Nested values such as `commit.rev`, `commit.rkey`, `commit.cid`, `commit.record.text`, `langs`, `reply`, and `subject` remain in the TreeDB payload.

BSON remaining-fields compaction detail: before compact `223981743` bytes across `12` files; after compact plus value-log rewrite `54685705` bytes across `15` files; compaction wall time `0.271s`; rewrite wall time `2.659s`; rewritten records `1000000`; rewritten value bytes `357429419`; rewritten source bytes `209421349`; BSON payload bytes before TreeDB storage `357429419`.

JSON remaining-fields compaction detail: before compact `204202382` bytes across `12` files; after compact plus value-log rewrite `52099928` bytes across `15` files; compaction wall time `0.272s`; rewrite wall time `2.207s`; rewritten records `1000000`; rewritten value bytes `339735546`; rewritten source bytes `189685621`; JSON payload bytes before TreeDB storage `339735546`.

Template-v1 remaining-fields compaction detail: before compact `211233407` bytes across `12` files; after compact plus value-log rewrite `49859640` bytes across `15` files; compaction wall time `0.259s`; rewrite wall time `2.667s`; rewritten records `1000471`; rewritten value bytes `259230751`; rewritten source bytes `196900849`; Template-v1 payload bytes before TreeDB storage `583083877`.

Template-v1 is loaded as one large insert batch in this experiment so template records and compact stored documents are learned together. The rewritten record count includes template-root records as well as primary documents.

The table below is one-column-at-a-time storage for the experimental granule codecs. It picks the smallest stored byte count observed for each derived `int64` column across raw, delta-varint, snappy, and lz4 combinations.

| Column | Best codec | Stored bytes | Ratio vs int64 values | Ratio vs ClickHouse total |
|---|---|---:|---:|---:|
| `cid_bytes` | `delta_varint` + `snappy` | 127657 | 0.015957 | 0.1254% |
| `commit_collection_code` | `delta_varint` + `snappy` | 537086 | 0.067136 | 0.5277% |
| `commit_operation_code` | `delta_varint` + `snappy` | 148297 | 0.018537 | 0.1457% |
| `commit_rev_bytes` | `delta_varint` + `lz4` | 31257 | 0.003907 | 0.0307% |
| `commit_rkey_bytes` | `delta_varint` + `lz4` | 86179 | 0.010772 | 0.0847% |
| `did_bytes` | `delta_varint` + `lz4` | 6749 | 0.000844 | 0.0066% |
| `did_code` | `delta_varint` + `lz4` | 2321001 | 0.290125 | 2.2803% |
| `kind_code` | `delta_varint` + `lz4` | 31312 | 0.003914 | 0.0308% |
| `line_bytes` | `delta_varint` + `snappy` | 952538 | 0.119067 | 0.9358% |
| `record_created_at_unix_ms` | `delta_varint` + `snappy` | 2968163 | 0.371020 | 2.9161% |
| `record_has_reply` | `delta_varint` + `snappy` | 144988 | 0.018124 | 0.1424% |
| `record_has_subject` | `delta_varint` + `snappy` | 261203 | 0.032650 | 0.2566% |
| `record_langs_count` | `delta_varint` + `snappy` | 192851 | 0.024106 | 0.1895% |
| `record_subject_string_bytes` | `delta_varint` + `snappy` | 347079 | 0.043385 | 0.3410% |
| `record_text_bytes` | `delta_varint` + `snappy` | 372759 | 0.046595 | 0.3662% |
| `record_type_code` | `delta_varint` + `snappy` | 571548 | 0.071443 | 0.5615% |
| `row_index` | `delta_varint` + `lz4` | 6738 | 0.000842 | 0.0066% |
| `time_us` | `delta_varint` + `none` | 2007816 | 0.250977 | 1.9726% |

## Raw Data

Machine-readable raw data is in `experiments/colgranule/JSONBENCH_COMPARISON_RAW.json`.
