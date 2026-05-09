# JSONBench ClickHouse Comparison

Generated from `$JSONBENCH_DATA` with row limit `1000000`; this local run read `1` file(s) and `1000000` rows. The comparison is a smoke-level column-kernel comparison, not a full database benchmark: TreeDB roots, collection WAL, query planning, persistence, and SQL execution are intentionally out of scope.

The ClickHouse numbers below use the local JSONBench result `ClickHouse` `26.4.3.1` on `Darwin 25.2.0 arm64`, so query timings and disk bytes are local-machine comparisons.

## Query Timing

| Query | Column-kernel best | ClickHouse local | Kernel / ClickHouse | Notes |
|---|---:|---:|---:|---|
| Q1 | 0.004284s | 0.014000s | 0.31x | Top event types |
| Q2 | 0.038895s | 0.027000s | 1.44x | Top event types with unique users |
| Q3 | 0.006631s | 0.014000s | 0.47x | Event counts by hour |
| Q4 | 0.009869s | 0.013000s | 0.76x | Top 3 post veterans |
| Q5 | 0.010027s | 0.013000s | 0.77x | Top 3 users with longest activity |

## Storage Footprint

| Item | Bytes | MiB | Notes |
|---|---:|---:|---|
| JSONBench compressed input files | 135176827 | 128.91 | Local `.json.gz` source bytes read by this run. |
| ClickHouse local total | 101786238 | 97.07 | `total_size` from local ClickHouse JSONBench result. |
| ClickHouse local data | 101761540 | 97.05 | `data_size` from local ClickHouse JSONBench result. |
| ClickHouse local index | 24559 | 0.02 | `index_size` from local ClickHouse JSONBench result. |
| Granule best-codec all derived columns | 11115221 | 10.60 | 10.92% of ClickHouse local total. |
| Granule best-codec query/index paths | 5045512 | 4.81 | 4.96% of ClickHouse local total. |
| TreeDB BSON remaining fields after compaction + value-log rewrite | 54670992 | 52.14 | Stores original JSON minus ClickHouse typed paths as BSON in a compressed no-index collection. |
| Granules all derived columns + TreeDB BSON remaining fields | 65786213 | 62.74 | 64.63% of ClickHouse local total. |
| Granules query/index paths + TreeDB BSON remaining fields | 59716504 | 56.95 | 58.67% of ClickHouse local total. |
| TreeDB JSON remaining fields after compaction + value-log rewrite | 52170488 | 49.75 | Stores original JSON minus ClickHouse typed paths as JSON in a compressed no-index collection. |
| Granules all derived columns + TreeDB JSON remaining fields | 63285709 | 60.35 | 62.18% of ClickHouse local total. |
| Granules query/index paths + TreeDB JSON remaining fields | 57216000 | 54.57 | 56.21% of ClickHouse local total. |
| TreeDB Template-v1 remaining fields after compaction + value-log rewrite | 49830135 | 47.52 | Stores original JSON minus ClickHouse typed paths as Template-v1 in a compressed no-index collection. |
| Granules all derived columns + TreeDB Template-v1 remaining fields | 60945356 | 58.12 | 59.88% of ClickHouse local total. |
| Granules query/index paths + TreeDB Template-v1 remaining fields | 54875647 | 52.33 | 53.91% of ClickHouse local total. |
| Conservative TreeDB BSON remaining fields after compaction + value-log rewrite | 56830020 | 54.20 | Stores original JSON minus only `time_us` as BSON in a compressed no-index collection. |
| Granules all derived columns + conservative TreeDB BSON remaining fields | 67945241 | 64.80 | 66.75% of ClickHouse local total. |
| Granules query/index paths + conservative TreeDB BSON remaining fields | 61875532 | 59.01 | 60.79% of ClickHouse local total. |
| Conservative TreeDB JSON remaining fields after compaction + value-log rewrite | 54941263 | 52.40 | Stores original JSON minus only `time_us` as JSON in a compressed no-index collection. |
| Granules all derived columns + conservative TreeDB JSON remaining fields | 66056484 | 63.00 | 64.90% of ClickHouse local total. |
| Granules query/index paths + conservative TreeDB JSON remaining fields | 59986775 | 57.21 | 58.93% of ClickHouse local total. |
| Conservative TreeDB Template-v1 remaining fields after compaction + value-log rewrite | 50678982 | 48.33 | Stores original JSON minus only `time_us` as Template-v1 in a compressed no-index collection. |
| Granules all derived columns + conservative TreeDB Template-v1 remaining fields | 61794203 | 58.93 | 60.71% of ClickHouse local total. |
| Granules query/index paths + conservative TreeDB Template-v1 remaining fields | 55724494 | 53.14 | 54.75% of ClickHouse local total. |
| Raw TreeDB key/value JSON after compaction + value-log rewrite | 268058973 | 255.64 | Stores `documentID(row) -> original JSON line bytes` with no collection document encoding. |
| Granules query/index paths + raw TreeDB key/value JSON | 273104485 | 260.45 | 268.31% of ClickHouse local total. |

The ClickHouse-aligned remaining-fields TreeDB collections store each original JSON row after deleting the same explicitly typed JSON paths used by the local ClickHouse JSONBench schema: `time_us`, `kind`, `did`, `commit.operation`, and `commit.collection`. The removed paths are represented by granule columns in this experiment. Nested values such as `commit.rev`, `commit.rkey`, `commit.cid`, `commit.record.text`, `langs`, `reply`, and `subject` remain in the TreeDB payload. The conservative rows keep those string paths in the TreeDB payload and remove only `time_us`.

BSON remaining-fields compaction detail: before compact `224591244` bytes across `12` files; after compact plus value-log rewrite `54670992` bytes across `15` files; compaction wall time `0.277s`; rewrite wall time `2.286s`; rewritten records `1000000`; rewritten value bytes `357429419`; rewritten source bytes `210028730`; BSON payload bytes before TreeDB storage `357429419`.

JSON remaining-fields compaction detail: before compact `204388645` bytes across `12` files; after compact plus value-log rewrite `52170488` bytes across `15` files; compaction wall time `0.264s`; rewrite wall time `2.581s`; rewritten records `1000000`; rewritten value bytes `339735546`; rewritten source bytes `189847262`; JSON payload bytes before TreeDB storage `339735546`.

Template-v1 remaining-fields compaction detail: before compact `211077380` bytes across `12` files; after compact plus value-log rewrite `49830135` bytes across `15` files; compaction wall time `0.262s`; rewrite wall time `2.725s`; rewritten records `1000471`; rewritten value bytes `259230751`; rewritten source bytes `196774129`; Template-v1 payload bytes before TreeDB storage `583083877`.

Template-v1 reuses one encoder across bounded insert batches, so template records and compact stored documents are learned across the whole measurement without retaining every row in memory. The rewritten record count includes template-root records as well as primary documents.

Conservative BSON remaining-fields compaction detail: before compact `251114357` bytes across `12` files; after compact plus value-log rewrite `56830020` bytes across `15` files; compaction wall time `0.279s`; rewrite wall time `2.676s`; rewritten records `1000000`; rewritten value bytes `474461494`; rewritten source bytes `236599318`; BSON payload bytes before TreeDB storage `474461494`.

Conservative JSON remaining-fields compaction detail: before compact `240990990` bytes across `12` files; after compact plus value-log rewrite `54941263` bytes across `15` files; compaction wall time `0.277s`; rewrite wall time `2.232s`; rewritten records `1000000`; rewritten value bytes `452778277`; rewritten source bytes `226357173`; JSON payload bytes before TreeDB storage `452778277`.

Conservative Template-v1 remaining-fields compaction detail: before compact `229026542` bytes across `12` files; after compact plus value-log rewrite `50678982` bytes across `15` files; compaction wall time `0.286s`; rewrite wall time `2.348s`; rewritten records `1000471`; rewritten value bytes `329422732`; rewritten source bytes `214435592`; Template-v1 payload bytes before TreeDB storage `684158576`.

Raw TreeDB key/value JSON detail: before compact `226266965` bytes across `13` files; after compact plus value-log rewrite `268058973` bytes across `17` files; compaction wall time `0.362s`; rewrite wall time `2.304s`; rewritten records `1000000`; rewritten value bytes `479778277`; rewritten source bytes `201775473`; raw JSON payload bytes before TreeDB storage `479778277`.

Raw TreeDB key/value JSON uses the public cached key/value write path. In the inspected run, value-log rewrite produced a dictionary-compressed rewrite segment, but the original ingest value-log segments remained classified as active and therefore stayed in the measured directory footprint. Treat this row as a cached raw-key/value retention fixture, not as the lower bound for compressed raw JSON bytes.

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
