# ClickHouse JSONBench 1M Part Audit

Date: 2026-06-12

This note records a physical audit of the preserved ClickHouse JSONBench 1M
Bluesky reference part used as the storage comparator for the TreeDB storage
parity work. It is evidence for gomap issues #2353, #1462, #2359, #2662,
#2663, and #2680.

The headline result is that the published ClickHouse `101,786,238` byte total
is a single active `MergeTree` wide part with one `JSON(...) CODEC(ZSTD(1))`
column. Nearly all bytes are in ClickHouse JSON v3 shared-object substreams, not
in the declared q1-q5 scalar paths.

## Audit Basis

Observed local inputs:

- gomap checkout: `a55e838e4b1a6bfd3f8bb54306c9bd3eda392420`
- JSONBench checkout: `7882c34beb78eee47732571526fe26f08dfa7874`
- ClickHouse version: `26.4.3.1`
- Database: `jsonbench_local_1000000_20260515_184738_65766`
- Table: `bluesky`
- Rows: `1,000,000`
- Active part: `all_1_1_0`
- Part type: `Wide`
- Part path:
  `/Users/michaelseiler/dev/snissn/JSONBench/.clickhouse/servers/jsonbench-local/data/store/4e4/4e4d71d0-67c7-45bc-a847-65787050d774/all_1_1_0/`
- Raw audit bundle:
  `/tmp/clickhouse_jsonbench_part_audit_20260612_224058`

The audit bundle contains:

- `sql/show_create_table.sql`
- `system/system_parts.jsonl`
- `system/system_parts_columns.jsonl`
- `system/system_columns.jsonl`
- `system/system_tables.jsonl`
- `system/system_databases.jsonl`
- `system/settings_selected.jsonl`
- `parts/part_files.jsonl`
- `parts/substream_files.jsonl`
- `parts/clickhouse_part_tree.txt`
- raw part metadata files copied from the part directory
- `summary.json`
- `summary.md`

The source ClickHouse result artifact was
`/Users/michaelseiler/dev/snissn/JSONBench/clickhouse/local_results/run_20260515_184738_65766/result.json`.
It reports:

| metric | value |
| --- | ---: |
| `version` | `26.4.3.1` |
| `dataset_size` | `1,000,000` |
| `num_loaded_documents` | `1,000,000` |
| `total_size` | `101,786,238` |
| `data_size` | `101,761,540` |
| `index_size` | `24,559` |

`result_times.json` has three attempts per q1-q5 query. This note uses those
timings only as provenance; the analysis below is about disk layout.

## DDL And Settings

Captured `SHOW CREATE TABLE`:

```sql
CREATE TABLE jsonbench_local_1000000_20260515_184738_65766.bluesky
(
    `data` JSON(max_dynamic_paths = 0, `commit.collection` LowCardinality(String), `commit.operation` LowCardinality(String), did String, kind LowCardinality(String), time_us UInt64) CODEC(ZSTD(1))
)
ENGINE = MergeTree
ORDER BY (data.kind, data.commit.operation, data.commit.collection, data.did, fromUnixTimestamp64Micro(data.time_us))
SETTINGS object_serialization_version = 'v3', dynamic_serialization_version = 'v3', object_shared_data_serialization_version = 'advanced', object_shared_data_serialization_version_for_zero_level_parts = 'map_with_buckets', index_granularity = 8192
```

The q1-q5 declared subpaths are:

| JSON path | ClickHouse type |
| --- | --- |
| `kind` | `LowCardinality(String)` |
| `commit.operation` | `LowCardinality(String)` |
| `commit.collection` | `LowCardinality(String)` |
| `did` | `String` |
| `time_us` | `UInt64` |

The rest of the JSON object is not stored as raw JSON text in the declared
subpaths. It is stored in ClickHouse JSON object/shared-data substreams.

## System Table Reconciliation

`system.parts` for the active part:

| field | value |
| --- | ---: |
| rows | `1,000,000` |
| marks | `124` |
| bytes_on_disk | `101,786,238` |
| data_compressed_bytes | `101,761,540` |
| data_uncompressed_bytes | `690,012,142` |
| primary_key_size | `2,994` |
| marks_bytes | `21,565` |
| part_type | `Wide` |
| active | `1` |

The `system.parts.bytes_on_disk` value exactly matches:

| byte class | bytes |
| --- | ---: |
| compressed data files, `.bin` | 101,761,540 |
| compressed mark files, `.cmrk2` | 21,565 |
| sparse primary index, `primary.cidx` | 2,994 |
| row count metadata, `count.txt` | 7 |
| serialization metadata, `serialization.json` | 132 |
| total | 101,786,238 |

The raw filesystem logical sum is `101,803,055` bytes because it also includes
`checksums.txt`, `columns.txt`, `columns_substreams.txt`,
`default_compression_codec.txt`, and `metadata_version.txt`. The published
ClickHouse storage number follows the `system.parts` basis, not a literal
`find` sum of every text metadata file in the part directory. The extra text
metadata is only `16,817` bytes, so it does not change the parity conclusion.

Allocated filesystem bytes were `102,502,400`, mainly because every tiny
`.cmrk2` stream occupies at least one filesystem block on this machine.

## Physical File Inventory

The part directory contains one pair of data/mark files per serialized
substream:

| extension | files | logical bytes | allocated bytes |
| --- | ---: | ---: | ---: |
| `.bin` | 106 | 101,761,540 | 102,023,168 |
| `.cmrk2` | 106 | 21,565 | 434,176 |
| `.txt` | 6 | 16,824 | 36,864 |
| `.cidx` | 1 | 2,994 | 4,096 |
| `.json` | 1 | 132 | 4,096 |

`columns_substreams.txt` reports `106` substreams for the single logical
column `data`:

- 1 object-structure substream
- 3 LowCardinality dictionary substreams
- 3 LowCardinality code/index substreams
- 2 `did` string substreams
- 1 `time_us` substream
- 32 shared-object bucket offset substreams
- 32 shared-object bucket path substreams
- 32 shared-object bucket value substreams

## JSON Substream Accounting

The following table maps the 106 substreams to inferred roles. The names come
from `system.parts_columns.substreams` and `columns_substreams.txt`; byte counts
come from the corresponding `.bin` and `.cmrk2` files in the part directory.

| role | streams | data bytes | mark bytes | total bytes |
| --- | ---: | ---: | ---: | ---: |
| shared JSON bucket values | 32 | 90,784,804 | 5,822 | 90,790,626 |
| declared high-cardinality string, `did` | 1 | 4,453,414 | 428 | 4,453,842 |
| declared numeric time, `time_us` | 1 | 3,987,864 | 418 | 3,988,282 |
| shared JSON bucket offsets | 32 | 1,923,683 | 8,796 | 1,932,479 |
| shared JSON bucket paths | 32 | 348,083 | 4,797 | 352,880 |
| declared string offsets, `did.size` | 1 | 258,711 | 337 | 259,048 |
| LowCardinality codes | 3 | 3,232 | 713 | 3,945 |
| JSON object structure | 1 | 1,436 | 64 | 1,500 |
| LowCardinality dictionaries | 3 | 313 | 190 | 503 |

The declared q1-q5 paths are small:

| declared path | compressed bytes including marks |
| --- | ---: |
| `commit.collection` | 1,432 |
| `commit.operation` | 1,271 |
| `did` | 4,453,842 |
| `did.size` | 259,048 |
| `kind` | 1,242 |
| `time_us` | 3,988,282 |
| declared subpath total | 8,705,117 |

The non-declared/shared JSON remainder dominates the ClickHouse footprint:

| shared JSON class | compressed bytes including marks |
| --- | ---: |
| bucket values | 90,790,626 |
| bucket offsets | 1,932,479 |
| bucket paths | 352,880 |
| object structure | 1,500 |
| shared/object total | 93,077,485 |

The largest individual streams are all shared JSON value buckets:

| decoded substream | bytes |
| --- | ---: |
| `data.object_shared_data.26.values` | 31,985,930 |
| `data.object_shared_data.28.values` | 15,163,151 |
| `data.object_shared_data.21.values` | 8,990,281 |
| `data.object_shared_data.20.values` | 6,706,714 |
| `data.object_shared_data.30.values` | 5,610,903 |
| `data.object_shared_data.27.values` | 5,244,695 |
| `data.object_shared_data.16.values` | 5,159,386 |
| `data.did` | 4,453,842 |
| `data.object_shared_data.17.values` | 4,335,606 |
| `data.time_us` | 3,988,282 |

## Compression, Marks, And Sparse Indexes

The single ClickHouse column reports:

| metric | bytes |
| --- | ---: |
| compressed data | 101,761,540 |
| uncompressed data | 690,012,142 |
| compression ratio | 6.78x |
| marks | 21,565 |
| primary index | 2,994 |

The active part has `124` marks for `1,000,000` rows, consistent with
approximately 8,065 rows per mark and the configured `index_granularity = 8192`
with adaptive edge effects.

Mark and primary-index overhead is extremely small:

| overhead | bytes | bytes per row |
| --- | ---: | ---: |
| `.cmrk2` marks | 21,565 | 0.0216 |
| `primary.cidx` | 2,994 | 0.0030 |
| marks plus primary index | 24,559 | 0.0246 |

This is the direct ClickHouse counterpart to TreeDB's pruning metadata,
locators, and any row-to-column address structures. TreeDB parity should not
carry multi-megabyte pruning metadata for this workload unless it buys a
measured query win that ClickHouse does not attempt.

## Product Interpretation

ClickHouse is not just compressing raw row JSON harder. It changes the storage
problem:

1. Sort rows by the q1-q5 access key:
   `kind`, `operation`, `collection`, `did`, `time_us`.
2. Promote query-relevant paths into typed substreams.
3. Store low-cardinality strings as tiny dictionaries plus codes.
4. Store high-cardinality `did` and numeric `time_us` as ordinary typed
   streams.
5. Store all remaining object data in 32 shared-object buckets with separate
   values, paths, and offsets.
6. Use very sparse marks and a tiny primary index.

The ClickHouse audit does not prove byte-for-byte preservation of the original
source JSON text. It proves compact semantic storage of the JSON object under
ClickHouse JSON v3 serialization. For TreeDB, this means the parity question
must separate:

- semantic document reconstruction
- byte-for-byte source JSON retention
- compatibility row/payload retention for older TreeDB APIs

If TreeDB must retain exact source JSON bytes, ClickHouse is not an apples to
apples lower bound. If semantic reconstruction is acceptable for the production
JSONBench path, ClickHouse gives a clear design target: store a compact shared
object representation and avoid duplicating declared paths in both retained
payload and column assets.

## Implications For TreeDB Workstreams

### #2662 Retained `value_vlog`

Current evidence from #2359 after #2647/#2648:

| TreeDB class | bytes |
| --- | ---: |
| durable excluding command WAL | 179,961,790 |
| `value_vlog` | 118,481,571 |
| `column_asset_segments` | 51,663,196 |
| `leaf_vlog` | 9,226,133 |
| row/compat assets | 17,007,056 |

ClickHouse stores about `93.08 MB` of shared/object JSON remainder after the
declared scalar paths. Therefore a blind `value_vlog <= 65 MB` target is not
the right first technical requirement unless some of the semantic remainder is
moved out of `value_vlog` into shared-object column assets or unless TreeDB's
retention contract is allowed to drop source-byte fidelity and beat ClickHouse
compression on the remainder.

Concrete #2662 requirements:

- Define the production JSONBench retention contract:
  semantic JSON object reconstruction, exact source JSON bytes, or both.
- Split `value_vlog` accounting into declared-path duplicates, undeclared
  shared-object values, source JSON syntax/key overhead, and compatibility
  payload bytes.
- Add a ClickHouse-shaped retained format experiment that elides declared paths
  from the retained remainder and stores undeclared fields as shared paths plus
  values.
- Measure the lower bound against ClickHouse's `93,077,485` byte shared/object
  total, not only against the previous rough `65 MB` budget.
- Keep persistent value-log semantics: this is durable storage and must remain
  GC/rewrite safe.

### #2663 Column Asset Segments

ClickHouse's declared scalar paths cost only about `8.71 MB` including marks.
The three LowCardinality paths are especially small:

| path class | bytes |
| --- | ---: |
| LowCardinality dictionaries plus codes for `kind`, `operation`, `collection` | 4,448 |
| `did` chars plus offsets | 4,712,890 |
| `time_us` | 3,988,282 |

Concrete #2663 requirements:

- Treat large typed dictionaries as suspect until they can explain why
  ClickHouse's three low-cardinality paths fit in about `4.4 KB`.
- Keep declared column encodings in one compact part-like layout, with marks
  near ClickHouse's `0.0216` bytes per row rather than megabytes.
- Avoid row/compat duplication of the same scalar values in declared columns,
  retained payload, and compatibility assets.
- Decide whether shared-object buckets are part of column assets or retained
  payload before locking a `column_asset_segments <= 25 MB` target. Declared
  q1-q5 paths alone fit well below that target; declared paths plus the full
  shared JSON remainder do not.

### #2359 Evidence Gate

The current-head storage parity gate should add a ClickHouse audit line beside
the copied `result.json` totals:

| ClickHouse byte class | bytes |
| --- | ---: |
| shared/object JSON remainder | 93,077,485 |
| declared q1-q5 subpaths | 8,705,117 |
| LowCardinality dictionary streams not attributed to subcolumns | 503 |
| primary index | 2,994 |
| count plus serialization metadata | 139 |
| system.parts total | 101,786,238 |

Future TreeDB evidence should report the same classes:

- retained/shared-object remainder
- declared q1-q5 scalar data
- dictionaries and codes
- locators/marks/pruning metadata
- primary/sort key metadata
- compatibility/source-byte payload bytes

## Reproduction Commands

Start the local ClickHouse server from the JSONBench checkout:

```sh
cd /Users/michaelseiler/dev/snissn/JSONBench
clickhousectl local server start --name jsonbench-local --foreground
```

In another shell, collect system-table evidence:

```sh
DB=jsonbench_local_1000000_20260515_184738_65766
TABLE=bluesky

clickhousectl local client --name jsonbench-local \
  --query "SHOW CREATE TABLE ${DB}.${TABLE} FORMAT TSVRaw" \
  > show_create_table.sql

clickhousectl local client --name jsonbench-local \
  --query "SELECT * FROM system.parts WHERE database='${DB}' AND table='${TABLE}' FORMAT JSONEachRow" \
  > system_parts.jsonl

clickhousectl local client --name jsonbench-local \
  --query "SELECT * FROM system.parts_columns WHERE database='${DB}' AND table='${TABLE}' FORMAT JSONEachRow" \
  > system_parts_columns.jsonl

clickhousectl local client --name jsonbench-local \
  --query "SELECT * FROM system.columns WHERE database='${DB}' AND table='${TABLE}' FORMAT JSONEachRow" \
  > system_columns.jsonl
```

Then walk the part directory from `system.parts.path` and record each file's
logical and allocated size. The local audit bundle named above includes those
outputs as `parts/part_files.jsonl` and `parts/substream_files.jsonl`.

## Follow-Ups

- Update #2680 with this audit and keep it open only if we still want a checked
  parser/tool with fixtures for ClickHouse part manifests.
- Update #1462 with the final byte-class map and the retention-contract
  distinction.
- Update #2359 so future parity evidence records TreeDB bytes in the same
  classes as ClickHouse.
- Update #2662 to revise the retained-payload budget around semantic shared
  object storage versus exact source-byte retention.
- Update #2663 to target ClickHouse-sized declared scalar encodings and tiny
  mark/locator metadata before larger format work.
