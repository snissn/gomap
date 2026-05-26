# TreeDB Column-Store JSONBench 10M Experiment

This note records a benchmark experiment comparing TreeDB's production
column-store JSONBench path with ClickHouse JSON on a local 10M-row Bluesky
fixture. It is a technical experiment, not a product claim: TreeDB is
pre-alpha, the column-store cells are query-shaped projections, and the
ClickHouse cell stores the full JSON object.

## Goal

Measure the preferred long-running/server-shaped TreeDB column-store rows
against ClickHouse for JSONBench q1..q5:

- q1/q2/q3 use prepared TreeDB physical query runners.
- q4/q5 use TreeDB aggregate metadata with Top-K result shaping.
- direct one-shot TreeDB rows are not the primary comparison here because they
  include setup cost a long-running service should amortize.

## Reproduction entry point

The reproduction entry point lives in JSONBench:

```sh
cd /path/to/JSONBench/treedb
ROWS=10000000 TRIES=3 GOMAP_REPLACE=/path/to/gomap \
  ./run_preferred_columnstore_clickhouse_compare.sh
```

Prerequisites:

- Go matching JSONBench's `treedb/go.mod` toolchain.
- `jq`.
- `clickhouse` with `clickhouse local` support.
- JSONBench Bluesky data under `DATA_DIR`, defaulting to
  `$HOME/data/bluesky`.
- A gomap checkout at the commit under test. Pass it through
  `GOMAP_REPLACE`; the script restores JSONBench `go.mod`/`go.sum` after the
  run.

Useful overrides:

```sh
# Put artifacts somewhere explicit.
OUT_DIR=/tmp/jsonbench_preferred_10m \
ROWS=10000000 TRIES=3 GOMAP_REPLACE=/path/to/gomap \
  ./run_preferred_columnstore_clickhouse_compare.sh

# Re-render/reuse an existing half of a run.
RUN_TREEDB=0 ./run_preferred_columnstore_clickhouse_compare.sh
RUN_CLICKHOUSE=0 ./run_preferred_columnstore_clickhouse_compare.sh
```

Primary outputs:

- `preferred_summary.md` — compact TreeDB-vs-ClickHouse table.
- `treedb/report.md`, `treedb/report.json` — TreeDB JSONBench report.
- `clickhouse/result.json` — ClickHouse JSONBench-compatible result.

The TreeDB helper `run_columnstore_benchmark.sh` also accepts
`ROWS=10000000`; it maps that row count to the JSONBench `10m` scale so all ten
input files are loaded.

## What the script runs

TreeDB runs only the preferred/server-shaped rows:

| Query | TreeDB row in summary | Timed behavior |
| --- | --- | --- |
| q1 | `column-store-prepared` | prepared physical group-count by event |
| q2 | `column-store-prepared` | prepared physical count + distinct user count |
| q3 | `column-store-prepared` | prepared physical event + hour count |
| q4 | `column-store-prepared-metadata` | aggregate metadata Top-K min post time |
| q5 | `column-store-prepared-metadata` | aggregate metadata Top-K activity span |

Current JSONBench stores q1/q2/q3 under the
`column-store-prepared-metadata` storage-layout string because that layout
prepares all physical runners and only q4/q5 declare aggregate metadata. The
summary relabels q1/q2/q3 to `column-store-prepared` to make the semantics
explicit.

ClickHouse runs the standard JSONBench `clickhouse/queries.sql` against a
`clickhouse local --path` MergeTree database using `JSONAsObject`.

## Current run

Run host:

- Host: `mikers@192.168.0.185` (`mikers-B560-DS3H-AC-Y1`).
- CPU: `11th Gen Intel(R) Core(TM) i5-11400F @ 2.60GHz`, 6 cores / 12
  threads.
- RAM: 31 GiB.
- OS: Ubuntu Linux 22.04 family, kernel `6.8.0-110-generic`.

Inputs and versions:

- JSONBench commit: `5fca4d68f3f06909de0cc87b2f012bc3583a3170`.
- gomap commit: `29dd3cb3857baff8b7889c21b8be30957f9e9bc2`.
- ClickHouse version: `26.4.2.10`.
- TreeDB rows loaded: 10,000,000.
- ClickHouse rows loaded: 9,999,994 of 10,000,000 requested.
- Tries: 3 query attempts.

Artifacts on the run host:

- `/home/mikers/jsonbench_runs/preferred_10m_20260526_220848/preferred_10m_treedb_clickhouse_summary.md`
- `/home/mikers/jsonbench_runs/preferred_10m_20260526_220848/treedb_10m_preferred/report.md`
- `/home/mikers/jsonbench_runs/preferred_10m_20260526_220848/clickhouse_10m/result.json`

## Results

| system/layout | query | best | loaded rows/s | scanned rows | storage | load | TreeDB vs ClickHouse |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| TreeDB `column-store-prepared` | q1 | 0.0122s | 822.9M | 10.0M | 1.11 GiB | 54.754s | 2.1x faster |
| ClickHouse JSON | q1 | 0.0260s | 384.6M | 10.0M | 986.82 MiB | 52.632s | baseline |
| TreeDB `column-store-prepared` | q2 | 0.0518s | 192.9M | 10.0M | 3.21 GiB | 105.283s | 3.8x faster |
| ClickHouse JSON | q2 | 0.1970s | 50.8M | 10.0M | 986.82 MiB | 52.632s | baseline |
| TreeDB `column-store-prepared` | q3 | 0.1201s | 83.3M | 10.0M | 2.63 GiB | 103.099s | 1.1x slower |
| ClickHouse JSON | q3 | 0.1060s | 94.3M | 10.0M | 986.82 MiB | 52.632s | baseline |
| TreeDB `column-store-prepared-metadata` | q4 | 0.0100s | 996.4M logical | 0 | 3.10 GiB | 124.950s | 8.9x faster |
| ClickHouse JSON | q4 | 0.0890s | 112.4M | 10.0M | 986.82 MiB | 52.632s | baseline |
| TreeDB `column-store-prepared-metadata` | q5 | 0.0098s | 1.02B logical | 0 | 3.10 GiB | 125.455s | 6.8x faster |
| ClickHouse JSON | q5 | 0.0670s | 149.3M | 10.0M | 986.82 MiB | 52.632s | baseline |

ClickHouse attempts:

| query | attempts | best | median |
| ---: | --- | ---: | ---: |
| q1 | 0.0270s, 0.0260s, 0.0400s | 0.0260s | 0.0270s |
| q2 | 0.2370s, 0.1970s, 0.2120s | 0.1970s | 0.2120s |
| q3 | 0.1160s, 0.1060s, 0.1310s | 0.1060s | 0.1160s |
| q4 | 0.1000s, 0.0890s, 0.0900s | 0.0890s | 0.0900s |
| q5 | 0.0670s, 0.0830s, 0.0680s | 0.0670s | 0.0680s |

## Interpretation

- TreeDB's prepared q1/q2 rows are faster than ClickHouse on this fixture.
- q3 is close but ClickHouse is slightly faster in this run.
- TreeDB q4/q5 are much faster because they use aggregate metadata and scan no
  base rows during the timed query. The rows/s value for those rows is logical
  loaded rows/s, not scanned rows/s.
- TreeDB load time is per query-shaped cell. q4/q5 have extra aggregate
  metadata construction cost during load.

## Caveats

- TreeDB column-store cells are query-shaped projections with retained payload
  disabled. ClickHouse stores the full JSON object. Storage size is therefore
  not an apples-to-apples product comparison.
- ClickHouse loading used JSONBench-compatible `JSONAsObject` with
  `input_format_allow_errors_*` fallback because the 10M source contains rows
  ClickHouse rejects as invalid JSON. The ClickHouse query rates use the actual
  loaded row count, 9,999,994.
- The comparison uses local `clickhouse local --path`, not a separately tuned
  ClickHouse server deployment.
- TreeDB is pre-alpha. Public APIs, benchmark harness details, and on-disk
  formats may change.

## Follow-ups

- Track the benchmark-shape cleanup in gomap issue #1889: introduce an explicit
  `column-store-prepared` JSONBench layout instead of relabeling q1/q2/q3 in
  summaries.
- Keep direct one-shot rows separate from server-shaped prepared rows when
  reporting future JSONBench results.
