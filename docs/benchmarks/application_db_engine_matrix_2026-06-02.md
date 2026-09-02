# application.db Offline Density Comparison: TreeDB vs goleveldb vs PebbleDB

Date: 2026-06-02

## Summary

This rerun rebuilds a fresh Celestia mainnet `application.db` snapshot into
TreeDB, goleveldb, and PebbleDB, then runs each engine's offline cleanup path.
The TreeDB result uses latest `origin/main` at
`94e322a0727f7a0d07154a006e9fa4ec4e3936f4`.

Primary compacted-size result:

| engine | compacted size | workflow |
| --- | ---: | --- |
| TreeDB | 1.690 GiB | `command_wal_relaxed`, rebuild, `CompactStorageFull`, offline index vacuum |
| PebbleDB | 2.108 GiB | snappy, 64 KiB blocks, 64 MiB target files, full compact |
| goleveldb | 2.221 GiB | snappy, 64 KiB blocks, restart interval 256, full compact |

TreeDB is now the smallest result in the reproducible three-engine run. It also
rebuilt the source corpus fastest, at the cost of higher peak RSS during
rebuild.

## Source Corpus

The source corpus came from a fresh Celestia mainnet state-sync restore using
goleveldb.

| item | value |
| --- | --- |
| Celestia binary | `celestia-app v8.0.8` |
| Go toolchain for Celestia binary | `go1.26.2` |
| source home | `/home/mikers/.celestia-app-mainnet-goleveldb-20260602102758` |
| source DB | `/home/mikers/.celestia-app-mainnet-goleveldb-20260602102758/data/application.db` |
| final local height | `11361318` |
| frozen target height | `11361263` |
| source size used by matrix | `2.573 GiB` |
| keys copied | `46,798,257` |
| key bytes copied | `1,467,857,658` |
| value bytes copied | `4,090,720,389` |

The state-sync run completed cleanly:

```text
sync_complete_utc=2026-06-02T20:40:08Z
duration_seconds=718
final_local_height=11361318
final_remote_height=11361263
end_app_bytes=2806978561
```

## Hardware And Environment

| item | value |
| --- | --- |
| host | `mikers-B560-DS3H-AC-Y1` |
| CPU | 11th Gen Intel Core i5-11400F, 6 cores / 12 threads |
| memory | 31 GiB RAM |
| filesystem | `/dev/nvme0n1p2`, NVMe-backed root filesystem |
| date captured | 2026-06-02 |

## Results

| engine | rebuild s | compact s | rebuild max RSS GiB | compact max RSS GiB | rebuild size GiB | final size GiB | delta vs source GiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| TreeDB | 31.89 | 67.35 | 4.711 | 1.450 | 2.115 | 1.690 | -0.883 |
| PebbleDB | 65.38 | 8.42 | 0.502 | 0.022 | 2.171 | 2.108 | -0.465 |
| goleveldb | 83.14 | 59.58 | 0.323 | 0.051 | 2.222 | 2.221 | -0.352 |

TreeDB compact stats:

| stat | value |
| --- | ---: |
| fully compacted | true |
| phase count | 17 |
| leaf generation GC files deleted | 65 |
| remaining leaf GC generations | 0 |
| remaining leaf pack generations | 0 |
| remaining value-log GC segments | 0 |
| remaining value-log rewrite segments | 0 |
| value-log rewrite bytes after | 36,817,789 |

Artifacts:

- report: `/tmp/application_db_engine_matrix_current_snappy_20260602_105146/results.md`
- JSON: `/tmp/application_db_engine_matrix_current_snappy_20260602_105146/results.json`

## Reproduction

Build the current Celestia binary used for the source restore:

```bash
git -C /home/mikers/dev/snissn/celestia-app fetch upstream --tags
rm -rf /tmp/celestia-app-v8.0.8
git -C /home/mikers/dev/snissn/celestia-app worktree add \
  /tmp/celestia-app-v8.0.8 v8.0.8
cd /tmp/celestia-app-v8.0.8
GOTOOLCHAIN=auto go build -o build/celestia-appd ./cmd/celestia-appd
```

Create a fresh LevelDB source DB:

```bash
env \
  CELESTIA_APPD_BIN=/tmp/celestia-app-v8.0.8/build/celestia-appd \
  DB_BACKEND=goleveldb \
  APP_DB_BACKEND=goleveldb \
  FREEZE_REMOTE_HEIGHT_AT_START=1 \
  POST_SYNC_DWELL_SECONDS=0 \
  CAPTURE_HEAP_ON_MAX_RSS=0 \
  CAPTURE_FULL_SMAPS_ON_MAX_RSS=0 \
  CAPTURE_DEBUG_VARS_ON_MAX_RSS=0 \
  CAPTURE_PPROF_ON_STUCK=0 \
  CAPTURE_PPROF_ON_WARN_STUCK=0 \
  NO_PROGRESS_FAIL_SECONDS=3600 \
  NO_PROGRESS_HARD_FAIL_SECONDS=7200 \
  KEEP_RECENT_RUNS=12 \
  /home/mikers/dev/snissn/celestia-app-p4/scripts/mainnet-treedb-fast-sync-forensics.sh
```

Run the matrix against latest TreeDB:

```bash
git -C /home/mikers/dev/snissn/gomap-clean fetch origin main
rm -rf /tmp/gomap-latest-main-bench
git -C /home/mikers/dev/snissn/gomap-clean worktree add \
  --detach /tmp/gomap-latest-main-bench origin/main

OUT_DIR=/tmp/application_db_engine_matrix_current_snappy_$(date +%Y%m%d_%H%M%S)
SOURCE_APP_DB=/home/mikers/.celestia-app-mainnet-goleveldb-20260602102758/data/application.db

TREEDB_REPO=/tmp/gomap-latest-main-bench \
COSMOS_DB_REPO=/home/mikers/dev/snissn/cosmos-db \
TREEDB_PROFILE=command_wal_relaxed \
TREEDB_FORCE_CHECKPOINT_ON_WRITE=0 \
GOLEVELDB_BLOCK_SIZE=65536 \
GOLEVELDB_BLOCK_RESTART_INTERVAL=256 \
PEBBLE_COMPRESSION=snappy \
PEBBLE_BLOCK_SIZE=65536 \
PEBBLE_TARGET_FILE_SIZE=67108864 \
OUT_DIR="$OUT_DIR" \
/tmp/gomap-latest-main-bench/scripts/run_application_db_engine_matrix.sh \
  "$SOURCE_APP_DB"
```

## Notes

This rerun exposed a benchmark-harness edge case: current Celestia data contains
zero-length values, and goleveldb rejects nil values. The matrix runner now
copies source values with `append([]byte{}, iter.Value()...)` so zero-length
values remain non-nil empty values. The logical data copied is unchanged.

The old April report used a PebbleDB zstd aggressive row. Repeating zstd on this
machine rebuilt PebbleDB to about `1.745 GiB`, but the compact step failed when
reopening/scanning the generated tables:

```text
pebble/table: decompressed into unexpected buffer
```

That zstd attempt is not used as a published result here. The table above uses
the complete reproducible snappy run for all three engines.

## Bottom Line

On the current Celestia source corpus and current TreeDB compaction path, the
headline offline compacted sizes are:

- TreeDB: `1.690 GiB`
- PebbleDB: `2.108 GiB`
- goleveldb: `2.221 GiB`

This supersedes the README's older April 13 application-db density headline.
