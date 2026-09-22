# Two-Index Collection Insert Exhaustive-Compact Rerun

Date: 2026-06-04 HST / 2026-06-04 UTC

This rerun refreshes the README two-index collection insert rows after the
canonical benchmark learned an explicit `exhaustive_compact` phase. It uses the
same primary shape as the June 3 latest-main report, but the public TreeDB
compacted-size headline now comes from the byte-minimized/VACUUM-equivalent
`treemap compact <dir> -rw -mode exhaustive` row rather than the production
`offline_compact` row or lower-level leaf-generation diagnostics.

These are developer-machine results. Treat them as current local engineering
evidence, not a formal product benchmark.

## Command

```sh
OUT=/tmp/collections_exhaustive_main_$(date +%Y%m%d_%H%M%S)
mkdir -p "$OUT"
GOWORK=off USE_BUILT_BIN=1 ./scripts/bench_collections_canonical.sh \
  -out-dir "$OUT" \
  -formats template-v1,json \
  -indexes 2 \
  -docs 100000 \
  -batch-size 16000 \
  -count 1 \
  2>&1 | tee "$OUT.stdout.log"
```

Measured run:

```text
/tmp/collections_exhaustive_main_20260604_113639
```

Measured code:

```text
branch: main
commit: d64a06b8e710
```

Host context:

```text
OS: Linux mikers-B560-DS3H-AC-Y1 6.8.0-111-generic x86_64
CPU: 11th Gen Intel(R) Core(TM) i5-11400F @ 2.60GHz, 6 cores / 12 threads
Go: go1.25.7 linux/amd64
```

Generated artifacts:

- `/tmp/collections_exhaustive_main_20260604_113639/benchmark_summary.md`
- `/tmp/collections_exhaustive_main_20260604_113639/benchmark_results.json`
- `/tmp/collections_exhaustive_main_20260604_113639/benchmark_matrix.csv`
- `/tmp/collections_exhaustive_main_20260604_113639/timed_matrix/collections_matrix_summary.md`
- `/tmp/collections_exhaustive_main_20260604_113639/offline_compact/compression_matrix.tsv`
- `/tmp/collections_exhaustive_main_20260604_113639/exhaustive_compact/compression_matrix.tsv`

## Timed Insert Rows

These rows are benchmark-timed post-insert measurements for two secondary
indexes, `100000` documents, batch size `16000`, and `command_wal_relaxed` for
TreeDB. The B/doc column in this section is the post-insert footprint after the
benchmark flush or checkpoint. It is not a compacted-state number.

The canonical runner uses `storage-cells=index-vlog`, which is the TreeDB layout
that writes data and index outer leaves to the value log.

| engine / format | layout | ns/doc | docs/sec | post-insert B/doc |
| --- | --- | ---: | ---: | ---: |
| TreeDB template-v1 | data and index outer leaves in value log | 1,434 | 697,350 | 211.6 |
| TreeDB JSON | data and index outer leaves in value log | 2,103 | 475,511 | 224.5 |
| SQLite native columns | WAL normal | 3,011 | 332,116 | 176.1 |
| SQLite JSON | WAL normal | 3,535 | 282,885 | 262.6 |

## Compacted Storage Rows

These rows separate production compaction, byte-minimized compaction, and
lower-level diagnostics. README compacted-size claims should use
`exhaustive_compact` for TreeDB template-v1 and SQLite `VACUUM` for SQLite.
TreeDB JSON is still omitted from the README compacted-size headline because the
canonical exhaustive offline fixture currently covers template-v1 rows; the JSON
`full_leafgen_pack_gc` row below remains diagnostic only.

| engine / format | phase | B/doc | comparison basis | README headline? |
| --- | --- | ---: | --- | --- |
| TreeDB template-v1 | `exhaustive_compact` | 22.8 | byte-minimized `treemap compact <dir> -rw -mode exhaustive` | yes |
| TreeDB template-v1 | `offline_compact` | 46.7 | production `treemap compact <dir> -rw -mode full` | no |
| TreeDB template-v1 | `full_leafgen_pack_gc` | 27.8 | diagnostic full leaf generation pack/GC plus offline index vacuum | no |
| TreeDB JSON | `full_leafgen_pack_gc` | 33.7 | diagnostic full leaf generation pack/GC plus offline index vacuum | no |
| SQLite native columns | `sqlite_vacuum` | 156.7 | SQLite `VACUUM` | yes |
| SQLite JSON | `sqlite_vacuum` | 231.7 | SQLite `VACUUM` | yes |

Derived comparison ratios from `benchmark_results.json`:

| TreeDB row | vs SQLite native columns after `VACUUM` | vs SQLite JSON after `VACUUM` | README headline? |
| --- | ---: | ---: | --- |
| template-v1 `exhaustive_compact` | 6.9x smaller | 10.2x smaller | yes |
| template-v1 `offline_compact` | 3.4x smaller | 5.0x smaller | no, production compaction row |
| template-v1 `full_leafgen_pack_gc` | 5.6x smaller | 8.3x smaller | no, diagnostic only |
| JSON `full_leafgen_pack_gc` | 4.7x smaller | 6.9x smaller | no, diagnostic only |

## Comparison With June 3 Latest-Main Report

Previous checked-in report:
`docs/benchmarks/collections_insert_two_index_latest_main_2026-06-03.md`.

| engine / format | previous docs/sec | current docs/sec | docs/sec delta | previous README compacted B/doc | current README compacted B/doc | B/doc delta |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| TreeDB template-v1 | 600,962 | 697,350 | +16.0% | 46.7 (`offline_compact`) | 22.8 (`exhaustive_compact`) | -51.2% |
| TreeDB JSON | 450,857 | 475,511 | +5.5% | — | — | — |
| SQLite native columns | 344,353 | 332,116 | -3.6% | 156.7 | 156.7 | +0.0% |
| SQLite JSON | 296,912 | 282,885 | -4.7% | 231.7 | 231.7 | +0.0% |

The main storage change is the comparison contract: the TreeDB template-v1
README compacted row now uses an explicit byte-minimized high-level compaction
mode. The older `offline_compact` number remains useful as the production
compaction point, and `full_leafgen_pack_gc` remains useful for diagnosing leaf
log packing behavior, but neither is the public storage-floor headline.
