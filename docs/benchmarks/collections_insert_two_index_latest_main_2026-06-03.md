# Two-Index Collection Insert Latest Main Rerun

Date: 2026-06-03 HST / 2026-06-04 UTC

This rerun refreshes the README benchmark claim for the two-index collection
insert workload on latest `origin/main` after the June 3 optimization stack. It
uses the canonical collection benchmark entry point with the same high-level
shape as the June 2 report.

These are developer-machine results. Treat them as current local engineering
evidence, not a formal product benchmark.

## Command

```sh
OUT=/tmp/collections_insert_latest_main_$(date +%Y%m%d_%H%M%S)
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
/tmp/collections_insert_latest_main_20260603_210058
```

Measured code:

```text
branch: main
commit: d7407b81cc5712374ca8c1588cfb05f6f7d8490d
```

Generated artifacts:

- `/tmp/collections_insert_latest_main_20260603_210058/benchmark_summary.md`
- `/tmp/collections_insert_latest_main_20260603_210058/benchmark_results.json`
- `/tmp/collections_insert_latest_main_20260603_210058/benchmark_matrix.csv`
- `/tmp/collections_insert_latest_main_20260603_210058/timed_matrix/collections_matrix_summary.md`

## Timed Insert Rows

These rows are benchmark-timed post-insert measurements for two secondary
indexes, `100000` documents, batch size `16000`, and `command_wal_relaxed` for
TreeDB. The B/doc column in this section is the post-insert footprint after the
benchmark flush or checkpoint. It is not a compacted-state number.

The canonical runner uses `storage-cells=index-vlog`, which is the TreeDB layout
that writes data and index outer leaves to the value log.

| engine / format | layout | ns/doc | docs/sec | post-insert B/doc |
| --- | --- | ---: | ---: | ---: |
| TreeDB template-v1 | data and index outer leaves in value log | 1,664 | 600,962 | 211.6 |
| TreeDB JSON | data and index outer leaves in value log | 2,218 | 450,857 | 224.5 |
| SQLite native columns | WAL normal | 2,904 | 344,353 | 176.1 |
| SQLite JSON | WAL normal | 3,368 | 296,912 | 262.6 |

## Compacted Storage Rows

These rows separate the current high-level compact path from lower-level
leaf-generation maintenance diagnostics. The root README uses the
`offline_compact` TreeDB template-v1 row as the current compacted-size headline
because `full_leafgen_pack_gc` can leave a large writable/current leaf generation
behind and is not a stable byte-minimized storage-floor contract. A true
exhaustive/VACUUM-equivalent TreeDB mode is tracked in
[#2288](https://github.com/snissn/gomap/issues/2288).

| engine / format | phase | B/doc | comparison basis | README headline? |
| --- | --- | ---: | --- | --- |
| TreeDB template-v1 | `offline_compact` | 46.7 | high-level `treemap compact <dir> -rw` | yes |
| TreeDB template-v1 | `full_leafgen_pack_gc` | 27.8 | diagnostic full leaf generation pack/GC plus offline index vacuum | no |
| TreeDB JSON | `full_leafgen_pack_gc` | 33.7 | diagnostic full leaf generation pack/GC plus offline index vacuum | no |
| SQLite native columns | `sqlite_vacuum` | 156.7 | SQLite `VACUUM` | yes |
| SQLite JSON | `sqlite_vacuum` | 231.7 | SQLite `VACUUM` | yes |

Derived comparison ratios from `benchmark_results.json`:

| TreeDB row | vs SQLite native columns after `VACUUM` | vs SQLite JSON after `VACUUM` | README headline? |
| --- | ---: | ---: | --- |
| template-v1 `offline_compact` | 3.4x smaller | 5.0x smaller | yes |
| template-v1 `full_leafgen_pack_gc` | 5.6x smaller | 8.3x smaller | no, diagnostic only |
| JSON `full_leafgen_pack_gc` | 4.7x smaller | 6.9x smaller | no, diagnostic only |

## Comparison With June 2 Current Report

Previous checked-in report:
`docs/benchmarks/collections_insert_two_index_current_2026-06-02.md`.

| engine / format | previous docs/sec | current docs/sec | docs/sec delta | previous README compacted B/doc | current README compacted B/doc | B/doc delta |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| TreeDB template-v1 | 626,959 | 600,962 | -4.1% | 49.7 | 46.7 | -6.1% |
| TreeDB JSON | 419,463 | 450,857 | +7.5% | — | — | — |
| SQLite native columns | 330,797 | 344,353 | +4.1% | 156.7 | 156.7 | +0.0% |
| SQLite JSON | 302,115 | 296,912 | -1.7% | 231.7 | 231.7 | +0.0% |

The latest-main collection insert rerun is broadly in the same range as the June
2 evidence. It does not show the same clear improvement that the YCSB server
workload shows: template-v1 throughput is slightly lower, JSON throughput is
higher, and the current high-level TreeDB template-v1 compacted row improved
from 49.7 B/doc to 46.7 B/doc. The lower `full_leafgen_pack_gc` rows remain in
this report as diagnostics, but they are no longer used as README compacted-size
headlines because they can leave writable/current leaf-generation bytes behind.

## Additional Layout Probe

A separate current-main layout probe was also run to keep the README context
honest when comparing TreeDB outer-leaf placement choices:

```text
/tmp/collections_insert_layout_latest_main_20260603_210000
```

That probe included both TreeDB layout cells and showed:

| engine / format | layout | ns/doc | docs/sec | post-insert B/doc |
| --- | --- | ---: | ---: | ---: |
| TreeDB template-v1 | data outer leaves in value log, index outer leaves in pager | 1,413 | 707,714 | 232.7 |
| TreeDB template-v1 | data and index outer leaves in value log | 1,455 | 687,285 | 211.6 |
| TreeDB JSON | data outer leaves in value log, index outer leaves in pager | 1,980 | 505,051 | 243.2 |
| TreeDB JSON | data and index outer leaves in value log | 2,060 | 485,437 | 224.5 |
| SQLite JSON | WAL normal | 3,206 | 311,915 | 262.6 |
| SQLite native columns | WAL normal | 3,376 | 296,209 | 176.1 |

The root README uses the canonical timed throughput rows, but only uses the
high-level `offline_compact` TreeDB template-v1 row for its compacted-size
highlight until the exhaustive compact mode from #2288 exists.

## Guardrail Notes

The canonical runner emitted the same non-blocking notes as the June 2 report:

- `phase.online_one_pass.partial`: online one-pass maintenance is partial and
  should not be described as full compaction.
- `raw_shape_labeled`: raw TreeDB rows are labeled separately and should not be
  mixed with collection rows.
