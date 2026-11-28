# TreeDB Deep Benchmark Refresh (2026-11-28, Sprint 22)

This report records the three-tier deep benchmark refresh (`relaxed`, `fast`,
and `durable`) run on `192.168.0.185` (`mikers-B560-DS3H-AC-Y1`) at `main`
commit `db277fee669c6487ffbfd627d9cf2af6ebb120ab` (`db277fee6`), immediately
following Sprint 22:

- **PR `#5936`**: Re-enabled `ValueLog.TemplateMode` (`TemplateOnly` and
  combined `TemplatePrepass` + Zstd dictionary compression) with piggybacked
  template staging and trainer cooldown fix.
- **PR `#5937`**: Enabled BSON and steady-state `template-v1` on the
  no-index buffered insert path, added zero-alloc fast BSON validation and
  parallel validation/template resolution, and expanded single-pass BSON index
  extraction from `<= 8` to `<= 64` indexes.
- **PR `#5938`**: Eliminated client-side insert saturation overhead in
  `cmd/mongo_gateway_bench` (fairly for both TreeDB and MongoDB via prebuilt
  per-batch `bson.Raw` command buffers for `driver-command-raw`), expanded
  secondary-index sweeps to `0, 1, 2, 4, 8, 16, 32` with an identical 32-field
  document shape (`-doc-extra-index-fields 32`), and added persistent HTML
  report support.
- **PR `#5939`**: Batched value-log appends (`2048` entries / `2 MiB`) inside
  `BuildNativeOrderedRootColdScan` to unlock TreeDB's 8-lane parallel value-log
  appenders during cold checkpoints (`2.1x–2.6x` faster settled load on ~1.0 KB
  BSON docs), raised `StandaloneCOWMemtableLimits()` `MaxSources` (`32 -> 128`)
  and `MaxGenerations` (`64 -> 256`), and added soft/hard `MaxTotalBytes` and
  `MaxRetiredBytes` pressure drainage for 32+ secondary indexes.

## Persistent HTML Reports

The three standalone interactive HTML reports are checked into the repository
and updated/overwritten on each future deep-benchmark refresh:

- [Relaxed WAL Deep Report (`command_wal_relaxed`)](deep_report_relaxed.html)
- [Fast No-WAL Deep Report (`no_wal_fast`)](deep_report_fast.html)
- [Durable WAL Deep Report (`command_wal_durable`)](deep_report_durable.html)

## Environment & Reproduction

- **Measured Commit**: `db277fee669c6487ffbfd627d9cf2af6ebb120ab`
- **Host**: `mikers-B560-DS3H-AC-Y1`, Linux `6.8.0-138-generic` `x86_64`
- **CPU**: 11th Gen Intel Core i5-11400F @ 2.60 GHz (6 cores / 12 threads)
- **External Comparators**: Docker `mongo:8`, SQLite via Go CGO (`sqlite_bench`)
- **Execution Discipline**: Three sequential runs on a quiet host with
  disposable DB directories and isolated Docker MongoDB containers (`GOWORK=off`):
  - Relaxed (`/tmp/gomap_deep_bench_s22_v3_relaxed`):
    `scripts/treedb_benchmark_run_report.sh --out /tmp/gomap_deep_bench_s22_v3_relaxed --tier pr --durability-tier relaxed --collection-docs 100000 --mongo-docs 100000 --mongo-load-indexes "4 8 16 32" --mongo-treedb-open-mode standalone --skip-raw --skip-load-modes --skip-collection-profiles`
  - Fast (`/tmp/gomap_deep_bench_s22_v3_fast`): same flags with `--durability-tier fast`
  - Durable (`/tmp/gomap_deep_bench_s22_v3_durable`): same flags with `--durability-tier durable`

---

## 1. Indexed Collection Insert Workload (`100k` Docs, Batch Size `16,000`)

All TreeDB rows match the canonical `Post-Insert Throughput by Index Count`
series in the HTML reports (`-count 1`, `100,000` documents, batch size
`16,000`, data and index outer leaves in value log). Compacted B/doc uses
`exhaustive_compact` for TreeDB `template-v1` (with `full_leafgen_pack_gc` and
`offline_compact` also recorded), `full_leafgen_pack_gc` for TreeDB BSON and
JSON, and SQLite after `VACUUM`.

### 1.1 Relaxed Tier (`command_wal_relaxed` vs. SQLite `WAL + NORMAL`, `2` Secondary Indexes)

| engine / format | layout | docs/sec | compacted B/doc | compaction method |
| --- | --- | ---: | ---: | --- |
| TreeDB BSON | data and index outer leaves in value log | 945,180 | 25.3 | `full_leafgen_pack_gc` |
| TreeDB template-v1 | data and index outer leaves in value log | 874,891 | 23.0 | `exhaustive_compact` (`24.2` `full_leafgen_pack_gc`; `65.3` `offline_compact`) |
| TreeDB JSON | data and index outer leaves in value log | 856,164 | 34.4 | `full_leafgen_pack_gc` |
| SQLite native columns | WAL normal | 374,953 | 156.7 | `VACUUM` |
| SQLite JSON | WAL normal | 308,547 | 231.7 | `VACUUM` |

TreeDB BSON reaches **2.52x** SQLite native insert throughput (**3.06x** SQLite
JSON) at **6.2x** smaller compacted size than SQLite native (**9.2x** smaller
than SQLite JSON), while TreeDB `template-v1` delivers **2.33x** SQLite native
at **6.8x** smaller compacted size (**10.1x** smaller than SQLite JSON).

### 1.2 Cross-Tier Collection Insert Matrix (`0`, `1`, and `2` Secondary Indexes, docs/sec)

| tier (TreeDB / SQLite) | indexes | TreeDB template-v1 | TreeDB BSON | TreeDB JSON | SQLite native | SQLite JSON |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Relaxed (`command_wal_relaxed` / `WAL NORMAL`) | 0 | 1,474,274 | 1,509,890 | 1,795,977 | 619,579 | 754,717 |
| Relaxed (`command_wal_relaxed` / `WAL NORMAL`) | 1 | **1,223,242** | **1,260,716** | 1,058,761 | 484,966 | 409,668 |
| Relaxed (`command_wal_relaxed` / `WAL NORMAL`) | 2 | **874,891** | **945,180** | 856,164 | 374,953 | 308,547 |
| Fast (`no_wal_fast` / `WAL OFF`) | 0 | 2,474,635 | **2,986,858** | 2,853,067 | 528,262 | 673,854 |
| Fast (`no_wal_fast` / `WAL OFF`) | 1 | **1,431,434** | **1,345,171** | 1,318,739 | 483,793 | 442,282 |
| Fast (`no_wal_fast` / `WAL OFF`) | 2 | 978,474 | **1,104,606** | 1,005,025 | 377,644 | 300,030 |
| Durable (`command_wal_durable` / `WAL FULL`) | 0 | 697,837 | **987,167** | 783,085 | 442,674 | 497,760 |
| Durable (`command_wal_durable` / `WAL FULL`) | 1 | **567,215** | **636,132** | 522,466 | 362,188 | 323,102 |
| Durable (`command_wal_durable` / `WAL FULL`) | 2 | **530,223** | **590,667** | 372,301 | 287,274 | 259,336 |

Key gains from PR `#5937` versus the November 27 Sprint 21 baseline:

- **1-index inserts** jumped **+116.8%** for BSON (`581.4k -> 1,260.7k`) and
  **+77.7%** for `template-v1` (`688.2k -> 1,223.2k`) in `relaxed`, **+32.6%**
  for BSON (`479.6k -> 636.1k`) in `durable`, and **+11.4%** for `template-v1`
  (`1,284.9k -> 1,431.4k`) in `fast`. Both `template-v1` and BSON now beat JSON
  at `1` index across all three durability tiers.
- **0-index `no_wal_fast` BSON** jumped **+45.0%** (`2,059.3k -> 2,986.9k docs/s`,
  **5.65x** SQLite native), overtaking JSON as the fastest 0-index format.
- **2-index `command_wal_durable` BSON** rose **+10.4%** (`535.0k -> 590.7k docs/s`,
  **+59%** faster than JSON `372.3k` and **2.06x** SQLite native).

---

## 2. Mongo API Full-Sweep & `0..32` Secondary-Index Scaling (`100k` Docs, Standalone COW)

All runs use TreeDB Mongo gateway `driver-command-raw` in standalone COW mode
against Docker `mongo:8` with prebuilt per-batch `bson.Raw` insert commands on
both engines, batch size `2,500`, `8` insert producers, and the `age_1` range
index enabled across all index counts. To hold document payload size strictly
constant across the `0..32` secondary-index sweep, every document carries all 32
indexable fields (`-doc-extra-index-fields 32`, `988 B` / ~1.0 KB per doc vs.
`292 B` / ~300 B in prior `0..2` sweeps). Index counts `0, 1, 2` run the full
read/range/update matrix; index counts `4, 8, 16, 32` run load-only scaling
(`--mongo-load-indexes "4 8 16 32"`).

### 2.1 Insert Throughput vs. Secondary Indexes (`0, 1, 2, 4, 8, 16, 32`, docs/sec)

TreeDB load throughput includes full settled `Checkpoint()` drain time
(`settled_drain_included=1`), whereas MongoDB `w:1` acknowledges without a
synchronous checkpoint.

| secondary indexes | Relaxed TreeDB | Relaxed MongoDB (`w:1`) | Relaxed Ratio | Fast TreeDB | Fast MongoDB (`w:1`) | Fast Ratio | Durable TreeDB | Durable MongoDB (`w:1,j:true`) | Durable Ratio |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 306,188 | 478,052 | 0.64x | 381,860 | 478,069 | 0.80x | 196,739 | 341,305 | 0.58x |
| 1 | **262,226** | 208,584 | **1.26x** | **364,160** | 261,281 | **1.39x** | **228,306** | 222,780 | **1.02x** |
| 2 | **277,201** | 186,889 | **1.48x** | **337,527** | 218,585 | **1.54x** | 133,542 | 180,767 | 0.74x |
| 4 | **219,916** | 141,579 | **1.55x** | **287,320** | 163,755 | **1.75x** | 131,624 | 146,955 | 0.90x |
| 8 | **127,918** | 82,272 | **1.55x** | **159,022** | 95,481 | **1.67x** | **102,699** | 93,236 | **1.10x** |
| 16 | **83,738** | 45,998 | **1.82x** | **87,049** | 49,314 | **1.77x** | **61,140** | 50,265 | **1.22x** |
| 32 | 20,238 | 26,438 | 0.77x | 18,830 | 26,320 | 0.72x | 18,858 | 26,352 | 0.72x |

- In both `relaxed` and `fast`, TreeDB outperforms MongoDB across every indexed
  point from `1` through `16` secondary indexes (**`1.26x–1.82x`** in `relaxed`,
  **`1.39x–1.77x`** in `fast`), peaking at **`1.82x`** at `16` indexes (`83.7k`
  vs. `46.0k docs/s`).
- In `durable` (`command_wal_durable` vs. MongoDB `w:1,j:true`), TreeDB leads
  at `1`, `8`, and `16` indexes (**`1.02x–1.22x`**).
- **Load-only post-checkpoint physical disk (`idx=4..32`, 100k ~1.0 KB docs)**:
  TreeDB uses **`35.48–92.18 MiB`** (`relaxed`), **`35.48–86.86 MiB`** (`fast`),
  and **`62.98–89.79 MiB`** (`durable`) versus MongoDB's **`241.1–269.4 MiB`**
  (**`0.15x–0.34x`** MongoDB physical `du`).
- **Diagnostic notes**:
  - At `idx=0`, each `988 B` (~1.0 KB) document exceeds TreeDB's `512 B`
    per-value inline threshold (`page.DefaultInlineThreshold`, ~99 MB raw across
    100k docs), routing primary records to the value log during the settled
    `Checkpoint()` drain included in TreeDB's timed load window (whereas ~300 B
    docs stay inline and MongoDB `w:1` does not wait for a checkpoint). PR
    `#5939`'s batched multi-lane cold-scan value-log appends improved `idx=0`
    by **2.39x** (`128.1k -> 306.2k docs/s`).
  - At `idx=32`, 34 active domains (`primary` + `age_1` + 32 secondary indexes)
    cross `StandaloneCOWMemtableLimits().MaxTotalBytes` (`1 GiB`) mid-load and
    trigger warm `zipper` merges onto existing roots. Supporting 32+ indexes
    cleanly without `ErrCOWCapacity` was landed in PR `#5939`; avoiding warm
    mid-load merge amplification at 32+ indexes is a clear follow-up target.

### 2.2 Full-Sweep Concurrent Reads & Updates (`idx=0, 1, 2`, ~1.0 KB Docs)

| tier | indexes | `_id` r16 TreeDB / MongoDB | `email` r16 TreeDB / MongoDB | `age` range r16 TreeDB / MongoDB | `updateOne` w16 TreeDB / MongoDB | `updateOne` w32 TreeDB / MongoDB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Relaxed | 0 | **68,429** / 24,586 (**2.78x**) | — | 16,945 / 21,487 (0.79x) | **43,363** / 28,727 (**1.51x**) | **48,723** / 29,822 (**1.63x**) |
| Relaxed | 1 | **71,207** / 24,773 (**2.87x**) | **57,336** / 22,197 (**2.58x**) | **23,962** / 18,366 (**1.30x**) | **41,373** / 26,471 (**1.56x**) | **44,141** / 26,747 (**1.65x**) |
| Relaxed | 2 | **58,978** / 24,049 (**2.45x**) | **57,228** / 23,238 (**2.46x**) | **20,403** / 20,139 (**1.01x**) | **40,361** / 26,548 (**1.52x**) | **46,918** / 26,282 (**1.79x**) |
| Fast | 0 | **61,832** / 24,030 (**2.57x**) | — | 18,653 / 18,798 (0.99x) | **50,159** / 27,916 (**1.80x**) | **54,222** / 29,464 (**1.84x**) |
| Fast | 1 | **49,264** / 23,466 (**2.10x**) | **42,053** / 24,626 (**1.71x**) | **19,811** / 19,598 (**1.01x**) | **52,926** / 27,029 (**1.96x**) | **58,665** / 28,984 (**2.02x**) |
| Fast | 2 | **54,056** / 24,052 (**2.25x**) | **58,330** / 21,650 (**2.69x**) | **21,040** / 18,798 (**1.12x**) | **43,527** / 26,074 (**1.67x**) | **54,320** / 29,257 (**1.86x**) |
| Durable | 0 | **64,115** / 26,173 (**2.45x**) | — | **21,128** / 19,799 (**1.07x**) | **3,455** / 1,619 (**2.13x**) | — |
| Durable | 1 | **57,676** / 26,429 (**2.18x**) | **60,353** / 23,000 (**2.62x**) | **19,155** / 18,611 (**1.03x**) | **3,882** / 1,717 (**2.26x**) | — |
| Durable | 2 | **58,123** / 26,273 (**2.21x**) | **50,619** / 23,678 (**2.14x**) | **21,016** / 20,152 (**1.04x**) | **3,851** / 1,666 (**2.31x**) | — |

Note: `idx=0, 1, 2` post-sweep physical `du` in `relaxed`/`fast` (`367–372 MiB`
vs. MongoDB `239–243 MiB`) reflects `485,000` total ~1.0 KB updates (`6 × 80,000`
concurrent writer-sweep updates across `w1..w32` plus `5,000` serial updates)
written to the append-only value log prior to value-log GC/compaction; in
`durable` (`11,000` total updates: `5 × 2,000` concurrent across `w1..w16` plus
`1,000` serial), TreeDB physical `du` is `43.29–45.79 MiB` vs. MongoDB
`242.95–246.48 MiB` (**`0.18x–0.19x`**).

---

## 3. Mongo API Producer Load Scaling (`100k` Standard ~300 B Gateway Docs)

With PR `#5938`'s prebuilt `bson.Raw` command buffers removing client-side
saturation bottlenecks for both engines (`1,000`-doc batches, `1..32` concurrent
producers, standard ~300 B gateway doc shape), TreeDB dominates every single
producer-count and index-count cell across all three durability tiers:

| tier | secondary indexes | p=1 TreeDB / MongoDB | p=4 TreeDB / MongoDB | p=8 TreeDB / MongoDB | p=16 TreeDB / MongoDB | p=32 TreeDB / MongoDB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Relaxed | 0 | **555,283** / 299,429 (**1.85x**) | **739,940** / 283,012 (**2.61x**) | **739,133** / 176,306 (**4.19x**) | **706,196** / 123,062 (**5.74x**) | **705,841** / 118,697 (**5.95x**) |
| Relaxed | 1 | **402,670** / 145,772 (**2.76x**) | **619,432** / 306,352 (**2.02x**) | **631,502** / 399,771 (**1.58x**) | **613,273** / 371,230 (**1.65x**) | **601,733** / 314,208 (**1.92x**) |
| Relaxed | 2 | **344,633** / 121,714 (**2.83x**) | **494,902** / 240,839 (**2.05x**) | **560,147** / 307,755 (**1.82x**) | **515,764** / 283,472 (**1.82x**) | **526,493** / 266,541 (**1.98x**) |
| Fast | 0 | **665,561** / 309,064 (**2.15x**) | **916,227** / 302,130 (**3.03x**) | **986,663** / 185,251 (**5.33x**) | **861,636** / 124,327 (**6.93x**) | **819,043** / 118,627 (**6.90x**) |
| Fast | 1 | **504,712** / 148,267 (**3.40x**) | **698,030** / 313,845 (**2.22x**) | **800,571** / 401,920 (**1.99x**) | **758,900** / 336,133 (**2.26x**) | **745,694** / 304,374 (**2.45x**) |
| Fast | 2 | **393,568** / 114,002 (**3.45x**) | **637,924** / 234,568 (**2.72x**) | **659,702** / 302,557 (**2.18x**) | **666,278** / 267,997 (**2.49x**) | **678,473** / 270,173 (**2.51x**) |
| Durable | 0 | **172,568** / 74,570 (**2.31x**) | **291,262** / 137,781 (**2.11x**) | **365,784** / 116,572 (**3.14x**) | **346,450** / 83,836 (**4.13x**) | — |
| Durable | 1 | **149,162** / 62,777 (**2.38x**) | **317,878** / 169,097 (**1.88x**) | **385,042** / 239,049 (**1.61x**) | **354,247** / 321,349 (**1.10x**) | — |
| Durable | 2 | **129,417** / 58,061 (**2.23x**) | **274,592** / 149,218 (**1.84x**) | **351,774** / 203,045 (**1.73x**) | **276,304** / 228,021 (**1.21x**) | — |

TreeDB reaches **`986,663 docs/s`** (`no_wal_fast`), **`739,940 docs/s`**
(`command_wal_relaxed`), and **`385,042 docs/s`** (`command_wal_durable`) over
the Mongo wire protocol, leading MongoDB by **`1.46x–6.93x`** across all
relaxed and fast producer points (`p=1, 2, 4, 8, 16, 32`; `1.58x–6.93x` across
the tabulated columns above, with `relaxed idx=0 p=2` at `527,102 / 361,046 =
1.46x`) and **`1.10x–4.13x`** across durable (`p=1..16`).

---

## 4. Combined Template + Dictionary Compression Microbenchmark (`PR #5936`)

In `BenchmarkTemplateDictCombined` (`TreeDB/template/trainer_staging_test.go`,
package-level `template.NewEngine` + grouped `zstd.NewWriter` microbenchmark on
32 structured schemas, `383 B/doc` raw), running template prepass ahead of
grouped Zstd dictionary compression shrinks stored bytes by **22.3%** versus
dictionary-only compression and speeds up the grouped Zstd compression stage
itself (timed over pre-encoded template outputs, excluding `eng.Encode` prepass
time):

| mode | stored B/doc | compression ratio | Zstd stage speed vs. `DictOnly` |
| --- | ---: | ---: | ---: |
| `DictOnly` (grouped Zstd dict) | 38.45 | 9.96x | 1.00x |
| `TemplatePrepass + Dict` | **29.86** | **12.83x** | **1.55x faster** (Zstd pass only) |

## Caveats

- Collection insert numbers in this run use `-count 1` (`100,000` docs per
  configuration) across all three durability tiers and three index counts.
- Full-sweep `0..32` runs use `-doc-extra-index-fields 32` (`988 B` / ~1.0 KB
  per doc) so all index counts share one identical document shape; do not
  compare those raw docs/sec numbers directly against earlier ~300 B/doc
  full-sweep runs (use Section 3's producer-scaling sweep for ~300 B/doc
  apples-to-apples numbers).
- Other README benchmark sections (Qdrant 500K, VectorDBBench, YCSB, Collection
  Read/Lookup, Collection Concurrency, Typed Metadata Mutation, Vector Search
  Serving, Client-Shape Matrix, Matched-Recall Vector Server, and Indexed
  Text/Vector/Hybrid) were not re-run in this `deep_bench` pass and retain their
  existing linked sources.
