# TreeDB Deep Benchmark Refresh (2026-11-27, Post-PR-4 Flat Single-Directory Layout)

This report records the three-tier deep benchmark refresh (`relaxed`, `fast`,
and `durable`) run on `192.168.0.185` (`mikers-B560-DS3H-AC-Y1`) at `main`
commit `f4f5f5d95a958886de22336a5b7ec0fdcfb0f095` (`f4f5f5d95`), immediately
following the storage streamlining rearchitecture:

- **PR 1**: Retuned compression defaults & trainer efficiency.
- **PR 2a (`#5918`)**: Hard-grafted `dictdb` into `index.db` `SystemRootPageID`
  (inline/chunked or optional raw lane-254 `ValuePtr`).
- **PR 2b (`#5925`)**: Zero-fsync piggybacked dictionary staging
  (`StageSystemOps`) and removal of checkpoint dict deferral.
- **PR 3 (`#5928`)**: Hard-grafted `templatedb` into `index.db`
  `SystemRootPageID`.
- **PR 4 (`#5930`)**: Collapsed `<root>/maindb/` into a pure flat
  single-directory layout and removed remaining legacy side-store plumbing.

## Delta Summary vs. November 26 Refresh (`e78d8b40a` -> `f4f5f5d95`, Relaxed Tier)

| Workload / Metric | Nov 26 (`e78d8b40a`) | Nov 27 (`f4f5f5d95`) | Delta |
| --- | ---: | ---: | ---: |
| 2-index bulk insert (`template-v1` / `BSON` / `JSON`) | 657.0k / 612.4k / 506.3k docs/s | **909.1k / 882.6k / 808.4k docs/s** | **+38.4% / +44.1% / +59.7%** |
| 2-index compacted B/doc (`template-v1` exhaustive / `BSON` / `JSON`) | 26.5 / 28.0 / 36.3 B/doc | **23.0 / 25.3 / 34.4 B/doc** | **-13.2% / -9.6% / -5.2%** |
| Mongo full-sweep `load_insert_many` (`100k` BSON, idx=0/1/2) | 292.7k / 276.4k / 239.6k docs/s | **516.9k / 468.7k / 428.5k docs/s** | **+76.6% / +69.6% / +78.9%** |
| Mongo full-sweep `updateOne w16` (`100k` BSON, idx=0/1/2) | 52.3k–53.8k ops/s | **59.6k–61.9k ops/s** | **+14%–15%** (`1.90x–1.98x` MongoDB) |
| Mongo full-sweep `updateOne w32` (`100k` BSON, idx=0/1/2) | 70.5k–72.2k ops/s | **80.6k–81.7k ops/s** | **+13%–14%** (`2.39x–2.48x` MongoDB) |
| Mongo full-sweep post-sweep `checkpoint` physical `du` (idx=0/1/2) | 18.97 / 101.80 / 102.19 MiB | **13.82 / 18.66 / 15.08 MiB** | **-27% / -82% / -85%** (`0.06x–0.09x` MongoDB) |

## Environment & Reproduction

- **Measured Commit**: `f4f5f5d95a958886de22336a5b7ec0fdcfb0f095`
- **Host**: `mikers-B560-DS3H-AC-Y1`, Linux `6.8.0-138-generic` `x86_64`
- **CPU**: 11th Gen Intel Core i5-11400F @ 2.60 GHz (6 cores / 12 threads)
- **External Comparators**: Docker `mongo:8`, SQLite via Go CGO (`sqlite_bench`)
- **Execution Discipline**: Three sequential runs on a quiet host with
  disposable DB directories and isolated Docker MongoDB containers:
  - Relaxed (`/tmp/gomap_deep_bench_s21_relaxed`):
    `./scripts/treedb_benchmark_run_report.sh --out-dir /tmp/gomap_deep_bench_s21_relaxed --tier pr --durability-tier relaxed --indexes "0 1 2" --collection-docs 100000 --collection-profile-benchtime 100000x --collection-profile-count 1 --mongo-docs 100000 --mongo-load-scaling-docs 100000 --mongo-treedb-open-mode standalone --skip-raw --skip-load-modes`
  - Fast (`/tmp/gomap_deep_bench_s21_fast`): same flags with `--durability-tier fast`
  - Durable (`/tmp/gomap_deep_bench_s21_durable`): same flags with `--durability-tier durable`

---

## 1. Indexed Collection Insert Workload (`100k` Docs, Batch Size `16,000`)

All TreeDB rows use data and index outer leaves in value log (`-count 1`,
`100,000` documents, batch size `16,000`). Compacted B/doc uses
`exhaustive_compact` for TreeDB `template-v1` (also noting `full_leafgen_pack_gc`
and `offline_compact`), `full_leafgen_pack_gc` for TreeDB BSON and JSON, and
SQLite after `VACUUM`.

### 1.1 Relaxed Tier (`command_wal_relaxed` vs. SQLite `WAL + NORMAL`, `2` Secondary Indexes)

| engine / format | layout | docs/sec | compacted B/doc | compaction method |
| --- | --- | ---: | ---: | --- |
| TreeDB template-v1 | data and index outer leaves in value log | 909,091 | 23.0 | `exhaustive_compact` (`24.2` `full_leafgen_pack_gc`; `65.3` `offline_compact`) |
| TreeDB BSON | data and index outer leaves in value log | 882,613 | 25.3 | `full_leafgen_pack_gc` |
| TreeDB JSON | data and index outer leaves in value log | 808,407 | 34.4 | `full_leafgen_pack_gc` |
| SQLite native columns | WAL normal | 362,319 | 156.7 | `VACUUM` |
| SQLite JSON | WAL normal | 326,904 | 231.7 | `VACUUM` |

TreeDB `template-v1` achieves **2.51x** SQLite native insert throughput (**2.78x**
SQLite JSON) at **6.8x** smaller compacted size than SQLite native (**10.1x**
smaller than SQLite JSON).

### 1.2 Cross-Tier Collection Insert Matrix (`0`, `1`, and `2` Secondary Indexes, docs/sec)

| tier (TreeDB / SQLite) | indexes | TreeDB template-v1 | TreeDB BSON | TreeDB JSON | SQLite native | SQLite JSON |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Relaxed (`command_wal_relaxed` / `WAL NORMAL`) | 0 | 1,348,072 | 1,251,721 | 1,734,906 | 551,876 | 775,795 |
| Relaxed (`command_wal_relaxed` / `WAL NORMAL`) | 1 | 688,231 | 581,395 | 965,251 | 451,671 | 434,783 |
| Relaxed (`command_wal_relaxed` / `WAL NORMAL`) | 2 | 909,091 | 882,613 | 808,407 | 362,319 | 326,904 |
| Fast (`no_wal_fast` / `WAL OFF`) | 0 | 2,400,384 | 2,059,308 | 2,822,467 | 627,353 | 767,460 |
| Fast (`no_wal_fast` / `WAL OFF`) | 1 | 1,284,852 | 1,237,471 | 1,123,217 | 486,618 | 426,076 |
| Fast (`no_wal_fast` / `WAL OFF`) | 2 | 1,093,374 | 1,041,558 | 879,507 | 351,124 | 268,097 |
| Durable (`command_wal_durable` / `WAL FULL`) | 0 | 743,494 | 880,282 | 762,195 | 402,739 | 476,644 |
| Durable (`command_wal_durable` / `WAL FULL`) | 1 | 526,593 | 479,616 | 484,966 | 375,657 | 321,130 |
| Durable (`command_wal_durable` / `WAL FULL`) | 2 | 539,084 | 535,045 | 452,899 | 270,856 | 259,875 |

Compacted 2-index storage in `no_wal_fast` and `command_wal_durable` matches the
relaxed tier (`23.0` B/doc `template-v1` exhaustive, `22.9` B/doc `no_wal_fast`
offline compact, `25.3` B/doc BSON, `34.4` B/doc JSON vs. `156.7` B/doc SQLite
native and `231.7` B/doc SQLite JSON).

---

## 2. Mongo API Full-Sweep Workload (`100k` BSON Docs, Standalone COW)

All runs use TreeDB Mongo gateway `driver-command-raw` in standalone COW mode
against Docker `mongo:8`, including the `age_1` range index across all
secondary-index counts (`0`, `1`, `2`). Physical MiB is the post-sweep
`checkpoint` physical `du` (prior to offline compaction).

### 2.1 Relaxed Tier (`command_wal_relaxed` vs. MongoDB `w:1`)

| secondary indexes | TreeDB load docs/sec | MongoDB load docs/sec | TreeDB `_id` r16 ops/sec | MongoDB `_id` r16 ops/sec | TreeDB `age` range r16 ops/sec | MongoDB `age` range r16 ops/sec | TreeDB physical MiB | MongoDB physical MiB |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 516,948 | 415,137 | 113,293 | 35,142 | 105,441 | 24,541 | 13.82 | 213.35 |
| 1 | 468,687 | 242,186 | 113,150 | 33,503 | 107,480 | 23,058 | 18.66 | 217.02 |
| 2 | 428,523 | 198,786 | 109,432 | 32,110 | 106,588 | 24,603 | 15.08 | 217.60 |

- **Zero phases in the entire relaxed full-sweep matrix had MongoDB ahead on
  ops/sec** (`1.25x–2.16x` on load, `3.22x–3.41x` on `_id r16`, `4.30x–4.66x`
  on indexed `age` range `r16`, `3.10x–3.74x` on indexed `email r16`, and
  `0.06x–0.09x` MongoDB physical disk).
- Concurrent `_id` `updateOne` (`concurrent_id_update_set_w*` in full sweep):
  - `w16`: `61,948` / `61,881` / `59,621 ops/sec` (`1.90x–1.98x` MongoDB `31.3k–32.0k`)
  - `w32`: `81,717` / `80,596` / `81,085 ops/sec` (`2.39x–2.48x` MongoDB `32.8k–33.9k`)

### 2.2 Fast Tier (`no_wal_fast` vs. MongoDB `w:1`)

| secondary indexes | TreeDB load docs/sec | MongoDB load docs/sec | TreeDB `_id` r16 ops/sec | MongoDB `_id` r16 ops/sec | TreeDB `age` range r16 ops/sec | MongoDB `age` range r16 ops/sec | TreeDB `updateOne` w32 ops/sec | MongoDB `updateOne` w32 ops/sec | TreeDB physical MiB | MongoDB physical MiB |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 642,037 | 432,561 | 92,873 | 34,574 | 77,858 | 25,147 | 93,268 | 32,863 | 11.48 | 213.39 |
| 1 | 459,687 | 217,813 | 104,079 | 34,855 | 108,192 | 25,149 | 94,306 | 33,764 | 11.90 | 217.03 |
| 2 | 490,079 | 208,586 | 118,764 | 33,092 | 114,744 | 25,758 | 97,410 | 35,406 | 12.11 | 217.57 |

### 2.3 Durable Tier (`command_wal_durable` vs. MongoDB `w:1,j:true`)

| secondary indexes | TreeDB load docs/sec | MongoDB load docs/sec | TreeDB `_id` r16 ops/sec | MongoDB `_id` r16 ops/sec | TreeDB `age` range r16 ops/sec | MongoDB `age` range r16 ops/sec | TreeDB `updateOne` w16 ops/sec | MongoDB `updateOne` w16 ops/sec | TreeDB physical MiB | MongoDB physical MiB |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 460,578 | 377,461 | 115,885 | 33,586 | 110,355 | 21,997 | 4,139 | 1,730 | 8.34 | 211.65 |
| 1 | 335,385 | 239,635 | 115,366 | 37,399 | 113,979 | 22,878 | 3,968 | 1,770 | 9.08 | 214.17 |
| 2 | 340,834 | 196,408 | 113,585 | 33,052 | 100,718 | 22,012 | 4,086 | 1,660 | 10.85 | 214.66 |

In durable mode, TreeDB leads across every phase (`1.22x–1.74x` load,
`1.00x–1.18x` serial durable `updateOne`, `2.24x–2.46x` concurrent durable
`updateOne w16`, `3.08x–3.45x` `_id r16`, `4.58x–5.02x` `age` range `r16`) at
**0.04x–0.05x** MongoDB physical disk (`8.34–10.85 MiB` vs. `211.65–214.66 MiB`).

## Caveats

- Collection insert numbers in this run use `-count 1` (`100,000` docs per
  configuration) across all three durability tiers and three index counts.
- Other README benchmark sections (Qdrant 500K, VectorDBBench, YCSB, Collection
  Read/Lookup, Collection Concurrency, Typed Metadata Mutation, Vector Search
  Serving, Client-Shape Matrix, Matched-Recall Vector Server, and Indexed
  Text/Vector/Hybrid) were not re-run in this `deep_bench` pass and retain their
  existing linked sources.
