# TreeDB geth-hot-KV write profile — 2026-06-13

Issue: [#2706](https://github.com/snissn/gomap/issues/2706), parent tracker [#2676](https://github.com/snissn/gomap/issues/2676).

This is a profiling/counter snapshot only. It does **not** change command-WAL durability semantics, value-log compression defaults, or page/layout behavior, and it is not public-testnet readiness evidence.

## Context

- gomap capture head: `866d4deb8df2eb7cd9bfaa67ebb8495ad83bedb0`
  - Based on post-A `origin/main` `281c4eb28c3e1c0fae28e6e813b8b7a1402cdf9e`.
  - The only code change before capture was benchmark harness/reporting surfacing of additional existing `Stat()` deltas; TreeDB write behavior was not changed.
- go-ethereum checkout: `/Users/michaelseiler/dev/snissn/go-ethereum`
- go-ethereum head: `6371ea5ca7502c58ece5353b0425fb5ec99a41d8`
- Platform: Apple M3, 16 GiB RAM, Darwin arm64, Go `go1.26.0`
- Dataset shape: `KEYS=1000000`, `READS=400000`, `KEY_SHAPES=geth-mixed`, `VALUE_SHAPES=geth-mixed`, `VALUE_SIZES=128`, `BATCH_TARGET_BYTES=102400`, `TREEDB_READ_INTEGRITIES=verify`, `ITERATION_MODES=value`, `ENGINES=treedb`.
- go command used a temporary `-modfile` replacing both gomap modules to this worktree:
  - `replace github.com/snissn/gomap => /Users/michaelseiler/orca/workspaces/gomap/2706-manager`
  - `replace github.com/snissn/gomap/TreeDB/integration/gethethdb => /Users/michaelseiler/orca/workspaces/gomap/2706-manager/TreeDB/integration/gethethdb`

## Commands

Profile capture:

```sh
GOFLAGS='-modfile=/tmp/geth_hotkv_gomap_2706_20260613T075042Z.mod -mod=mod' \
GETH_REPO=/Users/michaelseiler/dev/snissn/go-ethereum \
RUN_DIR=/tmp/geth_hotkv_write_matrix_20260613T075042Z \
PROFILE_DIR=/tmp/geth_hotkv_profiles_write_20260613T075042Z \
ENGINES=treedb \
KEYS=1000000 READS=400000 \
KEY_SHAPES=geth-mixed VALUE_SHAPES=geth-mixed VALUE_SIZES=128 \
BATCH_TARGET_BYTES=102400 \
TREEDB_READ_INTEGRITIES=verify \
ITERATION_MODES=value \
  scripts/treedb_geth_hot_kv_matrix.sh
```

Same-head no-pprof control:

```sh
GOFLAGS='-modfile=/tmp/geth_hotkv_gomap_2706_20260613T075119Z.mod -mod=mod' \
GETH_REPO=/Users/michaelseiler/dev/snissn/go-ethereum \
RUN_DIR=/tmp/geth_hotkv_write_noprofile_20260613T075119Z \
ENGINES=treedb \
KEYS=1000000 READS=400000 \
KEY_SHAPES=geth-mixed VALUE_SHAPES=geth-mixed VALUE_SIZES=128 \
BATCH_TARGET_BYTES=102400 \
TREEDB_READ_INTEGRITIES=verify \
ITERATION_MODES=value \
  scripts/treedb_geth_hot_kv_matrix.sh
```

Derived summaries:

```sh
P=/tmp/geth_hotkv_profiles_write_20260613T075042Z/01_geth-mixed_geth-mixed_v128_b102400_verify_value

go tool pprof -top -nodecount=40 "$P/cpu_write_treedb.pprof" > "$P/cpu_write_top.txt"
go tool pprof -top -cum -nodecount=60 "$P/cpu_write_treedb.pprof" > "$P/cpu_write_top_cum.txt"
go tool pprof -top -alloc_space -nodecount=40 "$P/allocs_cumulative_write_treedb.pprof" > "$P/allocs_cumulative_write_top_alloc_space.txt"
go tool pprof -top -alloc_objects -nodecount=40 "$P/allocs_cumulative_write_treedb.pprof" > "$P/allocs_cumulative_write_top_alloc_objects.txt"
go tool pprof -top -nodecount=40 "$P/mutex.pprof" > "$P/mutex_top.txt"
```

## Artifacts

- Profile matrix: `/tmp/geth_hotkv_write_matrix_20260613T075042Z`
  - `matrix_results.md`, `matrix_results.tsv`
  - `phase_counters.tsv`
  - `phase_stat_deltas.tsv`
  - `manifest.txt`
- Pprof root: `/tmp/geth_hotkv_profiles_write_20260613T075042Z/01_geth-mixed_geth-mixed_v128_b102400_verify_value`
  - `cpu_write_treedb.pprof`
  - `cpu_write_top.txt`, `cpu_write_top_cum.txt`
  - `allocs_cumulative_write_treedb.pprof`
  - `memstats_write_treedb.json`
  - `block.pprof`, `mutex.pprof`, `mutex_top.txt`
- No-pprof control: `/tmp/geth_hotkv_write_noprofile_20260613T075119Z`

## Throughput snapshot

The profiled run is the CPU evidence source; the no-pprof control is included to keep throughput separate from profiling overhead/noise. Single-run numbers moved materially across local captures, so use this for bottleneck ranking rather than throughput regression claims.

| run | write ops/sec | read ops/sec | iterate keys/sec | DeleteRange keys/sec | size bytes | post-delete bytes |
|---|---:|---:|---:|---:|---:|---:|
| profiled | 181,198 | 125,675 | 2,613,264 | 1,022,704 | 771,304,381 | 393,938,076 |
| same-head no-pprof control | 174,126 | 61,360 | 1,183,660 | 408,225 | 771,304,381 | 393,928,903 |
| pre-existing post-A reference from `/tmp/geth_hotkv_postmerge_1m_20260613T063345Z` | 262,270 | 84,061 | 1,033,019 | 482,376 | 771,304,381 | 393,939,065 |

## `cpu_write_treedb.pprof` highlights

CPU profile metadata: 5.70s duration, 3.15s total samples.

Flat CPU was dominated by memory movement, GC scanning, and syscalls:

| function | flat | flat % | cumulative |
|---|---:|---:|---:|
| `runtime.memmove` | 0.63s | 20.00% | 0.63s |
| `runtime.madvise` | 0.33s | 10.48% | 0.33s |
| `runtime.tryDeferToSpanScan` | 0.30s | 9.52% | 0.30s |
| `syscall.rawsyscalln` | 0.30s | 9.52% | 0.30s |
| `runtime.scanObject` | 0.27s | 8.57% | 0.62s |
| `syscall.Syscall` | 0.16s | 5.08% | 0.16s |
| `github.com/snissn/go-crc32-asm.gf2MatrixTimes` | 0.12s | 3.81% | 0.14s |
| `github.com/snissn/gomap/TreeDB/internal/memtable.(*AppendOnly).appendEntryCoreLocked` | 0.07s | 2.22% | 0.10s |

Cumulative TreeDB/application hotspots:

| function group | cumulative | cumulative % | interpretation |
|---|---:|---:|---|
| `caching.(*DB).flushLaneOnceWithCommandPublish` | 0.82s | 26.03% | cached flush/publish dominates application CPU |
| `zipper.(*Zipper).Apply` / `writeRecursive` | 0.62s | 19.68% | page merge/rewrite path visible |
| `zipper.(*Zipper).mergeLeaf` | 0.60s | 19.05% | leaf merge/persist work visible |
| `zipper.(*Zipper).mergeInternal` | 0.56s | 17.78% | internal merge work visible |
| `gethethdb.(*Batch).Put` / `cloneBytes` | 0.55s / 0.54s | 17.46% / 17.14% | adapter ownership copies remain material |
| `commandWALPublicBatch.write` / `caching.(*Batch).writeRegularLocked` | 0.54s | 17.14% | public batch write path |
| `caching.(*DB).appendValueLog` | 0.52s | 16.51% | value-log append path visible but not alone dominant |
| `leafPageLogWithRecordLengthHints.AppendLeafPages` | 0.47s | 14.92% | leaf-log page persistence visible |
| `valuelog.(*Writer).AppendFrameWithStatsInto` / `appendBlockFrameWithStats` | 0.29s | 9.21% | value-log frame encoding/writes |
| `commandWALPublicBatch.appendCommandWAL` / commitlog append | 0.20s | 6.35% | command-WAL append is visible but below allocation/zipper groups |
| `commitlog.writevFull` | 0.19s | 6.03% | command-WAL writev syscall path |

The mutex profile also points at the zipper leaf-page persistence/cache path (`zipper.(*Zipper).cachePersistedLeafPage`, `mergeLeaf`, and `mergeInternal`) with about 5.4s cumulative mutex delay. The block profile is dominated by idle/background goroutines and was not used for ranking.

## Phase-local memstats

Use these JSON files for phase-local allocation deltas. The allocation pprof files are cumulative process profiles and include setup/data-generation cost.

| phase | total alloc bytes | MiB | mallocs | frees | GC cycles | pause ns |
|---|---:|---:|---:|---:|---:|---:|
| write | 1,372,413,840 | 1,308.8 | 3,176,690 | 2,196,191 | 2 | 4,782,040 |
| read | 146,528,288 | 139.7 | 452,861 | 194 | 0 | 0 |
| iterate | 6,090,184 | 5.8 | 50,823 | 75 | 0 | 0 |
| DeleteRange | 918,607,248 | 876.1 | 2,263,582 | 1,195,865 | 1 | 116,333 |
| reopen verify | 4,285,405,928 | 4,086.9 | 1,017,697 | 914,188 | 7 | 818,000 |

The write phase remains allocation-heavy: about 1.37 GB and 3.18M mallocs for 1M writes. This matches the earlier #2676 note that write allocation pressure was still about 1.37 GiB / 3.18M mallocs after A-lane.

## Relevant counters

New `phase_stat_deltas.tsv` rows expose nonzero parseable TreeDB `Stat()` deltas for existing write-path counters.

| counter | write delta | note |
|---|---:|---|
| `treedb.command_wal.frames` | 3,629 | command-WAL frames accepted during write |
| `treedb.command_wal.live_accepted_frames` | 3,629 | live accepted frame count matches frame delta |
| `treedb.command_wal.live_covered_frames` | 3,629 | checkpoint covered the accepted write frames |
| `treedb.cache.checkpoint.runs` | 1 | final `SyncKeyValue`/checkpoint boundary |
| `treedb.cache.command_wal.checkpoint_publish.piggybacked` | 1 | command-WAL checkpoint publish piggybacked |
| `treedb.cache.vlog_write_mode.frames.off` | 1,821 | value-log auto chose `off` for these payload frames |
| `treedb.cache.vlog_write_mode.raw_bytes.off` | 179,200,000 | value-log raw bytes in off mode |
| `treedb.cache.vlog_write_mode.stored_bytes.off` | 179,200,000 | no compression savings for this write-mode bucket |
| `treedb.cache.vlog_payload_split.records.single_value` | 200,000 | single-value records stored through value log |
| `treedb.cache.vlog_shape.segments_total` | 3 | value-log segments created/active for this run |
| `treedb.cache.vlog_shape.bytes_total` | 181,665,556 | value-log bytes after write |
| `treedb.cache.memtable_stats.writes` | 104,468 | current exposed memtable write counter is not a full 1M op counter |
| `treedb.cache.memtable_stats.seq_writes` | 102,007 | current exposed memtable seq counter is partial/current-state oriented |
| `treedb.freelist.alloc_pages_total` | 782 | pages allocated during write |
| `treedb.freelist.append_alloc_pages_total` | 752 | mostly append allocation |
| `treedb.freelist.reuse_alloc_pages_total` | 30 | low page reuse during load |
| `treedb.freelist.free_pages_total` | 33 | low free count during load |
| `treedb.pages.total` | 752 | page count growth during write |

DeleteRange is owned by #2704, but for context this same run reported `treedb.command_wal.frames=5,050`, `total_alloc_bytes=918,607,248`, and `mallocs=2,263,582` in the DeleteRange phase.

Current command-WAL byte counters are not useful as phase deltas here (`treedb.command_wal.bytes=0`) because the stats scan observes the post-checkpoint/truncated command-WAL file set. Live frame counters are useful; byte/sync/flush counters would need dedicated live counters before changing durability behavior.

## Ranked write-path bottlenecks

1. **Allocation/copy/GC pressure is the top proven write bottleneck.**
   - Evidence: write phase allocated 1.37 GB / 3.18M mallocs; flat CPU is led by `runtime.memmove` plus GC scanning/span work; cumulative allocation profile still shows `gethethdb.cloneBytes`, memtable entry materialization, batch reserve/entry slices, and leaf-page/cache scratch allocations.
   - Next issue should target safe reductions in copies/allocations while preserving geth `ethdb.Batch` ownership/replay semantics.

2. **Zipper/leaf-page persist and leaf-log rewrite work is the next structural candidate, but needs raw-KV counters before layout changes.**
   - Evidence: `zipper.Apply`/`mergeLeaf`/`mergeInternal` are about 18–20% cumulative CPU, leaf-page log append is about 15% cumulative CPU, and mutex profile points at `zipper.cachePersistedLeafPage`/`persistLeafPageData`.
   - Existing ordered-root delta counters stay zero for this raw-KV path, so the next implementation issue should add raw-KV zipper counters first: leaf/internal merges, pages written, leaf-log bytes written, node loads, and cache lock hold/wait.

3. **Command-WAL append is visible but not the first optimization target from this run.**
   - Evidence: 3,629 write frames for 1M puts; command-WAL append/writev is about 6% cumulative CPU in the write profile. Syscall CPU is visible, but byte/sync/flush live counters are not yet sufficient.
   - Do not change durability semantics from this evidence. If command-WAL work is revisited, add live bytes/sync/flush counters and acceptance gates first.

4. **Value-log payload compression/defaults should not be changed from this run.**
   - Evidence: value-log auto selected `off` for the hot-KV value payload frames (`1,821` frames, `179.2 MB` raw/stored), and value-log append/frame work is below allocation/copy and zipper groups.
   - Leaf-log block compaction/compression is visible but not dominant enough to justify changing defaults without a targeted counter/profiling issue.

## Recommendation

Created child implementation issue [#2709](https://github.com/snissn/gomap/issues/2709) for **TreeDB geth-hot-KV write allocation/copy pressure**, with acceptance gates based on this run:

- keep command-WAL durable semantics and read-integrity defaults unchanged;
- preserve geth `ethdb.Batch` ownership, replay, repeated `Write`, `Reset`, and closed-batch behavior;
- reduce write-phase `memstats_write_treedb.json` totals from the 1.37 GB / 3.18M malloc baseline;
- reduce `cpu_write_treedb.pprof` flat/cumulative `runtime.memmove`, GC scan, `gethethdb.cloneBytes`, memtable clone/materialization, and batch entry allocation shares;
- rerun the same 1M/400k command and include `cpu_write_top`, phase-local memstats, and `phase_stat_deltas.tsv` counters.

A follow-up zipper issue should be counter-first unless allocation work is already exhausted: add raw-KV leaf/internal merge/page/byte/lock counters, then use those counters to choose a specific zipper/leaf-log optimization.
