# TreeDB random-write ceiling breaker M14 final gate

Issue: #2774 (M14). Parent tracker: #2743. Runtime SHA:
`e0a71320db7a727b451ebdd4b8254853cfc072a2` (M13 / PR #2780 merged).

## Decision

**Result: the span-run/span-native stack materially reduced the measured
random-write bottleneck when explicitly enabled, but it is not ready to become
the default configuration.** Keep `FlushApplySpanNative` and
`FlushBacklogCoalescing` opt-in/default-off for now.

Why the ceiling-breaker is real:

- On the same host/SHA, `span_native_c4` improved `random_write` by **+94.11%**
  versus current unconfigured defaults (`145,427 -> 282,282 ops/s`) while also
  reducing old-leaf decode bytes/op (`1603 -> 1395`), leaf merges/op
  (`0.355 -> 0.318`), and append frames/op (`0.355 -> 0.318`).
- Higher concurrency continued to move the write ceiling: `span_native_c16`
  reached **312,391 random_write ops/s** and **766,645 batch_random ops/s**.
- The intended path ran: c2/c4/c8/c16 rows used span-native for ~29.2M-29.7M
  ops. Non-zero fallback reasons were only the expected M12
  `close_or_checkpoint` drain fallbacks; root mismatch, output ownership, and
  reducer validation fallbacks stayed zero.
- Writer-side waiting improved on the useful rows: foreground-assist wait fell
  from **77.7s** on defaults to **32.5s** at c4 and **26.9s** at c16; stall
  waits were zero in all rows.

Why default-on is still blocked:

- `span_native_c1` is a clear unsafe default shape: `random_write` regressed
  **-20.19%**, `batch_random` regressed versus defaults, and foreground-assist
  wait increased to **106.0s**.
- Checkpoint-inclusive latency is not uniformly better. Useful span-native rows
  improved throughput, but post-run checkpoint time was generally higher than
  the default row (`6.49s` default vs `8.17s-9.45s` for c4/c8/c16/cache-off;
  `11.50s` for c4 without backlog). Random-write checkpoint time also rose
  (`2.70s` default vs `7.01s-9.22s` useful span-native rows).
- c8/c16 provide the highest throughput but increase CPU samples and
  contention-profile totals compared with c4. They are throughput-ceiling rows,
  not conservative production defaults.
- The cache-disabled row was strong for this write-only matrix, but it is not a
  default recommendation because read/scan guardrails were not part of M14.

Recommended rollout posture:

- **Default:** unchanged/off.
- **Write-heavy experimental opt-in:** use span-native plus backlog coalescing
  with `FlushApplyConcurrency >= 4` after workload-specific checkpoint and CPU
  profiling. c4 is the conservative balanced row; c8/c16 are throughput-ceiling
  rows with more contention/CPU evidence to watch.
- **Rollback knobs:** omit/disable `-treedb-flush-apply-span-native`, omit/disable
  `-treedb-flush-backlog-coalescing`, lower `-treedb-flush-apply-concurrency`,
  or return to the unconfigured defaults. The outer-leaf read cache can be
  disabled for write-only experiments (`-treedb-leaf-page-read-cache-entries=-1`)
  but should not be changed as a read-path default from this matrix alone.

## Evidence identity

Remote host and artifact root:

- host: redacted remote 4TB benchmark host (`mikers-B560-DS3H-AC-Y1`)
- kernel: Linux 6.8.0-124-generic x86_64
- Go: `go1.25.0 linux/amd64`
- CPU workers visible: `nproc=12`
- artifact root: `/mnt/fast4tb/gomap-profiles/2774_m14_matrix_20260616_132256`
- generated summaries:
  - `/mnt/fast4tb/gomap-profiles/2774_m14_matrix_20260616_132256/m14_matrix_summary.md`
  - `/mnt/fast4tb/gomap-profiles/2774_m14_matrix_20260616_132256/m14_matrix_summary.json`

Primary command shape for each 10M row:

```sh
./bin/unified-bench \
  -dbs treedb \
  -test sequential_write,batch_random,random_write \
  -keys 10000000 \
  -valsize 128 \
  -batchsize 8000 \
  -profile-dir "$OUT" \
  -path-label m8-m14-10mm-gate \
  -treedb-journal-lanes=1 \
  -checkpoint-between-tests \
  -progress=false
./bin/benchprof -profiles-dir "$OUT"
```

`FlushApplyConcurrency=1,2,4,8,16` rows also used:

```sh
-treedb-flush-apply-min-entries=1 \
-treedb-flush-apply-min-spans=1 \
-treedb-flush-apply-min-bytes=1 \
-treedb-flush-apply-span-native \
-treedb-flush-backlog-coalescing
```

Cache-axis note: TreeDB currently has a fixed/default-env outer-leaf read cache
(`outer_leaf_read_cache_entries=default/env`, effective 32768 in the run) and a
cache-disabled mode. There is not a separate adaptive outer-leaf read-cache mode;
the adaptive row in this gate is the M11 backlog-coalescing controller.

## Throughput and checkpoint summary

| Row | Concurrency | Span-native | Backlog | Cache | Seq ops/s | Batch ops/s | Random ops/s | Δ random vs default | Checkpoint batch | Checkpoint random | Post-run checkpoint |
|---|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|
| `default_unconfigured` | default | false | false | default | 2,560,082 | 322,644 | 145,427 | +0.00% | 1.16s | 2.70s | 6.49s |
| `legacy_parallel_c4` | 4 | false | false | default | 2,732,468 | 613,337 | 226,416 | +55.69% | 814.25ms | 10.51s | 7.58s |
| `span_native_c1` | 1 | true | true | default | 2,685,554 | 247,190 | 116,067 | -20.19% | 1.15s | 3.50s | 8.95s |
| `span_native_c2` | 2 | true | true | default | 2,729,639 | 609,965 | 215,099 | +47.91% | 815.96ms | 12.56s | 5.78s |
| `span_native_c4` | 4 | true | true | default | 2,710,831 | 696,101 | 282,282 | +94.11% | 824.29ms | 8.20s | 9.22s |
| `span_native_c8` | 8 | true | true | default | 2,705,301 | 704,500 | 309,287 | +112.68% | 826.92ms | 7.21s | 9.10s |
| `span_native_c16` | 16 | true | true | default | 2,725,757 | 766,645 | 312,391 | +114.81% | 833.62ms | 7.01s | 8.17s |
| `span_native_c4_no_backlog` | 4 | true | false | default | 2,728,485 | 680,940 | 276,950 | +90.44% | 831.71ms | 8.11s | 11.50s |
| `span_native_c4_cache_disabled` | 4 | true | true | disabled | 2,747,611 | 700,046 | 313,033 | +115.25% | 818.65ms | 7.38s | 9.45s |

## Bottleneck proof counters

| Row | old leaf B/op | leaf merges/op | append frames/op | apply ops/span | single-op span ratio | target split leaves | span-native used ops | fallbacks | close/chkp fallback ops | root reduce ns | publish ns |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `default_unconfigured` | 1,603 | 0.355 | 0.355 |  |  | 0 | 0 | 0 | 0 | 356,981 | 18,513,955,885 |
| `legacy_parallel_c4` | 1,453 | 0.330 | 0.330 | 3.033 | 0.633 | 0 | 0 | 734 | 0 | 346,427 | 15,514,455,153 |
| `span_native_c1` | 1,602 | 0.355 | 0.355 | 2.821 | 0.682 | 0 | 29,708,113 | 12 | 218,612 | 6,372,102,050 | 21,811,431,150 |
| `span_native_c2` | 1,378 | 0.316 | 0.316 | 3.167 | 0.610 | 0 | 29,688,490 | 10 | 218,611 | 4,081,879,756 | 11,235,511,751 |
| `span_native_c4` | 1,395 | 0.318 | 0.318 | 3.146 | 0.619 | 0 | 29,688,511 | 10 | 218,612 | 4,363,775,346 | 14,362,201,706 |
| `span_native_c8` | 1,406 | 0.320 | 0.321 | 3.123 | 0.619 | 0 | 29,196,607 | 19 | 710,980 | 4,319,757,006 | 15,358,903,079 |
| `span_native_c16` | 1,407 | 0.320 | 0.321 | 3.121 | 0.619 | 0 | 29,196,574 | 20 | 711,046 | 4,320,975,055 | 14,126,573,975 |
| `span_native_c4_no_backlog` | 1,472 | 0.332 | 0.332 | 3.014 | 0.644 | 0 | 29,698,193 | 10 | 218,612 | 5,093,285,131 | 15,133,140,814 |
| `span_native_c4_cache_disabled` | 1,368 | 0.313 | 0.314 | 3.192 | 0.607 | 0 | 29,686,165 | 10 | 218,612 | 4,089,201,403 | 13,555,548,916 |

Fallback reason detail:

| Row | Non-zero fallback reasons |
|---|---|
| `default_unconfigured` | none |
| `legacy_parallel_c4` | `span_native_not_implemented`: 29,914,354 ops / 9,863,096 spans |
| `span_native_c1` | `close_or_checkpoint`: 218,612 ops / 198,405 spans |
| `span_native_c2` | `close_or_checkpoint`: 218,611 ops / 198,080 spans |
| `span_native_c4` | `close_or_checkpoint`: 218,612 ops / 198,048 spans |
| `span_native_c8` | `close_or_checkpoint`: 710,980 ops / 576,298 spans |
| `span_native_c16` | `close_or_checkpoint`: 711,046 ops / 584,373 spans |
| `span_native_c4_no_backlog` | `close_or_checkpoint`: 218,612 ops / 198,061 spans |
| `span_native_c4_cache_disabled` | `close_or_checkpoint`: 218,612 ops / 198,032 spans |

## Writer stall / foreground assist / backlog counters

| Row | fg assist calls | fg assist flushes | fg assist wait ns | active assist skips | progress wait ns | stall waits | coordinator blocking fallbacks | hard-overload fallbacks | queue backlog bytes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `default_unconfigured` | 13 | 3 | 77,693,578,529 | 0 | 0 | 0 | 13 | 0 | 0 |
| `legacy_parallel_c4` | 6 | 0 | 44,435,758,710 | 2,467,377 | 0 | 0 | 6 | 6 | 0 |
| `span_native_c1` | 14 | 4 | 105,993,401,056 | 0 | 0 | 0 | 14 | 0 | 0 |
| `span_native_c2` | 6 | 0 | 48,451,003,280 | 2,467,375 | 0 | 0 | 6 | 6 | 0 |
| `span_native_c4` | 5 | 0 | 32,467,058,418 | 2,104,618 | 0 | 0 | 5 | 5 | 0 |
| `span_native_c8` | 5 | 0 | 27,885,986,352 | 1,917,688 | 0 | 0 | 5 | 4 | 0 |
| `span_native_c16` | 5 | 0 | 26,864,508,284 | 1,886,376 | 0 | 0 | 5 | 4 | 0 |
| `span_native_c4_no_backlog` | 5 | 0 | 34,727,494,749 | 2,104,728 | 0 | 0 | 5 | 5 | 0 |
| `span_native_c4_cache_disabled` | 5 | 0 | 31,334,449,162 | 1,973,916 | 0 | 0 | 5 | 5 | 0 |

Backlog coalescing counters:

| Row | admitted runs | extra memtables | extra ops | selected memtables max | selected ops max | last single-op ratio | last ops/span |
|---|---:|---:|---:|---:|---:|---:|---:|
| `default_unconfigured` | 0 | 0 | 0 | 0 | 0 | 0.000000 | 0.000000 |
| `legacy_parallel_c4` | 0 | 0 | 0 | 0 | 0 | 0.000000 | 0.000000 |
| `span_native_c1` | 0 | 0 | 0 | 0 | 0 | 0.502361 | 8.030750 |
| `span_native_c2` | 1 | 16 | 493,448 | 48 | 1,480,344 | 0.569091 | 3.962833 |
| `span_native_c4` | 2 | 32 | 986,896 | 48 | 1,480,344 | 0.595205 | 3.532906 |
| `span_native_c8` | 2 | 32 | 986,896 | 48 | 1,480,344 | 0.603832 | 3.409103 |
| `span_native_c16` | 2 | 32 | 986,896 | 48 | 1,480,344 | 0.603721 | 3.413830 |
| `span_native_c4_no_backlog` | 0 | 0 | 0 | 0 | 0 | 0.000000 | 0.000000 |
| `span_native_c4_cache_disabled` | 2 | 32 | 986,896 | 48 | 1,480,344 | 0.591578 | 3.482905 |

## Disk usage at end of run

| Row | index.db | WAL | value_vlog | leaf_vlog |
|---|---:|---:|---:|---:|
| `default_unconfigured` | 606 MiB | total=12 B files=1 other=12 B | total=0 B files=1 | total=3.7 GiB files=232 value=3.7 GiB other=24 KiB |
| `legacy_parallel_c4` | 484 MiB | total=12 B files=1 other=12 B | total=0 B files=1 | total=3.4 GiB files=216 value=3.4 GiB other=22 KiB |
| `span_native_c1` | 606 MiB | total=12 B files=1 other=12 B | total=0 B files=1 | total=3.7 GiB files=232 value=3.7 GiB other=24 KiB |
| `span_native_c2` | 417 MiB | total=12 B files=1 other=12 B | total=0 B files=1 | total=3.3 GiB files=208 value=3.3 GiB other=21 KiB |
| `span_native_c4` | 462 MiB | total=12 B files=1 other=12 B | total=0 B files=1 | total=3.3 GiB files=210 value=3.3 GiB other=21 KiB |
| `span_native_c8` | 447 MiB | total=12 B files=1 other=12 B | total=0 B files=1 | total=3.3 GiB files=210 value=3.3 GiB other=21 KiB |
| `span_native_c16` | 436 MiB | total=12 B files=1 other=12 B | total=0 B files=1 | total=3.4 GiB files=210 value=3.4 GiB other=21 KiB |
| `span_native_c4_no_backlog` | 510 MiB | total=12 B files=1 other=12 B | total=0 B files=1 | total=3.5 GiB files=220 value=3.5 GiB other=22 KiB |
| `span_native_c4_cache_disabled` | 443 MiB | total=12 B files=1 other=12 B | total=0 B files=1 | total=3.3 GiB files=208 value=3.3 GiB other=21 KiB |

## Allocation and profile top rows

Random-write allocation totals and top sampled allocation-object rows:

| Row | alloc-space total | alloc-objects total | top alloc-object rows |
|---|---:|---:|---|
| `default_unconfigured` | 2.71GB | 9,232,395 | `Zipper.mergeLeaf` 1,974,391; `cachingLeafPageLog.AppendLeafPages` 1,835,089; `putValueLogPtrsNoClear` 1,835,047 |
| `legacy_parallel_c4` | 3108.33MB | 24,681,672 | `DB.appendValueLogOneInternal` 10,043,614; `putVlogPreparedFrames` 7,121,739; `putValueLogPtrsNoClear` 1,835,047 |
| `span_native_c1` | 2537.35MB | 9,534,278 | `putValueLogRecordsNoClear` 2,140,891; `Zipper.mergeLeaf` 1,998,970; `putValueLogPtrsNoClear` 1,900,587 |
| `span_native_c2` | 3392MB | 22,969,237 | `DB.appendValueLogOneInternal` 9,437,399; `putVlogPreparedFrames` 6,400,829; `putValueLogPtrsNoClear` 1,747,667 |
| `span_native_c4` | 3595.10MB | 20,285,794 | `DB.appendValueLogOneInternal` 8,306,878; `putVlogPreparedFrames` 5,614,379; `cachingLeafPageLog.AppendLeafPages` 1,507,397 |
| `span_native_c8` | 3491.32MB | 21,927,356 | `DB.appendValueLogOneInternal` 8,863,944; `putVlogPreparedFrames` 5,963,910; `putValueLogRecordsNoClear` 1,638,437 |
| `span_native_c16` | 3498.51MB | 20,117,156 | `DB.appendValueLogOneInternal` 8,274,114; `putVlogPreparedFrames` 5,417,767; `putValueLogPtrsNoClear` 1,376,286 |
| `span_native_c4_no_backlog` | 3203.92MB | 21,591,960 | `DB.appendValueLogOneInternal` 8,749,251; `putVlogPreparedFrames` 5,483,304; `Zipper.mergeLeaf` 1,646,692 |
| `span_native_c4_cache_disabled` | 3530.94MB | 20,863,793 | `DB.appendValueLogOneInternal` 8,962,254; `putVlogPreparedFrames` 5,483,304; `cachingLeafPageLog.AppendLeafPages` 1,474,628 |

Random-write CPU/block/mutex/checkpoint top rows:

| Row | CPU total/top | block total/top | mutex total/top | post-run checkpoint CPU total/top |
|---|---|---|---|---|
| `default_unconfigured` | 101.39s; `lz4block.decodeBlock` 9.04s; `CompressBlock` 7.71s; `runtime.memmove` 5.95s | 1702.58s; `runtime.selectgo` 740.11s; `sync.Mutex.Lock` 641.85s; `WaitGroup.Wait` 168.98s | 684.50s; `sync.Mutex.Unlock` 681.91s | 2.21s; `linux.Syscall6` 0.24s; `runtime.memmove` 0.20s; `lz4block.decodeBlock` 0.19s |
| `legacy_parallel_c4` | 90.36s; `lz4block.decodeBlock` 8.22s; `CompressBlock` 6.77s; `runtime.memmove` 4.66s | 794.75s; `runtime.selectgo` 476.79s; `chanrecv2` 104.85s; `chanrecv1` 88.85s | 47.37s; `sync.Mutex.Unlock` 47.33s | 2.31s; `linux.Syscall6` 0.25s; `lz4block.decodeBlock` 0.20s; `runtime.memmove` 0.17s |
| `span_native_c1` | 78.79s; `lz4block.decodeBlock` 8.89s; `CompressBlock` 6.07s; `cmpbody` 4.20s | 1292.96s; `runtime.selectgo` 941.31s; `chanrecv1` 171.86s; `sync.Mutex.Lock` 99.13s | 92.25s; `sync.Mutex.Unlock` 89.38s | 2.48s; `linux.Syscall6` 0.36s; `runtime.memmove` 0.17s; `lz4block.decodeBlock` 0.15s |
| `span_native_c2` | 75.93s; `lz4block.decodeBlock` 7.76s; `CompressBlock` 5.69s; `cmpbody` 4.15s | 749.34s; `runtime.selectgo` 487.73s; `chanrecv1` 94.17s; `WaitGroup.Wait` 75.80s | 46.80s; `sync.Mutex.Unlock` 46.79s | 1.99s; `runtime.memmove` 0.20s; `lz4block.decodeBlock` 0.16s; `crc32IEEEVPCLMUL512` 0.12s |
| `span_native_c4` | 77.01s; `lz4block.decodeBlock` 7.54s; `CompressBlock` 5.91s; `cmpbody` 3.96s | 637.53s; `runtime.selectgo` 383.58s; `chanrecv2` 83.49s; `chanrecv1` 71.05s | 36.89s; `sync.Mutex.Unlock` 36.86s | 10.84s; `lz4block.decodeBlock` 0.95s; `CompressBlock` 0.79s; `runtime.memmove` 0.66s |
| `span_native_c8` | 89.56s; `lz4block.decodeBlock` 8.36s; `CompressBlock` 6.76s; `runtime.memmove` 4.35s | 676.23s; `runtime.selectgo` 332.99s; `chanrecv2` 166.11s; `chanrecv1` 64.96s | 57.16s; `sync.Mutex.Unlock` 57.02s | 7.79s; `lz4block.decodeBlock` 0.66s; `CompressBlock` 0.65s; `runtime.procyieldAsm` 0.43s |
| `span_native_c16` | 94.87s; `lz4block.decodeBlock` 8.57s; `CompressBlock` 6.84s; `runtime.memmove` 5.07s | 814.65s; `runtime.selectgo` 352.69s; `chanrecv2` 250.78s; `sync.Mutex.Lock` 93.72s | 93.02s; `sync.Mutex.Unlock` 92.81s | 8.52s; `lz4block.decodeBlock` 0.67s; `CompressBlock` 0.54s; `runtime.memmove` 0.44s |
| `span_native_c4_no_backlog` | 82.28s; `lz4block.decodeBlock` 8.08s; `CompressBlock` 6.55s; `runtime.memmove` 4.31s | 649.75s; `runtime.selectgo` 388.66s; `chanrecv2` 80.60s; `chanrecv1` 72.81s | 44.09s; `sync.Mutex.Unlock` 40.44s; `RWMutex.RUnlock` 3.63s | 11.42s; `lz4block.decodeBlock` 0.98s; `CompressBlock` 0.92s; `runtime.memmove` 0.68s |
| `span_native_c4_cache_disabled` | 73.21s; `lz4block.decodeBlock` 7.92s; `CompressBlock` 6.42s; `cmpbody` 3.90s | 549.41s; `runtime.selectgo` 322.63s; `chanrecv2` 72.98s; `chanrecv1` 64.59s | 35.29s; `sync.Mutex.Unlock` 35.26s | 5.99s; `lz4block.decodeBlock` 0.61s; `CompressBlock` 0.49s; `runtime.memmove` 0.31s |

## Notes and caveats

- The `span_native_c4_cache_disabled` row is a useful write-ceiling guardrail,
  not a default-read-cache decision. It should be followed by read/scan/reopen
  guardrails before changing cache defaults.
- `span_native_c1` proves span-native should not be default-enabled without a
  concurrency/config gate; it used span-native but lost the apply parallelism
  needed to offset reducer/prepare costs.
- c8/c16 reduce foreground assist wait and improve throughput, but contention
  and CPU profile totals rise. These rows should be treated as throughput knobs,
  not universal defaults.
- The matrix used 128-byte values, so ordinary `value_vlog` remained empty;
  persistent outer leaves are in `leaf_vlog` and remained durable storage.
