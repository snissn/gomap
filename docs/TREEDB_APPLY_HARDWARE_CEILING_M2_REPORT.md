# TreeDB apply hardware-ceiling M2 report

Issue: #2947. Parent: #2943. Root parents: #2916 / #2899.

## Verdict

Classification: **implementation-limited by a specific fixable cost**.

The current span-native apply path is **not proven hardware-close** on this host.
The apply workers stay occupied, but neither process CPU nor storage is
saturated, and the high-concurrency rows show avoidable implementation overhead:
value-log/leaf-log prepared-frame allocation churn plus growing leaf-log append /
coordination contention.

Follow-up blocker opened before closing #2947:

- #2960 — reduce span-native apply prepared-frame allocation and append
  contention.

This result does **not** close #2916/#2899. Uniform random B-tree COW remains an
algorithmically expensive shape (~3.2 ops/span in the #2931-compatible rows),
but the measured ceiling should not be called purely hardware/current-algorithm
limited until #2960 is addressed or explicitly waived.

## Evidence identity

- SHA: `ea5b00dbcb1441b10d5cdb7a831436edf24b186c`
- Branch/worktree: `snissn/2947-apply-hardware-ceiling`,
  `/home/mikers/orca/workspaces/gomap/2947-apply-hardware-ceiling`
- Host: `mikers-B560-DS3H-AC-Y1`, Linux `6.8.0-124-generic`, x86_64
- CPU: 11th Gen Intel Core i5-11400F, 6 cores / 12 threads (`nproc=12`)
- Go: `go1.26.0 linux/amd64`
- Artifact root: `/tmp/gomap_2947_apply_ceiling_20260623_115615`
- Generated summaries:
  - `/tmp/gomap_2947_apply_ceiling_20260623_115615/summary.md`
  - `/tmp/gomap_2947_apply_ceiling_20260623_115615/summary.json`
  - `/tmp/gomap_2947_apply_ceiling_20260623_115615/context_2931_shape_summary.md`
- Every substantial benchmark/profile command was serialized with
  `/tmp/gomap_diag_bench.lock`.

Each row has `benchprof_results.json`, `benchprof_results.md`, CPU, allocation,
block, mutex, trace, `perf stat`, `iostat`, `pidstat`, and `mpstat` artifacts.
Immediate-checkpoint rows also include checkpoint CPU profiles.

## Collection plan executed

- Primary matrix: c1/c2/c4/c8/c12/c16 apply-focused `random_write`
  without an immediate benchmark checkpoint.
- Context matrix: the same c1/c2/c4/c8/c12/c16 `random_write` rows with
  `-checkpoint-between-tests`.
- #2931 comparison: c4/c8/c16 using the exact final-gate test order
  `sequential_write,batch_random,random_write` with `-checkpoint-between-tests`.
- Fallback plan, not needed: if 10M rows exceeded the 30-45 minute row guards,
  keep c4/c8/c16 at 10M for #2931 comparison and rerun c1/c2/c12 at a smaller
  key count with an explicit caveat.

## Commands

Build:

```sh
make unified-bench benchprof
```

Apply-focused write-only sweep, changing only
`-treedb-flush-apply-concurrency=<1|2|4|8|12|16>`:

```sh
OUT=/tmp/gomap_2947_apply_ceiling_20260623_115615/write_only_c<N>
./bin/unified-bench -dbs treedb -test random_write \
  -keys 10000000 -valsize 128 -batchsize 8000 \
  -treedb-flush-admission-policy=explicit \
  -treedb-flush-apply-span-native -treedb-flush-backlog-coalescing \
  -treedb-flush-apply-min-entries=1 -treedb-flush-apply-min-spans=1 \
  -treedb-flush-apply-min-bytes=1 \
  -treedb-flush-apply-concurrency=<N> \
  -profile-dir "$OUT" -path-label native-fastpath -progress=false \
  -max-wall=30m -max-rss-mb=28672
./bin/benchprof -profiles-dir "$OUT"
```

Immediate-checkpoint context sweep used the same command plus
`-checkpoint-between-tests`, with output under
`/tmp/gomap_2947_apply_ceiling_20260623_115615/immediate_checkpoint_c<N>`.

#2931-compatible comparison rows:

```sh
OUT=/tmp/gomap_2947_apply_ceiling_20260623_115615/context_2931_shape_c<N>
./bin/unified-bench -dbs treedb \
  -test sequential_write,batch_random,random_write \
  -keys 10000000 -valsize 128 -batchsize 8000 \
  -checkpoint-between-tests \
  -treedb-flush-admission-policy=explicit \
  -treedb-flush-apply-span-native -treedb-flush-backlog-coalescing \
  -treedb-flush-apply-min-entries=1 -treedb-flush-apply-min-spans=1 \
  -treedb-flush-apply-min-bytes=1 \
  -treedb-flush-apply-concurrency=<4|8|16> \
  -profile-dir "$OUT" -path-label native-fastpath -progress=false \
  -max-wall=45m -max-rss-mb=28672
./bin/benchprof -profiles-dir "$OUT"
```

Host counters were captured around each command with:

```sh
perf stat -e task-clock,cycles,instructions,context-switches,cpu-migrations,page-faults,cache-references,cache-misses

iostat -x 1
pidstat -urdh -C unified-bench 1
mpstat -P ALL 1
```

## Apply-focused write-only sweep

`random_write` only, no immediate benchmark-requested checkpoint in the timed
row. The benchmark still checkpoints during final stats/close, but the
`random_write` CPU/allocation/contention profiles are scoped to the timed write
phase.

| c | random_write | timed wall | CPU profile | sampled CPU cores | worker wall-equivalent | worker busy | ops/span | single-op spans | old leaf B/op | leaf merges/op | replacement leaves/op | append frames/op | foreground assist wait |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 244,506/s | 40.90s | 50.93s | 1.25 | 1.00 | 100.0% | 3.136 | 44.8% | 1,352 | 0.319 | 0.438 | 0.319 | 31.13s |
| 2 | 502,197/s | 19.91s | 43.97s | 2.21 | 1.95 | 97.6% | 3.503 | 40.2% | 1,205 | 0.285 | 0.399 | 0.285 | 12.00s |
| 4 | 777,930/s | 12.85s | 41.57s | 3.23 | 3.79 | 95.2% | 3.791 | 32.3% | 1,105 | 0.264 | 0.364 | 0.263 | 4.47s |
| 8 | 731,567/s | 13.67s | 50.89s | 3.72 | 7.49 | 94.1% | 3.547 | 35.1% | 1,184 | 0.282 | 0.383 | 0.282 | 5.08s |
| 12 | 715,251/s | 13.98s | 52.82s | 3.78 | 11.17 | 93.5% | 3.547 | 35.1% | 1,184 | 0.282 | 0.383 | 0.282 | 4.72s |
| 16 | 701,785/s | 14.25s | 54.24s | 3.81 | 11.10 | 93.0% | 3.547 | 35.1% | 1,184 | 0.282 | 0.383 | 0.282 | 5.08s |

Interpretation:

- c4 is the local peak for this write-only shape.
- c8/c12/c16 keep workers occupied, but sampled process CPU remains only about
  3.7-3.8 effective cores and throughput flattens/regresses.
- This fails the hardware-close criterion: neither all cores nor storage are
  demonstrably saturated.

## Immediate-checkpoint context sweep

Same `random_write`-only row plus `-checkpoint-between-tests`.

| c | random_write | sampled CPU cores | worker wall-equivalent | worker busy | ops/span | single-op spans | foreground assist wait | after-run checkpoint |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 248,881/s | 1.25 | 1.00 | 100.0% | 3.136 | 44.8% | 30.61s | 5.43s |
| 2 | 500,076/s | 2.21 | 1.96 | 98.1% | 3.503 | 40.2% | 11.84s | 7.92s |
| 4 | 611,433/s | 3.08 | 3.82 | 95.9% | 3.552 | 37.0% | 8.62s | 4.80s |
| 8 | 743,095/s | 3.76 | 7.53 | 94.5% | 3.547 | 35.1% | 4.96s | 5.07s |
| 12 | 787,952/s | 3.87 | 11.06 | 92.6% | 3.555 | 34.6% | 2.80s | 8.33s |
| 16 | 712,243/s | 3.81 | 11.14 | 93.3% | 3.547 | 35.1% | 4.70s | 4.94s |

The immediate-checkpoint context does not change the classification: worker
occupancy is high, but target-phase sampled CPU still stays below four cores and
disk is not saturated.

## #2931-compatible comparison rows

These rows use the #2931 final-gate command shape
`sequential_write,batch_random,random_write` with `-checkpoint-between-tests`.
They are the apples-to-apples comparison for the #2931 c4/c8/c16 rows.

| c | sequential_write | batch_random | random_write | vs #2931 random_write | checkpoint total stats | active-background wait | flush_all | backend boundary | reducer publish | leaf sync | ops/span | single-op spans | worker wall-equivalent | worker busy | old leaf B/op | leaf merges/op | leaf lanes used | worker output lanes |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 4 | 2,355,104/s | 730,414/s | 249,761/s | -1.5% | 42.98s | 21.17s | 17.01s | 3.94s | 0.64s | 0.03s | 3.279 | 60.5% | 3.67 | 95.1% | 1,328 | 0.305 | 5 | 4 |
| 8 | 2,404,470/s | 711,587/s | 322,279/s | -0.4% | 60.16s | 32.01s | 27.26s | 0.00s | 0.96s | 0.05s | 3.190 | 60.5% | 7.13 | 94.0% | 1,363 | 0.313 | 9 | 8 |
| 16 | 2,290,313/s | 776,481/s | 352,589/s | +5.8% | 40.57s | 21.98s | 16.07s | 1.70s | 0.66s | 0.05s | 3.190 | 60.5% | 10.58 | 94.1% | 1,363 | 0.313 | 13 | 12 |

Comparison to `docs/TREEDB_LEAF_LOG_APPEND_LANES_M5_REPORT.md`:

- c4 is essentially flat: 249,761/s now vs 253,460/s in #2931.
- c8 is essentially flat: 322,279/s now vs 323,691/s in #2931.
- c16 is modestly higher: 352,589/s now vs 333,241/s in #2931.
- The shape remains ~3.2 ops/span with 60.5% single-op spans; this is the
  uniform-random COW workload shape, not a scheduler starvation signal.
- Leaf-log sync remains tiny (0.03-0.05s), so #2930/#2931 removed the old
  sync/fan-in class. The remaining ceiling is elsewhere.

## Cost classification

### Effective cores and worker busy/idle/wait

- Worker task occupancy is high across the sweep: ~93-100% busy ratio.
- Worker wall-equivalent occupancy scales to the configured budget (for example
  c8 ~7.1-7.5 and c16 ~10.6-11.1 in the context rows).
- Target-phase sampled CPU does **not** scale similarly: write-only c8/c12/c16
  are only ~3.7-3.8 sampled cores, and #2931-compatible c16 is ~3.3 sampled
  cores for `random_write`.

Conclusion: the worker scheduler can keep work in flight, but the process is not
CPU-saturating the 12-thread host. Busy worker time includes waiting/blocking and
is not enough to claim hardware-close.

### Old-leaf read/decode

In the #2931-compatible rows, old-leaf read/decode is about 1.33-1.36 KiB/op.
CPU profiles show LZ4 decode as a top cost:

- c4: `lz4block.decodeBlock` 8.58s / 9.3% flat.
- c8: `lz4block.decodeBlock` 7.41s / 8.3% flat.
- c16: `lz4block.decodeBlock` 7.29s / 7.8% flat.

This is expected COW data work, but not hardware saturation proof.

### Merge/build replacement leaf cost

#2931-compatible rows:

- leaf merges/op: 0.305 (c4), 0.313 (c8/c16)
- replacement leaf pages/op: 0.398 (c4), 0.406 (c8/c16)
- `Zipper.mergeLeaf` is a large cumulative CPU site (~54-56% cumulative in the
  context rows) but only ~2% flat.

This is the dominant current-algorithm work envelope.

### Leaf encode/checksum/compression

The top CPU rows are leaf compression/decompression, comparison, and copying:

- #2931-compatible c8 `random_write`: LZ4 decode 8.3%, LZ4 compress 8.1%,
  `runtime.memmove` 4.9%, `cmpbody` 4.5%.
- #2931-compatible c16 `random_write`: LZ4 decode 7.8%, LZ4 compress 7.3%,
  `runtime.memmove` 5.2%, `cmpbody` 4.2%.

CRC/checksum work is present but not a top limiter in these rows.

### Leaf-log append after #2930

The #2931-compatible rows preserve the desired selected-lane distribution:

- c4: 4 worker output lanes, 5 aggregate leaf-log lanes used.
- c8: 8 worker output lanes, 9 aggregate leaf-log lanes used.
- c16: 12 worker output lanes, 13 aggregate leaf-log lanes used.

Leaf-log sync is tiny (0.03-0.05s), so the old sync/fan-in bottleneck is not
back. However, append wait and contention still grow in the apply-focused rows:

| row | leaf-log append wait | reservation wait | block total | mutex total |
|---|---:|---:|---:|---:|
| write-only c4 | 16.38s | 0.41s | 182.85s | 8.64s |
| write-only c8 | 20.53s | 0.52s | 236.99s | 21.21s |
| write-only c12 | 22.84s | 0.57s | 296.63s | 38.50s |
| write-only c16 | 23.97s | 0.61s | 299.22s | 37.72s |

Block profiles at c8+ are dominated by `runtime.selectgo`, `chanrecv2`,
`chanrecv1`, and `sync.Mutex.Lock`; mutex profiles are almost entirely
`sync.Mutex.Unlock` wait accounting. This is not sufficient by itself to prove a
single lock is the only bottleneck, but it is enough to reject a hardware-close
claim.

### Reducer/root publish and backend boundary

In #2931-compatible rows:

- root/reducer publish remains material but not dominant:
  `flush_apply.reducer_publish.ns_total` is ~4.8-4.9s, while checkpoint-stage
  reducer publish is ~0.64-0.96s.
- checkpoint backend-boundary time is row-dependent (0-3.94s) and not the
  target-phase ceiling.

### Allocation and GC

Allocation profiles expose the clearest fixable blocker. In #2931-compatible
`random_write` rows:

- c4: 19.72M allocated objects; `appendValueLogOneInternal` 8.72M (44.2%) and
  `putVlogPreparedFrames` 5.26M (26.7%).
- c8: 18.60M allocated objects; `appendValueLogOneInternal` 8.54M (45.9%) and
  `putVlogPreparedFrames` 4.96M (26.7%).
- c16: 18.82M allocated objects; `appendValueLogOneInternal` 8.54M (45.4%) and
  `putVlogPreparedFrames` 5.70M (30.3%).

GC CPU is not the top flat cost, but this object churn is avoidable-looking and
is on the span-native apply/leaf-log output path. #2960 tracks reducing it and
then rechecking the remaining high-concurrency append contention.

### Disk / fsync / write bandwidth / IOPS

Host counters do not support an I/O-limited classification:

- write-only rows: `/tmp` device average write bandwidth is roughly 3.6-12.4
  MiB/s with average utilization ~4.5-5.7%; max utilization is generally below
  24%.
- #2931-compatible rows: average `/tmp` device utilization is ~5.7-6.5%; max is
  18-44% depending on row.
- leaf-log sync in context rows is 0.03-0.05s.

The runs do write data, but storage is not saturated and fsync/sync is not the
apply ceiling.

### Memory/cache behavior

`perf stat` cache-miss ratios are high enough to keep memory/cache behavior on
the watch list:

- write-only c8/c12/c16: ~32.6-34.2% cache misses.
- #2931-compatible c4/c8/c16: ~36.4%, 31.4%, 30.9% cache misses.
- max RSS is ~3.6-7.1 GiB across the large rows.

No memory-bandwidth counter was captured, so this report does not claim or rule
out a memory-bandwidth ceiling. The measured allocation/append overhead should
be addressed first; a future hardware-close pass should add memory-bandwidth or
uncore counters if the profile becomes otherwise clean.

## Final classification

Choose option 2 from #2947's exit gate:

> **implementation-limited by a specific fixable cost**

Specific blocker: **prepared-frame/value-log allocation churn and associated
high-concurrency leaf-log append/coordination contention in the span-native COW
apply path**. See #2960 for the before/after gate.

Not chosen:

- **hardware-close/current-algorithm-limited:** rejected because CPU and storage
  are not saturated, and the c8/c12/c16 flattening has allocation/contention
  evidence.
- **I/O/durability-limited:** rejected because disk utilization and sync time are
  low in the target rows.
- **inconclusive:** rejected for this milestone because there is a concrete
  implementation blocker with profile evidence. Memory-bandwidth counters are a
  useful next measurement only after #2960's overhead is reduced or waived.
