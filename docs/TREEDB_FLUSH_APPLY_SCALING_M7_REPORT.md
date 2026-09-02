# TreeDB flush/apply high-concurrency M7 final gate

Parent tracker: #2819. Final gate issue: #2832.

## Decision

Keep the default TreeDB flush admission policy at the conservative `auto` c4
candidate. Do **not** raise the default to physical-core scaling in this cycle.

Default/unconfigured cached TreeDB therefore remains:

- `FlushAdmissionPolicyAuto`
- admitted on suitable hosts as c4 span-native flush/apply
- backlog coalescing enabled
- adaptive write-side outer-leaf cache admission
- low-concurrency and unsafe WAL-off-relaxed shapes fail closed

Higher-concurrency rows (c8/c16 or physical-core-scaled candidates) remain
`FlushAdmissionPolicyExplicit` experiments. `FlushAdmissionPolicyOff` remains
the immediate rollback switch.

## Why c4 remains the default

The M0-M6 stack landed the intended scaling pieces:

- #2825: worker saturation and pipeline wait counters
- #2826: coarser span-native work units
- #2827: span-native leaf-log output path
- #2828: prepared publish/final-install split with GC-safe value-log pinning
- #2829: checkpoint barrier/debt/worker-participation counters and pre-drain kick
- #2830: old-leaf decode/recompress reduction
- #2831: random-write allocation pressure reduction

The final policy gate still favors c4 because it is the best balanced default
for mixed read/write deployments. Existing high-concurrency evidence shows c8
and c16 can be useful write-ceiling rows, but they are workload choices rather
than safe universal defaults: they can increase allocation, contention, cache,
and checkpoint sensitivity. The default should optimize for predictable durable
cached-mode behavior and straightforward rollback.

## Reproduction / evidence commands

Full production gate shape, for large hosts:

```sh
OUT=$(mktemp -d /tmp/gomap_profiles_XXXXXX)
./bin/unified-bench \
  -dbs treedb \
  -keys 10000000 \
  -valsize 128 \
  -batchsize 8000 \
  -test sequential_write,batch_random,random_write,random_read \
  -checkpoint-between-tests \
  -settle-before-scans \
  -treedb-cache-stats-before-reads \
  -profile-dir "$OUT"
./bin/benchprof -profiles-dir "$OUT"
```

Explicit comparison rows should use:

- default auto: no admission override
- rollback/off: `-treedb-flush-admission-policy=off`
- explicit c4/c8/c16: `-treedb-flush-admission-policy=explicit` plus the
  row-specific `-treedb-flush-apply-concurrency=N` and span/backlog knobs

Small same-host smoke command used for this gate PR:

```sh
TREEDB_COLLECTION_BENCH_BATCH_SIZE=1000 \
  GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkCollectionShapeInsertBatchCheckpoint/indexes_0$' \
  -benchtime=1000x -count=1 -timeout=10m
```

Smoke result excerpt on the PR host:

| benchmark | ns/op | insert_ns/doc | sync_ns/doc | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| `BenchmarkCollectionShapeInsertBatchCheckpoint/indexes_0-12` | 8,791 | 523.6 | 8,267 | 1,357 | 3 |
| `BenchmarkCollectionShapeInsertBatchCheckpointSingleStringJSON/indexes_0-12` | 8,613 | 524.7 | 8,088 | 870 | 3 |

## Guardrails to watch

When evaluating any explicit higher-concurrency row, include these counters and
artifacts in the report:

- `treedb.flush_admission.*`
- `treedb.flush_apply.*`
- `treedb.cache.flush_apply.*`
- `treedb.cache.checkpoint.debt_*`
- `treedb.cache.checkpoint.barrier_wait_*`
- `treedb.cache.checkpoint.flush_all.worker_passes_total`
- `treedb.cache.checkpoint.flush_all.workers_total/max`
- `treedb.cache.checkpoint.stage.*`
- read/cache hit and write-admission counters
- CPU, allocation, block, and mutex profiles
- `index.db`, `value_vlog`, and `leaf_vlog` footprint after checkpoint

## Rollback

No on-disk migration is involved. To roll back the scaling stack at runtime, set:

```go
treedb.Options{FlushAdmissionPolicy: treedb.FlushAdmissionPolicyOff}
```

or use the benchmark/runtime flag:

```sh
-treedb-flush-admission-policy=off
```

Use `FlushAdmissionPolicyExplicit` only for controlled experiments that also
record read/cache/checkpoint guardrails.
