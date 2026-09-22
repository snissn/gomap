# Nativewire BSON YCSB Profile Guard

Use this guard when changing the nativewire BSON YCSB load hot path tracked by
issue #2083. The primary local target is:

```sh
go test ./TreeDB/nativewire \
  -run '^$' \
  -bench '^BenchmarkNativewireYCSBLoad/inproc/bson_binary$' \
  -benchmem \
  -benchtime=1s \
  -count=3
```

Record the branch, commit, host context, `ns/op`, throughput, `B/op`, and
`allocs/op`. For implementation PRs, compare before and after with `benchstat`
when possible.

## Profiles

Capture CPU, allocation, block, and mutex profiles from the same benchmark
shape:

```sh
OUT=$(mktemp -d /tmp/treedb_nativewire_ycsb_profiles_XXXXXX)

go test ./TreeDB/nativewire \
  -run '^$' \
  -bench '^BenchmarkNativewireYCSBLoad/inproc/bson_binary$' \
  -benchmem \
  -benchtime=3s \
  -count=1 \
  -cpuprofile "$OUT/nativewire_ycsb_cpu.pprof" \
  -memprofile "$OUT/nativewire_ycsb_allocs.pprof" \
  -blockprofile "$OUT/nativewire_ycsb_block.pprof" \
  -mutexprofile "$OUT/nativewire_ycsb_mutex.pprof" \
  -mutexprofilefraction=1

go tool pprof -top "$OUT/nativewire_ycsb_cpu.pprof" \
  >"$OUT/nativewire_ycsb_cpu_top.txt"
go tool pprof -top "$OUT/nativewire_ycsb_allocs.pprof" \
  >"$OUT/nativewire_ycsb_allocs_top.txt"
go tool pprof -top "$OUT/nativewire_ycsb_block.pprof" \
  >"$OUT/nativewire_ycsb_block_top.txt"
go tool pprof -top "$OUT/nativewire_ycsb_mutex.pprof" \
  >"$OUT/nativewire_ycsb_mutex_top.txt"

printf 'profiles: %s\n' "$OUT"
```

The block profile is the guard for `lockMutation` wait. Record the total delay
reported for `(*Collection).lockMutation` or the nearest collection mutation
lock frame. If the symbol is absent, record that explicitly rather than
assuming there was no contention.

## Collection Boundary

Use the collection benchmark to separate nativewire/client overhead from raw
collection insert cost:

```sh
for batch in 1 16 128 1024; do
  TREEDB_COLLECTION_DOCUMENT_FORMAT=bson \
  TREEDB_COLLECTION_BENCH_BATCH_SIZE=$batch \
  go test ./TreeDB/collections \
    -run '^$' \
    -bench '^BenchmarkCollectionShapeInsertBatch/indexes_1$' \
    -benchmem \
    -benchtime=1s \
    -count=3
done
```

Record `ns/op`, `B/op`, `allocs/op`, and any reported docs/batch or insert
stats. Batch size 1 is the closest collection-side comparison for YCSB's
one-document nativewire insert shape; larger batches show the per-call overhead
ceiling.

## Correctness Gate

Run the focused correctness packages before merging changes that affect this
hot path:

```sh
go test ./TreeDB/nativewire ./TreeDB/collections -count=1
```
