# Negative point lookup qualification (#342)

The optional main-backend filter uses one fixed bit array, seven probes and a
per-open `hash/maphash` seed. At roughly ten bits per covered key the theoretical
false-positive rate is about 1%. Hash seed variation, actual key distribution,
churn and saturation require empirical measurement. This theoretical estimate
is not a speed claim. Keys longer than 1024 bytes use exact lookup.

`BenchmarkNegativeLookup` exercises public cached selection after checkpoint,
using 8192 persisted keys with misses interleaved inside the key range. Payloads
are compressible 256-byte values and deterministic random 4096-byte values.
Uniform and Zipf schedules each cover 0/50/90/99% misses. Routes include owned
`Get`, caller-buffer `GetAppend`, 64-key `GetMany`, callback `GetManyView` and a
snapshot retained across later updates of the same payload size and entropy.
Setup verifies actual coverage on the captured backend trees after checkpoint
and updates; an enabled fixture that silently falls back to exact fails before
timing. Reported filter bytes come from active storage, not the configured
budget. Benchmark setup, checkpoint and warming are outside timing. Default checksum verification stays enabled.

`BenchmarkNegativeLookupUpdate` measures ordinary cached 64-key batch `WriteSync`
selection and acknowledgement separately from `WriteSyncCheckpoint`, which also
flushes and publishes each batch. It reports actual backend publications/op; a
cached acknowledgement can defer membership hashing until a later flush.
`BenchmarkNegativeLookupBootstrap` includes
backend open/close and bootstrap; compare enabled versus disabled results as
whole-operation costs, not isolated hashing. Durable synchronization may mask
small publication costs. A separate preparation benchmark isolates hashing and
the bounded publication token from storage I/O. `BenchmarkNegativeFilterMembership`
reports empirical absent-key false positives, fixed retained bytes, allocation
cost and behavior at ten times the intended key count (saturation).

Example collection (serialize timing on an otherwise idle allocated runner):

```sh
GOWORK=off GOMAXPROCS=4 GOMEMLIMIT=1GiB go test ./TreeDB -run '^$' \
  -bench '^BenchmarkNegativeLookup$' -benchmem -benchtime=500ms -count=5
GOWORK=off GOMAXPROCS=4 GOMEMLIMIT=1GiB go test ./TreeDB -run '^$' \
  -bench '^BenchmarkNegativeLookup(Update|Bootstrap)$' -benchmem -benchtime=10x -count=5
TREEDB_HOT_PATH_STATS=1 GOWORK=off GOMAXPROCS=4 GOMEMLIMIT=1GiB \
  go test ./TreeDB -run '^$' -bench '^BenchmarkNegativeLookup$' \
  -benchmem -benchtime=10000x -count=1
```

Counters count shared exact point-descent entrances and filter rejections per
input key. Keep counters disabled for throughput comparison; their atomics add
cost. Batch routing can visit more than one exact descent entrance per key.
Measure empirical false positives separately with absent keys against a known
complete filter, and distinguish false positives from real hits. Report per-key
batch timing as ns/op divided by keys/op.

Retained storage is one word-rounded bit budget plus one small header per open
main handle. Old snapshots share that allocation; they do not allocate another
bit array per root. Coherent views and raw trees carry one pointer each; enabled
point publications carry one small temporary coverage token. Off mode allocates
no filter or coverage token. Stats `treedb.negative_lookup_filter.active_bytes`
reports active bit storage only, excluding headers and allocations still pinned
by old snapshots after coverage is dropped. Saturation never enlarges storage.

No retained final-base timing results have been collected yet. Provisional
correctness runs use Linux Go 1.26.3, GOMAXPROCS=2, GOMEMLIMIT=2GiB. The initial
Mac build hit ENOSPC and is infrastructure evidence, not a product failure.
Final qualification must record the merged predecessor, exact source hashes,
host, toolchain, commands, counters, raw repeated measurements, allocations,
retained memory and uncertainty before making a performance claim.
