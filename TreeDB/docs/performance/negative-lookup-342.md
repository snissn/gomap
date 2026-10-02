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
budget. Setup regenerates one expected payload buffer and checks every byte of
all 8192 original values, plus absence of every interleaved miss. Benchmark
setup, checkpoint and warming are outside timing. Default checksum verification
stays enabled. This 8192-key, 500ms matrix is bounded PR qualification; a larger
authoritative campaign waits for the graph's landed qualification harness.

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

Retained bounded qualification measured candidate
`37fb160c1ebfd21710cbce68da53d883c1e18eff` against merged predecessor
`a68a84c7195e0c5d8c339343a385b40acf818d03`, with 36 fresh processes,
930 performance samples and 160 separate diagnostic rows. At 99% uniform misses,
scalar Get median latency fell about 51%; GetMany64 per-key medians fell about
81%. Across all 0%-miss cases changes ranged -3.05% to +3.50%. Random4096 uniform
90%-miss routes recorded 0.1075 exact descent entrances/key and 0.8925 rejects/key.
The fixed filter retained 10272 bytes including its header; five-run empirical
false-positive median was 0.0077, degrading to 0.9935 with ten times the key count.

Enabled 64-key cached WriteSync acknowledgement median was +9.36%, with zero
backend publications/op and deferred membership hashing; this difference cannot
be attributed to membership publication cost. WriteSync plus checkpoint was
+3.55% for the combined I/O operation, and whole backend open/close including
bootstrap was +5.88%. These are opt-in workload costs; defaults stay off. The
isolated enabled coverage preparation measured 64 B/op and one allocation/op.

The [complete report and raw packet](../../../docs/benchmarks/treedb_negative_lookup_20261002/results.md)
retain exact source/script/binary identities, commands, five-sample medians and
spread, public allocations, counters, bootstrap/saturation limits, failed
infrastructure packets and post-measurement applicability. Timing used Linux
Go 1.26.3, GOMAXPROCS=4, GOMEMLIMIT=1GiB on the coordinator's exclusive runner
grant; shared external services remained. This is bounded 8192-key PR
qualification, not the larger authoritative campaign. A subsequent correction
clears stale coverage in raw Tree.SetRoot; the measured routes have no SetRoot
callers. Original measurements retain their original source identity; current
CI and independent correction review remain separate coordinator gates.
