# Quicksilver-inspired native workload

This deterministic synthetic sensitivity model reuses the native database
registry, owned reads, snapshots, profiles and benchprof. It does not reproduce
Cloudflare replication or claim an unpublished production histogram.

## Cases and command contract

`-suite quicksilver` defaults to `-quicksilver-case realistic`, 3,000,000 loaded
keys, four readers and 2,000,000 aggregate requests per fixed phase. `-keys`
supports 1..10,000,000 for rehearsals and the 10M confirmation. Campaigns must
use a reviewed, landed harness and record source/binary/native-library identity;
profiled diagnostics and repeated unprofiled timing runs are separate evidence.

```sh
GOWORK=off go build -o bin/unified-bench ./cmd/unified_bench
# Small public-path correctness/profile rehearsal, not retained qualification.
OUT=$(mktemp -d /tmp/quicksilver_profiles_XXXXXX)
GOMAXPROCS=4 ./bin/unified-bench -suite quicksilver -dbs treedb -profile durable \
  -keys 8192 -seed 24 -quicksilver-reads 65536 -quicksilver-duration 200ms \
  -quicksilver-mixture primary -quicksilver-working-set uniform \
  -quicksilver-miss-percent 90 -quicksilver-commit ordinary -profile-dir "$OUT"
# Held-out mixture changes key, length and content weights independently of seed.
GOMAXPROCS=4 ./bin/unified-bench -suite quicksilver -dbs treedb -profile durable \
  -keys 8192 -seed 91 -quicksilver-reads 65536 -quicksilver-duration 200ms \
  -quicksilver-mixture holdout -quicksilver-working-set 20% -quicksilver-commit sync
# Historical reproduction retains fixed seed/trace and default CommitSync.
GOMAXPROCS=4 ./bin/unified-bench -suite quicksilver -dbs treedb -profile durable \
  -quicksilver-case random4k -quicksilver-commit sync
```

For three-engine Linux rehearsals, install matching native libraries and build
with `CGO_ENABLED=1 go build -tags 'lmdb rocksdb'`; select
`-dbs treedb,lmdb,rocksdb`. LevelDB is rejected before opening any engine because
its checkpoint replaces the live handle while readers run. LMDB accepts at most
126 reader workers and pins write transactions to one OS thread.

`-quicksilver-commit=auto` resolves to ordinary `Batch.Commit` for realistic and
`Batch.CommitSync` for historical cases. `ordinary|sync` explicitly override
that API choice. It does **not** assert equal durability across adapters: the
current RocksDB adapter synchronizes ordinary writes and LMDB's ordinary commit
uses its environment policy. Engine stats and the native environment must be
reported with comparisons. No production sync API or checkpoint is modified.

Each run measures the initial/final durable `Checkpoint` and close/reopen
boundaries separately. During concurrent reads, up to four quarter-boundary
checkpoints have their own `checkpoint_ms` observations. Ordinary ACK costs and
checkpoint debt must be interpreted separately; clean reopen is not a power-loss
oracle. `load_seconds` covers initial load/commit groups; deleted-key preparation
and fixture histogram/compression analysis have separate durations.

## Generic fixture, access and mutation generation

Generation `generic-v1` uses variable-length namespace, hostname-like and opaque
binary keys. Every family contains a bijectively scrambled 64-bit identity in
an interior token with explicit boundaries, followed by a variable suffix.
Families have disjoint leading markers. The bijection and family markers prevent
collisions; no fixed numeric suffix is required. Primary families are equal
thirds; holdout uses approximately 50% namespace, 30% hostname and 20% opaque.

| Mixture | 32..256 B | 257..2048 B | 2049..32768 B | Structured / opaque |
| --- | ---: | ---: | ---: | ---: |
| primary | 80% | 18% | 2% | 50% / 50% |
| holdout | 95% | 4% | 1% | 25% / 75% |

Bucket and content selection use independent seeded draws; sizes use equally
weighted logarithmic bands inside each bucket. Actual counts, min/max lengths
and logical bytes appear in `loaded_distribution`. Structured payloads vary
zone/route tokens, TTL, enabled state, region and action in every block; opaque
payloads use seeded pseudorandom words. Both have ID/generation headers and
change their contents across update generations. These are byte values with
config-like contents, not a valid-JSON API contract.

`compressibility` reports actual stdlib DEFLATE BestSpeed bytes for up to 4096
distinct loaded records selected by a seeded coprime stride. Each record is
compressed independently, including its header. Counts, raw/compressed bytes,
ratios and sampled size buckets are reported for structured/opaque/total groups.
The codec/sample basis is explicit; this is not a TreeDB/LMDB/RocksDB codec claim.

`-seed` controls fixture and access generation; `-seed=0` resolves through the
existing time-based CLI seed and the resolved seed is recorded. Each reader
uses its own PCG stream for the full run. `-quicksilver-working-set` selects
`uniform`, `1%` or `20%`; reduced sets are dispersed collision-free across the
loaded population with a coprime stride and seeded offset. The mixed/concurrent
absent-request percentage is independently selected by
`-quicksilver-miss-percent=0..100` (default 90). Hits-only and misses-only phases
still force their respective request class.

Misses balance arbitrary reserved-prefix keys, never-loaded common-prefix keys
and previously loaded/deleted keys. A disjoint 1% population is inserted and
committed, then deleted and committed before the initial checkpoint. Its actual
domain is smaller than the loaded population and is disclosed here. During
concurrent reads, some present-class requests target newly inserted identities;
these can be absent before insertion. Deleted present-class keys can disappear.
`requested_present`, `requested_absent`, `observed_hits` and miss-class counters
keep these distinctions explicit.

Mutation targets are a collision-free permutation, bounded by
`-quicksilver-updates` (default min(40,000,keys)). Successive targets rotate
update/delete/insert/repeated-overwrite. Groups contain up to 1000 targets; each
overwrite target is written at generations 1,2,3,4 in **four separate commits**,
so engines cannot collapse the entire burst inside one batch. `mutations` gives
operation counts and `mutation_commit_batches` gives actual commit count.
`update_batch_ms` measures the complete mutation group including overwrite
commits. Inserts use another disjoint domain. A paced writer and all readers
start at one barrier; failure cancels waits and joins them before owner close.

The phase's `distinct_accesses` counts actual unique byte-key identities across
workers, with present/absent request subtotals. Setup-allocated worker bitmaps
are merged after timing. They do not infer distinctness from requested counts.
Generic cases have no fixed trace; historical `random4k` and `structured256`
retain their 65,536-entry trace, 10:1 miss/hit schedule and fixed seed 24.
Generic seed/locality/mixture/miss flags are rejected for historical cases.

## Correctness and measured boundaries

All adapters use owned `Get` results. `-quicksilver-read-batch=1` uses ordinary
Get; otherwise each worker acquires/releases an owned-read snapshot every
configured batch (default 64 reads). Every measured read checks absence or
ID/length/generation against its permitted mutation state. The realistic case
checks every full loaded value and all three miss domains after initial durable
reopen, then verifies every surviving base value, inserted value, deleted target
and miss domain byte-for-byte after final durable reopen. The initial scan warms
the dataset; an additional 50,000 selected-set warmup reads precede timing. This
is a warmed local workload, not a cold-device or power-loss claim.

The sampled quantiles are p50/p95/p99/p999; maximum covers every successful read.
Phase names and profile filenames remain `quicksilver_hits`,
`quicksilver_misses`, `quicksilver_mixed`, `quicksilver_concurrent`, with initial
and final checkpoint profiles. Existing benchprof parsers continue consuming
`benchprof_results.json/md`; `quicksilver_results.json` is the authoritative
resolved contract/distribution/correctness sidecar. No new framework or artifact
naming convention is introduced. The shared README describes profiling, timer,
sampling and process-allocation boundaries.

`seconds` and ops/sec include PCG/key generation, exact-distinct bitmap writes,
Get/snapshot work and read validation. Per-request latency samples time only
Get/snapshot work, as in the historical harness. Generator work is deliberately
included in throughput and disclosed rather than hidden behind a short trace.
Concurrent reader timing excludes a slow writer tail; `composition_seconds`
includes its checked drain. Process allocations include overlapping background
and writer work, while allocation profiles also include orchestration/summary.
Native engine allocations are outside Go MemStats.

## Allocation audit and focused checks

Keys use one fixed 128-byte scratch per worker; PCG has worker lifetime and
introduces no per-read allocation. Exact distinct tracking costs
`8*ceil(5*keys/64)*(workers+1)` bytes, plus one byte per loaded key for mutation
state. A 512 MiB tracking limit rejects excessive configurations before opening
databases. At 10M keys/64 workers, bitmaps cost 406,250,000 bytes (387.4 MiB). Samples
retain the existing 8,000,000-byte aggregate buffer. Generic cases retain no
payload/key corpus and no 65,536-entry trace.

Generic writes reuse one 32 KiB value scratch and one key scratch per mutation
or load group. `Batch.Set/Delete` retain their existing copying ownership;
`SetView` is not used with mutable scratch. Engine batch allocations and owned
read output copies are required costs. Full verification uses one 32 KiB
expected-value scratch and one byte/key final-state array outside timed reads.
Codec/histogram allocations occur only in setup and have a separate duration.

```sh
GOWORK=off go test ./cmd/unified_bench -run '^TestQuicksilver' -count=1
GOWORK=off go test -race ./cmd/unified_bench \
  -run '^TestQuicksilver(RealisticWorkflow|ErrorJoins|StatsAfterWriterDrain)$' -count=1
GOWORK=off go test ./cmd/unified_bench -run '^$' \
  -bench '^BenchmarkQuicksilverGenericKey$' -benchmem -count=3
# Linux native proof (requires native headers/libraries):
GOWORK=off CGO_ENABLED=1 go test -tags 'lmdb rocksdb' ./cmd/unified_bench \
  -run '^TestQuicksilverRealistic(LMDB|RocksDB)$' -count=1
```

The fixture/operation/artifact change uses the benchmark-instrumentation
exception to production test-first work: deterministic collision/mixture tests,
actual-stream distinct accounting, commit-dispatch probes, corruption rejection,
reopen/race checks and public adapter rehearsals supply the correctness evidence.
Million-key retained campaigns belong to the parent evidence gate after landing.
