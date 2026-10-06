# Ordinary public sustained MVCC construction

`BenchmarkCOWSustainedPublicMVCC` supports 36 leaves: three resolved durability
profiles, `cow_btree`/`append_only`/`btree`, inline/forced-pointer values, and
512/1024 logical keys. This is bounded ordinary public API construction and
matched supported-path evidence. Qualification is `pending_native_observations`.
Native eligibility and whole public maintenance charge remain typed `PENDING`;
the future native contract requires at most 32 records and 1 MiB for the whole
public call. This fixture neither simulates those observations nor qualifies
native pruning, M7, C4 maturity, parent #5044, or COW promotion.

Use fixed `-benchtime=1x` through `8x`. Each iteration denotes one full history
epoch; duration-based adaptive campaigns refuse. Seed timestamp 1 is followed
by insert, tombstone and reinsert at `10*epoch+1/+2/+3`. Real grouped writes use
16 keys, and ordinary CommitAt plus grouped equal-timestamp replacements add
no retained records. Full history contains `keys*(1+3*epochs)` records. Point,
historical, prefix-neighbor, state, timestamp, payload and iterator-count
oracles inspect actual outputs. Three joined ordinary callers perform grouped
replacement, point reads and full history reads; interval receipts establish
actual public-call overlap without claiming internal publication atomicity.

Two old version iterators retain the seed image through every epoch checkpoint,
then are consumed and released after the last pinned checkpoint. Every mode
performs the same public checkpoint after its joined epoch. COW requires the
backend commit sequence to stay unchanged within each cached-lag epoch and to
advance at that checkpoint; the completed checkpoint alone resets the baseline.
Legacy comparator snapshots can rotate and enqueue tables, so their actual
backend progress is retained and checked for nonregression. Logical retained
history grows across epochs even though the cached generation is checkpointed.
Forced-pointer
cases require a positive single-value value-log raw-byte counter at checkpoint,
which observes the persistent write path. Complete point/history payload
oracles after checkpoint, Close and reopen prove readability; the byte counter
alone does not inspect every index pointer. Close is a successful public call,
not an engine resource census after Close.

COW functional tests separately fill a finite cut-owner bound and require
public point/history refusal without fallback, release and successful retry.
A bounded total-byte pressure test holds actual public cuts, requires a real
write-capacity refusal before floor/publication/WAL effects, then releases and
retries the identical group. It permits at most 16,384 acquisitions and logs
actual owner count and charged bytes per owner. These paused pressure schedules
have no throughput claim. Unsupported COW PruneVersions is checked before
floor, WAL, capture and publication effects; no private maintenance producer is
invoked.

Options are explicit: four shards, 16 MiB flush threshold, disabled background
checkpoint and side stores; COW limits are 256 views, 64 generations, 32 sources,
256 resources, 256 MiB generation, 64 MiB in-flight and 2 GiB total/retired
bytes. These finite constructor ceilings accommodate conservative COW path-copy
charges within an epoch and the retained seed generation. They do not waive
the separate pending native whole-call 32-record/1-MiB contract.
Values are immutable shared 256-byte fixture payloads. Go timed allocations
include epoch checkpoints, call recorder, oracle checks and boundary Stats
overhead during epochs;
seed/oracle construction and cleanup are outside the benchmark timer. Engine
charges, Go B/op/allocs/op and caller process RSS are distinct observations.
Raw call durations include setup, checkpoint, Close and reopen, with all errors;
summing overlapping durations does not produce elapsed wall time.

The test binary flag `-cow-c4-public-output-dir` must name an absolute existing
directory outside source and database storage. Each actual benchmark invocation
creates one immutable JSON file, using exclusive hard-link publication from a
complete temporary file. Names bind the leaf hash, actual b.N and bounded process
invocation sequence. A requested count above 1 retains Go's initial 1x
calibration alongside the requested invocation; neither is discarded. Partial
lifecycle failures retain outcome/error receipts and refuse acceptance.

```sh
mkdir /tmp/cow-c4-raw
GOWORK=off GOMAXPROCS=4 go test ./TreeDB/mvcc -run '^$' \
  -bench '^BenchmarkCOWSustainedPublicMVCC/command_wal_relaxed/cow_btree/inline/N512$' \
  -benchtime=4x -benchmem -count=1 -args \
  -cow-c4-public-output-dir=/tmp/cow-c4-raw
```

The [standalone tooling](../../../scripts/cow_c3_read/README.md) uses the existing
source/build authority, compiled module audit, hermetic environment, host and
TMPDIR admission, watchdog and offline provenance checks. A closed C4 schema
adds exact raw work, options, resolved ACK, finite counters, lifecycle phase and
oracle validation. Configuration must be explicitly frozen before collection;
construction runs 36 candidate leaves and matched evidence runs separate
warmups plus three ABBA cycles. Raw distributions and matched comparisons are
descriptive, with no retained-tail or native qualification claim. Successful
collection and analysis remain separate from current-head review, race tests,
actual runtime evidence and coordinator acceptance.

The analyzer binds ordinary ACKs to observed command-WAL counters in write-only
windows. Seed and growth each append one command per 16-key group; each joined
epoch adds two such group series and one ordinary command. Durable mode requires
one file sync per ordinary command, relaxed mode requires zero ordinary syncs,
and NoWAL requires zero appends and syncs. A checkpoint counter change inside
these windows refuses attribution. Explicit checkpoint and later read-cleanup
windows remain separate observations; read cleanup can trigger additional
automatic checkpoints, whose run counts remain retained.

Call intervals must also follow the finite lifecycle order. Each caller's calls
are sequential, all three overlap workers finish before their epoch checkpoint,
and checkpoint, pin release, Close, reopen, and final Close follow their declared
stages. Configuration uses the shared canonical variant-path and Git-identity
validators. Copied-positive smoke cases include zero observed WAL appends, zero
durable ordinary syncs, unexplained relaxed syncs, premature checkpoints, and
Close before seed; updating artifact hashes cannot make these acceptable.
