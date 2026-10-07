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
All three callers reach a readiness barrier before their shared release. The
barrier lies outside each public-call timer and is included in epoch overhead;
readiness alone does not prove overlap. A scheduler that produces zero actual
overlap causes a retained refusal, without added writes, retries or exclusions.

Two old version iterators retain the seed image through every epoch checkpoint,
then are consumed and released after the last pinned checkpoint. Every mode
performs the same public checkpoint after its joined epoch. COW requires the
backend commit sequence to stay unchanged within each cached-lag epoch and to
advance at that checkpoint; the completed checkpoint alone resets the baseline.
Legacy comparator snapshots can rotate and enqueue tables, so their actual
backend progress is retained and checked for nonregression. Logical retained
history grows across epochs even though the cached generation is checkpointed.
Representation diagnostics inspect every expected physical MVCC entry through
public `AcquireSnapshot`, `GetEntryExact` and snapshot `Close`, before epochs,
after the final pinned checkpoint, and after reopen. Seed checks cover `keys`
entries; checkpoint and reopen each cover `keys*(1+3*epochs)`, including logical
tombstones, which are ordinary stored values. Actual flags and value pointers
must match the declared layout, and observed inline/pointer counts are retained.
Pointer entries require a nonzero pointer to a value-log file; a zero optional
length hint remains valid. All diagnostic snapshots close, including on error;
COW view, cut and external-lease counts must return to their preceding values.
Forced-pointer cases also require a positive single-value value-log raw-byte
counter at checkpoint. Complete logical point/history payload oracles prove
readability separately. Close is a successful public call, not an engine
resource census after Close.

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
checkpoint interval, idle trigger, WAL-size trigger and background index vacuum,
with generational policy explicitly Off and side stores disabled; COW limits are 256 views, 64 generations, 32 sources,
256 resources, 256 MiB generation, 64 MiB in-flight and 2 GiB total/retired
bytes. These finite constructor ceilings accommodate conservative COW path-copy
charges within an epoch and the retained seed generation. They do not waive
the separate pending native whole-call 32-record/1-MiB contract.
Values are immutable shared 256-byte fixture payloads. Go timed allocations
include epoch checkpoints, call recorder, oracle checks and boundary Stats
overhead during epochs;
seed/oracle construction, all representation diagnostics and cleanup are outside
the benchmark timer. Diagnostic snapshots can affect setup state and lifetime
high-water charges even while excluded from timed allocations. Legacy snapshots
can rotate pending mutable tables; COW snapshots acquire and release read owners.
Engine
charges, Go B/op/allocs/op and caller process RSS are distinct observations.
Raw call durations include setup, checkpoint, Close and reopen, with all errors;
summing overlapping durations does not produce elapsed wall time.
`public_calls/op` counts the complete recorded lifecycle, including diagnostics,
divided by epochs; `ns/op` covers only the declared timed epoch scope.

The test binary flag `-cow-c4-public-output-dir` must name an absolute existing
directory outside source and database storage. Each actual benchmark invocation
creates one immutable JSON file, using exclusive hard-link publication from a
complete temporary file. Names bind the leaf hash, actual b.N and bounded process
invocation sequence. A requested count above 1 retains Go's initial 1x
calibration alongside the requested invocation; neither is discarded. Partial
lifecycle failures retain outcome/error receipts and refuse acceptance.

```sh
mkdir /tmp/cow-c4-raw
GOWORK=off GOMAXPROCS=4 go test ./TreeDB/mvcc/cowsustained -run '^$' \
  -bench '^BenchmarkCOWSustainedPublicMVCC/command_wal_relaxed/cow_btree/inline/N512$' \
  -benchtime=4x -benchmem -count=1 -args \
  -cow-c4-public-output-dir=/tmp/cow-c4-raw
```

The [standalone tooling](../../../scripts/cow_c3_read/README.md) uses the existing
source/build authority, compiled module audit, hermetic environment, host and
TMPDIR admission, watchdog and offline provenance checks. A closed C4 schema
adds exact raw work, options, resolved ACK, finite counters, lifecycle phase and
oracle validation. Configuration must be explicitly frozen before collection;
construction runs 36 candidate leaves and may bind both variant labels to the
same product; matched evidence requires distinct source commits, trees,
manifest digests, binaries and independent non-nested build/source locations,
then runs separate warmups plus three ABBA cycles. Raw distributions and matched comparisons are
descriptive, with no retained-tail or native qualification claim. Successful
collection and analysis remain separate from current-head review, race tests,
actual runtime evidence and coordinator acceptance.

The analyzer binds ordinary ACKs to observed command-WAL counters in write-only
windows. Seed and growth each append one command per 16-key group; each joined
epoch adds two such group series and one ordinary command. Durable mode requires
one file sync per ordinary command, relaxed mode requires zero ordinary syncs,
and NoWAL requires observed absolute zero appends and syncs at every boundary.
All profiles require parseable actual counters and effective command-WAL/redo
routing: command profiles use external command WAL with cached redo disabled;
NoWAL uses disabled command WAL and disabled-unsafe cached redo. NoWAL does not
disable persistent value-log storage. A checkpoint counter change inside
these windows refuses attribution. Each declared public checkpoint must advance
its run count by exactly one; pin release advances none and preclose advances
exactly one. Actual options, rather than fixed labels, are recorded. Every real
boundary requires resolved generational Off, disabled scheduler/vacuum, no active
or pending worker and zero automatic checkpoint/maintenance runs. The untimed
3.2-second persistent-pointer pin sentinel crosses multiple periodic ticks and
the default idle interval, then verifies old payloads, exact release/preclose
windows, Close and reopen. Incompatible requested options and an actual Open
with HotWarmCold policy must be refused. Schema v2 packets cannot accept v1.

Call intervals must also follow the finite lifecycle order. Each caller's calls
are sequential, all three overlap workers finish before their epoch checkpoint,
and checkpoint, pin release, Close, reopen, and final Close follow their declared
stages. Configuration uses the shared canonical variant-path and Git-identity
validators. Copied-positive smoke cases include zero observed WAL appends, zero
durable ordinary syncs, unexplained relaxed syncs, premature checkpoints, and
Close before seed, plus actual routing, representation counts, logical-tombstone
coverage, diagnostic snapshot ownership and zero actual overlap; updating
artifact hashes cannot make these acceptable. Each diagnostic acquisition,
lookup and close has its own retained call. Recorder capacity includes the
additional `keys*(3+6*epochs)+6` calls outside the unchanged timed epoch work.

Both classes use the shared 15-key fixed process environment with
`CGO_ENABLED=0`. The 12-artifact build closure includes Go launcher/compiler
executables, implicit assembler include headers, and actual selected persistent
inputs before and after compilation, with file bytes and modes. External
GOROOT/module inputs use normalized identities that must match both builds;
repository candidate source may differ. Offline analysis requires the retained
live Go version and inventory to match both build receipts and the frozen
toolchain identity.
Copied-positive cases cover an unbound Go launcher, missing toolchain identity and an invalid matched
same-product configuration; source-manifest corruption is a corruption refusal,
not a claim to have exercised a valid semantic source-mode mutation.

The standalone C4 test package imports the ordinary exported MVCC product.
Build with `build.py --suite c4`; retain the complete selected-minus-product
harness closure, including the lifecycle fixture, TestMain and shared admission
helpers, with identical bytes and modes across products. Warmup receipts describe
their actual epoch count separately from measured work. At released, preclose
and reopened boundaries, require zero views, one active cut, positive current
roots, exact generation/frozen-root balance and exact external-lease balance.
The completion record has a closed schema and preserves the pending native
qualification claim. Copied-positive mutation checks refuse changes to any of
these observations even when dependent artifact hashes are recomputed.
