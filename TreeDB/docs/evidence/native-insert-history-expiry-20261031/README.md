# Ordinary native insert history-expiry repair

This is correctness evidence and an engineering regression-control fixture, not
Mongo throughput qualification. Parent #5238; origin is the failed durable1
component under #5361. Its original partial commits remain non-ACK counts. The
separate failed strict `j:true` ingestion remains unresolved by these tests.

## Causal baseline and candidate

- Production base: landed combined correctness
  `15601589437b3e41ede1f6cac73609dc331fba02`.
- New diagnostic-only test at
  `a7f29678099ccfaa3f6889341d4a6d1cc8c47cae`:
  `TestNativeOrdinaryInsertHistoryExpiry`.
- Actual baseline normal exit 1: opening=2, history floor=3, next LSN remains
  13, five real disjoint public batches produce 4097 effects. The paused public
  BSON target returns `transaction conflict history expired`. No injected target
  error, changed cap, moved hook or history-size change.
- Production repair at `694a6a8fcbbabf32e897b3d01da561aa9d3adb56`:
  the same genuine-eviction test passes, as do the route, explicit transaction,
  synchronous cleanup, typed-exclusion and actual postappend controls. The
  aggregate normal invocation failed its new FreshDuplicate fixture: it assumed
  that the winning public ACK consumed exactly one WAL identity. This failure is
  retained unchanged in `candidate-focused-01`.
- Test correction at `905d948c17e8108661fee503af49ff5c4f7f9005` records the actual
  winner frontier after its ACK and requires no later target assignment. Source
  explains the earlier extra identity: overlapping announced durable intents
  prevent idle-singleton capture, and promoting the winner can append a durable
  prefix barrier (`command_wal_group_commit.go`, `idleDurableCaptureEligibleLocked`;
  `db/command_wal_raw.go`, durable prefix append). The corrected narrow normal
  control passes. The first focused race run passes 20 tests/subtests with no
  skip or race report. Production blobs are identical to `694a6a8fc`.

The original real-expiry test requires exactly one target assignment after one
fresh attempt, exact acknowledged BSON bytes and returned ID, held old-read
absence before and after checkpoint, checked original manager/database Close,
and exact bytes from a fresh owner after reopen.

The remaining controls use maintained error seams or a private receipt wrapper
only to verify qualification boundaries: two-attempt exhaustion, fresh duplicate
validation, direct-manager, indexed, template and ordinary ColumnGraph routes,
explicit transaction expiry, synchronous Close failure (including one wrapping
the sentinel), actual postappend error, assigned/LSN-only/uncertain/installed
receipts and unrelated failures. Injected controls do not replace causal RED.

## Contract

Only an ordinary one-shot insertion can reconstruct once. Eligibility comes from
its captured metadata, the exact raw core history-refusal error and actual
publication result. Synchronous cancellation and every Close check must succeed;
asynchronous semantic retirement is not synchronously drained. Assignment,
uncertainty, logical installation, apply/replay, caller callbacks, explicit
transaction openings and other typed routes cannot acquire this retry authority.
Collection producer admission ends before fresh maintenance/capture/staging.
There is one shared two-attempt bound, not nested Collection/manager loops.

Accepted diagnostics count only the final plan; staging/publication timings sum
the two attempts and HistoryExpiryRetries records zero or one. Refused attempts
publish no success diagnostics. No queue, batching policy, history tier, memory
limit or ACK/durability contract changes.

## Reproduction and retained outputs

Use Go 1.26.10, `GOTOOLCHAIN=local GOWORK=off`, GOMAXPROCS=2, `-p=1`, CGO=1.
Every local cell used the existing exact host185 shared lease and trusted shared
Go/module caches, isolated fast-mount DB/temp/output, 180-second Go limit and
240-second outer command bound. Each terminal receipt includes original argv,
actual exit, source/tree, caps/resource observations and verified empty cleanup;
release receipts observe the same lease free. No quiet-host/performance inference
is made from these correctness runs. Original absolute paths are provenance,
not dependencies for reading this packet.

```sh
# New test on the diagnostic revision: expected genuine history-expiry RED.
go test -json -p=1 -count=1 -timeout=180s ./TreeDB/collections \
  -run '^TestNativeOrdinaryInsertHistoryExpiry$'
# Candidate full focused race selector:
go test -json -p=1 -count=1 -timeout=180s -race ./TreeDB/collections \
  -run '^TestNative(OrdinaryInsertHistoryExpiry|InsertHistoryExpiry.*)$'
```

`SHA256.json` binds the retained raw outputs and custody/terminal/release receipts.
Source-bound required CI, independent review, default-path allocation/cost
qualification and merged-main checks are subsequent acceptance gates. This packet
alone does not establish merge readiness or successful original max1 ingestion.

`BenchmarkNativeOrdinaryInsertRequest` is a new matched public-path engineering
fixture: serial default durable COW BSON, batch1/256, request ACK inside timing,
first/last exact readback plus checkpoint/checked Close/reopen outside timing.
It is not a full-corpus correctness proof or the concurrent expiry discriminator.
Both before/after arms must use identical benchmark bytes and fixed sample/Go
contracts; report ns/request, documents/s, B/op and allocs/op and all samples.
No benchmark from this fixture had run when this source packet was prepared.
