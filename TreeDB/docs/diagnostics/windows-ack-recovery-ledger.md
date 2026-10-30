# Windows ACK-image recovery ledger (#5340)

This diagnostic-only experiment starts from exact
`6fe6f26ac3ffbc5a02c78d3417dd85794b458aee`. It does not repair either retained
Windows timeout or enable replay batching. Source review is required before any
execution. Preserve the original failures and the fixed 15 s child / 20 s parent
bounds in `TestOrderedCompletionStableImageAtAcknowledgement`.

## Enablement and boundary

Only `GOMAP_RECOVERY_LEDGER=1` enables the ACK child helper. The helper allocates
one 4096-slot ledger and one test-owned reporting goroutine at child entry. It
records fixture preparation, freeze and live-capture boundaries directly. It installs the global
memory-only sink immediately before the original `powerlossreopen.Stable` call,
never the full file-copy durability observer. The original database remains
alive; all global entries must be distinguished by captured root/identity.

From the root Go module, a future reviewed Windows diagnostic uses PowerShell:

```powershell
$env:GOWORK = 'off'
$env:GOMAP_RECOVERY_LEDGER = '1'
go test ./TreeDB -run '^TestOrderedCompletionStableImageAtAcknowledgement$' -count=1 -timeout=2m -v
```

A supported Linux race control uses the same source, fixture and internal bounds:

```sh
GOWORK=off GOMAP_RECOVERY_LEDGER=1 go test -race ./TreeDB -run '^TestOrderedCompletionStableImageAtAcknowledgement$' -count=1 -timeout=2m -v
```

These commands are source-verified recipes, not execution evidence. No test,
build, package load, benchmark or workload was run during construction. Resource
custody/admission and the bounded plan must be current before any execution;
never repeat an unchanged candidate toward a favorable outcome.

## Ledger contract

The neutral `internal/recoverytrace` package imports only the standard library.
Each producer first captures one active ledger pointer. Slots are reserved once,
filled by that single producer, then published atomically. Published slots are
never overwritten. A reporter reads only published slots, tracks each slot
individually, and revisits earlier unpublished slots on later snapshots. There
is no callback, diagnostic file borrow, `Stat`, path reopen, new production lock,
physical sync replacement, persistence certificate or failure recovery.

The fixed array bounds retained event storage; each textual label owns at most
160 bytes. The identity's numeric/native object/generation fields and canonical
frontier summary/digest are copied by value. There is no retained RID slice,
resource token, original error or file handle. Labels explicitly report clipping.
After 4096 reservations, records are dropped with an explicit counter; correlation
zero is unusable. A trace with dropped/clipped/missing records is incomplete and
cannot prove the missing boundary. Metadata and log formatting allocations in the
test helper are separate from this storage bound. No measured allocation or
performance claim is made.

The test reporter snapshots new ready records every two seconds, before the
unchanged child timeout when scheduling/output permits, and on joined shutdown.
This is bounded diagnostic output (at most 4096 event lines plus periodic
summaries within the child lifetime), not guaranteed output on process kill or
an immediate runtime abort. Opt-in parent tests also retain successful child
output through `t.Logf`; default mode retains its original output handling. Producer recording does no output. Reporting may
perturb scheduling and elapsed time; a passing diagnostic run is not proof the
original failure was reproduced or explained. Restore does not join producers
that already captured the sink. Their writes remain bounded and safe, but may
arrive after the final report and must not be inferred absent.

Entries contain stage, outcome, sequence/correlation, and elapsed nanoseconds
since child-ledger construction. Sequence reservation order need not equal wall
clock order for concurrent producers. The stage values are:

| Value | Stage |
| --- | --- |
| 1 | fixture preparation through live capture |
| 2 | stable-image writable Open |
| 3 | WAL read / dependency lookup / classification |
| 4 | WAL suffix repair |
| 5 | one original replay frame (recorded LSN) |
| 6 | original admission wait (CommitSeq) |
| 7 | original durable fence (CommitSeq) |
| 8 | external-resource flush |
| 9 | external-resource sync / original durability observation |
| 10 | resource materialization |
| 11 | original index sync |
| 12 | original physical producer SyncThrough invocation |
| 13 | reopened original Close |
| 14 | source original Close |
| 15 | stable-model freeze / observer restore |
| 16 | live physical capture |

Outcomes are 1=entry, 2=returned nil, 3=returned error, 4=reserved marker.
A return points to its entry sequence. No deferred return event is fabricated
on panic, Goexit or a nonreturning operation. Stable Open return classification
checks both the outer materialization error and `Result.Rejected` / missing DB;
ordinary original Open rejection lives in `Result.Err`, even with outer error nil. The observation has no error
return and changes no original error identity or cleanup precedence.

Stage 12 is the original producer API invocation, not necessarily a single OS
syscall. Imported aliases recurse without an extra producer event; covered
frontiers produce none. The original token supplies its captured stable identity,
required frontier, certified frontier and whether owned operations are used.
An opaque callback may do additional work or ignore the supplied file. This
first slice does not identify that callback implementation, every namespace
sync, or every native pager/OS operation. Do not label the paired producer
interval as a measured single `FlushFileBuffers` duration without a further
source-bound native witness. No handle or filename is used as recapture authority.

There is no ambient current-LSN or goroutine-local context: token events have no
invented LSN/CommitSeq. Frame/fence/external phase ordering is useful evidence,
but concurrency alone cannot authoritatively attach an arbitrary token operation
to a logical frame. Missing direct parent linkage, producer identity, native-call
boundaries, or cleanup/handoff phases are explicit next-witness decisions.

When enabled, the helper observes and checks the original reopened and source
Close returns. The ordinary mode preserves the original deferred unchecked close
behavior. It neither invokes a second Close nor manufactures the original return.
All ACK ordering, history/revision/value assertions and storage limits remain.

## Focused source controls and qualification

Authored but unexecuted controls cover fixed-capacity rejection/overflow, label
clipping, no-overwrite publication, installation overlap/reuse and stale restore,
concurrent read/publication, imported physical owner and covered-frontier paths,
before/sync/after sentinel identity and metrics, physical panic/Goexit without a
fabricated return, and materialize/index/install error ordering. A real public adaptive-COW
refusal checks the separate rejected-result/outer-nil contract and unchanged
original error identity.

Future focused commands from the root module:

```sh
GOWORK=off go test ./TreeDB -run '^TestACKRecoveryLedgerRejectsOriginalPublicOpenFailure$' -count=1
GOWORK=off go test ./TreeDB/internal/recoverytrace -count=1
GOWORK=off go test -race ./TreeDB/internal/recoverytrace -count=1
GOWORK=off go test ./TreeDB/internal/rootpublication -run '^(TestStableResourceFileSyncObserverExactPhysicalBoundary|TestRecoveryLedgerPhysicalAbnormalExitHasNoReturn)$' -count=1
GOWORK=off go test ./TreeDB/db -run '^TestRecoveryLedgerStoragePhaseErrorsPreserveOriginalOrder$' -count=1
```

Require compiler/API, race, Windows public-path recovery and exact-head CI/review
before using this candidate as runtime evidence. It has no authority to weaken
existing gates, elide sync, advance a shared certification watermark, change WAL
repair/retirement, discard persistent pointers, merge per-frame roots or reuse a
failed accepted command. Actual replay batching remains a separate gated design.
