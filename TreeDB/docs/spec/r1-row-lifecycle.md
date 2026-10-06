# R1 sustained row lifecycle

Status: provisional correctness qualification for [#5060](https://github.com/snissn/gomap/issues/5060), under [#5056](https://github.com/snissn/gomap/issues/5056). Final qualification requires the accepted [#5057](https://github.com/snissn/gomap/issues/5057) baseline and retained harness, plus the accepted read and mutation predecessors. These tests establish bounded correctness and resource lifetime, not sustained capacity or a performance result.

The fixture and public operation boundaries are shared with
[R1 indexed row mutations](r1-indexed-mutations.md): external IDs; non-null
authoritative strings `email`, `city`, `name`, `bio`; retained JSON
`id`, `age`, `score`, `revision`, `active`, and null-versus-missing `optional`.
The indexed case has unique email and eight nonunique city buckets. Both index
modes use `command_wal_durable`, immutable typed row assets, and the persistent
value log. Generic `UpdateBatch` reconstructs and replaces a full row; it does
not acquire native partial-column support through this qualification.

## Deterministic mixed cycles

`TreeDB/collections/r1_lifecycle_5060_test.go` reuses the mutation fixture and
actual command-WAL crash process. `TestR1LifecycleMixedCycles5060` seeds 32 rows
and runs 12 cycles in each index mode. Every cycle performs typed insertion,
two-row replacement with a unique email handoff, an indexed-field generic
update, a nonindexed/retained-field generic update, a mixed existing/missing
typed upsert, and two deletes. Two new IDs and two removed IDs per cycle bound
the live set at 32. Updates preserve Unicode and exercise null/missing
transitions. Expected maps are maintained independently of stored documents.

After each operation, one fresh prepared read view checks all known primary
IDs, every complete field, and every historical/current email and city posting.
`FetchDocumentsByID` and `VisitIndexValueIDs` operate on the same captured
snapshot. Expected index owners are derived from the independent row map,
including empty results for deleted or superseded keys. Opening the view flushes
the collection's write domain; this test does not measure pending-view latency.
Pending same-manager admission, publication overlap and tombstone ordering stay
covered by the mutation and write-domain suites. The fixture grants no new
cross-manager visibility or transaction contract.

Two views, opened and fully warmed before cycles zero and six, keep their
captured full rows and postings throughout later mutations, checkpoint,
maintenance and value-log pointer replacement. Their original maps remain
separate from current state. Preparing current reads avoids coupling this test
to the predecessor's range-document materialization repair.

## Maintenance and resource lifetime

Every third cycle checkpoints, invokes `CompactRootOverlays`, and invokes
ordinary `ValueLogGC`. The selected fixture currently has no root overlays, so
those compaction calls report zero work. Actual overlay folding remains covered
by `TestCollectionCompactRootOverlaysFoldsIntoBaseRoots`; a no-op is not evidence
of compaction throughput or reclaimed bytes.

After churn, `ValueLogRewriteOnline` selects the segment containing a real
retained primary pointer and must copy live records. Ordinary value-log GC runs
with the old views still open; both views must remain readable. Source segments
can remain protected by old readers, active appenders or selectable recovery
roots. The test does not require immediate value-log deletion after rewrite or
view release. Existing retained-payload GC/rewrite coverage requires actual
value-log reclamation when its complete eligibility conditions hold.

The typed asset phase appends one valid, unpublished four-string candidate to
the live segment. This deliberately creates a known mixed segment; it is not
an assertion that ordinary row churn already makes every historical typed row
reclaimable. `ColumnAssetRewrite` must perform no remap while warmed mapped
resource handles protect that segment. Captured documents and indexes remain
unchanged. Closing both views must release all of their active handles.

After release, typed rewrite must copy/remap live manifest references and leave
the old source segment on disk. An immediate GC must preserve it while a
selectable durable generation still references it. The existing durable
fallback-advance helper checkpoints and lawfully advances that generation.
Only then must a fresh ordinary `ColumnAssetGC` delete the eligible old segment
and report positive deleted bytes. Current full rows and postings must remain
identical after remap, GC and final reopen. This phase reuses the production
reachability scanner, identity fences, pins, remapper and GC; it adds no scanner,
lease registry, COW format, or reclamation shortcut.

The report-only lifecycle report and broader lifecycle registry contract remain
owned by [#1954](https://github.com/snissn/gomap/issues/1954).
[Typed asset maintenance](typed-asset-maintenance-1788.md) defines destructive
planning and persistent resource retention. Incomplete closure must fail closed;
maintenance completion, retained bytes and remaining debt are separate facts.
This work does not supersede the active rewrite-resource closure work in
[#5037](https://github.com/snissn/gomap/issues/5037).

## Recovery after churn and maintenance

`TestR1LifecycleRecoveryAfterMaintenance5060` performs repeated mixed cycles,
checkpoint, ordinary value-log GC, typed rewrite/remap, view release and typed
GC. It then runs the existing real command-WAL subprocess for typed upsert at
pre-append rejection, post-sync error before ACK, and successful ACK, exiting
without `Close` or `Flush`. Reopen must preserve every earlier row and posting
and recover exactly the old state for rejection or the complete accepted
command for ambiguity/ACK. The reused process requires `ErrCommitAmbiguous`
for the post-sync cut and rejects automatic retry classification. This is a
process crash test, not a physical power-loss simulation. Broader replay
validation and concurrent COW cut coverage are reused rather than duplicated.

## Reproduction and evidence boundary

Run with Go 1.26 or later on a platform with exact relative namespace support;
the destructive mixed-segment test explicitly skips unsupported platforms.

```sh
GOWORK=off go test ./TreeDB/collections -run '^TestR1Lifecycle' -count=1 -v -timeout=5m
GOWORK=off go test -race ./TreeDB/collections -run '^TestR1Lifecycle' -count=1 -timeout=5m
GOWORK=off go test ./TreeDB/collections \
  -run '^(TestCollectionCompactRootOverlaysFoldsIntoBaseRoots|TestColumnRetainedPayloadValueLogPlacementGCRewrite|TestCollectionReadViewMappedPinsProtectRewriteCandidates|TestColumnAssetRewriteLifecycleSmokeWithMutationsM15C|TestColumnAssetLifecycleRegistryReleaseAndDBCloseCleanup1954|TestCollectionReadViewClosedAndNilFailClosed)$' \
  -count=1 -timeout=5m
```

These correctness tests log actual maintenance counts and bytes as diagnostic
facts. They are not a retained benchmark harness and set no throughput, tail
latency, allocation, memory or storage-growth acceptance threshold.

`BenchmarkR1Lifecycle5060` in `r1_lifecycle_5060_bench_test.go` is provisional
standalone diagnostic tooling. It seeds 4,096 live rows and performs 1,024 mixed
public calls per epoch: ordinary/prepared complete reads, indexed generic
updates, nonindexed typed replacement, delete/reinsert and typed upsert.
Independent full-row/posting checks run outside timed epochs. One warmed old
view spans the first churn/checkpoint/maintenance epoch and is then released.
It reports mixed p95/p99 and calls/second; epoch `B/op` and `allocs/op`;
call-loop allocation counters; sampled heap high and post-GC process retained
heap; and logical component bytes/file counts at ingest, churn, checkpoint,
maintenance, release and reopen. It logs no-op/protected maintenance honestly.
Call-loop times and allocations include caller encoding and oracle bookkeeping.
Prepared calls include opening and closing a fresh read view and its flush;
they do not measure steady reuse of a warmed current view. Phase memory samples
report process heap allocation, in-use bytes and object counts without forcing GC.
Heap high is a phase sample, not an exact peak; retained heap includes the
benchmark oracle and latency samples. Allocated filesystem blocks, exact RSS,
unsampled peak memory and a comparator are unavailable. The benchmark requires
ordinary complete-row reads, so capture must include the accepted read repair.
No sustained result is established by merely compiling this tooling. A fixed
epoch count makes diagnostic scope explicit, for example:

```sh
GOWORK=off go test ./TreeDB/collections -run '^$' \
  -bench '^BenchmarkR1Lifecycle5060$' -benchtime=5x -count=1 -benchmem -v
```

The standalone output is not an accepted #5057 comparator packet. Align the
eventual retained lifecycle extension with that harness's fixture, public
backend interfaces and source/schema validator after ownership handoff; do not
combine separate fixtures or timer scopes into a speedup claim.

Retained lifecycle measurement must use the reviewed, landed harness/schema and
freeze exact product and harness identities before collection. Its phases must
include ingest, repeated churn, checkpoint, lawful maintenance and reopen;
report mixed throughput and p95/p99 with operation/timer scope; allocations;
measured peak/live/retained memory; and component census separating index,
persistent value log/leaf logs, typed assets and redo WAL. Record checkpoint and
maintenance duration, completed work, protected/retained bytes and remaining
debt. A successful no-op, blocked remap or reader release must not be counted as
reclaimed storage. Preserve failed and partial runs, unsupported comparison
cells, fixture equality, cache/durability settings and provenance. Final cost
and sustained capacity acceptance remain pending that retained evidence.
