<!-- cow-current-host-migration:start -->
**Migration handoff: incomplete native qualification.** Pushed clean **codex/5047-floor-reopen-provisional @ 72d7dcda2ad4922157e61ba8229f283c106579e9** includes independently reviewed new cow_floor_wal_recovery_external_test.go. Normal/race13 nodes passed naturally (4children/8reopens), durable/relaxed inline/pointer command-WAL ACK then os.Exit without Close/Checkpoint. This is process-exit recovery, not power loss or native prune. [Evidence](https://github.com/snissn/gomap/issues/5047#issuecomment-6064298701), [recoverable receipts](https://github.com/snissn/gomap/blob/3b671cc377f882f5bfa0a3cd1fd51b84d183a915/handoff/issue-5044-20261008/README.md).

No provisional integration PR before usable all-19 ABI/final source. Await #5111/#5118 owner; then reconcile72d7, complete real Store.Prune/SAME rounds, physical ACK/partial progress, floor rejection/history/tombstone/pointer oracle, checkpoint/rewrite/GC and reopen. Never enable native prune through legacy fallback or close based on WAL-only tests. [Resume order/owners](https://github.com/snissn/gomap/issues/5044).
<!-- cow-current-host-migration:end -->

Parent: https://github.com/snissn/gomap/issues/5044

Accepted specification: [#5027 implementation plan](https://github.com/snissn/gomap/issues/5027#issuecomment-6002765873). Assigned coordinator and acceptance owner: `/root`.

Deliverable: Gomap PR routing existing public Store.GetAt/IterateVersions/CommitAt/CommitGroupAt/prune/replay through the integrated capability, removing only redundant decoding/publication fences after every supported mutator participates. Parent: #5044. Depends on merged [C3-read #5076](https://github.com/snissn/gomap/issues/5076) (built on C2 #5046) and accepted/merged Gomap#4878 contract. Acceptance: coordinator; #4878 owner retains bounded prune/deletion correctness authority. Performance class: performance-objective.

Begin with a failing deterministic point/scan reader-progress test through an actual Store while a multi-record writer pauses after final preparation, plus prune-floor admission tests. Preserve encoded MVCC timestamps and existing value/tombstone selectors. Physical cut sequence and application timestamp are distinct: a pinned view must retain earlier same-ts data and exclude later historical insertions. Initial Store grouped/floor fence removal is capability-dispatched, never table-name-dispatched; unsupported/batch-only adapters keep current fences. Retain maintenance/floor admission until one core cut is pinned, then read materializes outside the writer gate. New below-floor calls reject; existing admitted views retain their documented domain. Retain atomic whole-call/group publication and complete no-resurrection prune/floor/reopen ordering.

An optional OpenReadView(readTs) is activated only if an actual caller needs multiple reads on one cut; if selected, GetAt and IterateVersions share its exact admitted domain, budget and close ownership. Existing independent GetAt remains independently fresh. Do not add an unused public view framework or a second read registry. All-version iterators remain stable; Close/error/cancel release the one owner. Current floor/progress semantics are consumed from #4878, not reimplemented or weakened; M7 owns default-source producer ordering and prune fixed-work gates. Reconcile code/contract overlap with that owner before writing shared mvcc/caching files.

Test/public path: same-ts replacement, late-history, tombstones/raw deletes and floor equality; iterator and view lifetime under concurrent grouped writes, pruning, GC/rewrite/checkpoint and Close; supported mutator completeness and early rejection matrix; no writer gate held over scan/value decode. Exercise exact-key and real all-version shapes with counters. Recovery and bounded delete-progress fixtures from #4878 must apply to the new sources/cuts; unsupported native maintenance returns before advancing a floor. Existing adapters preserve characterization and guardrails.

Measure actual integrated public CommitAt+GetAt, CommitGroupAt+version scan under matched no_wal_fast/command_wal_relaxed/command_wal_durable semantics, concurrency and checkpoint debt. Record work/copy/source counts, sync/group eligibility, tails, B/op/allocs, live/pinned/retired/peak bytes and whole lifecycle. No claim of cross-request durable grouping from fence removal alone; Dgraph#34 owns that objective. Qualified dirty scans must improve their targeted rotation/capture work and measured lifecycle cost beyond noise; if COW write costs erase benefit, retain transitional option status and revise before qualification.

Docs: TreeDB/docs/spec/contracts.md and dgraph-mvcc-readiness-3673.md API/lifetime/floor/error sections, verification.md, docs/contracts/CONCURRENCY.md and TreeDB public examples. Dgraph#36 retains adapter route/EntryView/AllVersions parity owner; Dgraph#37 retains task/view, split/rollup/dependency admission and pruning activation. Supply exact engine contract to them; do not silently reinterpret their timestamp or cached-List lifetimes. Exit: proved capability is reached, unnecessary fences removed only on it, maintenance/durability/public-read and allocation/performance packet passes. Bypass or ambiguous floor/deletion proof blocks fence removal, not a request to hide it behind a fallback.

## Completion packet checklist

- [ ] Source-bound initial failing invariant test or explicit evidence-node exception recorded.
- [ ] Full assigned implementation/artifact and actual production path/eligibility/refusal contract delivered.
- [ ] Required correctness, concurrency/race, recovery/lifetime and refusal checks pass at the candidate identity.
- [ ] Allocation/copy/retention audit, measured minimization and applicable scaling/performance evidence accepted.
- [ ] Owning canonical docs/guides/examples are current; no contract update deferred.
- [ ] Necessary predecessors accepted/merged, current required CI and mature review passed where PR-bearing.
- [ ] Coordinator acceptance and exact merged/accepted output recorded; failure actions and remaining owner obligations preserved.


## Graph Reassessment: focused read milestone split (2026-10-06)

C3-read #5076 now owns the independently implementable coherent-cut read/floor admission, grouped fence narrowing and unsupported-maintenance before-effects refusal. C3 consumes its merged behavior/tests/docs rather than duplicating that PR. This issue retains integrated bounded pruning/replay/maintenance and complete MVCC lifecycle qualification on M7's accepted contract. Its prior read-progress RED packet remains the source-bound initial failure for the read milestone. All original completion, durability, fixed-work, lifetime, performance and shared-owner gates remain required here; the focused read milestone alone cannot close this issue or the parent.

<!-- native-retention-integration:start -->
The active native dependency remains #4878, whose ordinary PRIMARY/DATA publication prerequisite #5111 has its existing sole writer. That owner also owns producer plus shared consumer integration in cow_cut.go, cow_flush.go and dictionary snapshot admission; this graph will not create a competing writer. The current, unfrozen proposal uses SnapshotAllocationSizes.PrimaryMetadata as a retained allocation owner enrolled in the existing COW budget, with one growth-governing lease per actual owner/budget. It must not be added again to the scalar snapshot charge.

The consumer readiness review (`d59c940d`, exact8d880 source) identifies the existing startup, handoff and dictionary seams. Snapshot.Close can precede completion of active reads/iterators, so successful enrollment belongs to the actual last-read finalizer. Exact PRIMARY cleanup must drain before detaching the governor; partial or real failed cleanup retains its debt. Refusal and acquire/adopt/pin error paths must unwind untransferred ownership before effects. Exact tested source/ABI, shared-growth admission and lifetime proofs remain pending; this is a provisional integration map, not native acceptance. Reuse adequate predecessor checks and the existing publisher inventory where the new descriptor preserves their scope.
<!-- native-retention-integration:end -->

<!-- provisional-floor-reopen-test:start -->
A disjoint, provisional public Store recovery test is independently accepted at [17aaa84a](https://github.com/snissn/gomap/blob/17aaa84a7cd0c8efe7ab87a1db71c764eddde416/TreeDB/mvcc/cow_floor_recovery_external_test.go) on branch `codex/5047-floor-reopen-provisional`, based on C3 `8d8806b4`. It adds only an external-package test; C3 production source and the native owner's shared files are unchanged.

Local Go1.26.0 normal and race checks passed all six durability/storage combinations and two reopens per combination. On each fresh Store, prune is attempted before any floor loading: the unsupported response leaves WAL append/sync and COW publication/capture counters unchanged, and a subsequent floor read positively demonstrates that the capture counter is observable. It also verifies persisted-floor equality rejection, atomic mixed-group refusal, tombstones and exact retained history. Source SHA256 `6423cdfa188ceaa78e5f351650edb043bcaceabc27ba1a6c215851145a5336e6`; independent acceptance `c6c7a12513a6cf472cb98a74d3a0ecdd2b34a3ff4dfb566299032038ded97e72`.

This is checkpoint/clean-close recovery evidence for the current transitional unsupported-prune contract. Positive native deletion, crash/uncheckpointed WAL replay, Linux qualification and performance remain pending. Reconcile the test with the native contract and final merged base before the full implementation PR. #5047 remains open with all existing completion gates and dependency ownership intact.
<!-- provisional-floor-reopen-test:end -->
