# Ordinary #5111 execution handoff — source frozen

Safe transfer point at 2026-10-15 15:34 UTC. No compiler/test/profile/fixture repair or source edit was started for this handoff. No owned child is running; runtime is released. Keep the source frozen until root publishes this recovery snapshot and transfers the writer lease. Root owns Git/GitHub/CI/merge/cleanup. Do not stage, commit, push, rewrite, or delete from this lane without its transfer instructions.

## Exact source and ownership

- Host: mikers@192.168.0.111.
- B: `/home/mikers/benchmarks/mvcc-4871-repair-20261013`.
- ONLY current editable ordinary worktree: `B/ordinary-5111-final-main-20261015` (W).
- HEAD: `b3c7ff702538e552e685ded554a79eb256a3380c`.
- Normal index SHA256: `6c107c7b18c0fec50145fff023772d63243cda2d7dd078a1f78c1ae5cd664539` (unchanged).
- ER: `B/evidence/ordinary-5111-final-base-20261015/ci-read-custody-repair-20261015`.
- Snapshot: `ER/handoff-20261015-frozen`.
- `owned-source-manifest.json` is the complete whitelist: **68 changed paths, 12 untracked**, exact SHA256/bytes/tracked status. All current dirty paths are covered by the prior authored43 manifest, compatible19 list, or current vacuum/nonlast7. `source/` contains immutable exact bytes; `tracked.patch` includes tracked changes only, so do not omit new files when restoring.
- `source-census.json`: **3749** Go files plus root Go module/work files, matching latest checks' census convention. SHA256 `1292c8d1e54de196ff5ef4e420d067994436ceb4519b50cc91a62384475a1779`. Supplemental `all-nested-module-census.json` also records 16 nested-module files (3765 total); this is a scope difference, not source drift.
- `receipt.json`, `status.txt`, and `evidence-manifest.json` bind exact identities. No native/other worktree was modified. Native WIP, protected original Q32 fixture, two valuelog foreign tests and column-demo foreign source remain outside this lease. Main/ordinary baseline is readonly; current native representation remains open, no native runtime authorized.

## Current seven-path causal repair

1. `TreeDB/db/primary_arena_owner_v5.go`
2. `TreeDB/db/primary_arena_release_work.go`
3. `TreeDB/db/primary_arena_release_work_test.go`
4. `TreeDB/db/vacuum_online.go`
5. `TreeDB/db/durable_primary_rebuild_v5.go`
6. NEW `TreeDB/db/vacuum_shared_base.go`
7. NEW `TreeDB/db/vacuum_shared_base_test.go`

Nonlast ordinary owner release consumes its scalar edge under the existing owner mutex and returns without allocating a physical continuation. Possible last release uses the existing physical helper/receipt; common decrement and sticky error semantics remain shared. Exact zero-allocation and sticky sibling-error controls pass.

Vacuum rebuilds each exact immutable DATA base once per replacement construction, then applies each effective PRIMARY delta using existing fresh zipper COW. The cache is construction-local (four fixed entries), not a pool/global cache. Source pages/entry aliases remain pinned until synchronous Apply finishes; Batch.SetOps borrows bytes. No old-arena component ref is passed into the new arena. Tail and recapture frees are deferred. Final actual live union covers every accepted user/system/collection root; proven dead construction pages are passed into the **adopted canonical freelist generation**, not transient legacy Free. Required current/fallback/direct-parent roots stay independent. The recapture test proves retained versions survive later allocator reuse.

## Latest actual checks

Pinned toolchain: `B/toolchains/go1.26.3-from-185-20261014/bin/go`; GOROOT same, GOWORK=off, GOTOOLCHAIN=local, GOPROXY=off, GOSUMDB=off, CGO_ENABLED=1, GOMAXPROCS=4. Each normal command uses -p=1/-count=1/-timeout=90s and outer120s process-group termination. Full argv/env/raw/exit and before/after identities are in `ER/vacuum-shared-base-repair`; handoff `latest-controls/` copies them. Inputs/HEAD/index stayed identical in all three completed behavioral invocations.

- **PASS**, exit0/no timeout, 19.113s build+run: `go test -p=1 ./TreeDB/db -run '^(TestVacuumSharedBaseRetainsVersionsAcrossRecapture|TestPrimaryOwnerOrdinaryNonlastHasNoUnadmittedReceiptAllocation|TestPrimaryOwnerOrdinaryNonlastPreservesStickySiblingError|TestPrimaryOwnerWorkActualArenaNamespaceAndExternalContainingReceipt)$' -count=1 -timeout=90s -v` (`normal-db-corrected.*`). Four actual matching tests ran. An earlier `normal-db.*` was build-only failure (narrow interface lacked FreeMany); preserved, not behavioral RED. Corrected nil-session fallback uses maintained bulk interface when supported, otherwise existing scalar Free.
- **PASS**, exit0/no timeout, 15.502s: `go test -p=1 ./TreeDB -run '^TestPublicVacuumIndexOnlineHighDebtShrinkAndReopen$' -count=1 -timeout=90s -v` (`public-shrink.*`). ORIGINAL fixture and 40% shrink target unchanged. New exact shrink percentage was not logged; do not invent it.
- **RED**, exit1/no timeout, 21.564s: `go test -p=1 ./TreeDB/collections -run '^TestColumnPhysicalQuerySerialSidecarAllocationBudgetM1634$' -count=1 -timeout=90s -v` (`public-column.*`). Every shape exactly +1 allocation: q1 27>26, q2 46>45, q3 16>15, q4a/q4b 42>41, q5 43>42, q4b-metadata40>39, q5-metadata40>39. Guard unchanged. Nonlast fix does **not** explain remaining column cost. Stopped here, runtime released; no compiler escape/profile/benchmark follow-up was run.

Original vacuum observation is immutable at `ER/vacuum-original-structural-observation`: original failure 4,980,736→3,702,784B (25.6579%); three versions shared exactly297 source DATA pages but produced891 distinct output pages. This established the sharing mechanism, not a changed shrink threshold. Current original public test now passes.

## Prior coherent batch and evidence

- `ER/lifecycle-platform-candidate`: complete authored43 snapshot, controls and remaining-CI inventory. Shared consumed admission lineage advances with pending removal; durable WaitThrough/ACK still waits original terminal completion. Queued/running remap uses original File custody and joins before disposal, preserving aliases/accounting. Readonly owned ref counts, stable sync identity, Windows stable-sync child duplication, typed macOS oversized work and actual owned PRIMARY sync activation covered. Normal/race controls including actual Mongo concurrency passed; platform runtime still needs hosted proof.
- `ER/lifecycle-platform-guard`: immutable18-sample short public guard against exact5bca. Get318.4→324.6ns (+1.947%,0alloc); DATA21376→20636ns (-3.462%),31944→31503B,127→104objects; PRIMARY143491→147527ns (+2.813%),279177→226888B,474→392objects. All strict guard criteria passed. Previous E2 +5.146% PRIMARY RED preserved. These guards precede current vacuum/nonlast edits; no claim of final-current guard.
- `ER/platform-caller-fixture-repair`: complete19-path compatible batch and actual focused results. Includes scoped-token inspection for selected recovery Sets, true four-root capsule coverage, Windows capability preflight before fresh offline effects, accurate supported injection/closed-handle/mmap oracles, bounded analyzer/caller inventory, actual compaction source deletion/reopen assertions. Do not blanket-label all CI failures stale fixtures.
- Durable prior evidence refs root owns: `38787edc9868c41f7d7019d573fe98624001e9a5` compact metadata, `1fbe97de91ee70f4045cf1d56f686d0095846094` PRIMARY scan, earlier packets recorded by root. Root retains current43 publication identity. Native source snapshots are separate; do not transplant incomplete graft/BODY drafts into ordinary.

## Remaining priorities / exact hypotheses

1. **Column +1 allocation is unresolved.** Source-only audit traced public RunColumnPhysicalQuery→prepare snapshot→AcquireSnapshot/deferred Close. Actual exported Snapshot and three iterator handles are independently retained and intentionally not pooled. Nonlast owner now zeroalloc; index wrapper and manager release use existing embedded controls. Possible rootTrees/iterator/capture escape remains unproven. Need bounded source/escape attribution before a fix; compiler-report request was pending when handoff stop arrived. Do not remove required independent receipts or relax budgets.
2. Repair saved-format fixture through maintained fresh feature config before pager/index creation: `createSavedPrimaryFormatV5` currently lacks `freelist_patricia_v2` marker. Preserve optional dependency_directory_v2. `TestPrimaryDirectoryOrdinaryCapacityAndDeferredPhysicalExport` really fails exported reopen at line242 for the same missing marker; copy actual source format metadata/maintained config into exported fixture before restoring index, not weaken Open. No edits made yet.
3. **COWPublicEmptyCheckpoint unused rotated vlog retention**: latest complete CI reconstruction says no_wal_fast PASSES; both collection-WAL profiles fail actual deletion of `value-l255-000002.log` after5s although vacuum/GC succeed with zero debt. Exact holder still unknown; inspect actual retained resources/pins. Do not use disproven append-count hypothesis or waive deletion. Root has full raw local `.../ubuntu-{3,latest-2}-Test...-full-raw.txt` and will transfer it.
4. Collection pack retention must respect FOUR recoverable roots (both slot current records plus embedded parents); one ForceCommit after pack can legitimately retain prepack source. Prove exact roots/old-reader and further packed publication before changing deletion oracle.
5. Finish remaining M4 policy/debt, power-loss/recoverable reuse/meta closure/CWAL poison, format/fault/platform and hosted CI failures as one coherent batch. Original b3 CI inventory at `B/evidence/ordinary-5111-final-base-20261015/final-all-failure-diagnosis.json`; root local complete logs carry additional facts. No broad suite until focused viable source/cost boundary; no timing without quiet lease. Preserve all40%/allocation limits and genuine corruption/fault coverage.
6. Root owns publication/PR5127/GH/CI and will reconcile final source. No one-off push per helper. Current source is WIP, not full ordinary or native acceptance.

## Safe resume

First verify HEAD/index/source manifest and transferred ownership; read applicable AGENTS, refreshed executor/GoPerf/arena references. Existing eligible arena/scratch reuse is default, but exported retained handles and persistent/shared outputs cannot be reset at temporary batch end. New coordinator should obtain root's checkpoint/ref and transfer decision, then continue only the ordinary source lease. No active command needs resuming or killing. No new Go, repair, deletion, or Git mutation was performed during this handoff.
