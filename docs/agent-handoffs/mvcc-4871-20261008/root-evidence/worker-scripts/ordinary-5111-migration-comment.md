## Migration handoff — stopped WIP, 2026-10-08

User-requested handoff/spin-down supersedes implementation continuation. Source/test/performance activity is stopped. **Draft PR [#5127](https://github.com/snissn/gomap/pull/5127) preserves the exact current ordinary work; it is not dependency-ready or mergeable.** No new repair loop, tests, performance collection, AI review, or merge was started after the instruction.

### Durable source identity
- Pushed head: `d4cbf23f9333eed79718845d3ac253c5d7fc5513`
- Parent: `2d6b07f58902537cfe8d2b3e3a8c3c9ba0f3c07c`; tree: `e76c23955124cc4cc10ef59ad6b8490bb4066f2a`
- Branch: `codex/5111-ordinary-data-primary-publication`
- Sole lane: `mikers@192.168.0.185:/home/mikers/benchmarks/mvcc-ordinary-5111-root-20261008/source`
- 247 paths committed: 246 intended source/doc paths plus `.github/ci/ci_impact.json` discovery fingerprint. The complete file list is the pushed Git diff; no intended untracked source remains.
- Pre-stage owned-file map: `ordinary-5111-migration-handoff/owned-source-before-stage.json`, SHA-256 `6ab3c054ceb600035886c75636f8790bf9bc444aceb5292b48e0908920195d15`.
- Every one of those 246 source/doc bindings is unchanged after commit. Local checkout clean; push, ls-remote and independent GitHub API agree. Commit is a WIP preservation boundary, not a validation freeze.

### Final naturally joined command — RED, no subsequent repair
Output directory under the lane's parent: `ordinary-allocation-core-normal`.
```
go test -p=2 -v ./TreeDB/internal/retainedalloc ./TreeDB/internal/rootpublication ./TreeDB/internal/primaryarena -count=1 -timeout=180s
```
Exact wrapper: `/home/mikers/benchmarks/mvcc-ordinary-5111-root-20261008/run-ordinary-5111-bound.py`; exact executable `/home/mikers/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go`. Env: matching GOROOT, GOMAXPROCS=2, CGO_ENABLED=1, GOPATH=/home/mikers/go, GOWORK=off, GOENV=off, GOTOOLCHAIN=local, GOPROXY=off, GOSUMDB=off, GOCACHE=/home/mikers/.cache/go-build, GOMODCACHE=/home/mikers/go/pkg/mod.

Session 7886 naturally joined exit 1. retainedalloc and primaryarena pass; rootpublication has three actual failures, preserved without assertion changes:
1. `TestResourceKindBackingSharedSetAndScopedBorrow`, resource_allocation_test.go:407: `set release retired a scoped borrower`.
2. `TestResourceOwnedKindMergeBuilderTransferAndLastBorrow`, resource_kind_merge_allocation_test.go:110: `public release stole borrowed closure`.
3. `TestOwnedBuilderAddCoalescesAndRefusesBeforeTransfer`, resource_set_allocation_test.go:124: `actual repeated identity did not coalesce`.

Raw SHA-256: `06c394861466b760eecace3481b077e857b20c38ca73de3e37c6f956a5ef85c3`.
Result SHA-256: `b27746dde8bdd93bc0f8ca876590f3233a1b92e71bdfa2867f7b9448b2465f51`.
Equal before/after 10040-path source-map SHA-256: `7ad592208817c1c3959c6296aae75fa49b5d2c931a8029c0a9118108a72f5203`.
The CI metadata refresh happened afterward; all product/test/doc bytes are unchanged. Earlier summaries reporting only two failures omitted the scoped-kind case; this complete raw enumeration supersedes them.

Latest preceding focused race: `ordinary-primary-child-seal-v5-vacuum-fixed`, six named cases (5 DB + 1 rootpublication), exit 0. Raw `c4bfa62b63fa9160dbac1f62f0edd2bb9b8d6ef274bb2e0f191f7cc22a95b006`; equal source map `0015e3ad573634af6a82f2e1f0fde1c78453fd93815db9656be7b4ee7cee6033`. Covers private child-first sealing/cycle/admission refusal, V5 vacuum held Snapshot/cut/independent slots, record/manifest scratch refusal, rebind/older slot, and multipage manifest encoding. Its initial fixture admission failure remains in `ordinary-primary-child-seal-v5-vacuum-normal` (raw `baf7e95cc11d92af9ad5426619cd0b2d6c1d074b960718b1a236b1b663a3fa06`). These are component bindings, not all-candidate acceptance.

### Implemented boundary and remaining closure
The pushed ordinary route includes immutable logical-reader roots versus immutable promotion/slot roots within the SAME transaction, joint DATA/PRIMARY COMMIT recovery/vacuum, independently retained cuts/readers/ValuePtrs, typed ordinary admission/floor/queued-stop ownership, and actual arena + registry retained-allocation ownership. Canonical docs and API pre-alpha notes are included. Native witness-only/protected-floor/private Finish APIs remain deferred in frozen .111 source; this PR does not invent their qualification.

All 19 groups still need one final source-bound completeness inventory and affected closure validation. Existing component evidence is reusable only at its recorded bindings:
1. Arena address indexes/read roots.
2. Arena bank/chunk growth and logical-root custody.
3. Pager lookup/growth backing and mapping boundaries.
4. DATA prepared-root ownership.
5. Transaction/candidate/member/control storage.
6. Runtime current/slot/retired arrays and callbacks.
7. Promotion/capsule/recovery images, selection and output capacity.
8. Seal/prefix construction and callback lifetimes.
9. Unlocked retirement and exact failure custody.
10. Startup/handoff/dictionary SAME-budget fixed-pair producer/borrow adoption.
11. Namespace/file-owner backing and original proofs.
12. PRIMARY index token/operation descriptors.
13. Namespace token/lease/sync operands.
14. Same identity registry typed backing, diagnostics and cleanup.
15. Dependency-directory/lease custody and pre-open transfer.
16. Manifest encoding/loading/rebind and actual scratch overlap.
17. Builder/set/Clone/import/scope ownership — current REDs are here and in group18.
18. Rope/persistent exact RID/history/proof backing and last alias.
19. Release buffers, original consumed/deferred/completed/debt outcomes and scoped diagnostics.

Most recent source additions front-loaded private manifest/dependency bank custody with a producer-admitted contiguous ref ledger and intrusive same-owner failure edge; iterative child-first sealing admits actual frames before effects and rejects cycles; record/manifest/rebind codec scratch uses the canonical shared core; recovered leases use an intrusive pre-open list. The V5 dependency allocator's release error is now joined outward, with surviving failure custody at the same arena owner. Copied V5/V6 runtime metadata is admitted at its actual root. These still require coherent integration/race applicability review; no native cost credit follows.

Next action: inspect the three REDs causally at this exact head before touching assertions. Then finish the 19-group exact constructor/capacity/overlap/caller/alias/release/failure inventory and selected production closure; run the affected normal/race/recovery/GC/Close/physical-cut/provider suite once at stable source. Resolve any still-needed original outcome ABI narrowly without adopting unchecked R1 work. Reconcile one actual current-main plus landed #5118 window and refresh CI. Current main had only #5115 tooling beyond pinned baseline; #5080 is OPEN foreign C3 work, not an adopted merge dependency. Preserve read-cut/prune-preflight ABI if/when applicable.

### Unfinished qualification gates
- Current core is RED. No final all19 closure, full affected normal/race, exact-head required CI, independent review, mature PR, or root acceptance.
- CI inventory refresh and --check passed, preserving 14 workflows/56 original members; only the discovery source fingerprint changed. Contract test commands were deferred because spin-down prohibits new tests.
- #5118 owns the independently reviewed/landed existing-harness PRIMARY selector/report prerequisite. No expensive retained collection until harness and product identities are frozen.
- Existing-path matched ordinary point/batch/checkpoint/vacuum costs, real Store DURABLE/RELAXED profiles, enabled incremental allocations/read lookup/root scaling/cohort retention/DATA+PRIMARY physical bytes remain unqualified. Unmatched/noisy historical diagnostics are not acceptance.
- #4878 native 32R/1MiB, input32/resource16/file17/two-index-files, SAME Request rank through materialization/third override, original33/80/4096, Accepted/once-ACK/finite Finish, performance and M8 remain unchanged/unqualified. No native public glue is authorized by this handoff.
- Root owns graph disposition, final readiness and merge. No AI reviews or merge were requested.

### Retained paths and host release
All paths below remain intentionally retained; no cleanup/discard occurred:
- .185 source above and matching baseline `/home/mikers/benchmarks/mvcc-ordinary-5111-root-20261008/baseline-main-2d6`.
- .185 all evidence directories/scripts under `/home/mikers/benchmarks/mvcc-ordinary-5111-root-20261008`, including `ordinary-5111-migration-handoff` with pre-stage map, copied final raw/result, post-commit source check and CI refresh receipt.
- Shared build/module caches `/home/mikers/.cache/go-build` and `/home/mikers/go/pkg/mod`: preserved, not claimed disposable.
- .111 frozen native source `/home/mikers/benchmarks/mvcc-bounded-prune-4878-20261001/integration-terminal-primary-root-20261007`, donors, original fixtures and all frozen negative/component packets: unchanged/protected.
- Root authority/evidence `/home/mikers/benchmarks/mvcc-architecture-4871-root-state-20261001/decisions/m7-whole-publication-output-reassessment-root-20261007`, plus root mirrors/external evidence.
- Local small handoff body files: `/Volumes/FlashDrive/gomap-mvcc-4871-root-evidence-20261008/worker-scripts/ordinary-5111-wip-pr-body.md` and `ordinary-5111-migration-comment.md`.

Exact released disposable paths: **none**. No owned Go/test/perf/controller remains. Git push session93978 and draft creation68163 naturally joined 0; only bounded final GitHub verification/metadata transports were used for this handoff. Another agent must obtain the next host/lane grant before resuming; source/runtime is stopped for migration.
