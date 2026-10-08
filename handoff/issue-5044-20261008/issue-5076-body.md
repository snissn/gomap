<!-- cow-current-host-migration:start -->
**Migration handoff:** PR #5080 OPEN, **04fd2951ac56af0e0473c4037e71f2bd81612f29**, **codex/cow-c3-mvcc-views**. CI snapshot 2026-10-08T18:45:15.450393+00:00: {"SUCCESS": 55}. Refresh exact-head required aggregate/policy before a future merge. Current Codex [clean](https://github.com/snissn/gomap/pull/5080#issuecomment-6066292021); all3 actual CodeRabbit threads dispositioned/resolved. No duplicate review or handoff merge.

Original756-child evidence/all167 flags [accepted within scoped decision](https://github.com/snissn/gomap/issues/5076#issuecomment-6065636479). Doc-only04fd preserves9928 other blob/mode pairs and6083 compiled inputs; raw identities retained. [Recovery receipts](https://github.com/snissn/gomap/blob/3b671cc377f882f5bfa0a3cd1fd51b84d183a915/handoff/issue-5044-20261008/README.md), [whole graph](https://github.com/snissn/gomap/issues/5044). No global-performance/parent completion claim.
<!-- cow-current-host-migration:end -->

<!-- current-readiness-execution:start -->
C3 current head 04fd2951ac56af0e0473c4037e71f2bd81612f29 contains the independently reviewed benchmark-doc correction only. All9928 other tracked blob/mode pairs are unchanged. Fresh CI is running; prior3f passed all55 CI checks, including required TreeDB aggregate; the original Windows download failure and its single successful targeted retry are preserved.

The original hosted54-cell/756-child qualification completed naturally, and the sealed final artifact passed independent structural audit: all756 leaves,23 supervisors,720 raw metric summaries and all54 work/ACK/layout/activity ledgers verified. Source remains baseline2d6b07f versus actual candidate8d8806b, workflow3c33dd77. The separately verified6083-entry compiled-input equality applies evidence through3f to current04fd without relabeling execution.

[Authoritative scoped performance acceptance](https://github.com/snissn/gomap/issues/5076#issuecomment-6065636479): root explicitly accepted24 nonnoisy adverse pairs as measured costs under this issue's coordinator-acceptance clause. All143 noisy pairs remain inconclusive, including33 adverse/noisy pairs; all167 unique flags are individually retained. Concurrent COW has zero adverse flags and52 nonnoisy improvement candidates; forced-pointer reader phase rates improve in all3 ACK profiles. No global no-regression, timing causation, production memory plateau, native/prune, sustained C4 or parent completion claim.

The separate current raw-path gate passed all five original bounds with independent actual binary/custody review. [Current raw-path acceptance](https://github.com/snissn/gomap/pull/5080#issuecomment-6064118942) retains the historical failed gate and unchanged measured binaries; cause of run variance remains unresolved.

Production/source/evidence reviews are accepted and all owned readers/processes are released. Prior3f hosted Codex review was clean; all3 CodeRabbit findings were resolved with one doc fix and two accepted source-backed dispositions. Fresh current04fd CI, mature hosted Codex review and authorized merge remain.

C4 capture-root provenance fix is published as100ca3f775a947ff69564b97ac0c0b86a1dd2cc0; independent27 source/control checks passed and the actual review finding is resolved. Its current CI and genuinev3 finite construction remain pending. C4 tooling acceptance can follow C3 landing before whole Native completion; expensive native/sustained collection retains its own prerequisites. Active #5111 native owner still has priority on185; no new runtime grant is assumed.
<!-- current-readiness-execution:end -->

Parent: https://github.com/snissn/gomap/issues/5044

## Problem and outcome

The landed C2 immutable cut already publishes a whole batch atomically, but Store.CommitGroupAt takes an exclusive Store lock through the ordinary write. Actual Store.GetAt and IterateVersions therefore wait before reaching the engine's immutable cut. Deliver a focused production PR that lets admitted readers finish on their captured old cut while a grouped writer is preparing, and releases Store floor admission before value materialization. This milestone can be implemented from merged C2 without the unmerged M7 pruning producer.

Deliverable: one Gomap PR, including production dispatch, tests, matched public-path allocation/performance evidence and canonical contract updates. Acceptance owner: /root, coordinator of #5044. Required merged production predecessor: #5046 / PR #5069; required landed measurement prerequisite: #5078 before expensive retained collection and final acceptance. Performance class: performance-objective for actual reader progress, performance-sensitive for existing sequential public paths. Completing this node does not complete #5047, #5048 or the parent.

## Source preflight and minimal design

Verified base: 7649857532521db69ff4cdcf236a11377c85ab3d; TreeDB and module inputs equal the retained C3 diagnostic base 3cfe2ad896cad1818f72d3d14c74e548fecc1657. Reuse [C2 cut capture](https://github.com/snissn/gomap/blob/7649857532521db69ff4cdcf236a11377c85ab3d/TreeDB/caching/cow_cut.go#L263), [whole-cut publication](https://github.com/snissn/gomap/blob/7649857532521db69ff4cdcf236a11377c85ab3d/TreeDB/caching/cow_cut.go#L431), the existing owned COW successor, encoded MVCC keys and the Store floor authority. The unnecessary serialization is the [Store grouped fence](https://github.com/snissn/gomap/blob/7649857532521db69ff4cdcf236a11377c85ab3d/TreeDB/mvcc/mvcc.go#L236). The public DB's existing SeekGEVersionRange method also routes legacy implementations, so method presence alone does not prove atomic publication. Reuse the existing snapshot owner and finite engine admission; introduce only the narrow semantic capability needed by actual callers. No generic view framework, separate timestamp codec, prune producer or GC registry.

1. Qualify the resolved engine through an explicit whole-call atomic publication and error-returning coherent-cut capability. Dispatch by that capability, never memtable name or broad successor-method presence. Unsupported and batch-only adapters preserve their existing fences.
2. Qualified grouped writes retain shared Store floor admission through the existing Write/WriteSync call. A floor advance still excludes already-admitted commits until their ordinary ACK completes. Preserve duplicate checks, mode ACKs and atomic whole-call/group publication.
3. GetAt and IterateVersions validate the cached floor and pin one physical cut under Store read admission, then release that admission before iterator construction, seek, owned-byte materialization, decode or callbacks. A point read must seek on its admitted snapshot rather than reacquire a newer DB cut. Reuse the existing COW snapshot successor rather than imposing a general merging iterator on its optimized case.
4. Independent GetAt calls remain independently fresh. Physical cut identity and application timestamp remain distinct: admitted snapshots retain earlier same-ts data and exclude later historical inserts. Preserve exact-key bounds, tombstones, empty values, floor equality, single-Store namespace ownership and resource lifetimes.
5. Unsupported COW maintenance refuses before floor/WAL effects. The current PruneVersions re-syncs its floor before discovering that COW ReverseIterator is unsupported; add a narrow eligibility preflight, with adapters preserving their existing behavior. Do not implement or activate bounded pruning here. This refusal is not maintenance qualification.

## Evidence and acceptance

- Reuse the retained actual grouped-publication RED diagnostic in #5047's [reader-progress packet](https://github.com/snissn/gomap/issues/5047#issuecomment-6023101826): six normal and six race cases, command-WAL durable/relaxed and NoWAL, inline/forced-pointer values. Keep initial failure evidence and convert the actual desired-reader-progress test to PASS without weakening its publication window or assertions.
- Add discriminating floor admission, captured-cut materialization, all-version exact-key, same-ts/late-history/tombstone, fallback adapter, admission failure, unsupported-prune-before-effects and Close/error cleanup checks. Exercise accepted whole-group visibility, checkpoint/reopen and pointer lifetime. Tests must distinguish a fresh post-admission cut and fence removal without producer capability.
- Run affected package correctness/race tests and applicable public contract/lifecycle tests on frozen candidate inputs. Preserve the ordinary ACK contract, floor-before-delete/no-resurrection, old-view/vlog/dictionary lifetime and existing unsupported feature behavior. M7 and final graph gates remain pending.
- Audit allocation/copy sites and their ownership/frequency on changed hot paths; minimize temporary work with existing successor and snapshot primitives. Collect comparable sequential public CommitAt+GetAt and CommitGroupAt+IterateVersions throughput, bytes/op and allocs/op across the three durability profiles and inline/pointer values. Add an actual public concurrency comparison that demonstrates reader progress while group preparation is active; retain all repetitions and measurement boundaries. A paused-writer functional diagnostic is not an ops/sec claim. Material regressions require a repair or explicit coordinator acceptance with evidence; no parent benefit claim from this subset alone.
- Update TreeDB/docs/spec/contracts.md, cow-cache-publication.md, dgraph-mvcc-readiness-3673.md, verification.md and docs/contracts/CONCURRENCY.md and any affected public capability examples in the same PR. State the remaining unsupported prune/final qualification boundary clearly.
- Use an isolated topic worktree, exact GPT-6.1 Sol worker/reviewer routing, independent mature source/evidence review, current required CI and hosted Codex review before coordinator merge. Preserve unrelated dirty checkouts and the active M7 worktree/runner.

## Ownership and successor

This node owns Store read/floor admission and the narrow public/cached read capability/refusal adapter. #5047 consumes this merged milestone and continues to own integrated maintenance/replay and final MVCC lifecycle qualification once #4878's accepted bounded maintenance contract is applicable. #4878 retains the native pruning producer, allocator/index publication, floor/delete recovery and fixed-work authority; no competing shared-contract writer is created. C4 #5048 still requires #5047 and usable #5019 evidence tooling. All Q32/1MiB, durability, retention and public performance gates stay unchanged.

- [x] Actual desired reader-progress invariant passes through the public Store.
- [x] Production capability/floor/captured-cut/refusal behavior and risk-relevant tests delivered.
- [x] Allocation audit and matched public-path evidence accepted; all material adverse results explicitly accepted as documented measured costs under the coordinator clause, with noise retained as inconclusive.
- [x] Canonical docs and affected examples updated.
- [ ] Independent review, hosted review and current required CI pass; exact merged output accepted.

<!-- cow-c3-shared-functional-actual-20261008 -->
Historical checkpoint; superseded by current accepted twelve-command construction status above.
Current C3 head `8d8806b495422e44f7802a96f5737f536a3fb2f0`: the approved Linux185 shared functional attempt has ended and all owned test/controller/transfer processes are joined, with no cancellation signals. All four original compiler/environment/normal/race commands exited zero; normal and race each passed the exact 16 required tests. The evidence controller subsequently returned `CLOSED_FAILURE`: its parser applied test-package validation to 2,674 legitimate Go `build-output` diagnostics emitted by `go test -json -race -x`.

The original failed packet, raw receipts and output remain unchanged. Independent audit accepted preservation and ownership release only (`10c3b97915e97f4983a6b0bbc43267ad56605d6b4bb65a00d58d400189c960ad`); a strict parser correction and separately identified offline rectification/import artifact are under review. No test rerun has started. Quiet construction of fresh benchmark binaries, the complete 54-cell/756-process matched qualification, performance disposition, and native successor qualification remain pending with their original criteria. [Go build JSON format](https://pkg.go.dev/cmd/go#hdr-Build__json_encoding).
<!-- /cow-c3-shared-functional-actual-20261008 -->


<!-- cow-c3-functional-posthoc-accepted-20261008 -->
Historical checkpoint; superseded by current accepted twelve-command construction status above.

Strict functional-parser correction and separately derived evidence are independently accepted. The original four compiler/environment/normal/race commands all joined with exit 0; both normal and race output contain all 16 expected top-level MVCC tests. The original controller remains `CLOSED_FAILURE` and its raw packet is unchanged; there was no rerun or fabricated original result. Source review `fd222019f2bb6e1c516f377f7a3f67e85218bb885e8c5ea1409fda031a888f58` and actual evidence review `ef66e720d6b1988f9a387fc7dc79121112b359226c0ec7820ac5ad7671d547f1` bind the corrected strict parser and sidecar `63d8ff3947c01a141de4a82da4c9041ed628cf059c03f99e159bb350876b8a13`. All reviewers released their readers. This accepts shared nonperformance functional evidence only. Remaining fresh eight-command construction is under final transport review and quiet-host coordination; fresh 54-cell/756-process performance qualification and native #5111 -> #4878 -> #5047 -> #5048 gates remain open.
