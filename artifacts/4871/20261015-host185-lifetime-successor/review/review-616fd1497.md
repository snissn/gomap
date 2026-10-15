# Independent source and measurement-harness review — 616fd1497

**ACCEPT for exact source and harness.** No remaining blocking source finding was identified in the reviewed candidate. This is not hosted CI acceptance, performance acceptance, mergeability, merge authorization, native/Q32 qualification, or final graph completion.

- Review mode: `INTERNAL_SUBAGENT_REVIEW`.
- Reviewer: `/root/ordinary_review`; actual explicitly routed model `gpt-6.1-sol`, reasoning `high`; no construction role or recursive delegation.
- Repository/PR: `snissn/gomap` #5127.
- Base: `5bca87ffe6e1bfba2d693264d2dc82936adc234f`.
- Exact local head: `616fd1497b3e483f40bfbf3cefe4538d250bb33a`; tree: `ff0930180692ab50e08b346b83e7d7b19a5cb74c`.
- Candidate edits by reviewer: **false**. No tests, builds, benchmarks, services, Git mutations, GitHub actions, comments, or pushes performed by reviewer. Only review artifacts were written.
- Hosted review unavailability: workstream requires exact Sol6.1 routing; hosted manual review cannot pin that model and was not requested. No quota error, provider refusal, hosted response URL or hosted review result is invented.
- Candidate is local/unpushed at review; new hosted results are necessarily pending. Prior exact-head rejection records are preserved.

## Finding dispositions

| Finding | Exact-candidate source disposition |
|---|---|
| R1: certificate cache before accepted root custody | FIXED, still valid. Cache assignment follows successful SAME visible install/activation. Failed preparation/activation does not replace accepted cache. Accepted root has its conservative proof allowance before old-root withdrawal. Terminal/runtime cache clearing precedes final resource withdrawal. Original zipper construction remains captured in phase-aware abort custody before fallible descriptor preparation. Re-read activation, constructor handoff and terminal paths; unchanged from previously reviewed repair. |
| R2: closed PRIMARY iterator retained private borrowed aliases | FIXED, still valid. Close clears private directory/image/pager/callback/child aliases before child Close, retaining Domain/error semantics. Bound iterator clears inner before owner finalization. Exported handles remain unpooled. |
| R3: overlapping recovery ReadAll backing absent from admission | FIXED, still valid. Single preadmitted 4141-byte ReadFull backing includes oversize sentinel. Short-read normalization and maximum decision-size rejection remain. No new recovery allocation or physical-custody change in this batch. |
| R4: incomplete Set publication/capture lifetime | FIXED. Exact selected Set is acquired while publication exclusion still holds, before either capture lock is released. All replacing publishers respect rootReuse capture exclusion. Actual last consumer releases the exact selected Set, joining fallible release into operation return. Online dictionary and carry readers now use owned Snapshots; no shallow current-state fallback. Final mutable Set scrub remains intact. |
| R5: topology-only state copies dropped primaryRoot | FIXED. Audit and both leaf-generation publication branches carry `oldState.primaryRoot` unchanged. They neither acquire nor drop a duplicate current edge. Original actual logical publication and terminal withdrawal remain authoritative. |

No new blocking finding. The named architecture disposition from the a8 review is implemented: shared ordinary/native Set final cleanup clears mutable backing; therefore capture ownership must precede unlock across the complete publisher/consumer inventory, and topology-only copies transfer the SAME current-root edge.

## Publisher and consumer coverage

Read all 31 successor diffs, production helpers and adjacent callers. Independently searched all production `state.Store`, `state.Load`, `db.State`, and `.ValueLogSet` uses in TreeDB/db. The retained `candidate616-state-search.txt` contains the state observation inventory. The worker's broader callsite disposition and searches were read as cross-checks, not substituted for source inspection.

| All nine production state.Store sites | Reviewed exclusion and ownership |
|---|---|
| Queued visible activation | Existing rootReuse gate spans enqueue/activation; db.mu installs selected state and snapshot view. Accepted logical root retains its original current edge. |
| Legacy finalizeCommit | Existing rootReuse then db.mu gate spans publication. PRIMARY path remains queued; legacy DATA path retains original disposal. |
| RefreshValueLogSet | rootReuse then db.mu; fresh manager Set and SAME primaryRoot; old publication Set released after replacement. |
| publishValueLogSetNoRefresh | Same capture exclusion/current-edge preservation; fresh retained Set and original discovery fallback. |
| Online vacuum swap | Added rootReuse before db.mu around state/index/view replacement. Exact old Set remains owned until original later release. Original old index/PRIMARY physical receipt paths remain. |
| Compact audit topology | Added rootReuse before db.mu; original retained manager Set consumed once; same-topology no-op releases input. SAME primaryRoot preserved. |
| Leaf topology | Added rootReuse before db.mu. Same-Set branch takes the replacement publication ref before releasing prior ref; refresh branch obtains fresh manager Set. Both retain SAME primaryRoot. |
| Two read-only initial stores | Private initialization before DB escapes; initial Sets already owned. No concurrent published capture. |

Consumer tracing covered optimistic/serialized user rewrite, system/collection rewrite, CompactIndex, ordered-root merge/build, compact audit topology map comparisons, online dictionary scans/training, compact carried-plan sizing and carry.advance. New helper pins are acquired under rootReuse read + db.mu read, and deferred releases occur after actual tree/zipper/file use; named returned errors retain release errors. Existing writer/teardown/maintenance scopes retain selected index/pager lifetime. New rootReuse gates use the existing order, and leaf topology callers release db.mu before entering; ordered post-activation itself does not invoke leaf topology while the build gate is held.

Dictionary scan/training now receives the SAME Snapshot's `state` and `idx.pager`; leaf data comes from that state Set rather than a later manager view. Capture failure returns ErrClosed. Dictionary callbacks may publish after capture because no capture mutex is retained across their work. Offline dictionary input retains its original exclusive source pager/Set.

Carry plan uses a live Snapshot through sizing. carry.advance compares scalar/immutable published basis with the captured basis, invalidates mismatch, and sizes created files from the captured Set; it joins actual Snapshot.Close. Borrowed result state is subsequently used only for scalar/immutable basis.

Safe exceptions remain distinct: Snapshot-backed statistics, fragmentation, maintenance reachability, leaf-source scans, bounded readers and stable-resource readers; explicitly acquired fresh candidate/rebinding Sets; exclusive offline sources; initial unpublished construction; scalar/immutable identity-only checks. `DB.State` is documented shallow borrowed metadata, not an owning read API. ValueReaderForState requires its supplying live Snapshot/explicit retained Set and index. External caching/template callers were traced in prior coordinator evidence to live Snapshots; those are not falsely claimed as new full caller reads in this pass.

## Controls and platform behavior inspected

Read all three new actual-path lifetime tests. Ten consumers interleave real Refresh after exact capture, use real outer-leaf pointers, and select a Set distinct from an older held scan. Tests assert selected refs2 before refresh, refs1 after, successful physical reads, final refs0/Filesnil, and independent old-reader survival. Topology capture tests pause actual bounded admission and run real audit/same-Set/fresh-Set publication; ordinary and admitted readers survive and final retired Set drains. PRIMARY topology tests retain exact admitted metadata, advance slots/parents with six actual writes, distinguish Snapshot invalidation from last iterator ownership, and verify final root/governor/metadata drain.

Admission repair keeps production fences unchanged. The callback starts a real competing Refresh, observes it blocked during capture, refuses original admission before any reader/Set effect, then joins the same Refresh after callback return. It checks original final scrub and successful current-view retry. Same-DB synchronous publish/refresh/close from admission is now explicitly documented as nonreentrant. The callback audit covers actual budget/enrollment callbacks and no repaired callback weakens capture exclusion.

Platform controls invoke actual unsupported operations and require typed refusal plus unchanged pair/state/charge/custody as applicable. Offline controls retain every preexisting persistent input's bytes and no private pair artifacts; supported platforms retain original recovery/cut/fallback assertions. Native directory eligibility uses the actual physical pager's existing bounded-close capability before Bind consumes a ref/phase/charge. Unsupported bounded Bind leaves custody intact and the SAME directory's ordinary completion remains available. Final Set alias scrub is unchanged. Failed physical-close fixtures close retained startup LOCK only as explicit modeled process shutdown after original sticky-error/no-refund/no-retry/LOCK-held assertions.

Power-loss stable observer now filters this actual DB, serializes model Observe/Clone and drains/deactivates callback before restoring. Completed stable/volatile-cut oracles remain. C3 changes add failure-only stage and all-goroutine stacks at the unchanged 10s join; no retry/deadline increase. Windows C3 cause remains unresolved and must be assessed from new runtime evidence.

## New PRIMARY harness pre-review

Read the complete `scripts/mvcc_primary_cost_gate.py`, its seven failure controls, workflow integration, existing checkout guard, and exact committed Go benchmark/lifecycle harness. No timing was collected by this review or by this new harness preparation.

- Requires distinct full 40-hex immutable SHAs, fresh output outside source, clean exact candidate before build, and clean exact candidate/baseline after sampling. The baseline is a new detached worktree at the requested full SHA. Candidate uses its committed harness; only baseline receives a Go overlay containing those SAME retained bytes. Product sources are not overlaid.
- Builds both test binaries outside timing with GOWORK=off/GOMAXPROCS=1, retains both executables, SHA256/size/source SHA, harness/runner hashes, host/toolchain details, argv/status/output hashes and every raw sample. Final binary and harness drift checks remain. Worktree cleanup is in finally; retained executables/results remain external.
- Candidate actual public lifecycle/reopen test must explicitly PASS without SKIP. The unchanged benchmark performs genuine DATA materialization before durable public PRIMARY replacements and reports DATA generation advances. Baseline churn remains runnable; every candidate sample must report zero generation advances/op.
- Fixed eight alternating AB/BA pairs, 128 iterations/sample, CPU0/taskset and GOMAXPROCS1. Parse refuses missing/duplicate/unexpected/nonfinite/negative metrics, wrong counts, failed output and malformed/reordered/incomplete planned samples. Paired timing uses matching pair numbers, not unmatched medians. Limit is <=+5% median paired ns/op delta, with no median B/op or allocs/op growth.
- Five-second 2% external-process CPU preflight runs before samples; measured interval samples /proc every250ms, includes sibling jobs, and retains increments of observed processes after exit/PID reuse. Quiet failure cannot become acceptance. Ancestors and own descendants are excluded. Explicit sampling limit: processes entirely between observations may be missed. This is sampled process interference evidence, not proof of universal host quiescence.
- Command timeout/interruption kills the new child process group and waits for its child before error propagation. Failure retains receipts, partial samples and refusal result. No replacement sample, retry, threshold override or waiver path exists.
- Workflow runs sequentially after the existing raw five comparisons, with the same resolved full base/head. New path filtering/self-check inputs and unconditional artifact retention include the harness. Existing five comparison thresholds are unchanged. The seven controls cover activation, planned sample inventory, independent timing/allocation refusal, parse refusal, pairing, exited-process/PID reuse and own-process exclusion.

This bounded public replacement guard is suitable to collect evidence after this source review. It is not final native/C3/Dgraph qualification, a result for M0 variability, or a completed performance gate.

## Exact identity and evidence limits

Independently hashed all 10,244 paths in `ordinary-lifetime-work/root-candidate-616fd1497.json`: zero mismatches. Compared against worker `freeze-final.json` (10,242 files): differences are exactly `.github/ci/ci_impact.json`, `.github/scripts/test_mvcc_primary_cost_gate.py`, `.github/workflows/treedb-tests.yml`, `scripts/mvcc_primary_cost_gate.py`. All tested product/Go bytes are identical to worker freeze. Verified exact head/tree and clean status at start and completion.

Read HANDOFF, callsite-disposition, admission-callback-audit, failed-check-disposition, test-inventory-final, completion-receipt, harness preparation README/integration receipt. Read final normal/race argv/environment and selected raw named/package results; Windows compile command and inventory exit0. Reviewer did not execute those checks.

| Worker/integration receipt | Scope actually retained |
|---|---|
| final-normal-03 | Freeze-bound PASS: 50 top-level executions/103 subtests; focused DB custody, lifecycle, platform supported-path and original cleanup controls. |
| final-race-db-02 | Freeze-bound count2 PASS: 26 top-level executions/36 subtests; actual consumer/Refresh and topology capture controls. |
| final-race-public-02 | Freeze-bound PASS: 8 top-level executions/49 subtests; public shrink/reopen, 36 cuts, three chunked variants, negative lookup/empty checkpoint and all four C3 cells. Linux result only. |
| windows-all-final-01 | Freeze-bound Windows amd64/CGO-disabled compile of DB, public TreeDB and mvcc; exit0. No Windows runtime proof. |
| Harness integration receipt | Seven new controls plus existing equivalence/pairing/checkout/CI inventory/contracts/headroom recorded PASS. Reviewer read control sources/receipt, did not rerun them. No new timing. |

Preserved earlier RED/inexact runs are not relabeled final passes. New exact-head hosted platform/race/correctness checks are pending. Predecessor a8 43 success/10 failure/3 cancellation and its M0 p99 CV0.245>0.10 are predecessor results, not qualification or waivers. Admission-callback cancellation attribution is a source-supported self-deadlock repair; Windows C3 cause remains unattributed. No qualifying host185 local timing exists; historical host111/older raw timings do not qualify616. Fresh original-five raw gate, M0 variability guard and new PRIMARY gate remain mandatory for any applicable readiness claim.

## Review scope and release

This continues the independent whole-foundation review preserved in `review-f0415773.md/json` and `review-a8c52ed66.md/json`, with renewed depth over all 31 current successor paths and complete Set/state publisher inventory. Base-to-head is 534 changed files /86,309 insertions /5,235 deletions; no claim to have read every line. Prior 19-group allocation/persistent-vlog/current-and-slot-parent/SAME allocator/transaction/publisher/CWAL/physical-completion contract tracing remains applicable to unchanged bytes. The newly added pins are per maintenance consumer, not per foreground item; no new global pool, quota domain, generic budget increase or per-foreground checkpoint was introduced. Canonical legacy DATA allocations are not universally claimed PRIMARY-admitted.

Exact-head AGENTS inventory is root, TreeDB and separately scoped unchanged HashDB; root/TreeDB and CONTRIBUTING apply to this diff. Task-local mergeable/internal fallback and Go performance guidance were applied. No applicable hard review cap was found. Effective review fallback records the actual model and does not waive hosted CI, performance, authorization or remaining graph work.

**Source/harness disposition: ACCEPT. Remaining blocking source findings: none identified. Mergeable: false; merge authorized: false.** Reviewer/runtime lease released on completion; no fix work follows this review.
