# R1 architecture graph #5090 — execution handoff

Handoff requested by the user on 2026-10-08. Execution is stopped; all assigned workers are settled, all active runtime/SSH/fetch processes joined, and Linux111's R1 reservation is genuinely released. This packet preserves work for an executor assigned [#5090](https://github.com/snissn/gomap/issues/5090). It is source preservation, not product acceptance. The parent and outstanding implementation tickets remain open. No work is merged at this handoff.

## Start here

1. Read the current parent and relevant child bodies, including the accepted #5091 contract and recorded corrections. The leading handoff sections supersede older checkpoint status, while preserving its acceptance criteria and historical evidence. #5094 was activated and is a final qualification blocker.
2. Refresh live main, branch heads, PR reviews/CI and adjacent ownership. Fetch the branches below into isolated worktrees; preserve the dirty primary checkout. Use `GOWORK=off`. No prior conversation or local uncommitted source is required to recover these candidates.
3. Resume constructor/cleanup ownership through #5113 on the composed M/C candidate. Resolve the two newly traced production caller seams below before implementing online vacuum. The selected design is provisional; it has no vacuum implementation yet. Retain the same concrete constructor owner and actual failed capability custody.
4. Format any changed candidate in the isolated checkout, bind the resulting exact source, update the risk inventory, and run the missing normal/race gates on a freshly selected host. The latest M repairs are source-reviewed only. The portable historical run specs show prior exact tests and toolchain provenance; expand them for new tests, do not execute their expired launcher grants unchanged.
5. Integrate allocator and publication work deliberately, resolve the R mixed-churn regression and P predecessor ABI, then meet each node's original correctness, fit and performance gates. Product predecessors and #5096's reviewed harness must land before #5097 final qualification.

An assigned executor can revise causal scope and dependencies within the authorized graph, preserving the parent outcome. This handoff introduces no extra user approval step. A historical source or runtime selection is not a still-live reservation.

## Recoverable source and PRs

| Lane / tickets | GitHub branch and source commit | Draft PR | Status and immediate action |
| --- | --- | --- | --- |
| R / #5092 | `codex/r1-5092-captured-row-executor`, `113e5b2d3623c5ed5211d2f2dbe407ea46a110e5` | [#5104](https://github.com/snissn/gomap/pull/5104) | Read candidate pushed; mixed-churn regression unaccepted. Consume mapped M/C/P constructor/publication repairs before final matched rerun. |
| M/C/constructor / #5093, #5094, #5113 | `codex/r1-5093-5113-composed-handoff-20261008`, `169a34dc320d713ee08acbd3096b03178cf77d00` | [#5122](https://github.com/snissn/gomap/pull/5122) | Exact composed source, 298 changed paths relative to recorded base. Latest custody and constructor edits unformatted/uncompiled/unexecuted. Online vacuum remains unfinished. |
| L / #5108, #5095; #5103/#5105 history | `codex/r1-5108-allocator-handoff-20261008`, `46e5c249e5714be644a406c52addcae312ad372f` | [#5123](https://github.com/snissn/gomap/pull/5123) | Exact allocator continuation, 45 changed paths. Component normal/race evidence exists; whole integration/fit/economics remain open. |
| S production / #5098 | `codex/r1-5098-common-rollover-observer`, `8844d70630b38266722264e0922a5c6e0ee67440` | [#5112](https://github.com/snissn/gomap/pull/5112) | Production observer/reclaim correction; no natural-rollover qualification. |
| S diagnostic/control / #5098 | `codex/r1-5098-default-harness-handoff-20261008`, `28da479a02b629a906d0bf3fc38c1e639fb68e0d` | [#5124](https://github.com/snissn/gomap/pull/5124) | Includes production correction plus common controls/preflight harness, 15 changed paths. Last packet stopped at build-artifact cap before diagnostic DB birth. |
| P / #5099 | `codex/r1-5099-captured-closure-reuse`, `c9f85aebbf6a80d833913848ccef4d90acc0d1a8` | [#5125](https://github.com/snissn/gomap/pull/5125) | Existing pushed closure-reuse candidate now discoverable; final merged dependency ABI and whole C/P evidence remain open. |

Every branch was checked against its actual remote head. Source commits preserve exact source bytes without formatting or a new test run. [source-publication.json](source/source-publication.json) records bases, changed paths, whole-source manifest hashes and Git tree IDs. [local-worktree-inventory.json](source/local-worktree-inventory.json) records the original retained local checkouts. M/L/S full manifests are under `source/`.

M includes allocator/publication composition and older predecessor work; L is a separate continuation. S diagnostic includes #5112's correction. Do not blindly cherry-pick overlapping snapshots or merge these handoff drafts as finished nodes. Use base-aware diffs and source bindings to preserve one allocator, one publication engine, and one constructor owner. Re-split/rebase as needed with explicit integration evidence.

## Accepted prerequisites and remaining DAG

Architecture decision #5091/#5100 landed at `44a72f3856cac51afdb09e60c5394706f261e930`. Measurement H0 #5101/#5102 landed at `af56fe515668b5c8ddf7d1b06a01f251fefcf8d6`. These accepted decisions/tooling do not qualify product performance.

The shared read resolver #5092 is a final predecessor of #5093; #5093 precedes activated #5094 and #5098. #5113's same-engine constructor/admission/lifetime foundation is a final prerequisite of #5093/#5094/#5108/#5095. Allocator evolution is #5103/#5105 -> #5108 -> #5095 -> #5096. P #5099 requires its exact adopted shared-owner subset, not closure of unrelated umbrellas. #5096 must land a reviewed public scale/churn harness; #5097 then freezes merged product/harness and retains final matched evidence.

Provisional dependent construction remains allowed when its recorded contracts are usable. Final merge and qualification require actual predecessor identities. If R/M foundation ordering forms a practical cycle, record a narrow independently landable #5113 foundation decision in #5090; do not waive the R regression or other north-star gates. #5022/P1 remains held pending the user's separate discussion/activation. #5111, #5037 and #5054 remain independently owned; no unfrozen external ABI is adopted here.

## M/C and constructor closure: exact checkpoint

The pushed M source contains 10,077 source paths with whole-source manifest SHA-256 `7b1ffd5da22b9c0cd1807679533cc8b322eeb77e2b5501b96e1e9fcc9fa7ae83`.

The prior Linux M phase2 v11 packet is genuinely RED: 242/247 executed top-level tests passed, five rootpublication controls failed, three remaining packages and all race controls were unexecuted. TreeDB 20, db 112, Pager 12 and residentcredit 6 selected normal controls passed; db TestMain's leak guard passed. ROOT audit `bdaf4b17750ec39e7b508cffed849a95df5bd93e53f2634dc8fe651598e94365`, genuine release `c35388f3b758dcae400900d109f33ff55c7ffd4a2c718d92c26aa86b23a61222`. Source was 10,074 paths, SHA `3491dcbae1acf2a1d8758fabd3b62020b63355e697cc44335f806abe9b10e9ff`.

The required five failing controls are CrossKindCoalescingChecks, KeepsSmallBuilderLinear, AmbiguousPhysicalCandidatesUseExactFallback, CrossKindCollisionUsesExactFallback and RepresentativeReplacementUsesExactFallback. Preserve their exact full identifiers from the bundled run spec. PackedAlias is additional, not a replacement. The original selected inventory was 253 normal/race top-level controls / 57 phase plan; expand for actual new tests and lifecycle coverage rather than waiving missing phases.

Source-only checkpoint01 repairs transferred temporary-shell custody and callback execution under inherited builder/child gates. Independent review then found successful selection could lose paused Scope controls. Checkpoint02 corrects that: discharge only the exact empty, nonfailed, nonuncertain controls on their concrete Scope outside the builder lock; retain nonempty/Pending custody; Freeze refuses any remaining pending cleanup. Existing public selector/kind/duplicate-union paths are covered by three additional meaningful controls plus five earlier controls. Their presence is not runtime success. Checkpoint02 receipt `788316ef94df978b549f79f3b02c00b1241549b1025f2e54983c67dbbbac3861`; independent source-only ACCEPT receipt `260fbb5705bfa435d5fd0d856020732b92081c17432703f3d9e77e7ba4b9aecd`. Generic selector error/void Abandon custody remains unqualified.

The phase3 source slice precharges the actual initial index generation, reader registry with embedded 16 slots, graveyard control/full 64-slot initial backing, and conditional managed writer on the SAME retained constructor Pager Scope before child construction/binding. Ordinary unsupported ownership remains incomplete and finite admission CLOSED. Source-only independent ACCEPT receipt `922847d4cf403f9799f1a52a4f999b3aaba470a2561050d95629f07c7bb87107`. This does not accept replacement callers, later growth, opaque children, total fit or runtime.

Read the bundled [online-vacuum handoff](evidence/artifacts/M23-online-vacuum-handoff-v2/checkpoint01/handoff.md) and [source-bound inquiry](evidence/artifacts/M23-online-vacuum-constructor-owner-inquiry01/inquiry.md). The handoff has ZERO online-vacuum source edits and the exact same 10,077-file source as accepted checkpoint02. Receipt `0a157a812e5674a15643be14497d5e7967681233534c493449ed4461a7aa3559`.

Two newly traced seams need a coherent scope/design decision before editing:

- `TreeDB/db/compact_storage.go` holds maintenanceMu across its caller and invokes online vacuum with lockMaintenance=false. Cleanup outside inherited gates requires caller-wide invocation admission/lifetime so concurrent Close cannot invalidate the rest of CompactStorage. Merely unlocking a borrowed gate is insufficient.
- `TreeDB/db/recoverable_root_set.go` Release marks consumed first, ignores actual Set.Release/Snapshot.Close errors and clears fields. Its void return is not checked cleanup completion. Decide a checked actual-capability release/custody seam that retains exact remaining debt.

Then implement preborn fixed DB replacement custody on the SAME constructor owner before stale unlink/Open; retain nonnil partial Open results even with error; use `newConstructorIndexGen`; preserve exact runtime/resources/recovery handoff and namespace phase through failure. Cleanup order is stop/join -> exact resource/handoff release -> physical generation/Pager close -> unpublished namespace cleanup, retaining failed fields/cursors without replay. Waits/callbacks run outside inherited gates. Ambiguous rename preserves real recovery files. DB Close joins the actual replacement invocation and retries remaining custody before master/LOCK release. Successful installation transfers once and keeps old generations until their actual last reader. Correct the early-fatal old-reader fixture cleanup, then add public held-reader swap, prebirth refusal, partial Open/Close, Pending, rename, concurrent/reentrant Close and retry controls.

Whole metadata capacity stays 128 MiB. Preserve P8192/Q237942/F32/V64, leaf8192, cache64MiB, physical reserve1,599,848,960 and the recorded H63,010,304 ledger. Complete cumulative births, histories, zombies, loan-control bytes, platform backing and old/new overlap remain uncertified. No new budget, registry, master, retrocharge or finite-admission activation is selected.

## Allocator and manifest retirement

L source is 9,943 paths, SHA `63123da83b3b68a0f060b6a1f5fd2c8c3a7a5737e694a2b0295ba48d35efed78`. The composed packet-image membership component passed all 184 top-level/330 named controls in each pinned Mac normal/race build, without skip/fail/missing. ROOT audit `2015b950cd7361e95e94620439c539b066e788c0a406aa5f4c42d73b2ff04ee3`; exact source and compiler inputs are bundled. This evidence binds that historical composed source, not an arbitrary handoff branch or later complete pipeline.

Every staged image must belong to the exact current candidate's real owner ledger, including reused IDs; a highwater bound is insufficient. The retained initial RED fixture incorrectly assumed hint2 must allocate2; valid actual free-stack reuse selected5. Its correction proves the exact reused/appended reservation kind, original free membership and unchanged physical frontier without changing allocator policy. Complete staged-output coverage, pre-WAL preparation, forward-only uncertain publication, full joint fit and reclamation/foreground performance remain open. Preserve canonical Patricia topology and independent logical/recovery equivalence rather than add a shadow allocator.

## S: storage preflight and default exposure

S source is 9,878 paths, SHA `5e5acae9d69c8d31c1c440d7a475c5e118d00903fb7a30b7727e51d9b5c44918`. Latest census runtime v3 passed 14 top-level/38 named normal controls and 6/16 caching race controls. Race-harness compile exited0, but sampled resource monitoring caught the 4 GiB source/cache/output limit during that phase. The packet is FAILED: race-harness functional controls and the storage diagnostic were unexecuted, with no new DB birth. All20 command processes exited0; that does not override the driver's resource-guard failure. Audit `69f7634491649a60d6ee8f69cf108f8c5b3610cb2ec14a6c1010c54aaa830bb1`; genuine release `f2f88db3d0337754106c743eeedb060ecb67f5000c541cf13d951dc1b00a9022`; remote receipt `e6e326c479390f17c27230df074f284ecda31e628d26804fcb1312464d8eadf5`.

Earlier retry3 passed all14/38 controls in BOTH normal/race and both arms' loaded 4K current/held row/posting oracles, then failed its first physical census at root/index.db; the real file is maindb/index.db. It executed no 256-call mutation epoch or process cuts. Checkpoint05 fixes that three-line path issue; independent source-only ACCEPT receipt `c59089342f85203616fdacaec3dbecefeded00b3d420da55fe486474b5283273`. v3 did not reach runtime validation of that fix.

The earlier default discovery reached 6,550,856,608 apparent fixture bytes, including 5,456,789,504 index bytes, and failed the unchanged6GiB guard. All five leaf writers got traffic; all eight hot writers remained at zero natural rollovers. This is resource/exposure evidence, not a proven leak or starvation defect. N remains UNSET; fixed15 remains ungranted. No qualifying default rollover, reader-drained plateau or public economics result exists.

Resume by resolving duplicated source/cache/compiled-artifact staging against the reviewed resource plan. Inspect current retained namespaces and classify genuinely releasable outputs before deleting an exact allowlist; preserve failed DBs/evidence. Retain the two-arm 256-call short diagnostic, 600-second test timeout, default leaf32MiB/hot256MiB thresholds and original oracles. Caps remain 6GiB all-file fixtures INCLUDING WAL, 4GiB other source/cache/output, 12GiB namespace including2GiB shutdown slack, 40GiB global R; host free floor160GiB, prestage168GiB. Two-second monitoring is sampled observation, not an instantaneous quota/burst proof. Do not lower work/default thresholds, raise caps, discard failed DBs or use favorable retuning to obtain a pass.

The original ROOT release verifier accidentally referenced the older retry3 receipt and failed. That original failed packet is retained; only the corrected v2 audit/release above proves release. A post-cleanup last sample below4GiB does not refute the transient compile breach.

## R/P and quantitative gates still open

R focused matched 4K/16K point/range evidence and held32 controls passed; 16K mixed-churn median was +9.993% and remains unaccepted. The causal inquiry attributes the mapped work to finalization, not exclusively fsync; overlapping inclusive timings cannot be subtracted as exclusive cost. See [accepted residual mapping](https://github.com/snissn/gomap/issues/5092#issuecomment-6043107407). M/C/P plus the narrow #5015 shared owner must address their mapped construction/admission/publication residuals before final R qualification; do not generate an unrelated R trace loop.

P's inclusive16K result is 11,972,744B including601,920 closure bytes. Thirteen legacy rows are UNKNOWN. Historical scoped component controls at4K/16K passed; the external #5037/#5054 final ABI is unadopted/unmerged/unfrozen here. Exact dependency identity and whole C/P <=0.85 allocation ratio plus 25% avoidable-materialization elimination remain open.

Preserve the child tickets' complete frozen target tables, both noise rules and all semantic oracles. Key remaining targets include native M/C median and p95 <=0.85 generic, M1/32 <=0.75 generic B; C4 callers/64 requests <=32 physical syncs, throughput>=1.25 and p95<=0.8 with C<=4; GC <=N canonical+2N authority work, <=0.5 allocation at128/512, <=0.75 whole cost, <=0.25 legacy fence p95 and foreground allocation plateau. S needs each natural producer to roll twice, drained inactive storage<=0.85, public F/O and F/C<=1.15 with median/p95 and CV<=0.10/spread<=0.15. Required full read protocol is10,000 complete-read calls,50 mutations,batch32,populations4096/16384; older1,000-read blocks are diagnostic only. Component controls and source acceptance never substitute for these gates.

## Evidence recovery, host ownership and verification

The portable evidence directories retain source-only reviews/receipts, whole-source maps, actual compiler/tool/module inputs, old risk inventories, normal/race logs, sampled resources, failed runtime receipts and ROOT audit/release proofs. [file-inventory.json](file-inventory.json) records each copied file's SHA-256, size and original path. It also records duplicate base64 fetch envelopes retained only at their original location; their decoded files are included. `graph-at-handoff/` is a historical snapshot of issue bodies, not a substitute for current GitHub state. Historical drivers/specs are evidence; use new output namespaces, exact source freeze, real build identities and fresh host coordination on resume.

Large ELF executables, tar snapshots and benchmark DBs remain at their recorded native locations rather than being embedded in this packet. Both failed short-preflight DBs remain at:

- `/home/mikers/dev/codex-r1-5090-functional-linux111-20261008/S5098-preflight-v2/tmp/gomap-r1-tree-1927759698`
- `/home/mikers/dev/codex-r1-5090-functional-linux111-20261008/S5098-preflight-v2/tmp/gomap-r1-tree-3817663676`

Mac source/evidence root: `/private/tmp/gomap-r1-arch-5090-local-20261008`. Linux111 SSH: `mikers@192.168.0.111`; root `/home/mikers/dev/codex-r1-5090-functional-linux111-20261008`. M v11 remote packet is `state/M23-phase2-functional-v11`; S latest is `S5098-preflight-v2/run-census-v3` with source `S5098-preflight-v2/source-census-v3`. Native copies and failed evidence are retained. No cleanup of the primary checkout or evidence-bearing sources occurred.

R1 has no current runner reservation. Linux185 is separately owned by ordinary #5111; prior runtime grants/windows are expired. Select Mac/111/185 from fresh load/disk and ownership observations, coordinate actual shared usage, and use the best available matched baselines without claiming absolute quiet. The prior #5028 window was cancelled at the user's handoff request. Recheck live state rather than assuming this historical host snapshot persists.

Handoff validation consists of exact source->Git blob comparison, actual remote-head verification, portable evidence rehashing and GitHub ticket/PR readback. No new Go compilation, runtime test, benchmark or merge is part of this handoff.
