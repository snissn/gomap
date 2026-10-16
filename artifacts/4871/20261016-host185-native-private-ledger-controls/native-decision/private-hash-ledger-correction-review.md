# Private hash / selected-ledger correction review

**REJECT the exact 6c525 correction packet** pending three narrow fixture corrections. The corrected observer and selected Set constructor ledger have no remaining confirmed source blocker in this review. The first focused command failed three fixtures, so neither this packet nor its focused controls are accepted.

Reviewer: `/root/ordinary_review`, explicitly routed **gpt-6.1-sol/high**, `INTERNAL_SUBAGENT_REVIEW`. Candidate edits by reviewer: **false**. Hosted manual review cannot pin the required exact model and was not requested. Review used immutable archives; no mutable native source, Go execution, source/Git/GitHub changes or children.

## Exact source and verification

- Manifest `6c525298d214717016b20b93eafd04c1b19ef0044b6ee32eab443ba376ebe7fd`.
- Full archive `061ed6dc8c9eab89d371e5e8dfd5e92883d074d654f32221291b538ba323cd7c`.
- Seven-path patch `53e6311c3ce82bd41fdadea938d297647dc2599eac4fa42d1aca2c3a667d6a31` against manifest `15f0548d03b03923cc8785816b21ee957df3a4f114a2578cba0ef3bbbdd142ec`.
- Root selection `cac20b5e0ca11c9f2fe9d6e255b178654ac383d93918f25ac76cb3fc7be84202`.

Independently streamed and hashed all **10,389** archived files against the manifest, with no missing/extra/duplicate payloads. Compared manifests: exactly seven modifications, no additions/deletions. Read all seven changes and their actual candidate/seal/Set constructors, original allocation charges, private hash construction and retained-owner refund callers. The JSON records each changed-file hash and evidence bindings. Earlier source review and failures remain unchanged.

## Prior findings and corrected semantics

**L1 resolved:** candidate, member and sealed-resource wrappers are independent `CloneAdmitted` results. The evaluator no longer requires impossible member/seal wrapper equality. It observes all four surviving wrapper roles (candidate, member, transferred proof, sealed resources), with the cleared preparation resource role required absent.

**L2 resolved:** completed phase13 proof observation follows `seal.primary.proofResources`, after checking the original preparation holder was cleared. Transfer/phase guards prevent partial or dual-holder states from masquerading as supported completion. Pair observation acquires the original ordered Set locks and returns scalar allocation equality only. Every observed row must be live, selected, originally charged and empty in the supported form. Wrapper metadata, kind backing and empty-directory backing deduplicate independently; equal byte charges and equal physical identity do not merge allocations. The Set constructor aggregate already includes Extras, so Extras is not added again. Owner envelopes are not added to payload charges.

The fixed role/pair reads reserve traffic before inspection; changed result/containing sizes use `unsafe.Sizeof`. The supported four-wrapper evaluator fits within 22 record units (20 when expected kind backing is shared), under unchanged Q32. Checked sums retain overflow/incompleteness. Genuine Set constructor/holder controls and by-value positive/negative arithmetic controls are meaningful for this limited observer contract; they do not fabricate publication. Expected selected-empty observation now fails rather than silently reporting an unexpectedly incomplete ledger.

**L3 resolved in the selected Snapshot loop:** its intermediate assertion recognizes live backing versus detached scalar debt, checks overflow, and retains exact final alias/charge endpoints. Two independent sibling fixture issues remain below.

## Blocking fixture inventory

**C1 — peak-budget setup fails before the intended operation.** `TreeDB/freelist/private_control_capacity_v6_test.go:265` requests `4096-limited.Bytes()-128`. `limited.Bytes()` is not total governing occupancy. `NewCOWBudget` charges its control, enrollment has its original external lease, and `AcquireExternal` adds another rounded lease header (`cow_budget.go:66,104,128`). The raw failure at line267 is therefore setup admission, before `insert(9)` or its overlap oracle.

Minimum correction: use checked actual `budget.Stats().TotalBytes`, the added external lease header and actual old/new array charges to place the intended peak boundary. Prove filler admission succeeds, replacement alone fits after old backing release, and old+new overlap fails. Preserve no-new-key/no-next/original-owner and genuine final cleanup assertions. No production limit or admission change is justified.

**C2 — partial-clone provenance assumes detached debt remains charged.** `private_control_capacity_v6_test.go:404` requires exact Owner equality including changed/replaced debt. `RemovePendingWork` debits Owner bytes before its shrink/reset continuation completes (`owner_work.go:250`); the parent scalar debt remains until `CompletePendingWork` resets it (`:402`). `buildSnapshot` detaches the original snapshot before invoking that path (`private_control_capacity_v6.go:305`), including during `COWPreparationWorkV6.advance` phase5. Cuts with 18–20 available records hit this legitimate window.

Minimum correction: checked-sum all still-live clone/header/vector/table/snapshot/sort charges as `live`, and scalar changed/replaced debt separately. Assert `live <= Owner.Bytes <= live+debt` while removal is pending, exact pre-debit endpoints where asserted, and exact zero-owner/debt/no-alias completion after the SAME cancellation. Keep every independently charged live clone allocation included. No early physical refund is demonstrated by this failure.

**C3 — the intended commit/refund pause never leaves allocation phase3.** `private_control_capacity_v6_test.go:502` gives two records per ordinary phase. Phase3 needs three records for this exact unbound Owner: one insert-entry visit, one whole allocation admission and one original Owner Add (`private_control_capacity_v6.go:87,143`; `owner_growth_work.go:13`). The observed phase3 stall at line512 is therefore deterministic, effect-free refusal, not a failed committed-resume implementation.

Minimum correction: expose that exact three-record phase3 unit while preserving the three-record phase10 entry+commit pause and Q32 cap. Require reaching genuine phase11 with exactly one committed key, detached old array, scalar debt custody and no next-array alias; resume the SAME refund and verify duplicate/sink and final-zero assertions. Do not manually advance phase or broadly increase the allowance.

## Actual focused evidence

Only `normal-freelist-private-capacity` ran: **18 top-level RUN, 15 PASS, 3 FAIL**. The partial-clone family has three failing children. Raw log SHA `0729f4583dd3f0968823614b2a495c0663d182ad50fef86ebdccb579a08112fb`; result SHA `951b8cbc576327104e00c5c0b5d2c030d6b72ebd73e70da9c99fedd1c4879d0c`. Independently checked raw results and both before/after source manifests against frozen 6c525. There was no skip or timeout; the original leader joined and final process-group/adopted-child lists are empty. The batch correctly stopped at first failed command; **11 calls, including all races and the new DB/rootpublication/mvcc controls, did not run**. No compile-only or historical result is relabeled as their success.

This rejects fixture/control readiness without identifying a required production change. After a separately frozen minimal fixture correction, the unexecuted selected controls remain necessary; root owns any next source/runtime grant.

## Limits and release

This is source/evidence review of the seven-path correction, not complete native acceptance. The selected-empty constructor admission envelope is not an exact heap/transitive retained union. No actual33 successor observation, aggregate F/Finish/Close allowance, numeric allowance, Q32 fit, native publication or qualification is accepted or authorized. Original Q32 RED/ACK0 remains. Prior provisional hash/clone source disposition is preserved with its original attribution.

Review lease released; no candidate changes made.
