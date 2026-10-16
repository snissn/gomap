# Repair07 runtime disposition (append-only)

**REJECT complete fixture/control readiness.** The original repair07 review remains immutable; its no-blocker conclusion for all successor caller chains was incorrect. The repair06 production Join source disposition is unchanged.

Exact source manifest `398fd377bd569c52341e10cc9c90d6bd0762fec7a2ffc39413b4148d0dfe1750`, archive `437acc30342c067bd36ab7c4387cc9a9d57dae7d937bc6f931816c56a4a00772`. Normal raw SHA256 `ce41df687cb008a1f7fdda21e33eca3dc2035d74665f61655b377bdfbd06ac66`; result `9f7d46ef05033391ecdf311cbd5a9c3a1427d14333443a5c75caef0acf7f10ff`. Before/after manifests match all frozen source bytes. Result records joined leader, empty group/children, no timeout/skips.

Observed eight normal tops: six PASS, two FAIL; all six child cases PASS. The two successor tests fail at Enqueue, before intended successor activation/publication. Race and actual native driver were not executed.

## F7: rootless successor after a real durable lineage

`native_publication_v6_test.go:121` and `:166` retain `candidate(t,2,1)`. That generic helper creates no DurableRoot transaction. The new first fixture genuinely consumes transaction1, and `ReleaseNativePublicationTurnV6 -> completePublicationScalarsLockedV6` establishes durable lineage1/sequence1. `preflightDurableRootLocked` then correctly rejects the successor at `coordinator.go:528–529`: candidate2 lacks the next transaction. This is a fixture incompatibility, not a production regression or budget refusal.

The prior review verified startup `WaitThrough` dispatch eligibility but missed this earlier lineage preflight. No claim of completed successor source coverage should survive that result.

## Minimum coherent correction

Keep the first fixture available and prepare sequence2 on its SAME allocator, pager and reservation ledger after first Consume has called `PublishCOWCandidateV1`. That method installs generation1, clears `cow.prepared` and creates the next allocator transaction. Use the same durable lineage and a fresh candidate ID/generation/commit2; no independent allocator masquerading as the original chain.

Enqueue must perform actual second activation. Original startup `WaitThrough -> dispatchPublication(true) -> publisher -> finishPublishLocked -> consumeDurableRootGroup` must invoke second Consume and ordinary Set cleanup. Do not manually consume the second transaction or mutate phases/groups/admission lineage. Its callback must write/truncate the actual second prepared images and publish that same allocator candidate, with callback pointer checks.

Check second activation/Consume counts1, transaction/Set2 Released, actual allocator generation2, and durable/admission sequence2. Original Set1/receipt must remain under original incomplete physical custody across the successor; late Close must settle it once without regressing sequence2. The ambiguous case has no successor and keeps its existing unconsumed activated transaction/recovery assertions. The other six helper users' observed normal passes remain exact07 evidence only.

Preserve all original32-record/1MiB control vectors and deadlines. Ordinary construction/Consume/recovery remains explicitly outside that synthetic vector. No production change, allowance increase, native completion qualification or concurrent containing-admission claim follows.

Review performed read-only by explicitly routed gpt-6.1-sol/high, INTERNAL_SUBAGENT_REVIEW; no candidate edits, child agents or runtime executed by reviewer. Manual hosted review cannot pin the required exact model.
