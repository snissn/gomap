# Repair07 independent source review

**ACCEPT**, scoped to the one-file synthetic coordinator fixture correction. This composes with the historical repair06 source acceptance; it does not accept focused runtime or an actual native publication run.

Reviewed as INTERNAL_SUBAGENT_REVIEW by explicitly routed gpt-6.1-sol/high. Candidate edits: false; no children, Go execution, source or Git mutations. Manual hosted review cannot pin the required exact model and was not requested.

## Exact input and verification

- Manifest: `398fd377bd569c52341e10cc9c90d6bd0762fec7a2ffc39413b4148d0dfe1750` (10,433 paths).
- Full archive: `437acc30342c067bd36ab7c4387cc9a9d57dae7d937bc6f931816c56a4a00772`.
- Delta versus accepted-source06: `4b6f6763a9a340a96875b2a7501058f5684bcdfc9b8e521beece2acf2a55128e`.
- Sole changed file: `TreeDB/internal/rootpublication/native_publication_v6_test.go`, SHA256 `d003918fbd8aba1387e1e8f55133406e20aa2f090f433f35922683af1cf7ea60`. All other 10,432 paths match06.
- Raw index remains `4c6b63357522cd951fd864e661b167f52e6debcd5d5e150e002fdba478f4dbe1`.

Independently hashed every archive member and every live source path, reproduced the exact delta, checked all handoff artifact hashes and proposal test-source bindings. No product source, layout, F/G/P or original actual-driver fixture bytes changed.

## Findings and fixture fidelity

No blocking source finding remains in this delta. The preserved06 failure was before intended Join: `ReleaseNativePublicationTurnV6` requires a singleton durable group, but the old empty-Set candidate had none. It does not call the separate realClock-only completion-plan admission; Begin/Release support FakeClock. Those preliminary clock-cause claims were incorrect.

The new helper constructs the existing genuine ordinary PreparedCOW/transaction fixture and Enqueues it through actual activation. It checks raw transaction ownership/phase, activation count and original Set custody. Success invokes the original ordinary allocator Consume callback and verifies consumed/released transaction versus the still-Coordinator Set. It never fabricates a phase, group or fence. The receipt owner later disposes that Set through the unchanged physical-completion path.

Ambiguity now uses the same real singleton but does not Consume it. Assertions keep the original activated transaction and Set in Coordinator custody, then check both move to Recovery and finally Released through the original handoff. Consume count stays zero. Existing frontier, refusal, once-only cleanup and recovery assertions remain.

Default realClock plus retained `StartupSynchronous` avoids both an unexpected scheduler publication before Begin and a combined FakeClock control cost of33 records. The same32-record/1MiB vector spans Begin and Release without reset: Begin5 plus success Release24/26 gives29/31; ambiguity is at most21. Ordinary fixture creation, Enqueue, Consume and recovery are expressly outside this synthetic vector, not a native nil-work fallback or proof that the full native turn fitsQ32.

`CompleteStartup` is not bypassed or called with the native dispatch outstanding. Original `WaitThrough` invokes `dispatchPublication(true)`; its original eligibility permits the ordinary successor once the native head is detached while physical native dispatch remains. Coverage is ordinary-engine progress through startup dispatch, not background-scheduler service.

Read the complete changed file, genuine fixture/allocator callbacks, transaction activation/Consume/recovery, original Begin/Release/clock/timer rows, startup dispatcher, and all eight affected top-level tests plus six scrub children. Unchanged06 Join/callback coverage carries by exact bytes.

## Proposed controls and limits

The proposed normal/race calls select all eight shared-helper users and six children per mode, with exact source bindings, pinned toolchain, count1 and existing deadlines. The separate onceACK/paused-Finish-Close proposal names unchanged actual driver sources. Both proposals are source-consistent and still require root selection; this report grants no runtime.

Repair06 RED logs remain historical failures, and no race/actual06 result is inferred. Fresh focused controls and actual driver outcomes remain pending. The synthetic concurrency cases retain their externally kept-alive fixture owners and do not establish containing-admission lifetime for concurrent public Close. Original Q32 remains RED/ACK0. No numeric allowance, whole-union/Finish bound, performance, final native qualification or merge approval follows.
