# Root decision for the frozen native completion review

The frozen source manifest is `ccaa15b2381b87336937688cdc1e672c875564261da53fd6ba5c1f5e814107e6`, with 10,338 source files and 42 changed paths against `3dd8d2d11d129ea93fc6277215d06d7a4ba65650`. The independent exact `gpt-6.1-sol` review found three blocking source defects. Root independently read their actual implementations and accepts all three findings. This source image is rejected for an observer/publication grant. No prior runtime evidence is relabelled as evidence for a future repair.

The following is one coherent repair batch, not permission for an observer, performance experiment, native publication, allowance increase or successful teardown claim. Root owns Git/GitHub/merges. The native worker owns the native worktree source batch after the independent reviewer releases its read-only lease; no other source writer may modify it. Preserve all existing changes and provenance.

## N1: remove private preborn publication headers

`prepareActivationHeadersV6` currently creates independent `DBState` and `snapshotView` allocations during private preparation. `OrdinalScanWork.Reserve` accounts for operation traffic, not retained allocation admission. Candidate/member accounting and the existing root-bank charge do not admit these two allocations. Clearing the install's pointers after publication does not end all reader aliases. In particular `State()` clones a loaded DBState and `StateToken()` reads a snapshotView without acquiring a root-bank reference. Root therefore rejects an unproved claim that the root-bank last edge dominates every header alias, and rejects embedding the headers in a refundable member/install.

Select the narrower original-publication seam:

1. Remove the independent preborn header fields, allocations and private preparation dependency entirely. They must not exist across any native preparation/refusal/yield.
2. Admit the actual two allocation and initialization operations on the SAME work before allocator adoption. Preserve every existing sequence, conditional/Cut, snapshot-pin, installed-page and allocator admission check before irreversible effects.
3. Only after successful allocator adoption, in the SAME nonyielding original visible-install section, construct and immediately publish the original independent DBState and snapshotView outputs. There is no fallible admission, shape check, returned refusal or continuation between their creation and the original stores. Ordinary nil-work behavior retains its original lifecycle.
4. Price snapshot publication Z from the SAME existing leaf-generation operand that will be installed, under the original exclusion. Do not create a temporary escaping DBState merely to perform this preflight.
5. Update F, source layout, retained ledger and cancellation claims. Header allocation/init work moves into the final invocation; it is not subtracted from total traffic.

Root explicitly classifies only these original published DBState/snapshotView outputs in their original GC-managed lifetime domain for this diagnostic accounting model. This is a recorded boundary decision, not an inference from the DATA exclusion or the ordinary private-read paragraph. The ordinary candidate `a17febafb5353897c59a4e17f992e34c47a2585b` already allocates `&DBState` after allocator activation and `publishSnapshotView` creates the published view. The repair restores that placement instead of retaining additional native-private objects. Count all invoked allocation/initialization traffic. Do not claim per-header owner refund, deterministic GC reclamation, a transitive Go-heap quota, or full published-output residency bound. No native receipt, control, callback, array, construction frame or retained private payload is exempted. The existing conservative root descriptor and resource custody remains unchanged.

The adviser and independent reviewer both found this seam source-feasible subject to the conditions above. That advice does not accept the future implementation. Original Q32, retained/public gates and final qualification remain unchanged and unresolved.

## N2: make incomplete native join a real DB teardown barrier

`DB.closeAfterHooks` currently appends failed native Join/Stop errors and continues to recovery handoff, clears `db.rootPublication`, and tears down resources. `Coordinator.Stop` already refuses while native dispatch remains live, so the caller cannot treat its error as successful physical closure.

Select the existing DB close boundary. Preserve `closing=true` and closed admission, but return before recovery transfer/runtime detachment or any physical teardown when native receipt/dispatch custody is unresolved. Use protected original scalar disposition, not an unlocked list scan or a blanket stop on every ordinary cleanup error. Recheck the relevant condition after crossing the existing teardown gate/Stop, so an already admitted native producer cannot register in a gap and be detached. Do not wait for native Finish while holding locks it needs.

The existing `closeTeardownOnce` may retain the first incomplete-close error, consistent with the already chosen sticky hard-I/O policy. Preserve the SAME runtime/receipt/resources/lease and repeat that failure. This is intentionally an incomplete Close; it must not advertise successful resource release or recovery. Completed native receipts and ordinary cleanup errors with no live native debt retain established ordinary teardown behavior. Focused controls must cover both sides, plus repeated Close and preserved original custody.

## N3: work-bearing original pager callback construction and partial cleanup

Native capture phase4 calls ordinary `newStablePagerOwnedOperations` after a fixed six-record reservation. Its operations/path AddPending, original cleanup-custody AddPending, physical-owner retain and synchronous failure rollback can fan out over binding/enrollment state without SAME-work admission. A larger fixed reservation is rejected.

Extend the SAME original constructor/custody seams with typed staged construction retained in the existing native capture. Every actual owner growth uses `AddPendingWithOrdinalWork` under the original owner exclusion before allocation. Original cleanup-custody admission uses its original registry/owner order and checked closing/capacity decisions. Original physical-owner retain receives SAME work. Store each accepted original edge immediately in a release-safe partial state. No ordinary Add/Remove/Complete fallback on native refusal or error.

Later refusal retains these partial original edges. Cancellation/error cleanup drives original typed release, custody completion and RemovalWork after aliases clear; it does not synchronously walk/refund bindings. Preserve nil-work ordinary construction semantics and avoid a second owner, pool, registry, generic constructor framework or global hook. Cover successful construction, each partial failure/refusal boundary, rollback/cleanup refusal and resumption, unsupported lease capability, ordinary behavior, and actual containing size/work changes.

## Review and execution boundary

Complete all three repairs and adjacent ownership/caller audits as one source batch. Update the frozen packet's F/Finish/retained ledger and observer proposal to the final source. The observer plan remains advice only: do not implement its broad instrumentation or launch it as part of this repair. No new Go build/test/DB/publication runtime is authorized by this file; deliver the exact changed-source inventory and proposed focused command list first so root can bind a bounded verification grant. Preserve original Q32/1MiB/input32/resource16/file17/two-index/SAMERequest33/80/4096 and ACK0. Do not change fixture geometry or insert retry-until-success behavior.

Root will review the complete repaired source and independently review the exact frozen candidate before any subsequent native runtime milestone. Passing component tests will not establish actual publication, Finish, useful bounded progress, M0, or graph acceptance.
