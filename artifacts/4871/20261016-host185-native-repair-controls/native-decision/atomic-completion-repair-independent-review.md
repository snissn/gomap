# Independent native atomic-completion repair review

**REJECT the exact frozen packet because NR4 prevents its new N3 control from running.** The complete product review found no remaining source blocker for N1, N2 or N3. That source conclusion is separate from the failed fixture, unexecuted race checks, actual publication behavior and qualification. No numeric or runtime allowance is granted.

The reviewed identity is the 10,342-file manifest `dd03a710cb0a0c277465d6fd5f913c7455b4f3cad4515503516e39210a39e4db`, archive `c528ec5d9b7e6a438ac39d079dfa948fdeac137fd106a6e23bbceffeda615834`, and 16-path repair patch `f55ef3d05a0871596cb2a3c25d1c44efd8cf6e0a2ece94701574fc7da87fa119`. All current files and archive members were independently hash-compared to the manifest, including a final source comparison before root authorized the separate fixture successor. The prior 42-path packet plus this exact delta forms the recorded 52-path union. No Git identity/status was queried; this report binds the archived working-tree bytes. Live source has since entered the separately authorized fixture repair and is not claimed to remain identical.

## Blocking finding

**NR4 — new staged-constructor fixture panics before exercising its subject.** In the reviewed archive, `TreeDB/db/native_pager_operations_construction_v6_test.go:32` calls `idx.primary.Pager()` immediately after empty no-WAL `Open`. PRIMARY is initialized lazily, so `idx.primary` is nil. The actual first `before-output` subtest panicked there. The fixture therefore supplies no passing evidence for any of its seven staged-construction, resume, overflow or cancellation cases. Its frozen file SHA is `c948fdb8b76c231df5af6e6618b81b42125d9e30f217110f791bcfa577b349c3`.

Use the existing original warm-capture/actual-close sequence before taking callback/refund baselines, verify the resulting PRIMARY owner, and retain the same seven limits and cleanup assertions. This is a fixture correction, not a production defect or justification to alter admission. Root has authorized that separate successor; it is outside this exact review.

## N1: published outputs and pre-effect traffic

The preparation-time `DBState` and `snapshotView` allocations and their private retained fields are removed. `snapshot_publication_work_v6.go:13` reserves the two actual allocations and their sizes; `root_publication_activation.go:571` performs this before allocator adoption. Existing initialization and snapshot-publication traffic remain reserved. Failed admission or adoption returns before either output allocation.

After successful adoption, `root_publication_activation.go:656` creates the original `DBState`; the original state and snapshot-view stores immediately consume the two outputs in the same nonyielding installation. The native route has no intervening fallback/refusal. Snapshot publication now receives the same `leafGenerationView` directly, eliminating the temporary escaping state. Ordinary nil-work behavior remains intact.

This is source closure under root's explicit accounting decision: only these original published legacy outputs remain in their original GC domain. The report does not invent a last-reader refund, root-bank dominance proof, deterministic GC completion or an all-heap quota claim. All allocation/initialization traffic remains priced; new native retained controls remain subject to their original ownership charges. Removal of two private pointers changes actual containing-size operands rather than authorizing an arbitrary rounded refund.

## N2: Close, Stop, Bind and unresolved custody

`db.go:3497` captures the runtime under the teardown read gate, then joins native owners outside the exclusive gate. Outstanding native custody is checked before recovery handoff, runtime clearing or physical teardown. The exclusive-gate loop rechecks after competing registration; a remaining owner is joined outside the gate and an unresolved error returns with the same original graph linked. A final outstanding check also protects the Stop boundary. Repeated Close preserves the sticky failed teardown result; this is error-state Close, not successful physical settlement.

`native_mvcc_publication_binding_v6.go:31` crosses the original teardown read gate and rechecks closing before Coordinator binding. The actual production caller family uses this wrapper. The protected coordinator predicate at `native_publication_v6.go:590` checks existing native list/dispatch state without a new scan or owner registry. The gate closes the capture-to-registration race while avoiding owner Close under the teardown write lock. The original pending/receipt/dispatch/transaction graph remains authoritative for genuine hard-I/O failure. Successful Close still requires genuine join/settlement; pre-effect budget/stale refusal is not converted to poisoning.

The new tests cover unresolved/repeated Close, actual gate ordering, late binding, successful join and ordinary hook errors. Their synthetic owner error tests the boundary and does not itself prove a genuine physical-I/O runtime path.

## N3: actual staged constructor and cancellation

`stable_pager_owned_operations.go:83` uses the original callback constructor in stages: admit actual callback/path growth, create and attach that callback; construct and attach original cleanup custody; retain the original PRIMARY owner; then expose the complete operations object. `resource_original_cleanup.go:99` shares the original constructor and performs typed actual-owner admission under the existing registry/owner exclusion before edge increments or allocation. Ordinary nil-work construction remains on its original path.

Each growth prices actual pending/binding fan-out and concrete forward/rollback resize admission before effects. Work exhaustion remains resumable `false,nil`; unsupported capability remains a hard typed refusal. Accepted stage identities are retained across retry. A later error leaves the accepted callback/custody/owner edges attached for typed cancellation rather than calling ordinary synchronous unwind.

The capture close state machine detaches parent/spec aliases before final callback release, closes the original Snapshot, releases the owner and custody, and resumes original removal work through actual shrinking. It clears aliases before charge removal and does not duplicate accepted ownership on resume. Partial cleanup may refuse; the accepted graph remains charged and linked. No keeper registry or private refund was introduced. The added stage byte lives inside the already admitted cursor/capture representation; callback, path and custody retain their actual independent allocation charges.

For zero active pending/bindings and a nonnil owner, fresh construction costs eight records. This is not a universal constant: each typed growth uses actual current owner geometry, including rollback. Large indivisible growth may still refuse Q32 indefinitely. That is not useful-progress evidence. NR4 leaves the new end-to-end staged constructor control unverified at runtime.

## F and retained ledger

The symbolic final-turn expression is:

```
F = 140 + D + 2K + 2P + M + Z + V + A + R
R = 141 + 21P + 7M + 3S + N
F = 281 + D + 2K + 23P + 8M + 3S + N + Z + V + A
```

The fixed basis increases by two for the original published output allocations; their bytes add `sizeof(DBState)+sizeof(snapshotView)` to existing initialization traffic. Binding/capture preparation work is outside the Fence-started F interval and remains separately priced. Synthetic remainder values are not actual aggregate F.

The ledger accounts for the containing cursor/frame/captures, member/install environment, prefix/seal, runtime descriptor and capacity vectors, detached accounting debt, selected and two capture Snapshot wrappers, callback/path/custody, actual PRIMARY metadata/banks/enrollment/leases, and the explicitly inherited DATA and published-output domains. Owner policy allowance and overlapping backing/lease totals cannot be added as a disjoint physical total. Later-writer Finish geometry remains dynamic. No actual full F or canonical disjoint retained total has been observed. Original Q32/1MiB, 33/80/4096 RED/ACK0 remains unchanged.

## Coverage and verification limits

I read the complete 1,016-line repair patch and all 16 changed source/test paths, then traced adjacent activation, teardown/binding, constructor, owner-growth, cleanup/removal and original connected driver/Finish contracts. Unchanged prior graph coverage is carried from the rejected packet only where byte identity was verified. I reread repository/TreeDB contribution policies, task-local performance guidance and internal-review policy. The JSON records every delta and union path and exact document/command bindings.

I reviewed all four new control files and the proposed 20 focused plus two actual-route selectors statically. The proposed commands are bounded and preserve source/SDK/process receipts; they are proposals, not permission. I executed no Go command, test, build, benchmark or publication. Pinned Go executable SHA was independently checked.

Root's separately authorized, exact-source verification report records compile-only PASS for four packages; focused normal records 17 top-level PASS and the NR4 failure before N3 construction. The panic prevented later DB controls; race was not run. I inspected that report and summary, not independently rerun the campaign. Passing admission, teardown and custody controls support their narrow boundaries but do not waive NR4 or imply publication success.

After a separately frozen fixture successor and root-authorized focused verification, the proposed same-fixture prebuilder scalar observer is a defensible diagnostic next boundary. It must preserve all original refusals and label uncovered rows and subtotal/full-total distinctions honestly. This report does not authorize it, an actual native final driver, numeric allowance, native publication, ordinary merge, CI/performance qualification or a cap change.

Review mode: **INTERNAL_SUBAGENT_REVIEW**, actual reviewer **gpt-6.1-sol / high**. I constructed no candidate edits, spawned no children and performed no Git/GitHub mutation. Manual hosted review cannot pin the required model and was not requested. The independent reviewer lease is released after this report; the frozen fixture successor requires its own binding.
