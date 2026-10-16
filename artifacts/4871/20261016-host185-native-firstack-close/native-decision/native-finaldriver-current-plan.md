# Current-source native finaldriver packet

Read-only implementation plan, exact gpt-6.1-sol/high, no children. No source edits, Go commands or workload execution. Bindings: source manifest **5cde43f24dcc86eaa8c2017bbc6cb0dc81a29e749d9420d27be2ab4f2e4e140d**, full archive **0bcaee68f1b6a058bd358b5096265771d35ba503143ac9f8e255b2c215fad8f4**, tree **e523999e97e0af545da0072359cd1ac2fc57b5eb**; all 10,389 archived files verified against the manifest. Complete actual33 evidence archive **dd1fa65e7a2331758a45f490df85516e1cc975d787ff1c2e5f741970e0e5e806**. Companion JSON records the 53 current source hashes and advice applicability.

The next packet should add one real ready return, a bounded observation of the existing genuine Finish pause, a checked fixed selected-shape invocation envelope, and fresh actual-route fixtures. It does not need a new publisher, cleanup owner, scheduler or pause hook. Original Q32/1MiB controls remain unchanged and RED; ACK remains 0 in existing evidence. This plan selects neither a numeric allowance nor runtime.

## Bootstrap decision: compute before creating the fresh cursor

Use a **source-derived a-priori selected-shape envelope**, not the measured F, a successful previous cursor, or a changed limit on an errored cursor. The fresh diagnostic cursor receives the resulting fixed G on its FIRST call and retains it through Close. Implement these steps together:

1. Before cursor birth, observe only original pre-cursor operands under their existing short exclusions. Use bounded, admitted test-only scalar reads, not an owner graph walk: source root/path/leaf bounds; allocator logical/physical extent and source allocated/abandoned/changed/replaced counts; runtime debt/visible/seal prefix; exact original owner binding geometry and active-enrollment absence; source pager table count; cache checkpoint counter and profile flags. Reuse the fixture's already held original snapshot/capture operands; do not manufacture a preparation owner. A bounded owner predicate can inspect at most the selected fixed binding count plus the terminating link, admitting each visit before reading it; more bindings make this selected shape unsupported.
2. Combine those scalars with the fixture's known encoded key/value length bounds, 33 supplied point records, floor32, BatchSize1, fixed parser/trie/stack constants, inline-value threshold, no outer leaves and empty dependency domain. Assert the selected source path/leaf shape explicitly. Derive a conservative **count/capacity vector theta_hat** with pure checked constructor transforms: materialized input/output frame/page maxima, retirement maxima, private clone vector sizes, four private-table capture/growth maxima, metadata page and placement maxima, constructor/manifest/set/token counts, and native RAM-root additions. Use the actual sizing/growth rules in materialized input/write, allocator preparation/emission/metadata placement and private_control_capacity. Do not substitute historical three metadata pages or capacities16 for these transforms.
3. Evaluate the fixed named G expression below using theta_hat and the original concrete admission prices. If a transform, capability or arithmetic check is unavailable, return **G unavailable before cursor creation**. This is one finite selected constructor calculation, not a general heap model. The metadata placement/retirement/private-table transform is implementation work still required; no existing helper currently supplies the whole vector. It must be source-proved and tested in this same packet before an allowance can be proposed.
4. Run the fresh cursor to the new ready return. Before Fence, observe the actual ready geometry and require every relevant count, capacity, owner, pager, prefix and supported-shape predicate to fit theta_hat. Recompute the authoritative actual F only as a validation against the precomputed envelope. A mismatch refuses before adoption; it cannot increase limits, replace the work object, or restart until a convenient geometry appears.

For the lookup term, bootstrap with **Nready_hat = Npre + the source-derived maximum native constructors on this selected one-frame route**. Include all failed-but-still-linked constructors. Use **L = Nready_hat + 5** in G. At ready require the fresh SAME-pager N0 <= Nready_hat. The separate one-successor proof allows at most four new nodes (three optimistic attempts plus one fallback), and the fifth link allowance covers terminal nil. Historical N0=4 corroborates this route but does not establish the fresh bound. No root producers other than this selected preparation and the one designated successor are allowed by this diagnostic schedule; ordinary Enqueue semantics are unchanged.

This resolves when G is chosen. Remaining arithmetic is concrete implementation, not permission to choose a round number: add the fixed pre-shape transforms and complete unit expressions, then freeze/review their evaluated result before runtime.

## Minimal source changes

| Path / original symbols | Change and invariant |
|---|---|
| `TreeDB/mvcc/prune_cursor_v6.go`: PruneCursor, pruneNativeV6, step phases26/27/22/20, closeStepLockedV6 | One publicationReady scalar. After SAME candidate, seal and Bind succeed, admit the scalar transition and return at phase26 **before Fence**. On the next same-limit call skip completed preparation/Bind and invoke original Fence. No builder, publication turn or prepared-slot authority survives that return. Reset only after genuine terminal cleanup/ACK, or actual Close disposal. Retain failure latching and immutable limits. |
| `TreeDB/db/native_prebuilder_observer_for_test_v6.go` and original `native_mvcc_published_terminal_v6.go` state | Add a by-value bounded Finish view: backend phase, original release detached status, original receipt flags, source-witness presence and containing charge/custody presence. Admit actual fields before reads. No Ref, pointer, callback or owner escapes. |
| `TreeDB/internal/rootpublication/native_primary_finish_work_v6.go`: FinishDetachedV6 | Expose the existing finishing/phase predicate through an admitted scalar view under the SAME release mutex. Do not create a successful receipt or physical callback replacement. |
| `TreeDB/internal/rootpublication/native_publication_v6.go`: NativePublicationV6 | Under original Coordinator.mu expose only same-owner/bound/turn/fenced/complete/recovery scalars for the original receipt. No linked-list scan or registry. |
| Proposed `TreeDB/mvcc/native_finaldriver_envelope_test.go`, narrow original-package sizing helpers where private types require them | Fixed named pre-shape transforms, unit prices and checked evaluator. Forward authoritative F/lease/owner prices; do not fork publication policy or replace work. Unsupported rows prevent a numeric G. |
| Proposed `TreeDB/mvcc/native_finaldriver_diagnostic_v6_test.go` | Fresh original33 fixture arms with fixed reviewed G and actual ready/stale/cancel/ACK/Finish/Close paths. Preserve existing public controls byte/semantically except necessary separately reviewed readiness assertions. |

Native root witness and allocator cleanup already have the needed lifetime continuation; do not extend them again merely for observation. Actual `unsafe.Sizeof(PruneCursor)` and AllocationCharge must be recomputed after the bit, even if it fits existing padding. Include the ready branch, stores, caller entry/deferred error/stat tail and all other changed scalar work in actual prices.

## Whole-invocation G and actual progress units

Vectors are `(records,bytes)`. All sums/products and componentwise maxima are checked:

```
G(theta_hat,L) = (F(theta_hat) + C_fence(theta_hat))
                max U_birth(theta_hat) max U_prepare(theta_hat)
                max U_finish(theta_hat,L) max U_cancel(theta_hat,L)
                max U_close(theta_hat,L) max O_selected(theta_hat)
U_X = max over reachable persisted phase/index s:
      [complete replayed entry-to-child prefix(s) + next-progress unit(s)]
```

A maximum of individual Reserve calls is invalid. Include the repeated cursor/parent entry reservations and every child reservation needed before its next persisted advance. In particular phase7 detach+BindFinish, root phase8+Arena.Drop+whole lookup, and queue header+owner shrink+concrete resize must fit as **sums**. `closeLockedV6` resets counters between retries but has no no-progress detector; a timeout is not convergence evidence.

Implement a fixed row for each following reachable family, reusing the current unit-envelope source inventory rather than a generic walker:

- Cursor birth and concrete budget acquisition/resize; batch backing; materialized input/output header, payload/page/frame copies and final detach.
- Private FreelistTxn header and allocated/abandoned vectors, source-map capture, each of four private-table growth/snapshot/control units, metadata emission/placement, retired-vector construction and clear. Include original pending accounting debt through genuine removal.
- Original Owner atomic Add: header plus **all selected active/binding visits and both forward/rollback concrete Resize admissions before effects**. Removal units include each actual reentered owner/enrollment/binding/concrete lease prefix; number of bindings is not a guessed constant.
- Candidate/member/root/install/callback/empty-resource/manifest/promotion/seal constructors, actual runtime backing growth and accounting completion, and precise source/current/parent checks.
- Complete original F plus caller C_fence. Fund every required post-adoption caller tail before adoption; no fallible caller reservation may strand a successful fence. Phase22 ACK accounting may instead safely yield before its first effect.
- Original published phase7 refunds and BindFinish; genuine transaction retire callback, root/queue/Set/manifest/token/namespace release, original terminal accounting handoff; source-witness Drop; published allocator cleanup; both actual captures/Snapshot/pin/enrollment/lease cleanup; outer projection/codec/containing frame Close.
- Supported hard-I/O disposition and sticky recovery flags. Price their required controls before ambiguous authority. Do not turn a retained sticky error into successful cleanup or run private Abort after adoption.

For table type T, entry E and pointer width p, the source formulas are `Lookup=(1+L, sizeof(T)+L*(p+sizeof(E)))` and `Remove=(1+L,128+L*(p+2*sizeof(E)))`. These are child prices; add their actual parent prefixes. Ordinary rehash links existing entries and does not increase count. The count includes unsealed, failed and queued nodes until actual unlink.

Current actual33 F **367/882745** and expression `F293+D+2K+23P+8M+3S+N+Z+V+A` remain diagnostic observations, not G. They do not cover birth/preparation/Finish/Close or the new caller branch. New frozen source must derive its authoritative F and complete row set; leave any unclosed family explicit and G unavailable.

## Genuine pause, locks and custody

`ContinueFinishWorkV6` phase3 binds the original terminal RemovalWork, clears the physical transaction aliases and **returns false unconditionally**. `FinishDetachedV6` then identifies its phase1/2 accounting continuation. This is a real mandatory post-physical-disposal, pre-accounting-completion pause; no blocking hook is needed. It is not evidence of a physical callback still in flight.

Stop the future paused arm only at backend phase8 with genuine detached release, original receipt fenced/bound/not complete/turn released, live source witness and containing lease. Serialize through cursor.mu; observe release.mu and Coordinator.mu in separate existing short scopes. JoinNativePublications releases Coordinator.mu before calling its owner, so do not introduce an inverse coordinator-to-cursor hold. Source witness stays through phase8 and is released resumably in phase9 before allocator scratch/reset. Actual DB.Close must join that same receipt before Stop/teardown; Close's protected late-Bind check remains authoritative.

## Retained reconciliation

The completed observation establishes **P=1,710,512**, 21 constructor rows, and **P_named_roots=1,728,304** with two deduplicated root images. These are selected constructor facts, not a transitive retained-union/physical heap bound. H02/H03 and other excluded domains remain separate. Reconcile the new cursor field against actual whole-container charge exactly once; by-value views/evaluation add no persistent resource graph. Do not add Owner.Bytes to its contained constructor rows. The SAME source/parent bank alias counts once; target root is distinct; the shared pager table is observed once. Private control vectors/tables and accounting-only debt stay with their original owners through actual last-alias clear; canonical successor DATA outputs remain in original allocator custody. Repeat constructor reconciliation at ready on the fresh fixture rather than assuming the historical numbers remain exact.

## Meaningful controls and actual diagnostic ordering

First pure normal/race controls should test checked pre-shape/envelope evaluation, overflow and unsupported capability/counts, exact replay-prefix boundary versus leaf-only fit, ready-transition admission refusal, unchanged immutable limits/failure latch, scalar observation rejection and original receipt identity. Positive receipt authority must come from the real route, not a fabricated successful member. Preserve the existing Q32 actual33 refusal and custody controls.

The later separately selected real-route arms should be:

1. **Ready boundary and cancel:** actual phase26 ready return, zero ACK, unchanged visible/durable root, no builder/attempt/activation, original operands/cut; genuine cancellation drains original resources.
2. **Ready stale:** a real ordinary durable writer between ready and Fence; final exact-basis refusal before adoption, then original cancellation; newer reachable pages survive and reopen. A changed-limit arm uses a separate fresh cursor and preserves its original error contract.
3. **ACK:** one real selected+anchor request through original fence/Consume/Finish, ACK only after the original receipt completes. Derive the selected deleted key from the accepted batch; do not assume key1. Verify deletion, preserved anchor/current records, original receipt/frontier exactly once, held old reader **after native publication and after the designated successor**, and reopen. One batch ACK does not establish namespace completion.
4. **Paused genuine Finish and owning DB.Close:** reach the mandatory detached pause with zero ACK and live original receipt/frame/witness; complete one actual fresh-key durable writer34 while old Finish is paused. Assert the held old reader after native publication and again after writer34. Close that independently held reader before invoking the real owning public DB.Close; require DB join to drain the original receipt/accounting/source witness before teardown. Repeated Close and reopen validate terminal behavior. This Close arm need not fabricate a public phase22 ACK.
5. **Supported sticky hard-I/O:** use genuine existing error injection within a separately reviewed real-route grant; preserve whole-frame recovery-required custody and truthfully incomplete Close. No synthetic successful adopted receipt/private abort.

The existing accepted actual33 oldReader.Get runs **before writer34**; it proves only that ordering. Existing evidence proves neither positive native publication nor post-successor reader retention. No runtime permission follows from this plan. The next coherent source packet can implement all above mechanical seams and pricing together; numeric G and real-route execution remain gated on complete source expressions, frozen bounds and independent review.
