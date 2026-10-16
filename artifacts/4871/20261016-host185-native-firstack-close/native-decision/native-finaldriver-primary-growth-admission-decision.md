# PRIMARY Growth admission decision

Disposition: **necessary source repair**, with the SAME existing Owner and Claim custody. This is immutable-source advice, not acceptance of the mutable helper or permission to run the driver.

Immutable input is manifest `5cde43f24dcc86eaa8c2017bbc6cb0dc81a29e749d9420d27be2ab4f2e4e140d`, archive `0bcaee68f1b6a058bd358b5096265771d35ba503143ac9f8e255b2c215fad8f4`. The JSON binds seven inspected source files. The additionally authorized mutable `owner_atomic_remove_work.go` was read solely to assess potential reuse; its exact hash appears below and in JSON. No other mutable source was inspected.

## Reachability and actual gaps

`primaryarena/arena.go:198–199` initializes arena metadata and calls `p.SetPrimaryMetadataOwnerV6(&a.metadata)`. `PrepareNativeClaimV6` returns the original Claim before private growth. `Claim.Step:389–395` constructs `NewGrowthForPager(a.pager,c.local+1,w)` whenever `local==a.next`, irrespective of whether mapped capacity already covers that page. Thus a pre-cursor spare-capacity predicate cannot eliminate Growth. A checked predicate proving every requested bank comes from the actual reusable free/freePair supply could eliminate that branch, but no such selected predicate or reservation exists; a count alone cannot safely promise repeated reusable choices. Repair the reachable branch rather than assume absence.

`pager/grow_work.go:54–77`: wrapper Owner.Add precedes NewGrowth Reserve; a work refusal then calls plain Remove. `capture:395–474` computes next/old descriptor backing and calls plain Add(next). `install:477–528` calls plain Remove(old+wrapper) after publication. `Cancel:541–623` clears receiver then plain Remove(all private charge). These are actual binding/lease traversals, not covered by the fixed Growth Reserve rows. DATA's separate metadata=nil proof is inapplicable to this companion.

## Minimal coherent repair

1. **Wrapper birth:** preserve the original NewGrowth Reserve `(1,6*sizeof(Growth)+256)` but execute it before Owner growth. Perform original `AddWithWork(C,w)` (`C=AllocationCharge(sizeof(Growth))`) before allocating. Once both admissions succeed, allocation/init is infallible; eliminate the Add→work-refusal→compensation Remove window entirely. Unsupported WorkLease is a typed refusal; ready=false is a resumable work refusal. Genuine owner budget errors retain original semantics. Do not call NewGrowth and reserve twice. An explicit native origin/entry, propagated from the original native Claim, is the narrow way to preserve ordinary origin behavior and receiver accounting semantics; added fields must be admitted/repriced.

2. **Capture:** change the internal capture result to `(ready,error)` and pass SAME w. Retain existing fixed caller/capture prefixes. Calculate actual original next/old expressions under existing pager guards and admit `AddWithWork(next,w)` before `captured=true` and before any phase allocation. A refusal leaves no new retained charge or captured destination, and the original Claim/Growth remain resumable. Do not translate work refusal into g.failure. Prefer scalar locals/commit only after Add; if original private scalar fields are filled speculatively, document that they own no backing and are recomputed while captured=false. The next charge is added exactly once; metadataOld is an accounting subtraction, never a separately owned private allocation.

3. **Successful install:** existing allocMu→growMu→p.mu and live source/count validation remain authoritative. Preserve the original pager metadataReadMu exclusion **outside** the owner callback. Admit the entire final fixed tail, parent r/b and SAME-owner whole shrink before any pager or Claim callback effect. Use `RemoveWithTeardownWork(old, installScrub, w)` under that original exclusion. The callback performs the original pager swaps/logical count, the original infallible Claim install, marks installed and clears all private destination aliases; it takes no lock, allocates nothing, cannot fail and cannot reenter Owner. Refusal leaves pager count/backing, Claim ref/counters/epoch and g.installed unchanged. New backing remains charged to SAME Owner through its Pager lifetime. Remove **old backing only** here, not Growth wrapper C.

   Do not call the old g.install unchanged inside the callback: it locks metadataReadMu and recursively calls Owner.Remove. Factor only its original infallible effects from guards/accounting. `GrowInstallShrinkWithWork` is not needed for this stage because next was already admitted at capture; using it again with growth=next double-admits. It is reusable only if a deliberately combined transaction relocates the original admission, which is a larger change and unnecessary.

4. **Cancel:** retain existing per-suffix unmap progress, source mappings untouched and sticky genuine cleanup failure. Once mapped==oldChunks, retain final `(1,6*sizeof(Growth)+512)` row and admit SAME-owner whole shrink before resetting any destination aliases/accounting. Atomic teardown clears private vectors/headers, preserves original failure and cancelled state, and removes **next private backing only**. It must not debit metadataOld: old Pager backing was never cancelled ownership. On work refusal keep all final descriptors and their charge; already completed suffix unmaps are genuine progress and are not replayed. On unmap failure retain all remaining charge/custody, mark CleanupFailed as original, and do not force final teardown.

5. **Wrapper last-alias:** c.growth still aliases g until `Claim.finish`/Cancel returns. Refunding C inside Growth.install or Growth.Cancel therefore does not satisfy newly governed private wrapper custody. Keep `metadataCharge=C` on completed native Growth, with metadataOld=0 after install/cancel. Add a fixed admitted original-child accounting detach, available only after installed/cancelled and no physical cleanup failure. SAME native Claim receives `(owner,C)`, verifies owner identity, folds C into its existing metadataCharge, clears c.growth, and only then completes its existing parent handoff. Existing `Claim.DetachNativeAccountingV6` and disposing parent's RemovalWork refund the SAME total after the Claim alias is detached. This is an accounting transfer, **no Add/Remove, no new owner/enrollment/registry**, and must not charge the same wrapper to both owners. If a detach work refusal occurs, retain completed child pointer and C and retry without replaying pager install/unmaps. Precheck sum overflow before irreversible effects; C addition must not later fail. The successful/refused detach protocol must run before existing Claim.finish clears child aliases. Preserve the original fixed-return custody boundary, including the stack receiver surviving its method return; do not refund the wrapper from a callback executing on that receiver.

The install/cancel callbacks are fixed closures, not new retained registries. Any added escaping callback environment or continuation allocation must be explicitly charged; prefer factored infallible methods with original objects, and verify compiler escape/layout separately before accepting a frozen implementation.

## Whole atomic owner admission and repeated prefixes

Use the new helper only with its exact supported shape, under SAME Owner.mu from discovery through effects: controls/amount arithmetic valid; owner not closed/retired/cleanupFailed; no active enrolling/enrollment; every linked lease has original concrete geometry, SAME budget, open state, payload and binding.admitted >= current owner.bytes; each is WorkLease. Owner custody excludes independently closing those leases. No rollback==0 predicate is needed: that is stale resize scratch unused by shrink, as clarified by root a7b4b176.

For B actual bindings, let s(X)=sizeof(X), G=NativeOwnerGeometryForTestV6, E=EnrollmentWork, O=Owner, b=binding, and R=(1,2s(COWExternalLease)+2s(COWBudget)). The inspected helper whole-shrink admission is exactly:

```
S(B) = (2,2s(G)+2s(O)+2s(E))
     + B * [(2,2s(b)+2s(G)) + (1,2s(COWExternalLease)+2s(COWBudget)+2s(leaseGeometry))]
     + (1,2s(O)+2s(RemovalWork)+128)
     + (B+1)*(2,4s(O)+2s(E)+128)
     + B*(B+1)*[(4,8s(b)+2s(O)+256)+2R]
```

The second bracket includes actual concrete lease geometry observation. Discovery and all B+1 possible passes are admitted before debit/effect; each disposable binding can request at most one restart. Preserve overflow checks for B(B+1) and caller vector sums. No generic budget observer or foreign WorkLease fallback.

Wrapper/capture Add uses existing atomic AddWithWork, with forward and rollback lease admissions; reuse the exact A(B,e) from the preceding constructor report. If new atomic shrink refuses unsupported active enrollment rather than resumes it, classify this as a pre-effect supported-shape refusal and preserve original Claim/Growth custody for cancellation or later retry; do not poison physical I/O state merely for unsupported shape/work refusal. The selected empty-cache route still needs source-backed evidence that these helper predicates hold at each call, or a genuine typed refusal result.

Every restarted Growth.Step repeats its original entry `(1,2s(Growth)+1024)`. Initial capture repeats another such fixed prefix plus actual A until success; saved captured=true skips duplicate Add. Ready install repeats entry+final `(2+r,6s(Growth)+2048+b)`+S(B) until all fit; no partially admitted owner resize or visible callback is allowed. Final Cancel repeats fixed receiver-scrub row+S(B) until all fit, after its genuinely persistent per-unmap progress. Original Claim native Step/parent prefixes also repeat. A numeric G cannot substitute for any missing actual admission.

## Required focused controls

- Constructor zero/exact-boundary work with nonnil owner/bindings: no owner delta, no allocation/Claim.growth on refusal; then one admitted allocation and exact C. No compensate-by-plain-Remove path.
- Capture Add refusal then SAME Growth resume; no captured flag/backing allocation/duplicate next charge. Include forward budget failure rollback and unsupported lease.
- Final shrink refusal before both Pager and Claim callbacks; unchanged count/epoch/ref/counters, retained aliases and peak charge. Resume installs once, never replays callback, old backing withdrawn only after metadataReadMu readers drain.
- Cancel after partial new mappings, fixed-end shrink refusal and resume: source mappings untouched, no unmap replay, next charge retained until final alias scrub, C retained for Claim handoff. Genuine unmap error remains sticky/no refund.
- Actual native Claim wrapper handoff for successful and cancelled children: C transfers once into Claim metadataCharge; parent alias detachment precedes original RemovalWork debit; repeated completed child calls are idempotent.
- Ordinary nil-work constructor/install/cancel semantics unchanged, and a finite owner with no bindings is not used as the only test for the nonempty-binding actual path.

No Go or runtime was executed. This report neither accepts new source nor grants any numeric allowance, native publication, Q32 fit or actual final driver.

Potential-reuse helper exact read SHA256: `a872ad2fd9c0e2d640d431417c9af259c3e1fe96cc0d0faf0e81834cfc59c41b`.
