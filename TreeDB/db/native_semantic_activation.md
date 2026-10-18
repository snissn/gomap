Explicit semantic owner activation (construction checkpoint)

`BindPrivateCOWOwner` and ordinary COW Open preserve raw-only eligibility. A
legacy producer can still register its barrier, make raw preflight reject
before callbacks or assignment, unregister, and resume raw writes.

`DB.ActivateNativeSemanticOwner()` forwards to the SAME cache owner. It refuses
retained opening/private cuts, pending logical work, frozen sources, preexisting
semantic state, live legacy producers/barriers, backend staging/build activity,
and an incompatible disk basis. Recovered nonzero canonical LSN is allowed when
the cache basis and sealed accepted frontiers agree. No drain or migration runs.

Lock order is cache writer (TryLock), cut latch, backend teardown (TryLock), raw
publisher (TryLock), barrier registry, existing accepted-prefix latch. Publication
and Close progress holders cause refusal rather than waiting for their release.
The existing registry latch serializes activation with legacy constructor and
barrier registration. Unowned actual new command assignments and ordinary
physical finalization hold a scalar in-flight admission through completion;
authenticated same-owner and accepted-cut paths use original ownership instead.
Already-assigned intents retain exact replay-token and poison validation without
inventing another command. New sync-barrier assignments remain fenced.

Legacy constructors call `AdmitLegacyCommandProducer() (func(), error)` before
any live construction or hook installation, including hook-free constructors.
The once-only release lasts the whole manager lifetime. Activation refuses live
admissions; admission and barrier installation refuse after activation. Legacy
collection constructors have not been migrated in this lower-layer checkpoint.

The cache obtains `LockPrivateCOWStagingGuard(owner)` and uses the original
owner-qualified NoWAL finalizer. Ordinary guards confer no native authority.
`physicalOnly` confers no publication authority; the common finalizer requires
an existing accepted cut after activation. The existing coordinator checks cut
identity and canonical coverage before accepting roots.

OPEN lifecycle discriminator: the real public WAL Close reaches an empty
`RefreshCommandWALCheckpointFallback` after the cache checkpoint. This existing
same-root recovery-slot refresh has no accepted-cut authority and is correctly
refused by the current fence. The new all-profile public control preserves this
failure. No fallback exemption/general maintenance authority is implemented;
this checkpoint is not dependency-ready or a native activation claim.

Idle command-WAL recovery-slot convergence uses the COW maintenance admission
and flush fence. It checks the existing real fallback predicate against the exact
fully sealed cut's complete StateToken. Only a required convergence runs the
existing empty final physical batch with an AcceptedCut, complete build,
preadmitted accepted Snapshot and exact handoff. It neither advances logical
coverage/command identity nor uses the ordinary same-root publisher. The generic
post-maintenance recapture is skipped. Legacy non-COW refresh is unchanged.

Standalone caches without a bound transaction owner retain the ordinary raw
finalizer. The private finalizer still requires the exact nonnil bound owner;
ordinary backend admission continues to reject an activated semantic backend.
Sibling native attachment, transaction and append-guard paths retain their
existing exact-owner checks.

An unsealed suffix admitted after the caller's completed checkpoint defers
optional idle convergence. The route first checks the exact physical basis and
cut V/C correspondence; it neither rejects a valid newer suffix nor seals it.
The next explicit checkpoint owns that suffix's durability boundary. Complete
physical-basis and prefix inconsistencies still fail closed.

Focused and race controls cover standalone raw writes, accepted-reported-error
custody, exact preaccepted Snapshot handoff, active ordinary alias refusal, and
activated public raw Checkpoint/Close/reopen across all profiles. Higher-level
native semantic activation and whole-packet qualification remain separate gates.
