# Snapshot birth uses the original admitted arena lookup

Selected source correction, not an execution or publication grant. Native writer
owns implementation and controls; root owns Git/index/actions. Source stays mutable.

The immutable caching/backend audit found BeginNativeMVCCSnapshotV6 calling
root.arena.Acquire(root.ref, nil) after only fixed reference preflight. Actual
RAM bucket work must use the same finite work. Merely passing w after Snapshot
birth is incorrect: capturePhase 2 resumes registry and would skip a refused
root borrow. A new partial source-view lifetime is not selected.

Factor the original arena Acquire core to support one narrow admission callback
(e.g. AcquireAfterAdmissionV6). Under the SAME arena.mu, reserve its fixed row,
use existing validWithWorkV6 for the actual table/links, validate the original
bank, then invoke admission. Callback error/refusal leaves bank refs/counters
unchanged. On callback success, increment that SAME bank/counters nonfallibly;
there is no second lookup, returned permit, substitute index or stored callback.
Ordinary Acquire retains nil callback / nil work behavior. Document callback
non-reentry into this arena and no escaped operand. The selected callback is
ONLY the existing budget/enrollment admission, not manager/index/leaf work.

In BeginNativeMVCCSnapshotV6 hold the original primaryOwner.mu before the arena
operation; prevalidate same arena and refs in [1, max-1]. Keep existing DB
maintenance/publication/root/state exclusions. If admission or lookup refuses,
return before Snapshot, epoch or borrow effects; the original caching capture
continues to own any partial lease/enrollment. After successful acquisition,
consume the prevalidated owner ref with original retainLockedV6 and immediately
construct/initialize the admitted Snapshot, attach BOTH primaryOwner/primaryRoot
and initialize its embedded release queue before releasing owner.mu. There is
no fallible call, work refusal, or outward return between successful bank
acquisition and complete containing custody. Manager/index/leaf borrows occur
afterwards under existing publication exclusions and set flags immediately.
A no-primary-root capture retains original admission/birth behavior.

Lock order: physical primaryOwner.mu -> arena.mu -> original capture enrollment
pair/state locks -> metadata Owner.mu -> concrete COWBudget.mu. The actual
admitOwnedSnapshotV6 callback only AcquireRetentionWork and BeginEnrollmentPair
and scalar assignments; these do not call arena or physical primary owner.
Existing arena metadata growth already takes arena before metadata owner.
External/reentrant callbacks must honor the documented no-reentry contract;
current repository callers are owned capture and simple admission tests. Do
not move manager/index/leaf locks into the arena callback.

Controls must force actual collision lookup refusal before admission callback,
Snapshot/epoch/borrow effects; admission callback rejection must leave bank and
physical-owner refs unchanged; a sufficient call must attach both original
edges exactly once, support registry continuation, and cancel through the SAME
native finalizer with no leaked edges. Retain ordinary behavior. Update typed
source bound with actual callback plus lookup prefix; no observed counter may
choose the new fixed G.
