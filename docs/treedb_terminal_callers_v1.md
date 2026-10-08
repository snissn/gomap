# Checked terminal publication callers (staging)

Finite admission and the complete 128 MiB publisher certificate remain CLOSED.
This component changes actual caller ownership and ordering; it does not certify
finite constructors, generic exports, producer loans or a whole-process bound.

The selected DB reporter uses one synchronous call-local consumer. Publish
completes storage without consuming the allocator prefix. The reporter reserves
one complete group containing coordinator prefix sets, the overwritten durable
slot, retired seals and tokens, and each member's replaced visible closure.
Borrowed base slots and the adopted current visible closure are aliases, not
additional cleanup ownership. Preparation precedes the exact allocator publish
and logical Consume; physical cleanup runs after all engine locks are released.
The consumer ends before ACK. It is never stored in a candidate or coordinator.

Activation retains one previous-visible pointer on each actual member and
returns success after visible installation. Consume marks logical completion
once and keeps cleanup fields until checked release. A committed seal keeps
its overwritten owner and retired seals attached until cleanup completes.
Storage-complete refusal retains unconsumed recovery authority. Committed
cleanup failure retains the new durable slot and real unfinished fields;
cleanup-only recovery must never restore the old base or reconsume COW.
Ordinary Manager.Release consumes its Set reference before zombie deletion IO;
Abort clears that consumed Set even if IO fails, returns the error, and leaves
the actual zombie/retry ownership on the existing Manager. It never retries a
reference decrement through an already-consumed Set alias.

The direct candidate path has the same preparation before allocator publish,
and an explicit committed-cleanup phase held by the actual DB pending field.
Its executing reservation remains attached through consumer End while gate
release and synchronous cleanup occur; competing execution refuses rather than reconsuming COW.
The unlocked direct entry owns durablePublishMu/rootReuseMu and releases both
before cleanup. Finalization also releases its inherited write/commit
serialization through the existing release contract. Legacy callers lacking
that contract retain ordinary behavior and refuse finite metadata before any
publication effects. Visible-install failure after COW remains accepted and
retains the new durable authority and exact unfinished candidate.
Close retains runtime and recovery handoff on the DB, passes the saved exact
Manager even after clearing its public DB field, and attempts one complete
group before actual Manager.Close. Admission is closed and producers drained;
Close releases maintenance/teardown gates around each terminal helper and
reacquires them in original order before continuing producer/index teardown.
Completed parent close permits closed-cell
metadata-only cleanup. Allocator detach and namespace-link clearing follow
resolved terminal ownership. Any unresolved owner remains attached to DB.

Selected worker cleanup records observable poison/background error without
calling arbitrary notification callbacks: DB.Close/Coordinator.Stop would
otherwise join the worker invoking the callback. Ordinary generic callbacks
retain their existing semantics.

## Actual allocation census and remaining finite construction boundary

The member now contains an additional set pointer and consumed flag; the seal
and direct candidate each contain an overwritten-set pointer and committed
phase (seal also has storage-complete phase and candidate has executing phase). terminalCallerBackingCensusV1 uses
unsafe.Sizeof of these actual types and the shared Go1.26.3 scan classes, one
class per control instance, plus separate full-capacity classes for role and
token arrays. Counts must be actual instances and capacities, never lengths or
rounded aggregate bytes.

Complete-group finite token scratch is prepaid before make; failed preparation
does not refund births. DB role collection and caller controls are currently
ordinary construction. Their append growth, recovery-handoff copy, transient
adapter escape class and all originating controls still require constructor
prepayment and lifetime census before finite caller construction can be admitted.
The numeric helper is an inventory only, not a reservation/loan certificate.
No new queue, persistent callback, account exemption or extra tranche is added.
