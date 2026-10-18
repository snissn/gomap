# Experimental native JSON Create checkpoint

This source is an integration discriminator, not an accepted collection mode or
completion of issue #5142. `NewNativeJSONManager` explicitly selects the same
cache/backend owner and exposes a restricted `CreateCollection` operation. It
never embeds a legacy manager. Legacy manager construction first acquires the
same backend's producer admission; refused managers retain a named error and
expose no writable collection. Existing singleton startup replay remains the
canonical dispatcher, with scoped admitted manager lifetime.

The first actual public Create and capture passed in
`TestNativeJSONPublicCreateActivation`. The actual public cached checkpoint
failed with `ErrNativeSemanticUnsupported`. Public Close and final accounting
zero were not reached. The larger selected test
`TestNativeJSONPublicCreateInsertCapturedViewCheckpointReplay` is not implemented.
InsertBatch/Get, held old reads across insertion, suffix handoff, canonical WAL
crash recovery, and import of recovered native catalog identities remain open.

Known source obligations before acceptance:

- Bind handoff to the exact accepted snapshot using original build receipt
  authority. Merely checking a nonnil snapshot is insufficient.
- Prove complete native projection/resource coverage; nil SparseProjections is
  currently specific to the initial JSON construction, not all-domain evidence.
- Route all original private cleanup errors/custody through the original owner.
  Some scratch cleanup still discards errors; no failure-custody claim is made.
- Admit transient planner/effect/payload allocations and public retained control
  storage before allocation with the same finite original budget.
- The newly shared create identity/counter effects need concurrent different-name
  legacy-create exclusion/revalidation; a counter sampled before publication is
  insufficient. Sequential singleton replay alone does not prove this boundary.
- Complete every hook-free replay manager's lifetime beyond the selected JSON
  handler scopes. Held legacy admission must continue to block native activation.

No performance gate, full public JSON proof, recovery, or readiness claim follows
from the first Create. The known lower owner activation/Close/fallback defect is
also unaccepted. No production release or merge is authorized by this checkpoint.
