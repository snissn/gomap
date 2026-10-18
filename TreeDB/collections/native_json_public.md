# Experimental native JSON Create checkpoint

This source is an integration discriminator, not an accepted collection mode or
completion of issue #5142. `NewNativeJSONManager` explicitly selects the same
cache/backend owner and exposes a restricted `CreateCollection` operation. It
never embeds a legacy manager. Legacy manager construction first acquires the
same backend's producer admission; refused managers retain a named error and
expose no writable collection. Existing singleton startup replay remains the
canonical dispatcher, with scoped admitted manager lifetime.

`TestNativeJSONPublicCreateActivation` now passes actual public Create/capture,
public checkpoint, an ordinary public raw Set/Get with unchanged native catalog,
another checkpoint, an idle checkpoint, public Close, and final original cache
accounting zero. It checks one logical advance and canonical applied LSN 1→2
for the raw write. This is a lifecycle proof, not collection row read/write or
recovery coverage. The earlier public checkpoint refusal remains recorded in the
historical first discriminator log. The larger selected test
`TestNativeJSONPublicCreateInsertCapturedViewCheckpointReplay` is not implemented.
InsertBatch/Get, held old reads across insertion, suffix handoff, canonical WAL
crash recovery, and import of recovered native catalog identities remain open.

Known source obligations before acceptance:

- Preserve the lower exact accepted-snapshot invariant: both production
  PrepareHandoff calls receive the same group's preaccepted h.basis Snapshot.
  The upper adapter does not recapture or introduce separate identity authority.
- Prove complete native projection/resource coverage; nil SparseProjections is
  currently specific to the initial JSON construction, not all-domain evidence.
- Route all original private cleanup errors/custody through the original owner.
  Some scratch cleanup still discards errors; no failure-custody claim is made.
- Admit transient planner/effect/payload allocations and public retained control
  storage before allocation with the same finite original budget.
- The newly shared create identity/counter effects need concurrent different-name
  legacy-create exclusion/revalidation; a counter sampled before publication is
  insufficient. Non-WAL legacy create branches also still omit the shared
  identity updates. Sequential singleton replay alone does not prove parity.
- Complete every hook-free replay manager's lifetime beyond the selected JSON
  handler scopes. Held legacy admission must continue to block native activation.

Empty physical frontiers now create fresh immutable successor headers with no
build ledger, using both independently charged headers reserved before acceptance.
The original captured build remains single-use. Required two-successor capacity
continues to use the same maintenance promise, including after budget Close.

No performance gate, full public JSON proof, recovery, or readiness claim follows
from this lifecycle checkpoint. No production release or merge is authorized.
