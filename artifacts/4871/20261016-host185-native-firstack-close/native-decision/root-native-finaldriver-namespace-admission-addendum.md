# Native namespace constructor admission selection

Source-only addition to the active finaldriver source lease; no Go/runtime,
publication, index mutation or freeze is granted by this note.

The immutable 5cde43 source audit proves two genuine reachable gaps in
TreeDB/internal/rootpublication/resource_token.go:

- NewOwnedStableNamespaceCreationProofWithWorkV6 reserves its local fixed
  constructor work, then calls newStableNamespaceCreationProofWithMetadataModeV6
  whose metadata.AddPending does not charge original owner bindings.
- BindWithMetadataWithWorkV6 calls bindWithMetadata, which likewise uses
  metadata.AddPending and defers ordinary RemovePending on duplicate failure.
  Both DATA and PRIMARY captures execute Bind even when their proof is cached.

The sole native writer shall thread the existing optional work policy to these
same constructor owner additions and use AddPendingWithOrdinalWork. Preserve
nil-work ordinary behavior, exact rounded retained charges, original handles,
and existing cached-proof semantics. Budget refusal must have no allocation,
handle, owner or publication effect and remains a yield rather than a fatal
capacity error; unsupported concrete owner capabilities remain errors.

For creation, the existing preborn proof and pendingProof capture already retain
post-admission failure custody; keep using them. Do not add synchronous native
refund after charge succeeds.

For native Bind, admit then construct the SAME namespace token before the
fallible duplicate. Store its original metadata/charge and independent reference
edge immediately. A failed duplicate returns that partial token with its error,
and nativeStablePagerCaptureV6 must retain it in its existing namespace field
before propagating the error. Its existing ReleaseWithWorkV6 and original refund
completion consume that exact partial token. A missing duplicated handle is an
ordinary partial-construction state in that disposer. Do not use a replacement
cleanup object, permit across unlock, unmetered refund or ordinary Release.

BindCleanupRegistry is excluded as a growth seam on this selected route by the
required nonnil scoped metadata owner; it only attaches the original registry.
The final index token initializer has no nonempty exact-RID/obligation inventory
on this accepted route. Do not widen these unrelated ordinary paths.

Add focused controls for pre-effect owner-admission refusal, unsupported binding,
successful original charge/custody, injected duplicate failure retaining the
partial token and once-only finite cleanup, plus nil-work ordinary behavior.
The selected fixed progress envelope must include the namespace local constructor
row plus the complete actual owner-add prefix. No numeric allowance is accepted
until all source families and real precursor operands are composed.
