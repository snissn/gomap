# Selected retained constructor ledger: design, not a total or allowance

Independent read-only gpt-6.1-sol/high. Immutable23e manifest `23e37a210f156838de8adecbf8532c0cd16cd12f6d6c37fb05529b7974d0869f`, archive `ea996ddc0c637f7674efded9f0611b3d2108554b460ea35d9859e6b47cf25293`,10377 paths; root-recorded tree `52ca5ef561f6d27928356ca2f836e722be7c4b31`. Extracted cited source was rebound to that manifest. This extends accepted advice274f; it does not inspect the active hash/clone implementation. No source/runtime/Git changes, children, numeric allowance or native publication acceptance.

**Decision:** the following is an evaluable *constructor admission-envelope ledger* once its explicit missing scalar facts are supplied. Existing23e cannot yet emit that complete ledger, even for selected empty resources. This is not an exact allocator/RSS total. The closure domain is the SAME selected actual33 prebuilder's governed private allocations and separately named held-root envelopes; it excludes arbitrary shared-owner graphs, canonical DATA output and existing legacy published GC/service domains. It does not widen their admission policy.

Let `Q(n)=retainedalloc.AllocationCharge(n)`, with each independent allocation rounded separately and zero-length backing treated as0. Checked addition/multiplication is mandatory. Admission slack may conservatively exceed instantiated storage; identify it rather than presenting it as allocator exactness.

```
P = C + sum distinct leaseHeaderCharges(outer,capture) + Q(appSnapshot) + E_capture + I + O + A_ret + A_clone + A_hash + A_sort + T + S + R_commit + R_ret + sum distinct SetCharge + sum distinct KindCharge + sum distinct index(WrapperCharge+OperationsCharge+CustodyCharge+TokenCharge+ObligationCharge+PhysicalCharge+NamespaceCharge) + sum distinct selectedPinStateCharge
P_named_roots = P + sum distinct selected bank.metadataCharge
```

`E_capture` is P04's actual distinct enrollment/binding/concrete-lease header sum. All aggregates below include their constituent independent headers unless expressly separated. H02/H03 are reported in a separate shared-service column, **not added** to this private expression. Neither Owner.Bytes nor lease payload is a disjoint physical row. The expression suppresses embedded controls and transferred aliases. Therefore P is not a claim to the complete transitive heap reachable from the captured DB.

## Constructor rows and last-consumer rules

### P01 — Outer cursor, embedded controls, batch-entry backing, selected copied payload, charged projection and codec backing

- Source: `TreeDB/mvcc/prune_cursor_v6.go; TreeDB/tree/charged_projection.go`.
- Charge: `C = c.retainedBytes (constructor admission aggregate)`; field: `c.retainedBytes; batchPayloadCharge is a subset`.
- Last alias: Original cursor owns these; copied batch key/value remains until every prepared/input borrower is detached. Projection closes before retained lease closes.
- Suppression: Do not add sizeof embedded capture/preparations/record/frames again. Do not add batchPayloadCharge again. Do not add outer lease payload to C. Allowance/slack is a governing bound, not exact allocator bytes.
- State: source-defined; missing scalar export where noted.

### P02 — Concrete outer and application-capture retention lease headers

- Source: `TreeDB/caching/cow_cut.go; TreeDB/caching/native_mvcc_prune_v6.go`.
- Charge: `Q(sizeof(COWExternalLease)) for each distinct live concrete lease`; field: `Concrete header charge, distinct from admitted payload`.
- Last alias: Each original lease closes only after its dependent original objects close.
- Suppression: No payload reservation added to physical rows; outer and capture leases are separate. Missing concrete same-lease predicate must fail closed for unexpected sharing.
- State: source-defined; missing scalar export where noted.

### P03 — Application Snapshot wrapper

- Source: `TreeDB/db/native_mvcc_snapshot_v6.go`.
- Charge: `Q(sizeof(Snapshot))`; field: `Capture wrapper constructor charge`.
- Last alias: Original capture holds Snapshot; metadata enrollment follows genuine snapshot close.
- Suppression: Captured DBState is borrowed, not newly allocated. Inline first tree is embedded. Selected rootTrees capacity0/map nil; overflow storage makes selected-only ledger incomplete. Original published DBState/view remain accepted legacy GC outputs.
- State: source-defined; missing scalar export where noted.

### P04 — Capture enrollment objects, original owner/budget bindings, and binding concrete lease headers

- Source: `TreeDB/internal/retainedalloc/owner_enroll_work.go; TreeDB/internal/retainedalloc/owner.go`.
- Charge: `sum distinct Q(sizeof(Enrollment)) + sum distinct Q(sizeof(binding)) + sum distinct Q(sizeof(concrete lease))`; field: `Enrollment/control constructor delta; binding.lease`.
- Last alias: Capture result owns first Enrollment and companion; binding survives until actual last enrollment/producer cleanup; failed cleanup retains it.
- Suppression: EnrollmentPairWork is embedded. Owner.controls covers Enrollment/binding headers already; do not add it to these rows. Binding lease payload mirrors Owner coverage and is not storage. Reused bindings count once. Require ready/no pending redundant temporary lease.
- State: source-defined; missing scalar export where noted.

### P05 — Materialized input header, entry backing and independently copied owned payload

- Source: `TreeDB/zipper/materialized_input_work_v6.go`.
- Charge: `I = input.charge`; field: `input.charge`.
- Last alias: Input/output borrow relationships are detached before input cleanup/refund.
- Suppression: entries capacity diagnostic is already included; prefix/deletes/source certificate borrowed. Do not add c.records or unsafe one-record slice.
- State: source-defined; missing scalar export where noted.

### P06 — Materialized output header and linked original frames

- Source: `TreeDB/zipper/materialized_write_work.go; TreeDB/zipper/materialized_observer_for_test_v6.go`.
- Charge: `O = output.charge`; field: `output.charge`.
- Last alias: All original linked frames remain until output CloseStep after transfer or cancel cleanup.
- Suppression: FrameRaw, head and each frame ops slice are not extra: all frames are in O; frame ops borrow input entries. Canonical DATA page outputs remain in their existing domain.
- State: source-defined; missing scalar export where noted.

### P07 — Allocator retired-ID vector

- Source: `TreeDB/db/native_mvcc_allocator_preparation_v6.go`.
- Charge: `A_ret = Q(cap(retired)*sizeof(uint64))`; field: `allocatorPreparation.charge`.
- Last alias: retirements[0].PageIDs and private preparation borrow vector; detach before typed removal.
- Suppression: Containing allocator preparation and retirements[1] are embedded in cursor.
- State: source-defined; missing scalar export where noted.

### P08 — Captured FreelistTxn header and exact allocated/abandoned clone vectors

- Source: `TreeDB/freelist/allocator_preparation_work_v6.go; TreeDB/freelist/allocator_activation_work.go`.
- Charge: `A_clone = Q(sizeof(FreelistTxn)) + Q(cap(private.allocated)*sizeof(allocatedPage)) + Q(cap(private.abandonedAppends)*sizeof(ReservationExtentV1))`; field: `NEW same-owner headerCharge/allocatedCharge/abandonedCharge; names are design placeholders`.
- Last alias: All materialize/prune/inventory/planned aliases clear before typed original removal. Adoption briefly installs this clone; original activation replaces it with a different successor. Cleanup must not refund the canonical successor.
- Suppression: Q per independent allocation, zero backing0. Clone vectors are not the new map-key capture vectors. Existing23e only work-prices these three allocations: retained admission missing.
- State: missing in23e; root-selected current clone addendum, not inspected.

### P09 — Four private uint64 hash controls and exact source key-capture vectors

- Source: `TreeDB/freelist/allocator_preparation_work_v6.go; TreeDB/freelist/generation_v1_materialize_work.go; TreeDB/freelist/generation_v1_materialize_output_work.go; TreeDB/freelist/generation_v1_format.go`.
- Charge: `A_hash = sum all distinct live current/new/snapshot vector Q(capacity*sizeof(uint64)) + any independently allocated typed table header charge`; field: `NEW per-control currentCharge/newCharge/captureCharge; parameterized`.
- Last alias: Growth retains old+new until rehash completes and old aliases clear; cancel/adopt disposal retains original removal progress.
- Suppression: Embedded table controls have no separate heap header. Scalar detached refund debt is not live array backing. No len(map) approximation; all four original private maps must be nil/not used. Zero key presence bit is embedded.
- State: opaque in23e; active successor not reviewed.

### P10 — Two normalization scratch backings

- Source: `TreeDB/freelist/generation_v1_reservation_inventory_work.go`.
- Charge: `A_sort = sum distinct normalize.scratchCharge`; field: `scratchCharge in materialize.inventory.normalized and materialize.reuse.inventory.normalized`.
- Last alias: Original/scratch aliases swap; clear both before typed scratch refund.
- Suppression: Only independently allocated second vector is new scratch. Original canonical inventory entries remain original domain; do not count borrowed swapped backing twice.
- State: source-defined; missing scalar export where noted.

### P11 — Transaction, construction and terminal headers; visible member/install/callback allowance; promotion header/image

- Source: `TreeDB/internal/rootpublication/primary_transaction_construction_v6.go; TreeDB/db/native_mvcc_candidate_preparation_v6.go; TreeDB/db/primary_runtime_metadata.go`.
- Charge: `T = construction.metadataCharge at selected completed construction phase`; field: `construction.metadataCharge`.
- Last alias: Original transaction/receipt/terminal graph keeps charge until genuine terminal completion and last alias withdrawal.
- Suppression: T includes Q(transaction)+Q(construction)+Q(terminal), transferred candidatePreparation.charge, promotion header and image. Candidate, members[1], release queue and callback captures are represented by this containing/constructor envelope; do not sum member/install rows again. Terminal.charge is an accounting alias. Candidate phase1 includes primaryVisibleMetadataV6 plus FIVE additional callback-environment allowances and install; primaryVisibleMetadataV6 itself includes FIVE callback allowances plus 3-pointer allowance: preserve actual conservative formula, do not silently halve it.
- State: source-defined; missing scalar export where noted.

### P12 — Seal descriptor headers and prefix backing

- Source: `TreeDB/db/native_mvcc_seal_preparation_v6.go; TreeDB/db/primary_runtime_metadata.go`.
- Charge: `S = seal.metadataCharge = primarySealMetadataV6(prefix count)`; field: `seal.metadataCharge`.
- Last alias: Retained original seal/receipt/capsule dependencies persist through actual Finish or safe abort.
- Suppression: Includes Q(rootPublicationSealV1)+Q(primaryPublicationSealV5)+Q(prefix capacity*sizeof(uintptr)); embedded seal preparation is not an extra allocation.
- State: source-defined; missing scalar export where noted.

### P13 — Commit runtime containing header/capacity envelope and commit-retired vector

- Source: `TreeDB/db/native_mvcc_seal_preparation_v6.go; TreeDB/db/primary_runtime_metadata.go`.
- Charge: `R_commit = commitRuntime.metadataCharge; R_ret = seal.commitRetiredCharge`; field: `metadataCharge; commitRetiredCharge`.
- Last alias: Transferred/runtime/receipt aliases retain the original capacities until genuine cleanup.
- Suppression: Commit runtime differs from existing publication runtime. Do not add raw header or arrays already in metadataCharge. Consumed debt prefix is not the original allocation size.
- State: source-defined; missing scalar export where noted.

### P14 — Selected Set wrapper allocations

- Source: `TreeDB/internal/rootpublication/resource_set_allocation.go; TreeDB/db/native_prebuilder_observer_for_test_v6.go`.
- Charge: `sum distinct roles {0,1,4} set.metadata.charge`; field: `SetCharge`.
- Last alias: Original current/candidate/clone/receipt edges control final Set alias scrub and physical completion.
- Suppression: resourceAllocation header is INCLUDED. Require roles2/3 absent and selected empty shape; direct role1-vs4 equality is missing. Other pair tests already distinguish each from role0.
- State: source-defined; missing scalar export where noted.

### P15 — Shared selected kind backing

- Source: `TreeDB/internal/rootpublication/resource_kind_allocation.go`.
- Charge: `sum distinct kindBacking.allocation.charge`; field: `KindCharge`.
- Last alias: Shared backing survives all three Set wrappers until real final backing reference.
- Suppression: Observed three wrappers share one backing: count once. Includes kind backing, cells/owned strings and resourceAllocation header. Selected cells0 is not header0.
- State: source-defined; missing scalar export where noted.

### P16 — Index capture Snapshot wrappers, pager-owned operations objects/path copies, cleanup custody headers

- Source: `TreeDB/db/durable_root_runtime.go; TreeDB/db/native_stable_pager_capture_v6.go; TreeDB/db/stable_pager_owned_operations.go`.
- Charge: `sum DATA,PRIMARY (WrapperCharge + OperationsCharge + CustodyCharge)`; field: `Existing capture geometry fields`.
- Last alias: Original captured Snapshot/operations/arena owner and custody follow same retained file and receipt; clear before final physical completion/refund.
- Suppression: Two wrappers distinct, also distinct from application Snapshot. No separately allocated DBState in these constructors. Capture/control/frame structs embedded in seal/cursor are suppressed. Operations path already in OperationsCharge.
- State: source-defined; missing scalar export where noted.

### P17 — Index token and exact copied descriptor metadata

- Source: `TreeDB/internal/rootpublication/index_token_construction_work_v6.go; TreeDB/internal/rootpublication/resource_token.go; TreeDB/internal/rootpublication/resource_allocation.go`.
- Charge: `sum distinct token.metadata.charge`; field: `TokenCharge`.
- Last alias: Original Set/receipt/capture completion retains token metadata through physical failure or genuine completion.
- Suppression: Includes resourceAllocation header, owned descriptor strings and IdentityPin header per ownedTokenMetadataCharge. Do not add IdentityPin header again. Selected directory/RID/logical ropes/generic diagnostics absent; unexpected nonempty row is incomplete.
- State: source-defined; missing scalar export where noted.

### P18 — Empty index-token obligation backing and its allocation header

- Source: `TreeDB/internal/rootpublication/index_token_construction_work_v6.go; TreeDB/internal/rootpublication/resource_allocation.go`.
- Charge: `sum distinct token.obligationBacking.allocation.charge = Q(sizeof(resourceObligationBacking))+Q(sizeof(resourceAllocation)) for selected empty constructor`; field: `MISSING token obligation charge scalar`.
- Last alias: Original token obligation backing reference controls final clear/removal.
- Suppression: This is separate from TokenCharge even with zero obligation entries. Not the empty Set kind backing.
- State: known constructor allocation omitted by current index-token observer.

### P19 — Retained duplicated index-file physical backing

- Source: `TreeDB/internal/rootpublication/index_token_construction_work_v6.go`.
- Charge: `sum distinct token.physicalBacking.charge`; field: `PhysicalCharge`.
- Last alias: Original physical completion closes retained file before releasing this charge; failure remains charged.
- Suppression: Includes original retained-file metadata charge and physical header; do not add dup-file/path wrapper allowance again. Equal physical identity does not imply equal allocation.
- State: source-defined; missing scalar export where noted.

### P20 — Token namespace object, retained namespace file/name/path storage

- Source: `TreeDB/internal/rootpublication/resource_token.go; TreeDB/internal/rootpublication/index_token_construction_work_v6.go`.
- Charge: `sum distinct namespace.metadataCharge`; field: `NamespaceCharge`.
- Last alias: Original token namespace references retain it through final cleanup; refs are not allocation multiplicity.
- Suppression: Two-role direct namespace/backing identity determines dedup. Namespace proof/parent on original index owner is a separate original shared domain, not automatically included in token NamespaceCharge.
- State: source-defined; missing scalar export where noted.

### P21 — Selected physical-pin registry state headers

- Source: `TreeDB/internal/rootpublication/pin_registry.go`.
- Charge: `sum distinct Q(sizeof(identityPinState)) for two original physical pin states`; field: `MISSING selected state charge/accessor`.
- Last alias: Original selected pin keeps its state live until actual final pin/observer release and registry unlink.
- Suppression: IdentityPin header is already P17; registry table cell/backing is separate. Physical-projected identity is key; full generation token identity stays intact. Require state.zero==nil or report its independent channel backing unavailable.
- State: known maintained owner admission; selected state scalar omitted.

### H01 — Named held source/parent and target root-bank constructor envelopes

- Source: `TreeDB/internal/primaryarena/retention.go; TreeDB/internal/primaryarena/read_root_v6.go; TreeDB/internal/rootpublication/primary_data_certificate_v6.go`.
- Charge: `sum distinct selected bank.metadataCharge`; field: `BankMetadataCharge`.
- Last alias: SAME source Ref retained through unfinished receipt; target certificate/root edges retain target. Final bank release follows last real root/reader/slot edge.
- Suppression: Source=parent counted once; target distinct. Includes bank/header/image/certificate/root metadata descriptor allowances incl PreparedPrimaryRoot/DATA certificate and primaryStateRoot. Do not add those fields or copied image raw bytes again. Reused proof pointer can coexist with fresh conservative allowance; not exact heap bytes.
- State: source-defined; missing scalar export where noted.

### H02 — Named original shared publication runtime allocation envelope

- Source: `TreeDB/db/root_publication_activation.go; TreeDB/db/primary_runtime_metadata.go`.
- Charge: `existing runtime.metadataCharge (shared row, not selected-private sum)`; field: `runtime.metadataCharge`.
- Last alias: Original runtime owns its full current capacities and debt backing until original cleanup.
- Suppression: Separate from P13 commit runtime; header/capacity rows suppressed. Shared mutable; excluded from immutable P sum. Any claim to a contemporaneous selected-plus-runtime bound requires original runtime.mu sample and explicit interpretation.
- State: source-defined; missing scalar export where noted.

### H03 — Named existing arena/registry baselines, root table and registry tables, namespace-parent/proof and original arena owner wrapper

- Source: `TreeDB/internal/primaryarena/arena.go; TreeDB/pager/primary_read_root_v6.go; TreeDB/internal/rootpublication/pin_registry.go; TreeDB/db/primary_arena_owner_v5.go`.
- Charge: `Existing maintained original admission domains; report separately, not an invented residual`; field: `Owner.Baseline / original constructor fields / table charge fields`.
- Last alias: Original shared service owns them; selected edge protects required node/state, not all unrelated service allocations.
- Suppression: No traversal or summing arbitrary Owner graph. Root head backing already included in pager baseline; nodes separate. Count one selected node per distinct bank only with exact local ref-to-node proof; total NodeCount is NOT per-bank multiplicity. Registry tables local-lock samples can change between groups. Original parent/proof backing needs local explicit charge if a named held-shared bound includes it.
- State: outside private P; complete physical/shared-total claim unavailable.

## Minimum bounded scalar accessors and two-role facts

These are suggested signatures, not an authorization to add a new observer framework. Extend existing observers where possible. Every accessor must preadmit its actual reads/comparisons, return fixed scalars only, preserve the original locks/custody, and return `ready=false` or `complete=false` on refusal/unknown storage. No allocation, admission, retain, discovery side effect, pointer escape or synthetic owner is needed to price an existing object.

1. `COWPreparationWorkV6.MaintenancePrivateLedgerForTestV6(w) (PrivateLedgerV6, ready bool)`: three clone charges/caps, four-control current/new/capture charges and capacities, two normalization scratch charges, phase/old-map-absent/no-unknown-backing/cleanup-debt flags. Each pending scalar refund is classified separately from live backing. The active typed controls may expose these through their existing containing preparation.
2. `StableResourceToken.MaintenanceAllocationLedgerForTestV6(w) (TokenLedgerV6, ready bool)`: existing token/physical/namespace charges plus **obligation backing charge**, flags for selected-empty ancillary storage, and selected physical pin-state charge with no-zero-channel flag. Pin-state inspection must use the existing registry mutex and the original pin, not lookup an invented token or traverse every entry. The token's full generation identity and physical-projected pin identity remain distinct.
3. A package-local two-token comparison `maintenanceSelectedTokenPairV6(a,b,w) (TokenPairFactsV6, ready bool)`: SameTokenMetadata, SameObligationBacking, SamePhysicalBacking, SameNamespace, SamePinState, SameRegistry. Scalar identities only, no returned handles. Equal physical identities/strings or charges do not prove allocation identity. Existing genuine DATA/PRIMARY binding checks stay mandatory.
4. `NativeMVCCCurrentDeleteV6.MaintenanceSelectedRoleFactsForTestV6(w) (SelectedRoleFactsV6, ready bool)`: direct **Set role1=role4**, every present Set's same kind-backing relation, distinct application/DATA/PRIMARY wrappers, output Owner=registry Owner, and expected no-extra-root-cache/map/scope flags. Existing role-vs-candidate comparisons remain; do not turn unequal-to-candidate twice into equality. Compare original pointers locally without exporting them.
5. Original enrollment scalar accessor `Enrollment.MaintenancePairLedgerForTestV6(budget, firstOwner, secondOwner,w) (EnrollmentPairLedgerV6, ready bool)`: root/companion header charges, binding equality and header charges, concrete binding lease header charges, same budget, completed result, no pending temporary lease, no unreported enrollment/growth continuation. These parameters are already held original operands and must not be retained by observation. A two-owner pointer comparison does not require scanning all bindings; inspect the original selected enrollment bindings under their existing exclusion. Do not sum all Owner.Controls as if it were only this capture.
6. Extend transaction/bank rows with scalar coverage/transfer flags: completed constructor phase, member/install allowance transferred exactly once, promotion in aggregate, bank source=parent/target distinctions, prepared proof/root descriptor allowance included. Existing charges already provide the conservative aggregate; no callback graph walk is needed. If a named bank-node extension is desired, add one bounded original Ref-to-node identity/charge row; do not multiply table-wide NodeCount by each bank.

All pairwise comparisons are over the fixed roles already held by this attempt. They need no general identity registry, pool, owner, global lock, map traversal or cardinality assumption from33/80/4096. Their observer traffic and enlarged containing headers must be repriced; neither this design nor historical F367 supplies the new price.

## Stability and observation interpretation

The selected fixture has one synchronous preparer. Before emitting P, assert the SAME request/capture/transaction, completed relevant constructor phases and no adoption/cancel/close in progress. Retained original edges keep private input/output/transaction/seal/token/Set backing alive. Private arrays cannot grow/reset or transfer during the selected report interval; if any original owner can mutate them concurrently, that row is incomplete until a source-supported original exclusion or immutable upper-bound field is available. Future ordinary writes may mutate the original live allocator transaction; this does not permit mutation of the captured clone vectors. Growth-overlap charge and still-pending cleanup stay explicit.

The selected application Snapshot and both stable index captures are separate wrappers. Embedded cursor fields do not escape as additional allocations. Certificate/image/descriptor and transaction/member/install relationships use the original constructor aggregates rather than pointer reachability guesses. New legacy published DBState/snapshotView outputs do not yet exist at this prebuilder point. Future activation allocates them only in the original published-GC output boundary; do not claim a per-header private refund or prebuilder charge for them.

Owner totals, registry tables, pager tables and publication runtime capacities are shared mutable. Their existing local mutexes can provide a truthful local sample, but sequential local samples do not establish one atomic global heap instant. Before/after equal counters do not rule out ABA. A bank's immutable held image/certificate stays alive by its original Ref, while its aggregate may conservatively include unrelated/fresh descriptor allowance; label that coverage bound honestly. No new global exclusion is recommended.

A SAME-budget Stats call can separately expose a finite policy ceiling/admission snapshot after coverage is proven. It includes duplicated reservations/other work and cannot replace missing constructor coverage or become a disjoint physical sum. Do not sum Owner.Bytes, Owner.Pending, Owner.Controls, per-lease payload and the P rows: these overlap. Owner.Pending/Controls are subsets, not extra allocations.

## Concrete incompletes and smallest next step

- Ready constructor phase and immutable-private-lifetime proof for selected interval; no adopt/cancel/close/growth concurrently on SAME cursor.
- Original clone 3 charges and four typed hash/capture-vector charge fields after selected implementation, including peak old+new and pending cleanup, no opaque map fallback.
- Direct Set role1-versus-role4 identity; token/namespace/backing pair identities and index-wrapper-versus-application-wrapper distinctions; outputOwner-versus-registryOwner identity.
- Token obligation allocation charge, selected physical registry state header charge/zero-channel flag, and two-enrollment/binding/concrete-lease header facts.
- Scalar coverage/transfer facts for transaction aggregate, bank allowance suppression and private normalization scratch. Unexpected secondary storage must be incomplete.
- A whole transitive physical heap union or a cross-group atomic shared-capacity total is NOT supported by23e; it is not required to emit the bounded private constructor ledger.


Select only these scalar constructor fields/role facts within the coherent hash+clone packet, then source-review its freeze. A separately selected **one original promoted actual33 previsibility observation** can evaluate P/P_named_roots after repair. It must fail closed on an unexpected nonempty Set/manifest, unknown RID/directory/namespace/generic backing, output scope, unresolved constructor or unsupported alias. Nonempty geometry remains unavailable; do not reuse the old numeric367/871273 or infer FutureFinish/whole-union completeness from successful printing.

A future real finaldriver diagnostic needs its own legitimate ready-to-publish return boundary and authorization; the existing public prune route freezes limits and latches Q32 refusal. This ledger does not change that route or select a future allowance. Q32/1MiB/33/80/4096 remains RED/ACK0; actual native publication remains unqualified.
