# Primary construction restart-to-progress units

Read-only independent symbolic advice, gpt-6.1-sol/high. Immutable manifest `5cde43f24dcc86eaa8c2017bbc6cb0dc81a29e749d9420d27be2ab4f2e4e140d`; archive `0bcaee68f1b6a058bd358b5096265771d35ba503143ac9f8e255b2c215fad8f4`. No source changes, Go/runtime, numeric G or publication grant. Source symbol is **NewNativeMVCCPrimaryConstructionTransactionV6**, not NewNativePrimaryConstructionTransactionV6. The arena child is **PrepareNativeConstructedReadRootV6**, not a separate NewReadRootConstructionV6 allocation. Stop boundary is seal phases9/10: root owns both original stable-pager capture internals separately.

All formulas are checked `(records,bytes)` vectors. `s(T)` is original actual sizeof; `a(x)=retainedalloc.AllocationCharge(x)`; P is page size; C=PrimaryCapsuleSizeV6=3P; m=capsuleMetadataV6=512; p=pointer size. `L=Npre+6` includes the nil-link sentinel from the already proved selected schedule; do NOT add another sentinel to L. Larger geometry is unsupported unless separately proved. Add the true outer cursor/request prefix to every child alternative; do not use historical F or assume BatchSize1 limits arbitrary scans.

## Original Owner atomic Add and enrollment boundary

Let b be a checked upper bound on actual linked owner bindings, e in {0,1} indicate the original active enrollment with pending lease, R=(1,2s(memtable.COWExternalLease)+2s(memtable.COWBudget)). The exact supported original atomic Add/AddPending work envelope is

```
A(b,e)=(1,2s(retainedalloc.Owner)+2s(retainedalloc.EnrollmentWork))
       +e*((2,4s(retainedalloc.EnrollmentWork))+2R)
       +b*((3,6s(retainedalloc.binding))+2R).
```

`owner_growth_work.go:14-49` holds SAME Owner.mu across discovery, both forward/rollback admissions, then original resize. A refusal restarts the entire binding discovery; **A must fit after the constructor's repeated prefix**. Each actual pending/linked lease must implement WorkLease. Concrete R is native_retention_work.go:71-73. No unsupported Lease is silently treated as this concrete type. A protects actual allocation plus rollback, not a scalar quota guess. Quota refusal performs no allocation; genuine resize errors retain original first-error handling.

There is no new `Owner.Enroll*` call in transaction/root/candidate/seal constructors covered below. They Add to the SAME already established arena/runtime owner, and those governed bindings/active pending enrollment receive the actual resize traffic above. The two seal capture children later perform their real enrollment, outside this assigned boundary; reuse root's capture birth chain for that. Do not count two fresh owners merely because there are two index captures. All constructors retain their original metadata charge until original terminal cleanup; pending counters are transferred only by original helpers.

Define lookup vectors:

```
V1(L)=(1+L,s(pager.primaryReadRootTableV6)+L*(p+s(pager.primaryReadRootEntryV6)))
V2(L)=L*(1,p+2s(pager.primaryReadRootEntryV6)).
```

V1 is actual work-bearing root-owner lookup, including table header. V2 is the native construction/registration per-link duplicate-check envelope whose own caller supplies its fixed header Reserve. Both traverse an entire bucket from its head on refusal. Root's count includes all linked unsealed/abandoned nodes; no hidden tombstone class is omitted.

## Transaction and RAM root birth

Supported birth predicates are checked on the original objects: capsule format and recovered arena, nonnil same metadata owner, fixed RAM table heads present with retention==that owner, `Npre < 2*len(heads)` (the one native node may bring count up to that threshold), readSerial below `Namespace-PrimaryReadRootLocalBaseV6-1`, namespace/local nonoverflow, and concrete WorkLease geometry for every current binding/pending enrollment. No root table capacity is guessed from33. A native pre-cursor should refuse unsupported geometry before creating its diagnostic caller; the actual constructor independently rechecks it before installation. The outer DB creation path adds `(2,2s(NativeMVCCCurrentDeleteV6)+128)` before locks/constructor; original Request validation is another true ancestor when reached. No extra node is allocated by certificate registration or promotion custody.

`rootpublication/primary_transaction_construction_v6.go:58-113`:

```
Tnew=(3,2s(DurableRootTransaction)+2s(primaryTransactionConstructionV6)
          +2s(primaryTerminalCompletionV6))+A
Tprefix=(1,2s(DurableRootTransaction)+2s(primaryTransactionConstructionV6))
Bprefix=(1,2s(primaryarena.bank)+128)
Boperand=(1,2s(primaryarena.readRootOwnerConstructorV6)+2s(primaryarena.bank))
TableCtor=(2,2s(primaryReadRootTableV6)+2s(primaryReadRootEntryV6)
              +2s(pager.PrimaryReadRootOwnerConstructorV6))+V2(L)+A
U_root=Tprefix+Bprefix+Boperand+TableCtor.
```

Tnew admits three distinct allocations using summed `a(sizeof)` and stores the original completion/queue on that transaction. The constructor immediately tries U_root, but **returns nonnil t even if root preparation refuses**. The actual DB caller (`native_mvcc_current_delete_v6.go:320-335`) stores t before checking ready/error. Thus next durable constructor progress requires Tnew, not necessarily Tnew+U_root in one work turn. Resumption uses SAME t. Before root installation, arena/table checks may repeat but allocate nothing; after aggregate Add(bank+node) the bank callback and node install are nonfallible under arena->table->Owner lock order. The bank+node are independently charged; the temporary arena constructor operand is embedded and scrubbed. Native refuses absent/foreign table and rehash (`count>=2*heads`), before effects. The original ordinary rehash is NOT entered. Success increments readSerial once and stores t.root without another fallible step; future resume sees nonzero root and returns.

Promotion storage (`:275-318`) has compound

```
PromotionStorage=(1,2s(PrimaryPromotionV6)+2s(primaryTransactionConstructionV6))
 +(1,2C+2s(PrimaryPromotionV6))+A.
```

Its two independently charged allocations are descriptor and original C-byte image. No Add/make replay after c.promotion is stored. The outer DB phase is already3 when this returns; source caller must retain its genuine subsequent ready/projection protocol, not reset phase or grant.

## Current-source root capture, fold and canonical encoding

`native_mvcc_root_preparation_v6.go:43-178`: phase0 repeats Request.Validate, then `(2,2s(nativeMVCCRootPreparationV6)+2s(page.MetaPageBody))`, original TryLocks, BorrowReadRootCertificate `(2,2s(bank)+2P+2s(ReadRootCertificateV6))`, and original Acquire `(1,2s(bank))`. Only after success are source Ref/certificate, metadata projection and phase1 persisted. Original borrowed certificate must already exist: constructor attaches it at seal; a missing legacy certificate could call plain Add here and is not a supported native-private birth. Borrow/Acquire contain plain RAM bucket lookups (see final source caveat).

Phase1 per source entry repeats `(1,4s(node.PrimaryDirectoryEntry)+2P)` plus an entire nonpersistent captured-directory search of at most `ceil(log2(d+1))` visits, each `(1,2*(len(observed.Key)+len(entry.Key))+2s(node.PrimaryDirectoryEntry))`. Current/captured key max P gives a conservative per visit `(1,4P+2s(entry))`. Source `s.index` is advanced only after the search/fold/possible component borrow succeeds. Therefore sum this prefix and all search visits for the next s.index advance. The binary search locals restart on refusal. Retaining source directory/Ref keeps operands immutable.

For the **selected no-extra-writer prefix**, current source and captured directory contain the same components: each is found with equal key/operand/revision/kind, so all fold into materialized DATA; `s.count=0`, `s.physical=false`, no component group birth. Check this actual equality/shape before claiming the reduced bound. A changed source with unmerged physical components reaches GroupConstructionWorkV6; its newGroup uses ordinary Owner.Add, so this report does not silently close that unselected fan-out with a larger scalar. Directory bound is fixed format d<=49, not history33.

Canonical Encode units (`node/primary_directory_encode_work_v6.go`): initialization `(1,2P+2s(PrimaryDirectoryEncodeWorkV6))`; per validation `(1,2s(PrimaryDirectoryEntry)+2P)` repeated count times; clear/header `(1,2P)`; per encoded entry `(1,2s(entry)+2*PrimaryDirectoryEntrySize+2P)` repeated count times; checksum `(1,P)`. Every entry index/phase survives refusal. Selected output count0 skips both entry loops; initialization/header/checksum remain actual work.

## Directory seal, certificate, image and registration

`SealPrimaryConstructionWithWorkV6` holds t.mu, requires componentCount0 and the native constructor; it invokes the original embedded DirectorySealWorkV6. It does NOT run ordinary synchronous seal/unwind.

Initialization `(1,2P+2s(DirectorySealWorkV6))` copies one original immutable image and persists phase1. Canonical Decode init `(1,2P+2s(node.PrimaryDirectoryDecodeWorkV6))`; per entry phase1 `(1,2s(int)+2)`, phase2 `(1,2*PrimaryDirectoryEntrySize+2s(entry))`, phase3 `(1,2*PrimaryDirectoryEntrySize+2P)`; final padding scan `(1,P)`. Decode index/phase persists. The fixed reserved header/padding loops total <=P and introduce no allocation. Empty selected directory still validates its entire canonical framing.

After decode every invocation repeats **V1(L)** before inspecting the bank. Seal phase2 per directory entry `(1,2s(entry)+2s(Group)+2s(bank))`; phase3 per one of all49 group cells `(1,2s(Ref)+2s(Group))`. Even selected nil group/count0 must visit all49 cells; the stored index permits one-at-a-time advance, with V1 replay before each resumed visit. No component lookups are performed for nil groups.

A conservative final phase4 unit is:

```
SealFinal=V1(L)
 +(2,4P+2s(bank)+2s(Group)+2s(ReadRootCertificateV6))
 +(1,2s(ReadRootCertificateV6)+2s(bank))+A
 +(1,2P)+V1(L)+A
 +(1,2P+128)+V2(L).
```

First A admits actual certificate before `&ReadRootCertificateV6`; attached b.readCertificate survives a later refusal. Second A admits actual image before make/copy; b.image survives registration refusal. Both belong to SAME bank metadata until original last-bank/reader/slot edge. On retry already present certificate/image skip Add/allocation but still replay fixed validation and lookups; summing both A is conservative for progress and covers the initial path without invented refund. Register installs only the existing node's image, no extra table node or image copy. Its full bucket scan can refuse before install. Final install sets same certificate/view/image, digest, groups and sealed flag without further fallible step; outer transaction saves that certificate. Root scratch close then detaches original scratch aliases; source-witness acquired Ref/certificate remain through previsibility and original receipt Finish as already reviewed.

## Candidate construction up to seal preparation

`native_mvcc_candidate_preparation_v6.go` has no generic entry Reserve; original backing-refund Complete is a repeated entry child, supplied by the existing terminal/removal report. Request validation belongs to actual phase0 only.

|Phase|Restart-to-stored-advance expression / effects|
|---|---|
|0|Request.Validate + `(3,2s(candidatePreparation)+2s(visibleMember)+2s(visibleInstall))` + EmptySetPrepare under original write/durable/rootReuse/db/runtime exclusion; stores s.resources and phase1 only after success.|
|1|`(3,2s(visibleMember)+2s(visibleInstall)+10s(primaryVisibilityCallbackEnvironmentV6))+A`; admissions sum original visible metadata, five separately admitted callback environments and install. Allocate member/install once; phase2 persists.|
|2|EmptySetClone; s.member.resources and phase3 persist.|
|3|`(2,2s(visibleInstall)+128)` + bounded actual Set Acquire/leaf-generation scalar capture under rootReuse+db; no DBState/view prebirth. Require exact empty vlog shape; fileCount<=17 alone is not zero.|
|4|`(1,2s(primaryStateRootV5))+(1,2s(bank))+V1(L)+A`; Add attaches immutable descriptor allowance to the actual root bank before allocating primaryStateRoot; phase5 persists.|
|5|original entry backing refund + VisibleCapacity below; genuine detached old-backing refund may force yield, then callback environments/spec are installed once; phase6 persists.|
|6|`(1,2s(DurableRootTransaction)+2s(DurableRootTransactionSpec))+(2,4s(bank))`; original AcquireAndDetach transfers exactly one distinct state edge and clears constructor discovery edge, never its receipt root edge. Include plain bucket traversal caveat below.|
|7|`(3,2s(CandidateSpec)+2s(PreparedRootCandidate)+2s(DurableRootTransaction))`; empty admitted resource check and original fixed one-member validation/transfer; candidate lives in original constructor, no new candidate/group heap allocation on this branch. Phase8 persists.|

**EmptySetPrepare**: initial `(1,2s(StableResourceSet))`; if source exists, then EmptySetClone; otherwise `(6,2s(StableResourceSetBuilder)+2s(StableResourceSet)+2s(resourceKindBacking)+6s(resourceAllocation))+A` before four original containing/reference allocations. No temporary builder allocation. **EmptySetClone** (selected kind-backed case): `(1,2s(Set)+2s(resourceAllocation))+(1,2s(Set)+s(resourceAllocation)+s(resourceKindBacking)+64)+(1,2s(atomic.Int64))` rollback admission + actual retain CAS unit `(1,2s(atomic.Int64))` + A before new wrapper/reference header. Source lock keeps rollback nonlast. The original empty-directory branch substitutes `s(stableResourceSetExtras)` for kind and its actual directory refs atomic. With this exact no-extra-producer/idle-held-reader schedule there is no competing retain/drop of that source allocation during construction, so one CAS attempt suffices; contention is not given a guessed retry count. Unsupported resource shapes refuse before this route. Neither wrapper aliases its sibling's metadata allocation.

VisibleCapacity (`native_visible_capacity_work_v6.go`): repeated `(2,2s(rootPublicationRuntimeV1)+16)`. Phase0 optional prefix compaction uses `(1+2*length,3*length*p+48)` before copy/clear and persists phase1. Two separate persisted growth subphases cover visibleMembers then debt. For each original T-backed array define old length n, current cap c, charged full cap f, replacement q=max(4,need,2c) when doubling is safe (otherwise actual source chooses need). Growth prefix `(2,2s([]T)+2s(runtime)+2s(primaryBackingRefundV6)+16)` plus **whole** `(1+q+n+f,s(T)*(q+2n+f)+2s([]T))+A` before make/copy/full old clear; no replacement if cap>=need. Old detached charge uses original RemovalWork outside runtime.mu. Save phase after install before yielding for refund; do not require a second identical replacement. Counts/caps must include actual source schedule, not only ready-time vector length.

## Seal phases0-8, then two separately owned captures

The entry repeats backing refund and claim-refund RemovalWork; reuse original terminal children. Phase0 Request.Validate + `(2,2s(PrimaryCapsuleViewV6)+2s(DurableRootRecordV1)+128)` + parent CopyInto `(1,2C)` + EmptySetPrepare proof wrapper. Locks preserve actual parent/slot identity; the copy has no new arena edge. All must fit before phase1 is stored. Phase1 one EmptySetClone(member.resources), phase2 one manifest constructor below, phase3 banks wrapper+refs backing, phase4 Claim, phase5 original one-page manifest materialization, phase6 metadata seal, phase7 record scalar projection, phase8 private promotion; then phases9 and10 call the two **original** nativeStablePagerCapture children, each exactly once to completion with its own persistent phase. Their internal birth chain/enrollment is assigned to root and deliberately not duplicated here.

Manifest phase2: `(4,2s(DependencyManifestV1)+4s(resourceAllocation)+2s(dependencyManifestCursorV1)+128)+A`. Empty closure has no encoded pointer/vector backing; independently allocate manifest and allocation header under actual Set owner. PageCount must equal1.

Banks phase3 repeats `(3,2s(primaryBankConstructionV5)+2s(Ref)+128)`. Fresh wrapper adds `(2,2s(primaryBankConstructionV5)+2s(primaryArenaOwnerV5))+A`, retaining SAME original arena owner under its lock after Add. Store wrapper before capacity refusal. Initial refs capacity1 adds `(1,2s(primaryBankConstructionV5))+(1,2s(Ref))+A` before make; nonempty replacement is unsupported. These are independent advances; wrapper admission need not be replayed when refs allocation refuses.

Claim preparation: `(1,2s(Claim)+128)`; each skipped stale generic freelist head `(1,2s(bank)+64)` **advances original a.free and epoch**, so these skipped heads are persistent progress, not an indivisible whole-chain scan; then A before Claim allocation. Claim returned before private chunk/Growth. Each Claim.Step repeats `(1,2s(Claim))`; initial missing bankChunk adds `(1,2s(bankChunk)+128)+A` before new bankChunk. Store c.chunk before trying original pager.NewGrowthForPager. Its constructor/StepWithInstallAtCount are the existing **Growth child on the PRIMARY companion pager**, with target local+1; use root's same typed Growth birth/progress rows with that pager's actual pre-cursor geometry, not DATA pager geometry. c.growth/prepared persist. Final install costs `(1,2s(bank)+64)`, plus `(1,2s(bank))` if splitting a pair; supplied as original Growth install unit when Growth exists. Native finish clears private chunk/Growth aliases but does not refund Claim.

Late wrapper disposal repeats completed Claim.Step prefix if previous call installed it; then NativeInstalled `(1,2s(Claim))`, banks edge store `(1,2s(primaryBankConstructionV5)+2s(Ref))`, DetachNativeAccounting `(1,2s(Claim))`. The SAME parent's claim alias is cleared before original RemovalWork; bank edge was preserved first. Subsequent claim-refund completion is an entry child, no bank/claim replay or alternate Claim.Cancel after installation.

Phase5 materialization `(3,6P+2s(primaryManifestImagesV5))` covers original fixed empty manifest scratch/one-page write-to-private-array; phase6 SealMetadata reserves `(1,16P+2s(bank)+2s(metadataDecodeScratch)+2s([2]*bank))`, then V1 only for RAM refs (this manifest ref is physical; O(1) bank slot), decoded edge count and each actual lookup. Selected empty manifest has zero outgoing edges, so no edge backing Add. Its original PRIMARY pager WriteViewWithWork child remains priced by root's pager write unit; no physical publication/root Fence is performed here. Phase7 `(2,2s(DurableRootRecordV1)+2s(PrimaryProjectionV5)+128)`.

Phase8 is one indivisible **replayed capsule/custody chain** until s.promoted is stored:

```
(2,2s(DurableRootRecordV1)+2s(page.MetaPageBody)+128)
 +(1,2s(DurableRootTransaction)+2s(PrimaryPromotionV6))
 +CapsuleConstruct
 +(4,8s(bank)+4s(Ref)).
CapsuleConstruct=(1,2m+256)+(1,C+P+2m)
                 +2*(1,4P)+(1,2C+P+2m).
```

Parent is present on selected route. The capsule constructor performs actual parent CRC/digest, current/parent directory copies and final capsule framing/digest under these existing reservations. If custody refuses, the entire codec chain repeats; no synthetic mid-codec progress. AcquirePromotionCustody visits 4 original bank operands, its fixed duplicate multiplicity loop is 4x4, and it acquires exactly four original edges after all checks; root and parent each include a plain RAM-table lookup, closure edges are physical O(1). No new bank/image allocation here: original transaction already owns its promotion storage. It stores original promotion/custody and then parent stores phase9 without a further fallible operation.

## Multiplicities, omissions and enforcement caveat

Selected one-frame construction births exactly one transaction/constructor/terminal trio, one RAM bank+table node, one certificate, one RAM directory image, one promotion descriptor+image, one candidate visible member/install plus five callback environments, one primaryStateRoot, four selected resource wrappers (two Prepare sites and two Clone sites; a nonnil Prepare delegates clone), one manifest/header, one bank-construction wrapper and capacity1 refs backing, one Claim, at most one missing bankChunk and one pager Growth. Optional runtime replacements are the two candidate vectors. Original fixed directory entries/group-cell loops have max49; selected empty output removes entry loops but retains49 cell checks. These are construction-family multiplicities, not a retained-union sum; do not count the same embedded work/header twice or charge published legacy outputs to a new domain.

**Exact immutable-source caveat:** some original helpers use fixed Reserve then plain `a.valid/a.slot -> pager.PrimaryReadRootOwnerV6 -> t.find`: root phase0 Borrow/Acquire, candidate AcquireAndDetach, promotion root/parent checks. Their actual bucket traversal is finite under the independently checked L, but per-link Reserve is not present there. A test-only traffic envelope can conservatively add V1-equivalent visits for those real scans, while honestly labeling it a source-derived whole-scan price; it must not claim that source enforces each link before observing it. Root phase0 contains two such lookups (plus already retained current-root access); candidate binding one; promotion two. Native RAM birth, certificate/image path and registration DO use the work-bearing lookups shown above. No production repair is proposed or granted here; parent must preserve this distinction in its source decision.

Scope is the selected empty-resource one DATA leaf/outeroff/emptyvlog no-extra-pre-ready-producer route. Nonempty manifests, arbitrary component group birth, foreign WorkLease, active competing CAS mutation, absent certificate, unsupported RAM-table rehash, or unproved companion Growth geometry are not closed by a larger G. Root's future actual-caller container deltas must be composed after these immutable rows. Existing terminal reports cover cancellation/Finish/removal; this report grants neither arbitrary Finish total nor public native publication/Q32 fit.

## Source bindings

- `TreeDB/db/native_mvcc_current_delete_v6.go` `5dbc2fb4c8993ed43075650989d63616ae9afd4310b18aaee94c1f9443a9f600`.
- `TreeDB/db/native_mvcc_root_preparation_v6.go` `25c3d21cf4978dfb27e95eaa1a1617e42a700f2afd490aa9e82e592b853024fd`.
- `TreeDB/db/native_mvcc_candidate_preparation_v6.go` `45d7dd34004bc14b94fa1592ebe7979f3f33e8924d176d49cc8c49a56df937a7`.
- `TreeDB/db/native_mvcc_seal_preparation_v6.go` `e30088d2ab5ad5810fbe62c28c00495798adff8b133a50d7da7f0255fd930bae`.
- `TreeDB/db/native_visible_capacity_work_v6.go` `880ae85894c4051e11e80839c43270cbf616f8ca87fdca5a0cc462897f72cfc9`.
- `TreeDB/db/primary_runtime_metadata.go` `37ec071287c944ef9012a2a49ae8582070caa9cb6463be31e7193347c36f40b0`.
- `TreeDB/db/primary_bank_construction_v5.go` `667aa341a55f8de6f19545244495af914ab783f4a646189ec3f717ca203f11b8`.
- `TreeDB/internal/rootpublication/primary_transaction_construction_v6.go` `7b8790f943894d5c571bcfd58742d8d61e0c07ee9b59a7b23a7a76a0dd141f3a`.
- `TreeDB/internal/rootpublication/durable_root_transaction_native_mvcc_v6.go` `f1ceef27c3065f2b5f7ef28d156f5bab42de113ffe99b19ed0350ba975cf0785`.
- `TreeDB/internal/rootpublication/primary_empty_candidate_work_v6.go` `56c85430928b3a399404bd5f23c49af5c11c5e3bac5f1b40157467eaf42193bb`.
- `TreeDB/internal/rootpublication/primary_capsule_v6.go` `29269031e7a1c3ee02cd2dbeb47fba6f8914f1259faec78f6acdda348e2616d3`.
- `TreeDB/internal/rootpublication/resource_set_clone_work_v6.go` `117279f05552e0f467769380f9525e592ab18e95614b9c973699ec6d44e61444`.
- `TreeDB/internal/rootpublication/resource_set_allocation.go` `8d366ae46f74d914665b2e7d68cd29ac568fc74a56087491b85dfc02ae1f6258`.
- `TreeDB/internal/rootpublication/resource_allocation.go` `1e0e3508a3b8dfb3e222eafb6fdffdf32033688dfc1608482a307265d0e39a2b`.
- `TreeDB/internal/rootpublication/types.go` `3fc00c9bc50e6cf749ea6d129bd5b252a0e57b7cde8a9b80438e4a2156022aef`.
- `TreeDB/internal/primaryarena/arena.go` `515e86b817e326f21fd7c8cebf8c748de871b4efad472280490221c93d312553`.
- `TreeDB/internal/primaryarena/read_root_v6.go` `ebb053bf4be6d291d6fb38aed2969572c06cc397337ad07b526e1a120567141c`.
- `TreeDB/internal/primaryarena/directory_seal_work_v6.go` `a5a5fc95cbcc214afefe9fdf13207444420ae8b63650907ec8a4ea87bb976f7a`.
- `TreeDB/internal/primaryarena/group_construction_work_v6.go` `f58f7fe6aa67b6318d3afd624a51048da8c91449fa47013c43ef70e616916e5b`.
- `TreeDB/internal/primaryarena/metadata_work_v6.go` `817ea7caba64579932c51685a18be04a71a1c1d4a0942a0300b6af86d7a8da5c`.
- `TreeDB/internal/primaryarena/bundle.go` `f04865447b39319f36ade286c2ce979d1ccf32675f0bc45e4247e93a6033aade`.
- `TreeDB/internal/primaryarena/retention.go` `c4d9150bda025702051ed62da3936434725bc33acd6cf222cd25944b64b69a1b`.
- `TreeDB/internal/retainedalloc/owner_growth_work.go` `74735769ff46c27445a03ae785dad4e1a8ae3ecf84358d039486e0de9dcabf35`.
- `TreeDB/internal/retainedalloc/owner_enroll_work.go` `41e6e2370ea5df1c5965dc40146ddf5ecd61d824b48e0987076aeb22e8a5a054`.
- `TreeDB/internal/memtable/native_retention_work.go` `0863e195a9720e1490ddb1e0c6599533510de1523fb3f09a4ca1d7debb541fba`.
- `TreeDB/pager/primary_read_root_v6.go` `2c3c8041a53ef9cf3cb42b83c44ac08df7bf7573ccb856affe60489f10611083`.
- `TreeDB/node/primary_directory_encode_work_v6.go` `40ac034dc763234a23a777c54d661b650f3a45639d9ef6110874d5875dd23fc5`.
- `TreeDB/node/primary_directory_decode_work_v6.go` `67a8fcfe31abe12aad9d7624d963c3bebc9a5bc68aa1ac94d61255dfcfbeef29`.
- `TreeDB/node/primary_directory_native_mvcc.go` `e533b030e6a04252fe608a14cc1c308728e2c4aa3ec11c521c2df83ddf4c602e`.
- `TreeDB/node/primary_directory_v1.go` `532160409cbac93085b6884c29994fbdaf8801d8fc7fa4200f6aa9e35bca0230`.
