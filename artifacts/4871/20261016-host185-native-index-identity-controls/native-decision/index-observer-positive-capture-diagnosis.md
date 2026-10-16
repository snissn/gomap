# Positive original captures: canonical physical pin versus logical token generation

The current observer is source-proven incorrect for every successfully constructed token with nonzero logical generation. Its conjunction compares IdentityPin.identity with the full logical token identity. Both original registry entry points canonicalize the pin's Generation to0. Thus the two identities must differ, even for a correct DATA/PRIMARY capture. This is a second observer defect; the failed controls do not establish failed production capture/custody.

Bound source is final21bc95e755d65748083193171e2ba2ed4f0d72454189ac2e61b087e998d5acd3/10,377files, archive00e91ecda5dfea9925f4386084bf33168b67b0d28441c31b420b8563cff832c7. The first selected normal DB command failed, remaining3calls did not execute. Full immutable evidence is native-main-integration/index-observer-controls/evidence.tar.gz SHA53a8dcfa805b7d8ef1f6b6c5cd7b358840ba62e39f25fe09fd28faf487a3f5d1. Runtime/source/index unchanged and all processes joined. This diagnosis used source reads only; no new Go, test, retry, source or Git action.

## Exact causal chain

1. db/native_stable_pager_capture_v6.go:339–349 captures the physical identity from the original open file, retaining it in capture.identity, and sets resource spec.Generation=idx.id. Phase3 observes that physical identity in the SAME registry (:356–361). Namespace parent generation at:323 is a separate epoch derived from the exact parent directory identity.
2. rootpublication/index_token_construction_work_v6.go:183–191 duplicates the source file, assigns the original backing.file, token.pinned and token.pinnedRefs=&backing.refs. Phase5:204–209 captures the duplicate's physical identity, then sets Generation=spec.Generation. Phase6:212 invokes PinWithWorkV6(token.identity).
3. rootpublication/pin_registry_capture_work_v6.go:39–62 calls validateRegistryIdentity before the protected canonical lookup and original pin creation. pin_registry.go:184–189 calls physicalStableIdentity, whose entire body at:161–164 sets Generation=0. pinStateLockedV6 at:218–221 stores that canonical physical identity in the pin. The ordinary Pin at:192–215 applies the same normalization.
4. The original final initializer resource_token.go:666–685 preserves the full token identity, original pin, backing, pinned wrapper/counter and operations. It does not rewrite the canonical physical pin identity to logical generation.
5. The new DB observer at native_mvcc_seal_preparation_v6.go:164–167 correctly copies immutable capture.identity and sets only Generation=expectedIdx.id. The error is its shared helper primary_empty_candidate_work_v6.go:333: `pin.identity == identity`, immediately followed by the independently required `token.identity == identity` at:334. A valid original pin has Generation0; valid token/expected identity has nonzero idx.id (constructor rejects0 at index_token_construction_work_v6.go:33). Therefore the first equality is necessarily false, without any production lifetime failure or observer-induced mutation.

## Complete composite predicate audit

| Predicate | Original constructor/source invariant | Disposition |
|---|---|---|
| token unreleased | successful actual capture phase8 before original close; no positive-fixture release yet | valid observation requirement; retain |
| pinned and backing present | staged token phase1 creates original backing; phase4 installs duplicate | retain |
| backing.file==token.pinned | phase4 assigns SAME duplicate to both | retain; distinct original pager File wrapper remains irrelevant |
| pinnedRefs==&backing.refs and refs>0 | phase4 exact pointer; newResourcePinnedBackingWithWorkV6 initializes refs1 at resource_token_allocation.go:57–59 | retain |
| token.metadata.owner==owner and backing.owner==owner | phase1 original admissions use spec.MetadataOwner; same capture.metadata/operations.metadata | retain |
| backing.registry==registry and pin.registry==registry | original backing uses spec.PinRegistry; PinWithWorkV6 on that SAME registry creates original pin | retain |
| pin.identity==full expected | pin identity is intentionally canonical physical Generation0 | source-proven contradiction; fix only this comparison |
| token.identity==full expected | phase5 gives duplicate physical identity plus spec.Generation=idx.id | mandatory, retain exact all-field equality |
| token.ownedOperations==ops | phase2 retains exact original operations; shared final initializer preserves it | retain; raw SameTokenOperations=true |

Both failing raw geometries show ready=true, original refs1, pending nonzero custody and every other exposed callback/pager/owner/registry/snapshot flag true. The raw conjunction does not expose every private term independently; the constructor audit above shows their intended source invariants, not a new runtime proof that each private term individually passed. The guaranteed pin-generation contradiction is sufficient to explain both failures. No other contradictory predicate was found in this directly reachable constructor chain.

The pin registry deliberately coalesces physical identity across logical generations: pin_registry.go:176–181 documents this; TestIdentityPinRegistryCoalescesLogicalGenerations at pin_registry_test.go:88–107 verifies one generation's pin blocks deletion under another generation. Registry generation0 is not a missing index generation or stale authority. StableNamespaceToken validates its exact linked child's platform/volume/object via sameStableObject (resource_token.go:2632–2653), separately from its parent namespace epoch (resource_token.go:1500–1529). No namespace or registry production change is indicated.

## Smallest coherent correction and controls proposal

Inside the SAME admitted scalar observer, compare pin.identity exactly with physicalStableIdentity(expectedIdentity), and explicitly require canonical pin.Generation==0 for clarity. Keep token.identity==full expectedIdentity (all fields, including original idx.id), every existing backing/owner/registry/operations/custody check and both index-selected pager gates unchanged. Never normalize the token identity or compare both identities with a generation-blind predicate. No new reference, lookup, owner or physical I/O is needed. Account the additional fixed local normalized identity/call/control traffic before access in the existing observer Reserve; update only observer pricing (F293/R145, production layouts and authority unchanged).

The unexecuted ordinary-backing positive test is affected too: it constructs Generation7 through original ordinary Pin, which also canonicalizes Generation0. Its current positive check at native_index_observer_v6_test.go:61 must therefore fail by the same source proof; this is not a runtime result. After correction preserve its full expected-generation mismatch case (:74–75), and preserve wrong-pin-identity (:105–106), which sets the canonical pin Generation to1 and must still reject. Add independent temporary physical-pin Platform/VolumeID/ObjectID mismatch cases with the expected token identity unchanged; restore every private operand before original cleanup. Explicitly assert the original actual pin's generation0 and full token generation7 in that private fixture. The DATA/PRIMARY actual capture controls and their bounded32-record/1MiB cleanup/baselines stay unchanged.

Verification proposal after a new source freeze and independent acceptance: compile-only the same4packages, then a newly selected exact normalDB, normalrootpublication, raceDB, racerootpublication sequence with original7tops plus all required old/new children and first-failure stop. Do not run remaining predecessor calls or silently replay the failed attempt. Actual33 still requires a separate selection. No production constructor/release/refund/registry semantics or caps should change.

Original source-only ACCEPT is now historical; runtime rejects this observer. Q32 RED/ACK0/noAllowance, all private-map/full-retention and real Finish/C1/C3 obligations remain open. Source/runtime leases remain released after this read-only diagnosis.
