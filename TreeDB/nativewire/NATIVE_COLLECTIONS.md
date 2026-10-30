# Explicit native collection groups

`FixedPeerTCPGroupV1.NativeCollections` selects the ordinary public TreeDB COW
owner for a data group. It is opt-in: omitted/false keeps the existing backend
role and serialized shared configuration identity. True participates in the
shared configuration digest and must agree across peers and restarts. It is not
supported on the catalog group or alongside vector runtime/initialization.

The factory uses `ProfileCommandWALDurable`, `cow_btree` and the existing flat
group directory (`DisableSideStores`). It activates the original native backend
before constructing the FSM or provider. Supported ordinary JSON/BSON scalar
collection commands use the maintained deterministic adapter; unsupported
producers/formats fail closed through native eligibility. No legacy producer or
alternate publisher is created for an unsupported command. This explicit role
does not migrate the default fixed-peer route or qualify all producer families.

Ordinary BSON insertion on the public shared COW owner supports both returned
IDs and `omit_result_ids`, including the `InsertBatchNoResult` client method.
Both options use the same native validation, private preparation and command-WAL
publication; BSON bytes and binary types stay native. Omitted responses allocate
no response-only ID vector or ID byte arena. The existing v1 wire encoding and
response flags are unchanged. BSON vector support remains governed by its
separate eligibility rules.

The BSON no-result combiner may split a rejected group only when
`ErrMutationNotAccepted` proves no canonical assignment or logical publication.
Generic errors carry no such permission. Ambiguous, accepted-publication and
recovery-required outcomes are never replayed. Native final-row preparation
(including unique ownership checks) runs before intent construction and
`CommitNative`; replay/apply owners do not receive a not-accepted marker from
that boundary.

`TestCOWNativewireInsertResultIDParity` covers both ID options and JSON/BSON under
durable WAL, relaxed WAL and no-WAL profiles, including caller-buffer reuse,
checked checkpoint/close and fresh reopen. `TestCOWNativewireBSONInsertCombiner`
and the `TestCOWNativewireBSONCombiner*` controls retain deterministic member
outcomes, prepublication fallback, unmarked preappend refusal, and post-sync
ambiguity with child-process recovery. These correctness controls are separate
from successful-path throughput/allocation qualification; rejected pre-fix
requests are not an insertion-throughput baseline.

The public DB is the one database owner. The factory retains it until successful
FSM construction, then the FSM owns it; the backend is borrowed and is never
closed independently. Snapshot capture synchronizes the original semantic prefix
before the existing physical cut and exact progress/LSN checks. Restore closes
the same owner before file replacement and reopens a public native owner with the
same flat layout and options. Refused public close retains that owner for the
existing FSM/runtime cleanup retry; it does not authorize file replacement.

`TestFixedPeerTCPNativeCollectionsSnapshotLifecycleV1` exercises real TCP routing,
HashiCorp commitment/apply, JSON/BSON indexed insert/replace, a held old read,
provider snapshot/restart, and writable restored-owner delete/reinsert/reopen.
`TestFixedPeerTCPNativeCollectionRoleIdentityV1` checks omitted default identity
and explicit-role binding. Source construction alone is not runtime evidence.
These controls do not establish authenticated multi-host failover, replacement
coverage, default-route migration, performance acceptance or final Q.
