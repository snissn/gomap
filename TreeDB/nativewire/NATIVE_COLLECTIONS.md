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
