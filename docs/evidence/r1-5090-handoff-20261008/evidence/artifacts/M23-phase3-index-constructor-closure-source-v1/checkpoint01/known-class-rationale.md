# Phase3 constructor birth closure (source-only)

The original Pager Scope is retained, then a single existing ReserveStableMetadata call admits the checked sum BEFORE newIndexGen creates any child or binds its ManagedWriter. Failure returns nil, releases the attempted retain, and changes no cumulative or resident bytes. No request facet, new owner or ledger is installed.

Every allocation is rounded separately by the shared class rules:

| Actual allocation | Raw expression | Scan |
|---|---|---|
| index generation control | unsafe.Sizeof(indexGen{}) | true |
| registry with embedded16shards | unsafe.Sizeof(lifecycle.ReaderRegistry{}) | true |
| graveyard control | unsafe.Sizeof(lifecycle.Graveyard{}) | true |
| separately allocated initial batch array | GraveyardInitialBatchBackingBytes(), derived from the SAME constructor64constant and private batch layout | true |
| optional ManagedWriter | unsafe.Sizeof(allocatorownership.ManagedWriter{}) | false |

The control sum is fixed stack value storage, with no slice/result/control allocation. Overflow refuses before retention or birth. Nil allocator omits the writer class entirely. Unsupported ordinary class rules preserve raw census and the Pager incomplete coverage flag; they never enable finite eligibility. Numeric classes are not hardcoded: actual compiled unsafe layouts and the shared class rules determine the debit and independent test oracle. No Go or formatter was executed for this packet.

The same Scope holds its conservative aggregate through existing Pager, generation, reader and writer edges. This batch introduces no subobject refund; generation Close releases only its existing retain, and failed physical Close or unresolved writer keeps that exact holder. Replacement generations use their own actual Pager constructor scope from the SAME master, never reattribute the old scope.

## Remaining unproved families

Registry fallback seqs/free growth, Graveyard later batch/page-array growth, allocator and zipper intrinsic backing, Pager maps/channels/goroutine/mmap/OS-file controls, cache and identity registry history, opaque callback/resource aliases remain unproved. Duplicate allocator-writer bind still has its existing panic semantics; this packet does not activate or certify unsupported duplicate constructor input. Raw retaining exports still obey existing writer/Scope custody. All finite guards, fixed128MiB claims, whole publication qualification and performance acceptance remain OPEN/CLOSED as before.

## Scope

Only two production paths and two new test paths changed. index_gen.go and all lifecycle/Scope/allocator/close implementations outside the granted constructor seam remain byte-identical. Tests are authored, unformatted and unexecuted pending ROOT formatting/runtime authority.
