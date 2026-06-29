# Publication Readability Map (#3245)

This note maps the current TreeDB publication/readability contract for #2026 M0
and the #3044 production gate. It describes existing commit surfaces and the
fault model that tests should preserve.

## Invariant

A published commit MUST NOT make roots, catalog metadata, or value-log pointers
visible unless the bytes needed to read them are also readable by snapshots that
observe that commit. A snapshot sees the commit sequence, user root, system root,
value-log set, and `AppliedCommandLSN` as one published state tuple.

## Collection Catalog Publication

Collection metadata lives in the system root under
`systemCollectionMetaKey(collection)`. Collection root descriptors live in the
same system root under `systemCollectionRootKey(rootName)`. Creation and schema
mutations publish the metadata and root descriptors through a system-root delta.
Data mutations that create new ordered roots publish descriptor updates from
`buildRootDescriptorSystemDeltaIterator` after the ordered root apply returns
the new root IDs.

`loadCollectionCatalog` is the read boundary for this state. It first reads the
collection metadata, decodes the root list implied by that metadata, then reads
each root descriptor. EOF while reading either the metadata or a root descriptor
before a mutation commit point is treated as a bounded, retriable catalog-read
fault. The retry is intentionally scoped to errors wrapped as
`collections: load catalog ...`; unrelated EOFs are not hidden behind broad
retry.

## Ordered Roots And System Root

Collection root publication uses backend ordered-root group apply. The backend
builds the affected user roots, then applies the system-root delta that names
those roots. The commit is not visible until `finalizeCommitLockedWithOptions`
publishes the new backend state after data/value-log preparation and the meta
write. The published state contains `CommitSeq`, `RootPageID`,
`SystemRootPageID`, `AppliedCommandLSN`, and `ValueLogSet`.

Post-commit failures are not retried as ordinary collection mutations. If a
collection operation reaches the backend publish point and later catalog,
flush, response, or index-maintenance work cannot prove readability, callers
receive `ErrCommitAmbiguous` so they do not blindly replay non-idempotent work.

## Value-Log Pointer Publication

Large collection and raw cached-mode values can be pointerized into the
persistent value log. Value-log pointers are long-lived index payloads, not WAL
references. A commit that publishes pointer-bearing roots must publish a
`ValueLogSet` that can resolve those pointers, and cached-mode backend reads
must use the current-segment read barrier before resolving backend-visible
pointers whose current segment tail may still be buffered in-process.

## Snapshot Acquisition

`DB.AcquireSnapshot` reads the atomically published snapshot view and pins the
`ValueLogSet` through the value-log manager before returning. Snapshot point
reads, iterators, system-root catalog reads, and collection catalog refreshes use
that captured root/value-log tuple instead of mixing current roots with older or
newer value-log state.

## Applied LSN Visibility

`AppliedCommandLSN` is authoritative only as part of the selected backend meta
state. Command-WAL publication must advance roots and `AppliedCommandLSN` in the
same backend commit. A post-commit sidecar, stats counter, or system-root-only
marker is not a recovery source of truth for command visibility.

## Current Fault Tests

- `TreeDB/collections/publication_readability_test.go`
  - pre-commit catalog metadata/root EOF retries through `InsertBatch`,
  - retry exhaustion preserves the contextual catalog load error,
  - post-commit catalog EOF from vector-index maintenance returns
    `ErrCommitAmbiguous` while the document remains visible.
- `TreeDB/caching/vlog_current_segment_readbarrier_test.go`
  - cached-mode value-log pointer read barrier is installed/invocable and
    backend-visible pointer roots resolve through backend, cached, and snapshot
    reads.

## Non-Goals

This note does not define migrations, cross-version compatibility, Raft
ordering, native-wire submitter behavior, or Mongo gateway request semantics.
Those surfaces may cite this map, but #3245 owns only the local publication and
readability model above.
