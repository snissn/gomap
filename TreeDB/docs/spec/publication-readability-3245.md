# Publication Readability Map (#3245, #3382 Closeout)

This note maps the current TreeDB publication/readability contract for #2026 M0
and the #3382 closeout reconciliation. It describes the local commit surfaces
and current behavior fault model that tests should preserve. It does not claim
distributed Raft HA, read-index, snapshot/rejoin, or routing/fanout readiness;
those remain owned by #3044, #3045, and #3046.

## Invariant

A published commit MUST NOT make roots, catalog metadata, or value-log pointers
visible unless the bytes needed to read them are also readable by snapshots that
observe that commit. A snapshot sees the commit sequence, user root, system root,
value-log set, and `AppliedCommandLSN` as one published state tuple.

## #2026 Closeout State

As of the #3382 reconciliation, #2026 has one final local state:

- Local collection catalog publication/readability is covered by deterministic
  EOF fault tests that separate bounded pre-commit catalog retries from
  post-commit `ErrCommitAmbiguous` failures.
- Local forced-pointer value-log readability is covered at the collection,
  nativewire, snapshot, reopen, and current-writable value-log boundaries.
- External nativewire YCSB load evidence completed 100k and 1M insert loads with
  zero `INSERT_ERROR` on the recorded current-head evidence commit.
- The remaining invalid intermediate 1M nativewire YCSB artifact is classified
  as a non-reproduced client/harness/protocol/server-lifecycle interruption, not
  current evidence of a TreeDB catalog/value-log publication failure.

The closeout load evidence was recorded on gomap commit
`61910b8eac108c5f2c35f07a374879d1fb2dc5c8`. The diagnostic classification gate
was recorded on commit `3f2c712bb700806b29deb872ed90531d4828ab79`. #3382 does
not rerun that diagnostic because the delta from the diagnostic commit to the
current `origin/main` base `f42a7002b` is Raft snapshot-tail work, diagnostic
script/docs updates, and Q2 query benchmark/reference-fill work; it does not
change the nativewire insert path, collection catalog publication path, or
value-log pointer read-boundary code that #2026 owns.

After #3382 merges, #2026 can close for local single-node
publication/readability and the nativewire YCSB caveat. That closeout must not
be used as a distributed-cluster claim: #3044 owns single-group HA and honest
`raft_committed` write semantics, #3045 owns read-index/snapshot/rejoin
semantics, and #3046 owns multi-group routing and ring/token partitioning.

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
  - post-commit catalog metadata/root EOF from vector-index maintenance returns
    `ErrCommitAmbiguous` while the document remains visible,
  - cached forced-pointer catalog documents are readable from a fresh snapshot,
  - current-writable value-log pointer reads are either readable through the
    read barrier or fail with explicit unexpected-EOF context rather than being
    misclassified as commit ambiguity.
- `TreeDB/caching/vlog_current_segment_readbarrier_test.go`
  - cached-mode value-log pointer read barrier is installed/invocable and
    backend-visible pointer roots resolve through backend, cached, and snapshot
    reads.
- `TreeDB/nativewire/forced_pointer_readability_test.go`
  - nativewire YCSB-shaped forced-pointer inserts are readable before flush,
    after `FlushAll`/`Checkpoint`, and after close/reopen,
  - nativewire current-writable value-log reads hit the read barrier and surface
    explicit read-boundary EOF when injected.
- `docs/benchmarks/nativewire_ycsb_closeout_2026-06-30.md`
  - external 100k and 1M nativewire load evidence with zero `INSERT_ERROR`.
- `docs/benchmarks/nativewire_ycsb_insert_error_classification_2026-06-30.md`
  - diagnostic-gate classification of the invalid intermediate nativewire YCSB
    caveat.

## Non-Goals

This note does not define migrations, cross-version compatibility, Raft
ordering, distributed HA, read-index/snapshot/rejoin behavior, multi-group
routing, native-wire cluster submitter behavior, or Mongo gateway cluster
semantics. Those surfaces may cite this map, but #3245/#3382 own only the local
publication/readability model and evidence reconciliation above.
