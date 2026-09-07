# Typed Asset Maintenance Contract (#1788)

Status: implementation note for issue #1788, the post-#1744/#1758 row+column
copy-on-write maintenance pass. The #1954 typed-column lifecycle reuse/state
mapping and JSONBench report contract are documented in
[`typed-column-lifecycle-1954.md`](typed-column-lifecycle-1954.md).

TreeDB typed assets are immutable copy-on-write assets stored under the
compatibility `column_assets` manager. Maintenance operates on asset refs and
segment inventory; it must not scan logical rows or decode full typed-column
part images merely to compute reachability.

## Explicit typed column_graph serving admission

`Collection.EnsureColumnGraphServing(ctx, index, options)` enables the selected
durable, single-vector-index, row-string/typed-FP32 path. Create the collection,
load typed data and run the first `RebuildVectorIndex` explicitly before ensure.
Offline initialization remains part of query-ready/load time and peak evidence;
ongoing-work limits do not retroactively budget it.

`ColumnGraphServingOptions` requires positive publication, owner, cold metadata,
candidate-output, maintenance-inventory and scalar-filter limit groups, plus
fold-row/search-candidate limits. These expose existing admission mechanisms,
not a second quota/cache. Encoded bytes are attempted work per renewable epoch,
not physical disk size. Native file/pager residual ceilings remain separate.
Zero does not request an unbounded default. Policy is process-local, shared by
all collection managers on the DB, immutable until DB close, and must be reapplied
after reopen. Failed setup gates queries and new writes rather than restoring
feature-off writes. Ensure performs bounded cold reconciliation and maintenance
and may reclaim unreachable assets. Unchanged admitted-state ensure is idempotent
and does not renew attempted-work debt. A race at final state admission fails
explicitly; no blind retry installs stale metadata.

Configured folds prepare next-base metadata from the same producer records and
retain only post-capture owned suffix rows before physical publication. Exact
installed catalog/root identity and the ready derived state become visible under
the existing schema/storage exclusion, before checkpoint. Same-authority Ensure
does not install a temporary invalid marker. Healthy foreground operations may
wait on admission but must not receive a false snapshot mismatch during cutover.
Post-capture delete/reinsert coordinates and costs remain current; only folded
logical suffix debt is rebased. Encoded/candidate attempted work is not refunded
until successful existing epoch renewal. An uncertain physical result or failed
derived-state installation definitively fences new writes; it does not weaken
snapshot matching or invalidate independently held read owners.

Use `SearchVectorIndexWithBufferReadView` with exact query mode and explicit
`minimal`/`production` stats. Native declared scalar equality/range AND filtering,
ANN/delta search and full fetch share the same current owner. Close the returned
view on every path; later writes cannot change its fetched documents. Buffered
IDs last until buffer reset/reuse; plain search owns its result backing. Callback,
legacy range, quantized/rerank, in-search document fetch and unsupported controls
fail closed, without JSON search, unfiltered search or native_runtime fallback.
Retained documents and final serialization may still use JSON; indexed values do
not require JSON reconstruction for this ANN/scalar path.

Warm acquisition consumes immutable metadata on the exact publication state,
never query-side reconciliation or full manifest scans. Metadata retains no
snapshot; each owner binds the current pin and complete captured/current asset
union. Acquisition still includes drain, snapshot/catalog binding and mapping
admission. `ColumnGraphOwnerAcquireNanos` and `ColumnGraphDeltaScored` expose
these boundaries; reused-handle timings do not measure public acquisition, and
full fetch includes its typed materializer setup.

Configured `RebuildVectorIndex` and `FoldColumnGraphServing` use bounded fold and
coherent ready-state installation, followed by explicit maintenance. Existing
admission locks can pause requests; zero-pause availability is not promised.
`RenewColumnGraphServing` reclaims and renews work without folding or closing
readers. Use `errors.Is` with `ErrColumnGraphFoldNeeded`,
`ErrColumnGraphOwnerBudget` and `ErrColumnGraphSearchBudget` to distinguish
maintenance, held-reader pressure and query-work pressure. Folding cannot free
caller-held readers. Pre-append admission rejection does not commit; later native
publication errors retain the existing commit-ambiguity/recovery-required
contract and must not be blindly retried. Context interrupts storage-barrier
waits and is checked at boundaries; schema mutex waits and all native/decoder
inner instructions are not individually interruptible.

Current public tests cover typed mutation, same-owner full fetch, fold,
independent handles, normal reopen, options, held-owner pressure and canceled
setup, acknowledged-write process cuts, rejected stale keeper release and
deterministic fold/unchanged-ensure overlap. Scaled foreground latency and
end-to-end qualification remain separate M3 acceptance gates.
This API checkpoint does not certify the end-to-end Minima workload.

## Reachability roots

Typed asset reachability includes all refs exposed by the column manifest view:

- typed-row `tcs1_part_image` refs;
- typed-column `tcs1_typed_column_part` refs;
- derived `tcs1_aggregate_metadata`, `tcs1_dictionary_codes`, and
  `tcs1_int64_values` refs;
- vector-index state refs recorded in `TVIS` vector-index state records;
- legacy vector graph derived refs recorded in vector-graph manifest records;
- explicit maintenance candidates;
- pending-publish, prepared, prepared-query, quarantine, and snapshot-pinned
  refs supplied by callers or the shared lifecycle registry;
- logical quarantine segment records;
- active process-local `mappedresource` pins converted from typed-row and
  typed-column resource keys.

Active and recovery-authoritative manifest refs are both treated as protected.
Candidate-only refs are reclaimable. Any unknown source, malformed ref,
non-canonical segment, missing segment, out-of-bounds range, or unconvertible
active pin makes the plan incomplete and destructive maintenance fails closed.

`PlanColumnAssetReachability` accepts `MaxSegmentEntries` for bounded directory
discovery. A positive limit counts every listed entry, including empty files and
unknown names; excess entries return `ErrColumnAssetReachabilitySegmentLimit`
with an incomplete plan and no partial segment inventory. Negative limits are
invalid; zero preserves existing unlimited reporting. Discovery reads bounded
batches and checks cancellation between them. This bounds directory-listing
retention, not manifest decoding, caller-supplied refs, total planner memory, or
disk usage. It does not change GC authority or permit deletion from an
incomplete plan.

`MaxManifestRecords` and `MaxManifestBytes` must both be positive or both zero
(the unchanged default). Positive limits preflight encoded manifest input before
decoding: the active manifest and captured typed-graph base are each checked on
the same snapshot/root subsequently decoded. All keys count, including unknown
records, and declared part counts cannot exceed the record cap. Preflight checks
cancellation while scanning and returns `ErrColumnAssetReachabilityManifestLimit`
when input exceeds the cap. These are per-manifest input limits, not an estimate
of decoded heap or a combined working-set cap. Existing vector-partition lifecycle
limits and caller/process pin inventories remain separate.

`MaxLifecycleEntries` optionally bounds each lifecycle snapshot's matching records
plus refs and quarantine segments before copying under its existing registry lock.
It also bounds the combined expanded lifecycle input refs/segments and the
process-wide mapped-pin copy before filtering by database. Excess fails with
`ErrColumnAssetReachabilityLifecycleLimit`; zero retains unlimited reporting.
Copied strings retain their existing immutable backing; the cap covers slice
entries, not all process memory, decoder scratch, or vector-partition loading.
Several bounded snapshots/expanded lists can coexist, so a caller reserving
maintenance workspace must include their combined backing, not just one list.

`ColumnAssetGCOptions` forwards these discovery limits to the same planner.
Over-budget or incomplete discovery cannot authorize deletion. Internal lifecycle
consumers refreshing accounting must keep the existing storage-mutation epoch
across successful GC, fresh bounded inventory and accounting refresh. Failed or
partially completed cleanup statistics are not reclamation credit.

### Fold control-root placement

Fold preserves the configured storage policy of the current column manifest
and row-locator roots, using the same policy resolution as ordinary mutations.
The default therefore continues to inherit the database's native leaf policy;
explicit fast/compressed controls remain fast/compressed across fold. Independent
captured-base copies remain pager-backed and have separate ownership. This is
not support for changing an existing collection's immutable storage policy.

### Internal fold: simultaneous known workspace

The existing one-fold/renewal fence prevents two maintenance workspaces from
overlapping. Ordinary writes and retained readers can overlap a fold, so their
existing current/pending/retired ownership limits remain additive, not replaced
by the fold's cold limits. No new resource counter is needed to express this sum.
For admitted physical rows `N`, columns `C`, degree `M` (default 16), cold decoded
term limit `T`, and the existing manifest/asset/discovery limits, track:

| Coexisting term | Existing ownership and limit basis |
| --- | --- |
| Current/pending/retired typed state and reader resources | Publication receipts, owner state/asset limits; epoch retained admission includes current and retained owners, while pending receipts must be zero at renewal |
| Captured/latest manifests and lifecycle refs | Each raw manifest is preflighted; captured/current and expanded refs coexist, so add their record headers and backing rather than taking their maximum |
| Visibility rows, full reconstruction, FP32 values | `N` row headers, `N*C` union headers, and `N*dims*4` are individually bounded by `T`; raw asset bytes have a separate summed limit |
| Row projection | Contiguous selected columns now borrow capped value slices and IDs synchronously; only projected row headers are new. Noncontiguous/reordered selections retain their existing copies |
| Native construction | Five logical terms are each checked against `T` before materialization/output: node headers; 33 layer headers per node; `N*34*M` neighbor entries; at most 16 concurrent planning populations; reciprocal groups bounded by `min(33*N,16*34*M)` with degree-plus-batch scratch |
| Candidate output and publication | Encoded attempted-work and appender-attempt admission remain separate from buffer allocation. Owned rows, encoded row/typed images, graph output, locators and native publisher scratch can coexist |
| Discovery | Native entry, segment, manifest and lifecycle cardinality caps bound the selected epoch's inventories; several snapshot/expanded/map representations coexist |

Native levels are capped at 32 by `levelForDocumentID`; layer 0 has `2M` neighbors
and upper layers `M`. Planning width is at most 16 (8 for the M=8 specialization),
not an assumed `GOMAXPROCS`. Reciprocal workers are bounded by their group count
even if `GOMAXPROCS` changes. EF does not preallocate EF entries; visited/candidate
populations are bounded by N. Division checks reject degree/product overflow and
planning/reciprocal pressure without allocating graph buffers. The profiled
16K/M16 fixture remains admitted by its existing 512MiB per-term limit.

The combined **known logical storage** is the sum of the rows in this table,
not `T` alone (the five native terms alone can sum to `5*T`). This is not an exact
Go heap ceiling: slice capacity growth, old/new buffers during growth, maps,
allocator rounding, pooled spare search buffers and native publisher scratch
must be added for a heap/RSS estimate. The temporary native construction index
owns its `sync.Pool`; at most 16 active planning searches does not bound spare
pool backing. That index is not installed as a serving index or retained by the
fold result; reclamation follows normal GC/pool cleanup, not synchronous release
on return. Raw asset limits do not by
themselves bound every decoded expansion.
The selected row-string/FP32 schema avoids generic dictionary/list expansion;
broader schemas need their own decoder limits. Likewise, inherited allocator
cache-reset scans use the existing listing, not the optional planner cap:
bounded epoch inventory plus admitted managed output gives a finite selected
namespace envelope only while other producers/external directory mutation are
excluded. Public admission must establish those conditions; this checkpoint
does not close arbitrary-schema/process-wide workspace qualification.

Materialization still clones typed-cache-backed payloads before closing the
cache. Removing that copy merely because projection can borrow would violate a
different lifetime. The projection optimization changes neither codec validation
nor the captured/current/fallback asset union.

## Active mappedresource pins

Every `mappedresource.Manager` contributes to a process-wide active pin summary.
`PlanColumnAssetReachability`, `ColumnAssetGC`, and `ColumnAssetRewrite` filter
that summary by the collection's column-asset root and typed asset namespace,
then automatically add convertible `typed_row_asset` / `typed_column_asset` keys
as pinned refs. File-backed pins must carry either `ResourceRoot` or
`ResourcePath`; `AcquireFileRange` fills `ResourcePath` from the opened path, and
the column-asset read cache fills `ResourceRoot` for bytes it registers. Pins
from another DB root are ignored even when namespaces are reused in the same
process. A pin whose typed asset class, root, and namespace are relevant but
whose key cannot be converted to a `ColumnAssetRef` is not ignored: it sets
`MappedResources.UnconvertiblePins` and marks the plan incomplete.

The maintenance plan reports active handle accounting:

- `ActiveHandles`;
- `ActiveMappedBytes`;
- `ActiveHeapCopyBytes`;
- `ActiveDerivedMetadataBytes`;
- `PinnedRefs` and `PinnedBytes`;
- `UnconvertiblePins`;
- cumulative mappedresource `DeniedResources` and `FallbackReads`.

## Destructive actions

Selected native typed FP32 producers co-locate metadata and aligned typed images
in one fresh existing-manager segment per batch/fold attempt. A source replacement
shares its delete/insert output only within the same still-owned attempt.
Nonempty failed and superseded outputs therefore remain whole-file candidates rather than
unknown prefixes inside live files, including after process-local registry loss.
No captured, fallback, reader, or unknown-plan protection is weakened. Before/
after-seal and unsealed-suffix process cuts exercise complete post-reopen cleanup;
these are not physical power-loss tests. The eight-cycle equal-width fixture
demonstrates a column-byte plateau, not a whole-database quota. The reused
allocator uses IDs 2 through 1,048,575. At high-water exhaustion it rescans the
existing sorted namespace listing for an absent ID; `O_EXCL` remains authority.
GC must first remove protected-free files before those IDs can be reused. A
fully occupied band still fails closed; file 1 and the direct-view band are excluded.
Canonical zero-byte files without asset refs are also whole-segment candidates
when discovery captured an exact regular-file identity. Construction pins still
block deletion. Referenced-empty corruption, canonical-named directories or
symlinks, unknown names, and legacy identities remain fail-closed; explicit
empty quarantine stays protected. Deletion rechecks exact identity, file length,
root closure, and link authority. Empty removal increments segment counts, not
deleted-byte counts. This permits an epoch to recover from byte denial before
the first write. Neither ID reuse nor these cleanup tests activate public mutable
serving or certify a universal physical capacity bound.

`ColumnAssetGC` may delete only canonical whole segments whose bytes are wholly
reclaimable and whose plan is complete. Mixed live/dead segments become rewrite
debt and are retained.

`ColumnAssetRewrite` may copy protected manifest refs out of complete mixed
segments and publish a remapped manifest. It skips any mixed segment that also
contains refs protected by non-manifest sources such as snapshots, pending or
prepared refs, or active mappedresource pins. Rewrite preserves logical asset
identity: kind, namespace, generation, part id, length, and checksum are
unchanged while file id and offset may change.

On platforms with exact relative-namespace persistence, standalone vector/HNSW
rebuild and `ColumnAssetRewrite` acquire DB-scoped stable-resource authority
before mutating a segment. Authority includes the exact child and parent
handles, physical frontier, and namespace durability obligations. Physical
identity may be coalesced, but every logical ref remains an immutable
obligation through publication. The active rebuild closure includes adjacency
state, inverse norms, row refs, document ids, quantized assets, and the HNSW
search pack; physical-graph and legacy-adjacency sources must be added to that
closure if a future writer emits them again.

Authority remains held until the publication backend returns. A command-WAL
retry prepares and pins its successor before releasing the prior attempt, so a
failed replacement leaves the prior authority intact. Failure releases pins
exactly once but retains copied or newly appended segments as persistent GC
orphans; failure cleanup must not remove a pathname that may have been rebound.

Explicit stable-authority operations and destructive rewrite fail with the
typed unsupported-platform error before visibility when exact namespace
persistence is unavailable. Until strict stable-authority activation in the
publication closeout, ordinary vector rebuild retains its legacy compatibility
path on those platforms and makes no stable-resource certification claim.

Maintenance never deletes or rewrites bytes protected by active handles,
snapshots, pending/prepared/prepared-query state, quarantine records, or
uncertain protection state. Releasing the handle, snapshot, or lifecycle lease
permits a later plan to classify the segment normally.

## Non-goals and boundaries

This contract does not introduce query routing, vector-search switching, public
API cleanup, or a new search algorithm. Vector-index state assets, legacy vector
graph assets, and aggregate/dictionary/int64 sidecars remain derived refs tied
to their owning manifest generation. Value-log and leaf-log lifecycle remain
covered by their existing maintenance contracts. It also does not grant
root-candidate authority, discharge command-WAL-prefix durability work, or
declare rebuilt assets query-ready.
