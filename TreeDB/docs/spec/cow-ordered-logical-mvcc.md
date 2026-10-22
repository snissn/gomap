# COW ordered logical MVCC

`MemtableMode: "cow_ordered"` selects an experimental logical-history data plane
inside the existing TreeDB COW cache. It is opt-in; the default selector is
unchanged. The native writer, command acceptance, complete read cut, generation
budget, storage-resource owners and durable builder remain authoritative. This
implementation does not wrap the standalone `futureordered.DB` or import its
publication, arena or in-memory consolidation protocol.

The selected private numeric layout uses immutable value descriptors and tree
degree 32. A four-way local comparison evaluated inline and pointer values at
degrees 32 and 8. Pointer values at degree 32 were selected for their
allocation/admission tradeoff; the preserved inline degree-32 candidate remains
the experimental fallback, rather than a runtime selection policy.
The degree-8 alternatives increased object counts and lost history/range read
performance. These alternatives do not change visibility, durability or
retention semantics. Exact comparison sources, measurements and the default
selection gate are recorded separately in the qualification report. This spec
does not claim that the selected layout wins every foreground or complete
lifecycle workload.

## Representation and grouped preparation

The ordered primary is a persistent B-tree with one entry per canonical escaped
logical-key prefix. Each primary value is an immutable timestamp postimage. A
singleton stores its timestamp and latest value directly. A longer history uses
a balanced numeric timestamp tree and caches its maximum timestamp/value. Newer
timestamps use the tree's monotone `Load` route; backfills and replacements use
`Set`. Every general mutation still prepares a private postimage; there is no
post-acceptance mutable-append fast path.

Numeric nodes hold timestamp/pointer pairs. Their immutable descriptors carry
the physical-key and native-value string headers, so path copies share one
descriptor instead of copying both headers in every numeric pair. Singleton
histories and the latest-value cache stay inline. Promotion materializes the
prior singleton and the incoming version; a numeric replacement allocates a new
descriptor so retained cuts continue to select the old one.

On the measured 64-bit layout, a numeric pair is 16 bytes and its descriptor is
32 bytes, versus 40 bytes for an inline pair. The 48-byte pair-plus-descriptor
sum is larger than 40 bytes before considering node occupancy, allocator classes,
headers, payload backing and sharing across copied paths. The benefit is smaller
copied numeric nodes; the costs include descriptor objects and pointer loads.
Neither lower native admission charge nor less node copying proves universally
denser retained heap or faster reads.

Each retained version owns a canonical physical key string and a `cowValue`
encoding of payload, full `ValuePtr`, flags and native `EntryRevision`. The key
and encoded value share one immutable backing when its rounded admission charge
is no greater than two separate backings; small pointer/tombstone records can
retain the cheaper separate layout. Estimation and encoding use the same length
decision. Path copies share those immutable string backings. The primary prefix
aliases a live latest version's physical-key backing, including after replacement
or removal of the previous maximum; it is not a second full physical index.
Stable keys and values remain valid across cursor movement while their
root owner remains retained. Canonical encoding and ownership copies still
exist: typed operands stage directly into the admitted native batch arena, then
the prepared tree owns its retained strings.

Only exact operands qualified by the original Store admission enter this
representation. Key shape alone grants no authority. Raw data uses the original
physical tree. A later unqualified operation on an exact ordered member updates
that member, preventing a second incarnation or resurrection. One routing plan
classifies admission, ordered apply and raw fallback identically, including
same-batch replacement/removal and migration from an older raw member.

`Store.CommitGroupAt` retains distinct `(logical key, application timestamp)`
members of one atomic group and rejects duplicate pairs through its existing
validation order. Each touched history is forked once within preparation.
An existing primary PUT isolates its path and updates the pair in one descent;
its callback is invoked once after the complete path is private. A missing PUT
uses ordinary insertion. The private numeric map header is consumed when sealed,
rather than copied a second time. No mutable producer retains that consumed
header, and a later writer uses a fresh isolation identity. Erase/re-promotion
retains receipts for discarded maps as well as the surviving sealed map.
Application timestamps, native revisions, complete-publication sequence and WAL
LSN remain separate. A read cut selects the actual immutable history root; an
overwritten shared anchor cannot change an older cut.

For a single history operation, balanced primary/history navigation is
`O(log K + log H)` for `K` logical keys and `H` versions of the selected key,
plus key comparison and bounded-fanout copy work. A batch still performs its
individual tree operations; this is not a linear bulk loader. The
`treedb_safe` primary lookup deliberately uses owned-string rank pivots and has
the existing `O(log K * primary height)` lookup bound. Cross-source masking and
public output ownership add costs beyond a per-history predecessor search.
Unsorted mixed input can require backward qualification scans for ambiguous
duplicates, an `O(B²)` routing corner for a batch of `B` entries; ordinary native
sorted/deduplicated batches avoid that fallback. Monotone append is a tree
operation, not an `O(1)` append guarantee.

Sources: [ordered representation, routing and application](../../internal/memtable/cow_ordered.go),
[native immutable value encoding](../../internal/memtable/cow_value.go),
[Store grouped route](../../mvcc/native_ordered.go),
[original logical-input binding](../../internal/mvccadmission/logical.go),
[admitted canonical staging](../../caching/cow_logical_input.go).

## Original acceptance and ownership protocol

The existing COW writer gate stabilizes the opening basis while the host
prepares changed shard roots, native metadata, dependent resources and the next
complete cut. All ordinary capacity refusal and backing allocation occur before
irreversible command acceptance. `COWPrepared.Publish` adopts already-prepared
roots; it does not discover splits, rebase or reserve another quota tranche.
The accepted ticket and existing publication owner bind that adoption to the
canonical command. Existing guarded native semantic finalization and uncertain
acceptance custody remain in force.

Qualification is observed and committed under the same live backend admission
authority as ordinary cached writes. Rejected private preparation cancels its
own work. An accepted command is not canceled or refunded as an unaccepted
attempt when subsequent checkpoint/cleanup reports an error. Old cuts retain
their selected shard roots, semantic state, backend basis and physical resources.

This is a serialized, lock-based COW protocol. The writer gate can span canonical
WAL work. It is neither the standalone prototype's stripe protocol nor a
lock-free or wall-clock bounded progress guarantee. Volatile/no-WAL, relaxed
command-WAL and durable command-WAL acknowledgments keep their established
contracts; selecting `cow_ordered` does not alter WAL recovery or sync policy.

Sources: [public native entry point](../../cow_ordered_native.go),
[prepare/publish/cancel](../../internal/memtable/cow_tree.go),
[canonical accepted publication](../../caching/cow_batch.go),
[complete cut and preparation](../../caching/cow_cut.go),
[COW publication specification](cow-cache-publication.md).

## Physical reads and cursors

Physical `Get`, successor/predecessor and traversal project ordered histories
and merge them with the disjoint raw tree. Physical timestamp suffix order is
the reverse of numeric timestamp order. The ordinary ordered cursor positions
the primary stack once, then its reusable numeric stack; it retains those
positions for `Next`/`Prev`. Forward/reverse seeks accept arbitrary byte bounds,
including partial suffixes. A finite empty bound is distinct from an unbounded
reverse `Last` operation. Maintenance traversal separately exposes admitted,
resumable work steps.

A forward seek to a complete canonical version key in the cursor's current
logical prefix reuses its retained primary position and searches the numeric
history directly. If no eligible timestamp remains, it advances to the next
primary key. Short, extended, malformed and reserved timestamp-zero bounds,
and unpositioned or exhausted cursors, retain the full primary search. This
reuse does not bypass raw-source merging, physical masks, domain checks or
the original read owner.

The public read cut owns all sources used by a read. With an empty pinned disk
basis, an exact canonical version range searches only its retained logical
shard when the existing no-range-spans condition holds. Malformed timestamp
suffixes share that shard and still reach the decoder. Generic ranges retain
the full source queue. A winning inline candidate is copied directly from the
same cut; pointer values use the original admitted materialization path.
Physical tombstones retain the established cross-source suppression/fallback.

`VersionIterator` reuses an encoding buffer separate from its decoded logical
key buffer. Source seeks consume the pivot synchronously. This preserves
seeking with `EntryView().Key`; owned results remain independent of subsequent
movement. Reusable primary/history cursor stacks avoid allocation on warmed
navigation after their admitted growth. Forward `VersionIterator.NextKey`
skips remaining versions using at most eight physical cursor steps, then seeks
to the exact current-key upper bound for a deeper history. A snapshot-bound
source performs those bounded merged-cursor operations under one existing read
guard; generic sources use the same bounded algorithm through ordinary iterator
methods. It does not read
discarded version values. The upper bound supports maximum codec keys and
prefix-child keys without appending a synthetic logical NUL. The next selected
version still passes the original options, timestamp ceiling, tombstone and
storage-error handling; the same retained snapshot owns it. Reverse use returns
`ErrNextKeyReverse`; ordinary `Next` and `Seek` retain their history semantics.
Its counters describe physically inspected records, excluding records bypassed
by a seek. If the next key starts above the original read timestamp, `NextKey`
seeks directly to that key at the timestamp ceiling. This optimization is local
to distinct-key movement; ordinary `Next`, constructor positioning and `Seek`
retain their established behavior. Public snapshot/iterator construction,
movement guards, merged reseeks, native metadata decoding and requested owned
results still cost work and memory.

Durable-basis reads reuse *existing verified* backend leaf-cache entries under
the retained basis and its original read lock. Cache identity includes the
pinned physical owner; integrity checks remain authoritative. Dictionary leaves
retain their original admitted decoder fallback. A cache miss follows the
original read/decode path. This does not populate a second cache, guarantee a
hit after a fresh reopen, eliminate cold leaf I/O, or remove per-seek shared
workspace locking.

Sources: [ordered cursor](../../internal/memtable/cow_ordered.go),
[resumable traversal](../../internal/memtable/cow_ordered_traversal.go),
[cut-bound reads and merges](../../caching/cow_read.go),
[physical identity/integrity path](../../caching/cow_read_workspace.go),
[version iterator](../../mvcc/versions.go).

## Logical tombstones and qualified floor pruning

A logical tombstone is a retained MVCC version and suppresses older logical
values. Internal membership removal changes a private timestamp postimage.
Public physical deletion must also suppress any older incarnation of the same
physical version in the durable basis, so it may retain a physical tombstone
mask until coverage transfers. These operations are not interchangeable.

The optional `PrepareMVCCPrune` boundary supports the existing qualified Store
floor prune. After the original eligibility/floor checks, `cow_ordered` performs
the existing public checkpoint before capturing the prune cut. New writes remain
subject to the Store's established floor exclusion. After any actual delete-write
attempt, completion checkpoints again **after** the prune batch, iterator and
snapshot cleanup, including an accepted-prefix/error outcome. The original error
remains discoverable through `errors.Join`. If no delete write was attempted,
there is no completion checkpoint. Unsupported producers keep their prior path.

A successful nonempty supported prune transfers its completed delete prefix to
the exact successor basis, retaining old cuts and any newer above-floor tail.
Both drains and any persistence waits are part of prune cost; requested delete
batch acknowledgment modes remain intact, but these boundaries can persist more
work. Arbitrary raw physical erasure can still leave a linear run of cross-source
masks until handoff. No all-path logarithmic physical-delete/read claim is made.

Sources: [public prepare/completion boundary](../../mvcc_read_cut.go),
[Store pruning and cleanup order](../../mvcc/versions.go),
[qualified boundary regressions](../../mvcc/native_ordered_prune_boundary_test.go).

## Borrowed flush and exact prefix handoff

Sealed source cursors stream physical records directly into the maintained
revision-aware durable builder, in bounded chunks and oldest-to-newest source
order. Canonical keys, inline values, pointers, flags and native revisions are
borrowed from retained immutable roots; aliases survive cursor movement until
the enclosing source owner releases them. There is no public cloning export or
new in-memory history base in the flush path.

The captured prefix, accepted-command coverage, stable physical-resource lease
and build-group custody remain the original host objects. The successor cut
publishes the new backend basis together with exactly the covered frozen prefix.
Basis and prefix identity are checked before installation; newer source tails
and older read cuts survive. Accepted-plus-error build ownership follows the
existing retry/recovery path. Cursor admission, chunks, builder work, persistence,
retry and release belong in lifecycle accounting.

Volatile generation retirement and persistent value-log reclamation remain
distinct. A generation release never authorizes deleting a value-log segment;
existing reachability, snapshots, dictionaries and storage owners govern GC and
rewrite.

Sources: [sorted streaming and basis installation](../../caching/cow_flush.go),
[root-owned physical aliases](../../internal/memtable/cow_tree.go),
[value-log lifecycle](value-log-lifecycle.md).

## Reservation, receipts and finite progress

The original finite COW budget admits the conservative preparation envelope
before allocation. Grouped numeric-history estimation uses the smaller of valid
independent per-operation and grouped cardinality bounds, including singleton
promotion and mixed raw gaps. Raw/primary/numeric maps record actual allocated
nodes and item/child backings, including discarded growth arrays and histories
discarded by erase/demotion. After private preparation, reconciliation refunds
only the unused **node** reservation. Payload, wrapper, temporary and resource
charges keep their original rules.

Descriptor objects and private numeric headers have separate pre-admitted
wrapper envelopes, including singleton promotion and mixed remove/re-promotion
cycles. Consuming a private numeric header requires one header per construction
or fork, with no second sealing header. These allocations are not hidden in the
smaller numeric-pair receipt or refunded by node reconciliation.

A receipt is not new allocation authority. Exceeding the original envelope is
a diagnostic refusal before WAL acceptance; the estimator must already cover
every allocation. The retained charge is allocation/ownership accounting, not
just currently reachable node size, Go heap usage or process RSS. Memory can
remain uncollected after its owners release it. Actual operation, iterator,
prepared-command and resource cleanup govern credit release; source retirement
alone is insufficient.
The conservative envelope can refuse preparation even when its eventual actual
receipt would fit. Reconciliation does not bypass that original admission gate.

The host's preliminary rollover prediction has a positive-only shortcut for
fully qualified inline/tombstone PUTs in monotone logical-prefix order. It scans
the operands once and uses conservative full-capacity structural bounds without
membership or history-tree searches. It bypasses only that preliminary exact
estimate when the bound proves the existing generation, mutable, retained,
in-flight and total limits cannot request pressure or rollover. Unsupported
operands, overflow, a loose bound or pressure use the original predictor. Final
canonical preparation still performs the original exact admission and retains
the sole preparation/publication authority. This bound is not an admission
receipt and cannot refuse a write or spend budget.

Finite source, generation, view, resource, pending and retirement limits can
refuse further preparation when old cuts or accepted custody hold capacity.
Durable spill and eventual last-owner release provide progress under the stated
limits; indefinite old snapshots cannot imply unlimited writes at bounded RAM.
No quota increase is part of this representation.

Sources: [preallocation and node reconciliation](../../internal/memtable/cow_tree.go),
[history estimator/receipts](../../internal/memtable/cow_ordered.go),
[actual map allocation tracking](../../internal/cowbtree/map.go),
[positive-only prediction bound](../../internal/memtable/cow_ordered_prediction.go),
[host prediction frontiers](../../caching/cow_generation_prediction.go),
[generation budget](../../internal/memtable/cow_budget.go).

At the direct future-timestamp seek and the unchanged eight-step current-group
upper-bound fallback, capable COW composites may retain child cursors already at
or beyond the target. A successful full common-bound seek must first establish
alignment; constructor positions alone are insufficient. Fresh, invalid,
nonforward or errored states retain full seek behavior. Retained COW child heads
are checked under their existing mutex, and the original merge selector still
applies source priority, physical masks and bounds. Generic and durable children
lacking the optional head operation continue to seek normally. The movement uses
the same snapshot read pin, invalidates the selected-value cache, and does not read
discarded values or step through future histories.

## Activation and validation

`SupportsLogicalMVCC` reports the actual optional producer. Stats
`treedb.cache.cow.ordered.logical_keys` and
`treedb.cache.cow.ordered.physical_versions` sum current/frozen root populations,
not unique durable database records; both can become zero after checkpoint.

The following tests are implementation witnesses, not a substitute for the
final exact-source qualification record:

| Contract | Source witnesses |
| --- | --- |
| Typed groups, authority, duplicates and fallback | [Store native route](../../mvcc/native_ordered_test.go), [accepted authority](../../caching/cow_ordered_admission_test.go), [complete group cuts/revisions](../../caching/cow_ordered_prepared_group_test.go) |
| Refusal before append; no new admission after acceptance | [canonical refusal](../../cow_ordered_reopen_test.go), [public prepared adoption](../../cow_ordered_adoption_test.go) |
| Histories, physical bounds, movement and stack reuse | [storage oracle](../../internal/memtable/cow_ordered_storage_test.go), [fused seek oracle](../../internal/memtable/cow_ordered_fused_seek_test.go), [seek buffer ownership](../../mvcc/version_seek_buffer_test.go) |
| Inline/shard reads; verified leaf/dictionary fallback | [successor controls](../../caching/cow_inline_successor_test.go), [leaf cache](../../caching/cow_ordered_leaf_cache_test.go), [dictionary leaf](../../caching/cow_ordered_leaf_dictionary_test.go) |
| Prune, old cuts, interrupted prefix and reopen | [prune integration](../../mvcc/native_ordered_prune_test.go), [floor boundary](../../mvcc/native_ordered_prune_boundary_test.go) |
| Exact bound/receipt and last-owner release | [allocation envelopes](../../internal/memtable/cow_ordered_lifecycle_test.go), [mixed history estimates](../../internal/memtable/cow_ordered_estimator_test.go), [discarded-map receipts](../../internal/memtable/cow_ordered_receipt_test.go), [active read/iterator lifetime](../../caching/cow_ordered_lifecycle_test.go) |
| Immutable descriptor sharing, promotion and accounting | [descriptor controls](../../internal/memtable/cow_ordered_descriptor_test.go) |
| Private header consumption and fused primary updates | [map ownership controls](../../internal/cowbtree/map_owned_update_test.go), [grouped old-cut/receipt controls](../../internal/memtable/cow_ordered_fused_test.go) |
| Lazy routing, conditional backing and validated application | [local ownership and charge controls](../../internal/memtable/cow_ordered_local_optimization_test.go) |
| Positive-only prediction and exact fallback | [structural upper-bound controls](../../internal/memtable/cow_ordered_prediction_test.go), [host frontier controls](../../caching/cow_generation_prediction_test.go) |
| Distinct-key movement, future versions, masks and snapshot close | [MVCC next-key controls](../../mvcc/next_key_test.go), [coalesced read-guard controls](../../caching/snapshot_iterator_mvcc_test.go) |
| Aligned forward seeks, mixed-source priority and retained-cut cache lifetime | [merge controls](../../internal/merging/forward_seek_test.go), [host lifetime controls](../../caching/cow_forward_seek_test.go), [deep future-seek controls](../../mvcc/forward_seek_test.go) |
| Durable prefix/new tail, pressure, persistent pointers and GC | [paused handoff](../../caching/cow_ordered_prefix_pause_test.go), [reopen/ACK](../../cow_ordered_reopen_test.go), [rewrite/compact](../../cow_ordered_rewrite_test.go), [last-owner GC](../../cow_ordered_gc_progress_test.go) |

The public comparison harness is `benchmarks/orderedmem_native`. Qualification
must bind selected source/binaries and include adaptive TreeDB, existing
`cow_btree`, the candidate and full managed Badger under matched workload and
acknowledgment contracts. Foreground, persistence, retirement, close and reopen
are distinct phases. Long histories and held cuts must run at unchanged finite
budgets. Range harnesses that reseek once per logical key must be described as
such, separately from sequential physical cursor throughput. A component win,
passing correctness tests or a previous standalone benchmark establishes neither
a default-selection verdict nor a universal performance optimum.
