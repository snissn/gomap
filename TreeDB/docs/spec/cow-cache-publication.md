# Immutable COW cached publication

`Options.MemtableMode = "cow_btree"` selects an explicit immutable cache for
raw point operations and externally encoded MVCC versions. Experimental
`Options.MemtableMode = "cow_ordered"` shares these same owners and adds the
[Store-admitted logical-history representation](cow-ordered-logical-mvcc.md);
its [activation and integration limits](cow-ordered-mvcc.md) are explicit. It is independent
of the durability profile. Existing `append_only`, `btree`, adaptive selection
and profile defaults retain their own dispatch. COW is a pre-alpha capability;
qualified public ownership also enables the raw transaction contract in
[raw-cow-transactions.md](raw-cow-transactions.md). Application MVCC timestamp
semantics remain separate. `TreeDB/mvcc` retains Store floor admission while
qualified coherent read cuts permit shared grouped publication admission.

## Selection and finite admission

Use a production profile and select the cache before Open:

```go
opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, "./cow-data")
opts.MemtableMode = "cow_btree"
database, err := treedb.Open(opts)
```

`COWMemtableLimits` is the public finite-limit bundle. An all-zero bundle uses
the following defaults; a nonzero bundle must supply every positive finite
field and satisfy the consistency checks. It is not a process RSS quota.

| Field | Default | Ownership counted |
| --- | --- | --- |
| `MaxViews` | 1,024 | Active root views and cursors |
| `MaxGenerations` | 64 | Writable, parked empty successors, frozen and still-pinned generations |
| `MaxSources` | 32 | Dirty active raw generations and frozen sources through final retained-owner drain |
| `MaxResources` | 256 | Distinct generation resource identities |
| `MaxGenerationBytes` | 64 MiB | Cumulative generation allocation charge |
| `MaxTotalBytes` | 512 MiB | Generation history and admitted external storage |
| `MaxRetiredBytes` | 512 MiB | Conservatively reserved potential retirement |
| `MaxInFlightBytes` | 64 MiB | Preparations and outstanding external leases |

The standalone Mongo opener supplies an explicit product bundle: 512 MiB
in-flight and 1 GiB total/retired, with the other limits above. Half the in-flight
budget remains reserved for physical maintenance; the producer partition covers
overlapping ordinary BSON/command/COW images for 16 MiB documents and byte-bounded
wire-sized insert groups. This does not change core defaults or qualify earlier
fixed-limit measurements under a different limit. Scalar BSON index work is
bounded from referenced field bytes; arrays retain their conservative source
bounds. Explicit `StandaloneOptions.COWMemtableLimits` overrides remain finite and
can refuse before assignment. Neither bundle is a process RSS bound or a promise
that arbitrary concurrent/index shapes fit.

The COW default is four fixed shards. Explicit shard counts are normalized and
must fit the source/generation limits. Open validates the mode, limits and
required cached/backend capabilities before mutating storage. Domain ingress,
legacy cached redo-journal combinations and manual bypass capabilities are
outside this mode's contract.

Limits count concrete allocation capacities, including wrappers, mutation and
source vectors, pending batches, read scratch, decoder state and retained
physical/dictionary owners. Generation history is cumulative: replacing a key
does not refund nodes or values still owned by that generation. External
leases share total, in-flight and potential-retirement admission through their
actual cleanup. One generation resource slot is used by its resolver; it is
not an additional physical deletion authority.

The original accepted-prefix queue is bounded by both the source/generation
product and one sixteenth of `MaxInFlightBytes`, including its actual rounded
arrays and lease. The lazy transaction journal likewise selects its finite
ring from the actual namespace-aware control/backing sizes. These bounds leave
producer capacity beside the separately reserved maintenance tranche; lowering
a byte limit never assumes an older record layout. Full queue admission first
checkpoints outside publication gates and retries; an unsuccessful drain or
admission leaves the next command unassigned. Explicit checkpoints use the same
preadmitted maintenance lane even when producer admission is exhausted.

On reopen the opening complete cut starts at the same recovered canonical prefix
as its original accepted coordinator and exact disk basis. An empty checkpoint
or Close therefore preserves that recovered prefix instead of inventing a
zero-LSN predecessor. Logical sequence numbering remains local to the opening
engine lifetime.

Refusal preserves existing views. An operation that cannot reserve its whole
command fails before WAL append or visibility publication. It must not split
an accepted command, silently switch cache modes or route through a backend
bypass. Long-lived readers can exhaust finite capacity; closing them permits
drain and subsequent admission. Heap reclamation by Go may occur later.

Before producer placement, a command predicts changed-path history using the
existing eligibility policy. An accepted-only generation scalar triggers whole
cut rollover at three quarters of the finite generation capacity, leaving
headroom for exact resource metadata; C1 remains the final admission authority.
An oversized indivisible command refuses before producer or journal effects.
Rollover rejoins write admission iteratively after unlocked retirement and
signals the ordinary flush worker. It neither scans history nor hides a
foreground checkpoint inside an accepted publication.

Before a raw shard's first dirty acceptance, its original generation reserves a
source slot and the cache constructs the actual empty rollover writer/root using
the existing maintenance constructor. Both must succeed before canonical append
or logical publication. The preparation owns partial provision until install;
cancellation returns the source slot and drains the empty successor outside
publication gates. Later writes to that same dirty generation reuse its provision.

The cache owns one parked empty successor per dirty raw shard. Read cuts never
retain that successor. Rollover adopts it without another generation allocation;
a refused rollover leaves the exact parked ownership available for retry. The
empty successor reserves no source until its own first dirty acceptance. Close
drains unused parked ownership outside writer/cut gates. A dirty generation's
source slot survives its last real view, root and writer edge even if it has not
been frozen. Standalone memtable writers retain Freeze-time source admission.

This moves fixed empty-writer, root and resolver construction to the first dirty
write, and adds bounded original cache/preparation vectors. It does not claim a
performance improvement or cover other maintenance-byte/native-build admission
obligations. P3 native generation source cells remain their independent original
owners in the same shared budget; raw provision must neither double charge nor
refund those cells.

## One immutable read cut

A cut contains a fixed shard-root vector, a bounded ordered frozen-source
vector and one retained backend snapshot basis. Its owners cover the exact
index generation, value-log files and dictionary definitions used by reads.
Acquisition pins the existing complete cut under `cutMu`. It neither rotates
mutable shards nor scans, copies or sorts historical records.

When the backend basis uses PRIMARY, `SnapshotAllocationSizes.PrimaryRoot`
reports the immutable root wrapper retained by that capture. Raw wrapper,
pin-set and value-log capacities are rounded separately by the shared
`cowSnapshotRetentionCharge`, used by startup, handoff and dictionary readers.
`PrimaryMetadata` and `RegistryMetadata` are different: they identify the
already-rounded producer-maintained owners of the physical arena and the
distinct DB physical-identity/namespace registry. They are not scalar sizes to
add to the wrapper charge. Startup, backend handoff and dictionary capture use
one fixed arena-plus-registry pair enrolled against the SAME COW budget before
capture effects. Partial pair refusal unwinds both handles. The same owner and
budget share one governing lease; distinct budgets each govern that owner's
actual closure.
Producer growth reserves full replacement capacity before allocation, including
old/new overlap, and refunds only storage whose actual ownership has ended.
The existing COW limits remain the governing limits.

A successful capture adopts its pair enrollment into the exact Snapshot.
Refusal leaves both handles with the admitting caller for unwind. Actual COW
startup/activation separately attaches producer custody before persistent
effects; releasing a borrower cannot detach that producer governor. A temporary
dictionary capture borrows already producer-owned storage and detaches its
temporary governor after actual successful cleanup, allowing its budget to
close without preventing later valid backend writes. New storage caused by
borrowing must be admitted and transferred to an actual producer, disposed, or
retained as cleanup debt.

Snapshot Close invalidates the public wrapper; a nil Close result alone does
not prove physical cleanup while admitted readers or finalization remain.
The SAME original finalizer completes registry, value-log, PRIMARY and exact
ordinary release-queue cleanup, scrubs the wrapper's actual aliases, and then
publishes its once-only completion outcome before detaching temporary custody.
A failed cleanup retains the exact original debt and governing charge; consumed
physical references are never replayed. Independent adapter disposal refunds
only its own detached metadata, not the original physical action.
Retired generations remain governed through their last real reader and cleanup.
Physical arena Close rejects further growth but keeps admitted runtime,
transaction, promotion-image and callback tails until their existing owners
clear those aliases. Cleanup may shrink a closed budget. No capture-time graph
walk, separate publication registry or blanket generation exemption is implied.
Existing OS mapping and backend decode/cache storage keep their own narrowly
specified owner policies; this descriptor is not a universal RSS quota.

Writers publish one complete successor cut. A reader sees the entire old or
new command, including a command that changes multiple shards. Newer mutable
roots and frozen sources shadow older sources and backend entries. Physical
deletion markers suppress older entries; MVCC logical tombstones remain normal
encoded values interpreted by the MVCC layer.

The empty-backend shortcut is a proof about the captured root. It accepts an
empty leaf directly, or a canonical PRIMARY directory with no components whose
digest-validated base is an empty leaf. Nonempty/internal bases and any component
(including an inline absence) retain the same-cut merge path; physical validation
errors propagate. It never consults a newer published root to infer emptiness.

For a nonempty captured backend, one-successor selection uses fixed admitted
ancestor, key and leaf scratch on that same immutable cut. It compares one
metadata candidate from each eligible source and materializes only the winning
value-log pointer. PRIMARY base/component digests retain ordinary read validation.
A physical tombstone or unsupported source returns to the existing complete
same-cut merge so suppression and advancement remain correct. The returned key
and value are owned copies; this changes neither publication nor durability
authority.

Point lookup, successor selection, merge traversal, pointer/checksum decoding
and copying occur after the cut latch is released. Public values are owned
copies; `GetAppend` appends into caller storage. Forward iterators retain their
cut and admitted scratch through Close. Snapshot Close invalidates its bound
iterators, and physical ownership drains after admitted operations/cursors
finish. DB Close denies new reads and waits for admitted reads before storage
teardown; it does not promise storage reads after DB Close.
The inherited `Print` and `FragmentationReport` diagnostics require caller
synchronization with Close.
Public reads capture their existing cached/backend owner under the public
lifecycle lock, then release that lock before lower-layer read admission or
callbacks. A callback may therefore close the DB without retaining that lock.
Read-owner capture uses nonblocking read admission: exclusive or pending Close
returns `ErrClosed`, and `Stats` returns nil, instead of waiting. This also lets
synchronous error notifications and worker notifications finish when Close
owns the lifecycle lock and waits for workers. Notification ordering is unchanged.
During the final Close flush, the existing backend read barrier remains active
for private COW build chunks: a later chunk may read external leaves buffered
by an earlier chunk. Close removes that authority before lane teardown.

Cached inline values, tombstones and pointer metadata need no decoder workspace.
Forward iterators omit only zero-entry roots from their exact captured immutable
cache-source vector; tombstone-bearing roots retain precedence. They omit the
disk cursor only when their exact retained backend root is a validated empty normal leaf; later backend state cannot establish
that fact for an old cut. Memory-only iterators retain their smaller owner and
admit value workspace only if needed. Physical iterators place their independent
workspace in the construction owner and reserve the full inspection envelope
with that owner's single lease before allocation or initial seek. Decoder and
buffer growth, and point page scratch, remain lazily admitted. Codec slots grow
within finite admission,
and actual Snappy/LZ4 headers use bounded native owners without constructing
ZSTD state. Owned callback copies remain separately admitted even for inline
values.

When the original forward source vector contains exactly one source, the merge
iterator retains that source directly instead of allocating a heap or comparing
competitors on each entry. It still suppresses physical tombstones, enforces the
original domain, preserves lazy value/pointer/revision access, and owns the child
through Close. Its actual wrapper and source-vector backing remain separately
admitted; the heap charge is zero. Several original sources with only one
currently valid child still use the ordinary multi-source merge, because seeking
and cleanup retain every original child. Reverse iterators keep their existing
implementation.

Retained snapshot point reads and bound iterators reuse their own admitted
physical traversal scratch. A snapshot successor keeps using already admitted
cut-local point storage when present. Otherwise it tries the retained backend
basis's original traversal mutex without waiting. If available, it
borrows that basis's admitted workspace and copies the winning key and inline
value before unlocking. Dictionary-free Snappy/LZ4 leaf decoding retains the
successor's independent block-decoder policy in that workspace; this borrowing
does not add a generic decoder mutex wait. A busy basis takes the original
cut-local traversal and block-decoder path. Shared capacity refusal also releases
the basis mutex before trying the cut-local owner. Thus independent retained readers
can still proceed while another owner holds shared scratch. Capable dictionary
leaves now complete that original traversal with a per-call independently owned
definition capability; the basis retains neither the caller nor its provider.
Captured-only providers retain the complete-cut dictionary retry. Pointer
materialization still uses the complete caller cut after traversal unlocks.
A dictionary capacity refusal can hand a synchronous point back to that same
cut, preserving fresh-owner refund outside the basis mutex.

Successful owned point copy-out and cut-local traversal can return workspace
metadata and optional point buffers through the original budget's idle scratch
stack. It retains at most four
entries and 128 KiB of charged payload. Snapshot, backend basis and all successor
page aliases are cleared first. Cut-local codecs, dictionary capabilities,
input/output buffers and allocation chains are discarded and fully refunded
before parking; they are never pooled. Parking consumes the original one or two
receipt capabilities and refunds their wrappers; a borrower receives fresh
receipts. Pointer-only cached reads need no point arrays or second receipt.
Borrowed value/iterator materialization stays conservative; owned append paths
become eligible only after copying into the caller's destination. Stale close, resize, shrink or split cannot affect a new owner.
Any otherwise refused in-flight, retirement or total byte admission discards
idle payload and refunds its charge under the same budget mutex, without
callbacks, I/O or cache locks. Active borrowers are never reclaimed. Budget
Close drains all idle entries, and bounded retention work reserves the fixed
maximum descriptor eviction work before it can have an effect.

Each actual capacity is reserved before allocation. Cut-local capacity is
refunded through the existing reader Close. Shared successor scratch remains
charged to its exact backend basis until the last basis reference releases;
successor calls and short-lived snapshot closes neither refund nor reset live
shared backing. A later handoff cannot rebind that scratch to a different root.
Capacity exhaustion refuses through the existing error rather than using
unadmitted storage. This reuse adds no pool, root publication or recovery owner.
Physical iterator construction failures and Close drain workspace children
before refunding the aggregate construction lease, including on verified-cache
hits that need no decoder or buffer growth.
Direct DB point reads and native physical readers retain the original shared
workspace. ZSTD records in COW read workspaces use the maintained bounded decoder
continuation. Dictionary reads first reuse an already-admitted matching cut-local
decoder. Otherwise they decode through the original cache physical workspace.
Owned version-successor reads finish leaf copy-out or owned value append before
releasing its mutex. Snapshot and iterator leaves decode into their original
caller destination; borrowed pointer values are copied into the original reader's
admitted output before unlocking. Shared decoder backing does not change each
reader's returned-byte ownership or movement/Close boundaries. A provider without
independent definition admission retains the original complete-cut local route.
Snappy/LZ4 block decoding and other users of the generic `COWDecoder` retain their
existing implementation and admission contracts.

The bounded ZSTD adapter admits its wrapper and complete input/raw buffer
capacities on the containing codec's original receipt before allocation. The
library's `Retain` grows that same receipt before allocating actual continuation,
table or scratch backing; `Release` refunds only after the dependent aliases are
detached. The containing codec's base reservation remains until its wrapper and
buffers are detached and the original lease closes. No generic decoder envelope
is substituted for these concrete allocations. Input and exact output capacity
remain hard limits. The maximum decoded size is `max(rawCap, windowCap, 1024)`
and the maximum window is `windowCap`; COW workspaces select
`windowCap=max(rawCap, 2048)`. The decoded-size limit covers concatenated frames
within one operation, and output remains bounded by the caller's exact `dst`.

Finite replenished `OrdinalScanWork` quanta admit work before effects. A no-error
continuation pause may obtain another quantum; capacity errors are propagated,
not retried as work exhaustion. Cumulative actual work is checked for overflow,
and a requested unit that cannot fit a fresh quantum refuses. This synchronous
adapter adds no aggregate per-read work limit or latency guarantee. Terminal
error cleanup uses finite quanta and closes the failed continuation before the
read returns; any later read reconstructs through normal admission.
Native physical value reads also decode the normal producer's trained dictionary
records: the shared decoder
owns independently admitted, SHA-verified definition bytes from the built-in pure
read provider. A per-call provider capability never installs borrowed cut bytes
in shared scratch or remains in the decoder. Partial preparation, replacement
and final cache cleanup release the original definition owner and reservations.
The shared workspace retains one ZSTD producer decoder, reusing matching
definitions and retiring incompatible idle storage before replacement admission.
The adapter checks the complete definition's SHA-derived GoMap producer identity
before construction and validates its ZSTD dictionary grammar once. Successful
reads rebind frames within that same validated immutable context and owner,
retaining actual admitted tables and reusable buffers. Frame detach removes all
previous caller input/output aliases before returning; it does not refund live
reusable storage. Full reset or Close retires the context and its backing.
Different content-addressed definitions can reuse an inner ZSTD dictionary ID;
switching definitions can therefore reconstruct the decoder. Identity is never
established by that inner ID alone.
On initial borrowed ZSTD codec admission refusal, one nonblocking physical-mutex
attempt may refund only idle shared ZSTD storage and retry the same admission
once after a refund. The original live native capture also exposes this operation
to prepared-pack admission after its complete reservation refuses capacity. Only
an actual release permits one retry of that same reservation. The operation
removes decoder, definition and output references before refunding their original
receipt; busy borrowers return immediately, and small raw Snappy/LZ4 block codecs
remain. Allocators and retained receipts acquire no cache backpointer. The
original finite limits and legitimate capacity refusals remain in force.
Warm decoders survive producer Close while held captures remain; a cold read
after Close refuses fresh admission. This does not change the native external-leaf
producer's codec selection or the original record/CRC verification policy.

Synchronous DB point reads (`Get`, `GetAppend` and versioned variants) append
the fully shape-checked, CRC-verified and decoded winning value directly into
caller-owned output while the original workspace/physical-read lock and exact cut/file pins
remain held. This avoids both shared value-output and cut-local value-output
materialization. Input/raw/decoder admission and dictionary resolution remain
unchanged. Exported Snapshot, iterator, leaf and callback View materializers keep
their existing admitted output ownership; shared decoder aliases never escape.

Every physical leaf read still inspects the current record metadata before
reusing a verified decoded cache entry. Capable dictionary leaves participate in
that same cache after the original native/capability and shape/limit policy.
Population requires the existing independently checked definition identity, full
record CRC and decoded-page validation. With the cache's fixed COW limits, the
verified immutable page remains usable after a producer decoder is retired:
a hit needs no decoder, dictionary recapture or physical decoder mutex. Private
callers using different limits bypass dictionary hits and follow decode/refusal.
Captured-only providers keep their original local definition policy before any
cache reuse. Leaf inspection reads one fixed maximum
metadata window, interprets only the checked header and frame prefix, and ignores
extra bytes. EOF beyond complete required metadata is harmless; truncated
metadata and corruption retain the original errors. Uncompressed value-prefix
checks reuse the window when present and otherwise read their exact bytes.
Registered file identity checks, full CRC verification before cache promotion,
page validation, cache disablement and captured-cut lifetimes remain required.

After a full verified decode, repeated COW misses may populate the original
backend leaf cache through explicit cache-owned admission. Each configured slot
can lazily allocate one 4096-byte page, reused on eviction; no reader scratch or
file is retained. The default 8192 slots bound page backing to 32 MiB, and the
maximum 262144 slots bound it to 1 GiB, shared across readers. Admission keeps
the cache's existing repeated-miss, epoch, victim and nonblocking lock policy;
disabled caches or contended slots decline while the uncached read succeeds.
The separate `StoreVerifiedCachedLeafPage` API still permits only existing
backing and never allocates a cold page. Cache `bytes` counts valid-entry bytes;
`backing_bytes` counts allocated slot backing, including legacy/write admission,
which can remain retained independently of validity. This cache-owned capacity
is separate from the caller's temporary workspace admission.
Existing byte-identical pages can be marked record-verified. Replacement resets
independent page-checksum verification, and every subsequent hit still requires
record inspection through the exact retained file identity.

On Windows and race builds, `ReadAt` makes fixed inspection buffers heap
allocations. The serialized read workspace conservatively reserves the original
three-buffer, separately rounded 4,144-byte peak envelope before inspection and
keeps it through Close; the consolidated leaf window fits that same envelope.
Normal Unix builds retain stack inspection and their zero-allocation check.
Leaf namespace checks avoid the Windows volume/parent string join for canonical
native file paths produced by `filepath.Join`. Arbitrary noncanonical paths may
need lexical normalization allocation and are outside the low-level bounded
leaf-read caller contract; legacy namespace classification remains unchanged.

Successor reads search the captured cache roots using existing immutable
lower-bound lookups. An exact empty retained backend user root omits the disk
probe. Otherwise one owned disk candidate competes with the immutable sources
on the same cut, preserving newest-source ties. Winning physical tombstones and
missing successor capabilities use the general merge on that same snapshot. Selection,
materialization and caller copies retain the ordinary read/Close ownership;
the Store uses this same successor on its admitted snapshot after releasing
floor admission; it never seeks through a newly captured DB cut.


`GetManyView` retains one immutable snapshot, materializes bounded admitted
temporary value ownership per callback, and invokes callbacks outside read,
writer and publication locks. Callback values remain read-only and valid only
for that callback. Empty input does not bypass closed-handle checks.

## External MVCC read admission

The resolved public `SupportsMVCCReadCut` capability qualifies whole-call atomic
publication plus error-returning `AcquireMVCCReadCut`, using the existing Snapshot
owner and `SeekGEVersionRange`. The Store caches this qualification once. Legacy
successor-method presence does not establish it. Qualified grouped commits retain
shared floor admission through ordinary `Write`/`WriteSync` acknowledgement and
batch cleanup; floor advancement still waits for admitted commits.

Reads check the floor and capture one physical cut under shared admission, then
release admission before materialization. Point seek, iterator construction and
decoding use that cut, preserving old same-ts replacements and excluding late
historical inserts. Independent calls remain independently fresh. Capture refusal
keeps its capacity/closed error and never falls back to an unqualified producer.
Snapshot/iterator Close releases the single owner and preserves ordinary error
reporting and borrowed/owned output contracts.

The point successor's disk/tombstone merge uses an internal iterator inside the
already admitted read, closing its sources and workspace before ending admission.
It does not recursively admit a snapshot-bound iterator behind queued DB Close.

COW `PruneVersions` explicitly refuses eligibility before floor re-sync or WAL
side effects. This read milestone is independent of the native bounded pruning
producer. Full C3 maintenance/replay/lifecycle qualification and C4 sustained
performance qualification remain required. The bounded public diagnostic and
allocation ownership audit are documented in
[the C3 read fixture](../benchmarks/cow-c3-read-5076/README.md).

## Supported mutation and refusal boundary

The initial capability supports `Set`, `SetSync`, `Delete`, `DeleteSync`, point
batches, encoded MVCC point/grouped commits and recorded raw physical deletion
replay. Reads include owned point/versioned reads, presence/prefix queries,
forward successor and forward/reverse iteration over the same coherent cut.
Empty point keys and values preserve the ordinary public API semantics through
staging, checkpoint and reopen. Successor presence is reported by `found`; a
zero-length returned key/value alone does not indicate absence.

Callback mutation (`Update`/`UpdateSync`), native range
deletion, adapter-only after-write `SetViewWithReplayBytes` /
`DeleteViewWithReplayBytes` retention, and manual-root/backend mutation bypass are
unsupported. Refusal precedes user callbacks, command append, MVCC floor
mutation and visible point effects. Unsupported batch operations are rejected at staging before adding an
operation; callers must honor the returned error. A command containing an
unsupported operation cannot reach publication. Large sorted batches remain on the COW prepared
path rather than activating the legacy streaming bypass.

## Prepare, canonicalize, finalize, publish

The command sequencer covers all changed-shard preparation and complete-cut
installation. For ordinary raw writes, all allocation/refusal points precede
acceptance:

1. Admit staged operations, successor roots/cut and concrete resource owners.
   Public command-WAL batches admit their own wrapper and scan the inner owned
   entries, avoiding a second staged payload copy.
2. Assign final `EntryRevision` and cached `IsPtr`/`ValuePtr` through the existing
   prepared-revision authority; prepare immutable roots from those owned entries.
3. Let the canonical backend intent resolve RID, encode the existing command
   payload and capture its ordered external dependency closure.
4. Before append, validate canonical RID/pointer/revision identity and attach
   an independently retained generation read lease. WAL dependency custody
   remains owned by the WAL intent/debt, including materialized/self-contained
   frames with no external closure.
5. Append using existing command-WAL hooks, release the WAL barrier, and install
   the already-prepared complete cut without allocation, callback, rebase or
   rebuilding roots.

Newly produced pointer bytes are flushed for live readability through the
registered producer before publication. This visibility flush is not fsync or
a durable acknowledgement. A single ordinary unsynced COW point Put with an
external command WAL uses the existing shard-affine value-log lane when pointer
placement is required. This applies to both no-flush placement and the relaxed
profile's buffered visibility flush. It avoids opening unrelated cold lanes;
multi-entry commands and durable sync reservations retain ordinary lane selection.
Canonical preparation, registered pointer visibility and publication remain the
same. A busy affinity lane uses the existing lane admission and Close protocol.
Read owners use the existing physical identity pin
registry and registered value-log subset, with one owner per newly introduced
distinct resource rather than one per historical RID. Actual frame dictionary
IDs select retained definitions; cut readers do not resolve through unrelated
global definition/codec state.

`no_wal_fast` uses the same canonical RID planner and final metadata authority
without appending a journal or retaining journal dependency custody. Registered
producer visibility and independently owned live file/dictionary pins still
precede cut publication. Explicit NoWAL sync releases command/writer/admission
ownership before the existing sealed-root Checkpoint. Pre-append refusal has
no visible effect; post-append errors retain existing ambiguous/poison/reopen
rules.

## Captured-prefix flush and backend maintenance

Write/maintenance rollover freezes a bounded source frontier without walking
its records. Flush retains that prefix and its exact backend basis, streams
bounded canonical physical chunks oldest to newest, and uses one existing
`RootPublicationBuildGroup` for one final backend publication. Intermediate
chunk progress does not remove old-prefix precedence.

Before backend acceptance, the group admits the exact successor Snapshot and
handoff/cleanup capacity. After acceptance, handoff adopts that retained receipt's
basis, without acquiring a current Snapshot, and removes only the exact
proved-covered prefix. Newer mutable roots and later frozen sources stay in the
successor cut. Handoff admission refusal retains
the accepted receipt and captured prefix; a permitted retry finishes handoff
instead of reapplying the prefix. Backend acceptance remains an irreversible
receipt even if later durability waiting or cleanup returns an error. The
reported error and existing recovery policy remain authoritative.

Checkpoint serializes through `flushMu`, takes exclusive write admission for a
brief source/WAL frontier cut, then releases writer/admission locks before
value-log sync, backend build/publication and handoff cleanup. Post-frontier
writers are admitted only under the existing command-WAL cutover contract;
NoWAL retains its full checkpoint drain. Applied-LSN coverage and cleanup use
the existing dependency-prefix and sealed dual-meta authorities.

Backend compact/vacuum, GC/rewrite and leaf-generation maintenance serialize
through the same flush fence and refresh an equivalent backend basis while
preserving all cache sources. A failed refresh blocks fresh writes until a
permitted refresh succeeds. Old cuts keep exact physical read/deletion pins.
Selected-source rewrite uses the existing exclusive writer/rotation fence and
revalidation before reclaim. `CompactStorage`'s specialized
`UnsafeValueLogReclaimFencedUnreferenced` option is refused before checkpoint
or writer rotation in this mode; its pre-held fence cannot be nested inside
the COW maintenance fence. Ordinary `CompactStorage` remains supported.
Its automatic unsafe reclamation policy stays disabled for COW. Successful
concrete backend registration of a rotated leaf segment already transfers its
registration and manifest identity to that backend; COW does not retain duplicate
pending registration debt across an empty checkpoint. Unreferenced empty leaf
segments can therefore be reclaimed without later trying to reopen them.
After backend compaction refreshes the COW basis, the same maintenance writer
fence covers one ordinary leaf-GC settle pass for generations held retiring by
the old basis. Independent old cuts and every recoverable slot/parent retain
their existing protections. The returned usage/debt audit follows that settle
pass; errors or canceled operations do not authorize it. This pin retirement
adds no checkpoint or durable-root publication.
Value-log segments remain persistent storage: deletion uses existing
reachability/pin/scanner authorities, never segment age or a second COW GC
tracker. Retired owner cleanup occurs outside writer/admission/cut locks.

Native COW online index vacuum borrows the original private backend's staging
owner through `LockPrivateCOWStagingGuard(db.txnOwner)`, keeping the existing
maintenance-writer, group and raw lock order. Native activation still rejects the
unowned staging entrypoint. This changes neither accepted command identity nor
WAL acknowledgement and does not authorize higher-level barrier callbacks on
that private backend.

Native producer pressure preparation joins at most one captured-prefix materialization
and the original semantic cleanup frontier before consumer locks. It also inspects
pending typed construction through an optional immutable-state inventory,
borrowing the original retained cut and physical basis off the cut latch. That
inspection cut is released before maintenance; it creates no snapshot or
allocation authority. Its complete construction inventory T includes the maximum
operation workspace F once, because typed collections materialize sequentially.
For each finite maintenance byte limit L, advisory scheduling applies the same
three-quarter watermark to variable construction T-F against remaining capacity
L-F: pressure is T-F >= (L-F)-(L-F)/4. Subtraction retains upward rounding without
multiplication overflow. F=0 retains the original watermark; invalid T<F or F>=L
requests maintenance conservatively. Raw reserved-byte, source, generation and
typed-member pressure remain independent. The full T is still reported and used
by exact final admission and original build reservations; this scheduling policy
grants no credit or authority for an incoming write. Additional capacity refusal
on a required public workload is a failed scheduling candidate.

With an external canonical command WAL, the original actually-started automatic
maintenance worker may supply a positive size trigger, a total-WAL-byte hook,
the original cutover hook and deferred covered-prefix cleanup capability. Only
in that configuration, pressure preparation uses the same writer fence, finite
rollover retry, captured raw+semantic cut, value-log sync, build group and basis
handoff without forcing active-journal rotation or covered-file maintenance.
It joins that publication's original physical seal receipt and accepted-prefix
retirement, then the original semantic refund frontier. The root records the
same original captured AppliedLSN; a successful request's original durable WAL
ACK and recovery skipping remain unchanged. The active journal remains
appendable, and original append-size rollover still applies.

The running worker retains its first captured size threshold; later
StartAutoCheckpoint calls do not change that worker's behavior. Capability
selection uses that captured positive threshold, not merely a later notification
setting. If this capability is absent, pressure retains the full checkpoint
path. Explicit Checkpoint, Close, public Drain and automatic maintenance retain
their existing boundaries. The default 2 GiB trigger is asynchronous and soft,
not a hard bound on journal growth or a cleanup deadline: held roots, busy
maintenance, errors and scheduling can retain files. Pressure does not install
another worker or change any finite producer limit.

If the same finite live accounting, semantic construction debt, physical basis
and logical sequence remain after a successful drain, later producers skip
another identical drain. Retained readers still cause ordinary admission
refusal. Changes to outstanding ownership, physical basis or logical publication
permit maintenance again; historical peak alone does not. A concurrent logical
suffix prevents remembering that checkpoint as covering the new work. The
remembered state holds no additional captures or allocation leases, and is
covered by the original cache header admission.

The four existing gated native CRUD paths revalidate a value-only current
pressure observation after acquiring their collection mutation gate, before
opening a transaction or invoking a document callback. Changed pressured state
releases that gate before the same producer checkpoint and cleanup join, then
revalidates again. An unchanged pressured post-preparation observation proceeds
to exact admission/refusal; pinned owners cannot cause a maintenance spin. The
non-draining observation retains no cut on return, never waits for the producer
maintenance mutex and grants no reserved headroom. No application body is retried
by this pre-opening loop, and different collections keep their original gates.
Plain collections retain the original advisory preparation without a new gate.

## Complete-prefix and native ownership seam

The existing lower publication owner keeps one finite accepted FIFO for the DB.
Each entry distinguishes canonical assignment or uncertainty, complete logical
installation, WAL completion and the exact physical seal. V (visible logical
sequence), C (visible command prefix), W (stable WAL prefix) and S (sealed root)
are separate; a NoWAL publication has no fabricated command LSN or W advance.
Installed ordinary raw-only publications share a coalesced FIFO coverage run.
The run's descriptor and receipt name its exact endpoint; iteration is not one
visit per raw command. No raw effects move into this queue: immutable COW roots
still own them. Native custody, projection, exact-seal request, charged and
root-neutral entries retain their original independent slots. Canonical V+1/C+1
validation occurs before whole-cut installation and only then can its raw slot
merge, so assigned failures cannot disappear into a run.

Before releasing the original writer exclusion, rollover marks its current
exact V/C endpoint in that same FIFO. A grouped durable ACK can append a bare
root-neutral command after that cut's last root-changing mutation. Rollover
closes this suffix only from the cut's actual retained endpoint (or sealed S),
through contiguous installed root-neutral entries with unchanged V and no
projection, custody, seal target or resource charge. Proof and the original
strict boundary mark share the accepted FIFO mutex. An assigned barrier still
installing is joined through the original condition, with poison and Close
checks; its installer does not need the held cache writer exclusion. The proved
command endpoint becomes the successor's visible and frozen C, including when
only the barrier changed. No command coverage is inferred from V distance.
Writes after cutover cannot extend this
entry, including writes before the physical worker calls CaptureAcceptedCut.
Capture accepts only a retained endpoint; it never derives an interior LSN from
logical sequence distance. Active or handed-off entries cannot merge. Empty
idle convergence captures the already sealed S boundary. No additional clock,
ledger, background scheduler, or per-command allocation is introduced.

A captured finite prefix T includes every accepted entry through its boundary,
with sparse native domain projections against the same complete raw/system/catalog
basis. One build group publishes that boundary. An uncovered legacy producer
blocks the raw path before append; a partial native candidate cannot advance
coverage.


COW checkpoint and Close take canonical coverage only from the retained cut's
`frozenCommandLSN` and its exact disk snapshot basis. The accepted queue must
prove every assignment in that interval installed before the same BuildGroup
publishes it. Public raw pending counters are notifications, not coverage; native
and raw ordering does not change that rule, and no separate root-neutral batch
completes COW coverage. The flush returns its actual coverage/completion status
for cleanup, including native-only prefixes with zero raw entries.

At the exclusive writer cutover, the COW cache rotates the active command WAL
independently of public raw notifications. After value-log lane flush, it preserves
the WAL flush/readiness boundary before building the exact captured prefix. Later
suffix bytes may become durable without entering that root. Rotation or root
publication never fabricates W; only the original canonical WAL owner records
its completed stable prefix. Non-COW caches retain their public producer coverage
provider and existing group-commit behavior.

The fixed backend-to-cache native bridge uses the existing owner and command-WAL
handler registration direction. Identity-free capability, dependency, footprint
and admission checks run before canonical assignment. An internal admitted native
finalizer may then construct actual-LSN assets privately. Any failure after
assignment retains the original sparse partial-resource ledger and assigned
identity, poisons continuation and installs no partial cut. Final installation
under the cut latch does not allocate or execute a destructor. A stale equivalent
physical basis requires preparation against the compatible latest basis before
acceptance; it is not itself a logical transaction conflict. The same bounded
transaction journal separates raw and semantic namespaces. These lower seams do
not activate collection producers whose legacy barriers are still registered.

Native build materialization of an already logically accepted plan is terminal
on failure. Only `NativeSemanticBuildRetryableRefusal` proves an untouched
preadmission refusal; an ordinary capacity error after original asset creation
cannot restart or overwrite that work. Accepted disk publication retries only
its exact handoff and never rebuilds or reapplies its prefix.

A cold native capture without published semantic state may expose an optional
cut-owned catalog projection. One serialized initializer runs the unchanged
catalog importer against that exact original capture, including schema,
incarnation and physical-root validation. A successful import transfers its
original engine reference and the same admitted immutable catalog maps to the
cut; readers borrow them while retaining that cut. `State()`, logical sequence,
snapshot and publication roots remain unchanged. Successor cuts start with an
empty projection slot; held readers retain their original catalog and basis.
No manager/global catalog cache or replacement capture is introduced.

Failed partial imports are never shared and follow the original private-read
cleanup after initialization locks are released. The embedded projection and
capture controls use the existing containing-allocation admission formulas;
catalog backing keeps the importer's original receipts. Coheld old and new
catalogs remain subject to the same finite admission limits. Last-cut release
detaches the projection reference and sends its original cleanup record through
the existing semantic cleanup queue before withdrawing the active cut.

Last semantic release detaches one preborn cleanup record in constant work and
enqueues it before the last retained cut leaves the active-cut count. The
existing flush worker performs actual destruction outside publication, build and
cut gates; cleanup wakes preserve unrelated coalesced flush requests. Close stops
admission and joins outside worker-needed gates. If user cuts survive Close, the
same worker continues only cleanup until closed, initial Close cleanup complete,
zero active cuts and no runnable/running cleanup are serialized together.
Successful destruction returns an independent accounting receipt; original record
and resource aliases end before that receipt refunds their containing allocation.
Failure parks the same original Work, objects, error and charge, without replay.
A tiny guard bound at Open rejects same-caller public/cache/backend Close from
actual Finish, destruction and error-notification callbacks before side effects;
unrelated Close waits normally. No additional scheduler is introduced.

Explicit sealed requests release their publication/materialization in-flight
count and original staging, collection and teardown progress gates before
waiting on their own physical receipt. Deterministic apply computes its immutable
accepted proof before releasing the owner and publishes result/progress only
after that exact seal succeeds. Reusable vector captures keep their caller-owned
pin while releasing the storage-progress barrier needed by the seal worker.
NoWAL explicit boundaries retain the full captured drain.

## Verification and performance scope

The [verification specification](verification.md) separates public route,
metadata/atomicity, handoff, lifetime, process-crash and modeled power-loss
proofs. Source-bound internal passing tests do not substitute for public COW
dispatch. Match ordinary and `treedb_safe` claims to their affected paths.

Fixed-shard/source capture cost must be independent of historical record count;
changed-operation preparation still depends on tree height, payload and
resource setup. Scanning costs include actual returned/visited records. Compare
N and 2N dirty fixtures across `append_only`, `btree` and `cow_btree` using the
same profile/instrumentation, including acquire/read/release, allocations,
tails, sync/checkpoint and pinned/retired/drained memory. Tiny MVCC fixtures are
diagnostic. No production speedup or default promotion follows from C1's
internal benchmarks; sustained public qualification belongs to C4.
The [current repair and cost report](../benchmarks/cow-c2-ci-repair-5046/report.md)
and coordinator disposition quantify the remaining pointer-read, acknowledged-write
and public MVCC costs. The [earlier integration packet](../benchmarks/cow-c2-integration-5046/report.md)
retains its historical source identity. Snapshot gains alone do not establish workload suitability.

The allocation bound and safe-build tradeoff are specified separately in
[immutable memtable ownership](cow-memtable-ownership.md).

Selected publisher manifests do not export `Entries()` snapshots: their strings,
frontier storage and namespace metadata belong to admitted immutable encodings.
`WithEntriesV1` is a synchronous read-only scope whose callback cannot retain or
mutate any supplied metadata, or pass it to a constructor that borrows strings.
The scope retains the same allocation edge and reserves its actual entry-array
scratch before allocation. Refusal runs no callback. Release during a scope ends
the publisher's edge but preserves the encoding and charge until the callback
ends. Generic and recovery-loaded manifests keep their existing diagnostic
getter lifetime; the recovery resolver refuses selected borrowed metadata when
constructing independent long-lived resource tokens.

### Selected immutable resource metadata

Selected PRIMARY publication admits new token descriptors, namespace/handle
backing, immutable resource entries, persistent obligation indexes, proof/rope
nodes, manifests and constructor scratch before allocating them. Each distinct
physical edge retains its original registry/operation/deletion authority.
Importing a pre-existing generic provider admits independent selected metadata
before its first visible clone; it does not relabel that provider's physical
owner or copy an opaque callback as a completion certificate.

The persistent obligation index is the canonical selected history. Same-identity
coalescing retains exact existing nodes and adds admitted changed paths rather
than cloning the whole history. Selected borrowing scopes cover actual decode,
serialization and callback use. Long-lived selected diagnostics require their
own admitted owner and explicit Close; raw selected getters refuse. Detached
legacy telemetry copies only scalar values and canonical built-in kind names,
retaining no selected strings or tokens. Generic diagnostics keep their existing
post-release contract.

Private manifest construction admits contiguous page images, one reusable
canonical encoder page, bank-ref backing and child-first traversal frames. Full
replacement backing is admitted during overlap. Pre-Open dependency leases use
intrusive links in the existing index owner, and failed private bank cleanup
remains in the SAME arena owner's preadmitted failure custody. These ordinary
helpers and their synchronous cleanup are not bounded native prune evidence.

The [ordinary PRIMARY allocation inventory](../design/ordinary-primary-allocation-5111.md)
records the actual constructor/capacity/caller and last-alias boundaries. Public
set wrapper disposal refunds its own admitted allocation while an active scope
retains immutable descriptor and original physical closure. Same-identity
coalescing updates the canonical selected index without mutating an older shared
rope's frontier. Temporary captures and real producer enrollment are distinct.

Ordinary PRIMARY component claims hold the arena lock across preparation and
installation. Background manifest allocation therefore cannot invalidate their
private epoch between these steps. The existing nonnil ordinary work meter still
records the same reservations; resumable maintenance retains its split
PrepareClaim/Step contract and epoch validation. If private cancellation fails,
the already admitted arena receipt retains the exact claim and both causes.
Further claims and physical Close report that same failure without retrying,
discarding private mappings or refunding their charge.

Snapshots retaining PRIMARY roots also retain the existing physical arena owner.
Their original last-read finalizer drops the bank edge, settles original cleanup
and releases that physical owner before detaching the paired governor. DB.Close
cannot dispose the mapping needed by this finalizer. Cleanup failure retains the
exact Snapshot at that owner; private one-shot reads cannot reuse a failed owner.
This extends physical cleanup custody without adding a Snapshot read-after-close
promise.


A successor backend basis preadmits its existing wrapper, retained VLog/leaf
capacities, pin vector and original cleanup custody before capture effects.
Startup, handoff and maintenance use the same admission and adoption boundary.
The original Snapshot finalizer reports actual completion after alias scrub and
its existing pager callback's available final action. Accepting Snapshot.Close
while a reader remains does not refund the basis. A physical cleanup failure
retains that exact basis and its charge in the SAME identity registry, including
DATA-only captures; repeated Close does not replay cleanup or refund failure.
Independent surviving pager descriptors retain their own original operations,
without extending an already scrubbed Snapshot payload's lifetime.

Bounded capture performs external admission before taking the existing state
read lock. It then revalidates the exact view under rootReuseMu followed by
db.mu, before reader registration or PRIMARY references. Same-root topology
refresh during admission refuses this capture; retry uses the successor view.
The captured VLog Set is acquired after fallible reference admission while the
state lock excludes replacement, and the lock is released before leaf setup.
This adds no publication authority, changes no command-WAL acknowledgement,
and does not qualify the remaining ordinary allocation, platform or cost gates.

Public COW point, many, append, presence and successor reads return any available
original completion error alongside their operation result. Successful cleanup
preserves existing missing-key normalization and refusal sentinels. The owned
iterator closes its bound child after excluding active moves, detaches consumed
Snapshot aliases and only then releases its wrapper allocation; failed physical
basis cleanup retains the original basis independently.

Public raw conditional transactions are supported under the ownership and bounded
history rules in [raw-cow-transactions.md](raw-cow-transactions.md). Standalone
caller-owned backend combinations remain fail-closed.

Native physical rollover prepares an owned active successor through
`NativeSemanticState.PrepareRollover` under the existing writer owner, before
raw source freezing and cut installation. The whole cut adopts that edge and
separately retains the original captured semantic state. Neither the logical
sequence nor canonical identity changes. A nonnil error result also transfers
one private edge; its preborn cleanup record already owns every partial
allocation, and cancellation queues it outside publication and writer gates
before dropping the opening retained cut. Native-only captured coverage may
therefore drive the same finite capacity drain/retry as raw frozen tables.

`NativeMaintenanceAllocator` exposes only the existing reserved maintenance
budget capability. Native build ledgers and handoff metadata must reserve their
finite capacity before physical acceptance, so producer exhaustion cannot force
postaccept recapture or block the exact accepted handoff. Source generations and
source-slot ownership remain on their original producer budget. Full native
collection source representation and producer activation belong to the native
collection packet; the legacy producer fence remains until that migration.

Native two-phase transactions use `BeginNativeConditionalTxn` on the same fixed
backend bridge. Its opening State, Snapshot and sequence borrow the original P1
`ConditionalTxn` snapshot; commit uses that transaction's original batch and
raw read/range dependencies. The caller cannot replace its opening sequence.
The original owned `NativeLogicalPlan` contains immutable staged effects and
prepares them against latest compatible semantic roots under the existing
writer/publication gate, after checking all original raw and native read/write
dependencies and history floor. This internal operation does not recapture,
invoke replay preparation, reacquire teardown protection, wait or publish.
Journal preparation and canonical append occur only after the complete private
result passes its exact-basis invariant. A nonnil partial state returned with an
error transfers one edge to existing off-gate cancellation; original custody is
queued before the transaction's opening cut withdraws. Commit/Close terminate
the original transaction once. Physical handoff alone is not a logical conflict;
changed logical dependencies still refuse before assignment.

## Ordinary DATA dependency projection

Ordinary point-put DATA publication can update its exact external dependency
closure from the successful zipper apply's old/new outer-leaf edge counts and
logical value-pointer delta. Span-native apply collects the same receipt from
its validated leaf spans and final reducer outputs after successful reduction,
before releasing those operands. Only pure point puts produce this evidence;
maintenance point operations retain the exact scanner. Private build groups
compose these signed counts,
including cancellation of intermediate leaf replacements. A separate process-local
raw census uses the existing value-log reference tracker's admitted metadata owner;
it is never serialized as logical ValuePtr accounting. It is installed only after
successful root activation and is bound to that root pair, commit sequence and
physical index incarnation. Replaced, aborted and closed evidence releases its
original metadata charges.

This projection requires a distinct user root and an empty pager-backed system
leaf, which rules out attached descriptors and role aliases. PRIMARY roots, delete
operations, unsupported producers, stale evidence and invalid arithmetic retain
the exact candidate scanner. Reopen starts without a raw census and reconstructs
one through that scanner. Snapshot resource pinning, recovery validation and GC
reachability remain authoritative and unchanged.

Cloning a predecessor census is optional. If its admitted metadata capacity is
exhausted (either retained metadata capacity or an enrolled COW budget capacity),
partial clones release their original charges before the existing dependency
planner or final-root scanner runs under the same finite budget.
Required capture admission can still refuse, and other ownership, identity or
registration errors remain fatal.

## Immutable map copy backing

COW map copies allocate independent item and child backing sized to the live
length plus one spare slot, capped at the tree maximum. Further private growth
uses the original admitted mutation work and records the actual allocation.
Internal nodes contain their child slice header in the same allocation; leaves
keep the smaller header. Admission charges the rounded containing allocation,
and owned cleanup clears its embedded child reference before refunding it.
Raw, logical and history trees retain degree 32; lower-fanout experiments did not
justify their additional allocation objects and mixed batch performance. No new
pool, arena, publication owner or retention lifetime is introduced.

### Public captured WAL epochs

A public durable COW mutation records canonical assignment without claiming WAL
stability, installs its complete cut, then releases original writer/raw guards
before waiting for the coordinator's captured exact prefix. Later complete cuts
may become visible during that sync, but their ACK requires a later certificate.
The original accepted FIFO and coalesced coverage runs remain the sole complete
prefix owner. The WAL owner alone calls ObserveStableWAL after successful sync;
checkpoint/root sealing does not synthesize W. V2 external dependency ordering
can still serialize earlier append preparation.

### Caller-owned logical MVCC acknowledgement handoff

`mvcc.SubmitCommitGroupAt` uses the same Store validation and discard-floor
exclusion, producer preparation, command append/registration, accepted cut
installation, and captured-prefix durability coordinator as `CommitGroupAt`.
The submit call returns after the complete logical cut is installed. It does
not acknowledge durability. The caller supplies a reusable `CommitCompletion`
record; a dedicated FIFO completion owner calls `Complete` to drive physical
progress, finish the original ordered ACK and release batch, lifecycle and
floor custody. Application `Wait` is not the progress driver. A record cannot
be copied, reused or abandoned while it owns a command. Completed records are
idempotent and reusable after all old users have left. Submission errors retain
no caller completion custody; assigned errors keep the existing poisoning and
uncertain-publication handling, never a retry or subdivision opportunity.
There is no engine request queue, per-request worker or new publication owner.
The ordinary blocking path remains inline; unsupported profiles and measured
phase instrumentation retain their synchronous writer.

`DB.EstimateMVCCWriteWork` supplies an overflow-saturating composable work
estimate for the Store-owned ordered logical path. It includes escaped codec
bytes, retained backing growth, placement scratch and canonical tree-copy work.
Mutually exclusive singleton insertion and existing-history promotion are not
added together. Whole-command cut/root/frame controls are reserved once per
aggregate in `MVCCWriteWorkLimit`; journal controls and the original maintenance
partition are deducted before dividing the producer envelope between two
simultaneously owned aggregate records. Each individual transaction and the
entire combined cohort must fit this same limit. Zero means unsupported, not
an unlimited certificate. Final canonical admission remains authoritative for
retained histories, pins and placement-discovered resources. This estimator
creates no lease, accepted ticket, LSN or durability certificate, and raw writes
in the Store's reserved namespace remain unsupported. Oversized indivisible
engine commands still refuse before append; consumers that already support
pre-commit chunking must apply that boundary before copying or assigning the
next physical command, and must not recombine chunks into an oversized cohort.

`Store.BeginCommitGroup(&builder)` retains the original floor exclusion and DB
lifecycle custody once. Each `AppendGroup` validates its entire new group
(timestamp, key envelope, duplicate physical version and retained floor), then
encodes into the existing native batch's owned arena. Caller operands may be
reused when AppendGroup returns. Before seal there is no assigned command,
accepted cut or root-dependent preparation. The native entry vector grows
geometrically; its old backing remains charged through the same batch lifetime.
`Submit(mode, &completion)` prepares the complete staged cut once and transfers
its existing custody without recursively acquiring either read lock behind a
queued floor writer or Close. `Abort` releases only unassigned staging. Every
Submit error releases the builder; an occupied completion retains its previous
command. Prevalidation errors are not storage errors and preserve the unchanged
builder until the caller aborts or continues. A staging/storage failure is
terminal for that builder and must never trigger assigned-command subdivision.
The incremental builder is scoped to the cached command-WAL logical route;
unsupported adapters/profiles keep their established synchronous writer.

The work limit reserves two aggregate envelopes, including unassigned successor
staging. Zero exclusively means unsupported. A supported configuration without
headroom returns 1, below every valid row estimate, so it must refuse shaping
rather than disable the bound. The public integration-test bridge at
`TreeDB/testutil/durabilitycut` shares the original serialized observer. The new
`AfterLogicalMVCCStaging` boundary witnesses successful native owned staging; it
is not a persistence event, accepted prefix or acknowledgement.

Consumers capture `DB.NewMVCCWriteWorkEstimator()` at Open and use its immutable
value for per-entry shaping. It contains only scalar canonical path/layout
constants, retains no engine/budget/root pointer, and remains safe after Close.
This avoids per-entry lifecycle locking and repeated tree-height computation.
The convenience DB estimate/limit methods retain lifecycle protection; their
unsupported/closed result cannot race a cleared cached owner.
