# Immutable COW cached publication

`Options.MemtableMode = "cow_btree"` selects an explicit immutable cache for
raw point operations and externally encoded MVCC versions. It is independent
of the durability profile. Existing `append_only`, `btree`, adaptive selection
and profile defaults retain their own dispatch. COW is a pre-alpha capability;
selecting it does not add conflict detection, a transaction scheduler, or new
MVCC timestamp semantics. `TreeDB/mvcc` retains Store floor admission while
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
| `MaxGenerations` | 64 | Writable, frozen and still-pinned generations |
| `MaxSources` | 32 | Frozen sources through final retained-owner drain |
| `MaxResources` | 256 | Distinct generation resource identities |
| `MaxGenerationBytes` | 64 MiB | Cumulative generation allocation charge |
| `MaxTotalBytes` | 512 MiB | Generation history and admitted external storage |
| `MaxRetiredBytes` | 512 MiB | Conservatively reserved potential retirement |
| `MaxInFlightBytes` | 64 MiB | Preparations and outstanding external leases |

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

## One immutable read cut

A cut contains a fixed shard-root vector, a bounded ordered frozen-source
vector and one retained backend snapshot basis. Its owners cover the exact
index generation, value-log files and dictionary definitions used by reads.
Acquisition pins the existing complete cut under `cutMu`. It neither rotates
mutable shards nor scans, copies or sorts historical records.

Writers publish one complete successor cut. A reader sees the entire old or
new command, including a command that changes multiple shards. Newer mutable
roots and frozen sources shadow older sources and backend entries. Physical
deletion markers suppress older entries; MVCC logical tombstones remain normal
encoded values interpreted by the MVCC layer.

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
that fact for an old cut. Value/leaf workspace and point page scratch are
admitted only when actually needed. Codec slots grow within finite admission,
and actual Snappy/LZ4 headers use bounded native owners without constructing
ZSTD state. Owned callback copies remain separately admitted even for inline
values.

On Windows and race builds, `ReadAt` makes the three fixed inspection buffers
heap allocations. The serialized read workspace reserves their separately
rounded 4,144-byte peak envelope before inspection and keeps it through Close.
Normal Unix builds retain stack inspection and their zero-allocation check.
Leaf namespace checks avoid the Windows volume/parent string join for canonical
native file paths produced by `filepath.Join`. Arbitrary noncanonical paths may
need lexical normalization allocation and are outside the low-level bounded
leaf-read caller contract; legacy namespace classification remains unchanged.

Successor reads search the captured cache roots using existing immutable
lower-bound lookups only when the exact retained backend user root is proven
empty. They inspect all captured shards/sources and preserve newest-source
ties. Winning physical tombstones, nonempty disk bases and missing successor
capabilities use the general merge on that same captured snapshot. Selection,
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
forward successor and forward iteration over the same coherent cut.
Empty point keys and values preserve the ordinary public API semantics through
staging, checkpoint and reopen. Successor presence is reported by `found`; a
zero-length returned key/value alone does not indicate absence.

Callback mutation (`Update`/`UpdateSync`), CAS/conditional transactions, range
deletion, reverse traversal, adapter-only after-write `SetViewWithReplayBytes` /
`DeleteViewWithReplayBytes` retention, and manual-root/backend mutation bypass are
unsupported. Refusal precedes user callbacks, command append, MVCC floor
mutation and visible point effects. Unsupported batch operations are rejected at staging before adding an
operation; callers must honor the returned error. A command containing an
unsupported operation cannot reach publication. Large sorted batches remain on the COW prepared
path rather than activating the legacy streaming bypass.

## Prepare, canonicalize, finalize, publish

The command sequencer covers all changed-shard preparation and complete-cut
installation. All allocation/refusal points precede acceptance:

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
a durable acknowledgement. Read owners use the existing physical identity pin
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

After backend acceptance, handoff captures/adopts the successor basis and
removes only the exact proved-covered prefix. Newer mutable roots and later
frozen sources stay in the successor cut. Admission/capture refusal retains
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
Value-log segments remain persistent storage: deletion uses existing
reachability/pin/scanner authorities, never segment age or a second COW GC
tracker. Retired owner cleanup occurs outside writer/admission/cut locks.

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
