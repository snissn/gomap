# Immutable COW memtable ownership

`TreeDB/internal/memtable/cow_*.go` supplies the internal foundation for the
immutable cached-read integration. It is not a public DB mode: C1 does not add
`cow_btree` to `ModeFromString`, `Table`, or public dispatch. The existing mutable
`BTree`, its arena, `Freeze`, stolen batches and lock-holding iterators retain
their current behavior. No on-disk format, WAL, MVCC codec or durability default
changes here.

## Private preparation and immutable read headers

`NewCOWWriter` owns a degree-32 tidwall/btree v1.8.1 private map and one source
generation. `Prepare` first validates and reserves the complete allocation
charge, then forks that private header, copies accepted keys/values into owned
immutable strings and applies the batch. `Remove` physically deletes a key;
a normal entry carrying `FlagTombstone` preserves a cached physical-key deletion
marker and its revision. This is distinct from an MVCC logical tombstone, whose
existing codec remains in ordinary value bytes. Pointer/revision/flags metadata
is stored verbatim.
Replacing an existing key reuses its already owned immutable string; only its
new value is copied. Get/successor/cursor records share immutable strings and
create no intermediate payload copy.

Preparation forks again to create a separate read header. Only private headers
call `Map.Copy`: this operation changes the source isolation ID and shallowly
copies values. A published read header never calls Copy or a mutable dependency
method. No value Copy/IsoCopy method, arena, pool reuse or borrowed payload enters
this capability. Published `COWRecord` strings are immutable; byte conversions
for safe output create caller-owned copies. Pointer resolution requires the
enclosing cut/view lease to remain alive.

One preparation may be pending per writer. The cached installer must keep its
external complete-command sequencer across all changed-shard preparation,
WAL/dependency admission and complete-cut installation. `prepared.Root()` is an
already allocated staged header for constructing that cut before frame
acceptance. It cannot be retained or acquired before `Publish`; it must not be
exposed or used to resolve pointers. `Publish` transfers already allocated state
and activates the root's first reference; it does not allocate, resolve data,
invoke callbacks or rebase. `Cancel` refunds private node/payload reservations
immediately and transfers attached resource cleanup with its retained charge.

## Generation, resource and retirement ownership

The writer, each distinct immutable root and independently pinned views/cursors
keep their generation alive. Multiple cuts retain the same root with
`Retain`/`Release`; the root keeps one generation reference until its last source
reference releases. Cumulative node/payload/wrapper history is released only
after the writer and every root owner are gone and deferred cleanup completes.
Replacements therefore consume generation history even when the logical table
size stays constant.

`COWPrepareOptions.ResourceSlots` reserves bounded distinct resource identity
and release callback staging. `ResourceBytes` covers the concrete independently
retained lease/closure capacities supplied by caching. `ExtraBytes` covers its
cut/vector/backend-lease/retirement and temporary allocations. Use
`COWAllocationCharge` for allocator rounding, and include the actual backing
capacities. This is an obligation on every C2 allocation site, not permission
to declare external allocations free.

`AttachResources` accepts newly introduced, distinct `COWResourceID{Kind, ID}`
identities and one independently retained owner callback. The generation's
identity and callback arrays have fixed capacities, allocated and charged when
the generation is created. Already retained identities are checked through
`HasResource`; they receive no new physical owner. There is no historical owner
per record, full-database resource census, physical GC registry or backend import
in this package. Caching retains its existing concrete value-log manager and
stable dictionary/template authorities, independently of WAL debt ownership.

Cancellation and final source release return a bounded `COWRetirement` ownership
transfer. Drain it outside publication/admission locks. Copied descriptors share
a cleanup claim, so repeated or concurrent drains cannot repeat callbacks or
refunds. Published callbacks run after every generation owner releases;
cancelled preparations release only their privately attached owners. Generation
history, resource identities and wrapper charges remain until all callbacks
finish. Cancelled callback/staging storage remains in `DeferredBytes` and
`RetiredBytes` until cleanup finishes. The budget lock never runs callbacks or
IO. The installer reserves its retirement queue descriptor/capacity before
accepting a frame; C1 creates no unbounded queue.

`Freeze` admits a write-triggered source rollover without walking records; it
requires no pending preparation. Construct/reserve the successor empty writer
and complete cut before freezing. Caching retains frozen membership until its
flush proves backend coverage, then releases its root. A closed writer also
marks any still-pinned generation retired. Old views are never revoked to meet
a budget. `budget.Close` denies new retention-increasing admissions while old
leases keep working; its own control wrapper stays charged until final release.

## Finite limits

Every `COWLimits` field must be positive and finite. Defaults are 1,024 active
views/cursors, 64 generations, 32 frozen/pinned sources, 256 distinct resources
per generation, 64 MiB generation history, 512 MiB total charge, 512 MiB retirement
capacity and 64 MiB in-flight reservations. They are internal starting limits,
not public production tuning or an RSS quota.

The source-bound mixed-operation charge remains intentionally conservative.
The measured 1,024-operation mixed witness reserves about 45.5 MiB and fits the
default in-flight limit; its 4,096-operation counterpart reserves about 182 MiB
and is refused under those defaults. Callers must admit the complete command
before its WAL frame, use validated larger finite limits when needed, or return
capacity refusal. A batch must not be split after acceptance to evade admission.

The retirement limit reserves potential retirement of *all* current generation
history, in-flight preparations and cancelled deferred owners. This
conservative precharge guarantees a later Close/Freeze does not depend on an old
reader releasing capacity. `MaxSources` counts frozen generations until their
last old source lease is gone and cleanup finishes, including flushed
generations pinned by readers.
`MaxGenerations` covers writable, frozen and pinned generations together. Views
and cursors consume the same view limit. Total charge includes budget control,
generation history, pending preparations and active view/iterator wrappers.

Capacity refusal happens before private allocation. The cached installer must
roll over from cumulative history on the write/maintenance side and stop before
WAL acceptance if it cannot reserve the whole command. Finite retention cannot
guarantee unconditional writer progress with arbitrary long-lived readers.
Close allows reclamation/admission to resume; Go GC may reclaim heap later.

## Source-derived allocation bound

The pinned dependency node has an isolation ID, count, pair slice and optional
child-slice pointer. `cowValue` owns strings, `ValuePtr`, revision and flags. Tests
bind the mirrored node/pair sizes and field offsets to the actual dependency.
The audited methods are `Copy`, `copy`, `nodeSet`, `nodeSplit`, `Set`, `delete`,
`nodeRebalance`, `Height`, `Ascend`, `Iter` and `Seek` in v1.8.1 `map.go`.

- Copies retain item capacity; branch copies allocate at most 64 child slots.
  Growth occurs only when capacity is below the maximum logical node size;
  Go slice growth is at most doubling before allocator rounding. Reservations
  bound 126 pairs and 128 children, rounded conservatively. A generation's
  maximum cardinality retains the bound after deletes shrink a small tree.
- A replacement copies one path and cannot split/grow. An insertion copies its
  original path, adds at most one split node per level and a root, and grows
  bounded pair/child arrays. Split arrays are shared; retry uses private nodes.
  Delete additionally copies at most one sibling per level; merging can grow
  an array twice. The bound accounts for discarded root-construction buffers.
- Height uses the greater of actual Height and the maximum at batch cardinality
  `current length + operation count`: a degree-32 height-h tree needs at least
  `2*32^(h-1)-1` records. No batch crosses an uncharged height boundary.
- Within one fork each original node copies only once, even for mixed operations.
  Original node count is bounded by `length/31 + 1`; fresh split nodes are already
  private. Per-operation growth remains charged. Insert-only batches also bound
  all geometric array growth and split truncations by eight full capacities per
  possible final node. Delete-only batches charge each original node plus four
  array-growth capacities. These caps reduce overcharge without a node census.
- Core reservations include both map headers, immutable root, candidate wrapper,
  owned string allocation capacities, resource staging arrays, fixed generation
  ID/callback arrays, writer/generation/budget wrappers and caller-supplied
  publication/lease allocations. No temporary entry array or payload clone is
  needed during preparation. Cursor reservations include wrapper, independently
  closable view, bound string and doubled read-only dependency stack capacity.

`SeekGE` traverses immutable nodes under its existing source owner without
allocating a cursor/view. `CursorWithExtraBytes` reserves an existing merging
iterator adapter and its domain bounds in the same admission; `Cursor` supplies
zero extra bytes. Independent cursors traverse concurrently. One cursor or view
serializes its own operations against Close, and a cursor owns its own lease even
when its creating view closes.

## Verification and measurement scope

`cow_baseline_red_test.go` retains deliberately failing pre-C1 shallow-byte and
unbounded-history witnesses behind `cow_baseline_red`. They are falsifiers,
not ordinary passing tests. `cow_test.go` covers input/output alias attempts,
published header identity, legacy arena poison/reuse isolation, cancellation,
split/delete/replacement/batch height, resource exact-once release, Close races,
finite retention, restart after release and source/allocator reserve witnesses.

Foundation costs are measured with:

```sh
GOWORK=off go test ./TreeDB/internal/memtable -run '^TestCOW' -count=1 -v
GOWORK=off go test -race ./TreeDB/internal/memtable -count=1
GOWORK=off go test ./TreeDB/internal/memtable -run '^$' \
  -bench '^BenchmarkCOW' -benchmem -benchtime=1000x -count=3
```

The fixed-count prepare benchmark stays within one finite generation. Its timer
includes Prepare/Publish/root release; fixture construction and final release
are excluded and named. Capture includes Acquire/Close. Scan includes cursor
construction, every Record/Next and Close. B/op/allocs, engine charge and actual
returned records remain explicit. Retention tests sample HeapAlloc/HeapInuse and
post-GC pinned/drained endpoints; samples are not exact transient peak or RSS.
These are internal mechanism witnesses, not public DB/Alpha performance or a
substitute for C2/C4 flush/WAL/checkpoint lifecycle qualification.
