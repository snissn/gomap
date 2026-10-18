# Native collection ownership construction

The optional `internal/cowbtree.OwnedMap` uses the maintained map's mutation
algorithms. A private builder forks a fresh isolation header while its caller
holds the opening immutable map. Sealing visits only new nodes and attaches
original child/value edges; it does not mutate an old header. Every mutable Map
entrypoint rejects a sealed original, including empty/no-op calls.

The builder admits a finite conservative mutation cohort before allocating its
header, copied nodes, backing or ownership metadata. Admission refusal leaves
the opening map unchanged. Sealing has no fallible resource operation after
admission. Ordinary readers retain one header, and last header release only
marks the original map eligible for worker cleanup. Neither operation scans the
catalog. Original shared node edges use atomic retain/release; the retained
opening cut prevents a live child from reaching zero during successor sealing.

Physical cleanup runs on the existing semantic cleanup worker. It clears each
successful child/value slot, continues independent siblings on failure, and
parks the exact failed graph and containing charge without retry. Value lifetime
adapters return original independent accounting receipts instead of refunding a
value descriptor while its map slot still aliases it. `Finish` similarly returns
receipts after clearing the header's tree/owner fields. The containing worker
must clear its original map/builder aliases before completing those receipts.
No refund receipt points back to a map, value, work record or destructor.

The first collection adapter, `nativeDomainState`, owns an immutable root-local
COW source, its physical base root and interpretation. Its worker detaches the
source field before draining the original COW retirement. Its descriptor/name
receipt returns through the map cleanup chain. The enclosing engine cut must
supply the exact physical snapshot for reads; the domain never recaptures a
manager's current catalog. This construction does not activate collection
producer routing.

The optional node ownership pointer affects the shared raw map layout. On
Go 1.26 amd64 the source node header grows from 48 to 56 bytes, crossing the
48-byte to 64-byte allocator size class. The memtable layout mirror includes
this pointer so its existing admission calculation prices the changed node.
Before maturity, compare exact baseline/candidate `BenchmarkCOWPrepareReplace`
binaries under the frozen measurement protocol and run the applicable product
raw-path gate. The pointer is a measured construction cost, not a free change.

The complete private engine graph now separates immutable schema, logical
collection incarnation, full domain/name directories and sparse pending
collections/domains. A shared schema is copied only at construction or explicit
replacement. Directory nodes own actual descriptor edges. Engine retain and
last release only change one atomic reference and return the original preborn
cleanup record; catalog traversal belongs to that record on the existing
worker. Its independently admitted terminal receipt owns no engine or work
backpointer. Sparse mutation custody separately owns only newly touched plans.
Assigned failure records the first error and retains those exact original plans
and charges, without retaining a complete engine ancestor.

Native pending sources have an active immutable root and a finite captured
source list. Physical source ownership and list-position metadata are separate:
multiple views/list positions share the same original root and source-slot
receipt. The receipt uses the root's original COW budget and the existing finite
source counter. Every variable native ACTIVE or captured generation reserves
one slot before its first accepted mutation, sharing the same counter with
raw frozen sources. Native read views must retain their enclosing engine/source
owner through all root access. A directly acquired lower COW view separately
retains its generation/view charge; it is not a detached native engine view.
Rotation does not freeze a generation with private forks. Already prepared
forks may finish or cancel privately; the complete-cut basis check must reject
an old physical basis before canonical assignment. New active writes after
rotation use a fresh generation. No-op rotation retains the same descriptor
without consuming another source slot. Canceled private rotation preserves its
original active generation's slot and adds no captured-source slot. The slot
refund follows the last real generation/root/writer/source edge.

The original native writer preadmits the physical source owner's metadata
before its first mutation can become accepted. Source-slot and metadata-byte
admission are one original budget operation. Forks share that generation's
original accounting cell. Capturing a root atomically claims one exact root
edge in that existing source, without asking the producer for
additional bytes. The cell contains no root, generation, engine or work
backpointer. Generation cleanup and source cleanup independently detach their
edges before the last independent allocation receipt refunds the cohort.
Canceled rotation releases its claim while leaving the original generation's
slot and metadata preadmission available for another attempt. Both installed history and pending
fork reservations include that cohort in the finite generation limit.

An engine acquired before rotation owns the active pending root, rather than a
later captured list descriptor. Its original generation continues to hold the
physical source slot after that captured list is released. Admitting active
sources before accepted writes also prevents an overfull first whole-cut
capture from stranding an uncaptured logical prefix. Actual public native
read-view and transaction routing remain activation gates; private constructor
controls do not establish those public paths.

Private whole-engine rollover adopts each partial domain, map builder and
collection into its preborn original successor work before the next fallible
stage. On error the returned successor is private and owned: lower publication
queues its release after leaving gates, without reading or installing it.
Before either helper returns, infallible sealing from the original admitted
cohorts converts every remaining private builder into owned child/value edges.
This also covers failure: a parked partial must survive opening/latest cut
cleanup without retaining that whole engine. Its maps remain private throughout.
Sparse source suffix construction clones only newer list
positions and shares original source slots; a failed suffix construction returns
its exact owned partial list and independent scratch receipt for worker cleanup.

Captured-prefix handoff remains a construction gate: it must clone only newer
list-position metadata, share original physical-source receipts, drop the
accepted prefix, and preserve newer active effects. Required build/handoff
metadata must use the existing preadmitted maintenance capacity before physical
acceptance. Producer exhaustion must not prevent completion of an accepted
handoff. These ownership foundations do not activate collection producers or
replace the required public transaction, recovery and retention evidence.

### Original handoff capacity (construction)

The sparse handoff helper replaces only captured touched domains with their
accepted physical roots. It preserves later active roots and newer captured
source positions, shares their original physical owners, and removes the
accepted captured prefix. Logical incarnation and interpretation checks keep
newer catalog changes. The separate captured successor removes the materialized
pending domains from its pending directory.

Before physical acceptance, `nativeHandoffCapacity` calculates capacity for both
possible successors. It includes the complete private successor ledger, three
global map builders, two map builders per touched collection, each touched
domain/collection descriptor, source-position and scratch capacities bounded by
the original finite source limit, and every independent allocation receipt.
Directory builders use the existing maintained map admission formula. Their
item ceiling follows the original governing `MaxTotalBytes` divided by the
concrete map-pair size: each logical directory item occupies an actual node
backing slot, and the original node cohort remains charged until its last edge.
This remains valid with aliased descriptor values and empty names. The
maintenance child is included through its original parent reservation, never
added as separate capacity. Captured mutation counts are added to that item
ceiling to match the maintained builder's conservative height formula. The
maximum representable height helper remains available as a conservative bound.
Default whole-prefix progress controls and producer eligibility remain required
before activation; no configured capacity is increased to conceal this cost.

`COWExternalLease.SplitNativeBytes` transfers already admitted bytes from the
same original held build reservation into independent child receipts. It uses
the same aggregate rounding as native admission, preserves an explicit original
build-ledger floor and the parent receipt, changes no total/reserved/external
byte counter, and checks the finite receipt-count bound. A child has no parent,
Work, state or resource pointer. Existing promised capacity remains usable after
fresh maintenance admission closes. An insufficient accepted promise is a
terminal original-ledger error, not a reason to acquire replacement capacity.

### Bounded physical domain construction

The private `nativeDomainBuild` owns one exact immutable domain descriptor,
bounded captured-source pointer scratch and a fresh 256-entry batch. All scratch
is admitted before allocation from original maintenance capacity. It applies
captured sources from oldest to newest, followed by the captured active root,
through the existing `RootPublicationBuildGroup.BuildNativeOrderedRoot` method.
Replacements, deletions and entry revisions follow that order. No current
catalog or engine ancestor is retained or sampled.

`COWRoot.ScanOwned` traverses under the existing descriptor/source edge; it
admits no additional cursor or view, including after budget Close. Batch bytes
borrow immutable generation strings through each synchronous apply and are
cleared before the next apply. Pointer batches also record the original file
IDs for the group's existing pin path, flushing before a fifth distinct file
would allocate a batch file map. Exact pointer/dictionary and typed asset
closures still have to accompany the complete build before producer activation.

`batch.NewOwnedBounded` uses exact fresh backing with terminal unpooled cleanup.
Close clears the complete entry backing and arena headers and detaches callbacks
and readers. The original native build worker must detach this batch before
completing its original containing receipt; no sync.Pool keeps refunded native
backing alive. A materialization failure is sticky on the same original domain
build, keeps its descriptor, last successful private root identity and charge,
and prevents a second build attempt or destructor retry. Integration into the
complete engine build/resource ledger remains an activation gate.

### Original engine build ledger (construction)

The captured engine adopts `nativeEngineBuild` before constructing any domain
scratch. One original maintenance reservation includes the ledger, fixed result
and domain-pointer arrays, every domain scratch child and independent receipt,
and both sparse handoff constructors. Child transfers preserve the original
ledger floor; neither traversal nor accepted handoff asks for fresh admission.
Only an untouched initial capacity refusal can prove build retry eligibility.
Once construction begins, the first failure belongs to that same ledger.

The ledger owns exact domain descriptor edges and no engine, snapshot, cache,
or physical group ancestor. Results borrow their name and boundary from those
owned descriptors. An empty captured domain carries an explicit unchanged-root
proof; it cannot consume any later active or captured source. A failed handoff
returns no state edge and parks its exact owned private partial on the original
ledger. Engine cleanup finishes this ledger before releasing directories; a
failed ledger preserves those exact source edges and its original charge.

The engine's actual pending system mutations use a native catalog source with
the same prewrite active-plus-captured source bound as row domains. Schema,
incarnation-counter, and outcome encodings must be supplied by their original
producer plan; a catalog-only helper does not update schema directories by
itself. The source has no independently published physical root. Build streams
its captured rows into the complete group's exact private system root.

Qualified physical root descriptor keys are copied from captured schema names
under the original build floor. Their values reside in the admitted result
array. After domain materialization, Build reuses a domain's fixed entry scratch
to apply those descriptors to the same system root. Flushing at collection
boundaries preserves sorted deltas even when incarnation and schema-name
orders differ, without a catalog-sized sorting allocation. Only the group's
outer final batch accepts catalog rows and physical roots together.

Catalog rollover and handoff use the same original source/position ownership
and preaccepted two-successor promise. A no-op checkpoint retains the same
state, and handoff removes only the exact captured metadata prefix while
preserving newer active/captured metadata and the newer next-incarnation value.
Cancellation adds no source slot; old pre-rotation engine views keep the
original generation slot until their actual last edge.

Complete producer encodings, the exact stable resource closure, and native
public read/transaction routing still have to join before the complete semantic
interface can be activated. Constructor and private physical-root controls do
not qualify producer migration.

Construction controls exercise latest and captured successors after exact
producer exhaustion and maintenance admission Close, then prove independent
accepted-prefix source retirement with the newer suffix alive. A late private
failure retains the original pending edge until its exact partial Work cleans
up off-gate. Full native `PrepareBuild`/`PrepareHandoff` integration and the
public captured collection/transaction routes are still open.

`COWRoot.RetainOwned` shares a caller-owned immutable root during that accepted
maintenance construction. It adds no generation, source, pin, resource or byte
admission, including after budget Close. The original owner stays live through
the call, and a zero-reference root cannot be revived. Ordinary `Retain` keeps
its existing Close refusal. The last actual root/writer/source edge still owns
the original generation charge and finite source slot.

The reconstructed composite payload codec is construction-only. Its bounded
planner and whole validator preserve ordered raw/JSON/inline typed row members,
explicit collection incarnation, and borrowed payload ownership; they exclude
recursive composites and standalone DDL/import/graph/partition protocols. A
malformed final member exposes no valid prefix to the scanner. Encoding owns one
command image and completes validation before changing caller output capacity.
The V2 frame identity still rejects this command kind: canonical aggregate RID
fencing, the durable-only typed gate, full-frame admission, native final-effect
preparation and one outer replay/install must join before it can be emitted or
replayed. Payload codec tests alone do not qualify composite transactions.

### Catalog directory and exact opening inventory (construction)

The common private catalog replacement helper applies create, rename, drop and
schema replacement to both name and incarnation directories and to actual
system-tree mutations. Create consumes the monotone next incarnation; rename
preserves it; drop does not reclaim it. Before admission, the helper requires
the matching durable name identity, counter, metadata and removal effects.
Original producer plans remain responsible for canonical schema validation and
complete physical root descriptor effects. Partial builders acquire their real
shared edges before returning, including after a later admission refusal.

Opening resource selection uses the existing recoverable-root capture and all
five captured snapshot identity fields. A missing exact identity requires
reprepare; selection never falls back to current visible resources. An explicit
opening import may convert generic metadata once into the original registry's
selected persistent representation. Subsequent same-owner imports share its
persistent roots. This opening census is not a collection commit operation.

The original native inventory owns an independent enrollment in the same
budget before import, then releases the original census off-gate. Engines share
one inventory descriptor directly, without retaining an engine ancestor or the
census. Last cleanup releases selected resources before its enrollment and
returns the original independent containing receipt only after aliases detach.
Any original census or physical resource cleanup failure parks the same
inventory Work, selected set, enrollment and charge. Error observation does not
retry its destructor. The opening basis must already enroll the original
storage registry before the census allocates; the actual cache basis supplies
that premise.

This seam does not yet provide the native pending value-pointer resolver,
complete typed asset updates, catalog bootstrap decoder, or public producer
routing. The complete semantic interface remains fenced until those owners and
all producer paths join the same immutable cut.

### Exact selected resource construction

The native opening inventory implements the lower captured selector directly.
Its caller retains the original inventory edge and independent enrollment before
creating a publication context. Selection follows the exact immutable logical
resource index and looks up one exact logical obligation. It never samples the
current backend inventory or constructs a full resource catalog copy.

The opt-in selector admits its original operation ledger before borrowing the
source kind root. That ledger adopts every private token, entry, index, kind
root and output wrapper before later fallible work. Its single private rope
initially owns entry metadata with physical entries detached. Only after all
allocation succeeds does the exclusive cloned token transfer to that rope.
Cancellation therefore never tries to release a caller-owned token as a shared
token. Original cleanup failure returns the operation's own admitted receipt,
which preserves the exact remaining objects and both preparation and cleanup
errors. Error observation adds no destructor retry or replacement cleanup owner.

`NativePublicationResources.InitializeWithMetadata` selects this same original
metadata authority for its existing builder. The admitted native plan must
already retain the inventory and enrollment. The generic constructor and
existing context/finalization behavior are unchanged. This constructor does not
establish that typed asset producers supply selected metadata, or that every
shared builder partial-construction path is safe. Those producer migrations
and public native activation remain required gates.
