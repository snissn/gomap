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
The original worker cancels unsealed builders before queued opening-cut cleanup
can release their borrowed basis. Sparse source suffix construction clones only newer list
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
Directory builders use the existing maintained map admission formula, with a
maximum representable height to cover later catalog growth. This conservative
height bound remains a sizing concern for large touched sets; required default
capacity/progress controls and a tighter source-bound item ceiling remain open
before activation. No configured capacity is increased to conceal this cost.

`COWExternalLease.SplitNativeBytes` transfers already admitted bytes from the
same original held build reservation into independent child receipts. It uses
the same aggregate rounding as native admission, preserves an explicit original
build-ledger floor and the parent receipt, changes no total/reserved/external
byte counter, and checks the finite receipt-count bound. A child has no parent,
Work, state or resource pointer. Existing promised capacity remains usable after
fresh maintenance admission closes. An insufficient accepted promise is a
terminal original-ledger error, not a reason to acquire replacement capacity.

Construction controls exercise latest and captured successors after exact
producer exhaustion and maintenance admission Close, then prove independent
accepted-prefix source retirement with the newer suffix alive. A late private
failure retains the original pending edge until its exact partial Work cleans
up off-gate. Full native `PrepareBuild`/`PrepareHandoff` integration and the
public captured collection/transaction routes are still open.
