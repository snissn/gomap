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
source counter. Native read views must retain their enclosing engine/source
owner through all root access. A directly acquired lower COW view separately
retains its generation/view charge; it is not a detached native engine view.
Rotation does not freeze a generation with private forks. Already prepared
forks may finish or cancel privately; the complete-cut basis check must reject
an old physical basis before canonical assignment. New active writes after
rotation use a fresh generation. No-op rotation retains the same descriptor
without consuming a source slot. Canceled private rotation refunds the original
slot after detaching its resource and containing descriptor aliases.

Captured-prefix handoff remains a construction gate: it must clone only newer
list-position metadata, share original physical-source receipts, drop the
accepted prefix, and preserve newer active effects. Required build/handoff
metadata must use the existing preadmitted maintenance capacity before physical
acceptance. Producer exhaustion must not prevent completion of an accepted
handoff. These ownership foundations do not activate collection producers or
replace the required public transaction, recovery and retention evidence.
