# Experimental ordered MVCC memory

`MemtableMode: "cow_ordered"` is an opt-in data plane inside the existing COW
cache. It uses the same batch preparation, command acceptance, complete read
cut, generation budget, native resources and sorted durable builder as
`cow_btree`. It does not introduce another publication sequence or durability
authority. The default selector is unchanged.

Immutable COW modes require the exact selector `cow_ordered` or `cow_btree`.
`adaptive:cow_ordered` and `adaptive:cow_btree` are rejected with
`caching.ErrCOWUnsupported` before backend opening or storage creation; they
cannot participate in adaptive memtable rotation. Ordinary mutable
`adaptive:<mode>` selectors retain their existing behavior.

Qualified Store operands are bound by the original MVCC admission capability.
One group can contain distinct timestamps for the same logical key. Its
application timestamps remain independent of native entry revisions, complete
publication sequence and WAL LSN. Raw reserved-namespace writes continue to
invalidate qualification through the original authority.

The primary tree has one entry per escaped logical-key prefix. An immutable
history postimage contains a singleton directly, or a balanced numeric timestamp
tree with a cached latest entry. Preparation forks only affected paths and owns
the complete postimage before command acceptance. Publication adopts prepared
roots. Old read cuts retain their selected postimages. Internal membership erase
changes the successor timestamp tree. Public physical deletion, including Store
floor pruning, instead retains a physical tombstone mask until durable handoff
so an older version in the backend cannot reappear. Logical MVCC tombstones
remain retained versions. Raw keys use the existing physical tree, and physical
traversal merges the two disjoint representations. Cross-source reads may walk
physical erase masks; the numeric tree's logarithmic predecessor bound does not
describe that complete public path.

Qualified Store floor pruning uses an additional boundary for `cow_ordered`:
the existing public checkpoint seals and drains the source prefix before the
prune cut is captured. After any delete-write attempt, a second checkpoint runs
after prune iterator and snapshot cleanup, even when a write returned an error.
It publishes the exact successor basis for accepted deletes, retaining old cuts
and any newer above-floor work. The original write error remains observable;
completion errors are joined with it. A successful nonempty prune therefore
leaves no erase masks for its completed prefix in active overlays. Both drains
and their persistence waits are part of this pruning operation's cost. Delete
batches retain their requested acknowledgement mode; the added boundaries may
persist more work. Other engines keep their previous pruning path. This is a
floor-boundary capability, not a logarithmic arbitrary physical-delete claim.

Each version retains its canonical physical key and full native value metadata.
These stable aliases are needed by borrowed native cursors and flush planning.
The initial producer still encodes the canonical command format and the native
batch owns its publication bytes. This implementation does not eliminate all
physical encoding or copying. Ordinary reads use admitted reusable cursor
stacks; the maintenance traversal can stop between charged steps. Actual read,
iterator, prepared-command and resource owners govern retirement.

Logical operands encode directly into that batch's admitted arena. Inline-only
groups avoid pointer-producer capture and pointer/dictionary preparation vectors.
The original preparation bound is reserved before node allocation. Private raw,
primary and numeric-history maps record their actual node/backing allocations,
including discarded arrays; preparation refunds only the unused node portion of
that reservation. Payload, wrapper, temporary and resource reservations retain
their original rules. This receipt grants no allocation authority: an overrun is
a diagnostic refusal before WAL acceptance, not permission to allocate beyond a
proven bound. Actual allocations remain charged until their generation retires.

Durable-basis iterator reads reuse existing verified leaf-cache entries under the
retained basis. Dictionary leaves keep their original admitted decoder fallback.
Cache misses retain the original read/decode path; this change does not populate
a second cache or promise a cold-read improvement.

Synchronous public point reads retain the complete cut through copy-out. Inline
reads append directly from cached bytes or protected basis scratch into caller
storage; they do not create a public Snapshot handle. Pointer and dictionary
materialization retain the original admitted workspace and dependency policy on
that same cut. Inline points allocate no COW view and may therefore proceed at
`MaxViews` without increasing the view count; their actual scratch and byte
admission limits still apply. Pointer and dictionary fallback construct an
admitted Snapshot and retain its finite view limit. Exported snapshots remain
distinct lifetime-bearing handles.

Sealed roots project physical records directly into the maintained durable
builder. Prefix handoff publishes its successor backend basis together with
exact covered sources, retaining newer work and old cuts. Generation retirement
does not authorize value-log segment deletion. Existing reachability and storage
owners still govern GC.

Activation is visible through `treedb.cache.cow.ordered.logical_keys` and
`treedb.cache.cow.ordered.physical_versions`. These sum retained mutable/frozen
root counts, rather than unique database keys. They can become zero after a
checkpoint. `SupportsLogicalMVCC` reports the actual optional producer route.

The public comparison harness is `benchmarks/orderedmem_native`. Production
`adaptive`, production `cow_btree`, this candidate, and full managed Badger must
be measured with distinct volatile, relaxed-WAL and durable-WAL acknowledgments.
Foreground, persistence, release, close and reopen costs have separate phase
boundaries. A component result or a passing correctness test is not a default
selection decision.

Integration is tracked in [#5193](https://github.com/snissn/gomap/issues/5193)
under [#5192](https://github.com/snissn/gomap/issues/5192). The selected prototype
is `2119eed86e46fecaac29bc96b33c7d26f3a3c5a5` over
`b03c73fa010987b8ce4c904ff6dca2d2497be393`. Its characterized public API and
canonical storage bytes are retained. Source-bound focused tests and earlier
measurements remain evidence for their original source and declared scope;
integration needs current-head checks and cannot inherit a full-repository pass.
Raw 500k unified workloads do not exercise the typed history index and show
material forced-COW regressions. Keep the adaptive default, raw-KV and managed
MVCC comparisons separate, and retain adverse results during optimization.

### Bounded scalar version-successor batches

`mvcc.Store.GetManyAt(keys, timestamp)` accepts at most
`mvcc.MaxGetManyAtKeys` (32) logical keys and returns ordered `GetManyResult`
entries. Each successful `Result.Value` is independently owned after the call.
Duplicate, binary and empty logical keys retain `GetAt` semantics. Inputs must
remain unchanged until the synchronous call returns. Timestamp zero and a nil
Store are rejected before empty/oversize handling. Invalid keys and malformed
records are per-key errors. Qualified batches admit the discard floor once and
capture one physical cut; floor/capture failures are call errors. A final Close
error is also a call error and preserves already-produced results. Callers must
check both the call error and each consumed item error and must never retry an
error as a miss. The singleton delegates to `GetAt`; its storage/floor failure is
a call error. Unqualified producers retain scalar `GetAt` calls and their
per-key failure/visibility behavior rather than promising a coherent batch cut.

`MVCCReadBatchSnapshot` is an optional capability alongside the existing
qualified `MVCCReadSnapshot`; absent capability uses scalar successors on that
same captured cut. It receives complete encoded version-range bounds, never
physical exact `GetMany` lookups. Probes are sorted by those full physical lower
bounds, then restored to input order. Each probe still applies overlay revision
and priority, physical tombstone suppression, MVCC envelope/timestamp validation
and owned value copy-out. The escaped bound/result arrays are sized to the
actual input/valid-probe count rather than allocating a32-entry backing for
small batches; the operation and public cap remain32.

The optimized batch borrows one existing admitted point workspace for at most32
metadata lookups. It retains only the current validated ordinary base leaf and
reuses it when the full physical lower bound is inside that leaf's decoded
[first,last] key extent. Outside, empty, PRIMARY and uncertain extents use the
original root traversal on the same cut; no walk across sparse gaps is added.
Key reconstruction views are ephemeral. The descriptor is invalidated before
scratch reuse and cleared before releasing its workspace. Operation-local
candidate/inspection staging is finite and charged to the existing COW budget;
it introduces no cache, pool, mmap, decoder or physical owner. The traversal
slot is released before complete-cut value resolution or re-acquiring fallback,
with the original fresh-workspace refund policy retained for value capacity.
Only established dictionary/admission eligibility permits fallback; actual
I/O/integrity errors are per-input errors and are never retried through a clean
read. Contention, missing capability and staging admission refusal keep the
maintained scalar path. No change is made to commit ACK, read timestamp,
publication, pruning or all-version posting reconstruction.
