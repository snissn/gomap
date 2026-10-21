# Experimental ordered MVCC memory

`MemtableMode: "cow_ordered"` is an opt-in data plane inside the existing COW
cache. It uses the same batch preparation, command acceptance, complete read
cut, generation budget, native resources and sorted durable builder as
`cow_btree`. It does not introduce another publication sequence or durability
authority. The default selector is unchanged.

Qualified Store operands are bound by the original MVCC admission capability.
One group can contain distinct timestamps for the same logical key. Its
application timestamps remain independent of native entry revisions, complete
publication sequence and WAL LSN. Raw reserved-namespace writes continue to
invalidate qualification through the original authority.

The primary tree has one entry per escaped logical-key prefix. An immutable
history postimage contains a singleton directly, or a balanced numeric timestamp
tree with a cached latest entry. Preparation forks only affected paths and owns
the complete postimage before command acceptance. Publication adopts prepared
roots. Old read cuts retain their selected postimages. Physical removal changes
membership in the successor postimage; logical tombstones remain retained
versions. Raw keys use the existing physical tree, and physical traversal merges
the two disjoint representations.

Each version retains its canonical physical key and full native value metadata.
These stable aliases are needed by borrowed native cursors and flush planning.
The initial producer still encodes the canonical command format and the native
batch owns its publication bytes. This implementation does not eliminate all
physical encoding or copying. Ordinary reads use admitted reusable cursor
stacks; the maintenance traversal can stop between charged steps. Actual read,
iterator, prepared-command and resource owners govern retirement.

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
