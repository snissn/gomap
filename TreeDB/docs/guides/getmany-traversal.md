# Natural GetMany batches and shared traversal

Supply naturally available keys to `DB.GetMany` or `DB.GetManyView`. For a tree
batch of at least 64 keys whose leaves use the leaf log, both routes sort local
key/index probes and traverse shared internal branches once per probe interval.
Caller keys keep their order and contents. Leaf groups still use the existing
checked reader and materializer. Small batches and pager leaves retain the
whole-call point-read fallback, selected before any grouped callback runs.

The planner uses existing child-search semantics: the greatest separator at or
below the key selects its child, with the first child selected below the first
separator. Root fences prune below the inclusive lower bound and at or above
the exclusive upper bound. The checked loader continues to enforce selected
root extents, checksums and page types, and the 50-level corruption guard still
applies. Internal node views remain on the traversal stack. Multi-key intervals
find the exact floor separator once per occupied child, then partition sorted
probes at the next separator. Singleton intervals retain the checked child
search. Sparse intervals skip untouched children. Base-delta separator decoding
borrows one existing page-sized scratch buffer lazily for the planning call;
parent separator comparisons finish before a child can overwrite that buffer.
No separator views survive in pooled scratch. The buffer returns before leaf
materialization, separately from any active leaf buffer or lease.

`GetMany` returns independent, capacity-capped owned values, including duplicate
inputs. Misses are nil; present empty values are non-nil. `GetManyView` reports
each original input index, including duplicates and misses; callback order is
unspecified. Values are read-only and valid only during the callback. Copy them
before retaining them. Leaf leases remain held through the callback and are
released on success and error. Snapshot owners retain root/resource pins and
active-read lifetime; the planner does not acquire or release those pins.

Sharing occurs within an existing tree batch or worker chunk. Memtable/cache
precedence, cached snapshot per-key reads and conditional-transaction revision
reads keep their existing paths. No format, pointer persistence, durability,
GC/rewrite, filter default or public API contract changes are involved.

Local probe/group slices, presence flags and the child-ref map use the existing
pool and 8192-key retention bound. Sorting uses `slices.SortFunc`, without
boxing, key copies or an extra sort buffer. Recursive intervals pass scalar
bounds and child refs; no per-interval work list is allocated. Borrowed keys,
child refs and presence flags are cleared before scratch returns to the pool.

## Focused verification

Use Go 1.26 with `GOWORK=off` and the desired explicit toolchain. Semantic,
fallback, integrity, ownership, lease and zero-allocation warm planner checks:

```sh
go test ./TreeDB/tree -run '^TestTreeGetManySharedTraversal|^TestNegativeFilter' -count=1
```

The existing disposable overlay instruments successful internal loads in the
production checked loader, so it follows the shared planner without a change:

```sh
python3 scripts/treedb_algorithm_work_overlay.py "$PWD" /tmp/getmany-loader-overlay
go test -overlay=/tmp/getmany-loader-overlay/overlay.json \
  -tags=algorithm_work_overlay ./TreeDB/tree \
  -run '^TestTreeGetManySharedTraversalInternalLoads$' -count=1 -v
```

The deterministic three-level fixture expects seven actual loads in each mode;
its per-key predecessor performs 212 loads on the same shuffled 72 inputs.
This is a focused mechanism regression, not public throughput acceptance.

A separate disposable overlay counts calls to the existing node search and
entry-view methods. Its same fixture performs 210 child searches before the
separator partition refinement; the refined planner performs 28 entry reads
and no child searches, in each mode:

```sh
python3 TreeDB/tree/testdata/getmany_routing_overlay.py "$PWD" /tmp/getmany-routing-overlay
go test -overlay=/tmp/getmany-routing-overlay/overlay.json \
  -tags=getmany_routing_overlay ./TreeDB/tree \
  -run '^TestTreeGetManySharedTraversalRoutingWork$' -count=1 -v
```

Run the semantic suite normally and under the race detector. It includes
malformed directory offsets, empty internal pages, and long shared prefixes
across parent/child decodes. The normal warm planner check expects zero
allocations.

Use the unchanged `BenchmarkAlgorithmGetMany` with the full equivalent consumer
for public allocation and latency comparison. Timed runs must use normal
binaries; the overlay adds a mutex/map counter and is only for untimed load
counts. A small `TREEDB_ALGORITHM_PILOT=1` allocation check does not qualify the
250k-key host-local latency target. Retain uniform and pointer cells as guards,
and measure both owned and view locality cohorts before adopting a candidate.
