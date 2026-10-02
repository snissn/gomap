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
applies. Internal node views and decode scratch remain on the traversal stack;
separator views never enter pooled scratch.

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

## Callback pointer materialization

Grouped pointer callbacks prefer the existing key-aware append reader, then the
key-aware view reader, then the generic append reader, then the generic view
reader. This matches iterator value routing and preserves key-aware lookup when
only a generic appender is available. The append route can reuse the value log's
existing compressed grouped-frame cache; CRC checks, cache identity, admission
limits and manager-wide retention budget remain in force. Cache hits copy the
selected bytes into the callback destination, so callbacks borrow neither cache
slots nor mapped value storage through this route.

One page-sized destination is acquired lazily per grouped tree batch when an
appender first reads a pointer. Inline-only batches and reader-only routes do
not acquire it. It is separate from the active leaf-page scratch and is reused
only after each callback returns. Returned backing is never saved in scratch;
values larger than a page use temporary output, leaving the pooled destination
at one page. Repeated oversized values can therefore allocate repeatedly.
Callbacks must still copy values they retain. Owned `GetMany` results and
`GetAppend` path counters keep their existing behavior.

A bounded local Go 1.26, 8192-key normal-binary allocation pilot with the existing
full-byte consumer reduced pointer callback batches from 124918–139197 B/op and
175 allocations/op to 776 B/op and 7 allocations/op across sorted, clustered
and uniform probes. Inline callbacks remained at 776 B/op and 7 allocations/op.
These are warm fixture measurements, not a zero-allocation API claim or
qualification of full-size latency, cold cache, peak memory or larger-than-memory
behavior. Full 250k-key paired runs and all owned/inline guard cells remain the
acceptance gate.

## Focused verification

Use Go 1.26 with `GOWORK=off` and the desired explicit toolchain. Semantic,
fallback, integrity, ownership, lease and zero-allocation warm planner checks:

```sh
go test ./TreeDB/tree -run '^TestTreeGetManySharedTraversal|^TestNegativeFilter' -count=1
go test ./TreeDB/tree -run '^TestTreeGetManyPointerView' -count=1
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

Use the unchanged `BenchmarkAlgorithmGetMany` with the full equivalent consumer
for public allocation and latency comparison. Timed runs must use normal
binaries; the overlay adds a mutex/map counter and is only for untimed load
counts. A small `TREEDB_ALGORITHM_PILOT=1` allocation check does not qualify the
250k-key host-local latency target. Retain uniform and pointer cells as guards,
and measure both owned and view locality cohorts before adopting a candidate.
