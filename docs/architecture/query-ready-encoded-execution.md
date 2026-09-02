# Query-ready encoded base-plus-delta execution

M4 adds one shared physical operator over the immutable `QRBG` base and the
snapshot-visible bounded `QRDG` deltas selected by the caller. The operator is
used for JSONBench q1-q5 and qexpr; query names and final answers are not
persisted in the generation format.

## Execution contract

Explicit generation open constructs the structural snapshot/tombstone state,
decodes local dictionaries once, and builds stable semantic domains and
part-local translations once. `QueryReadyPreparedGeneration.PrepareOperator`
then binds only query-shaped predicates and bounded reusable scratch to that
immutable state. `Run` scans the mmap-backed fixed-width `QRXS` code/int64
vectors and absence bitmaps directly; it does not decode source column blocks.

Visibility also applies the primary-ID mapping persisted with each QRBG part.
Ordinary base/delta parts preserve their encoded IDs; validated insert-only
source parts may use disjoint logical base offsets for their zero-based local
IDs. Cached and scan-based locator lookup, supersession, and tombstones all use
the same translated logical identity.

The shared operators cover group count, group count plus distinct, grouped
hour count, grouped minimum, grouped span, and qexpr's sum of squared
second-of-day. Equality and IN predicates, grouping, ordering, and top-K all
run on the same base-plus-delta visibility view. One-to-three 8-bit equality
predicates use a shared fused scan kernel, while grouped min/max/span use a
bounded top-K result shape instead of sorting every group. Updates, deletes,
tombstones, and part-local dictionary additions therefore have the same
semantics for all six production cells.

The collection adapter accepts selected generation files and the existing
`ColumnPhysicalQueryRequest`. It derives the recovery-authoritative schema
identity from the collection, holds the existing generation lease, and returns
the existing physical-query result shape. `PrepareQueryReadyColumnGeneration`
selects the exact insert-only typed-column inventory pinned by a DB snapshot
and uses M5's bounded build/handoff path to create a rebuildable QRBG. The
returned owner supplies the exact M3 byte range and reclaims the unpublished
asset after all runners close. It does not publish a root, assume GC ownership,
or change WAL, document, or recovery authority.

The generic `RunColumnPhysicalQuery` and `PrepareColumnPhysicalQuery` entry
points continue to preserve their existing behavior. The canonical JSONBench
`column-store-full-prepared` lane explicitly reopens the collection, prepares
one query-independent generation outside timed query attempts, and routes
q1-q5 and qexpr through the query-ready adapter. M6 owns the final integrated
counter and matrix evidence.

## Guardrails and observability

Nullable low-cardinality strings retain their null/default presence bits while
execution applies the existing JSONBench semantic empty bucket; explicit empty,
null, and missing rows therefore remain structurally distinguishable without
changing query results. Unsupported nullable numeric reductions fail closed.
Group-by-distinct scratch has an explicit 64M-cell ceiling. Prepared runners
reuse scan, reduction, and result buffers; result slices remain runner-owned
and are valid until the next run or close.

Diagnostics separate preparation, base scan, delta merge, predicates,
reduction, grouping, and ordering/top-K time. They also report rows, physical
bytes scanned, code translations, domain count, bounded scratch bytes, and explicit
zero-valued counters for document materialization, legacy scan fallback, and
precomputed answers.

Focused same-host evidence uses:

```sh
GOWORK=off go test ./TreeDB/internal/typedcolumn \
  -run '^$' \
  -bench '^BenchmarkQueryReadyEncodedJSONBenchOperators/(q2|q3|q5)$' \
  -benchmem -count=5

GOWORK=off go test ./TreeDB/internal/typedcolumn \
  -run '^$' -bench '^BenchmarkQueryReadyEncodedDeltaDepthCurve$' \
  -benchmem -count=5
```

Capture CPU and allocation profiles separately for q2, q3, and q5 so that a
regression is attributed to the current encoded execution rather than carried
forward from an older harness. The graph-level M6 closeout owns the canonical
1M cold/reopen comparison and final JSONBench wiring evidence.
