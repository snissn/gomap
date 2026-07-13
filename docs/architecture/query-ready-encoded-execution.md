# Query-ready encoded base-plus-delta execution

M4 adds one shared physical operator over the immutable `QRBG` base and the
snapshot-visible bounded `QRDG` deltas selected by the caller. The operator is
used for JSONBench q1-q5 and qexpr; query names and final answers are not
persisted in the generation format.

## Execution contract

`QueryReadyPreparedGeneration.PrepareOperator` constructs query-shaped state
after query-independent file open. It decodes only the structural part state
needed by the existing snapshot/tombstone reader, builds stable semantic
domains for requested low-cardinality columns, and translates each part's
local codes into those domains. Payload values stay in encoded column blocks
until `Run` scans them.

The shared operators cover group count, group count plus distinct, grouped
hour count, grouped minimum, grouped span, and qexpr's sum of squared
second-of-day. Equality and IN predicates, grouping, ordering, and top-K all
run on the same base-plus-delta visibility view. Updates, deletes, tombstones,
and part-local dictionary additions therefore have the same semantics for all
six production cells.

The collection adapter accepts caller-selected generation files and the
existing `ColumnPhysicalQueryRequest`. It derives the recovery-authoritative
schema identity from the collection, holds the existing generation lease, and
returns the existing physical-query result shape. It deliberately does not
select, publish, retain, reclaim, or rewrite assets; those responsibilities
remain with later graph milestones and the existing lifecycle owners.

The existing `RunColumnPhysicalQuery` and `PrepareColumnPhysicalQuery` entry
points do not auto-route to this adapter yet. Canonical JSONBench routing must
follow M5's generation publication/handoff so those entry points can obtain an
authoritatively selected file set; M6 owns the resulting end-to-end counter and
matrix evidence.

## Guardrails and observability

Nullable low-cardinality strings retain their null/default presence bits while
execution applies the existing JSONBench semantic empty bucket; explicit empty,
null, and missing rows therefore remain structurally distinguishable without
changing query results. Unsupported nullable numeric reductions fail closed.
Group-by-distinct scratch has an explicit 64M-cell ceiling. Prepared runners
reuse scan, reduction, and result buffers; result slices remain runner-owned
and are valid until the next run or close.

Diagnostics separate preparation, base scan, delta merge, predicates,
grouping, and ordering/top-K time. They also report rows, decoded blocks and
bytes, code translations, domain count, bounded scratch bytes, and explicit
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
