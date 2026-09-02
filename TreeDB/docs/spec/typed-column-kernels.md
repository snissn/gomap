# Typed-Column Kernel Dispatch Framework (#1839 K1)

Status: internal pre-alpha contract. Implementation lives in
`TreeDB/internal/typedkernel` and is not a public API.

Kernel dispatch is a prepare/block-planning boundary decision. Callers provide
both the semantic descriptor from `columnsemantics` (#1843) and the layout
capabilities from `columnlayout` (#1838). Dispatch fails closed unless both gates
report `supported`; fallback or unsupported statuses are returned before a
physical reducer is selected. Matching is therefore based on logical semantics
and layout capability, not physical encoding alone.

The registry is immutable/caller-owned. `DefaultRegistry` returns a static table
with int64 count-rows and non-null int64 aggregate reducers. Tests and future
planners can build their own registry with extra kernels; there is no mutable
process-wide cache. Scratch/reuse is likewise explicit through caller/session
owned values.

Reducers dispatch once outside the hot row loop. Concrete reducers switch on
`typedcolumn.RowSelection.Kind()` for empty, all, range, ranges, bitmap, and
sparse selections. The initial int64 reducer represents count rows, count
non-null, sum, avg, min, and max separately, with checked int64 sum accumulation.
Issue #1840 durable int64 stats may answer all/full-block-covered count/sum/avg before
payload decode; partial selections, visibility masks, or unsupported stats fall
back to these typedkernel reducers. Nullable value aggregates remain
fallback/unsupported until a future kernel composes null/default masks
explicitly. Direct/zero-copy access is obtained through `TreeDB/internal/typeddecode`
before invoking a concrete kernel.
