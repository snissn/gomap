// Package typedkernel contains internal typed-column aggregate and predicate
// kernel substrate.
//
// Dispatch is a prepare/block-planning boundary operation for aggregate
// reducers: callers provide both the #1843 semantic descriptor and the #1838
// layout capabilities, and the registry only returns a concrete reducer after
// both gates report support. Reducers then run over a typedcolumn.RowSelection
// without consulting generic capability tables in per-row hot loops.
//
// Dictionary-code equality and in-list helpers are provided as concrete
// predicate kernels over low-cardinality uint32 codes. They intentionally do not
// resolve dictionary strings per row and do not expose lexical range/prefix
// semantics; dictionary code order is not treated as lexical string order.
// Dictionary group/count/distinct reducer substrate currently lives in
// typedcolumn.AggregateArena selection-aware helpers; the typedkernel aggregate
// registry does not yet register a dictionary group-by reducer.
//
// The package has no public API surface and no process-wide mutable caches.
// DefaultRegistry returns an immutable static table; tests and future planners
// can build caller-owned registries with additional kernels.
package typedkernel
