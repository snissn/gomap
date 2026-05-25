// Package typedkernel contains internal typed-column aggregate kernel dispatch.
//
// Dispatch is a prepare/block-planning boundary operation: callers provide both
// the #1843 semantic descriptor and the #1838 layout capabilities, and the
// registry only returns a concrete reducer after both gates report support.
// Reducers then run over a typedcolumn.RowSelection without consulting generic
// capability tables in per-row hot loops.
//
// The package has no public API surface and no process-wide mutable caches.
// DefaultRegistry returns an immutable static table; tests and future planners
// can build caller-owned registries with additional kernels.
package typedkernel
