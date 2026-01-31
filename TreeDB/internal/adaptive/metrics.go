package adaptive

// Metrics represents the telemetry gathered during a single commit.
type Metrics struct {
	LeafFill        float64 // 0..1
	Splits          int
	IndexWriteBytes int
	SlabWriteBytes  int
	SlabDeadBytes   int

	// LeafLocalMergeSizingMismatch counts cases where the zipper's sizing logic
	// predicted a local leaf merge should fit, but the builder returned ErrNodeFull.
	// This should stay near-zero; it's a guardrail against undercounting entry bytes.
	LeafLocalMergeSizingMismatch uint64
	// LeafLocalRebalanceSizingMismatch counts cases where the zipper's sizing logic
	// found a feasible local rebalance split, but leaf rebuild hit ErrNodeFull.
	LeafLocalRebalanceSizingMismatch uint64

	// SlabWriteBytesByFile tracks bytes appended to each slab file during this
	// commit (keyed by FileID).
	SlabWriteBytesByFile map[uint32]int64
	// SlabDeadBytesByFile tracks bytes that became dead in each slab file during
	// this commit due to overwrite/delete of pointer values.
	SlabDeadBytesByFile map[uint32]int64
}
