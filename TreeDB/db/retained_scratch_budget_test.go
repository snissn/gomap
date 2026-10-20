package db

import "github.com/snissn/gomap/TreeDB/internal/retainedalloc"

type retainedScratchBudget struct{ capacity uint64 }
type retainedScratchLease struct{ budget *retainedScratchBudget }

func (b *retainedScratchBudget) AcquireRetention(bytes uint64) (retainedalloc.Lease, error) {
	if bytes > b.capacity {
		return nil, retainedalloc.ErrCapacity
	}
	return &retainedScratchLease{b}, nil
}
func (l *retainedScratchLease) Resize(bytes uint64) error {
	if bytes > l.budget.capacity {
		return retainedalloc.ErrCapacity
	}
	return nil
}
func (l *retainedScratchLease) Close() {}
