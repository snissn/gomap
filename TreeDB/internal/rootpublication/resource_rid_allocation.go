package rootpublication

import (
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"slices"
	"unsafe"
)

// AdmittedRIDFrontier owns constructor scratch until its synchronous consumer
// has copied exact membership into independently admitted token metadata.
// Frontier borrows the live object; Close invalidates that view.
type AdmittedRIDFrontier struct {
	allocation *resourceAllocation
	frontier   DurableFrontier
	membership exactRIDMembership
	values     []uint64
}

func AcquireRIDFrontierMetadata(owner *retainedalloc.Owner, rids []uint64) (*AdmittedRIDFrontier, error) {
	if owner == nil {
		return nil, ErrResourceOwnership
	}
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(AdmittedRIDFrontier{})))
	if err := diagnosticChargeAdd(&charge, uint64(len(rids)), 8); err != nil {
		return nil, err
	}
	allocation, err := newResourceAllocation(owner, charge)
	if err != nil {
		return nil, err
	}
	out := &AdmittedRIDFrontier{allocation: allocation, values: make([]uint64, len(rids))}
	copy(out.values, rids)
	slices.Sort(out.values)
	out.values = slices.Compact(out.values)
	out.frontier = exactRIDSummary(out.values)
	if len(out.values) != 0 {
		out.membership.values = out.values
		out.frontier.exactRIDs = &out.membership
	}
	return out, nil
}
func (r *AdmittedRIDFrontier) Frontier() DurableFrontier {
	if r == nil || r.allocation == nil {
		return DurableFrontier{}
	}
	return r.frontier
}
func (r *AdmittedRIDFrontier) Close() {
	if r == nil || r.allocation == nil {
		return
	}
	a := r.allocation
	r.allocation = nil
	clear(r.values[:cap(r.values)])
	r.values = nil
	r.frontier = DurableFrontier{}
	r.membership = exactRIDMembership{}
	a.drop()
	a.refund()
}
