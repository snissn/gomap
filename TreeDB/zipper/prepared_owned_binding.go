package zipper

import "github.com/snissn/gomap/TreeDB/pager"

// DetachOperationalBindings removes all operational engine edges from this
// private sealed owner between concrete serialized calls. Owned arrays and the
// workspace reader remain live. No callback or saved operational binding is
// returned or retained. A generic zipper cannot use this seam.
func (w *PreparedOwnedWorkspace) DetachOperationalBindings(z *Zipper) error {
	if w == nil || w.closed || !w.sealed || z == nil || z.preparedOwned != w ||
		w.pager != z.pager || !preparedOwnedSameWriter(w.writer, z.leafPageLog) {
		return ErrPreparedOwnedWorkspace
	}
	w.pager, w.writer = nil, nil
	z.pager, z.allocator, z.leafPageLog = nil, nil, nil
	z.parallelMergePressure = nil
	// This private owner never inherits generic reader/cache authority. The
	// only reader retained through the boundary is its exact sealed workspace.
	z.leafPageReader = w
	return nil
}

// BindOperationalContext is call-time binding only. The concrete DB consumer
// verifies the scalar captured generation and real snapshot registration first.
// It must detach on every return, including failed Prepare/Apply. This method
// grants no generic reader, current-index lookup or new allocation authority.
func (w *PreparedOwnedWorkspace) BindOperationalContext(z *Zipper, p *pager.Pager, a PageAllocator, writer LeafPageLog) error {
	if w == nil || w.closed || !w.sealed || !w.bound || z == nil || z.preparedOwned != w ||
		p == nil || a == nil || w.config != preparedOwnedConfig(z) ||
		w.pager != nil || w.writer != nil || z.pager != nil || z.allocator != nil || z.leafPageLog != nil {
		return ErrPreparedOwnedWorkspace
	}
	if !preparedOwnedSameWriter(writer, writer) {
		return ErrPreparedOwnedWorkspace
	}
	w.pager, w.writer = p, writer
	z.pager, z.allocator, z.leafPageLog = p, a, writer
	z.leafPageReader = w
	return nil
}
