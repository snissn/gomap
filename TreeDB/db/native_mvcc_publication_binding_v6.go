package db

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// BindNativeMVCCPublicationV6 links the SAME admitted selected-delete frame to
// the original coordinator. It adds no wrapper, independent resource edge or
// publication authority. The owner is the original containing PruneCursor;
// its physical close returns its original lease only after this link detaches.
// Binding is a private bounded step, not permission to enqueue or yield with
// an active publication turn.
func (c *NativeMVCCCurrentDeleteV6) BindNativeMVCCPublicationV6(owner rootpublication.NativePublicationOwnerV6, w *iterator.OrdinalScanWork) (bool, error) {
	if c == nil || w == nil || owner == nil {
		return false, ErrNativeMVCCSnapshotWork
	}
	if !w.Reserve(1, uint64(unsafe.Sizeof(*c))+2*uint64(unsafe.Sizeof(c.publicationCoordinator))+uint64(unsafe.Sizeof(c.publicationBound))) {
		return false, nil
	}
	if c.publicationBound {
		return true, nil
	}
	if c.phase != 4 || c.db == nil || c.db.rootPublication == nil || c.db.rootPublication.coordinator == nil {
		return false, rootpublication.ErrDurableRootOwnership
	}
	coordinator := c.db.rootPublication.coordinator
	ready, err := coordinator.BindNativePublicationV6(&c.publication, owner, w)
	if ready && err == nil {
		c.publicationCoordinator, c.publicationBound = coordinator, true
	}
	return ready, err
}

// Only actual successful physical cancellation/completion reaches this helper.
// A failed callback leaves the SAME linked frame and containing charge intact;
// retained=true is never inferred merely from a returned error. No builder,
// pending entry or active attempt may still borrow this containing storage.
func (c *NativeMVCCCurrentDeleteV6) completeNativePublicationBindingV6(w *iterator.OrdinalScanWork) (bool, error) {
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(c.publicationCoordinator))+2*uint64(unsafe.Sizeof(c.publicationBound))) {
		return false, nil
	}
	if !c.publicationBound {
		return true, nil
	}
	ready, err := c.publicationCoordinator.CompleteNativePublicationWithWorkV6(&c.publication, nil, false, w)
	if ready && err == nil {
		c.publicationCoordinator, c.publicationBound = nil, false
	}
	return ready, err
}
