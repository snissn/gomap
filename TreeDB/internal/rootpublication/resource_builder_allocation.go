package rootpublication

import (
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"strings"
	"unsafe"
)

// NewStableResourceSetBuilderWithMetadata uses the existing builder owner and
// transfer machinery. Every input must have genuine same-owner metadata; it
// refuses generic/foreign children before token or set ownership changes.
func NewStableResourceSetBuilderWithMetadata(owner *retainedalloc.Owner, required ...ReachabilityField) (*StableResourceSetBuilder, error) {
	if owner == nil {
		return nil, ErrResourceOwnership
	}
	width := uint64(unsafe.Sizeof(ReachabilityField("")))
	if uint64(len(required)) > ^uint64(0)/width {
		return nil, retainedalloc.ErrCapacity
	}
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(StableResourceSetBuilder{})))
	array := retainedalloc.AllocationCharge(uint64(len(required)) * width)
	if array > ^uint64(0)-charge {
		return nil, retainedalloc.ErrCapacity
	}
	charge += array
	for _, field := range required {
		n := retainedalloc.AllocationCharge(uint64(len(field)))
		if n > ^uint64(0)-charge {
			return nil, retainedalloc.ErrCapacity
		}
		charge += n
	}
	allocation, err := newResourceAllocation(owner, charge)
	if err != nil {
		return nil, err
	}
	builder := &StableResourceSetBuilder{metadata: allocation, selected: true, state: ResourceOwnerBuilder, requiredFields: make([]ReachabilityField, len(required))}
	for i, field := range required {
		builder.requiredFields[i] = ReachabilityField(strings.Clone(string(field)))
	}
	return builder, nil
}

func (builder *StableResourceSetBuilder) disposeSelectedBuilderLocked() {
	if builder.metadata == nil {
		return
	}
	allocation := builder.metadata
	clear(builder.requiredFields)
	builder.requiredFields, builder.required, builder.indexed, builder.metadata = nil, nil, nil, nil
	if allocation.drop() {
		allocation.refund()
	}
}

// Prepare every immutable node before claiming the original token. Existing
// builder and token owner guards make the final transfer atomic. Real storage
// callbacks and disposal execute only after dropping the builder mutex.
func (builder *StableResourceSetBuilder) addAdmittedToken(token *StableResourceToken) error {
	builder.mu.Lock()
	if builder.closed || builder.abandoned || builder.metadata == nil || token.metadata == nil || token.metadata.owner != builder.metadata.owner || ResourceOwnerState(token.owner.Load()) != ResourceOwnerToken {
		builder.mu.Unlock()
		return ErrResourceOwnership
	}
	owner := builder.metadata.owner
	backing, err := newOwnedResourceEntry(owner, token)
	if err != nil {
		builder.mu.Unlock()
		return err
	}
	var view stableResourceKindView
	err = appendOwnedResourceEntryToView(&view, backing, owner)
	backing.release()
	if err != nil {
		builder.mu.Unlock()
		releaseStableResourceKindView(view)
		return err
	}
	incoming, err := newResourceKindBacking(owner, []resourceKindCell{{kind: token.kind, view: view}})
	if err != nil {
		builder.mu.Unlock()
		releaseStableResourceKindView(view)
		return err
	}
	next := incoming
	if builder.kindViews != nil {
		next, err = mergeOwnedResourceKindBackings(builder.kindViews, incoming)
		if err != nil {
			builder.mu.Unlock()
			incoming.release()
			return err
		}
	}
	if err = token.claim(ResourceOwnerShared); err != nil {
		builder.mu.Unlock()
		if next != incoming {
			next.release()
		}
		incoming.release()
		return err
	}
	old := builder.kindViews
	builder.kindViews = next
	builder.mu.Unlock()
	if next != incoming {
		incoming.release()
	}
	if old != nil {
		old.release()
	}
	return nil
}
