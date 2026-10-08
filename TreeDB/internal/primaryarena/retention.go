package primaryarena

import (
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"unsafe"
)

// MetadataOwner is the same physical arena owner's producer-maintained added
// metadata capacity. Enrollment never traverses roots or mutable allocator data.
func (a *Arena) MetadataOwner() *retainedalloc.Owner { return &a.metadata }
func (a *Arena) newChunk() (*bankChunk, error) {
	if err := a.metadata.Add(retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(bankChunk{})))); err != nil {
		return nil, err
	}
	return new(bankChunk), nil
}
func (a *Arena) discardChunk(c **bankChunk) {
	if *c != nil {
		*c = nil
		a.metadata.Remove(retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(bankChunk{}))))
	}
}
func (a *Arena) newGroup(g Group) (*Group, error) {
	if err := a.metadata.Add(retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(Group{})))); err != nil {
		return nil, err
	}
	return &g, nil
}
func (a *Arena) edgeCharge(n int) uint64 {
	return retainedalloc.AllocationCharge(uint64(n) * uint64(unsafe.Sizeof(Ref{})))
}

// AdmitRootMetadata reserves immutable descriptor capacity before allocation.
// Its conservative per-root envelope includes shared immutable descriptors
// borrowed by this root; the actual last bank/reader/slot edge refunds it.
func (a *Arena) AdmitRootMetadata(r Ref, bytes uint64) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	b := a.valid(r)
	if b == nil {
		return ErrStale
	}
	if err := a.metadata.Add(bytes); err != nil {
		return err
	}
	b.metadataCharge += bytes
	return nil
}

// CancelRootMetadata unwinds a refused descriptor construction before visibility.
func (a *Arena) CancelRootMetadata(r Ref, bytes uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	b := a.valid(r)
	if b == nil || b.metadataCharge < bytes {
		panic("primary metadata cancellation lost root")
	}
	b.metadataCharge -= bytes
	a.metadata.Remove(bytes)
}

// AdoptRootMetadata transfers capacity already admitted by the constructing
// transaction to the actual promotion root; it allocates no second owner.
func (a *Arena) AdoptRootMetadata(r Ref, bytes uint64) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	b := a.valid(r)
	if b == nil {
		return ErrStale
	}
	b.metadataCharge += bytes
	return nil
}

// AdoptPendingRootMetadata transfers already admitted immutable storage from
// the existing transaction to this independently owned slot root.
func (a *Arena) AdoptPendingRootMetadata(r Ref, bytes uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	b := a.valid(r)
	if b == nil {
		panic("pending metadata lost slot root")
	}
	a.metadata.TransferPending(bytes)
	b.metadataCharge += bytes
}
