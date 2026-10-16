package primaryarena

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/pager"
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
	return a.AdmitRootMetadataWithWorkV6(r, bytes, nil)
}
func (a *Arena) AdmitRootMetadataWithWorkV6(r Ref, bytes uint64, w *iterator.OrdinalScanWork) error {
	if !reserve(w, 1, 2*uint64(unsafe.Sizeof(bank{}))) {
		return retainedalloc.ErrWorkBudget
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	b, ready := a.validWithWorkV6(r, w)
	if !ready {
		return retainedalloc.ErrWorkBudget
	}
	if b == nil {
		return ErrStale
	}
	if ready, err := retainedalloc.AddWithOrdinalWork(&a.metadata, bytes, w); !ready || err != nil {
		if err == nil {
			err = retainedalloc.ErrWorkBudget
		}
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

// NativeBankGeometryForTestV6 separates the existing bank metadata envelope
// from physical allocation provenance. It grants no root edge or table permit.
type NativeBankGeometryForTestV6 struct {
	Valid, Sealed, ImageBorrowed, TableBackingOpaque, TableApplicable bool
	References, MetadataCharge, EdgeCapacity, ImageBytes, Class       uint64
	BankRaw, CertificateRaw                                           uint64
	PagerTable                                                        pager.NativeReadRootTableGeometryForTestV6
}

func (a *Arena) MaintenanceNativeBankGeometryForTestV6(r Ref, w *iterator.OrdinalScanWork) (g NativeBankGeometryForTestV6, ready bool) {
	if a == nil || w == nil || !w.Reserve(2, 2*uint64(unsafe.Sizeof(bank{}))+2*uint64(unsafe.Sizeof(g))) {
		return g, false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	b, ok := a.validWithWorkV6(r, w)
	if !ok {
		return g, false
	}
	if b == nil {
		return g, true
	}
	g.Valid, g.Sealed, g.References, g.MetadataCharge, g.EdgeCapacity, g.ImageBytes, g.Class = true, b.sealed, b.refs, b.metadataCharge, uint64(cap(b.edges)), uint64(len(b.image)), uint64(b.class)
	// The pager node and certificate borrow this SAME bank image. It may be
	// RAM-backed by the original constructor; it is not a second image
	// allocation merely because those consumers hold aliases.
	g.ImageBorrowed = true
	g.BankRaw = uint64(unsafe.Sizeof(*b))
	if b.readCertificate != nil {
		g.CertificateRaw = uint64(unsafe.Sizeof(*b.readCertificate))
	}
	g.PagerTable, ready = a.pager.MaintenanceNativeReadRootTableGeometryForTestV6(Local(r.PageID), b, b.image, &a.metadata, w)
	g.TableApplicable = Local(r.PageID) >= pager.PrimaryReadRootLocalBaseV6
	g.TableBackingOpaque = !ready || g.PagerTable.Overflow || !g.PagerTable.TableOwnerMatches || g.TableApplicable && (!g.PagerTable.Present || !g.PagerTable.SameOwner || !g.PagerTable.SameImage)
	return g, ready
}
