package pager

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/page"
	"sync"
	"unsafe"
)

const PrimaryReadRootLocalBaseV6 uint64 = 1 << 60
const primaryReadRootBucketsV6 = 256

// This is the single address index for both custody and reads. Fixed heads have
// no hidden Go map capacity. Removed nodes release actual backing; monotone IDs
// prevent a stale address from resolving to a newly allocated root.
type primaryReadRootEntryV6 struct {
	id    uint64
	image []byte
	owner any
	next  *primaryReadRootEntryV6
}
type primaryReadRootTableV6 struct {
	mu        sync.RWMutex
	heads     []*primaryReadRootEntryV6
	count     int
	retention *retainedalloc.Owner
}

func (t *primaryReadRootTableV6) find(id uint64) *primaryReadRootEntryV6 {
	if len(t.heads) == 0 {
		return nil
	}
	for e := t.heads[id%uint64(len(t.heads))]; e != nil; e = e.next {
		if e.id == id {
			return e
		}
	}
	return nil
}
func (p *Pager) SetPrimaryMetadataOwnerV6(owner *retainedalloc.Owner) {
	t := &p.immutableReadRoots
	t.retention = owner
	// Initial fixed backing is included in the owner's constructor envelope.
	t.heads = make([]*primaryReadRootEntryV6, primaryReadRootBucketsV6)
}

// PreparePrimaryReadRootOwnerV6 admits the sole table node before allocation.
// Custody exists before sealing; readers see no image until registration.
func (p *Pager) PreparePrimaryReadRootOwnerV6(local uint64, owner any) error {
	if p == nil || owner == nil || local < PrimaryReadRootLocalBaseV6 || local >= page.PrimaryBankNamespace {
		return errors.New("pager: invalid read-root owner")
	}
	t := &p.immutableReadRoots
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.find(local) != nil {
		return errors.New("pager: duplicate read-root owner")
	}
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryReadRootEntryV6{})))
	if t.retention != nil {
		if err := t.retention.Add(charge); err != nil {
			return err
		}
	}
	if t.count >= len(t.heads)*2 {
		newCharge := retainedalloc.AllocationCharge(uint64(len(t.heads)*2) * uint64(unsafe.Sizeof((*primaryReadRootEntryV6)(nil))))
		if t.retention != nil {
			if err := t.retention.Add(newCharge); err != nil {
				t.retention.Remove(charge)
				return err
			}
		}
		next := make([]*primaryReadRootEntryV6, len(t.heads)*2)
		for _, head := range t.heads {
			for e := head; e != nil; {
				oldNext := e.next
				i := e.id % uint64(len(next))
				e.next = next[i]
				next[i] = e
				e = oldNext
			}
		}
		oldCharge := retainedalloc.AllocationCharge(uint64(cap(t.heads)) * uint64(unsafe.Sizeof((*primaryReadRootEntryV6)(nil))))
		t.heads = next
		if t.retention != nil {
			t.retention.Remove(oldCharge)
		}
	}
	bucket := local % uint64(len(t.heads))
	t.count++
	t.heads[bucket] = &primaryReadRootEntryV6{id: local, owner: owner, next: t.heads[bucket]}
	return nil
}
func (p *Pager) PrimaryReadRootOwnerV6(local uint64) any {
	t := &p.immutableReadRoots
	t.mu.RLock()
	defer t.mu.RUnlock()
	if e := t.find(local); e != nil {
		return e.owner
	}
	return nil
}
func (p *Pager) primaryReadRootImageV6(local uint64) ([]byte, bool) {
	t := &p.immutableReadRoots
	t.mu.RLock()
	defer t.mu.RUnlock()
	if e := t.find(local); e != nil && e.image != nil {
		return e.image, true
	}
	return nil, false
}
func (p *Pager) RegisterPrimaryReadRootV6(local uint64, image []byte, w *iterator.OrdinalScanWork) (bool, error) {
	if ok, err := pagerReserve(w, 1, uint64(2*page.PageSize+128)); !ok || err != nil {
		return ok, err
	}
	if p == nil || local < PrimaryReadRootLocalBaseV6 || local >= page.PrimaryBankNamespace || len(image) != page.PageSize || page.DecodeHeader(image).PageID != page.PrimaryBankNamespace+local || !page.VerifyChecksumNonMutating(image) {
		return false, errors.New("pager: invalid immutable primary read root")
	}
	t := &p.immutableReadRoots
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.find(local)
	if e == nil || e.image != nil {
		return false, errors.New("pager: invalid immutable read registration")
	}
	e.image = image
	return true, nil
}
func (p *Pager) RemovePrimaryReadRootV6(local uint64, w *iterator.OrdinalScanWork) (bool, error) {
	if ok, err := pagerReserve(w, 1, 128); !ok || err != nil {
		return ok, err
	}
	if p == nil || local < PrimaryReadRootLocalBaseV6 || local >= page.PrimaryBankNamespace {
		return false, errors.New("pager: invalid read address retirement")
	}
	t := &p.immutableReadRoots
	t.mu.Lock()
	defer t.mu.Unlock()
	link := &t.heads[local%uint64(len(t.heads))]
	for *link != nil && (*link).id != local {
		link = &(*link).next
	}
	if *link == nil {
		return false, errors.New("pager: absent read address retirement")
	}
	e := *link
	*link = e.next
	t.count--
	e.image = nil
	e.owner = nil
	e.next = nil
	if t.retention != nil {
		t.retention.Remove(retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryReadRootEntryV6{}))))
	}
	return true, nil
}

// PrimaryMetadataBaselineV6 includes the companion pager wrapper and its exact
// fixed address heads. OS mappings and existing decode/cache policy are separate.
func (p *Pager) PrimaryMetadataBaselineV6() uint64 {
	alloc := retainedalloc.AllocationCharge
	ptr := uint64(unsafe.Sizeof(uintptr(0)))
	size := alloc(uint64(cap(p.chunks))*3*ptr) + alloc(uint64(cap(p.dirtyChunks.words))*8)
	if view := p.atomicChunks.Load(); view != nil {
		size += alloc(uint64(unsafe.Sizeof(chunkList{})))
		if len(p.chunks) == 0 || len(view.data) == 0 || &p.chunks[0] != &view.data[0] {
			size += alloc(uint64(cap(view.data)) * 3 * ptr)
		}
	}
	if v := p.verified.Load(); v != nil {
		size += alloc(uint64(unsafe.Sizeof(verifiedBitset{}))) + alloc(uint64(cap(v.chunks))*3*ptr) + uint64(len(v.chunks))*alloc(verifiedChunkWords*8)
	}
	if v := p.prefetched.Load(); v != nil {
		size += alloc(uint64(unsafe.Sizeof(prefetchBitset{}))) + alloc(uint64(cap(v.words))*8)
	}

	return size + retainedalloc.AllocationCharge(uint64(primaryReadRootBucketsV6)*uint64(unsafe.Sizeof((*primaryReadRootEntryV6)(nil)))) + retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(Pager{}))) +
		retainedalloc.AllocationCharge(uint64(len(p.path)))
}
func (p *Pager) ClearPrimaryReadRootsV6() {
	t := &p.immutableReadRoots
	t.mu.Lock()
	defer t.mu.Unlock()
	t.heads = nil
	t.count = 0
}

func (p *Pager) PrimaryMetadataFixedV6() uint64 {
	return retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(Pager{}))) + retainedalloc.AllocationCharge(uint64(len(p.path)))
}
