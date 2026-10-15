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
	mu            sync.RWMutex
	heads         []*primaryReadRootEntryV6
	count         int
	clearPosition int
	retention     *retainedalloc.Owner
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

// PrimaryReadRootOwnerConstructorV6 is the concrete arena's original, already
// admitted construction operand. Under the caller's arena lock its constructor
// is nonfallible and cannot reenter the pager or allocate any other control.
// It creates only the independently charged bank after aggregate admission.
type PrimaryReadRootOwnerConstructorV6 interface {
	ConstructPrimaryReadRootOwnerV6() any
}

// PrepareConstructedPrimaryReadRootOwnerWithWorkV6 keeps aggregate allocation
// admission, construction and table installation in the original arena->table
// lock order. It never returns a lock/permit or an installed partial owner.
// Rehash is deliberately unsupported on this finite route before any effect.
func (p *Pager) PrepareConstructedPrimaryReadRootOwnerWithWorkV6(local uint64, metadata *retainedalloc.Owner, bankCharge uint64, constructor PrimaryReadRootOwnerConstructorV6, w *iterator.OrdinalScanWork) (bool, error) {
	if w == nil || !w.Reserve(2, 2*uint64(unsafe.Sizeof(primaryReadRootTableV6{}))+2*uint64(unsafe.Sizeof(primaryReadRootEntryV6{}))+2*uint64(unsafe.Sizeof(constructor))) {
		return false, nil
	}
	if p == nil || constructor == nil || metadata == nil || bankCharge == 0 || local < PrimaryReadRootLocalBaseV6 || local >= page.PrimaryBankNamespace {
		return false, retainedalloc.ErrWorkRequired
	}
	t := &p.immutableReadRoots
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.retention != metadata || len(t.heads) == 0 || t.count >= len(t.heads)*2 {
		return false, retainedalloc.ErrWorkRequired
	}
	link := &t.heads[local%uint64(len(t.heads))]
	for {
		if !w.Reserve(1, uint64(unsafe.Sizeof(*link))+2*uint64(unsafe.Sizeof(primaryReadRootEntryV6{}))) {
			return false, nil
		}
		e := *link
		if e == nil {
			break
		}
		if e.id == local {
			return false, retainedalloc.ErrWorkRequired
		}
		link = &e.next
	}
	nodeCharge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryReadRootEntryV6{})))
	if bankCharge > ^uint64(0)-nodeCharge {
		return false, retainedalloc.ErrCapacity
	}
	if ready, err := retainedalloc.AddWithOrdinalWork(metadata, bankCharge+nodeCharge, w); !ready || err != nil {
		return false, err
	}
	// Everything after aggregate admission is the concrete original
	// constructor and nonfallible table install, under unchanged exclusions.
	owner := constructor.ConstructPrimaryReadRootOwnerV6()
	bucket := local % uint64(len(t.heads))
	t.heads[bucket] = &primaryReadRootEntryV6{id: local, owner: owner, next: t.heads[bucket]}
	t.count++
	return true, nil
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

// PrimaryReadRootOwnerWithWorkV6 uses the SAME address table and returns the
// existing owner borrow. Each original bucket link is admitted before it is
// inspected; there is no count-first traversal or copied lookup directory.
func (p *Pager) PrimaryReadRootOwnerWithWorkV6(local uint64, w *iterator.OrdinalScanWork) (any, bool) {
	if w == nil || !w.Reserve(1, uint64(unsafe.Sizeof(primaryReadRootTableV6{}))) {
		return nil, false
	}
	t := &p.immutableReadRoots
	t.mu.RLock()
	defer t.mu.RUnlock()
	if len(t.heads) == 0 {
		return nil, true
	}
	link := &t.heads[local%uint64(len(t.heads))]
	for {
		if !w.Reserve(1, uint64(unsafe.Sizeof(*link))+uint64(unsafe.Sizeof(primaryReadRootEntryV6{}))) {
			return nil, false
		}
		e := *link
		if e == nil {
			return nil, true
		}
		if e.id == local {
			return e.owner, true
		}
		link = &e.next
	}
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
	var e *primaryReadRootEntryV6
	if w == nil {
		e = t.find(local)
	} else {
		if len(t.heads) == 0 {
			return false, retainedalloc.ErrWorkRequired
		}
		link := &t.heads[local%uint64(len(t.heads))]
		for {
			if !w.Reserve(1, uint64(unsafe.Sizeof(*link))+2*uint64(unsafe.Sizeof(primaryReadRootEntryV6{}))) {
				return false, nil
			}
			e = *link
			if e == nil || e.id == local {
				break
			}
			link = &e.next
		}
	}
	if e == nil || e.image != nil {
		return false, errors.New("pager: invalid immutable read registration")
	}
	e.image = image
	return true, nil
}
func (p *Pager) RemovePrimaryReadRootV6(local uint64, w *iterator.OrdinalScanWork) (bool, error) {
	owner, charge, ready, err := p.removePrimaryReadRootV6(local, w, false)
	if ready && owner != nil {
		owner.Remove(charge)
	}
	return ready, err
}

// RemovePrimaryReadRootAccountingV6 returns only the original node's accounting
// after unlink/scrub and callback return. Its original arena queue owns the
// external RemovalWork and must retain the result across admission refusal.
func (p *Pager) RemovePrimaryReadRootAccountingV6(local uint64, w *iterator.OrdinalScanWork) (*retainedalloc.Owner, uint64, bool, error) {
	if w == nil {
		return nil, 0, false, retainedalloc.ErrWorkRequired
	}
	return p.removePrimaryReadRootV6(local, w, true)
}
func (p *Pager) removePrimaryReadRootV6(local uint64, w *iterator.OrdinalScanWork, accounting bool) (*retainedalloc.Owner, uint64, bool, error) {
	if ok, err := pagerReserve(w, 1, 128); !ok || err != nil {
		return nil, 0, ok, err
	}
	if p == nil || local < PrimaryReadRootLocalBaseV6 || local >= page.PrimaryBankNamespace {
		return nil, 0, false, errors.New("pager: invalid read address retirement")
	}
	t := &p.immutableReadRoots
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.heads) == 0 {
		return nil, 0, false, errors.New("pager: absent read address retirement")
	}
	link := &t.heads[local%uint64(len(t.heads))]
	for {
		if w != nil && !w.Reserve(1, uint64(unsafe.Sizeof(*link))+2*uint64(unsafe.Sizeof(primaryReadRootEntryV6{}))) {
			return nil, 0, false, nil
		}
		e := *link
		if e == nil || e.id == local {
			break
		}
		link = &e.next
	}
	if *link == nil {
		return nil, 0, false, errors.New("pager: absent read address retirement")
	}
	if w != nil && !accounting {
		return nil, 0, false, retainedalloc.ErrWorkRequired
	}
	e := *link
	*link = e.next
	t.count--
	e.image = nil
	e.owner = nil
	e.next = nil
	return t.retention, retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryReadRootEntryV6{}))), true, nil
}

// PrimaryMetadataBaselineV6 includes the companion pager wrapper and its exact
// fixed address heads. OS mappings and existing decode/cache policy are separate.
func (p *Pager) PrimaryMetadataBaselineV6() uint64 {
	alloc := retainedalloc.AllocationCharge
	ptr := uint64(unsafe.Sizeof(uintptr(0)))
	size := alloc(uint64(cap(p.chunks))*3*ptr) + alloc(uint64(cap(p.mappings))*uint64(unsafe.Sizeof(mappingOwner{}))) + alloc(uint64(cap(p.dirtyChunks.words))*8)
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
	for {
		done, err := p.ClearPrimaryReadRootsWorkV6(nil)
		if err != nil {
			panic(err)
		}
		if done {
			return
		}
	}
}

func (p *Pager) PrimaryMetadataFixedV6() uint64 {
	return retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(Pager{}))) + retainedalloc.AllocationCharge(uint64(len(p.path)))
}
