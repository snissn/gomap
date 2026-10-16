package primaryarena

import (
	"crypto/sha256"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"unsafe"
)

// FlattenClaim owns a private flat array and an independent immutable component
// baseline. It never retains a publication/root or uses one for lookup. Exactly
// one flat baseline is allowed; actual current directory and sparse overrides
// are freshly validated before its private array becomes current custody.
type FlattenClaim struct {
	arena            *Arena
	queue            *ReleaseQueue
	baseline, flat   *Group
	next             uint8
	ready, cancelled bool
}

func (a *Arena) PrepareFlattenCurrentGroup(current Ref, classSlot uint16, w *iterator.OrdinalScanWork) (*FlattenClaim, bool, error) {
	return a.PrepareFlattenCurrentGroupWithReleaseQueue(current, classSlot, &a.ordinaryRelease, w)
}

// PrepareFlattenCurrentGroupWithReleaseQueue keeps all private cancellation and
// replaced custody edges on the exact maintenance owner's finite release queue.
func (a *Arena) PrepareFlattenCurrentGroupWithReleaseQueue(current Ref, classSlot uint16, queue *ReleaseQueue, w *iterator.OrdinalScanWork) (*FlattenClaim, bool, error) {
	cost := uint64(6*page.PageSize) + 2*uint64(unsafe.Sizeof(bank{})) + 4*uint64(unsafe.Sizeof(Group{})) + 2*uint64(unsafe.Sizeof(FlattenClaim{})) + 2*uint64(unsafe.Sizeof(ReleaseQueue{}))
	if !reserve(w, 5, cost) {
		return nil, false, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	b := a.valid(current)
	if b == nil || !b.sealed || b.class != Directory || classSlot >= groupSize || queue == nil || queue.owner != a {
		return nil, false, ErrStale
	}
	if sha256.Sum256(b.image) != b.digest || !page.VerifyChecksumNonMutating(b.image) {
		return nil, false, ErrFormat
	}
	g := b.groups[0]
	if g == nil || g.owner != a || g.refs == 0 {
		return nil, false, ErrStale
	}
	if g.base == nil {
		return &FlattenClaim{arena: a, queue: queue, ready: true}, true, nil
	}
	baseline := g.base
	if baseline.owner != a || baseline.refs == 0 || baseline.base != nil || baseline.refs == ^uint64(0) {
		return nil, false, ErrStale
	}
	flat, err := a.newGroup(Group{owner: a, refs: 1})
	if err != nil {
		return nil, false, err
	}
	baseline.refs++
	a.counters.GroupsCreated++
	a.counters.EdgeReads++
	a.counters.EdgeWrites++
	a.counters.Bytes += cost
	return &FlattenClaim{arena: a, queue: queue, baseline: baseline, flat: flat}, true, nil
}

// Step acquires at most eight actual baseline component refs in one return. An
// above-floor overwrite of direct overrides is matched fresh at installation.
// A different baseline rejects this private candidate; it cannot authorize old
// neighbor bytes. Without intervening writes every call advances a finite49
// cursor, then installs a genuinely flat current group with charged edges.
func (c *FlattenClaim) Step(current Ref, w *iterator.OrdinalScanWork) (bool, bool, error) {
	if c == nil || c.cancelled {
		return false, false, ErrStale
	}
	if c.ready {
		return true, false, nil
	}
	a := c.arena
	cost := uint64(6*page.PageSize) + 2*uint64(unsafe.Sizeof(bank{})) + 4*uint64(unsafe.Sizeof(Group{})) + 2*uint64(unsafe.Sizeof(ReleaseQueue{}))
	if !reserve(w, 5, cost) {
		return false, false, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	b := a.valid(current)
	if b == nil || !b.sealed || b.class != Directory {
		return false, false, ErrStale
	}
	if sha256.Sum256(b.image) != b.digest || !page.VerifyChecksumNonMutating(b.image) {
		return false, false, ErrFormat
	}
	old := b.groups[0]
	if old == nil || old.owner != a || old.refs == 0 || old.base != c.baseline || c.baseline.owner != a || c.baseline.refs == 0 || c.baseline.base != nil {
		return false, false, ErrStale
	}
	if int(c.next) < groupSize {
		end := min(int(c.next)+releaseCells, groupSize)
		rbytes := uint64(end-int(c.next)) * (4*page.PageSize + 2*uint64(unsafe.Sizeof(bank{})))
		if !reserve(w, uint64(end-int(c.next))*2, rbytes) {
			return false, false, nil
		}
		for i := int(c.next); i < end; i++ {
			r := c.baseline.components[i]
			if r == (Ref{}) {
				continue
			}
			cb := a.valid(r)
			if cb == nil || cb.class != Component || !cb.sealed || cb.refs == ^uint64(0) || sha256.Sum256(cb.image) != cb.digest || !page.VerifyChecksumNonMutating(cb.image) {
				return false, false, ErrFormat
			}
			n := node.NewNode(cb.image)
			if n.Type() != page.PageTypeLeaf || n.Count() != 1 {
				return false, false, ErrFormat
			}
		}
		for i := int(c.next); i < end; i++ {
			r := c.baseline.components[i]
			if r == (Ref{}) {
				continue
			}
			a.valid(r).refs++
			c.flat.components[i] = r
			a.counters.EdgeReads++
			a.counters.EdgeWrites++
		}
		c.next = uint8(end)
		a.counters.Bytes += cost + rbytes
		return false, true, nil
	}
	count := uint64(0)
	for i := range old.components {
		if old.overrideMask&(uint64(1)<<uint(i)) != 0 {
			count++
		}
	}
	if count > 2 {
		return false, false, ErrFormat
	}
	edgeBytes := count*(6*page.PageSize+4*uint64(unsafe.Sizeof(bank{}))) + 4*uint64(unsafe.Sizeof(Group{}))
	if !reserve(w, 4+count*3, edgeBytes) {
		return false, false, nil
	}
	for i, r := range old.components {
		if old.overrideMask&(uint64(1)<<uint(i)) == 0 {
			continue
		}
		entry, e := b.directory.ClassEntry(uint16(i))
		if r == (Ref{}) {
			if e != nil || !entry.InlineAbsence() {
				return false, false, ErrFormat
			}
			continue
		}
		cb := a.valid(r)
		if e != nil || cb == nil || !cb.sealed || cb.class != Component || cb.refs == ^uint64(0) || r.PageID != entry.Operand.Ref.Page || cb.digest != entry.Operand.Digest || node.ValidatePrimaryComponent(entry, cb.image) != nil {
			return false, false, ErrFormat
		}
		if prior := c.flat.components[i]; prior != (Ref{}) && a.valid(prior) == nil {
			return false, false, ErrStale
		}
	}
	for i, r := range old.components {
		if old.overrideMask&(uint64(1)<<uint(i)) == 0 {
			continue
		}
		if r != (Ref{}) {
			a.valid(r).refs++
			a.counters.EdgeReads++
			a.counters.EdgeWrites++
		}
		prior := c.flat.components[i]
		if prior != (Ref{}) {
			cb := a.valid(prior)
			cb.refs--
			a.counters.EdgeReads++
			a.counters.EdgeWrites++
			if cb.refs == 0 {
				a.enqueueOn(c.queue, Local(prior.PageID), cb)
			}
		}
		c.flat.components[i] = r
	}
	b.groups[0] = c.flat
	a.dropGroupOn(c.queue, old)
	a.dropGroupOn(c.queue, c.baseline)
	c.baseline = nil
	c.flat = nil
	c.ready = true
	a.counters.EdgeWrites++
	a.counters.Bytes += cost + edgeBytes
	return true, true, nil
}
func (c *FlattenClaim) Cancel(w *iterator.OrdinalScanWork) (bool, error) {
	if c == nil || c.cancelled || c.ready {
		return true, nil
	}
	cost := 4*uint64(unsafe.Sizeof(Group{})) + 2*uint64(unsafe.Sizeof(ReleaseQueue{})) + 128
	if !reserve(w, 3, cost) {
		return false, nil
	}
	a := c.arena
	a.mu.Lock()
	defer a.mu.Unlock()
	a.dropGroupOn(c.queue, c.flat)
	a.dropGroupOn(c.queue, c.baseline)
	c.flat = nil
	c.baseline = nil
	c.cancelled = true
	a.counters.Bytes += cost
	return true, nil
}
