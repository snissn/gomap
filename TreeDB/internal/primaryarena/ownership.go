package primaryarena

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/pager"
	"unsafe"
)

// BorrowComponent returns the exact current bank identity without adding an
// owner. An already-owned complete source root or private component claim must
// protect it until the caller's NewGroup acquires independent component refs.
func (a *Arena) BorrowComponent(id uint64, w *iterator.OrdinalScanWork) (Ref, bool, error) {
	if !reserve(w, 1, 2*uint64(unsafe.Sizeof(bank{}))) {
		return Ref{}, false, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	b := a.slot(id)
	if b == nil || b.refs == 0 || !b.sealed || b.class != Component {
		return Ref{}, false, ErrStale
	}
	a.counters.EdgeReads++
	a.counters.Bytes += 2 * uint64(unsafe.Sizeof(bank{}))
	return Ref{id, b.incarnation}, true, nil
}

// TakePublicationBundle moves the ordinary producer's two real private root
// handles into the common publisher. It never acquires an old published root by
// ID. Exactly one take is allowed, after complete directory construction and
// before the private record is sealed. The caller owns both returned handles.
func (a *Arena) TakePublicationBundle(id uint64, w *iterator.OrdinalScanWork) (PublicationBundle, bool, error) {
	if !reserve(w, 2, 4*uint64(unsafe.Sizeof(bank{}))+64) {
		return PublicationBundle{}, false, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.capsuleFormat && Local(id) >= pager.PrimaryReadRootLocalBaseV6 {
		d := a.slot(id)
		if d == nil || d.class != Directory || !d.sealed || d.refs != 1 || !d.privatePublication {
			return PublicationBundle{}, false, ErrStale
		}
		d.privatePublication = false
		return PublicationBundle{Directory: Ref{id, d.incarnation}}, true, nil
	}
	d := a.slot(id)
	r := a.slot(id + 1)
	if d == nil || r == nil || d.class != Directory || r.class != Record || !d.sealed || r.sealed || d.refs != 1 || r.refs != 1 || !d.privatePublication || !r.privatePublication || d.pair != Local(id) || r.pair != d.pair {
		return PublicationBundle{}, false, ErrStale
	}
	d.privatePublication = false
	r.privatePublication = false
	a.counters.Bytes += 4*uint64(unsafe.Sizeof(bank{})) + 64
	return PublicationBundle{Ref{id, d.incarnation}, Ref{id + 1, r.incarnation}}, true, nil
}

// AbandonPublicationBundle drops an untaken ordinary output's private handles.
// All descendants remain on the charged release queue; no tree drain is hidden.
func (a *Arena) AbandonPublicationBundle(id uint64, w *iterator.OrdinalScanWork) (bool, error) {
	cost := 8*uint64(unsafe.Sizeof(bank{})) + 128
	if !reserve(w, 4, cost) {
		return false, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.capsuleFormat && Local(id) >= pager.PrimaryReadRootLocalBaseV6 {
		d := a.slot(id)
		if d == nil || d.class != Directory || !d.sealed || d.refs != 1 || !d.privatePublication {
			return false, ErrStale
		}
		d.privatePublication = false
		d.refs--
		a.enqueue(Local(id), d)
		a.counters.RootDrops++
		return true, nil
	}
	d, r := a.slot(id), a.slot(id+1)
	if d == nil || r == nil || d.class != Directory || r.class != Record || !d.sealed || r.sealed || d.refs != 1 || r.refs != 1 || !d.privatePublication || !r.privatePublication || d.pair != Local(id) || r.pair != d.pair {
		return false, ErrStale
	}
	d.privatePublication = false
	r.privatePublication = false
	d.refs--
	r.refs--
	a.enqueue(Local(id), d)
	a.enqueue(Local(id+1), r)
	a.counters.RootDrops += 2
	a.counters.Bytes += cost
	return true, nil
}

// ValidateUnsealedPublicationBundle proves two actual, still-owned private
// bank identities before transaction ownership transfer. Geometry alone cannot
// authorize a guessed record incarnation or a reused/stale private claim.
func (a *Arena) ValidateUnsealedPublicationBundle(bundle PublicationBundle, w *iterator.OrdinalScanWork) (bool, error) {
	bytes := 4*uint64(unsafe.Sizeof(bank{})) + 2*uint64(unsafe.Sizeof(Ref{}))
	if !reserve(w, 2, bytes) {
		return false, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	d, r := a.valid(bundle.Directory), a.valid(bundle.Record)
	if d == nil || r == nil || d.class != Directory || r.class != Record || !d.sealed || r.sealed || bundle.Record.PageID != bundle.Directory.PageID+1 || d.pair != Local(bundle.Directory.PageID) || r.pair != d.pair {
		return false, ErrStale
	}
	a.counters.EdgeReads += 2
	a.counters.Bytes += bytes
	return true, nil
}

// BorrowDirectory resolves an already owned, sealed physical directory.
// The caller holds root admission and an independent containing record/state.
func (a *Arena) BorrowDirectory(id uint64, w *iterator.OrdinalScanWork) (Ref, bool, error) {
	if !reserve(w, 1, 2*uint64(unsafe.Sizeof(bank{}))) {
		return Ref{}, false, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	b := a.slot(id)
	if b == nil || b.refs == 0 || !b.sealed || b.class != Directory {
		return Ref{}, false, ErrStale
	}
	a.counters.EdgeReads++
	a.counters.Bytes += 2 * uint64(unsafe.Sizeof(bank{}))
	return Ref{id, b.incarnation}, true, nil
}

func (a *Arena) Recovered() bool { a.mu.Lock(); defer a.mu.Unlock(); return a.recovered }

// BorrowDependency resolves an independently retained complete metadata root.
func (a *Arena) BorrowDependency(id uint64, w *iterator.OrdinalScanWork) (Ref, bool, error) {
	if !reserve(w, 1, 2*uint64(unsafe.Sizeof(bank{}))) {
		return Ref{}, false, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	b := a.slot(id)
	if b == nil || b.refs == 0 || !b.sealed || b.class != Dependency {
		return Ref{}, false, ErrStale
	}
	a.counters.EdgeReads++
	a.counters.Bytes += 2 * uint64(unsafe.Sizeof(bank{}))
	return Ref{id, b.incarnation}, true, nil
}

// ReleaseUntakenOrdinaryBundle releases only the producer's still-private pair.
// The caller retains ordinary writer admission and the exact constructed ID.
// A completed take has already transferred both handles to the common publisher;
// cleanup must not drop those handles a second time after an error.
func (a *Arena) ReleaseUntakenOrdinaryBundle(id uint64) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	d, r := a.slot(id), a.slot(id+1)
	a.counters.EdgeReads += 2
	a.counters.Bytes += 4*uint64(unsafe.Sizeof(bank{})) + 64
	if d == nil || r == nil || d.class != Directory || r.class != Record || d.pair != Local(id) || r.pair != d.pair || d.privatePublication != r.privatePublication {
		return ErrStale
	}
	if !d.privatePublication {
		return nil
	}
	if !d.sealed || r.sealed || d.refs != 1 || r.refs != 1 {
		return ErrStale
	}
	d.privatePublication, r.privatePublication = false, false
	d.refs--
	r.refs--
	a.enqueue(Local(id), d)
	a.enqueue(Local(id+1), r)
	a.counters.RootDrops += 2
	a.counters.Bytes += 4*uint64(unsafe.Sizeof(bank{})) + 64
	return nil
}
