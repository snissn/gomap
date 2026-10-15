package freelist

import (
	"reflect"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

// transactionCloneWorkV1 continues the original allocator staging copy. The
// source is inspected only while its allocator lock is held. A caller that
// yields must revalidate the original transaction before resuming; the copy
// never occupies allocatorCOWStateV1.prepared.
type transactionCloneWorkV1 struct {
	source               *FreelistTxn
	staged               FreelistTxn
	allocated, abandoned int
	changed, replaced    reflect.MapIter
	phase                uint8
	charge               uint64
}

func cloneTxnChargeV1(t *FreelistTxn) uint64 {
	// Go maps are admitted conservatively for their full bucket/control backing,
	// including both keys and values and allocator rounding, before make.
	return retainedalloc.AllocationCharge(uint64(len(t.allocated))*uint64(unsafe.Sizeof(allocatedPage{}))) +
		retainedalloc.AllocationCharge(uint64(len(t.abandonedAppends))*uint64(unsafe.Sizeof(ReservationExtentV1{}))) +
		retainedalloc.AllocationCharge(64+uint64(len(t.changedChunks))*64) +
		retainedalloc.AllocationCharge(64+uint64(len(t.replacedMetadata))*64)
}

func (s *transactionCloneWorkV1) step(t *FreelistTxn, owner *retainedalloc.Owner, w *iterator.OrdinalScanWork) (*FreelistTxn, bool, error) {
	if s.phase == 0 {
		if err := t.valid(); err != nil {
			return nil, false, err
		}
		if w == nil {
			// Keep the ordinary synchronous copy on its original range/copy path.
			clone := *t
			clone.allocated = append([]allocatedPage(nil), t.allocated...)
			clone.abandonedAppends = append([]ReservationExtentV1(nil), t.abandonedAppends...)
			clone.changedChunks = make(map[uint64]struct{}, len(t.changedChunks))
			for key := range t.changedChunks {
				clone.changedChunks[key] = struct{}{}
			}
			clone.replacedMetadata = make(map[uint64]struct{}, len(t.replacedMetadata))
			for key := range t.replacedMetadata {
				clone.replacedMetadata[key] = struct{}{}
			}
			s.staged, s.source, s.phase = clone, t, 5
			return &s.staged, true, nil
		}
		if owner == nil {
			return nil, false, retainedalloc.ErrCapacity
		}
		charge := cloneTxnChargeV1(t)
		if ok, err := appendReserve(w, 5, charge+uint64(unsafe.Sizeof(*s))); !ok || err != nil {
			return nil, false, err
		}
		if err := owner.AddPending(charge); err != nil {
			return nil, false, err
		}
		s.charge = charge
		s.source, s.staged = t, *t
		s.staged.allocated = make([]allocatedPage, len(t.allocated))
		s.staged.abandonedAppends = make([]ReservationExtentV1, len(t.abandonedAppends))
		s.staged.changedChunks = make(map[uint64]struct{}, len(t.changedChunks))
		s.staged.replacedMetadata = make(map[uint64]struct{}, len(t.replacedMetadata))
		s.changed.Reset(reflect.ValueOf(t.changedChunks))
		s.replaced.Reset(reflect.ValueOf(t.replacedMetadata))
		s.phase = 1
	}
	if s.source != t {
		return nil, false, ErrGenerationParent
	}
	for s.phase < 5 {
		switch s.phase {
		case 1:
			if s.allocated == len(t.allocated) {
				s.phase = 2
				continue
			}
			if ok, err := appendReserve(w, 1, 2*uint64(unsafe.Sizeof(allocatedPage{}))); !ok || err != nil {
				return nil, false, err
			}
			s.staged.allocated[s.allocated] = t.allocated[s.allocated]
			s.allocated++
		case 2:
			if s.abandoned == len(t.abandonedAppends) {
				s.phase = 3
				continue
			}
			if ok, err := appendReserve(w, 1, 2*uint64(unsafe.Sizeof(ReservationExtentV1{}))); !ok || err != nil {
				return nil, false, err
			}
			s.staged.abandonedAppends[s.abandoned] = t.abandonedAppends[s.abandoned]
			s.abandoned++
		case 3:
			if ok, err := appendReserve(w, 1, 64); !ok || err != nil {
				return nil, false, err
			}
			if !s.changed.Next() {
				s.changed.Reset(reflect.Value{})
				s.phase = 4
				continue
			}
			s.staged.changedChunks[s.changed.Key().Uint()] = struct{}{}
		case 4:
			if ok, err := appendReserve(w, 1, 64); !ok || err != nil {
				return nil, false, err
			}
			if !s.replaced.Next() {
				s.replaced.Reset(reflect.Value{})
				s.phase = 5
				continue
			}
			s.staged.replacedMetadata[s.replaced.Key().Uint()] = struct{}{}
		}
	}
	return &s.staged, true, nil
}
