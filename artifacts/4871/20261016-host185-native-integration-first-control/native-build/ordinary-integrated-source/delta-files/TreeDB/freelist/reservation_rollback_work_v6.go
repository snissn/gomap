package freelist

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"unsafe"
)

func (l *ReservationLedger) rollbackPreVisibleWithWorkV6(c CandidateIDV1, w *iterator.OrdinalScanWork) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if w != nil {
		r := l.candidates[c]
		records, bytes := uint64(1), uint64(16)
		if r != nil {
			records += uint64(len(r.ids))
			bytes += uint64(unsafe.Sizeof(*r)) + 16*uint64(len(r.ids))
			if r.tailReserved && r.tailWriteAttempted && !r.reusedMetadata && len(l.burnedTails) == cap(l.burnedTails) {
				records += 1 + uint64(len(l.burnedTails))
				bytes += uint64(unsafe.Sizeof(reservationInterval{})) * uint64(3*len(l.burnedTails)+1)
			}
		}
		if ok, e := appendReserve(w, records, bytes); !ok || e != nil {
			return false, e
		}
	}
	return true, l.rollbackPreVisibleLockedV6(c)
}
