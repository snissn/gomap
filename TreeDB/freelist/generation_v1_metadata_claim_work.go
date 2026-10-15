package freelist

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"unsafe"
)

// Saturate the admission bound: overflow must refuse an indivisible claim,
// never make a large current ledger look like a small one.
func reservationBoundAddV1(a, b uint64) uint64 {
	if a > ^uint64(0)-b {
		return ^uint64(0)
	}
	return a + b
}
func reservationBoundMulV1(a, b uint64) uint64 {
	if b != 0 && a > ^uint64(0)/b {
		return ^uint64(0)
	}
	return a * b
}
func (l *ReservationLedger) claimStorageBoundLockedV1(candidate CandidateIDV1, n, coverage uint64) (uint64, uint64) {
	owners, candidates := uint64(len(l.owners)), uint64(len(l.candidates))
	ids, oldCoverage := uint64(0), uint64(0)
	if r := l.candidates[candidate]; r != nil {
		ids, oldCoverage = uint64(len(r.ids)), uint64(len(r.abandonedCoverage))
	}
	add, mul := reservationBoundAddV1, reservationBoundMulV1
	records := add(1, candidates)
	records = add(records, mul(n, add(1, add(owners, n))))
	records = add(records, mul(n, add(1, add(ids, n))))
	records = add(records, mul(coverage, add(1, add(oldCoverage, coverage))))
	bytes := add(uint64(unsafe.Sizeof(reservation{})), mul(128, add(1, add(candidates, add(owners, n)))))
	bytes = add(bytes, mul(8, mul(n, add(1, add(ids, n)))))
	bytes = add(bytes, mul(uint64(unsafe.Sizeof(reservationInterval{})), mul(coverage, add(1, add(oldCoverage, coverage)))))
	return records, bytes
}
func (l *ReservationLedger) claimReusedMetadataWithWorkV1(candidate CandidateIDV1, start, count, highWater uint64, data []allocatedPage, abandoned []ReservationExtentV1, w *iterator.OrdinalScanWork) (bool, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if w != nil {
		burned, candidates, n := uint64(len(l.burnedTails)), uint64(len(l.candidates)), uint64(len(data))
		add, mul := reservationBoundAddV1, reservationBoundMulV1
		records, bytes := l.claimStorageBoundLockedV1(candidate, n, uint64(len(abandoned)))
		records = add(records, add(2, burned))
		records = add(records, mul(n, add(2, add(burned, candidates))))
		records = add(records, mul(count, add(1, add(burned, candidates))))
		bytes = add(bytes, mul(records, 24))
		if ok, e := appendReserve(w, records, bytes); !ok || e != nil {
			return false, false, e
		}
	}
	return l.claimReusedMetadataLocked(candidate, start, count, highWater, data, abandoned), true, nil
}

func (l *ReservationLedger) reserveTailWithWorkV1(candidate CandidateIDV1, minimum, statePages uint64, data []uint64, extents []ReservationExtentV1, w *iterator.OrdinalScanWork) (uint64, uint64, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if w != nil {
		add, mul := reservationBoundAddV1, reservationBoundMulV1
		owners, burned, candidates, n := uint64(len(l.owners)), uint64(len(l.burnedTails)), uint64(len(l.candidates)), uint64(len(data))
		ranges := add(owners, add(burned, candidates))
		// Each conflicting interval/owner advances start beyond its end, so it
		// cannot conflict again. Geometry and the final no-conflict pass are
		// included; the original core performs the actual validation and claim.
		records, bytes := l.claimStorageBoundLockedV1(candidate, n, uint64(len(extents))+1)
		records = add(records, add(2, burned))
		records = add(records, mul(n, mul(2, add(1, add(burned, candidates)))))
		records = add(records, mul(add(ranges, 1), add(ranges, 1)))
		records = add(records, uint64(len(extents)))
		bytes = add(bytes, mul(records, 24))
		if ok, e := appendReserve(w, records, bytes); !ok || e != nil {
			return 0, 0, false, e
		}
	}
	start, count, e := l.reserveTailLockedV1(candidate, minimum, statePages, data, extents)
	return start, count, true, e
}
