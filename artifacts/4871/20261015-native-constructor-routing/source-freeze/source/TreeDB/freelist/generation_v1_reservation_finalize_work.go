package freelist

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

// Finalization merges the already-normalized inventory with the contiguous
// skipped tail and exact target interval. It replaces the second full clone
// and sort in MaterializeCandidate. The completed vector is candidate output:
// it must not be reset with these scalar controls or a batch arena.
type reservationFinalizeWorkV1 struct {
	input                  []ReservationExtentV1
	entries                []ReservationExtentV1
	target                 ReservationExtentV1
	gapStart, gapRemaining uint64
	minimum, start, count  uint64
	index                  int
	pendingCount           uint64
	phase                  uint8
	bound, targetDone      bool
	firstErr               error
}

func (s *reservationFinalizeWorkV1) step(input []ReservationExtentV1, minimum, start, count uint64, w *iterator.OrdinalScanWork) (bool, error) {
	if s.firstErr != nil {
		return false, s.firstErr
	}
	if !s.bound {
		s.input, s.minimum, s.start, s.count, s.bound = input, minimum, start, count, true
	}
	if s.minimum != minimum || s.start != start || s.count != count || len(s.input) != len(input) || len(input) != 0 && &s.input[0] != &input[0] {
		return false, ErrGenerationFormat
	}
	ready, err := s.advance(input, minimum, start, count, w)
	if err != nil {
		s.firstErr = err
	}
	return ready, err
}

func (s *reservationFinalizeWorkV1) advance(input []ReservationExtentV1, minimum, start, count uint64, w *iterator.OrdinalScanWork) (bool, error) {
	size := uint64(unsafe.Sizeof(ReservationExtentV1{}))
	for {
		switch s.phase {
		case 0:
			if start < 2 || count == 0 || count > uint64(^uint32(0)) || start > ^uint64(0)-count {
				return false, ErrGenerationFormat
			}
			gap := uint64(0)
			if start > minimum {
				gap = start - minimum
			}
			gaps := uint64(0)
			if gap != 0 {
				gaps = (gap-1)/uint64(^uint32(0)) + 1
			}
			if gaps > uint64(^uint(0)>>1)-uint64(len(input))-1 {
				return false, ErrGenerationFormat
			}
			n := len(input) + int(gaps) + 1
			if ok, err := appendReserve(w, 1, uint64(n)*size); !ok || err != nil {
				return false, err
			}
			s.entries = make([]ReservationExtentV1, 0, n)
			s.target = ReservationExtentV1{StartPageID: start, Count: uint32(count), Kind: ReservationTargetMetadata}
			s.gapStart, s.gapRemaining = minimum, gap
			s.phase = 1
		case 1:
			for s.index < len(input) || s.gapRemaining != 0 || !s.targetDone {
				// At most three fixed-size stream heads, one validation/last
				// comparison, and one output write. No input-sized scan here.
				if ok, err := appendReserve(w, 5, 5*size); !ok || err != nil {
					return false, err
				}
				var next ReservationExtentV1
				source := uint8(0)
				if s.index < len(input) {
					next, source = input[s.index], 1
				}
				if s.gapRemaining != 0 {
					gap := ReservationExtentV1{StartPageID: s.gapStart, Count: uint32(min(s.gapRemaining, uint64(^uint32(0)))), Kind: ReservationAbandonedAppend}
					if source == 0 || compareReservationExtentsV1(gap, next) < 0 {
						next, source = gap, 2
					}
				}
				if !s.targetDone && (source == 0 || compareReservationExtentsV1(s.target, next) < 0) {
					next, source = s.target, 3
				}
				var err error
				s.entries, err = appendNormalizedReservationExtentV1(s.entries, next, false)
				if err != nil {
					return false, err
				}
				if next.Kind == ReservationPendingMetadataRetirement {
					s.pendingCount += uint64(next.Count)
				}
				switch source {
				case 1:
					s.index++
				case 2:
					s.gapStart += uint64(next.Count)
					s.gapRemaining -= uint64(next.Count)
				case 3:
					s.targetDone = true
				}
			}
			s.phase = 2
		case 2:
			return true, nil
		default:
			return false, ErrGenerationFormat
		}
	}
}
