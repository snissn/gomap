package freelist

import (
	"reflect"
	"slices"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

// The inventory owns a single extent vector. Allocation records and replacement
// metadata enter it directly: no two ID vectors, per-ID retirement expansion,
// sortedUnique copies or second sorting pass. Persistent candidate output is
// not arena scratch; its consumer owns the completed extent vector.
type reservationInventoryWorkV1 struct {
	txn                            *FreelistTxn
	root                           *stateNode
	allocated, abandoned, replaced int
	entries                        []ReservationExtentV1
	index, outIndex                int
	replacements                   reflect.MapIter
	replacementID                  uint64
	normalized                     reservationNormalizeWorkV1
	phase                          uint8
	firstErr                       error
}

func (s *reservationInventoryWorkV1) step(t *FreelistTxn, w *iterator.OrdinalScanWork) (bool, error) {
	if s.firstErr != nil {
		return false, s.firstErr
	}
	if t == nil || t.base == nil || w != nil && !t.consumed {
		return false, ErrGenerationFormat
	}
	if s.txn == nil {
		s.txn, s.root = t, t.root
		s.allocated, s.abandoned, s.replaced = len(t.allocated), len(t.abandonedAppends), len(t.replacedMetadata)
	}
	if s.txn != t || s.root != t.root || s.allocated != len(t.allocated) || s.abandoned != len(t.abandonedAppends) || s.replaced != len(t.replacedMetadata) {
		return false, ErrGenerationFormat
	}
	ready, err := s.advance(t, w)
	if err != nil {
		s.firstErr = err
	}
	return ready, err
}

func (s *reservationInventoryWorkV1) advance(t *FreelistTxn, w *iterator.OrdinalScanWork) (bool, error) {
	if t == nil || t.base == nil {
		return false, ErrGenerationFormat
	}
	for {
		switch s.phase {
		case 0:
			count := len(t.allocated) + len(t.abandonedAppends) + len(t.replacedMetadata)
			if count < len(t.allocated) || count < len(t.abandonedAppends) || count < len(t.replacedMetadata) {
				return false, ErrGenerationFormat
			}
			if ok, err := appendReserve(w, 1, uint64(count)*uint64(unsafe.Sizeof(ReservationExtentV1{}))); !ok || err != nil {
				return false, err
			}
			s.entries = make([]ReservationExtentV1, count)
			s.phase = 1
		case 1:
			n, err := stateEncodeEntriesV1(w, len(t.allocated)-s.index, uint64(unsafe.Sizeof(allocatedPage{}))+uint64(unsafe.Sizeof(ReservationExtentV1{})))
			if err != nil || n == 0 && s.index < len(t.allocated) {
				return false, err
			}
			end := s.index + n
			for ; s.index < end; s.index++ {
				a := t.allocated[s.index]
				s.entries[s.outIndex] = ReservationExtentV1{StartPageID: a.id, Count: 1, Kind: a.kind}
				s.outIndex++
			}
			if s.index < len(t.allocated) {
				return false, nil
			}
			s.index, s.phase = 0, 2
		case 2:
			n, err := stateEncodeEntriesV1(w, len(t.abandonedAppends)-s.index, 2*uint64(unsafe.Sizeof(ReservationExtentV1{})))
			if err != nil || n == 0 && s.index < len(t.abandonedAppends) {
				return false, err
			}
			end := s.index + n
			for ; s.index < end; s.index++ {
				s.entries[s.outIndex] = t.abandonedAppends[s.index]
				s.outIndex++
			}
			if s.index < len(t.abandonedAppends) {
				return false, nil
			}
			if w == nil {
				// Keep the ordinary map range and stack-only operand borrowing.
				for id := range t.replacedMetadata {
					s.entries[s.outIndex] = ReservationExtentV1{StartPageID: id, Count: 1, Kind: ReservationPendingMetadataRetirement, LastReachableCommitSeq: t.base.commitSeq}
					s.outIndex++
				}
				s.phase = 4
				continue
			}
			s.replacements.Reset(reflect.ValueOf(t.replacedMetadata))
			s.phase = 3
		case 3:
			// The enclosing constructor freezes this original transaction.
			// The iterator never survives a mutation of its private map.
			if s.outIndex == len(s.entries) {
				s.replacements.Reset(reflect.Value{})
				s.phase = 4
				continue
			}
			if ok, err := appendReserve(w, 1, 8+uint64(unsafe.Sizeof(ReservationExtentV1{}))); !ok || err != nil {
				return false, err
			}
			if !s.replacements.Next() {
				return false, ErrGenerationFormat
			}
			reflect.ValueOf(&s.replacementID).Elem().SetIterKey(&s.replacements)
			id := s.replacementID
			s.entries[s.outIndex] = ReservationExtentV1{StartPageID: id, Count: 1, Kind: ReservationPendingMetadataRetirement, LastReachableCommitSeq: t.base.commitSeq}
			s.outIndex++
		case 4:
			s.normalized.entries, s.normalized.owned, s.normalized.dedupIDs = s.entries, true, true
			s.phase = 5
		case 5:
			return s.normalized.advance(nil, w)
		default:
			return false, ErrGenerationFormat
		}
	}
}

// The same canonical compare/validate/coalesce core is used by ordinary range
// normalization and retained inventory. The ordinary nil driver keeps the
// maintained slices.SortFunc fast path. Finite callers retain an admitted
// stable merge sort rather than hiding an input-sized sort in one record.
type reservationNormalizeWorkV1 struct {
	input                                                    []ReservationExtentV1
	entries, scratch, original, out                          []ReservationExtentV1
	metadata                                                 *retainedalloc.Owner
	scratchCharge                                            uint64
	removal                                                  retainedalloc.RemovalWork
	index, width, base, left, right, leftEnd, rightEnd, dest int
	phase                                                    uint8
	bound, owned, dedupIDs                                   bool
	firstErr                                                 error
}

func (s *reservationNormalizeWorkV1) step(input []ReservationExtentV1, w *iterator.OrdinalScanWork) (bool, error) {
	if s.firstErr != nil {
		return false, s.firstErr
	}
	if !s.bound {
		s.input, s.bound = input, true
	}
	if len(s.input) != len(input) || len(input) != 0 && &s.input[0] != &input[0] {
		return false, ErrGenerationFormat
	}
	ready, err := s.advance(input, w)
	if err != nil {
		s.firstErr = err
	}
	return ready, err
}

func (s *reservationNormalizeWorkV1) advance(input []ReservationExtentV1, w *iterator.OrdinalScanWork) (bool, error) {
	size := uint64(unsafe.Sizeof(ReservationExtentV1{}))
	for {
		switch s.phase {
		case 0:
			if !s.owned {
				if ok, err := appendReserve(w, 1, uint64(len(input))*size); !ok || err != nil {
					return false, err
				}
				s.entries = make([]ReservationExtentV1, len(input))
				s.phase = 1
			} else {
				s.phase = 2
			}
		case 1:
			n, err := stateEncodeEntriesV1(w, len(input)-s.index, 2*size)
			if err != nil || n == 0 && s.index < len(input) {
				return false, err
			}
			copy(s.entries[s.index:s.index+n], input[s.index:s.index+n])
			s.index += n
			if s.index < len(input) {
				return false, nil
			}
			s.index, s.phase = 0, 2
		case 2:
			if w == nil {
				slices.SortFunc(s.entries, compareReservationExtentsV1)
				s.phase = 5
				continue
			}
			if len(s.entries) < 2 {
				s.phase = 5
				continue
			}
			if ok, err := appendReserve(w, 1, uint64(len(s.entries))*size); !ok || err != nil {
				return false, err
			}
			// The inventory's original vector is canonical DATA output. This
			// second vector exists only for retained sorting and belongs to the
			// original producer's pending PRIMARY storage until scrubbed.
			if s.metadata != nil {
				charge := retainedalloc.AllocationCharge(uint64(len(s.entries)) * size)
				if err := s.metadata.AddPending(charge); err != nil {
					return false, err
				}
				s.scratchCharge = charge
			}
			s.original = s.entries
			s.scratch = make([]ReservationExtentV1, len(s.entries))
			s.width, s.phase = 1, 3
		case 3:
			if s.width >= len(s.entries) {
				s.phase = 5
				continue
			}
			if s.base == len(s.entries) {
				s.entries, s.scratch = s.scratch, s.entries
				s.width *= 2
				s.base = 0
				continue
			}
			s.left, s.right = s.base, min(s.base+s.width, len(s.entries))
			s.leftEnd, s.rightEnd = s.right, min(s.base+2*s.width, len(s.entries))
			s.dest, s.phase = s.base, 4
		case 4:
			for s.dest < s.rightEnd {
				comparing := s.left < s.leftEnd && s.right < s.rightEnd
				records, bytes := uint64(1), 2*size
				if comparing {
					records, bytes = 3, 3*size
				}
				if ok, err := appendReserve(w, records, bytes); !ok || err != nil {
					return false, err
				}
				if s.right == s.rightEnd || s.left < s.leftEnd && compareReservationExtentsV1(s.entries[s.left], s.entries[s.right]) <= 0 {
					s.scratch[s.dest] = s.entries[s.left]
					s.left++
				} else {
					s.scratch[s.dest] = s.entries[s.right]
					s.right++
				}
				s.dest++
			}
			s.base, s.phase = s.rightEnd, 3
		case 5:
			// An odd number of merge passes leaves the sorted values in the
			// extra vector. Copy them back before exposing/coalescing output;
			// no finite-only backing escapes into the allocator generation.
			if len(s.original) != 0 && &s.entries[0] != &s.original[0] {
				n, err := stateEncodeEntriesV1(w, len(s.entries)-s.index, 2*size)
				if err != nil || n == 0 && s.index < len(s.entries) {
					return false, err
				}
				copy(s.original[s.index:s.index+n], s.entries[s.index:s.index+n])
				s.index += n
				if s.index < len(s.entries) {
					return false, nil
				}
				s.entries = s.original
			}
			s.out, s.index, s.phase = s.entries[:0], 0, 6
		case 6:
			for s.index < len(s.entries) {
				if ok, err := appendReserve(w, 3, 3*size); !ok || err != nil {
					return false, err
				}
				extent := s.entries[s.index]
				var err error
				s.out, err = appendNormalizedReservationExtentV1(s.out, extent, s.dedupIDs)
				if err != nil {
					return false, err
				}
				s.index++
			}
			s.phase = 7
		case 7:
			ready, err := s.releaseScratch(w)
			if !ready || err != nil {
				return false, err
			}
			s.phase = 8
		case 8:
			return true, nil
		default:
			return false, ErrGenerationFormat
		}
	}
}

// releaseScratch also handles cancellation during a merge pass, when entries
// may alias the extra vector. All aliases are removed before its charge falls.
// The original vector remains canonical allocator output on success.
func (s *reservationNormalizeWorkV1) releaseScratch(w *iterator.OrdinalScanWork) (bool, error) {
	if s.scratch != nil || s.original != nil {
		if ok, err := appendReserve(w, 1, uint64(unsafe.Sizeof(*s))); !ok || err != nil {
			return false, err
		}
		if len(s.original) != 0 && len(s.entries) != 0 && &s.entries[0] != &s.original[0] {
			s.entries = nil
		}
		s.scratch, s.original = nil, nil
	}
	if s.scratchCharge != 0 {
		ready, err := s.metadata.RemovePendingWork(s.scratchCharge, &s.removal, w)
		if !ready || err != nil {
			return false, err
		}
		s.scratchCharge = 0
	}
	return true, nil
}

// appendNormalizedReservationExtentV1 is the canonical ordered validation and
// coalescing step. Sorting inventory and merging the final target/gap streams
// both enter this core; neither publishes a second normalization authority.
func appendNormalizedReservationExtentV1(out []ReservationExtentV1, extent ReservationExtentV1, dedupIDs bool) ([]ReservationExtentV1, error) {
	if extent.StartPageID < 2 || extent.Count == 0 || extent.Kind < ReservationReusedData || extent.Kind > ReservationAbandonedAppend || extent.StartPageID+uint64(extent.Count) < extent.StartPageID {
		return out, ErrGenerationFormat
	}
	if len(out) > 0 {
		last := &out[len(out)-1]
		lastEnd := last.StartPageID + uint64(last.Count)
		if lastEnd > extent.StartPageID {
			// Old per-kind sortedUnique removed these duplicate singleton IDs.
			if dedupIDs && extent.Count == 1 && extent.Kind != ReservationAbandonedAppend && extent.Kind != ReservationTargetMetadata && last.Kind == extent.Kind && last.LastReachableCommitSeq == extent.LastReachableCommitSeq {
				return out, nil
			}
			return out, ErrGenerationFormat
		}
		if last.Kind == extent.Kind && last.LastReachableCommitSeq == extent.LastReachableCommitSeq && lastEnd == extent.StartPageID && uint64(last.Count)+uint64(extent.Count) <= uint64(^uint32(0)) {
			last.Count += extent.Count
			return out, nil
		}
	}
	return append(out, extent), nil
}
