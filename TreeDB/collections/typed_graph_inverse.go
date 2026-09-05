package collections

import "errors"

var errTypedGraphInverseRequired = errors.New("collections: typed graph inverse row mapping missing; rebuild required")

// Physical coordinates deliberately exclude LSN from ordering: two graph rows
// cannot own the same physical slot, even if their alleged versions differ.
func compareColumnVectorGraphPhysicalRow(a, b DocumentRowRef) int {
	if a.Generation < b.Generation {
		return -1
	}
	if a.Generation > b.Generation {
		return 1
	}
	if a.PartID < b.PartID {
		return -1
	}
	if a.PartID > b.PartID {
		return 1
	}
	if a.RowIndex < b.RowIndex {
		return -1
	}
	if a.RowIndex > b.RowIndex {
		return 1
	}
	return 0
}

func (s *columnVectorGraphRowRefStateSource) inversePermutationActive() bool {
	return s.preparedViewActive() && s.ordinalsByPhysicalRow.Alive() && s.ordinalsByPhysicalRow.Rows == s.rows
}

func (s *columnVectorGraphRowRefStateSource) inverseRef(index int) (int, DocumentRowRef, bool) {
	ordinal, ok := s.ordinalsByPhysicalRow.Value(index)
	if !ok || ordinal < 0 || ordinal >= int64(s.rows) {
		return 0, DocumentRowRef{}, false
	}
	ref, ok := s.rowRefForOrdinal(int(ordinal))
	return int(ordinal), ref, ok
}

func (s *columnVectorGraphRowRefStateSource) validateInversePermutation() error {
	if !s.inversePermutationActive() {
		return errors.New("collections: graph inverse row mapping unavailable")
	}
	var previous DocumentRowRef
	for i := 0; i < s.rows; i++ {
		_, ref, ok := s.inverseRef(i)
		if !ok || (i > 0 && compareColumnVectorGraphPhysicalRow(previous, ref) >= 0) {
			return errors.New("collections: graph inverse row mapping is not a strict physical-coordinate permutation")
		}
		previous = ref
	}
	return nil
}

func (s *columnVectorGraphRowRefStateSource) ordinalForPhysicalRow(ref DocumentRowRef) (int, bool) {
	if !s.inversePermutationActive() {
		return 0, false
	}
	lo, hi := 0, s.rows
	for lo < hi {
		mid := lo + (hi-lo)/2
		_, candidate, ok := s.inverseRef(mid)
		if !ok {
			return 0, false
		}
		if compareColumnVectorGraphPhysicalRow(candidate, ref) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == s.rows {
		return 0, false
	}
	ordinal, candidate, ok := s.inverseRef(lo)
	return ordinal, ok && compareColumnVectorGraphPhysicalRow(candidate, ref) == 0 && candidate.AppliedCommandLSN == ref.AppliedCommandLSN
}
