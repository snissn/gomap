package collections

import (
	"slices"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// orderedTextDocumentField takes ownership of terms and their occurrence lanes.
// Call only after the final lookup; sorting invalidates accumulator indices.
func orderedTextDocumentField(field string, length uint32, terms []textDocumentTermState) textDocumentFieldState {
	slices.SortFunc(terms, func(a, b textDocumentTermState) int { return compareTextStrings(a.Term, b.Term) })
	return textDocumentFieldState{Field: field, Length: length, Terms: terms}
}

func (a *textTermAccumulator) takeDocumentField(field string) textDocumentFieldState {
	out := orderedTextDocumentField(field, a.length, a.terms)
	// All backing remains charged, including obsolete map/growth cohorts.
	// Detach callbacks/indices: the prepared state is the only consumer now.
	a.terms, a.lookup, a.admit = nil, nil, nil
	a.lookupCapacity = 0
	return out
}

func textTermGrowthCapacity(length, capacity int) (int, error) {
	if length < capacity {
		return capacity, nil
	}
	if capacity == 0 {
		return 1, nil
	}
	if capacity > int(^uint(0)>>1)/2 {
		return 0, memtable.ErrCOWCapacity
	}
	return 2 * capacity, nil
}

// addAdmittedToken uses the maintained analyzer's failable sink. The input and
// Builder promise is separately admitted BEFORE analysis, since WriteRune runs
// before this callback. Every concrete new backing here is admitted before make;
// no old backing credit is returned, even after successful replacement.
func (a *textTermAccumulator) addAdmittedToken(token TextToken) error {
	const max = uint64(^uint32(0))
	if uint64(a.length) == max || a.storePositions && (token.Position < 0 || uint64(token.Position) > max) || a.storeOffsets && (token.StartOffset < 0 || token.EndOffset < token.StartOffset || uint64(token.EndOffset) > max) {
		return memtable.ErrCOWCapacity
	}
	idx := -1
	if a.lookup != nil {
		if found, ok := a.lookup[token.Term]; ok {
			idx = found
		}
	} else {
		for i := range a.terms {
			if a.terms[i].Term == token.Term {
				idx = i
				break
			}
		}
	}
	var term textAnalyzedTerm
	if idx >= 0 {
		term = a.terms[idx]
		if uint64(term.Frequency) == max {
			return memtable.ErrCOWCapacity
		}
	}
	var charge nativeTextAdmissionSum
	termCapacity, lookupCapacity := cap(a.terms), a.lookupCapacity
	if idx < 0 {
		var err error
		termCapacity, err = textTermGrowthCapacity(len(a.terms), cap(a.terms))
		if err != nil {
			return err
		}
		if termCapacity != cap(a.terms) {
			charge.array(uint64(termCapacity), uint64(unsafe.Sizeof(textAnalyzedTerm{})))
		}
		if len(a.terms) >= textTermAccumulatorInlineTerms && len(a.terms) >= lookupCapacity {
			if lookupCapacity == 0 {
				lookupCapacity = 2 * textTermAccumulatorInlineTerms
			} else {
				lookupCapacity, err = textTermGrowthCapacity(len(a.terms), lookupCapacity)
				if err != nil {
					return err
				}
			}
			// Established Swiss-map bound: headers, rounded groups and all
			// internal growth through this capacity. Each explicit rebuild is
			// independently charged; earlier maps stay on the same receipt.
			charge.mapEntries(uint64(lookupCapacity), uint64(unsafe.Sizeof(string("")))+uint64(unsafe.Sizeof(int(0))))
		}
	}
	positionCapacity, offsetCapacity := cap(term.Positions), cap(term.Offsets)
	var err error
	if a.storePositions {
		positionCapacity, err = textTermGrowthCapacity(len(term.Positions), positionCapacity)
		if err != nil {
			return err
		}
		if positionCapacity != cap(term.Positions) {
			charge.array(uint64(positionCapacity), uint64(unsafe.Sizeof(uint32(0))))
		}
	}
	if a.storeOffsets {
		offsetCapacity, err = textTermGrowthCapacity(len(term.Offsets), offsetCapacity)
		if err != nil {
			return err
		}
		if offsetCapacity != cap(term.Offsets) {
			charge.array(uint64(offsetCapacity), uint64(unsafe.Sizeof(textTokenOffset{})))
		}
	}
	if charge.bad {
		return memtable.ErrCOWCapacity
	}
	// With no new backing there is no allocation promise to acquire.
	if charge.total != 0 {
		if err := a.admit(charge.total); err != nil {
			return err
		}
	}
	if termCapacity != cap(a.terms) {
		terms := make([]textAnalyzedTerm, len(a.terms), termCapacity)
		copy(terms, a.terms)
		a.terms = terms
	}
	if lookupCapacity != a.lookupCapacity {
		lookup := make(map[string]int, lookupCapacity)
		for i := range a.terms {
			lookup[a.terms[i].Term] = i
		}
		a.lookup, a.lookupCapacity = lookup, lookupCapacity
	}
	if idx < 0 {
		idx = len(a.terms)
		a.terms = append(a.terms, textAnalyzedTerm{Term: token.Term})
		if a.lookup != nil {
			a.lookup[token.Term] = idx
		}
	}
	stored := &a.terms[idx]
	if a.storePositions {
		if positionCapacity != cap(stored.Positions) {
			positions := make([]uint32, len(stored.Positions), positionCapacity)
			copy(positions, stored.Positions)
			stored.Positions = positions
		}
		stored.Positions = append(stored.Positions, uint32(token.Position))
	}
	if a.storeOffsets {
		if offsetCapacity != cap(stored.Offsets) {
			offsets := make([]textTokenOffset, len(stored.Offsets), offsetCapacity)
			copy(offsets, stored.Offsets)
			stored.Offsets = offsets
		}
		stored.Offsets = append(stored.Offsets, textTokenOffset{Start: uint32(token.StartOffset), End: uint32(token.EndOffset)})
	}
	stored.Frequency++
	a.length++
	return nil
}
