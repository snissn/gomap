package collections

import (
	"hash/maphash"
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
	a.nativeLookup = nil
	return out
}

func (a *textTermAccumulator) textNormalizerAdmission() func(uint64) error {
	return a.admit
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

// addAdmittedToken uses the maintained analyzer's failable sink. Input and
// options/conversion promises are separately admitted BEFORE analysis. The same
// admission callback also governs Builder growth before WriteRune. Every
// concrete new backing here is admitted before make;
// no old backing credit is returned, even after successful replacement.
func (a *textTermAccumulator) addAdmittedToken(token TextToken) error {
	const max = uint64(^uint32(0))
	if uint64(a.length) == max || a.storePositions && (token.Position < 0 || uint64(token.Position) > max) || a.storeOffsets && (token.StartOffset < 0 || token.EndOffset < token.StartOffset || uint64(token.EndOffset) > max) {
		return memtable.ErrCOWCapacity
	}
	idx := -1
	if a.nativeLookup != nil {
		if found, ok := a.nativeLookup.lookup(token.Term); ok {
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
	termCapacity := cap(a.terms)
	if idx < 0 {
		var err error
		termCapacity, err = textTermGrowthCapacity(len(a.terms), cap(a.terms))
		if err != nil {
			return err
		}
		if termCapacity != cap(a.terms) {
			charge.array(uint64(termCapacity), uint64(unsafe.Sizeof(textAnalyzedTerm{})))
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
	// Every replacement lane and any index growth is admitted before effects.
	// The small inline lookup remains linear until the ninth distinct term.
	if idx < 0 && len(a.terms) >= textTermAccumulatorInlineTerms {
		if a.nativeLookup == nil {
			// Header/cells plus all prospective lanes are one refusal boundary.
			initial, err := localIDIndexCharge(len(a.terms) + 1)
			if err != nil || initial > ^uint64(0)-charge.total {
				return memtable.ErrCOWCapacity
			}
			if err := a.admit(initial + charge.total); err != nil {
				return err
			}
			// Construction uses the already admitted exact initial cell cohort. Install
			// the actual callback before any later growth can occur.
			capacity, _ := localIDCapacity(len(a.terms) + 1)
			a.nativeLookup = &localIDIndex{cells: make([]localIDCell, capacity), admit: a.admit, seed: maphash.MakeSeed()}
			for i := range a.terms {
				a.nativeLookup.insert(a.terms[i].Term, i)
			}
		} else if err := a.nativeLookup.prepare(charge.total); err != nil {
			return err
		}
	} else if charge.total != 0 {
		if err := a.admit(charge.total); err != nil {
			return err
		}
	}

	if termCapacity != cap(a.terms) {
		terms := make([]textAnalyzedTerm, len(a.terms), termCapacity)
		copy(terms, a.terms)
		a.terms = terms
	}
	if idx < 0 {
		idx = len(a.terms)
		a.terms = append(a.terms, textAnalyzedTerm{Term: token.Term})
		if a.nativeLookup != nil {
			a.nativeLookup.insert(token.Term, idx)
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
