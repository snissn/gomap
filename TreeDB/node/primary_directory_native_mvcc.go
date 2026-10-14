package node

import (
	"bytes"
	"encoding/binary"

	"github.com/snissn/gomap/TreeDB/page"
)

// searchPrimaryComponentWithVisit observes only actual binary-search operands.
// The immutable view has already passed complete canonical validation. A
// refused visit neither compares the unadmitted key nor changes output.
func (v PrimaryDirectoryView) searchPrimaryComponentWithVisit(key []byte, visit func(PrimaryDirectoryEntry) bool) (PrimaryDirectoryEntry, bool, bool, error) {
	lo, hi := 0, v.count
	for lo < hi {
		mid := lo + (hi-lo)/2
		observed, err := v.Entry(mid)
		if err != nil {
			return PrimaryDirectoryEntry{}, false, false, err
		}
		if visit != nil && !visit(observed) {
			return PrimaryDirectoryEntry{}, false, false, nil
		}
		comparison := bytes.Compare(observed.Key, key)
		if comparison == 0 {
			return observed, true, true, nil
		}
		if comparison < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return PrimaryDirectoryEntry{}, false, true, nil
}

// ReplacePrimaryComponentWithVisit is the maintained exact-key replacement
// operation with before-effect admission for each actual lookup operand. It
// keeps the original class and heap location; unrelated immutable components
// retain their physical identities. The caller admits the fixed page copy,
// component encoding and CRC before calling. Refusal leaves dst unchanged.
func (v PrimaryDirectoryView) ReplacePrimaryComponentWithVisit(dst []byte, id uint64, e PrimaryDirectoryEntry, visit func(PrimaryDirectoryEntry) bool) (PrimaryDirectoryView, bool, error) {
	if len(dst) != page.PageSize || id < 2 || !primaryComponentValid(e) || e.Revision == 0 || (e.Kind != PrimaryPut && e.Kind != PrimaryAbsence) {
		return PrimaryDirectoryView{}, false, ErrPrimaryDirectory
	}
	old, found, ready, err := v.searchPrimaryComponentWithVisit(e.Key, visit)
	if !ready || err != nil {
		return PrimaryDirectoryView{}, ready, err
	}
	if !found || e.ClassSlot != old.ClassSlot {
		return PrimaryDirectoryView{}, false, ErrPrimaryDirectory
	}
	copy(dst, v.data)
	binary.LittleEndian.PutUint64(dst[:8], id)
	rec := dst[PrimaryDirectoryRecordsOffset+int(e.ClassSlot)*PrimaryDirectoryEntrySize:][:PrimaryDirectoryEntrySize]
	encodePrimaryComponent(rec, e)
	page.UpdateChecksum(dst)
	return PrimaryDirectoryView{data: dst, count: v.count, heapEnd: v.heapEnd}, true, nil
}

// SearchPrimaryComponentWithVisit supplies the exact current-key predicate to
// its existing native constructor. It owns no remembered directory position or
// publisher; each visit is an observation of this current immutable view.
func (v PrimaryDirectoryView) SearchPrimaryComponentWithVisit(key []byte, visit func(PrimaryDirectoryEntry) bool) (PrimaryDirectoryEntry, bool, bool, error) {
	return v.searchPrimaryComponentWithVisit(key, visit)
}
