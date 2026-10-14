package node

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/snissn/gomap/TreeDB/page"
)

// PrimaryDirectoryV2 is a complete primary operand, not a publication lineage.
// The base and every component are independently immutable. An absence component
// masks exactly its own key; it never changes the meanings of other keys.
const (
	PrimaryDirectoryHeaderSize    = 112
	PrimaryDirectoryEntrySize     = 56
	PrimaryDirectoryMaxEntries    = 49
	PrimaryDirectoryOrderSize     = 64
	PrimaryDirectoryRecordsOffset = PrimaryDirectoryHeaderSize + PrimaryDirectoryOrderSize
	PrimaryDirectoryHeapOffset    = PrimaryDirectoryRecordsOffset + PrimaryDirectoryMaxEntries*PrimaryDirectoryEntrySize
	primaryDirectoryVersion       = 3
)

var ErrPrimaryDirectory = errors.New("node: invalid primary directory")
var ErrPrimaryDirectoryFull = errors.New("node: primary directory capacity exhausted")

type PrimaryComponentKind uint8

const (
	PrimaryPut     PrimaryComponentKind = 1
	PrimaryAbsence PrimaryComponentKind = 2
)

// PrimaryOperand names exact independently recoverable physical bytes. Digest
// binds the complete logical page, including its native page CRC.
type PrimaryOperand struct {
	Ref    page.ChildRef
	Digest [32]byte
}

func (e PrimaryDirectoryEntry) InlineAbsence() bool {
	return e.Kind == PrimaryAbsence && e.Operand == (PrimaryOperand{})
}
func primaryComponentValid(e PrimaryDirectoryEntry) bool {
	return e.InlineAbsence() || (e.Operand.Ref.Kind == page.ChildRefPage && primaryRefValid(e.Operand.Ref) && e.Operand.Digest != ([32]byte{}))
}

type PrimaryDirectoryEntry struct {
	Key      []byte
	Operand  PrimaryOperand
	Revision page.EntryRevision
	Kind     PrimaryComponentKind
	// ClassSlot is stable across exact-key replacement and sorted insertion.
	ClassSlot uint16
}

// PrimaryDirectoryView borrows one checksummed immutable directory page.
// Views may not outlive the captured index owner.
type PrimaryDirectoryView struct {
	data    []byte
	count   int
	heapEnd int
}

func primaryRefValid(r page.ChildRef) bool {
	switch r.Kind {
	case page.ChildRefPage:
		return r.Page >= 2 && r.Log == (page.LogRecordRef{})
	case page.ChildRefLeafLog:
		return r.Page == 0 && r.Log.RecordLength() != 0
	default:
		return false
	}
}

func encodePrimaryOperand(dst []byte, op PrimaryOperand) {
	dst[0] = byte(op.Ref.Kind)
	binary.LittleEndian.PutUint64(dst[1:9], op.Ref.Page)
	page.EncodeLogRecordRef(dst[9:27], op.Ref.Log)
	copy(dst[27:59], op.Digest[:])
}
func decodePrimaryOperand(src []byte) PrimaryOperand {
	op := PrimaryOperand{Ref: page.ChildRef{Kind: page.ChildRefKind(src[0]), Page: binary.LittleEndian.Uint64(src[1:9]), Log: page.DecodeLogRecordRef(src[9:27])}}
	copy(op.Digest[:], src[27:59])
	return op
}

// EncodePrimaryDirectory emits one complete page with a sorted key-order
// table and stable physical component classes. Classes are a permutation of
// [0,count); they do not refer to previous directories or replay state.
func EncodePrimaryDirectory(dst []byte, id, baseSequence uint64, base PrimaryOperand, entries []PrimaryDirectoryEntry) error {
	if len(dst) != page.PageSize || id < 2 || base.Ref.Kind != page.ChildRefPage || !primaryRefValid(base.Ref) || base.Digest == ([32]byte{}) || baseSequence == 0 || len(entries) > PrimaryDirectoryMaxEntries {
		return ErrPrimaryDirectory
	}
	var byClass [PrimaryDirectoryMaxEntries]*PrimaryDirectoryEntry
	end := PrimaryDirectoryHeapOffset
	for i := range entries {
		e := &entries[i]
		if int(e.ClassSlot) >= len(entries) || byClass[e.ClassSlot] != nil || (i > 0 && bytes.Compare(entries[i-1].Key, e.Key) >= 0) || !primaryComponentValid(*e) || e.Revision == 0 || (e.Kind != PrimaryPut && e.Kind != PrimaryAbsence) {
			return ErrPrimaryDirectory
		}
		if len(e.Key) > len(dst)-end {
			return ErrPrimaryDirectoryFull
		}
		end += len(e.Key)
		byClass[e.ClassSlot] = e
	}
	clear(dst)
	binary.LittleEndian.PutUint64(dst[:8], id)
	binary.LittleEndian.PutUint16(dst[12:14], uint16(page.PageTypePrimaryDirectory))
	binary.LittleEndian.PutUint16(dst[14:16], uint16(len(entries)))
	copy(dst[16:24], []byte("TDPRDIR3"))
	binary.LittleEndian.PutUint16(dst[24:26], primaryDirectoryVersion)
	binary.LittleEndian.PutUint16(dst[26:28], PrimaryDirectoryHeaderSize)
	binary.LittleEndian.PutUint64(dst[32:40], baseSequence)
	encodePrimaryOperand(dst[40:99], base)
	for i := range entries {
		dst[PrimaryDirectoryHeaderSize+i] = byte(entries[i].ClassSlot)
	}
	heap := PrimaryDirectoryHeapOffset
	for slot := 0; slot < len(entries); slot++ {
		e := byClass[slot]
		rec := dst[PrimaryDirectoryRecordsOffset+slot*PrimaryDirectoryEntrySize:][:PrimaryDirectoryEntrySize]
		encodePrimaryComponent(rec, *e)
		binary.LittleEndian.PutUint16(rec[49:51], uint16(heap))
		binary.LittleEndian.PutUint16(rec[51:53], uint16(len(e.Key)))
		copy(dst[heap:], e.Key)
		heap += len(e.Key)
	}
	page.UpdateChecksum(dst)
	return nil
}
func encodePrimaryComponent(rec []byte, e PrimaryDirectoryEntry) {
	binary.LittleEndian.PutUint64(rec[:8], e.Operand.Ref.Page)
	copy(rec[8:40], e.Operand.Digest[:])
	binary.LittleEndian.PutUint64(rec[40:48], uint64(e.Revision))
	rec[48] = byte(e.Kind)
}
func decodePrimaryComponent(rec []byte) PrimaryDirectoryEntry {
	id := binary.LittleEndian.Uint64(rec[:8])
	ref := page.ChildRef{}
	if id != 0 {
		ref = page.PageChildRef(id)
	}
	e := PrimaryDirectoryEntry{Operand: PrimaryOperand{Ref: ref}, Revision: page.EntryRevision(binary.LittleEndian.Uint64(rec[40:48])), Kind: PrimaryComponentKind(rec[48])}
	copy(e.Operand.Digest[:], rec[8:40])
	return e
}

// DecodePrimaryDirectory validates canonical class ownership, exact key order,
// contiguous class-ordered key heap and all reserved bytes before borrowing.
func DecodePrimaryDirectory(src []byte) (PrimaryDirectoryView, error) {
	bad := func() (PrimaryDirectoryView, error) { return PrimaryDirectoryView{}, ErrPrimaryDirectory }
	if len(src) != page.PageSize || !page.VerifyChecksumNonMutating(src) || binary.LittleEndian.Uint64(src[:8]) < 2 || binary.LittleEndian.Uint16(src[12:14]) != uint16(page.PageTypePrimaryDirectory) || !bytes.Equal(src[16:24], []byte("TDPRDIR3")) || binary.LittleEndian.Uint16(src[24:26]) != primaryDirectoryVersion || binary.LittleEndian.Uint16(src[26:28]) != PrimaryDirectoryHeaderSize || binary.LittleEndian.Uint64(src[32:40]) == 0 {
		return bad()
	}
	count := int(binary.LittleEndian.Uint16(src[14:16]))
	if count > PrimaryDirectoryMaxEntries {
		return bad()
	}
	for _, r := range [][2]int{{28, 32}, {99, PrimaryDirectoryHeaderSize}, {PrimaryDirectoryHeaderSize + count, PrimaryDirectoryRecordsOffset}, {PrimaryDirectoryRecordsOffset + count*PrimaryDirectoryEntrySize, PrimaryDirectoryHeapOffset}} {
		for _, b := range src[r[0]:r[1]] {
			if b != 0 {
				return bad()
			}
		}
	}
	base := decodePrimaryOperand(src[40:99])
	if base.Ref.Kind != page.ChildRefPage || !primaryRefValid(base.Ref) || base.Digest == ([32]byte{}) {
		return bad()
	}
	v := PrimaryDirectoryView{data: src, count: count}
	var seen [PrimaryDirectoryMaxEntries]bool
	for i := 0; i < count; i++ {
		slot := int(src[PrimaryDirectoryHeaderSize+i])
		if slot >= count || seen[slot] {
			return bad()
		}
		seen[slot] = true
	}
	heap := PrimaryDirectoryHeapOffset
	for slot := 0; slot < count; slot++ {
		rec := v.classRecord(slot)
		off := int(binary.LittleEndian.Uint16(rec[49:51]))
		n := int(binary.LittleEndian.Uint16(rec[51:53]))
		if off != heap || n > len(src)-heap {
			return bad()
		}
		e := decodePrimaryComponent(rec)
		if !primaryComponentValid(e) || e.Revision == 0 || (e.Kind != PrimaryPut && e.Kind != PrimaryAbsence) {
			return bad()
		}
		for _, b := range rec[53:] {
			if b != 0 {
				return bad()
			}
		}
		heap += n
	}
	v.heapEnd = heap
	var prior []byte
	for i := 0; i < count; i++ {
		e, _ := v.Entry(i)
		if i > 0 && bytes.Compare(prior, e.Key) >= 0 {
			return bad()
		}
		prior = e.Key
	}
	for _, b := range src[heap:] {
		if b != 0 {
			return bad()
		}
	}
	return v, nil
}
func (v PrimaryDirectoryView) classRecord(slot int) []byte {
	return v.data[PrimaryDirectoryRecordsOffset+slot*PrimaryDirectoryEntrySize:][:PrimaryDirectoryEntrySize]
}
func (v PrimaryDirectoryView) Count() int { return v.count }
func (v PrimaryDirectoryView) Base() (PrimaryOperand, uint64) {
	return decodePrimaryOperand(v.data[40:99]), binary.LittleEndian.Uint64(v.data[32:40])
}
func (v PrimaryDirectoryView) Entry(i int) (PrimaryDirectoryEntry, error) {
	if i < 0 || i >= v.count {
		return PrimaryDirectoryEntry{}, ErrPrimaryDirectory
	}
	slot := int(v.data[PrimaryDirectoryHeaderSize+i])
	rec := v.classRecord(slot)
	e := decodePrimaryComponent(rec)
	off := int(binary.LittleEndian.Uint16(rec[49:51]))
	n := int(binary.LittleEndian.Uint16(rec[51:53]))
	e.Key = v.data[off : off+n]
	e.ClassSlot = uint16(slot)
	return e, nil
}

// ClassEntry observes one exact physical class in a validated complete page.
// It does not establish that a caller's remembered key still owns that class.
func (v PrimaryDirectoryView) ClassEntry(slot uint16) (PrimaryDirectoryEntry, error) {
	if int(slot) >= v.count {
		return PrimaryDirectoryEntry{}, ErrPrimaryDirectory
	}
	rec := v.classRecord(int(slot))
	e := decodePrimaryComponent(rec)
	off := int(binary.LittleEndian.Uint16(rec[49:51]))
	n := int(binary.LittleEndian.Uint16(rec[51:53]))
	e.Key = v.data[off : off+n]
	e.ClassSlot = slot
	return e, nil
}
func (v PrimaryDirectoryView) Search(key []byte) (PrimaryDirectoryEntry, bool) {
	lo, hi := 0, v.count
	for lo < hi {
		mid := lo + (hi-lo)/2
		e, _ := v.Entry(mid)
		if bytes.Compare(e.Key, key) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == v.count {
		return PrimaryDirectoryEntry{}, false
	}
	e, _ := v.Entry(lo)
	return e, bytes.Equal(e.Key, key)
}

// InsertPrimaryComponentWithVisit copies a complete immutable certificate,
// appends one stable class and its canonical key heap cell, and shifts only the
// byte order table. visit admits each actual negative-lookup operand before its
// key is compared. It must not mutate either borrowed operand. A refusal leaves
// dst unchanged; existing exact keys require replacement rather than insertion.
func (v PrimaryDirectoryView) InsertPrimaryComponentWithVisit(dst []byte, id uint64, e PrimaryDirectoryEntry, visit func(PrimaryDirectoryEntry) bool) (PrimaryDirectoryView, bool, error) {
	if len(dst) != page.PageSize || id < 2 || int(e.ClassSlot) != v.count || !primaryComponentValid(e) || e.Revision == 0 || (e.Kind != PrimaryPut && e.Kind != PrimaryAbsence) {
		return PrimaryDirectoryView{}, false, ErrPrimaryDirectory
	}
	if v.count >= PrimaryDirectoryMaxEntries || len(e.Key) > page.PageSize-v.heapEnd {
		return PrimaryDirectoryView{}, false, ErrPrimaryDirectoryFull
	}
	lo, hi := 0, v.count
	for lo < hi {
		mid := lo + (hi-lo)/2
		observed, err := v.Entry(mid)
		if err != nil {
			return PrimaryDirectoryView{}, false, err
		}
		if visit != nil && !visit(observed) {
			return PrimaryDirectoryView{}, false, nil
		}
		comparison := bytes.Compare(observed.Key, e.Key)
		if comparison == 0 {
			return PrimaryDirectoryView{}, false, ErrPrimaryDirectory
		}
		if comparison < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	copy(dst, v.data)
	binary.LittleEndian.PutUint64(dst[:8], id)
	binary.LittleEndian.PutUint16(dst[14:16], uint16(v.count+1))
	order := dst[PrimaryDirectoryHeaderSize : PrimaryDirectoryHeaderSize+v.count+1]
	copy(order[lo+1:], order[lo:v.count])
	order[lo] = byte(e.ClassSlot)
	rec := dst[PrimaryDirectoryRecordsOffset+v.count*PrimaryDirectoryEntrySize:][:PrimaryDirectoryEntrySize]
	encodePrimaryComponent(rec, e)
	binary.LittleEndian.PutUint16(rec[49:51], uint16(v.heapEnd))
	binary.LittleEndian.PutUint16(rec[51:53], uint16(len(e.Key)))
	copy(dst[v.heapEnd:], e.Key)
	page.UpdateChecksum(dst)
	return PrimaryDirectoryView{data: dst, count: v.count + 1, heapEnd: v.heapEnd + len(e.Key)}, true, nil
}

// ReplacePrimaryComponent copies a previously fully validated immutable page
// and changes exactly one existing key's component. Stable classes avoid
// relocating unrelated component custody. The returned view derives from the
// complete old certificate plus the checked exact replacement, never ancestry.
func (v PrimaryDirectoryView) ReplacePrimaryComponent(dst []byte, id uint64, e PrimaryDirectoryEntry) (PrimaryDirectoryView, error) {
	old, found := v.Search(e.Key)
	if !found || len(dst) != page.PageSize || id < 2 || e.ClassSlot != old.ClassSlot || !primaryComponentValid(e) || e.Revision == 0 || (e.Kind != PrimaryPut && e.Kind != PrimaryAbsence) {
		return PrimaryDirectoryView{}, ErrPrimaryDirectory
	}
	copy(dst, v.data)
	binary.LittleEndian.PutUint64(dst[:8], id)
	rec := dst[PrimaryDirectoryRecordsOffset+int(e.ClassSlot)*PrimaryDirectoryEntrySize:][:PrimaryDirectoryEntrySize]
	encodePrimaryComponent(rec, e)
	page.UpdateChecksum(dst)
	return PrimaryDirectoryView{data: dst, count: v.count, heapEnd: v.heapEnd}, nil
}
func VerifyPrimaryOperand(op PrimaryOperand, data []byte) bool {
	return len(data) == page.PageSize && page.VerifyChecksumNonMutating(data) && sha256.Sum256(data) == op.Digest
}

// ValidatePrimaryComponent rejects a validly checksummed component whose
// logical cell disagrees with its directory entry. Absence is a native
// revision-bearing tombstone, not an instruction to replay a mutation.
func ValidatePrimaryComponent(entry PrimaryDirectoryEntry, data []byte) error {
	if !VerifyPrimaryOperand(entry.Operand, data) {
		return ErrPrimaryDirectory
	}
	n := NewNode(data)
	if n.Type() != page.PageTypeLeaf || n.Count() != 1 {
		return ErrPrimaryDirectory
	}
	k, _, _, flags, revision, err := n.GetLeafEntryViewWithRevision(0)
	if err != nil || !bytes.Equal(k, entry.Key) || revision != entry.Revision ||
		(entry.Kind == PrimaryAbsence) != (flags&FlagTombstone != 0) {
		return ErrPrimaryDirectory
	}
	if entry.Operand.Ref.Kind == page.ChildRefPage && page.DecodeHeader(data).PageID != entry.Operand.Ref.Page {
		return ErrPrimaryDirectory
	}
	return nil
}

// WithPhysicalImage transfers a validated certificate onto byte-identical
// immutable physical storage. Construction checks equality before exposure.
func (v PrimaryDirectoryView) WithPhysicalImage(image []byte) PrimaryDirectoryView {
	if len(image) != page.PageSize || !bytes.Equal(v.data, image) {
		panic("node: primary physical certificate mismatch")
	}
	return PrimaryDirectoryView{data: image, count: v.count, heapEnd: v.heapEnd}
}
