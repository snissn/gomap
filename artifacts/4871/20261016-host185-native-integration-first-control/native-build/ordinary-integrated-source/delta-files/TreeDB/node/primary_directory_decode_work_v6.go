package node

import (
	"bytes"
	"encoding/binary"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
)

// PrimaryDirectoryDecodeWorkV6 validates the original directory format one
// record at a time. Its original parent retains immutable src through Result.
// It acquires no physical edge and manufactures no publication certificate.
type PrimaryDirectoryDecodeWorkV6 struct {
	view        PrimaryDirectoryView
	seen        [PrimaryDirectoryMaxEntries]bool
	prior       []byte
	index, heap int
	phase       uint8
	failure     error
}

func (s *PrimaryDirectoryDecodeWorkV6) Step(src []byte, w *iterator.OrdinalScanWork) (bool, error) {
	if s == nil || w == nil {
		return false, ErrPrimaryDirectory
	}
	if s.failure != nil {
		return false, s.failure
	}
	ready, err := s.step(src, w)
	if err != nil {
		s.failure = err
	}
	return ready, err
}
func (s *PrimaryDirectoryDecodeWorkV6) step(src []byte, w *iterator.OrdinalScanWork) (bool, error) {
	if s.phase == 0 {
		if !w.Reserve(1, 2*page.PageSize+2*uint64(unsafe.Sizeof(*s))) {
			return false, nil
		}
		if len(src) != page.PageSize || !page.VerifyChecksumNonMutating(src) || binary.LittleEndian.Uint64(src[:8]) < 2 || binary.LittleEndian.Uint16(src[12:14]) != uint16(page.PageTypePrimaryDirectory) || !bytes.Equal(src[16:24], []byte("TDPRDIR3")) || binary.LittleEndian.Uint16(src[24:26]) != primaryDirectoryVersion || binary.LittleEndian.Uint16(src[26:28]) != PrimaryDirectoryHeaderSize || binary.LittleEndian.Uint64(src[32:40]) == 0 {
			return false, ErrPrimaryDirectory
		}
		count := int(binary.LittleEndian.Uint16(src[14:16]))
		if count > PrimaryDirectoryMaxEntries {
			return false, ErrPrimaryDirectory
		}
		for _, r := range [4][2]int{{28, 32}, {99, PrimaryDirectoryHeaderSize}, {PrimaryDirectoryHeaderSize + count, PrimaryDirectoryRecordsOffset}, {PrimaryDirectoryRecordsOffset + count*PrimaryDirectoryEntrySize, PrimaryDirectoryHeapOffset}} {
			for _, b := range src[r[0]:r[1]] {
				if b != 0 {
					return false, ErrPrimaryDirectory
				}
			}
		}
		base := decodePrimaryOperand(src[40:99])
		if base.Ref.Kind != page.ChildRefPage || !primaryRefValid(base.Ref) || base.Digest == ([32]byte{}) {
			return false, ErrPrimaryDirectory
		}
		s.view = PrimaryDirectoryView{data: src, count: count}
		s.heap, s.phase = PrimaryDirectoryHeapOffset, 1
	}
	if len(src) != page.PageSize || &src[0] != &s.view.data[0] {
		return false, ErrPrimaryDirectory
	}
	for {
		switch s.phase {
		case 1:
			for s.index < s.view.count {
				if !w.Reserve(1, 2*uint64(unsafe.Sizeof(int(0)))+2) {
					return false, nil
				}
				slot := int(src[PrimaryDirectoryHeaderSize+s.index])
				if slot >= s.view.count || s.seen[slot] {
					return false, ErrPrimaryDirectory
				}
				s.seen[slot] = true
				s.index++
			}
			s.index, s.phase = 0, 2
		case 2:
			for s.index < s.view.count {
				if !w.Reserve(1, 2*PrimaryDirectoryEntrySize+2*uint64(unsafe.Sizeof(PrimaryDirectoryEntry{}))) {
					return false, nil
				}
				rec := s.view.classRecord(s.index)
				off, n := int(binary.LittleEndian.Uint16(rec[49:51])), int(binary.LittleEndian.Uint16(rec[51:53]))
				if off != s.heap || n > len(src)-s.heap {
					return false, ErrPrimaryDirectory
				}
				e := decodePrimaryComponent(rec)
				if !primaryComponentValid(e) || e.Revision == 0 || (e.Kind != PrimaryPut && e.Kind != PrimaryAbsence) {
					return false, ErrPrimaryDirectory
				}
				for _, b := range rec[53:] {
					if b != 0 {
						return false, ErrPrimaryDirectory
					}
				}
				s.heap += n
				s.index++
			}
			s.view.heapEnd = s.heap
			s.index, s.phase = 0, 3
		case 3:
			for s.index < s.view.count {
				// Every key is bounded by this one admitted page. The exact spans are
				// derived only after the fixed entry observation is admitted.
				if !w.Reserve(1, 2*PrimaryDirectoryEntrySize+2*page.PageSize) {
					return false, nil
				}
				e, err := s.view.Entry(s.index)
				if err != nil || (s.index > 0 && bytes.Compare(s.prior, e.Key) >= 0) {
					return false, ErrPrimaryDirectory
				}
				s.prior = e.Key
				s.index++
			}
			s.phase = 4
		case 4:
			if !w.Reserve(1, page.PageSize) {
				return false, nil
			}
			for _, b := range src[s.heap:] {
				if b != 0 {
					return false, ErrPrimaryDirectory
				}
			}
			s.prior = nil
			s.phase = 5
		case 5:
			return true, nil
		default:
			return false, ErrPrimaryDirectory
		}
	}
}
func (s *PrimaryDirectoryDecodeWorkV6) Result() (PrimaryDirectoryView, bool) {
	if s == nil || s.phase != 5 || s.failure != nil {
		return PrimaryDirectoryView{}, false
	}
	return s.view, true
}
