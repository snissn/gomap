package node

import (
	"bytes"
	"encoding/binary"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
)

// PrimaryDirectoryEncodeWorkV6 emits the maintained directory format from
// immutable producer-owned entries. dst remains private until the SAME arena
// constructor validates and seals it. Its parent admits this containing work.
type PrimaryDirectoryEncodeWorkV6 struct {
	dst          []byte
	entries      []PrimaryDirectoryEntry
	classes      [PrimaryDirectoryMaxEntries]int
	seen         [PrimaryDirectoryMaxEntries]bool
	id, sequence uint64
	base         PrimaryOperand
	index, heap  int
	phase        uint8
}

func (s *PrimaryDirectoryEncodeWorkV6) Step(dst []byte, id, sequence uint64, base PrimaryOperand, entries []PrimaryDirectoryEntry, w *iterator.OrdinalScanWork) (bool, error) {
	if s == nil || w == nil {
		return false, ErrPrimaryDirectory
	}
	if s.phase == 0 {
		if !w.Reserve(1, 2*page.PageSize+2*uint64(unsafe.Sizeof(*s))) {
			return false, nil
		}
		if len(dst) != page.PageSize || id < 2 || base.Ref.Kind != page.ChildRefPage || !primaryRefValid(base.Ref) || base.Digest == ([32]byte{}) || sequence == 0 || len(entries) > PrimaryDirectoryMaxEntries {
			return false, ErrPrimaryDirectory
		}
		s.dst, s.entries, s.id, s.sequence, s.base = dst, entries, id, sequence, base
		s.heap, s.phase = PrimaryDirectoryHeapOffset, 1
	}
	if len(dst) != page.PageSize || &dst[0] != &s.dst[0] || id != s.id || sequence != s.sequence || base != s.base || len(entries) != len(s.entries) || len(entries) > 0 && &entries[0] != &s.entries[0] {
		return false, ErrPrimaryDirectory
	}
	for {
		switch s.phase {
		case 1:
			for s.index < len(entries) {
				if !w.Reserve(1, 2*uint64(unsafe.Sizeof(PrimaryDirectoryEntry{}))+2*page.PageSize) {
					return false, nil
				}
				i := s.index
				e := entries[i]
				if int(e.ClassSlot) >= len(entries) || s.seen[e.ClassSlot] || i > 0 && bytes.Compare(entries[i-1].Key, e.Key) >= 0 || !primaryComponentValid(e) || e.Revision == 0 || e.Kind != PrimaryPut && e.Kind != PrimaryAbsence {
					return false, ErrPrimaryDirectory
				}
				if len(e.Key) > len(dst)-s.heap {
					return false, ErrPrimaryDirectoryFull
				}
				s.heap += len(e.Key)
				s.classes[e.ClassSlot] = i
				s.seen[e.ClassSlot] = true
				s.index++
			}
			s.phase = 2
		case 2:
			if !w.Reserve(1, 2*page.PageSize) {
				return false, nil
			}
			clear(dst)
			binary.LittleEndian.PutUint64(dst[:8], id)
			binary.LittleEndian.PutUint16(dst[12:14], uint16(page.PageTypePrimaryDirectory))
			binary.LittleEndian.PutUint16(dst[14:16], uint16(len(entries)))
			copy(dst[16:24], []byte("TDPRDIR3"))
			binary.LittleEndian.PutUint16(dst[24:26], primaryDirectoryVersion)
			binary.LittleEndian.PutUint16(dst[26:28], PrimaryDirectoryHeaderSize)
			binary.LittleEndian.PutUint64(dst[32:40], sequence)
			encodePrimaryOperand(dst[40:99], base)
			s.index, s.heap, s.phase = 0, PrimaryDirectoryHeapOffset, 3
		case 3:
			for s.index < len(entries) {
				if !w.Reserve(1, 2*uint64(unsafe.Sizeof(PrimaryDirectoryEntry{}))+2*PrimaryDirectoryEntrySize+2*page.PageSize) {
					return false, nil
				}
				slot := s.index
				e := entries[s.classes[slot]]
				rec := dst[PrimaryDirectoryRecordsOffset+slot*PrimaryDirectoryEntrySize:][:PrimaryDirectoryEntrySize]
				encodePrimaryComponent(rec, e)
				binary.LittleEndian.PutUint16(rec[49:51], uint16(s.heap))
				binary.LittleEndian.PutUint16(rec[51:53], uint16(len(e.Key)))
				copy(dst[s.heap:], e.Key)
				s.heap += len(e.Key)
				// The key-order table and class-order heap retain their original orders.
				dst[PrimaryDirectoryHeaderSize+slot] = byte(entries[slot].ClassSlot)
				s.index++
			}
			s.phase = 4
		case 4:
			if !w.Reserve(1, page.PageSize) {
				return false, nil
			}
			page.UpdateChecksum(dst)
			s.phase = 5
		case 5:
			return true, nil
		default:
			return false, ErrPrimaryDirectory
		}
	}
}
