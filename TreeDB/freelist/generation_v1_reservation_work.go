package freelist

import (
	"crypto/sha256"
	"encoding/binary"
	"hash"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
)

// reservationPageEncodeWorkV1 is the original reservation-chain encoder.
// Controls may be reused only after completion. Every output page remains
// independently owned by its candidate/writer; completing this control does
// not authorize resetting that page or its extent backing.
type reservationPageEncodeWorkV1 struct {
	pages                                         [][]byte
	record                                        *ReservationRecordV1
	id                                            uint64
	pageIndex, entryIndex, hashIndex, finishIndex int
	digest                                        hash.Hash
	phase                                         uint8
	firstErr                                      error
}

func (s *reservationPageEncodeWorkV1) step(id uint64, record *ReservationRecordV1, w *iterator.OrdinalScanWork) (bool, error) {
	if s.firstErr != nil {
		return false, s.firstErr
	}
	if s.record == nil {
		s.record, s.id = record, id
	}
	if s.record != record || s.id != id {
		return false, ErrGenerationFormat
	}
	ready, err := s.advance(id, record, w)
	if err != nil {
		s.firstErr = err
	}
	return ready, err
}

// The synchronous caller enters advance directly and borrows its local record;
// only step retains cross-call operand identity. This preserves the ordinary
// caller-owned controls and avoids a new escaping record/continuation.
func (s *reservationPageEncodeWorkV1) advance(id uint64, record *ReservationRecordV1, w *iterator.OrdinalScanWork) (bool, error) {
	if record == nil {
		return false, ErrGenerationFormat
	}
	for {
		switch s.phase {
		case 0:
			count := int(reservationPagesForEntries(uint64(len(record.Extents))))
			if count > int(^uint16(0)) || id > ^uint64(0)-uint64(count) {
				return false, ErrGenerationFormat
			}
			// The two original vectors are allocated once; filling their entries
			// and all actual chain work are admitted in the later stages.
			if ok, err := appendReserve(w, 1, uint64(count)*(8+uint64(unsafe.Sizeof([]byte{})))); !ok || err != nil {
				return false, err
			}
			record.pageID = id
			record.pageIDs = make([]uint64, count)
			s.pages = make([][]byte, count)
			s.phase = 1
		case 1:
			if s.pageIndex == len(s.pages) {
				s.phase = 3
				continue
			}
			if ok, err := appendReserve(w, 1, page.PageSize+reservationHeaderSize+8); !ok || err != nil {
				return false, err
			}
			b := make([]byte, page.PageSize)
			pageID := id + uint64(s.pageIndex)
			record.pageIDs[s.pageIndex] = pageID
			encodeReservationPageHeaderV1(b, pageID, s.pageIndex, len(s.pages), record)
			s.pages[s.pageIndex] = b
			s.entryIndex = s.pageIndex * reservationEntriesPerPage
			s.phase = 2
		case 2:
			end := min((s.pageIndex+1)*reservationEntriesPerPage, len(record.Extents))
			n, err := stateEncodeEntriesV1(w, end-s.entryIndex, reservationEntrySize+uint64(unsafe.Sizeof(ReservationExtentV1{})))
			if err != nil || n == 0 && s.entryIndex < end {
				return false, err
			}
			limit := s.entryIndex + n
			b := s.pages[s.pageIndex]
			for ; s.entryIndex < limit; s.entryIndex++ {
				o := reservationHeaderSize + (s.entryIndex-s.pageIndex*reservationEntriesPerPage)*reservationEntrySize
				encodeReservationExtentV1(b[o:o+reservationEntrySize], record.Extents[s.entryIndex])
			}
			if s.entryIndex != end {
				return false, nil
			}
			s.pageIndex++
			s.phase = 1
		case 3:
			if w == nil {
				// Preserve the original stack-local ordinary SHA256 driver.
				record.digest = reservationDigest(s.pages)
				s.phase = 5
				continue
			}
			// One standard fixed-page SHA256 primitive, including its small
			// context allocation, has a conservative complete-page byte charge.
			// No chain scan is hidden in this admission.
			if ok, err := appendReserve(w, 1, page.PageSize); !ok || err != nil {
				return false, err
			}
			s.digest = sha256.New()
			s.phase = 4
		case 4:
			if s.hashIndex == len(s.pages) {
				if ok, err := appendReserve(w, 1, sha256.BlockSize+sha256.Size); !ok || err != nil {
					return false, err
				}
				s.digest.Sum(record.digest[:0])
				s.digest = nil
				s.phase = 5
				continue
			}
			if ok, err := appendReserve(w, 1, page.PageSize+sha256.BlockSize); !ok || err != nil {
				return false, err
			}
			hashReservationPageV1(s.digest, s.pages[s.hashIndex])
			s.hashIndex++
		case 5:
			if s.finishIndex == len(s.pages) {
				s.phase = 6
				continue
			}
			if ok, err := appendReserve(w, 1, page.PageSize+sha256.Size); !ok || err != nil {
				return false, err
			}
			b := s.pages[s.finishIndex]
			copy(b[112:144], record.digest[:])
			finishPage(b)
			s.finishIndex++
		case 6:
			return true, nil
		default:
			return false, ErrGenerationFormat
		}
	}
}

func encodeReservationPageHeaderV1(b []byte, id uint64, index, count int, r *ReservationRecordV1) {
	start := index * reservationEntriesPerPage
	entries := min(reservationEntriesPerPage, len(r.Extents)-start)
	encodePageHeader(b, id, page.PageTypeFreelistReservation, uint16(entries))
	copy(b[16:24], reservationMagic[:])
	binary.LittleEndian.PutUint16(b[24:26], 1)
	binary.LittleEndian.PutUint16(b[26:28], reservationHeaderSize)
	binary.LittleEndian.PutUint16(b[28:30], reservationEntrySize)
	copy(b[32:48], r.CandidateID[:])
	binary.LittleEndian.PutUint64(b[48:56], r.GenerationID)
	binary.LittleEndian.PutUint64(b[56:64], r.BaseID)
	if index+1 < count {
		binary.LittleEndian.PutUint64(b[64:72], id+1)
	}
	binary.LittleEndian.PutUint32(b[72:76], uint32(entries))
	binary.LittleEndian.PutUint16(b[76:78], 1)
	binary.LittleEndian.PutUint16(b[78:80], uint16(count))
	copy(b[80:112], r.BaseDigest[:])
	binary.LittleEndian.PutUint32(b[144:148], uint32(index))
	binary.LittleEndian.PutUint32(b[148:152], uint32(len(r.Extents)))
}

func encodeReservationExtentV1(b []byte, e ReservationExtentV1) {
	binary.LittleEndian.PutUint64(b[:8], e.StartPageID)
	binary.LittleEndian.PutUint32(b[8:12], e.Count)
	b[12] = byte(e.Kind)
	binary.LittleEndian.PutUint64(b[16:24], e.LastReachableCommitSeq)
}

func hashReservationPageV1(h hash.Hash, b []byte) {
	var zeros [32]byte
	_, _ = h.Write(b[:8])
	_, _ = h.Write(zeros[:4])
	_, _ = h.Write(b[12:112])
	_, _ = h.Write(zeros[:])
	_, _ = h.Write(b[144:])
}
