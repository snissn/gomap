package freelist

import (
	"encoding/binary"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
)

// candidateOutputWorkV1 is the original post-claim materialization pipeline.
// Its containing original constructor owns the frozen private transaction and
// ledger reservation. No allocator prepared slot or live transaction is held.
// Scalar controls may be reused after completion; pages, extents and descriptors
// remain candidate-owned until the actual last consumer releases them.
type candidateOutputWorkV1 struct {
	txn                                                  *FreelistTxn
	recorded                                             *recordingSink
	input                                                []ReservationExtentV1
	generationID, commitSeq, start, reserved, statePages uint64
	next, reservationID, headerID, chainPages            uint64
	index                                                int
	phase                                                uint8
	bound, empty                                         bool
	emission                                             statePageEmissionWorkV1
	emptyEncoder                                         statePageEncodeWorkV1
	emptyPage                                            []byte
	finalized                                            reservationFinalizeWorkV1
	record                                               ReservationRecordV1
	reservationEncoder                                   reservationPageEncodeWorkV1
	generation                                           *FreelistGenerationV1
	header                                               []byte
	dirty                                                []uint64
	result                                               *FreelistCandidateV1
	firstErr                                             error
}

func (s *candidateOutputWorkV1) step(t *FreelistTxn, generationID, commitSeq, start, reserved, statePages uint64, input []ReservationExtentV1, recorded *recordingSink, w *iterator.OrdinalScanWork) (*FreelistCandidateV1, bool, error) {
	if s.firstErr != nil {
		return nil, false, s.firstErr
	}
	if t == nil || !t.consumed || recorded == nil {
		return nil, false, ErrGenerationFormat
	}
	if w != nil {
		if concrete, ok := recorded.sink.(*CandidatePageSinkV1); !ok || concrete == nil || recorded.beforeFirstWrite != nil {
			return nil, false, ErrGenerationFormat
		}
	}
	if !s.bound {
		s.txn, s.recorded, s.input = t, recorded, input
		s.generationID, s.commitSeq, s.start, s.reserved, s.statePages = generationID, commitSeq, start, reserved, statePages
		s.bound = true
	}
	if s.txn != t || s.recorded != recorded || s.generationID != generationID || s.commitSeq != commitSeq || s.start != start || s.reserved != reserved || s.statePages != statePages || len(s.input) != len(input) || len(input) != 0 && &s.input[0] != &input[0] {
		return nil, false, ErrGenerationFormat
	}
	ready, err := s.advance(t, generationID, commitSeq, start, reserved, statePages, input, recorded, w)
	if err != nil {
		s.firstErr = err
	}
	return s.result, ready, err
}

// advance borrows synchronous operands. Only step retains those identities;
// ordinary MaterializeCandidate therefore avoids the emission escape regression.
func (s *candidateOutputWorkV1) advance(t *FreelistTxn, generationID, commitSeq, start, reserved, statePages uint64, input []ReservationExtentV1, recorded *recordingSink, w *iterator.OrdinalScanWork) (bool, error) {
	for {
		switch s.phase {
		case 0:
			rawCount := uint64(len(input)) + 1
			if start > t.highWater {
				rawCount += (start-t.highWater-1)/uint64(^uint32(0)) + 1
			}
			s.chainPages = reservationPagesForEntries(rawCount)
			if s.chainPages > uint64(^uint16(0)) || statePages > ^uint64(0)-s.chainPages-1 || statePages+s.chainPages+1 != reserved || start > ^uint64(0)-reserved {
				return false, ErrGenerationFormat
			}
			s.next, s.empty = start, t.root.freeCount+t.root.retiredCount == 0
			s.phase = 1
		case 1:
			if !s.empty {
				var ready bool
				var err error
				if w == nil {
					if !s.emission.begun {
						s.emission.frames[0].node, s.emission.depth, s.emission.begun = t.root, 1, true
					}
					ready, err = s.emission.advance(generationID, &s.next, recorded, nil)
				} else {
					ready, err = s.emission.step(t.root, generationID, &s.next, recorded, w)
				}
				if !ready || err != nil {
					return false, err
				}
				s.phase = 4
				continue
			}
			if ok, err := appendReserve(w, 1, page.PageSize+16+uint64(unsafe.Sizeof(statePageEncodeWorkV1{}))); !ok || err != nil {
				return false, err
			}
			s.emptyPage = make([]byte, page.PageSize)
			t.root.pageID = s.next
			s.next++
			s.phase = 2
		case 2:
			ready, err := s.emptyEncoder.indexPage(s.emptyPage, t.root.pageID, generationID, t.root, w)
			if !ready || err != nil {
				return false, err
			}
			t.root.checksum = binary.LittleEndian.Uint32(s.emptyPage[8:12])
			s.phase = 3
		case 3:
			if ready, err := recorded.writeWithWork(t.root.pageID, s.emptyPage, w); !ready || err != nil {
				return false, err
			}
			s.emptyPage = nil
			s.phase = 4
		case 4:
			s.reservationID = s.next
			s.headerID = s.reservationID + s.chainPages
			s.next = s.headerID + 1
			if s.next-start != reserved {
				return false, ErrGenerationFormat
			}
			s.phase = 5
		case 5:
			var ready bool
			var err error
			if w == nil {
				ready, err = s.finalized.advance(input, t.highWater, start, reserved, nil)
			} else {
				ready, err = s.finalized.step(input, t.highWater, start, reserved, w)
			}
			if !ready || err != nil {
				return false, err
			}
			s.phase = 6
		case 6:
			if ok, err := appendReserve(w, 1, uint64(unsafe.Sizeof(ReservationRecordV1{}))); !ok || err != nil {
				return false, err
			}
			s.record = ReservationRecordV1{CandidateID: recorded.candidateID, GenerationID: generationID, BaseID: t.base.generationID, BaseDigest: t.base.ref.Digest, Extents: s.finalized.entries}
			s.phase = 7
		case 7:
			var ready bool
			var err error
			if w == nil {
				ready, err = s.reservationEncoder.advance(s.reservationID, &s.record, nil)
			} else {
				ready, err = s.reservationEncoder.step(s.reservationID, &s.record, w)
			}
			if !ready || err != nil {
				return false, err
			}
			s.index, s.phase = 0, 8
		case 8:
			for s.index < len(s.reservationEncoder.pages) {
				if ready, err := recorded.writeWithWork(s.reservationID+uint64(s.index), s.reservationEncoder.pages[s.index], w); !ready || err != nil {
					return false, err
				}
				s.index++
			}
			s.phase = 9
		case 9:
			if ok, err := appendReserve(w, 1, uint64(unsafe.Sizeof(FreelistGenerationV1{}))+reserved*8); !ok || err != nil {
				return false, err
			}
			s.generation = &FreelistGenerationV1{generationID: generationID, commitSeq: commitSeq, parentGenerationID: t.base.generationID, parentCommitSeq: t.base.commitSeq, highWater: max(t.highWater, s.next), root: t.root, record: s.record, metadataPages: make([]uint64, int(reserved))}
			s.index, s.phase = 0, 10
		case 10:
			n, err := stateEncodeEntriesV1(w, len(s.generation.metadataPages)-s.index, 16)
			if err != nil || n == 0 && s.index < len(s.generation.metadataPages) {
				return false, err
			}
			end := s.index + n
			for ; s.index < end; s.index++ {
				s.generation.metadataPages[s.index] = start + uint64(s.index)
			}
			if s.index < len(s.generation.metadataPages) {
				return false, nil
			}
			s.phase = 11
		case 11:
			// Original fixed-page header, canonical header SHA and page CRC;
			// the data-dependent pending-range count was accumulated at phase5.
			if ok, err := appendReserve(w, 1, page.PageSize+2*generationHeaderSize); !ok || err != nil {
				return false, err
			}
			s.header = encodeGenerationPageWithPendingCountV1(s.headerID, s.generation, t.root.checksum, s.finalized.pendingCount)
			s.phase = 12
		case 12:
			if ready, err := recorded.writeWithWork(s.headerID, s.header, w); !ready || err != nil {
				return false, err
			}
			s.phase = 13
		case 13:
			if ok, err := appendReserve(w, 1, 32+uint64(unsafe.Sizeof(GenerationRefV1{}))+uint64(unsafe.Sizeof(FreelistTxnStats{}))+uint64(len(recorded.pages))*8); !ok || err != nil {
				return false, err
			}
			g := s.generation
			copy(g.ref.Digest[:], s.header[152:184])
			g.ref = GenerationRefV1{HeaderPageID: s.headerID, GenerationID: generationID, CommitSeq: commitSeq, HighWater: g.highWater, Digest: g.ref.Digest}
			t.stats.LogicalDelta, t.stats.COWChunks = uint64(len(t.changedChunks)), uint64(len(t.changedChunks))
			t.stats.COWPages = uint64(len(recorded.pages))
			t.stats.COWBytes = t.stats.COWPages * page.PageSize
			if _, owns := recorded.sink.(*CandidatePageSinkV1); !owns {
				t.stats.CandidatePageIsolationCopies, t.stats.CandidatePageIsolationBytes = t.stats.COWPages, t.stats.COWBytes
			}
			t.stats.FreeIDs, t.stats.RetiredIDs = g.root.freeCount, g.root.retiredCount
			t.stats.GenerationID, t.stats.ReservationRecords = generationID, uint64(len(s.record.pageIDs))
			t.stats.Reservations = uint64(len(t.allocated)) + reserved
			t.stats.PendingMetadataRetirements = s.finalized.pendingCount
			s.dirty = make([]uint64, len(recorded.pages))
			s.index, s.phase = 0, 14
		case 14:
			n, err := stateEncodeEntriesV1(w, len(recorded.pages)-s.index, uint64(unsafe.Sizeof(PageImageV1{}))+8)
			if err != nil || n == 0 && s.index < len(recorded.pages) {
				return false, err
			}
			end := s.index + n
			for ; s.index < end; s.index++ {
				s.dirty[s.index] = recorded.pages[s.index].PageID
			}
			if s.index < len(recorded.pages) {
				return false, nil
			}
			s.phase = 15
		case 15:
			if ok, err := appendReserve(w, 1, uint64(unsafe.Sizeof(FreelistCandidateV1{}))); !ok || err != nil {
				return false, err
			}
			s.result = &FreelistCandidateV1{generation: s.generation, pages: recorded.pages, dirtyIDs: s.dirty}
			s.phase = 16
		case 16:
			return true, nil
		default:
			return false, ErrGenerationFormat
		}
	}
}
