package freelist

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"unsafe"
)

// CandidateMaterializationWorkV1 owns one original transaction after its
// constructor is admitted. It never installs an allocator prepared slot. Page
// images and candidate extents are immutable output, not resettable arena
// scratch: AppendPageSink consumers may retain them after this owner returns.
type CandidateMaterializationWorkV1 struct {
	txn                         *FreelistTxn
	sink                        *CandidatePageSinkV1
	generationID, commitSeq     uint64
	candidateID                 CandidateIDV1
	reuse                       metadataReuseWorkV1
	detach                      stateDetachWorkV1
	counter                     statePageCountWorkV1
	inventory                   reservationInventoryWorkV1
	rootInsert                  stateMapInsertWorkV1
	output                      candidateOutputWorkV1
	recorded                    recordingSink
	extents                     []ReservationExtentV1
	dataIDs                     []uint64
	index                       int
	start, reserved, statePages uint64
	phase                       uint8
	private, detachBegun        bool
	firstErr                    error
	result                      *FreelistCandidateV1
}

// Step binds exact constructor operands once. A false,nil result retains the
// same private transaction/claim/output; callers may run ordinary writers
// between calls. No caller may mutate the consumed transaction or substitute a
// sink/candidate. A real error is sticky and retains original rollback custody.
func (s *CandidateMaterializationWorkV1) Step(t *FreelistTxn, generationID, commitSeq uint64, candidate CandidateIDV1, sink *CandidatePageSinkV1, w *iterator.OrdinalScanWork) (*FreelistCandidateV1, bool, error) {
	if s == nil || w == nil {
		return nil, false, ErrGenerationFormat
	}
	if s.firstErr != nil {
		return nil, false, s.firstErr
	}
	if s.txn == nil {
		if t == nil {
			return nil, false, ErrGenerationFormat
		}
		if e := t.valid(); e != nil {
			return nil, false, e
		}
		if sink == nil || generationID == 0 || commitSeq == 0 || candidate == (CandidateIDV1{}) {
			return nil, false, ErrGenerationFormat
		}
		if t.base.ref.HeaderPageID != 0 && generationID <= t.base.generationID {
			return nil, false, ErrGenerationParent
		}
		if ok, e := appendReserve(w, 1, uint64(unsafe.Sizeof(recordingSink{}))+64); !ok || e != nil {
			return nil, false, e
		}
		s.txn, s.sink, s.generationID, s.commitSeq, s.candidateID = t, sink, generationID, commitSeq, candidate
		s.private = t.privatePreparation
		// Freeze edits before any retained inventory or mutation. No page ID has
		// been assigned yet; the same rollback owner survives every refusal.
		t.consumed = true
		s.recorded.sink, s.recorded.ledger, s.recorded.candidateID = sink, t.ledger, candidate
	}
	if s.txn != t || s.sink != sink || s.generationID != generationID || s.commitSeq != commitSeq || s.candidateID != candidate {
		return nil, false, ErrGenerationFormat
	}
	ready, e := s.advance(w)
	if e != nil {
		s.firstErr = e
		t.privatePreparation = false
	}
	return s.result, ready, e
}
func (s *CandidateMaterializationWorkV1) advance(w *iterator.OrdinalScanWork) (bool, error) {
	t := s.txn
	for {
		switch s.phase {
		case 0:
			ready, e := s.reuse.step(t, s.candidateID, w)
			if !ready || e != nil {
				return false, e
			}
			s.start, s.reserved, s.extents = s.reuse.start, s.reuse.count, s.reuse.extents
			s.phase = 1
		case 1:
			// Shared immutable zero-ID siblings still need private output identities.
			// The selected reused path is already private; copying it too preserves
			// the same output and prevents any inherited sibling from being mutated.
			if !s.private {
				if !s.detachBegun {
					s.detach.begin(t.root)
					s.detachBegun = true
				}
				ready, e := s.detach.step(w)
				if !ready || e != nil {
					return false, e
				}
				t.root = s.detach.result
			}
			t.privatePreparation = false
			s.phase = 2
		case 2:
			if t.root.pageID == 0 {
				s.phase = 5
				continue
			}
			ready, e := s.rootInsert.step(&t.replacedMetadata, t.root.pageID, w)
			if !ready || e != nil {
				return false, e
			}
			s.phase = 3
		case 3:
			if ok, e := appendReserve(w, 1, uint64(unsafe.Sizeof(stateNode{}))); !ok || e != nil {
				return false, e
			}
			t.root = cloneStateNode(t.root)
			s.phase = 4
		case 4:
			if t.root.chunk != nil {
				if ok, e := appendReserve(w, 1, uint64(unsafe.Sizeof(stateChunk{}))); !ok || e != nil {
					return false, e
				}
				t.root.chunk = t.root.chunk.clone()
			}
			s.phase = 5
		case 5:
			if t.highWater == ^uint64(0) && !s.reuse.reused {
				return false, ErrNoAllocatablePage
			}
			ready, e := s.counter.step(t.root, w)
			if !ready || e != nil {
				return false, e
			}
			s.statePages = s.counter.count
			if t.root.freeCount+t.root.retiredCount == 0 {
				s.statePages = 1
			}
			if s.reuse.reused {
				s.phase = 10
			} else {
				s.phase = 6
			}
		case 6:
			ready, e := s.inventory.step(t, w)
			if !ready || e != nil {
				return false, e
			}
			s.extents = s.inventory.normalized.out
			s.phase = 7
		case 7:
			if ok, e := appendReserve(w, 1, uint64(len(t.allocated))*8); !ok || e != nil {
				return false, e
			}
			s.dataIDs = make([]uint64, len(t.allocated))
			s.phase = 8
		case 8:
			n, e := stateEncodeEntriesV1(w, len(s.dataIDs)-s.index, uint64(unsafe.Sizeof(allocatedPage{}))+8)
			if e != nil || n == 0 && s.index < len(s.dataIDs) {
				return false, e
			}
			end := s.index + n
			for ; s.index < end; s.index++ {
				s.dataIDs[s.index] = t.allocated[s.index].id
			}
			if s.index < len(s.dataIDs) {
				return false, nil
			}
			s.phase = 9
		case 9:
			start, count, ready, e := t.ledger.reserveTailWithWorkV1(s.candidateID, t.highWater, s.statePages, s.dataIDs, s.extents, w)
			if !ready || e != nil {
				return false, e
			}
			s.start, s.reserved = start, count
			s.phase = 10
		case 10:
			result, ready, e := s.output.step(t, s.generationID, s.commitSeq, s.start, s.reserved, s.statePages, s.extents, &s.recorded, w)
			if !ready || e != nil {
				return false, e
			}
			s.result = result
			s.phase = 11
		case 11:
			return true, nil
		default:
			return false, ErrGenerationFormat
		}
	}
}
