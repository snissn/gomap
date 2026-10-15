package freelist

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"unsafe"
)

// candidateBeginWorkV1 prepares the original successor privately. It never
// installs a.cow.prepared, retains the live allocator transaction, or creates
// another ledger. The caller must bind the resulting successor before any
// irreversible activation; this work is not permission to yield with a prepared
// allocator slot installed.
type candidateBeginWorkV1 struct {
	base          *FreelistGenerationV1
	parent        GenerationRefV1
	ledger        *ReservationLedger
	txn           *FreelistTxn
	detach        stateDetachWorkV1
	mutation      stateMutationWorkV1
	markNode      *stateNode
	pageIndex     int
	chunkNo       uint64
	phase         uint8
	mutationBegun bool
	pending       pendingRetirementCursorV1
}

func reserveStateMapInsertV1(m map[uint64]struct{}, key uint64, w *iterator.OrdinalScanWork) (bool, error) {
	if w == nil {
		m[key] = struct{}{}
		return true, nil
	}
	if ok, e := appendReserve(w, 1, 16); !ok || e != nil {
		return false, e
	}
	if _, exists := m[key]; exists {
		return true, nil
	}
	// A Go map insertion may grow its original backing. Admit a conservative
	// full live-entry rehash and old/new storage before insertion; large maps
	// honestly require a retained backing-growth seam instead of hiding it.
	count := uint64(len(m))
	if ok, e := appendReserve(w, 1+count, 256+128*(count+1)); !ok || e != nil {
		return false, e
	}
	m[key] = struct{}{}
	return true, nil
}

// pendingRetirementCursorV1 borrows the original immutable reservation extents.
// It is copied into the active mutation by value; it never captures the large
// enclosing continuation in a method-value closure on ordinary BeginCandidate.
type pendingRetirementCursorV1 struct {
	extents                []ReservationExtentV1
	highWater              uint64
	extentIndex            int
	offset, chunkNo, items uint64
	extentLoaded           bool
}

func (s *pendingRetirementCursorV1) seekPending(w *iterator.OrdinalScanWork) (bool, bool, error) {
	for s.extentIndex < len(s.extents) {
		e := s.extents[s.extentIndex]
		if !s.extentLoaded {
			if ok, err := appendReserve(w, 1, uint64(unsafe.Sizeof(e))); !ok || err != nil {
				return false, false, err
			}
			s.extentLoaded = true
		}
		if e.Kind != ReservationPendingMetadataRetirement || e.LastReachableCommitSeq == 0 || s.offset >= uint64(e.Count) {
			s.extentIndex++
			s.offset = 0
			s.extentLoaded = false
			continue
		}
		id := e.StartPageID + s.offset
		if id < 2 {
			if ok, err := appendReserve(w, 1, 16); !ok || err != nil {
				return false, false, err
			}
			s.offset++
			continue
		}
		if id >= s.highWater {
			s.extentIndex++
			s.offset = 0
			s.extentLoaded = false
			continue
		}
		return true, true, nil
	}
	return false, true, nil
}
func (s *pendingRetirementCursorV1) applyRetired(c *stateChunk, w *iterator.OrdinalScanWork) (bool, error) {
	for {
		found, ready, err := s.seekPending(w)
		if !ready || err != nil {
			return false, err
		}
		if !found {
			return true, nil
		}
		e := s.extents[s.extentIndex]
		id := e.StartPageID + s.offset
		if id>>freelistChunkShift != s.chunkNo {
			return true, nil
		}
		if ok, err := appendReserve(w, 1, 32); !ok || err != nil {
			return false, err
		}
		offset := id & (freelistChunkSize - 1)
		c.setFree(offset, false)
		c.retired[offset] = e.LastReachableCommitSeq
		s.items++
		s.offset++
	}
}
func (s *candidateBeginWorkV1) markPath(w *iterator.OrdinalScanWork) (bool, error) {
	for s.markNode != nil {
		n := s.markNode
		if ok, e := appendReserve(w, 1, uint64(unsafe.Sizeof(stateNode{}))); !ok || e != nil {
			return false, e
		}
		if n.freeCount+n.retiredCount == 0 {
			if n.pageID != 0 {
				if ok, e := reserveStateMapInsertV1(s.txn.replacedMetadata, n.pageID, w); !ok || e != nil {
					return false, e
				}
			}
			s.markNode = nil
			return true, nil
		}
		if n.chunk != nil {
			if n.chunk.chunkNo == s.chunkNo && n.chunk.pageID != 0 {
				if ok, e := reserveStateMapInsertV1(s.txn.replacedMetadata, n.chunk.pageID, w); !ok || e != nil {
					return false, e
				}
			}
			s.markNode = nil
			return true, nil
		}
		if chunkPrefixV1(s.chunkNo, int(n.depth)) != n.prefix {
			s.markNode = nil
			return true, nil
		}
		if n.pageID != 0 {
			if ok, e := reserveStateMapInsertV1(s.txn.replacedMetadata, n.pageID, w); !ok || e != nil {
				return false, e
			}
		}
		s.markNode = n.child[chunkNibble(s.chunkNo, int(n.depth))]
	}
	return true, nil
}
func (s *candidateBeginWorkV1) step(base *FreelistGenerationV1, parent GenerationRefV1, ledger *ReservationLedger, w *iterator.OrdinalScanWork) (*FreelistTxn, bool, error) {
	if s.phase != 0 && (base != s.base || parent != s.parent || (ledger != nil && ledger != s.ledger)) {
		return nil, false, ErrGenerationParent
	}
	for {
		switch s.phase {
		case 0:
			if ok, e := appendReserve(w, 1, uint64(unsafe.Sizeof(FreelistTxn{}))+2*48); !ok || e != nil {
				return nil, false, e
			}
			if base == nil || base.root == nil {
				return nil, false, ErrGenerationFormat
			}
			if base.ref.HeaderPageID != 0 && (parent.GenerationID != base.ref.GenerationID || parent.Digest != base.ref.Digest || parent.HeaderPageID != base.ref.HeaderPageID) {
				return nil, false, ErrGenerationParent
			}
			if ledger == nil {
				if w != nil {
					return nil, false, ErrGenerationFormat
				}
				ledger = NewReservationLedger()
			}
			s.base, s.parent, s.ledger = base, parent, ledger
			s.txn = &FreelistTxn{base: base, ledger: ledger, root: base.root, highWater: base.highWater, changedChunks: map[uint64]struct{}{}, replacedMetadata: map[uint64]struct{}{}}
			s.detach.begin(base.root)
			s.pending = pendingRetirementCursorV1{extents: base.record.Extents, highWater: base.highWater}
			s.phase = 1
		case 1:
			if base != s.base || parent != s.parent || (ledger != nil && ledger != s.ledger) {
				return nil, false, ErrGenerationParent
			}
			ready, e := s.detach.step(w)
			if !ready || e != nil {
				return nil, false, e
			}
			s.txn.root = s.detach.result
			s.txn.privatePreparation = true
			s.phase = 2
		case 2:
			if s.base.ref.HeaderPageID != 0 {
				if ok, e := reserveStateMapInsertV1(s.txn.replacedMetadata, s.base.ref.HeaderPageID, w); !ok || e != nil {
					return nil, false, e
				}
			}
			s.phase = 3
		case 3:
			for s.pageIndex < len(s.base.record.pageIDs) {
				if ok, e := appendReserve(w, 1, 8); !ok || e != nil {
					return nil, false, e
				}
				id := s.base.record.pageIDs[s.pageIndex]
				if ok, e := reserveStateMapInsertV1(s.txn.replacedMetadata, id, w); !ok || e != nil {
					return nil, false, e
				}
				s.pageIndex++
			}
			s.phase = 4
		case 4:
			found, ready, e := s.pending.seekPending(w)
			if !ready || e != nil {
				return nil, false, e
			}
			if !found {
				s.phase = 8
				continue
			}
			extent := s.base.record.Extents[s.pending.extentIndex]
			s.chunkNo = (extent.StartPageID + s.pending.offset) >> freelistChunkShift
			s.markNode = s.txn.root
			s.pending.items = 0
			s.pending.chunkNo = s.chunkNo
			s.phase = 5
		case 5:
			ready, e := s.markPath(w)
			if !ready || e != nil {
				return nil, false, e
			}
			if ok, e := appendReserve(w, 1, uint64(unsafe.Sizeof(stateMutationWorkV1{}))); !ok || e != nil {
				return nil, false, e
			}
			s.mutation.begin(s.txn.root, s.chunkNo, true, &s.txn.stats, nil, nil)
			s.mutation.pending = s.pending
			s.mutation.pendingEnabled = true
			s.mutationBegun = true
			s.phase = 6
		case 6:
			ready, e := s.mutation.step(w)
			if !ready || e != nil {
				return nil, false, e
			}
			s.pending = s.mutation.pending
			s.phase = 7
		case 7:
			if ok, e := reserveStateMapInsertV1(s.txn.changedChunks, s.chunkNo, w); !ok || e != nil {
				return nil, false, e
			}
			s.txn.root = s.mutation.result
			s.txn.stats.PrivateMutationPaths++
			s.txn.stats.StateMutationPaths++
			s.txn.stats.StateMutationItems += s.pending.items
			s.mutationBegun = false
			s.phase = 4
		case 8:
			return s.txn, true, nil
		default:
			return nil, false, ErrGenerationFormat
		}
	}
}
