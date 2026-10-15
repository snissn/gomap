package freelist

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

// txnPruneWorkV1 is the canonical summary-pruned DFS and chunk mutation used
// by ordinary and finite preparation. The four-word selection mask is private
// fixed scratch. It is reused only after the SAME chunk mutation has finished;
// no generation page or persistent COW chunk is reset or pooled.
type txnPruneFrameV1 struct {
	node    *stateNode
	next    int
	entered bool
}
type txnPruneWorkV1 struct {
	txn           *FreelistTxn
	cap           ReuseCapability
	frames        [chunkTrieDepth + 2]txnPruneFrameV1
	depth         int
	selected      *stateChunk
	mask          [4]uint64
	offset, count uint64
	marker        candidateBeginWorkV1
	mutation      stateMutationWorkV1
	phase         uint8
	begun, done   bool
}

func (s *txnPruneWorkV1) step(t *FreelistTxn, cap ReuseCapability, w *iterator.OrdinalScanWork) (bool, error) {
	if t == nil || t.consumed {
		return false, ErrCandidateConsumed
	}
	if s.txn == nil {
		s.txn, s.cap = t, cap
	}
	if s.txn != t || s.cap != cap {
		return false, ErrGenerationFormat
	}
	return s.advance(t, cap, w)
}

func (s *txnPruneWorkV1) advance(t *FreelistTxn, cap ReuseCapability, w *iterator.OrdinalScanWork) (bool, error) {
	if t == nil || t.consumed || cap.oldestRecoverableCommitSeq == 0 {
		return true, nil
	}
	if !s.begun {
		if ok, e := appendReserve(w, 1, 32); !ok || e != nil {
			return false, e
		}
		t.stats.OldestRecoverableCommitSeq = cap.oldestRecoverableCommitSeq
		t.stats.MinPinnedSnapshotCommitSeq = cap.minPinnedSnapshotCommitSeq
		t.stats.HistoryFloorCommitSeq = cap.historyFloorCommitSeq
		s.frames[0].node = t.root
		s.begun = true
	}
	for !s.done {
		switch s.phase {
		case 0:
			if s.depth < 0 {
				s.done = true
				continue
			}
			f := &s.frames[s.depth]
			if !f.entered {
				if ok, e := appendReserve(w, 1, uint64(unsafe.Sizeof(stateNode{}))); !ok || e != nil {
					return false, e
				}
				f.entered = true
				n := f.node
				if n == nil || n.retiredCount == 0 || n.minRetiredSeq == 0 || n.minRetiredSeq >= capabilityThreshold(cap) {
					s.depth--
					continue
				}
				t.stats.PageVisits++
				if n.chunk != nil {
					s.selected = n.chunk
					s.mask = [4]uint64{}
					s.offset, s.count = 0, 0
					s.phase = 1
					continue
				}
			}
			if f.next == len(f.node.child) {
				s.depth--
				continue
			}
			if ok, e := appendReserve(w, 1, 8); !ok || e != nil {
				return false, e
			}
			child := f.node.child[f.next]
			f.next++
			if child == nil {
				continue
			}
			if s.depth+1 >= len(s.frames) {
				return false, ErrGenerationFormat
			}
			s.depth++
			s.frames[s.depth] = txnPruneFrameV1{node: child}
		case 1:
			for s.offset < freelistChunkSize {
				if ok, e := appendReserve(w, 1, 16); !ok || e != nil {
					return false, e
				}
				off := s.offset
				seq := s.selected.retired[off]
				s.offset++
				if seq == 0 || !cap.permits(seq) {
					continue
				}
				s.mask[off/64] |= uint64(1) << (off % 64)
				s.count++
				if lag := cap.oldestRecoverableCommitSeq - seq; lag > t.stats.ReuseLag {
					t.stats.ReuseLag = lag
				}
			}
			if s.count == 0 {
				s.depth--
				s.phase = 0
				continue
			}
			s.marker.txn, s.marker.markNode, s.marker.chunkNo = t, t.root, s.selected.chunkNo
			s.phase = 2
		case 2:
			if w == nil {
				t.markReplacedPath(s.selected.chunkNo)
			} else {
				ready, e := s.marker.markPath(w)
				if !ready || e != nil {
					return false, e
				}
			}
			s.marker = candidateBeginWorkV1{}
			s.mutation.begin(t.root, s.selected.chunkNo, t.privatePreparation, &t.stats, nil, nil)
			s.mutation.pruneEnabled, s.mutation.pruneMask = true, s.mask
			s.phase = 3
		case 3:
			ready, e := s.mutation.step(w)
			if !ready || e != nil {
				return false, e
			}
			t.root = s.mutation.result
			s.phase = 4
		case 4:
			if w == nil {
				t.changedChunks[s.selected.chunkNo] = struct{}{}
			} else {
				ready, e := s.marker.mapInsert.step(&t.changedChunks, s.selected.chunkNo, w)
				if !ready || e != nil {
					return false, e
				}
			}
			t.stats.StateMutationPaths++
			t.stats.StateMutationItems += s.count
			if t.privatePreparation {
				t.stats.PrivateMutationPaths++
			}
			s.marker = candidateBeginWorkV1{}
			s.selected = nil
			s.depth--
			s.phase = 0
		default:
			return false, ErrGenerationFormat
		}
	}
	return true, nil
}
