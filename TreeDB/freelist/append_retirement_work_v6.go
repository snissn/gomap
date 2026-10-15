package freelist

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"unsafe"
)

// appendRetirementWorkV6 retains a private persistent-tree edit, never the live
// allocator transaction. Revoking private mutation before borrowing its root
// keeps that root immutable while ordinary writers run between calls. Adoption
// rechecks the current root and generation under the original allocator lock.
// A concurrent writer invalidates preparation; it cannot receive stale state.
// This control is embedded in the producer's charged AppendWork allocation.
type appendRetirementWorkV6 struct {
	root       *stateNode
	generation *FreelistGenerationV1
	highWater  uint64
	node       *stateNode
	replaced   [chunkTrieDepth + 2]uint64
	count      int
	extent     [1]ReservationExtentV1
	mutation   stateMutationWorkV1
	phase      uint8
}

func (s *appendRetirementWorkV6) step(a *Allocator, id uint64, w *iterator.OrdinalScanWork) (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cow == nil || a.cow.txn == nil {
		return false, ErrGenerationFormat
	}
	if a.cow.waitErr != nil {
		return false, a.cow.waitErr
	}
	if a.cow.prepared != nil {
		return false, nil
	}
	t := a.cow.txn
	if err := t.valid(); err != nil {
		return false, err
	}
	if s.phase != 0 && (t.root != s.root || t.highWater != s.highWater || a.cow.generation != s.generation) {
		if ok, e := appendReserve(w, 1, uint64(unsafe.Sizeof(*s))); !ok || e != nil {
			return false, e
		}
		*s = appendRetirementWorkV6{}
	}
	if s.phase == 0 {
		if id < 2 || id >= t.highWater || a.cow.generation.commitSeq == ^uint64(0) {
			return false, ErrGenerationFormat
		}
		if ok, e := appendReserve(w, 1, uint64(unsafe.Sizeof(*s))); !ok || e != nil {
			return false, e
		}
		t.privatePreparation = false
		s.root, s.node, s.highWater, s.generation = t.root, t.root, t.highWater, a.cow.generation
		s.extent[0] = ReservationExtentV1{StartPageID: id, Count: 1, Kind: ReservationPendingMetadataRetirement, LastReachableCommitSeq: a.cow.generation.commitSeq + 1}
		s.phase = 1
	}
	key := id >> freelistChunkShift
	if s.phase == 1 {
		for s.node != nil {
			if ok, e := appendReserve(w, 1, uint64(unsafe.Sizeof(stateNode{}))); !ok || e != nil {
				return false, e
			}
			n := s.node
			s.node = nil
			pageID := uint64(0)
			switch {
			case n.freeCount+n.retiredCount == 0:
				pageID = n.pageID
			case n.chunk != nil:
				if n.chunk.chunkNo == key {
					pageID = n.chunk.pageID
				}
			case chunkPrefixV1(key, int(n.depth)) == n.prefix:
				pageID = n.pageID
				s.node = n.child[chunkNibble(key, int(n.depth))]
			}
			if pageID != 0 {
				if s.count == len(s.replaced) {
					return false, ErrGenerationFormat
				}
				s.replaced[s.count] = pageID
				s.count++
			}
		}
		s.mutation.begin(s.root, key, false, nil, nil, nil)
		s.mutation.pendingEnabled = true
		s.mutation.pending = pendingRetirementCursorV1{extents: s.extent[:], highWater: s.highWater, chunkNo: key}
		s.phase = 2
	}
	if s.phase == 2 {
		ready, e := s.mutation.step(w)
		if !ready || e != nil {
			return false, e
		}
		s.phase = 3
	}
	// The current maps are read only here. Admit their complete actual copy and
	// insertion backing before replacing either map or the live tree root.
	records := uint64(3 + s.count + len(t.replacedMetadata) + len(t.changedChunks))
	bytes := uint64(512 + 128*(s.count+len(t.replacedMetadata)+len(t.changedChunks)+1))
	if ok, e := appendReserve(w, records, bytes); !ok || e != nil {
		return false, e
	}
	replaced := make(map[uint64]struct{}, len(t.replacedMetadata)+s.count)
	for k := range t.replacedMetadata {
		replaced[k] = struct{}{}
	}
	for _, k := range s.replaced[:s.count] {
		replaced[k] = struct{}{}
	}
	changed := make(map[uint64]struct{}, len(t.changedChunks)+1)
	for k := range t.changedChunks {
		changed[k] = struct{}{}
	}
	changed[key] = struct{}{}
	t.root = s.mutation.result
	t.replacedMetadata, t.changedChunks = replaced, changed
	t.requiresGeneration = true
	t.stats.StateMutationPaths++
	t.stats.StateMutationItems++
	a.stats.FreePages++
	if TestHookRetireCOWBeforeUnlock != nil {
		TestHookRetireCOWBeforeUnlock()
	}
	return true, nil
}
