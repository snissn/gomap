package freelist

import (
	"maps"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

// Placement borrows a consumed private transaction. It never keeps an iterator
// over the live ledger across a yield. A speculative run is rechecked by the
// original atomic claim before any free bit is removed.
type metadataReuseWorkV1 struct {
	search                                          metadataChunkSearch
	walk                                            metadataFreeSearchWorkV1
	chunk                                           *stateChunk
	free                                            [4]uint64
	offset, run, runStart, start, count, statePages uint64
	path                                            *stateNode
	planned                                         FreelistTxn
	marker                                          candidateBeginWorkV1
	inventory                                       reservationInventoryWorkV1
	counter                                         statePageCountWorkV1
	mutation                                        stateMutationWorkV1
	extents                                         []ReservationExtentV1
	phase                                           uint8
	begun, reused                                   bool
}

type metadataFreeSearchFrameV1 struct {
	node    *stateNode
	next    int
	visited bool
}
type metadataFreeSearchWorkV1 struct {
	frames [chunkTrieDepth + 2]metadataFreeSearchFrameV1
	depth  int
	begun  bool
}

func (s *metadataFreeSearchWorkV1) step(root *stateNode, lower uint64, visits *uint64, w *iterator.OrdinalScanWork) (*stateChunk, bool, error) {
	if !s.begun {
		s.frames[0].node, s.begun = root, true
	}
	for s.depth >= 0 {
		f := &s.frames[s.depth]
		if !f.visited {
			if ok, e := appendReserve(w, 1, uint64(unsafe.Sizeof(stateNode{}))); !ok || e != nil {
				return nil, false, e
			}
			f.visited = true
			n := f.node
			if n == nil || n.freeCount == 0 || n.maxChunk < lower {
				s.depth--
				continue
			}
			*visits++
			if n.chunk != nil {
				return n.chunk, true, nil
			}
		}
		if f.next == 16 {
			s.depth--
			continue
		}
		if ok, e := appendReserve(w, 1, 8); !ok || e != nil {
			return nil, false, e
		}
		child := f.node.child[f.next]
		f.next++
		if child == nil {
			continue
		}
		if s.depth+1 >= len(s.frames) {
			return nil, false, ErrGenerationFormat
		}
		s.depth++
		s.frames[s.depth] = metadataFreeSearchFrameV1{node: child}
	}
	return nil, true, nil
}

func (s *metadataReuseWorkV1) step(t *FreelistTxn, candidate CandidateIDV1, w *iterator.OrdinalScanWork) (bool, error) {
	for {
		switch s.phase {
		case 0:
			if !s.begun {
				if ok, e := appendReserve(w, 1, 24); !ok || e != nil {
					return false, e
				}
				s.search = t.metadataChunkSearch()
				s.begun = true
			}
			if s.search.attempts >= metadataReuseChunkAttempts {
				s.phase = 15
				continue
			}
			var ready bool
			var e error
			if w == nil {
				s.chunk = s.search.next(t)
				ready = true
			} else {
				s.chunk, ready, e = s.walk.step(t.root, s.search.lower, &t.stats.PageVisits, w)
				if !ready || e != nil {
					return false, e
				}
				s.walk = metadataFreeSearchWorkV1{}
				if s.chunk == nil && !s.search.wrapped {
					s.search.wrapped, s.search.lower = true, 0
					continue
				}
				if s.chunk != nil && s.search.wrapped && s.chunk.chunkNo >= s.search.initial {
					s.chunk = nil
				}
				if s.chunk != nil {
					s.search.lower, s.search.last = s.chunk.chunkNo+1, s.chunk.chunkNo
					s.search.attempts++
				}
			}
			if s.chunk == nil {
				s.phase = 15
				continue
			}
			s.start, s.count, s.run, s.runStart, s.offset = 0, 0, 0, 0, 0
			s.phase = 1
		case 1:
			if w == nil {
				s.start, s.count = t.reusableChunkRun(s.chunk)
				s.phase = 3
				continue
			}
			l := t.ledger
			l.mu.Lock()
			records := uint64(1 + len(l.burnedTails) + len(l.candidates))
			ok, e := appendReserve(w, records, 32+records*24)
			if !ok || e != nil {
				l.mu.Unlock()
				return false, e
			}
			s.free = s.chunk.free
			base := s.chunk.chunkNo << freelistChunkShift
			for _, r := range l.burnedTails {
				clearReservedChunkRange(&s.free, base, r.start, r.count)
			}
			for _, r := range l.candidates {
				if r.tailReserved {
					clearReservedChunkRange(&s.free, base, r.tailStart, r.tailCount)
				}
			}
			l.mu.Unlock()
			s.phase = 2
		case 2:
			for s.offset < freelistChunkSize {
				if ok, e := appendReserve(w, 1, 24); !ok || e != nil {
					return false, e
				}
				off := s.offset
				s.offset++
				if s.free[off/64]&(uint64(1)<<(off%64)) == 0 {
					s.run = 0
					continue
				}
				id := s.chunk.chunkNo<<freelistChunkShift | off
				t.ledger.mu.Lock()
				_, owned := t.ledger.owners[id]
				t.ledger.mu.Unlock()
				if owned {
					s.run = 0
					continue
				}
				if s.run == 0 {
					s.runStart = id
				}
				s.run++
				if s.run > s.count {
					s.start, s.count = s.runStart, s.run
				}
			}
			s.phase = 3
		case 3:
			if s.count < 3 || s.chunk.freeCount() < 4 {
				s.phase = 0
				continue
			}
			ready, e := s.counter.step(t.root, w)
			if !ready || e != nil {
				return false, e
			}
			s.statePages = s.counter.count
			s.counter = statePageCountWorkV1{}
			s.path = t.root
			s.phase = 4
		case 4:
			for s.path != nil {
				if ok, e := appendReserve(w, 1, uint64(unsafe.Sizeof(stateNode{}))); !ok || e != nil {
					return false, e
				}
				n := s.path
				if n.pageID != 0 {
					s.statePages++
				}
				if n.chunk != nil {
					s.path = nil
					break
				}
				s.path = n.child[chunkNibble(s.chunk.chunkNo, int(n.depth))]
			}
			if minimum := s.statePages + 2; minimum > s.count || minimum >= s.chunk.freeCount() {
				s.phase = 0
				continue
			}
			s.phase = 5
		case 5:
			count := uint64(len(t.replacedMetadata))
			if ok, e := appendReserve(w, 1+count, uint64(unsafe.Sizeof(FreelistTxn{}))+256+128*count); !ok || e != nil {
				return false, e
			}
			s.planned = *t
			s.planned.replacedMetadata = maps.Clone(t.replacedMetadata)
			if w == nil {
				s.planned.markReplacedPath(s.chunk.chunkNo)
				s.phase = 7
			} else {
				s.marker.txn, s.marker.markNode, s.marker.chunkNo = &s.planned, t.root, s.chunk.chunkNo
				s.phase = 6
			}
		case 6:
			ready, e := s.marker.markPath(w)
			if !ready || e != nil {
				return false, e
			}
			s.marker = candidateBeginWorkV1{}
			s.phase = 7
		case 7:
			var ready bool
			var e error
			if w == nil {
				ready, e = s.inventory.advance(&s.planned, nil)
			} else {
				ready, e = s.inventory.step(&s.planned, w)
			}
			if !ready || e != nil {
				return false, e
			}
			s.extents = s.inventory.normalized.out
			need := s.statePages + reservationPagesForEntries(uint64(len(s.extents))+1) + 1
			if need > s.count || need >= s.chunk.freeCount() {
				s.inventory = reservationInventoryWorkV1{}
				s.extents = nil
				s.phase = 0
				continue
			}
			s.count = need
			s.phase = 8
		case 8:
			claimed, ready, e := t.ledger.claimReusedMetadataWithWorkV1(candidate, s.start, s.count, t.highWater, t.allocated, t.abandonedAppends, w)
			if !ready || e != nil {
				return false, e
			}
			if !claimed {
				s.inventory = reservationInventoryWorkV1{}
				s.extents = nil
				s.phase = 0
				continue
			}
			// This ledger reservation belongs to the same private constructor. No
			// prepared allocator slot has been installed and it can be rolled back.
			if w == nil {
				start, count := s.start, s.count
				t.mutate(s.chunk.chunkNo, func(c *stateChunk) {
					for id := start; id < start+count; id++ {
						c.setFree(id&(freelistChunkSize-1), false)
					}
				})
				s.reused = true
				s.phase = 15
				continue
			}
			s.marker.txn, s.marker.markNode, s.marker.chunkNo = t, t.root, s.chunk.chunkNo
			s.phase = 9
		case 9:
			ready, e := s.marker.markPath(w)
			if !ready || e != nil {
				return false, e
			}
			s.marker = candidateBeginWorkV1{}
			s.mutation.begin(t.root, s.chunk.chunkNo, t.privatePreparation, &t.stats, nil, nil)
			s.mutation.freeRangeEnabled = true
			s.mutation.freeStart, s.mutation.freeCount = s.start, s.count
			s.phase = 10
		case 10:
			ready, e := s.mutation.step(w)
			if !ready || e != nil {
				return false, e
			}
			t.root = s.mutation.result
			s.phase = 11
		case 11:
			ready, e := s.marker.mapInsert.step(&t.changedChunks, s.chunk.chunkNo, w)
			if !ready || e != nil {
				return false, e
			}
			t.stats.StateMutationPaths++
			t.stats.StateMutationItems += s.count
			if t.privatePreparation {
				t.stats.PrivateMutationPaths++
			}
			s.reused = true
			s.phase = 15
		case 15:
			if ok, e := appendReserve(w, 1, 16); !ok || e != nil {
				return false, e
			}
			s.search.finish(t, s.reused)
			s.phase = 16
		case 16:
			return true, nil
		default:
			return false, ErrGenerationFormat
		}
	}
}
