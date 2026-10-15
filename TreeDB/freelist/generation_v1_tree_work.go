package freelist

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"unsafe"
)

// stateSummaryWorkV1 retains the actual canonical summary walk. Its node is
// private to the original mutation owner until the enclosing operation returns.
type stateSummaryWorkV1 struct {
	node  *stateNode
	index int
	seen  bool
	phase uint8
}

func (s *stateSummaryWorkV1) step(w *iterator.OrdinalScanWork) (bool, error) {
	n := s.node
	if s.phase == 0 {
		if ok, e := appendReserve(w, 1, 5*8); !ok || e != nil {
			return false, e
		}
		n.freeCount, n.retiredCount, n.minRetiredSeq, n.minChunk, n.maxChunk = 0, 0, 0, 0, 0
		if n.chunk != nil {
			if ok, e := appendReserve(w, 1, 4*8); !ok || e != nil {
				return false, e
			}
			n.freeCount = n.chunk.freeCount()
			n.minChunk, n.maxChunk = n.chunk.chunkNo, n.chunk.chunkNo
			s.phase = 1
		} else {
			s.phase = 2
		}
	}
	if s.phase == 1 {
		// The ordinary nil driver keeps the original tight summary primitive.
		if w == nil {
			n.retiredCount, n.minRetiredSeq = n.chunk.retiredSummary()
			s.index = freelistChunkSize
		} else {
			for s.index < freelistChunkSize {
				if ok, e := appendReserve(w, 1, 8); !ok || e != nil {
					return false, e
				}
				seq := n.chunk.retired[s.index]
				if seq != 0 {
					n.retiredCount++
					if n.minRetiredSeq == 0 || seq < n.minRetiredSeq {
						n.minRetiredSeq = seq
					}
				}
				s.index++
			}
		}
		s.phase = 3
	}
	if s.phase == 2 {
		for s.index < len(n.child) {
			if ok, e := appendReserve(w, 1, uint64(unsafe.Sizeof((*stateNode)(nil)))+5*8); !ok || e != nil {
				return false, e
			}
			child := n.child[s.index]
			s.index++
			if child == nil {
				continue
			}
			n.freeCount += child.freeCount
			n.retiredCount += child.retiredCount
			if seq := child.minRetiredSeq; seq != 0 && (n.minRetiredSeq == 0 || seq < n.minRetiredSeq) {
				n.minRetiredSeq = seq
			}
			// index includes nil children, so use the accumulated presence rather
			// than index to identify the first nonempty canonical child.
			if !s.seen {
				s.seen = true
				n.minChunk, n.maxChunk = child.minChunk, child.maxChunk
			} else {
				if child.minChunk < n.minChunk {
					n.minChunk = child.minChunk
				}
				if child.maxChunk > n.maxChunk {
					n.maxChunk = child.maxChunk
				}
			}
		}
		s.phase = 3
	}
	return true, nil
}

// stateDetachWorkV1 is the original PageID-zero detachment traversal. Durable
// nodes remain shared. The fixed path stack is bounded by the canonical key
// width, not by the number of dirty descendants.
type stateDetachFrameV1 struct {
	source, out *stateNode
	index       int
	phase       uint8
}
type stateDetachWorkV1 struct {
	frames      [chunkTrieDepth + 2]stateDetachFrameV1
	depth       int
	result      *stateNode
	begun, done bool
}

func (s *stateDetachWorkV1) begin(n *stateNode) {
	*s = stateDetachWorkV1{begun: true}
	s.frames[0].source = n
}
func (s *stateDetachWorkV1) pop(n *stateNode) {
	s.frames[s.depth] = stateDetachFrameV1{}
	if s.depth == 0 {
		s.result = n
		s.done = true
		return
	}
	s.depth--
	parent := &s.frames[s.depth]
	parent.out.child[parent.index-1] = n
}
func (s *stateDetachWorkV1) step(w *iterator.OrdinalScanWork) (bool, error) {
	for !s.done {
		f := &s.frames[s.depth]
		switch f.phase {
		case 0:
			if ok, e := appendReserve(w, 1, uint64(unsafe.Sizeof(stateNode{}))); !ok || e != nil {
				return false, e
			}
			if f.source == nil || f.source.pageID != 0 {
				s.pop(f.source)
				continue
			}
			// Preserve the original detach copy, including the zero-ID checksum.
			out := *f.source
			f.out = &out
			f.phase = 1
		case 1:
			if f.source.chunk != nil {
				if f.source.chunk.pageID == 0 {
					if ok, e := appendReserve(w, 1, uint64(unsafe.Sizeof(stateChunk{}))); !ok || e != nil {
						return false, e
					}
					chunk := *f.source.chunk
					f.out.chunk = &chunk
				}
				s.pop(f.out)
				continue
			}
			f.phase = 2
		case 2:
			if f.index == len(f.source.child) {
				s.pop(f.out)
				continue
			}
			if ok, e := appendReserve(w, 1, 2*uint64(unsafe.Sizeof((*stateNode)(nil)))); !ok || e != nil {
				return false, e
			}
			child := f.source.child[f.index]
			f.index++
			if child == nil || child.pageID != 0 {
				continue
			}
			if s.depth+1 >= len(s.frames) {
				return false, ErrGenerationFormat
			}
			s.depth++
			s.frames[s.depth] = stateDetachFrameV1{source: child}
		}
	}
	return true, nil
}

type stateMutationFrameV1 struct {
	source, out, only  *stateNode
	phase              uint8
	slot, index, count int
	summary            stateSummaryWorkV1
}

// stateMutationWorkV1 drives the SAME compressed path, private detachment,
// callback and summary stages for ordinary and work-bearing callers. A caller
// supplying work must supply an actually resumable callback; an opaque ordinary
// callback is never invoked with a finite allowance.
type stateMutationWorkV1 struct {
	frames         [chunkTrieDepth + 2]stateMutationFrameV1
	depth          int
	key            uint64
	private        bool
	stats          *FreelistTxnStats
	ordinary       func(*stateChunk)
	apply          func(*stateChunk, *iterator.OrdinalScanWork) (bool, error)
	detach         stateDetachWorkV1
	result         *stateNode
	done           bool
	callbackErr    error
	pending        pendingRetirementCursorV1
	pendingEnabled bool
}

func (s *stateMutationWorkV1) begin(n *stateNode, key uint64, private bool, stats *FreelistTxnStats, ordinary func(*stateChunk), apply func(*stateChunk, *iterator.OrdinalScanWork) (bool, error)) {
	*s = stateMutationWorkV1{key: key, private: private, stats: stats, ordinary: ordinary, apply: apply}
	s.frames[0].source = n
}
func (s *stateMutationWorkV1) push(n *stateNode) error {
	if s.depth+1 >= len(s.frames) {
		return ErrGenerationFormat
	}
	s.depth++
	s.frames[s.depth] = stateMutationFrameV1{source: n}
	return nil
}
func (s *stateMutationWorkV1) pop(n *stateNode) {
	s.frames[s.depth] = stateMutationFrameV1{}
	if s.depth == 0 {
		s.result = n
		s.done = true
		return
	}
	s.depth--
	// The parent consumes the retained child result in its admitted next phase.
	s.result = n
}
func (s *stateMutationWorkV1) step(w *iterator.OrdinalScanWork) (bool, error) {
	if s.callbackErr != nil {
		return false, s.callbackErr
	}
	if w != nil && s.apply == nil && !s.pendingEnabled {
		return false, ErrGenerationFormat
	}
	for !s.done {
		f := &s.frames[s.depth]
		switch f.phase {
		case 0:
			if ok, e := appendReserve(w, 1, uint64(unsafe.Sizeof(stateNode{}))); !ok || e != nil {
				return false, e
			}
			n := f.source
			if n == nil || n.freeCount+n.retiredCount == 0 {
				f.phase = 1
				continue
			}
			split := firstDifferingDepthV1(n.minChunk, s.key)
			if split < chunkTrieDepth && (n.chunk != nil || split < int(n.depth)) {
				f.slot = split
				f.phase = 10
				if e := s.push(nil); e != nil {
					return false, e
				}
				continue
			}
			f.phase = 2
		case 1:
			if ok, e := appendReserve(w, 1, uint64(unsafe.Sizeof(stateNode{}))+uint64(unsafe.Sizeof(stateChunk{}))); !ok || e != nil {
				return false, e
			}
			f.out = &stateNode{chunk: &stateChunk{chunkNo: s.key}}
			f.phase = 5
		case 2:
			n := f.source
			f.out = n
			if !s.private || n.pageID != 0 {
				if ok, e := appendReserve(w, 1, uint64(unsafe.Sizeof(stateNode{}))); !ok || e != nil {
					return false, e
				}
				f.out = cloneStateNode(n)
				if s.private {
					f.phase = 3
					continue
				}
			}
			f.phase = 4
		case 3:
			if s.detach.begun {
				ready, e := s.detach.step(w)
				if !ready || e != nil {
					return false, e
				}
				f.out.child[f.index-1] = s.detach.result
				s.detach = stateDetachWorkV1{}
			}
			if f.index == len(f.out.child) {
				f.phase = 4
				continue
			}
			if ok, e := appendReserve(w, 1, 2*uint64(unsafe.Sizeof((*stateNode)(nil)))); !ok || e != nil {
				return false, e
			}
			child := f.out.child[f.index]
			f.index++
			if child != nil && child.pageID == 0 {
				s.detach.begin(child)
			}
		case 4:
			n := f.source
			if n.chunk != nil {
				if !s.private || n.pageID != 0 || n.chunk.pageID != 0 {
					if ok, e := appendReserve(w, 1, uint64(unsafe.Sizeof(stateChunk{}))); !ok || e != nil {
						return false, e
					}
					f.out.chunk = n.chunk.clone()
				} else if s.stats != nil {
					if ok, e := appendReserve(w, 1, 8); !ok || e != nil {
						return false, e
					}
					s.stats.PrivateChunkReuses++
				}
				f.phase = 5
				continue
			}
			if ok, e := appendReserve(w, 1, 2*uint64(unsafe.Sizeof((*stateNode)(nil)))); !ok || e != nil {
				return false, e
			}
			f.slot = chunkNibble(s.key, int(f.out.depth))
			f.phase = 7
			if e := s.push(f.out.child[f.slot]); e != nil {
				return false, e
			}
		case 5:
			if s.pendingEnabled {
				ready, e := s.pending.applyRetired(f.out.chunk, w)
				if e != nil {
					s.callbackErr = e
					return false, e
				}
				if !ready {
					return false, nil
				}
			} else if s.apply != nil {
				ready, e := s.apply(f.out.chunk, w)
				if e != nil {
					s.callbackErr = e
					return false, e
				}
				if !ready {
					return false, nil
				}
			} else {
				s.ordinary(f.out.chunk)
			}
			f.summary = stateSummaryWorkV1{node: f.out}
			f.phase = 6
		case 6:
			ready, e := f.summary.step(w)
			if !ready || e != nil {
				return false, e
			}
			if f.out.freeCount+f.out.retiredCount == 0 {
				if ok, e := appendReserve(w, 1, uint64(unsafe.Sizeof(stateNode{}))); !ok || e != nil {
					return false, e
				}
				f.out = &stateNode{}
			}
			s.pop(f.out)
		case 7:
			if ok, e := appendReserve(w, 1, 2*uint64(unsafe.Sizeof((*stateNode)(nil)))); !ok || e != nil {
				return false, e
			}
			child := s.result
			if child.freeCount+child.retiredCount == 0 {
				child = nil
			}
			f.out.child[f.slot] = child
			f.index, f.count = 0, 0
			f.only = nil
			f.phase = 8
		case 8:
			for f.index < len(f.out.child) {
				if ok, e := appendReserve(w, 1, uint64(unsafe.Sizeof((*stateNode)(nil)))); !ok || e != nil {
					return false, e
				}
				child := f.out.child[f.index]
				f.index++
				if child != nil {
					f.count++
					f.only = child
				}
			}
			if f.count == 0 {
				if ok, e := appendReserve(w, 1, uint64(unsafe.Sizeof(stateNode{}))); !ok || e != nil {
					return false, e
				}
				s.pop(&stateNode{})
				continue
			}
			if f.count == 1 {
				s.pop(f.only)
				continue
			}
			f.summary = stateSummaryWorkV1{node: f.out}
			f.phase = 6
		case 10:
			leaf := s.result
			if leaf.freeCount+leaf.retiredCount == 0 {
				s.pop(f.source)
				continue
			}
			if ok, e := appendReserve(w, 1, uint64(unsafe.Sizeof(stateNode{}))+2*uint64(unsafe.Sizeof((*stateNode)(nil)))); !ok || e != nil {
				return false, e
			}
			out := &stateNode{depth: uint8(f.slot), prefix: chunkPrefixV1(s.key, f.slot)}
			out.child[chunkNibble(f.source.minChunk, f.slot)] = f.source
			out.child[chunkNibble(s.key, f.slot)] = leaf
			f.out = out
			f.summary = stateSummaryWorkV1{node: out}
			f.phase = 6
		default:
			return false, ErrGenerationFormat
		}
	}
	return true, nil
}
